package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.metrics.ServiceNowMetrics;

/** The sink-writer MBean the task registers reflects what the writer and reporter did. */
class SinkMetricsTest {

    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private SinkTaskHarness h;

    @BeforeEach
    void setUp() {
        h = new SinkTaskHarness();
    }

    @AfterEach
    void tearDown() {
        h.close();
    }

    private Map<String, String> props(Map<String, String> overrides) {
        Map<String, String> p =
                h.props(
                        Map.of(
                                ServiceNowMetrics.CONNECTOR_NAME_KEY,
                                "snow-sink",
                                SinkConfig.TASK_ID,
                                "1"));
        p.putAll(overrides);
        return p;
    }

    @Test
    void writesAreCountedByOperationAndInFlightReturnsToZero() throws Exception {
        h.start(props(Map.of()));
        ObjectName name = ServiceNowMetrics.sinkWriterName("snow-sink", 1);
        assertThat(name.toString())
                .isEqualTo("sh.oso.servicenow:type=sink-writer,connector=snow-sink,task=1");
        assertThat(h.task.mbeanName()).isEqualTo(name);
        assertThat(ServiceNowMetrics.isRegistered(name)).isTrue();

        h.put(
                TestSupport.record(0, 0, null, Map.of("short_description", "a")),
                TestSupport.record(1, 0, null, Map.of("short_description", "b")));
        String id = h.snow.tables().all("incident").get(0).get("sys_id");
        h.put(TestSupport.record(0, 1, id, Map.of("urgency", "1")));
        h.put(TestSupport.record(0, 2, id, null));

        assertThat(server.getAttribute(name, "Creates")).isEqualTo(2L);
        assertThat(server.getAttribute(name, "Patches")).isEqualTo(1L);
        assertThat(server.getAttribute(name, "Puts")).isEqualTo(0L);
        assertThat(server.getAttribute(name, "Deletes")).isEqualTo(1L);
        assertThat(server.getAttribute(name, "Written")).isEqualTo(4L);
        TabularData byOp = (TabularData) server.getAttribute(name, "WrittenByOperation");
        assertThat(byOp.get(new Object[] {"CREATE"}).get("value")).isEqualTo(2L);
        assertThat(byOp.get(new Object[] {"DELETE"}).get("value")).isEqualTo(1L);
        assertThat(server.getAttribute(name, "InFlight")).isEqualTo(0);
        assertThat((int) server.getAttribute(name, "MaxInFlightObserved")).isBetween(1, 2);
        assertThat(server.getAttribute(name, "Failures")).isEqualTo(0L);
        assertThat(server.getAttribute(name, "Ambiguous")).isEqualTo(0L);
        assertThat(server.getAttribute(name, "Retries")).isEqualTo(0L);
        assertThat(server.getAttribute(name, "ThrottledMillis")).isEqualTo(0L);
        assertThat((long) server.getAttribute(name, "LastSuccessfulRequestEpochMs")).isPositive();

        List<String> attributes = new ArrayList<>();
        for (MBeanAttributeInfo info : server.getMBeanInfo(name).getAttributes()) {
            attributes.add(info.getName());
            assertThat(info.isWritable()).isFalse();
        }
        assertThat(attributes)
                .containsExactlyInAnyOrder(
                        "Creates",
                        "Patches",
                        "Puts",
                        "Deletes",
                        "WrittenByOperation",
                        "Written",
                        "Failures",
                        "Ambiguous",
                        "RetriesExhausted",
                        "InFlight",
                        "MaxInFlightObserved",
                        "Retries",
                        "ThrottledMillis",
                        "ReporterSuccess",
                        "ReporterError",
                        "LastSuccessfulRequestEpochMs");

        h.task.stop();
        assertThat(ServiceNowMetrics.isRegistered(name)).isFalse();
        assertThat(h.task.mbeanName()).isNull();
    }

    @Test
    void failuresRetriesThrottlingAndReportsAreCounted() throws Exception {
        h.start(
                props(
                        Map.of(
                                SinkConfig.REPORTER_BOOTSTRAP_SERVERS, "localhost:9",
                                SinkConfig.REPORTER_SUCCESS_TOPIC, "ok",
                                SinkConfig.REPORTER_ERROR_TOPIC, "bad",
                                SinkConfig.BEHAVIOR_ON_API_ERRORS, "log")));
        ObjectName name = ServiceNowMetrics.sinkWriterName("snow-sink", 1);

        h.snow.faults().rateLimit(1, Duration.ofSeconds(1));
        h.put(TestSupport.record(0, 0, null, Map.of("short_description", "throttled")));
        assertThat((long) server.getAttribute(name, "Retries")).isEqualTo(1);
        assertThat((long) server.getAttribute(name, "ThrottledMillis"))
                .isGreaterThanOrEqualTo(1000);

        h.snow.faults().forbidTable("incident");
        h.put(TestSupport.record(0, 1, null, Map.of("short_description", "forbidden")));
        assertThat(server.getAttribute(name, "Failures")).isEqualTo(1L);
        assertThat(server.getAttribute(name, "ReporterSuccess")).isEqualTo(1L);
        assertThat(server.getAttribute(name, "ReporterError")).isEqualTo(1L);

        h.snow.faults().clear();
        h.snow.faults().serverError(100, 503);
        assertThatThrownBy(
                        () ->
                                h.put(
                                        TestSupport.record(
                                                0, 2, null, Map.of("short_description", "x"))))
                .isInstanceOf(RetriableException.class);
        assertThat(server.getAttribute(name, "RetriesExhausted")).isEqualTo(1L);
        assertThat(server.getAttribute(name, "Failures")).isEqualTo(1L);
        assertThat((long) server.getAttribute(name, "Retries")).isGreaterThanOrEqualTo(4);
        assertThat(server.getAttribute(name, "InFlight")).isEqualTo(0);
    }

    @Test
    void tasksWithoutAnAssignedIdStillGetDistinctNames() {
        h.start(h.props(Map.of(ServiceNowMetrics.CONNECTOR_NAME_KEY, "lonely")));
        ObjectName first = h.task.mbeanName();
        assertThat(first).isNotNull();
        assertThat(first.getKeyProperty("connector")).isEqualTo("lonely");

        ServiceNowSinkTask second = new ServiceNowSinkTask(p -> h.producer);
        second.initialize(h.context);
        second.start(h.props(Map.of(ServiceNowMetrics.CONNECTOR_NAME_KEY, "lonely")));
        try {
            assertThat(second.mbeanName()).isNotNull().isNotEqualTo(first);
            assertThat(ServiceNowMetrics.isRegistered(first)).isTrue();
            assertThat(ServiceNowMetrics.isRegistered(second.mbeanName())).isTrue();
        } finally {
            second.stop();
        }
    }
}
