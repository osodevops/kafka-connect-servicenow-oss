package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.metrics.ServiceNowMetrics;
import sh.oso.servicenow.testing.MockServiceNowServer;

/** The per-table MBean the source task registers reflects what the poller did. */
class SourceMetricsTest {

    private static final int ROWS = 20;

    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private MockServiceNowServer snow;
    private TaskHarness harness;

    @BeforeEach
    void setUp() {
        snow = MockServiceNowServer.start();
        Fixtures.seed(snow.tables(), "incident", ROWS, Instant.now().minusSeconds(3600), 4);
        harness =
                new TaskHarness(snow)
                        .with(ServiceNowMetrics.CONNECTOR_NAME_KEY, "snow-src")
                        .with(SourceConfig.TASK_ID, "2");
    }

    @AfterEach
    void tearDown() {
        harness.close();
        snow.close();
    }

    @Test
    void mbeanReportsRecordsEmittedAndCursorAfterPolling() throws Exception {
        harness.start();
        assertThat(harness.pollUntil(ROWS, 10_000)).hasSize(ROWS);

        ObjectName name = ServiceNowMetrics.sourceTableName("snow-src", 2, "incident");
        assertThat(name.toString())
                .isEqualTo(
                        "sh.oso.servicenow:type=source-table,connector=snow-src,task=2,table=incident");
        assertThat(harness.task().mbeanNames()).containsExactly(name);
        assertThat(ServiceNowMetrics.isRegistered(name)).isTrue();

        TableMetrics plain = harness.task().metrics().get("incident");
        assertThat(server.getAttribute(name, "Table")).isEqualTo("incident");
        assertThat(server.getAttribute(name, "RecordsEmitted")).isEqualTo((long) ROWS);
        assertThat(server.getAttribute(name, "CursorTimestamp"))
                .isEqualTo(plain.cursorTimestamp())
                .asString()
                .matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
        assertThat((long) server.getAttribute(name, "CursorEpochSeconds")).isPositive();
        assertThat((long) server.getAttribute(name, "LagSeconds")).isGreaterThanOrEqualTo(0);
        assertThat((long) server.getAttribute(name, "PagesFetched")).isGreaterThanOrEqualTo(1);
        assertThat((long) server.getAttribute(name, "LastSuccessfulRequestEpochMs")).isPositive();
        assertThat(server.getAttribute(name, "Phase")).isIn("backfill", "stream");
        assertThat(server.getAttribute(name, "ThrottledMillis")).isEqualTo(0L);

        List<String> attributes = new ArrayList<>();
        for (MBeanAttributeInfo info : server.getMBeanInfo(name).getAttributes()) {
            attributes.add(info.getName());
            assertThat(info.isReadable()).isTrue();
            assertThat(info.isWritable()).isFalse();
        }
        assertThat(attributes)
                .containsExactlyInAnyOrder(
                        "Table",
                        "CursorTimestamp",
                        "CursorEpochSeconds",
                        "LagSeconds",
                        "Phase",
                        "RecordsEmitted",
                        "PagesFetched",
                        "DuplicatesSuppressed",
                        "RowsSkipped",
                        "Retries",
                        "ThrottledMillis",
                        "LastPollDurationMs",
                        "LastSuccessfulRequestEpochMs",
                        "SchemaVersion");
    }

    @Test
    void throttledTimeIsAttributedToTheTableAndTheBeanGoesAwayOnStop() throws Exception {
        harness.start();
        assertThat(harness.pollUntil(ROWS, 10_000)).hasSize(ROWS);
        ObjectName name = ServiceNowMetrics.sourceTableName("snow-src", 2, "incident");

        snow.faults().rateLimit(1, Duration.ofSeconds(1));
        harness.pollFor(1500);

        assertThat((long) server.getAttribute(name, "ThrottledMillis"))
                .isGreaterThanOrEqualTo(1000)
                .isEqualTo(harness.task().metrics().get("incident").throttledMillis());

        harness.task().stop();
        assertThat(ServiceNowMetrics.isRegistered(name)).isFalse();
        assertThat(harness.task().mbeanNames()).isEmpty();
    }

    @Test
    void restartingInTheSameJvmReplacesTheBean() throws Exception {
        harness.start();
        harness.pollUntil(ROWS, 10_000);
        ObjectName name = ServiceNowMetrics.sourceTableName("snow-src", 2, "incident");
        assertThat(server.getAttribute(name, "RecordsEmitted")).isEqualTo((long) ROWS);

        harness.restart();
        assertThat(ServiceNowMetrics.isRegistered(name)).isTrue();
        assertThat(server.getAttribute(name, "RecordsEmitted")).isEqualTo(0L);
    }
}
