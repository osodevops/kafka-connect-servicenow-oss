package sh.oso.servicenow.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.lang.management.ManagementFactory;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ServiceNowMetricsTest {

    /** A tiny MXBean so the test does not depend on either connector module. */
    public interface ProbeMXBean {
        long getCount();

        String getLabel();
    }

    static final class Probe implements ProbeMXBean {
        private final long count;
        private final String label;

        Probe(long count, String label) {
            this.count = count;
            this.label = label;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public String getLabel() {
            return label;
        }
    }

    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private ObjectName name;

    @AfterEach
    void tearDown() {
        ServiceNowMetrics.unregister(name);
    }

    @Test
    void registersAnMXBeanThatCanBeQueriedThroughThePlatformServer() throws Exception {
        name = ServiceNowMetrics.sourceTableName("snow-src", 0, "incident");
        assertThat(name.toString())
                .isEqualTo(
                        "sh.oso.servicenow:type=source-table,connector=snow-src,task=0,table=incident");

        assertThat(ServiceNowMetrics.register(name, new Probe(42, "a"), ProbeMXBean.class))
                .isTrue();

        assertThat(ServiceNowMetrics.isRegistered(name)).isTrue();
        assertThat(server.getAttribute(name, "Count")).isEqualTo(42L);
        assertThat(server.getAttribute(name, "Label")).isEqualTo("a");
    }

    @Test
    void reRegistrationReplacesThePreviousBean() throws Exception {
        name = ServiceNowMetrics.sinkWriterName("snow-sink", 3);
        assertThat(name.toString())
                .isEqualTo("sh.oso.servicenow:type=sink-writer,connector=snow-sink,task=3");

        assertThat(ServiceNowMetrics.register(name, new Probe(1, "first"), ProbeMXBean.class))
                .isTrue();
        assertThat(ServiceNowMetrics.register(name, new Probe(2, "second"), ProbeMXBean.class))
                .isTrue();

        assertThat(server.getAttribute(name, "Count")).isEqualTo(2L);
        assertThat(server.queryNames(new ObjectName("sh.oso.servicenow:type=sink-writer,*"), null))
                .filteredOn(n -> n.equals(name))
                .hasSize(1);
    }

    @Test
    void unregisterIsIdempotentAndNeverThrows() {
        name = ServiceNowMetrics.sinkWriterName("snow-sink", 0);
        ServiceNowMetrics.register(name, new Probe(0, ""), ProbeMXBean.class);
        ServiceNowMetrics.unregister(name);
        assertThat(ServiceNowMetrics.isRegistered(name)).isFalse();
        assertThatCode(() -> ServiceNowMetrics.unregister(name)).doesNotThrowAnyException();
        assertThatCode(() -> ServiceNowMetrics.unregister(null)).doesNotThrowAnyException();
    }

    @Test
    void registrationFailureIsReportedNotThrown() {
        name = ServiceNowMetrics.sinkWriterName("snow-sink", 9);
        // A bean that does not implement the interface is not a compliant MXBean.
        Object notABean = new Object();
        @SuppressWarnings({"unchecked", "rawtypes"})
        boolean registered = ServiceNowMetrics.register(name, notABean, (Class) ProbeMXBean.class);
        assertThat(registered).isFalse();
        assertThat(ServiceNowMetrics.isRegistered(name)).isFalse();
    }

    @Test
    void quotesValuesThatJmxWouldMisparse() throws Exception {
        name = ServiceNowMetrics.sourceTableName("acme: prod,eu=1", 2, "incident");
        assertThat(name.getKeyProperty("connector")).isEqualTo("\"acme: prod,eu=1\"");
        assertThat(name.getKeyProperty("table")).isEqualTo("incident");
        assertThat(ObjectName.unquote(name.getKeyProperty("connector")))
                .isEqualTo("acme: prod,eu=1");
        assertThat(ServiceNowMetrics.register(name, new Probe(7, "q"), ProbeMXBean.class)).isTrue();
        assertThat(server.getAttribute(name, "Count")).isEqualTo(7L);
        assertThat(ServiceNowMetrics.value("plain_value")).isEqualTo("plain_value");
        assertThat(ServiceNowMetrics.value("")).isEqualTo("unknown");
        assertThat(ServiceNowMetrics.value("a*b")).isEqualTo(ObjectName.quote("a*b"));
    }

    @Test
    void connectorNameAndTaskIdComeFromTheTaskConfiguration() {
        assertThat(ServiceNowMetrics.connectorName(Map.of("name", "snow-src")))
                .isEqualTo("snow-src");
        assertThat(ServiceNowMetrics.connectorName(Map.of())).isEqualTo("unknown");
        assertThat(ServiceNowMetrics.connectorName(null)).isEqualTo("unknown");

        assertThat(ServiceNowMetrics.taskId(Map.of(ServiceNowMetrics.TASK_ID_KEY, "4")))
                .isEqualTo(4);
        int first = ServiceNowMetrics.taskId(Map.of());
        int second = ServiceNowMetrics.taskId(Map.of(ServiceNowMetrics.TASK_ID_KEY, "x"));
        int third = ServiceNowMetrics.taskId(Map.of(ServiceNowMetrics.TASK_ID_KEY, "-1"));
        assertThat(first).isGreaterThanOrEqualTo(0);
        assertThat(second).isEqualTo(first + 1);
        assertThat(third).isEqualTo(first + 2);
    }
}
