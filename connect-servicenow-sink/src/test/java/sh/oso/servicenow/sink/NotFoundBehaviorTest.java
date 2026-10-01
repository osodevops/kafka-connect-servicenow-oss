package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

class NotFoundBehaviorTest {

    private static final String MISSING = "0123456789abcdef0123456789abcdef";

    private MockServiceNowServer snow;
    private ServiceNowClient client;
    private ServiceNowWriter writer;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
    }

    @AfterEach
    void tearDown() {
        if (writer != null) {
            writer.close();
        }
        if (client != null) {
            client.close();
        }
        snow.close();
    }

    private Outcome write(String behavior, String op) {
        SinkConfig config =
                new SinkConfig(
                        TestSupport.props(
                                snow,
                                Map.of(
                                        SinkConfig.NOT_FOUND_BEHAVIOR, behavior,
                                        SinkConfig.OPERATION_MODE, "fixed",
                                        SinkConfig.OPERATION_FIXED, op)));
        client = ServiceNowClient.create(config.coreConfig());
        writer = new ServiceNowWriter(config, client);
        Object value = op.equals("delete") ? null : Map.of("short_description", "x");
        SinkRecord r = TestSupport.record(0, 1, MISSING, value);
        return writer.writeOne(r);
    }

    @ParameterizedTest
    @CsvSource({"patch", "put", "delete"})
    void failReportsNotFound(String op) {
        Outcome o = write("fail", op);
        assertThat(o.isSuccess()).isFalse();
        assertThat(o.classification()).isEqualTo(Classification.NOT_FOUND);
        assertThat(o.status()).isEqualTo(404);
        assertThat(o.error().getMessage()).contains(MISSING);
        assertThat(snow.tables().exists("incident", MISSING)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({"patch", "put", "delete"})
    void ignoreSucceedsWithThe404Status(String op) {
        Outcome o = write("ignore", op);
        assertThat(o.isSuccess()).isTrue();
        assertThat(o.status()).isEqualTo(404);
        assertThat(o.operation()).isEqualTo(Operation.fromConfig(op));
        assertThat(snow.tables().exists("incident", MISSING)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({"patch", "put"})
    void createInsertsTheRowWithTheSameSysId(String op) {
        Outcome o = write("create", op);
        assertThat(o.isSuccess()).isTrue();
        assertThat(o.operation()).isEqualTo(Operation.CREATE);
        assertThat(o.status()).isEqualTo(201);
        assertThat(o.sysId()).isEqualTo(MISSING);
        assertThat(snow.tables().get("incident", MISSING).orElseThrow())
                .containsEntry("short_description", "x");
    }

    @ParameterizedTest
    @CsvSource({"delete"})
    void createOnDeleteIsIgnored(String op) {
        Outcome o = write("create", op);
        assertThat(o.isSuccess()).isTrue();
        assertThat(o.operation()).isEqualTo(Operation.DELETE);
        assertThat(snow.tables().size("incident")).isZero();
    }
}
