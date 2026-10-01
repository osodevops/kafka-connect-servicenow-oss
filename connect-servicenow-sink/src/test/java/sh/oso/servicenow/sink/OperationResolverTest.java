package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class OperationResolverTest {

    private static final String ID = "0123456789abcdef0123456789abcdef";

    private static OperationResolver resolver(Map<String, String> overrides) {
        return new OperationResolver(TestSupport.config(overrides));
    }

    private static SinkRecord withHeader(String op, Object key, Object value) {
        return TestSupport.record(0, 1, key, value, TestSupport.headers("snow.operation", op));
    }

    @Test
    void inferenceFollowsKeyAndValue() {
        OperationResolver r = resolver(Map.of());
        assertThat(r.resolve(TestSupport.record(0, 1, ID, null), ID)).isEqualTo(Operation.DELETE);
        assertThat(r.resolve(TestSupport.record(0, 1, null, Map.of("a", "b")), null))
                .isEqualTo(Operation.CREATE);
        assertThat(r.resolve(TestSupport.record(0, 1, ID, Map.of("a", "b")), ID))
                .isEqualTo(Operation.PATCH);
        OperationResolver put = resolver(Map.of(SinkConfig.UPDATE_METHOD, "PUT"));
        assertThat(put.resolve(TestSupport.record(0, 1, ID, Map.of("a", "b")), ID))
                .isEqualTo(Operation.PUT);
    }

    @Test
    void headerWinsOverInferenceAndIsCaseInsensitive() {
        OperationResolver r = resolver(Map.of());
        assertThat(r.resolve(withHeader("create", null, Map.of()), null))
                .isEqualTo(Operation.CREATE);
        assertThat(r.resolve(withHeader("Post", null, Map.of()), null)).isEqualTo(Operation.CREATE);
        assertThat(r.resolve(withHeader("PATCH", ID, Map.of()), ID)).isEqualTo(Operation.PATCH);
        assertThat(r.resolve(withHeader("put", ID, Map.of()), ID)).isEqualTo(Operation.PUT);
        assertThat(r.resolve(withHeader("UPDATE", ID, Map.of()), ID)).isEqualTo(Operation.PATCH);
        assertThat(r.resolve(withHeader("delete", ID, Map.of()), ID)).isEqualTo(Operation.DELETE);
        assertThatThrownBy(() -> r.resolve(withHeader("MERGE", ID, Map.of()), ID))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("MERGE");
    }

    @Test
    void upsertHeaderMeansUpdateWithAnIdAndCreateWithout() {
        OperationResolver r = resolver(Map.of(SinkConfig.UPDATE_METHOD, "PUT"));
        assertThat(r.resolve(withHeader("UPSERT", ID, Map.of()), ID)).isEqualTo(Operation.PUT);
        assertThat(r.resolve(withHeader("upsert", null, Map.of()), null))
                .isEqualTo(Operation.CREATE);
    }

    @Test
    void headerModeFallsBackUnlessRequired() {
        OperationResolver lenient = resolver(Map.of(SinkConfig.OPERATION_MODE, "header"));
        assertThat(lenient.resolve(TestSupport.record(0, 1, null, Map.of("a", "b")), null))
                .isEqualTo(Operation.CREATE);
        OperationResolver strict =
                resolver(
                        Map.of(
                                SinkConfig.OPERATION_MODE, "header",
                                SinkConfig.OPERATION_HEADER_REQUIRED, "true"));
        assertThatThrownBy(
                        () ->
                                strict.resolve(
                                        TestSupport.record(0, 1, null, Map.of("a", "b")), null))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("snow.operation");
        assertThat(strict.resolve(withHeader("DELETE", ID, Map.of()), ID))
                .isEqualTo(Operation.DELETE);
    }

    @Test
    void fixedModeUsesTheConfiguredOperationButAHeaderStillWins() {
        OperationResolver r =
                resolver(
                        Map.of(
                                SinkConfig.OPERATION_MODE,
                                "fixed",
                                SinkConfig.OPERATION_FIXED,
                                "put"));
        assertThat(r.resolve(TestSupport.record(0, 1, ID, Map.of("a", "b")), ID))
                .isEqualTo(Operation.PUT);
        assertThat(r.resolve(withHeader("delete", ID, Map.of()), ID)).isEqualTo(Operation.DELETE);
        assertThatThrownBy(() -> r.resolve(TestSupport.record(0, 1, null, Map.of("a", "b")), null))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("needs a sys_id");
    }

    @Test
    void operationsThatNeedAnIdOrAValueRejectRecordsWithout() {
        OperationResolver r = resolver(Map.of());
        assertThatThrownBy(() -> r.resolve(withHeader("PATCH", null, Map.of()), null))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("needs a sys_id");
        assertThatThrownBy(() -> r.resolve(withHeader("CREATE", ID, null), ID))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("tombstone");
        OperationResolver fixedCreate =
                resolver(
                        Map.of(
                                SinkConfig.OPERATION_MODE,
                                "fixed",
                                SinkConfig.OPERATION_FIXED,
                                "create"));
        assertThatThrownBy(() -> fixedCreate.resolve(TestSupport.record(0, 1, ID, null), ID))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("tombstone");
        OperationResolver fixedDelete =
                resolver(
                        Map.of(
                                SinkConfig.OPERATION_MODE,
                                "fixed",
                                SinkConfig.OPERATION_FIXED,
                                "delete"));
        assertThat(fixedDelete.resolve(TestSupport.record(0, 1, ID, null), ID))
                .isEqualTo(Operation.DELETE);
    }
}
