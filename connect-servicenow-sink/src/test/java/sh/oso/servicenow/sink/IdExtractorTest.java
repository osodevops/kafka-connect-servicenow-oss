package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class IdExtractorTest {

    private static final String A = "0123456789abcdef0123456789abcdef";
    private static final String B = "fedcba9876543210fedcba9876543210";

    private static IdExtractor extractor(Map<String, String> overrides) {
        return new IdExtractor(TestSupport.config(overrides));
    }

    @Test
    void primitiveKeyIsTheSysId() {
        SinkRecord r = TestSupport.record(0, 1, A, Map.of("x", "y"));
        assertThat(extractor(Map.of()).extract(r)).contains(A);
        SinkRecord bytes =
                TestSupport.record(0, 1, A.getBytes(StandardCharsets.UTF_8), Map.of("x", "y"));
        assertThat(extractor(Map.of()).extract(bytes)).contains(A);
        SinkRecord upper = TestSupport.record(0, 1, " " + A.toUpperCase() + " ", Map.of("x", "y"));
        assertThat(extractor(Map.of()).extract(upper)).contains(A);
    }

    @Test
    void structAndMapKeysUseTheConfiguredField() {
        Schema keySchema = SchemaBuilder.struct().field("sysId", Schema.STRING_SCHEMA).build();
        Struct key = new Struct(keySchema).put("sysId", A);
        SinkRecord r =
                new SinkRecord(TestSupport.TOPIC, 0, keySchema, key, null, Map.of("x", "y"), 1);
        assertThat(extractor(Map.of(SinkConfig.SYS_ID_KEY_FIELD, "sysId")).extract(r)).contains(A);
        assertThat(extractor(Map.of()).extract(r)).isEmpty();
        SinkRecord m = TestSupport.record(0, 1, Map.of("sys_id", B), Map.of("x", "y"));
        assertThat(extractor(Map.of()).extract(m)).contains(B);
    }

    @Test
    void valueFieldAndHeaderAreSources() {
        SinkRecord v = TestSupport.record(0, 1, null, Map.of("sys_id", A));
        assertThat(extractor(Map.of()).extract(v)).contains(A);
        SinkRecord h =
                TestSupport.record(
                        0, 1, null, Map.of("x", "y"), TestSupport.headers("snow.sys_id", B));
        assertThat(extractor(Map.of()).extract(h)).contains(B);
        SinkRecord none = TestSupport.record(0, 1, null, Map.of("x", "y"));
        assertThat(extractor(Map.of()).extract(none)).isEmpty();
        SinkRecord tombstone = TestSupport.record(0, 1, A, null);
        assertThat(extractor(Map.of()).extract(tombstone)).contains(A);
    }

    @Test
    void disagreementIsRejectedUnlessAPrecedenceIsSet() {
        SinkRecord r = TestSupport.record(0, 1, A, Map.of("sys_id", B));
        assertThatThrownBy(() -> extractor(Map.of()).extract(r))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("different sys_ids");
        assertThat(extractor(Map.of(SinkConfig.SYS_ID_PRECEDENCE, "key")).extract(r)).contains(A);
        assertThat(extractor(Map.of(SinkConfig.SYS_ID_PRECEDENCE, "value")).extract(r)).contains(B);
        SinkRecord same = TestSupport.record(0, 1, A, Map.of("sys_id", A));
        assertThat(extractor(Map.of()).extract(same)).contains(A);
    }

    @Test
    void precedenceFallsBackWhenThePreferredSourceIsAbsent() {
        SinkRecord r = TestSupport.record(0, 1, null, Map.of("sys_id", B));
        assertThat(extractor(Map.of(SinkConfig.SYS_ID_PRECEDENCE, "key")).extract(r)).contains(B);
    }

    @Test
    void malformedIdsAreRejected() {
        SinkRecord r = TestSupport.record(0, 1, "INC0010042", Map.of("x", "y"));
        assertThatThrownBy(() -> extractor(Map.of()).extract(r))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("32-character hex");
        Map<String, Object> value = new HashMap<>();
        value.put("sys_id", "");
        assertThat(extractor(Map.of()).extract(TestSupport.record(0, 1, null, value))).isEmpty();
        assertThat(extractor(Map.of()).extract(TestSupport.record(0, 1, 42L, Map.of()))).isEmpty();
    }
}
