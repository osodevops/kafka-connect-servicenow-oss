package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.schema.TableMetadataClient;
import sh.oso.servicenow.testing.MockServiceNowServer;

class RecordMapperTest {

    private MockServiceNowServer snow;

    @AfterEach
    void tearDown() {
        if (snow != null) {
            snow.close();
        }
    }

    private static RecordMapper mapper(Map<String, String> overrides) {
        return new RecordMapper(TestSupport.config(overrides), null);
    }

    private static Map<String, Object> map(RecordMapper m, Object value) {
        return m.map(TestSupport.record(0, 1, null, value), "incident").body();
    }

    @Test
    void mapValuesBecomeStringsWithCustomFieldsUntouched() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("short_description", "Printer on fire");
        value.put("urgency", 1);
        value.put("active", true);
        value.put("u_cost_centre", "CC-42");
        value.put("ratio", 2.5);
        assertThat(map(mapper(Map.of()), value))
                .containsExactly(
                        entry("short_description", "Printer on fire"),
                        entry("urgency", "1"),
                        entry("active", "true"),
                        entry("u_cost_centre", "CC-42"),
                        entry("ratio", "2.5"));
    }

    @Test
    void structLogicalTypesRenderAsServiceNowStrings() {
        Schema schema =
                SchemaBuilder.struct()
                        .field("opened_at", Timestamp.SCHEMA)
                        .field("due_date", org.apache.kafka.connect.data.Date.SCHEMA)
                        .field("at", Time.SCHEMA)
                        .field("cost", Decimal.schema(2))
                        .field("count", Schema.INT64_SCHEMA)
                        .field("note", Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        Struct s =
                new Struct(schema)
                        .put("opened_at", new Date(1790667042000L))
                        .put("due_date", new Date(1790640000000L))
                        .put("at", new Date(3_600_000L + 65_000L))
                        .put("cost", new BigDecimal("12.50"))
                        .put("count", 7L);
        Map<String, Object> body =
                mapper(Map.of())
                        .map(TestSupport.struct(0, 1, null, schema, s, null), "incident")
                        .body();
        assertThat(body)
                .containsExactly(
                        entry("opened_at", "2026-09-29 07:30:42"),
                        entry("due_date", "2026-09-29"),
                        entry("at", "01:01:05"),
                        entry("cost", "12.50"),
                        entry("count", "7"));
    }

    @Test
    void jsonStringValuesAreParsedAndOtherPrimitivesRejected() {
        assertThat(map(mapper(Map.of()), "{\"short_description\":\"x\",\"urgency\":2}"))
                .containsExactly(entry("short_description", "x"), entry("urgency", "2"));
        assertThatThrownBy(() -> map(mapper(Map.of()), "not json"))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("not a JSON object");
        assertThatThrownBy(() -> map(mapper(Map.of()), 42L))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("Unsupported record value type");
    }

    @Test
    void nullPolicyOmitsClearsOrRejects() {
        Map<String, Object> value = new HashMap<>();
        value.put("short_description", "x");
        value.put("comments", null);
        assertThat(map(mapper(Map.of()), value)).containsOnlyKeys("short_description");
        assertThat(map(mapper(Map.of(SinkConfig.NULL_BEHAVIOR, "clear")), value))
                .containsEntry("comments", "");
        assertThatThrownBy(() -> map(mapper(Map.of(SinkConfig.NULL_BEHAVIOR, "reject")), value))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("'comments' is null");
    }

    @Test
    void nestedValuesAreRejectedByDefaultNamingThePath() {
        Map<String, Object> value =
                Map.of(
                        "number",
                        "INC1",
                        "location",
                        Map.of("city", "Leeds", "geo", Map.of("lat", 1)));
        assertThatThrownBy(() -> map(mapper(Map.of()), value))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("'location' is a Map")
                .hasMessageContaining("flatten or stringify");
        Schema inner = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("location", inner).build();
        Struct s = new Struct(schema).put("location", new Struct(inner).put("city", "Leeds"));
        assertThatThrownBy(
                        () ->
                                mapper(Map.of())
                                        .map(
                                                TestSupport.struct(0, 1, null, schema, s, null),
                                                "incident"))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("'location' is a Struct");
        assertThatThrownBy(() -> map(mapper(Map.of()), Map.of("tags", List.of("a", "b"))))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("'tags' is an array");
    }

    @Test
    void flattenJoinsNestedKeysWithTheDelimiter() {
        Map<String, Object> geo = new LinkedHashMap<>();
        geo.put("lat", 53.8);
        geo.put("lng", null);
        Map<String, Object> location = new LinkedHashMap<>();
        location.put("city", "Leeds");
        location.put("geo", geo);
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("number", "INC1");
        value.put("location", location);
        assertThat(map(mapper(Map.of(SinkConfig.NESTED_BEHAVIOR, "flatten")), value))
                .containsExactly(
                        entry("number", "INC1"),
                        entry("location_city", "Leeds"),
                        entry("location_geo_lat", "53.8"));
        assertThat(
                        map(
                                mapper(
                                        Map.of(
                                                SinkConfig.NESTED_BEHAVIOR, "flatten",
                                                SinkConfig.NESTED_FLATTEN_DELIMITER, ".",
                                                SinkConfig.NULL_BEHAVIOR, "clear")),
                                value))
                .containsEntry("location.geo.lng", "");
        assertThatThrownBy(
                        () ->
                                map(
                                        mapper(Map.of(SinkConfig.NESTED_BEHAVIOR, "flatten")),
                                        Map.of("location", Map.of("tags", List.of("x")))))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("'location_tags' is an array");
    }

    @Test
    void stringifySendsJsonOnlyWhenConfigured() {
        Schema inner =
                SchemaBuilder.struct()
                        .field("city", Schema.STRING_SCHEMA)
                        .field("since", Timestamp.SCHEMA)
                        .build();
        Schema schema =
                SchemaBuilder.struct()
                        .field("location", inner)
                        .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                        .build();
        Struct s =
                new Struct(schema)
                        .put(
                                "location",
                                new Struct(inner)
                                        .put("city", "Leeds")
                                        .put("since", new Date(1790667042000L)))
                        .put("tags", List.of("a", "b"));
        Map<String, Object> body =
                mapper(Map.of(SinkConfig.NESTED_BEHAVIOR, "stringify"))
                        .map(TestSupport.struct(0, 1, null, schema, s, null), "incident")
                        .body();
        assertThat(body)
                .containsExactly(
                        entry("location", "{\"city\":\"Leeds\",\"since\":\"2026-09-29 07:30:42\"}"),
                        entry("tags", "[\"a\",\"b\"]"));
        Map<String, Object> geo = new LinkedHashMap<>();
        geo.put("lat", 1);
        geo.put("ok", true);
        assertThat(map(mapper(Map.of(SinkConfig.NESTED_BEHAVIOR, "stringify")), Map.of("geo", geo)))
                .containsEntry("geo", "{\"lat\":1,\"ok\":true}");
    }

    @Test
    void renameThenAllowOrDenyListsApply() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("desc", "x");
        value.put("prio", "2");
        value.put("internal", "secret");
        assertThat(
                        map(
                                mapper(
                                        Map.of(
                                                SinkConfig.FIELD_RENAME,
                                                        "desc:short_description,prio:priority",
                                                SinkConfig.FIELD_DENYLIST, "internal")),
                                value))
                .containsExactly(entry("short_description", "x"), entry("priority", "2"));
        assertThat(
                        map(
                                mapper(
                                        Map.of(
                                                SinkConfig.FIELD_RENAME, "desc:short_description",
                                                SinkConfig.FIELD_ALLOWLIST, "short_description")),
                                value))
                .containsOnlyKeys("short_description");
    }

    @Test
    void reservedFieldsAreStrippedUnlessExplicitlyAllowed() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("sys_id", "0123456789abcdef0123456789abcdef");
        value.put("sys_created_on", "2026-01-01 00:00:00");
        value.put("sys_updated_on", "2026-01-01 00:00:00");
        value.put("sys_mod_count", "3");
        value.put("sys_created_by", "admin");
        value.put("sys_updated_by", "admin");
        value.put("snow.operation", "PATCH");
        value.put("snow.table", "incident");
        value.put("short_description", "x");
        assertThat(map(mapper(Map.of()), value)).containsOnlyKeys("short_description");
        assertThat(
                        map(
                                mapper(
                                        Map.of(
                                                SinkConfig.FIELD_ALLOWLIST,
                                                "sys_id,short_description")),
                                value))
                .containsOnlyKeys("sys_id", "short_description");
    }

    @Test
    void unknownFieldPolicyUsesTheDictionary() {
        snow = MockServiceNowServer.start();
        TestSupport.seedDictionary(snow, "incident", "short_description", "u_cost_centre");
        TableMetadataClient metadata =
                new TableMetadataClient(snow.client().tableApi(), Duration.ofMinutes(5));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("short_description", "x");
        value.put("u_cost_centre", "CC-1");
        value.put("urgencyy", "1");
        SinkRecord record = TestSupport.record(0, 1, null, value);

        RecordMapper fail =
                new RecordMapper(
                        TestSupport.config(Map.of(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "fail")),
                        metadata);
        assertThatThrownBy(() -> fail.map(record, "incident"))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("[urgencyy]")
                .hasMessageContaining("incident");

        RecordMapper drop =
                new RecordMapper(
                        TestSupport.config(Map.of(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "drop")),
                        metadata);
        RecordMapper.Mapped dropped = drop.map(record, "incident");
        assertThat(dropped.body()).containsOnlyKeys("short_description", "u_cost_centre");
        assertThat(dropped.droppedUnknownFields()).isEmpty();

        RecordMapper report =
                new RecordMapper(
                        TestSupport.config(Map.of(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "report")),
                        metadata);
        RecordMapper.Mapped reported = report.map(record, "incident");
        assertThat(reported.body()).containsOnlyKeys("short_description", "u_cost_centre");
        assertThat(reported.droppedUnknownFields()).containsExactly("urgencyy");

        RecordMapper passthrough =
                new RecordMapper(
                        TestSupport.config(
                                Map.of(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "passthrough")),
                        null);
        int before = snow.journal().count("GET");
        assertThat(passthrough.map(record, "incident").body()).containsKey("urgencyy");
        assertThat(snow.journal().count("GET")).isEqualTo(before);

        assertThatThrownBy(
                        () ->
                                new RecordMapper(
                                        TestSupport.config(
                                                Map.of(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "fail")),
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void dictionaryPermissionFailureIsAPermissionError() {
        snow = MockServiceNowServer.start();
        snow.faults().forbidTable("sys_db_object");
        TableMetadataClient metadata =
                new TableMetadataClient(snow.client().tableApi(), Duration.ofMinutes(5));
        RecordMapper m =
                new RecordMapper(
                        TestSupport.config(Map.of(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "fail")),
                        metadata);
        assertThatThrownBy(
                        () -> m.map(TestSupport.record(0, 1, null, Map.of("a", "b")), "incident"))
                .isInstanceOf(RecordError.class)
                .satisfies(
                        e ->
                                assertThat(((RecordError) e).classification())
                                        .isEqualTo(Classification.PERMISSION))
                .hasMessageContaining("passthrough");
    }
}
