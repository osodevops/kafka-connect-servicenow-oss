package sh.oso.servicenow.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.table.FieldValue;
import sh.oso.servicenow.table.Record;

class RecordSchemaMapperTest {

    private static Record record(Map<String, FieldValue> fields) {
        return Record.of(fields);
    }

    private static Map<String, FieldValue> fields(Object... kv) {
        LinkedHashMap<String, FieldValue> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            Object v = kv[i + 1];
            m.put(
                    (String) kv[i],
                    v == null
                            ? FieldValue.NULL
                            : v instanceof FieldValue f ? f : FieldValue.of((String) v));
        }
        return m;
    }

    @Test
    void schemalessPreservesNullVersusAbsentAndRichShape() {
        RecordSchemaMapper mapper = new RecordSchemaMapper(SchemaMode.SCHEMALESS, "incident", null);
        SchemaAndValue sv =
                mapper.map(
                        record(
                                fields(
                                        "a",
                                        "1",
                                        "b",
                                        null,
                                        "ref",
                                        new FieldValue("x", "X", "http://l"))));
        assertThat(sv.schema()).isNull();
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) sv.value();
        assertThat(value)
                .containsEntry("a", "1")
                .containsEntry("b", null)
                .doesNotContainKey("missing");
        assertThat(value.keySet()).containsExactly("a", "b", "ref");
        assertThat(value.get("ref"))
                .isEqualTo(Map.of("value", "x", "display_value", "X", "link", "http://l"));
        assertThat(mapper.currentSchema()).isNull();
    }

    @Test
    void stringsSchemaOnlyGrowsAndKeepsEveryFieldOptional() {
        RecordSchemaMapper mapper = new RecordSchemaMapper(SchemaMode.STRINGS, "incident", null);
        SchemaAndValue first = mapper.map(record(fields("a", "1", "b", null)));
        assertThat(first.schema().name()).isEqualTo("incident");
        assertThat(first.schema().version()).isEqualTo(1);
        assertThat(first.schema().fields())
                .extracting(org.apache.kafka.connect.data.Field::name)
                .containsExactly("a", "b");
        assertThat(((Struct) first.value()).get("b")).isNull();
        SchemaAndValue second = mapper.map(record(fields("c", new FieldValue("v", "V", null))));
        assertThat(second.schema().version()).isEqualTo(2);
        assertThat(second.schema().fields())
                .extracting(org.apache.kafka.connect.data.Field::name)
                .containsExactly("a", "b", "c", "c_display_value");
        assertThat(second.schema().field("a").schema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        assertThat(((Struct) second.value()).get("a")).isNull();
        assertThat(((Struct) second.value()).get("c_display_value")).isEqualTo("V");
        SchemaAndValue third = mapper.map(record(fields("a", "2")));
        assertThat(third.schema()).isSameAs(second.schema());
    }

    @Test
    void typedCoercesMappedFieldsAndFailsOnUnknownOnes() {
        TypedFieldMapping typed =
                TypedFieldMapping.parse(
                        List.of(
                                "sys_id:string",
                                "priority:int32",
                                "active:boolean",
                                "sys_updated_on:timestamp",
                                "cost:decimal",
                                "due:date",
                                "ratio:float64"));
        RecordSchemaMapper mapper = new RecordSchemaMapper(SchemaMode.TYPED, "incident", typed);
        SchemaAndValue sv =
                mapper.map(
                        record(
                                fields(
                                        "sys_id",
                                        "abc",
                                        "priority",
                                        "3",
                                        "active",
                                        "true",
                                        "sys_updated_on",
                                        "2026-09-29 07:30:40",
                                        "cost",
                                        "12.345",
                                        "due",
                                        "2026-10-01",
                                        "ratio",
                                        "")));
        Struct s = (Struct) sv.value();
        assertThat(s.get("priority")).isEqualTo(3);
        assertThat(s.get("active")).isEqualTo(true);
        assertThat(s.get("sys_updated_on"))
                .isEqualTo(Date.from(java.time.Instant.parse("2026-09-29T07:30:40Z")));
        assertThat(s.get("cost")).isEqualTo(new BigDecimal("12.35"));
        assertThat(s.get("due"))
                .isEqualTo(Date.from(java.time.Instant.parse("2026-10-01T00:00:00Z")));
        assertThat(s.get("ratio")).isNull();
        assertThat(sv.schema().version()).isEqualTo(1);
        assertThatThrownBy(() -> mapper.map(record(fields("sys_id", "abc", "extra", "x"))))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("extra")
                .hasMessageContaining("FAIL");
        assertThatThrownBy(() -> mapper.map(record(fields("priority", "high"))))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("priority");
    }

    @Test
    void typedBackwardAddsUnknownFieldsAsOptionalStringsButStillRejectsBadValues() {
        TypedFieldMapping typed =
                TypedFieldMapping.parse(List.of("priority:int32"), SchemaEvolution.BACKWARD);
        RecordSchemaMapper mapper = new RecordSchemaMapper(SchemaMode.TYPED, null, typed);
        SchemaAndValue sv = mapper.map(record(fields("priority", "1", "extra", "x")));
        assertThat(sv.schema().version()).isEqualTo(2);
        assertThat(sv.schema().field("extra").schema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        assertThat(((Struct) sv.value()).get("extra")).isEqualTo("x");
        assertThatThrownBy(() -> mapper.map(record(fields("priority", "high"))))
                .isInstanceOf(ServiceNowException.class);
    }

    @Test
    void typedPermissiveNullsBadValues() {
        TypedFieldMapping typed =
                TypedFieldMapping.parse(List.of("priority:int32"), SchemaEvolution.PERMISSIVE);
        RecordSchemaMapper mapper = new RecordSchemaMapper(SchemaMode.TYPED, null, typed);
        Struct s = (Struct) mapper.map(record(fields("priority", "high", "extra", "x"))).value();
        assertThat(s.get("priority")).isNull();
        assertThat(s.get("extra")).isEqualTo("x");
    }

    @Test
    void typedMappingParsesTypesAndRejectsGarbage() {
        TypedFieldMapping m =
                TypedFieldMapping.parse(
                        List.of(
                                "a:int64",
                                " b : decimal(4) ",
                                "c:time",
                                "d:bytes",
                                "e:int8",
                                "f:int16",
                                "g:float32",
                                ""));
        assertThat(m.fields()).containsKeys("a", "b", "c", "d", "e", "f", "g");
        assertThat(m.fields().get("b").parameters()).containsEntry("scale", "4");
        assertThat(m.evolution()).isEqualTo(SchemaEvolution.FAIL);
        assertThat(m.withEvolution(SchemaEvolution.PERMISSIVE).evolution())
                .isEqualTo(SchemaEvolution.PERMISSIVE);
        assertThat(m.toString()).contains("a");
        assertThatThrownBy(() -> TypedFieldMapping.parse(List.of("a:unknown")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TypedFieldMapping.parse(List.of("nocolon")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TypedFieldMapping.parse(List.of("a:int32", "a:string")))
                .hasMessageContaining("twice");
        assertThatThrownBy(() -> TypedFieldMapping.parse(List.of("a:int32(2)")))
                .hasMessageContaining("scale");
        assertThatThrownBy(() -> new RecordSchemaMapper(SchemaMode.TYPED, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(SchemaMode.fromConfig("Strings")).isEqualTo(SchemaMode.STRINGS);
        assertThat(SchemaMode.fromConfig(null)).isEqualTo(SchemaMode.SCHEMALESS);
        assertThatThrownBy(() -> SchemaMode.fromConfig("avro"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(SchemaEvolution.fromConfig("backward")).isEqualTo(SchemaEvolution.BACKWARD);
        assertThat(SchemaEvolution.fromConfig(null)).isEqualTo(SchemaEvolution.FAIL);
        assertThatThrownBy(() -> SchemaEvolution.fromConfig("forward"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
