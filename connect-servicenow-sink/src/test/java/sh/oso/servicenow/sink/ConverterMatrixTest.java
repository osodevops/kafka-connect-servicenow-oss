package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 * The sink's input paths, driven by the converters a worker can bundle.
 *
 * <p>(a) {@code JsonConverter} with {@code schemas.enable=false} delivers a schemaless {@code Map};
 * (b) a {@code Struct} with a {@code Schema}; (c) {@code JsonConverter} with {@code
 * schemas.enable=true} round-trips a {@code Struct} through its envelope back to path (b); (d) a
 * nested {@code Struct} is rejected under the default policy; (e) {@code Decimal} and {@code
 * Timestamp} logical types render as ServiceNow strings.
 *
 * <p>The Avro, JSON Schema and Protobuf converters are published by Confluent under the Confluent
 * Community License and are not bundled in the plugin ZIP. Every one of them hands the sink a
 * {@code Struct} with a {@code Schema}, exactly what path (b) exercises, so they need no special
 * handling here. Apicurio's Apache-2.0 converters behave the same way.
 */
class ConverterMatrixTest {

    private static final Schema INCIDENT =
            SchemaBuilder.struct()
                    .name("incident")
                    .field("short_description", Schema.STRING_SCHEMA)
                    .field("urgency", Schema.INT32_SCHEMA)
                    .field("active", Schema.BOOLEAN_SCHEMA)
                    .field("opened_at", Timestamp.SCHEMA)
                    .field("cost", Decimal.schema(2))
                    .field("comments", Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

    private static Struct incident() {
        return new Struct(INCIDENT)
                .put("short_description", "Printer on fire")
                .put("urgency", 1)
                .put("active", true)
                .put("opened_at", new Date(1790667042000L))
                .put("cost", new BigDecimal("12.50"));
    }

    private static final RecordMapper MAPPER = new RecordMapper(TestSupport.config(Map.of()), null);

    private static Map<String, Object> map(SchemaAndValue sv) {
        SinkRecord r = new SinkRecord(TestSupport.TOPIC, 0, null, null, sv.schema(), sv.value(), 1);
        return MAPPER.map(r, "incident").body();
    }

    private static JsonConverter converter(boolean schemas) {
        JsonConverter c = new JsonConverter();
        c.configure(Map.of("schemas.enable", Boolean.toString(schemas)), false);
        return c;
    }

    @Test
    void schemalessJsonMapPath() {
        byte[] json =
                "{\"short_description\":\"Printer on fire\",\"urgency\":1,\"active\":true,\"cost\":12.5,\"comments\":null}"
                        .getBytes(StandardCharsets.UTF_8);
        SchemaAndValue sv = converter(false).toConnectData(TestSupport.TOPIC, json);
        assertThat(sv.schema()).isNull();
        assertThat(sv.value()).isInstanceOf(Map.class);
        assertThat(map(sv))
                .containsOnly(
                        entry("short_description", "Printer on fire"),
                        entry("urgency", "1"),
                        entry("active", "true"),
                        entry("cost", "12.5"));
    }

    @Test
    void structWithSchemaPath() {
        assertThat(map(new SchemaAndValue(INCIDENT, incident())))
                .containsExactly(
                        entry("short_description", "Printer on fire"),
                        entry("urgency", "1"),
                        entry("active", "true"),
                        entry("opened_at", "2026-09-29 07:30:42"),
                        entry("cost", "12.50"));
    }

    @Test
    void jsonConverterWithSchemasRoundTripsToTheStructPath() {
        JsonConverter c = converter(true);
        byte[] envelope = c.fromConnectData(TestSupport.TOPIC, INCIDENT, incident());
        assertThat(new String(envelope, StandardCharsets.UTF_8))
                .contains("\"schema\"", "\"payload\"");
        SchemaAndValue back = c.toConnectData(TestSupport.TOPIC, envelope);
        assertThat(back.value()).isInstanceOf(Struct.class);
        assertThat(map(back)).isEqualTo(map(new SchemaAndValue(INCIDENT, incident())));
    }

    @Test
    void nestedStructIsRejectedByDefault() {
        Schema caller = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        Schema schema =
                SchemaBuilder.struct()
                        .field("short_description", Schema.STRING_SCHEMA)
                        .field("caller", caller)
                        .build();
        Struct s =
                new Struct(schema)
                        .put("short_description", "x")
                        .put("caller", new Struct(caller).put("name", "Abel"));
        JsonConverter c = converter(true);
        SchemaAndValue back =
                c.toConnectData(TestSupport.TOPIC, c.fromConnectData(TestSupport.TOPIC, schema, s));
        assertThatThrownBy(() -> map(back))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("'caller' is a Struct");
    }

    @Test
    void logicalTypesSurviveTheJsonEnvelope() {
        JsonConverter c = converter(true);
        Schema schema =
                SchemaBuilder.struct()
                        .field("opened_at", Timestamp.SCHEMA)
                        .field("cost", Decimal.schema(3))
                        .build();
        Struct s =
                new Struct(schema)
                        .put("opened_at", new Date(0L))
                        .put("cost", new BigDecimal("1234.567"));
        SchemaAndValue back =
                c.toConnectData(TestSupport.TOPIC, c.fromConnectData(TestSupport.TOPIC, schema, s));
        assertThat(map(back))
                .containsExactly(
                        entry("opened_at", "1970-01-01 00:00:00"), entry("cost", "1234.567"));
    }
}
