package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.cursor.Cursor;
import sh.oso.servicenow.cursor.SourceOffset;
import sh.oso.servicenow.cursor.SourcePartition;
import sh.oso.servicenow.schema.RecordSchemaMapper;
import sh.oso.servicenow.schema.SchemaMode;
import sh.oso.servicenow.table.DisplayValue;
import sh.oso.servicenow.table.Record;

class SourceRecordFactoryTest {

    static final Instant TS = Instant.parse("2026-09-29T07:30:42Z");
    static final Instant EXTRACTED = Instant.parse("2026-09-29T07:31:00Z");
    static final TableSpec SPEC =
            new TableSpec(
                    "inc",
                    "incident",
                    "snow.incident",
                    Instant.EPOCH,
                    "sys_updated_on",
                    "sys_id",
                    "",
                    List.of(),
                    DisplayValue.FALSE,
                    true,
                    true,
                    null,
                    100,
                    1000);

    private static Record row() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("sys_id", Fixtures.sysId(1));
        m.put("sys_updated_on", "2026-09-29 07:30:42");
        m.put("sys_mod_count", "3");
        m.put("short_description", "hello");
        return Record.ofStrings(m);
    }

    private static SourceRecordFactory factory(SchemaMode mode, boolean envelope) {
        return new SourceRecordFactory(
                SPEC,
                "acme.service-now.com",
                SourcePartition.of(
                        "acme.service-now.com", "incident", SPEC.fingerprint(), "sys_updated_on"),
                new RecordSchemaMapper(mode, "sh.oso.servicenow.incident", null),
                envelope,
                Clock.fixed(EXTRACTED, ZoneOffset.UTC));
    }

    private static SourceRecord create(SourceRecordFactory f) {
        Cursor cursor = new Cursor(TS, Fixtures.sysId(1));
        return f.create(row(), cursor, SourceOffset.of(cursor, "backfill", SPEC.fingerprint()));
    }

    @Test
    void keyTopicTimestampHeadersPartitionAndOffset() {
        SourceRecord r = create(factory(SchemaMode.SCHEMALESS, false));
        assertThat(r.key()).isEqualTo(Fixtures.sysId(1));
        assertThat(r.keySchema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(r.topic()).isEqualTo("snow.incident");
        assertThat(r.kafkaPartition()).isNull();
        assertThat(r.timestamp()).isEqualTo(TS.toEpochMilli());
        assertThat(r.valueSchema()).isNull();
        assertThat(Fixtures.value(r)).containsEntry("short_description", "hello");
        assertThat(r.headers().lastWithName("snow.table").value()).isEqualTo("incident");
        assertThat(r.headers().lastWithName("snow.instance").value())
                .isEqualTo("acme.service-now.com");
        assertThat(r.headers().lastWithName("snow.schema.mode").value()).isEqualTo("schemaless");
        assertThat(r.headers().lastWithName("snow.extracted_at").value())
                .isEqualTo(EXTRACTED.toEpochMilli());
        assertThat(Fixtures.partition(r))
                .containsEntry("instance", "acme.service-now.com")
                .containsEntry("table", "incident")
                .containsEntry("timestamp_field", "sys_updated_on")
                .containsEntry("query_fingerprint", SPEC.fingerprint());
        assertThat(Fixtures.offset(r))
                .containsEntry("version", "1")
                .containsEntry("timestamp", "2026-09-29 07:30:42")
                .containsEntry("sys_id", Fixtures.sysId(1))
                .containsEntry("phase", "backfill")
                .containsEntry("fingerprint", SPEC.fingerprint());
    }

    @Test
    void operationHeaderIsAlwaysUpsert() {
        for (SchemaMode mode : List.of(SchemaMode.SCHEMALESS, SchemaMode.STRINGS)) {
            for (boolean envelope : List.of(false, true)) {
                SourceRecord r = create(factory(mode, envelope));
                assertThat(r.headers().lastWithName("snow.source.operation").value())
                        .isEqualTo("UPSERT");
                assertThat(r.headers().lastWithName("snow.operation")).isNull();
                assertThat(r.value()).isNotNull();
            }
        }
    }

    @Test
    void schemalessEnvelopeIsAMapWithANullBefore() {
        SourceRecord r = create(factory(SchemaMode.SCHEMALESS, true));
        Map<String, Object> env = Fixtures.value(r);
        assertThat(env).containsKeys("before", "after", "source", "op", "ts_ms");
        assertThat(env.get("before")).isNull();
        assertThat(env.get("op")).isEqualTo("u");
        assertThat(env.get("ts_ms")).isEqualTo(TS.toEpochMilli());
        assertThat(Fixtures.map(env.get("after"))).containsEntry("short_description", "hello");
        assertThat(Fixtures.map(env.get("source")))
                .containsEntry("table", "incident")
                .containsEntry("instance", "acme.service-now.com");
    }

    @Test
    void structEnvelopeReusesTheSchemaWhileTheAfterSchemaIsStable() {
        SourceRecordFactory f = factory(SchemaMode.STRINGS, true);
        SourceRecord a = create(f);
        SourceRecord b = create(f);
        assertThat(a.valueSchema()).isSameAs(b.valueSchema());
        assertThat(a.valueSchema().name()).isEqualTo("sh.oso.servicenow.incident.Envelope");
        Struct env = (Struct) a.value();
        assertThat(env.get("before")).isNull();
        assertThat(((Struct) env.get("after")).getString("short_description")).isEqualTo("hello");
        assertThat(((Struct) env.get("source")).getString("table")).isEqualTo("incident");
        assertThat(env.getString("op")).isEqualTo("u");
        assertThat(env.getInt64("ts_ms")).isEqualTo(TS.toEpochMilli());
        assertThat(f.schemaVersion()).isEqualTo(1);
        assertThat(f.mode()).isEqualTo(SchemaMode.STRINGS);
        assertThat(factory(SchemaMode.SCHEMALESS, false).schemaVersion()).isNull();
    }
}
