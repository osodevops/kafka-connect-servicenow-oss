package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;
import sh.oso.servicenow.testing.RequestJournal;

class SchemaModesSourceTest {

    static final String FIELDS = "sys_id,sys_updated_on,priority,short_description";

    MockServiceNowServer snow;
    Instant now;
    Instant ts;
    TaskHarness harness;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
        now = snow.clock().instant();
        ts = now.minusSeconds(60);
        snow.tables().insert("incident", Fixtures.row(0, ts));
        snow.tables().insert("incident", Fixtures.row(1, ts.plusSeconds(1)));
        harness = new TaskHarness(snow);
    }

    @AfterEach
    void tearDown() {
        harness.close();
        snow.close();
    }

    @Test
    void stringsModeEmitsAStructWhoseSchemaOnlyGrows() throws Exception {
        harness.with(SourceConfig.SCHEMA_MODE, "strings");
        harness.start();
        List<SourceRecord> records = harness.pollUntil(2, 5_000);
        Struct v = (Struct) records.get(0).value();
        assertThat(v.schema().name()).isEqualTo("sh.oso.servicenow.incident");
        assertThat(v.schema().version()).isEqualTo(1);
        assertThat(v.getString("short_description")).isEqualTo("row 0");
        assertThat(v.getString("sys_updated_on")).isEqualTo(SnowTimestamp.format(ts));
        assertThat(v.schema().fields())
                .allSatisfy(f -> assertThat(f.schema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA));
        assertThat(records.get(0).headers().lastWithName("snow.schema.mode").value())
                .isEqualTo("strings");
        assertThat(harness.task().metrics().get("incident").schemaVersion()).isEqualTo(1);

        Map<String, String> extra = Fixtures.row(5, now.minusSeconds(5));
        extra.put("u_extra", "x");
        snow.tables().insert("incident", extra);
        Struct grown = (Struct) harness.pollUntil(1, 5_000).get(0).value();
        assertThat(grown.schema().version()).isEqualTo(2);
        assertThat(grown.getString("u_extra")).isEqualTo("x");
        assertThat(grown.schema().field("short_description")).isNotNull();
        assertThat(harness.task().metrics().get("incident").schemaVersion()).isEqualTo(2);
    }

    @Test
    void typedModeCoercesTheMappedFields() throws Exception {
        harness.with(SourceConfig.SCHEMA_MODE, "typed")
                .with(TableSpec.key("t1", TableSpec.FIELDS), FIELDS)
                .with(
                        SourceConfig.TYPED_FIELDS,
                        "sys_id:string,sys_updated_on:timestamp,sys_mod_count:int32,priority:int32,"
                                + "short_description:string");
        harness.start();
        Struct v = (Struct) harness.pollUntil(2, 5_000).get(0).value();
        assertThat(v.getInt32("priority")).isEqualTo(1);
        assertThat(v.getInt32("sys_mod_count")).isEqualTo(0);
        assertThat(v.get("sys_updated_on")).isInstanceOf(Date.class);
        assertThat(((Date) v.get("sys_updated_on")).getTime()).isEqualTo(ts.toEpochMilli());
        assertThat(v.getString("sys_id")).isEqualTo(Fixtures.sysId(0));
        assertThat(v.schema().fields()).hasSize(5);
    }

    @Test
    void typedModeFailsOnUnmappedFieldsUnlessEvolutionAllowsThem() throws Exception {
        String mapping =
                "sys_id:string,sys_updated_on:timestamp,sys_mod_count:int32,priority:int32";
        harness.with(SourceConfig.SCHEMA_MODE, "typed")
                .with(TableSpec.key("t1", TableSpec.FIELDS), FIELDS)
                .with(SourceConfig.TYPED_FIELDS, mapping)
                .with(SourceConfig.STARTUP_PROBE, "false");
        harness.start();
        assertThatThrownBy(() -> harness.pollFor(1_000))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("short_description")
                .hasMessageContaining("incident");
        harness.close();

        TaskHarness backward =
                new TaskHarness(snow)
                        .with(SourceConfig.SCHEMA_MODE, "typed")
                        .with(TableSpec.key("t1", TableSpec.FIELDS), FIELDS)
                        .with(SourceConfig.TYPED_FIELDS, mapping)
                        .with(SourceConfig.SCHEMA_EVOLUTION, "backward");
        backward.start();
        Struct v = (Struct) backward.pollUntil(2, 5_000).get(0).value();
        assertThat(v.getString("short_description")).isEqualTo("row 0");
        assertThat(v.schema().field("short_description").schema())
                .isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        backward.close();
    }

    @Test
    void envelopeInStringsModeIsAStruct() throws Exception {
        harness.with(SourceConfig.SCHEMA_MODE, "strings").with(SourceConfig.EMIT_ENVELOPE, "true");
        harness.start();
        SourceRecord r = harness.pollUntil(1, 5_000).get(0);
        Struct env = (Struct) r.value();
        assertThat(r.valueSchema().name()).isEqualTo("sh.oso.servicenow.incident.Envelope");
        assertThat(env.get("before")).isNull();
        assertThat(((Struct) env.get("after")).getString("short_description")).isEqualTo("row 0");
        Struct source = (Struct) env.get("source");
        assertThat(source.getString("table")).isEqualTo("incident");
        assertThat(source.getString("instance")).isEqualTo("localhost");
        assertThat(env.getString("op")).isEqualTo("u");
        assertThat(env.getInt64("ts_ms")).isEqualTo(ts.toEpochMilli());
    }

    @Test
    void displayValueAllYieldsRichFieldsAndKeepsTheCursorParseable() throws Exception {
        harness.with(TableSpec.key("t1", TableSpec.DISPLAY_VALUE), "all");
        harness.start();
        SourceRecord r = harness.pollUntil(1, 5_000).get(0);
        Map<String, Object> v = Fixtures.value(r);
        assertThat(Fixtures.map(v.get("short_description")))
                .containsEntry("value", "row 0")
                .containsEntry("display_value", "row 0 (display)");
        assertThat(Fixtures.map(v.get("sys_updated_on")))
                .containsEntry("value", SnowTimestamp.format(ts));
        assertThat(r.timestamp()).isEqualTo(ts.toEpochMilli());
    }

    @Test
    void displayValueTrueTakesDisplayValuesExceptForTheCursorFields() throws Exception {
        harness.with(TableSpec.key("t1", TableSpec.DISPLAY_VALUE), "true");
        harness.start();
        SourceRecord r = harness.pollUntil(1, 5_000).get(0);
        assertThat(Fixtures.field(r, "short_description")).isEqualTo("row 0 (display)");
        assertThat(Fixtures.field(r, "sys_updated_on")).isEqualTo(SnowTimestamp.format(ts));
        assertThat(Fixtures.field(r, "sys_id")).isEqualTo(Fixtures.sysId(0));
        assertThat(Fixtures.field(r, "sys_mod_count")).isEqualTo("0");
        assertThat(snow.journal().entries())
                .filteredOn(
                        e -> "incident".equals(e.table()) && e.path().contains("sysparm_query="))
                .allSatisfy(e -> assertThat(e.path()).contains("sysparm_display_value=all"));
    }

    @Test
    void projectionSendsTheRequestedFieldsPlusSysModCount() throws Exception {
        harness.with(
                TableSpec.key("t1", TableSpec.FIELDS), "sys_id,sys_updated_on,short_description");
        harness.start();
        SourceRecord r = harness.pollUntil(1, 5_000).get(0);
        assertThat(Fixtures.value(r).keySet())
                .containsExactlyInAnyOrder(
                        "sys_id", "sys_updated_on", "short_description", "sys_mod_count");
        List<RequestJournal.Entry> lists =
                snow.journal().entries().stream()
                        .filter(
                                e ->
                                        "incident".equals(e.table())
                                                && e.path().contains("sysparm_query="))
                        .toList();
        assertThat(lists).isNotEmpty();
        assertThat(lists.get(0).path())
                .contains(
                        "sysparm_fields=sys_id%2Csys_updated_on%2Cshort_description%2Csys_mod_count")
                .contains("sysparm_no_count=true")
                .contains("sysparm_exclude_reference_link=true")
                .doesNotContain("sysparm_query_no_domain");
    }

    @Test
    void queryDomainFalseSendsTheNoDomainParameterAndCategoryIsForwarded() throws Exception {
        harness.with(TableSpec.key("t1", TableSpec.QUERY_DOMAIN), "false")
                .with(TableSpec.key("t1", TableSpec.QUERY_CATEGORY), "kafka");
        harness.start();
        assertThat(harness.pollUntil(2, 5_000)).hasSize(2);
        assertThat(snow.journal().entries())
                .filteredOn(e -> "incident".equals(e.table()))
                .allSatisfy(
                        e ->
                                assertThat(e.path())
                                        .contains("sysparm_query_no_domain=true")
                                        .contains("sysparm_query_category=kafka"));
    }
}
