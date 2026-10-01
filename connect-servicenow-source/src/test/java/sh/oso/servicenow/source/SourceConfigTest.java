package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.config.CoreConfigDefs;
import sh.oso.servicenow.schema.SchemaEvolution;
import sh.oso.servicenow.schema.SchemaMode;
import sh.oso.servicenow.table.DisplayValue;

class SourceConfigTest {

    static Map<String, String> base() {
        Map<String, String> p = new HashMap<>();
        p.put(CoreConfigDefs.URL, "https://acme.service-now.com");
        p.put(CoreConfigDefs.AUTH_USERNAME, "connect");
        p.put(CoreConfigDefs.AUTH_PASSWORD, "secret");
        p.put(SourceConfig.TABLES, "inc");
        p.put("snow.table.inc.name", "incident");
        p.put("snow.table.inc.topic", "snow.incident");
        return p;
    }

    @Test
    void appliesDocumentedDefaults() {
        SourceConfig c = new SourceConfig(base());
        assertThat(c.getInt(SourceConfig.POLL_INTERVAL_MS)).isEqualTo(5000);
        assertThat(c.getInt(SourceConfig.BATCH_SIZE)).isEqualTo(5000);
        assertThat(c.overlap()).isEqualTo(Duration.ofSeconds(2));
        assertThat(c.safetyLag()).isEqualTo(Duration.ofSeconds(1));
        assertThat(c.dedupWindowRecords()).isEqualTo(50_000);
        assertThat(c.emitEnvelope()).isFalse();
        assertThat(c.schemaMode()).isEqualTo(SchemaMode.SCHEMALESS);
        assertThat(c.schemaEvolution()).isEqualTo(SchemaEvolution.FAIL);
        assertThat(c.typedFields()).isNull();
        assertThat(c.badRowBehavior()).isEqualTo(SourceConfig.BadRowBehavior.FAIL);
        assertThat(c.startupProbe()).isTrue();
        assertThat(c.core().instanceHost()).isEqualTo("acme.service-now.com");

        TableSpec t = c.tables().get(0);
        assertThat(t.alias()).isEqualTo("inc");
        assertThat(t.name()).isEqualTo("incident");
        assertThat(t.topic()).isEqualTo("snow.incident");
        assertThat(t.startTimestamp()).isEqualTo(Instant.EPOCH);
        assertThat(t.timestampField()).isEqualTo("sys_updated_on");
        assertThat(t.sysIdField()).isEqualTo("sys_id");
        assertThat(t.query()).isEmpty();
        assertThat(t.fields()).isEmpty();
        assertThat(t.displayValue()).isEqualTo(DisplayValue.FALSE);
        assertThat(t.excludeReferenceLink()).isTrue();
        assertThat(t.queryDomain()).isTrue();
        assertThat(t.queryCategory()).isNull();
        assertThat(t.batchSize()).isEqualTo(5000);
        assertThat(t.pollIntervalMs()).isEqualTo(5000L);
    }

    @Test
    void perAliasOverridesWinOverGlobals() {
        Map<String, String> p = base();
        p.put(SourceConfig.BATCH_SIZE, "100");
        p.put(SourceConfig.POLL_INTERVAL_MS, "1000");
        p.put("snow.table.inc.batch.size", "25");
        p.put("snow.table.inc.poll.interval.ms", "250");
        p.put("snow.table.inc.start.timestamp", "2026-01-02 03:04:05");
        p.put("snow.table.inc.query", " active=true ");
        p.put("snow.table.inc.fields", "sys_id, sys_updated_on, number");
        p.put("snow.table.inc.display.value", "all");
        p.put("snow.table.inc.exclude.reference.link", "false");
        p.put("snow.table.inc.query.domain", "false");
        p.put("snow.table.inc.query.category", "kafka");
        TableSpec t = new SourceConfig(p).tables().get(0);
        assertThat(t.batchSize()).isEqualTo(25);
        assertThat(t.pollIntervalMs()).isEqualTo(250L);
        assertThat(t.startTimestamp()).isEqualTo(Instant.parse("2026-01-02T03:04:05Z"));
        assertThat(t.query()).isEqualTo("active=true");
        assertThat(t.fields()).containsExactly("sys_id", "sys_updated_on", "number");
        assertThat(t.displayValue()).isEqualTo(DisplayValue.ALL);
        assertThat(t.excludeReferenceLink()).isFalse();
        assertThat(t.queryDomain()).isFalse();
        assertThat(t.queryCategory()).isEqualTo("kafka");
    }

    @Test
    void aliasesMustBeLowerCaseIdentifiersAndUnique() {
        Map<String, String> p = base();
        p.put(SourceConfig.TABLES, "Inc");
        assertThatThrownBy(() -> new SourceConfig(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Inc");
        p.put(SourceConfig.TABLES, "inc,inc");
        assertThatThrownBy(() -> new SourceConfig(p)).hasMessageContaining("twice");
        p.put(SourceConfig.TABLES, " , ");
        assertThatThrownBy(() -> new SourceConfig(p)).hasMessageContaining("at least one");
    }

    @Test
    void nameAndTopicAreRequiredPerAlias() {
        Map<String, String> p = base();
        p.remove("snow.table.inc.topic");
        assertThatThrownBy(() -> new SourceConfig(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("snow.table.inc.topic");
        Map<String, String> q = base();
        q.put("snow.table.inc.name", "Incident");
        assertThatThrownBy(() -> new SourceConfig(q)).hasMessageContaining("snow.table.inc.name");
    }

    @Test
    void projectionMustIncludeTheCursorFields() {
        Map<String, String> p = base();
        p.put("snow.table.inc.fields", "number,short_description");
        assertThatThrownBy(() -> new SourceConfig(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("snow.table.inc.fields")
                .hasMessageContaining("sys_id")
                .hasMessageContaining("sys_updated_on");
        p.put("snow.table.inc.fields", "number,sys_id,sys_updated_on");
        assertThat(new SourceConfig(p).tables().get(0).fields()).hasSize(3);
    }

    @Test
    void baseQueryMustNotOrder() {
        Map<String, String> p = base();
        p.put("snow.table.inc.query", "active=true^ORDERBYnumber");
        assertThatThrownBy(() -> new SourceConfig(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("snow.table.inc.query")
                .hasMessageContaining("ORDERBY");
    }

    @Test
    void typedModeNeedsAMapping() {
        Map<String, String> p = base();
        p.put(SourceConfig.SCHEMA_MODE, "typed");
        assertThatThrownBy(() -> new SourceConfig(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(SourceConfig.TYPED_FIELDS);
        p.put(SourceConfig.TYPED_FIELDS, "sys_id:string,priority:int32");
        SourceConfig c = new SourceConfig(p);
        assertThat(c.typedFields().fields()).containsKeys("sys_id", "priority");
        p.put(SourceConfig.TYPED_FIELDS, "sys_id=string");
        assertThatThrownBy(() -> new SourceConfig(p)).hasMessageContaining("field:type");
    }

    @Test
    void rejectsBadTimestampsFieldsAndEnums() {
        Map<String, String> p = base();
        p.put("snow.table.inc.start.timestamp", "2026-01-02T03:04:05Z");
        assertThatThrownBy(() -> new SourceConfig(p))
                .hasMessageContaining("snow.table.inc.start.timestamp");
        Map<String, String> q = base();
        q.put("snow.table.inc.timestamp.field", "caller_id.name");
        assertThatThrownBy(() -> new SourceConfig(q))
                .hasMessageContaining("snow.table.inc.timestamp.field");
        Map<String, String> r = base();
        r.put("snow.table.inc.sys.id.field", "sys_updated_on");
        assertThatThrownBy(() -> new SourceConfig(r)).hasMessageContaining("must differ");
        Map<String, String> s = base();
        s.put("snow.table.inc.display.value", "yes");
        assertThatThrownBy(() -> new SourceConfig(s)).isInstanceOf(ConfigException.class);
        Map<String, String> t = base();
        t.put(SourceConfig.BAD_ROW_BEHAVIOR, "ignore");
        assertThatThrownBy(() -> new SourceConfig(t)).isInstanceOf(ConfigException.class);
    }

    @Test
    void coreKeysAreValidatedToo() {
        Map<String, String> p = base();
        p.remove(CoreConfigDefs.URL);
        assertThatThrownBy(() -> new SourceConfig(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("snow.url");
    }

    @Test
    void taskTablesSelectsTheAssignedSubset() {
        Map<String, String> p = base();
        p.put(SourceConfig.TABLES, "inc,prb");
        p.put("snow.table.prb.name", "problem");
        p.put("snow.table.prb.topic", "snow.problem");
        SourceConfig all = new SourceConfig(p);
        assertThat(all.aliases()).containsExactly("inc", "prb");
        assertThat(all.taskTables()).hasSize(2);
        p.put(SourceConfig.TASK_TABLES, "prb");
        assertThat(new SourceConfig(p).taskTables())
                .extracting(TableSpec::name)
                .containsExactly("problem");
        p.put(SourceConfig.TASK_TABLES, "nope");
        assertThatThrownBy(() -> new SourceConfig(p).taskTables()).hasMessageContaining("nope");
    }

    @Test
    void dynamicDefinitionCarriesPerAliasKeysOnly() {
        Map<String, String> p = base();
        p.put(SourceConfig.TABLES, "inc, prb ,inc,BAD");
        assertThat(SourceConfig.rawAliases(p)).containsExactly("inc", "prb", "BAD");
        assertThat(SourceConfig.configDef().names()).doesNotContain("snow.table.inc.name");
        assertThat(SourceConfig.configDef(p).names())
                .contains("snow.table.inc.name", "snow.table.prb.topic", "snow.url")
                .doesNotContain("snow.table.BAD.name");
        assertThat(SourceConfig.rawAliases(Map.of())).isEmpty();
        assertThat(SourceConfig.rawAliases(Map.of(SourceConfig.TABLES, List.of("a", "b"))))
                .containsExactly("a", "b");
    }

    @Test
    void internalTaskTablesKeyIsDocumentedAsInternal() {
        assertThat(
                        SourceConfig.configDef()
                                .configKeys()
                                .get(SourceConfig.TASK_TABLES)
                                .documentation)
                .startsWith("Internal:");
    }
}
