package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.table.DisplayValue;

class TableSpecTest {

    private static TableSpec spec(String name, String query, boolean domain, List<String> fields) {
        return new TableSpec(
                "t1",
                name,
                "topic",
                Instant.EPOCH,
                "sys_updated_on",
                "sys_id",
                query,
                fields,
                DisplayValue.FALSE,
                true,
                domain,
                null,
                100,
                1000);
    }

    @Test
    void definesEveryPerAliasKeyUnderThePrefix() {
        ConfigDef def = TableSpec.defFor("inc");
        assertThat(def.names())
                .containsExactlyInAnyOrder(
                        "snow.table.inc.name",
                        "snow.table.inc.topic",
                        "snow.table.inc.start.timestamp",
                        "snow.table.inc.timestamp.field",
                        "snow.table.inc.sys.id.field",
                        "snow.table.inc.query",
                        "snow.table.inc.fields",
                        "snow.table.inc.display.value",
                        "snow.table.inc.exclude.reference.link",
                        "snow.table.inc.query.domain",
                        "snow.table.inc.query.category",
                        "snow.table.inc.batch.size",
                        "snow.table.inc.poll.interval.ms");
        Map<String, ConfigDef.ConfigKey> keys = def.configKeys();
        assertThat(keys.get("snow.table.inc.start.timestamp").defaultValue)
                .isEqualTo("1970-01-01 00:00:00");
        assertThat(keys.get("snow.table.inc.timestamp.field").defaultValue)
                .isEqualTo("sys_updated_on");
        assertThat(keys.get("snow.table.inc.sys.id.field").defaultValue).isEqualTo("sys_id");
        assertThat(keys.get("snow.table.inc.display.value").defaultValue).isEqualTo("false");
        assertThat(keys.get("snow.table.inc.exclude.reference.link").defaultValue).isEqualTo(true);
        assertThat(keys.get("snow.table.inc.query.domain").defaultValue).isEqualTo(true);
        assertThat(keys.get("snow.table.inc.batch.size").defaultValue).isNull();
        assertThat(keys.get("snow.table.inc.name").hasDefault()).isFalse();
        assertThat(keys.get("snow.table.inc.topic").hasDefault()).isFalse();
        assertThat(keys.values()).allSatisfy(k -> assertThat(k.group).isEqualTo("Table inc"));
    }

    @Test
    void fingerprintFollowsTableQueryAndDomainOnly() {
        TableSpec a = spec("incident", "active=true", true, List.of());
        assertThat(a.fingerprint()).startsWith("sha256:");
        assertThat(
                        spec("incident", "active=true", true, List.of("sys_id", "sys_updated_on"))
                                .fingerprint())
                .isEqualTo(a.fingerprint());
        assertThat(spec("incident", "  active=true ", true, List.of()).fingerprint())
                .isEqualTo(a.fingerprint());
        assertThat(spec("incident", "active=false", true, List.of()).fingerprint())
                .isNotEqualTo(a.fingerprint());
        assertThat(spec("incident", "active=true", false, List.of()).fingerprint())
                .isNotEqualTo(a.fingerprint());
        assertThat(spec("problem", "active=true", true, List.of()).fingerprint())
                .isNotEqualTo(a.fingerprint());
    }

    @Test
    void normalisesOptionalMembers() {
        TableSpec s = spec("incident", null, true, null);
        assertThat(s.query()).isEmpty();
        assertThat(s.fields()).isEmpty();
        assertThat(s.queryCategory()).isNull();
        assertThat(s.key(TableSpec.NAME)).isEqualTo("snow.table.t1.name");
        assertThat(TableSpec.key("x", TableSpec.TOPIC)).isEqualTo("snow.table.x.topic");
        assertThat(TableSpec.group("x")).isEqualTo("Table x");
        assertThat(s.toString()).contains("incident").contains("topic");
    }
}
