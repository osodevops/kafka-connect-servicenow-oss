package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.config.CoreConfigDefs;

class ServiceNowSourceConnectorTest {

    private static Map<String, String> props(String... aliases) {
        Map<String, String> p = new HashMap<>();
        p.put(CoreConfigDefs.URL, "https://acme.service-now.com");
        p.put(CoreConfigDefs.AUTH_USERNAME, "connect");
        p.put(CoreConfigDefs.AUTH_PASSWORD, "secret");
        p.put(SourceConfig.TABLES, String.join(",", aliases));
        for (String a : aliases) {
            p.put(TableSpec.key(a, TableSpec.NAME), a);
            p.put(TableSpec.key(a, TableSpec.TOPIC), "snow." + a);
        }
        return p;
    }

    private static Map<String, ConfigValue> byName(Config config) {
        Map<String, ConfigValue> out = new HashMap<>();
        for (ConfigValue v : config.configValues()) {
            out.put(v.name(), v);
        }
        return out;
    }

    @Test
    void taskConfigsCapAtTheTableCountAndCoverEveryTableOnce() {
        ServiceNowSourceConnector connector = new ServiceNowSourceConnector();
        connector.start(props("incident", "problem", "change_request"));

        List<Map<String, String>> configs = connector.taskConfigs(9);
        assertThat(configs.size()).isBetween(1, 3);
        List<String> assigned = new ArrayList<>();
        for (Map<String, String> c : configs) {
            assertThat(c).containsEntry(CoreConfigDefs.URL, "https://acme.service-now.com");
            assigned.addAll(List.of(c.get(SourceConfig.TASK_TABLES).split(",")));
        }
        assertThat(assigned).containsExactlyInAnyOrder("incident", "problem", "change_request");

        List<Map<String, String>> single = connector.taskConfigs(1);
        assertThat(single).hasSize(1);
        assertThat(single.get(0).get(SourceConfig.TASK_TABLES))
                .isEqualTo("incident,problem,change_request");
        assertThat(connector.taskConfigs(2)).isEqualTo(connector.taskConfigs(2));
        connector.stop();
    }

    @Test
    void validateReportsPerAliasErrorsAgainstTheirOwnKeys() {
        ServiceNowSourceConnector connector = new ServiceNowSourceConnector();

        Map<String, String> missingTopic = props("incident");
        missingTopic.remove("snow.table.incident.topic");
        Map<String, ConfigValue> values = byName(connector.validate(missingTopic));
        assertThat(values).containsKey("snow.table.incident.topic");
        assertThat(values.get("snow.table.incident.topic").errorMessages()).isNotEmpty();
        assertThat(values.get("snow.table.incident.name").errorMessages()).isEmpty();

        Map<String, String> badFields = props("incident");
        badFields.put("snow.table.incident.fields", "number");
        values = byName(connector.validate(badFields));
        assertThat(values.get("snow.table.incident.fields").errorMessages())
                .anySatisfy(m -> assertThat(m).contains("sys_updated_on"));

        Map<String, String> badAlias = props("incident");
        badAlias.put(SourceConfig.TABLES, "Incident");
        values = byName(connector.validate(badAlias));
        assertThat(values.get(SourceConfig.TABLES).errorMessages()).isNotEmpty();
        assertThat(values).doesNotContainKey("snow.table.Incident.name");

        Map<String, String> badUrl = props("incident");
        badUrl.put(CoreConfigDefs.URL, "ftp://nope");
        values = byName(connector.validate(badUrl));
        assertThat(values.get(CoreConfigDefs.URL).errorMessages()).isNotEmpty();
    }

    @Test
    void validatePassesACompleteConfiguration() {
        Config config = new ServiceNowSourceConnector().validate(props("incident", "problem"));
        assertThat(config.configValues())
                .allSatisfy(v -> assertThat(v.errorMessages()).as(v.name()).isEmpty());
        assertThat(byName(config))
                .containsKeys("snow.table.incident.query", "snow.table.problem.topic");
    }

    @Test
    void staticDefinitionHasNoPerAliasKeys() {
        ServiceNowSourceConnector connector = new ServiceNowSourceConnector();
        assertThat(connector.config().names())
                .contains(SourceConfig.TABLES, CoreConfigDefs.URL, SourceConfig.SCHEMA_MODE)
                .noneMatch(n -> n.startsWith(TableSpec.PREFIX));
        assertThat(connector.taskClass()).isEqualTo(ServiceNowSourceTask.class);
        assertThat(connector.version()).isNotBlank();
        assertThat(new ServiceNowSourceTask().version()).isEqualTo(connector.version());
    }

    @Test
    void exactlyOnceIsUnsupportedAndStartFailsFast() {
        ServiceNowSourceConnector connector = new ServiceNowSourceConnector();
        assertThat(connector.exactlyOnceSupport(props("incident")))
                .isEqualTo(ExactlyOnceSupport.UNSUPPORTED);
        Map<String, String> bad = props("incident");
        bad.put("snow.table.incident.query", "ORDERBYnumber");
        assertThatThrownBy(() -> connector.start(bad)).isInstanceOf(ConfigException.class);
    }
}
