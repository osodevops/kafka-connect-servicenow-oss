package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.config.CoreConfigDefs;

class SinkConfigTest {

    @Test
    void defaultsMatchThePrd() {
        SinkConfig c = TestSupport.config(Map.of());
        assertThat(c.routingMode()).isEqualTo(SinkConfig.RoutingMode.FIXED);
        assertThat(c.fixedTable()).isEqualTo("incident");
        assertThat(c.tableHeader()).isEqualTo("snow.table");
        assertThat(c.operationMode()).isEqualTo(SinkConfig.OperationMode.KEY_VALUE);
        assertThat(c.operationHeader()).isEqualTo("snow.operation");
        assertThat(c.operationHeaderRequired()).isFalse();
        assertThat(c.updateMethod()).isEqualTo(Operation.PATCH);
        assertThat(c.sysIdKeyField()).isEqualTo("sys_id");
        assertThat(c.sysIdValueField()).isEqualTo("sys_id");
        assertThat(c.sysIdPrecedence()).isEqualTo(SinkConfig.SysIdPrecedence.REJECT);
        assertThat(c.maxInFlight()).isEqualTo(8);
        assertThat(c.nullBehavior()).isEqualTo(SinkConfig.NullBehavior.OMIT);
        assertThat(c.nestedBehavior()).isEqualTo(SinkConfig.NestedBehavior.REJECT);
        assertThat(c.flattenDelimiter()).isEqualTo("_");
        assertThat(c.createAmbiguousBehavior())
                .isEqualTo(SinkConfig.CreateAmbiguousBehavior.FAIL_AMBIGUOUS);
        assertThat(c.notFoundBehavior()).isEqualTo(SinkConfig.NotFoundBehavior.FAIL);
        assertThat(c.errorBehavior()).isEqualTo(SinkConfig.ErrorBehavior.FAIL);
        assertThat(c.successTopic()).isNull();
        assertThat(c.errorTopic()).isNull();
        assertThat(c.includeRequestBody()).isFalse();
        assertThat(
                        new SinkConfig(
                                        TestSupport.offlineProps(
                                                Map.of(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "fail")))
                                .unknownFieldBehavior())
                .isEqualTo(SinkConfig.UnknownFieldBehavior.FAIL);
        assertThat(SinkConfig.configDef().names())
                .contains(CoreConfigDefs.URL, CoreConfigDefs.RETRY_MAX_ATTEMPTS);
    }

    @Test
    void fixedRoutingRequiresATable() {
        Map<String, String> p = TestSupport.offlineProps();
        p.remove(SinkConfig.TABLE);
        assertThatThrownBy(() -> new SinkConfig(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(SinkConfig.TABLE);
        assertThatThrownBy(() -> TestSupport.config(Map.of(SinkConfig.TABLE, "../sys_user")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("table name");
    }

    @Test
    void topicMapIsParsedFromDynamicKeys() {
        Map<String, String> p = TestSupport.offlineProps();
        p.remove(SinkConfig.TABLE);
        p.put(SinkConfig.ROUTING_MODE, "topic_map");
        p.put("snow.sink.topic.incidents.table", "incident");
        p.put("snow.sink.topic.acme.cmdb.servers.table", "cmdb_ci_server");
        SinkConfig c = new SinkConfig(p);
        assertThat(c.topicTables())
                .containsEntry("incidents", "incident")
                .containsEntry("acme.cmdb.servers", "cmdb_ci_server");
        assertThat(SinkConfig.topicDef("x").names()).containsExactly("snow.sink.topic.x.table");

        p.put("snow.sink.topic.bad.table", "Sys_User");
        assertThatThrownBy(() -> new SinkConfig(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("snow.sink.topic.bad.table");

        Map<String, String> none =
                TestSupport.offlineProps(Map.of(SinkConfig.ROUTING_MODE, "topic_map"));
        assertThatThrownBy(() -> new SinkConfig(none))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("at least one");
    }

    @Test
    void headerRoutingRequiresAnAllowlist() {
        assertThatThrownBy(() -> TestSupport.config(Map.of(SinkConfig.ROUTING_MODE, "header")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(SinkConfig.TABLE_ALLOWLIST);
        SinkConfig c =
                TestSupport.config(
                        Map.of(
                                SinkConfig.ROUTING_MODE, "header",
                                SinkConfig.TABLE_ALLOWLIST, "incident, problem"));
        assertThat(c.tableAllowlist()).containsExactly("incident", "problem");
        assertThatThrownBy(
                        () ->
                                TestSupport.config(
                                        Map.of(
                                                SinkConfig.ROUTING_MODE, "header",
                                                SinkConfig.TABLE_ALLOWLIST, "incident,../x")))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void fixedOperationModeRequiresTheOperation() {
        assertThatThrownBy(() -> TestSupport.config(Map.of(SinkConfig.OPERATION_MODE, "fixed")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(SinkConfig.OPERATION_FIXED);
        SinkConfig c =
                TestSupport.config(
                        Map.of(
                                SinkConfig.OPERATION_MODE,
                                "fixed",
                                SinkConfig.OPERATION_FIXED,
                                "Put"));
        assertThat(c.fixedOperation()).isEqualTo(Operation.PUT);
        assertThatThrownBy(() -> TestSupport.config(Map.of(SinkConfig.OPERATION_FIXED, "upsert")))
                .isInstanceOf(ConfigException.class);
        assertThat(TestSupport.config(Map.of(SinkConfig.UPDATE_METHOD, "put")).updateMethod())
                .isEqualTo(Operation.PUT);
    }

    @Test
    void correlationLookupRequiresTheField() {
        assertThatThrownBy(
                        () ->
                                TestSupport.config(
                                        Map.of(
                                                SinkConfig.CREATE_AMBIGUOUS_BEHAVIOR,
                                                "correlation_lookup")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(SinkConfig.CORRELATION_FIELD);
        SinkConfig c =
                TestSupport.config(
                        Map.of(
                                SinkConfig.CREATE_AMBIGUOUS_BEHAVIOR, "correlation_lookup",
                                SinkConfig.CORRELATION_FIELD, "correlation_id"));
        assertThat(c.correlationField()).isEqualTo("correlation_id");
    }

    @Test
    void reporterTopicsRequireBootstrapServers() {
        assertThatThrownBy(
                        () -> TestSupport.config(Map.of(SinkConfig.REPORTER_SUCCESS_TOPIC, "ok")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(SinkConfig.REPORTER_BOOTSTRAP_SERVERS);
        SinkConfig c =
                TestSupport.config(
                        Map.of(
                                SinkConfig.REPORTER_ERROR_TOPIC,
                                "errors",
                                SinkConfig.REPORTER_BOOTSTRAP_SERVERS,
                                "kafka:9092",
                                "snow.sink.reporter.producer.security.protocol",
                                "SASL_SSL"));
        assertThat(c.reporterProducerProps()).containsEntry("security.protocol", "SASL_SSL");
        assertThat(c.errorTopic()).isEqualTo("errors");
    }

    @Test
    void renamesAndListsAreValidated() {
        SinkConfig c =
                TestSupport.config(
                        Map.of(
                                SinkConfig.FIELD_RENAME, "desc:short_description, prio : priority",
                                SinkConfig.FIELD_DENYLIST, "internal"));
        assertThat(c.fieldRenames())
                .containsEntry("desc", "short_description")
                .containsEntry("prio", "priority");
        assertThat(c.fieldDenylist()).containsExactly("internal");
        assertThatThrownBy(() -> TestSupport.config(Map.of(SinkConfig.FIELD_RENAME, "nocolon")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("from:to");
        assertThatThrownBy(
                        () ->
                                TestSupport.config(
                                        Map.of(
                                                SinkConfig.FIELD_ALLOWLIST, "a",
                                                SinkConfig.FIELD_DENYLIST, "b")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("cannot be combined");
    }

    @Test
    void inFlightCannotExceedTheInstanceBound() {
        assertThatThrownBy(() -> TestSupport.config(Map.of(SinkConfig.MAX_IN_FLIGHT, "16")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CoreConfigDefs.HTTP_MAX_CONCURRENT_REQUESTS);
        SinkConfig c =
                TestSupport.config(
                        Map.of(
                                SinkConfig.MAX_IN_FLIGHT, "16",
                                CoreConfigDefs.HTTP_MAX_CONCURRENT_REQUESTS, "16"));
        assertThat(c.maxInFlight()).isEqualTo(16);
        assertThatThrownBy(() -> TestSupport.config(Map.of(SinkConfig.MAX_IN_FLIGHT, "0")))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void enumerationsAreCaseInsensitiveAndRejectUnknownValues() {
        assertThat(TestSupport.config(Map.of(SinkConfig.NULL_BEHAVIOR, "CLEAR")).nullBehavior())
                .isEqualTo(SinkConfig.NullBehavior.CLEAR);
        assertThatThrownBy(() -> TestSupport.config(Map.of(SinkConfig.NESTED_BEHAVIOR, "explode")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("[reject, flatten, stringify]");
        assertThatThrownBy(
                        () ->
                                TestSupport.config(
                                        Map.of(SinkConfig.BEHAVIOR_ON_API_ERRORS, "retry")))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void secretsNeverAppearInToString() {
        assertThat(TestSupport.config(Map.of()).toString()).doesNotContain("secret");
    }
}
