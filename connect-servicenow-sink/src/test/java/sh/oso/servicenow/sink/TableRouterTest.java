package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class TableRouterTest {

    private static SinkRecord record(String topic, String... headers) {
        return TestSupport.record(
                topic, 0, 1, null, Map.of("a", "b"), TestSupport.headers(headers));
    }

    @Test
    void fixedRoutingIgnoresTheRecord() {
        TableRouter r = new TableRouter(TestSupport.config(Map.of(SinkConfig.TABLE, "problem")));
        assertThat(r.route(record("anything", "snow.table", "sys_user"))).isEqualTo("problem");
    }

    @Test
    void topicMapRoutesByTopicAndRejectsUnmappedTopics() {
        Map<String, String> p = TestSupport.offlineProps();
        p.remove(SinkConfig.TABLE);
        p.put(SinkConfig.ROUTING_MODE, "topic_map");
        p.put("snow.sink.topic.incidents.table", "incident");
        p.put("snow.sink.topic.problems.table", "problem");
        TableRouter r = new TableRouter(new SinkConfig(p));
        assertThat(r.route(record("incidents"))).isEqualTo("incident");
        assertThat(r.route(record("problems"))).isEqualTo("problem");
        assertThatThrownBy(() -> r.route(record("changes")))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("snow.sink.topic.changes.table");
    }

    @Test
    void headerRoutingValidatesAgainstTheAllowlistAndThePathPattern() {
        TableRouter r =
                new TableRouter(
                        TestSupport.config(
                                Map.of(
                                        SinkConfig.ROUTING_MODE, "header",
                                        SinkConfig.TABLE_ALLOWLIST, "incident,problem")));
        assertThat(r.route(record("t", "snow.table", "incident"))).isEqualTo("incident");
        assertThat(r.route(record("t", "snow.table", " problem "))).isEqualTo("problem");
        assertThatThrownBy(() -> r.route(record("t", "snow.table", "../sys_user")))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("not a valid table name");
        assertThatThrownBy(() -> r.route(record("t", "snow.table", "Incident")))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("not a valid table name");
        assertThatThrownBy(() -> r.route(record("t", "snow.table", "sys_user")))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("not in " + SinkConfig.TABLE_ALLOWLIST);
        assertThatThrownBy(() -> r.route(record("t")))
                .isInstanceOf(RecordError.class)
                .hasMessageContaining("no 'snow.table' header");
    }

    @Test
    void headerNameIsConfigurable() {
        TableRouter r =
                new TableRouter(
                        TestSupport.config(
                                Map.of(
                                        SinkConfig.ROUTING_MODE, "header",
                                        SinkConfig.TABLE_HEADER, "target",
                                        SinkConfig.TABLE_ALLOWLIST, "incident")));
        assertThat(r.route(record("t", "target", "incident"))).isEqualTo("incident");
    }
}
