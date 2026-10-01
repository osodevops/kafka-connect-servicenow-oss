package sh.oso.servicenow.sink;

import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServiceNowSinkTaskTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TopicPartition P0 = new TopicPartition(TestSupport.TOPIC, 0);

    private SinkTaskHarness h;

    @BeforeEach
    void setUp() {
        h = new SinkTaskHarness();
    }

    @AfterEach
    void tearDown() {
        h.close();
    }

    @Test
    void createPatchAndTombstoneFollowInference() {
        h.start(h.props(Map.of()));
        h.put(TestSupport.record(0, 0, null, Map.of("short_description", "new")));
        assertThat(h.snow.tables().size("incident")).isEqualTo(1);
        String id = h.snow.tables().all("incident").get(0).get("sys_id");

        h.put(TestSupport.record(0, 1, id, Map.of("urgency", "1")));
        Map<String, String> row = h.snow.tables().get("incident", id).orElseThrow();
        assertThat(row).containsEntry("urgency", "1").containsEntry("short_description", "new");

        h.put(TestSupport.record(0, 2, id, null));
        assertThat(h.snow.tables().exists("incident", id)).isFalse();
        assertThat(h.snow.journal().count("DELETE", "incident")).isEqualTo(1);
        assertThat(h.preCommit(TestSupport.record(0, 2, id, null)))
                .containsEntry(P0, new OffsetAndMetadata(3));
    }

    @Test
    void offsetsAdvanceOnlyPastCompletedRecords() {
        h.start(h.props(Map.of()));
        String id = h.snow.tables().insert("incident", Map.of("state", "1"));
        SinkRecord ok = TestSupport.record(0, 0, id, Map.of("state", "2"));
        h.put(ok);
        assertThat(h.preCommit(ok)).containsEntry(P0, new OffsetAndMetadata(1));

        h.snow.faults().serverError(100, 503);
        SinkRecord stuck = TestSupport.record(0, 1, id, Map.of("state", "3"));
        assertThatThrownBy(() -> h.put(stuck)).isInstanceOf(RetriableException.class);
        assertThat(h.preCommit(stuck)).containsEntry(P0, new OffsetAndMetadata(1));
        assertThat(h.dlq).isEmpty();

        h.snow.faults().clear();
        h.put(stuck);
        assertThat(h.preCommit(stuck)).containsEntry(P0, new OffsetAndMetadata(2));
        assertThat(h.snow.tables().get("incident", id).orElseThrow().get("state")).isEqualTo("3");

        h.task.close(java.util.List.of(P0));
        assertThat(h.preCommit(stuck)).isEmpty();
    }

    @Test
    void failBehaviorReportsToTheDlqThenFailsTheTask() {
        h.start(h.props(Map.of()));
        SinkRecord bad = TestSupport.record(0, 0, null, "not json");
        assertThatThrownBy(() -> h.put(bad))
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasMessageContaining(TestSupport.TOPIC + "-0@0")
                .hasMessageContaining("RECORD_ERROR");
        assertThat(h.dlq).hasSize(1);
        assertThat(h.dlq.get(0).record()).isSameAs(bad);
        assertThat(h.dlq.get(0).error()).isInstanceOf(RecordError.class);
        assertThat(h.preCommit(bad)).isEmpty();
    }

    @Test
    void logBehaviorAbsorbsTheFailureAfterDlqAcceptance() {
        h.start(h.props(Map.of(SinkConfig.BEHAVIOR_ON_API_ERRORS, "log")));
        SinkRecord bad = TestSupport.record(0, 0, null, "not json");
        SinkRecord good = TestSupport.record(0, 1, null, Map.of("short_description", "x"));
        assertThatCode(() -> h.put(bad, good)).doesNotThrowAnyException();
        assertThat(h.dlq).hasSize(1);
        assertThat(h.snow.tables().size("incident")).isEqualTo(1);
        assertThat(h.preCommit(bad, good)).containsEntry(P0, new OffsetAndMetadata(2));
    }

    @Test
    void ignoreBehaviorAbsorbsSilentlyAndWorksWithoutADlq() {
        h.dlqConfigured = false;
        h.start(h.props(Map.of(SinkConfig.BEHAVIOR_ON_API_ERRORS, "ignore")));
        SinkRecord bad = TestSupport.record(0, 0, null, "not json");
        assertThatCode(() -> h.put(bad)).doesNotThrowAnyException();
        assertThat(h.dlq).isEmpty();
        assertThat(h.preCommit(bad)).containsEntry(P0, new OffsetAndMetadata(1));
    }

    @Test
    void oldWorkersWithoutAnErrantRecordReporterAreTolerated() {
        h.oldWorker = true;
        h.start(h.props(Map.of(SinkConfig.BEHAVIOR_ON_API_ERRORS, "log")));
        SinkRecord bad = TestSupport.record(0, 0, null, "not json");
        assertThatCode(() -> h.put(bad)).doesNotThrowAnyException();
        assertThat(h.dlq).isEmpty();
    }

    @Test
    void unauthorizedOnceIsRefreshedAndRetriedOnce() {
        h.start(h.props(Map.of()));
        h.snow.faults().unauthorizedOnce();
        h.put(TestSupport.record(0, 0, null, Map.of("short_description", "x")));
        assertThat(h.snow.tables().size("incident")).isEqualTo(1);
        assertThat(
                        h.snow.wireMock()
                                .countRequestsMatching(
                                        postRequestedFor(urlPathEqualTo("/api/now/table/incident"))
                                                .build())
                                .getCount())
                .isEqualTo(2);
    }

    @Test
    void forbiddenTableFailsTheRecordWithAPermissionDiagnostic() {
        h.start(h.props(Map.of()));
        h.snow.faults().forbidTable("incident");
        SinkRecord r = TestSupport.record(0, 0, null, Map.of("short_description", "x"));
        assertThatThrownBy(() -> h.put(r))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("PERMISSION")
                .hasMessageContaining("403")
                .hasMessageContaining("'connect'")
                .hasMessageContaining("incident");
        RecordError e = (RecordError) h.dlq.get(0).error();
        assertThat(e.classification()).isEqualTo(Classification.PERMISSION);
        assertThat(e.status()).isEqualTo(403);
    }

    @Test
    void reportersReceiveSuccessAndErrorRecordsAndFlushOnPreCommit() throws Exception {
        h.start(
                h.props(
                        Map.of(
                                SinkConfig.REPORTER_BOOTSTRAP_SERVERS, "kafka:9092",
                                SinkConfig.REPORTER_SUCCESS_TOPIC, "ok",
                                SinkConfig.REPORTER_ERROR_TOPIC, "errors",
                                SinkConfig.BEHAVIOR_ON_API_ERRORS, "log")));
        SinkRecord good = TestSupport.record(0, 5, null, Map.of("short_description", "x"));
        SinkRecord bad = TestSupport.record(0, 6, "not-a-sys-id", Map.of("short_description", "y"));
        h.put(good, bad);
        assertThat(h.producer.history()).hasSize(2);
        JsonNode ok = JSON.readTree(h.producer.history().get(0).value());
        assertThat(ok.get("operation").asText()).isEqualTo("CREATE");
        assertThat(ok.get("status").asInt()).isEqualTo(201);
        assertThat(ok.get("source").get("offset").asLong()).isEqualTo(5);
        assertThat(ok.get("sys_id").asText()).hasSize(32);
        JsonNode err = JSON.readTree(h.producer.history().get(1).value());
        assertThat(err.get("classification").asText()).isEqualTo("RECORD_ERROR");
        assertThat(err.get("source").get("offset").asLong()).isEqualTo(6);
        assertThat(h.producer.flushed()).isFalse();
        h.preCommit(good, bad);
        assertThat(h.producer.flushed()).isTrue();
    }

    @Test
    void unknownFieldsUnderReportAreDroppedAndReported() throws Exception {
        TestSupport.seedDictionary(h.snow, "incident", "short_description", "u_cost_centre");
        h.start(
                h.props(
                        Map.of(
                                SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "report",
                                SinkConfig.REPORTER_BOOTSTRAP_SERVERS, "kafka:9092",
                                SinkConfig.REPORTER_ERROR_TOPIC, "errors")));
        h.put(
                TestSupport.record(
                        0,
                        0,
                        null,
                        Map.of("short_description", "x", "u_cost_centre", "CC", "urgencyy", "1")));
        Map<String, String> row = h.snow.tables().all("incident").get(0);
        assertThat(row)
                .containsEntry("short_description", "x")
                .containsEntry("u_cost_centre", "CC");
        assertThat(row).doesNotContainKey("urgencyy");
        assertThat(h.producer.history()).hasSize(1);
        JsonNode report = JSON.readTree(h.producer.history().get(0).value());
        assertThat(report.get("message").asText()).contains("[urgencyy]");
    }

    @Test
    void headerRoutingAndOperationHeadersAreHonoured() {
        h.start(
                h.props(
                        Map.of(
                                SinkConfig.ROUTING_MODE, "header",
                                SinkConfig.TABLE_ALLOWLIST, "incident,problem")));
        h.put(
                TestSupport.record(
                        0,
                        0,
                        null,
                        Map.of("short_description", "p"),
                        TestSupport.headers("snow.table", "problem", "snow.operation", "create")));
        assertThat(h.snow.tables().size("problem")).isEqualTo(1);
        String id = h.snow.tables().all("problem").get(0).get("sys_id");
        h.put(
                TestSupport.record(
                        0,
                        1,
                        null,
                        Map.of("short_description", "q"),
                        TestSupport.headers(
                                "snow.table",
                                "problem",
                                "snow.operation",
                                "PUT",
                                "snow.sys_id",
                                id)));
        assertThat(h.snow.tables().get("problem", id).orElseThrow())
                .containsEntry("short_description", "q");
        assertThat(h.snow.journal().count("PUT", "problem")).isEqualTo(1);
    }

    @Test
    void emptyBatchesAreNoOps() {
        h.start(h.props(Map.of()));
        assertThatCode(() -> h.task.put(java.util.List.of())).doesNotThrowAnyException();
        assertThat(h.snow.journal().entries()).isEmpty();
    }
}
