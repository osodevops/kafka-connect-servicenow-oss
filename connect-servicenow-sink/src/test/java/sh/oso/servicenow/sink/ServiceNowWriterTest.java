package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.config.CoreConfigDefs;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

class ServiceNowWriterTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private MockServiceNowServer snow;
    private ServiceNowClient client;
    private ServiceNowWriter writer;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
    }

    @AfterEach
    void tearDown() {
        if (writer != null) {
            writer.close();
        }
        if (client != null) {
            client.close();
        }
        snow.close();
    }

    private ServiceNowWriter writer(Map<String, String> overrides) {
        SinkConfig config = new SinkConfig(TestSupport.props(snow, overrides));
        client = ServiceNowClient.create(config.coreConfig());
        writer = new ServiceNowWriter(config, client);
        return writer;
    }

    private List<String> seedRows(int partitions) {
        List<String> ids = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            ids.add(snow.tables().insert("incident", Map.of("seq", "-1", "partition", "" + p)));
        }
        return ids;
    }

    private static List<SinkRecord> patches(List<String> ids, int perPartition) {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < perPartition; i++) {
            for (int p = 0; p < ids.size(); p++) {
                records.add(TestSupport.record(p, i, ids.get(p), Map.of("seq", i)));
            }
        }
        return records;
    }

    @Test
    void preservesOrderWithinEachPartitionUnderBoundedConcurrency() throws Exception {
        snow.faults().latency(Duration.ofMillis(5));
        List<String> ids = seedRows(4);
        ServiceNowWriter w = writer(Map.of(SinkConfig.MAX_IN_FLIGHT, "8"));
        List<Outcome> outcomes = w.write(patches(ids, 50));

        assertThat(outcomes).hasSize(200).allMatch(Outcome::isSuccess);
        for (String id : ids) {
            List<String> bodies = snow.journal().bodies("PATCH", "incident", id);
            assertThat(bodies).hasSize(50);
            List<Integer> seqs = new ArrayList<>();
            for (String b : bodies) {
                seqs.add(JSON.readTree(b).get("seq").asInt());
            }
            for (int i = 0; i < 50; i++) {
                assertThat(seqs.get(i)).isEqualTo(i);
            }
            Map<String, String> row = snow.tables().get("incident", id).orElseThrow();
            assertThat(row.get("seq")).isEqualTo("49");
            assertThat(row.get("sys_mod_count")).isEqualTo("50");
        }
        assertThat(snow.journal().maxInFlight()).isBetween(2, 8);
    }

    @Test
    void inFlightNeverExceedsTheConfiguredBound() {
        snow.faults().latency(Duration.ofMillis(10));
        List<String> ids = seedRows(6);
        ServiceNowWriter w = writer(Map.of(SinkConfig.MAX_IN_FLIGHT, "3"));
        List<Outcome> outcomes = w.write(patches(ids, 5));
        assertThat(outcomes).hasSize(30).allMatch(Outcome::isSuccess);
        assertThat(snow.journal().maxInFlight()).isBetween(2, 3);
    }

    @Test
    void rateLimitStormNeverExceedsMaxInFlight() {
        snow.faults().rateLimit(40, Duration.ZERO);
        snow.faults().latency(Duration.ofMillis(2));
        List<String> ids = seedRows(4);
        ServiceNowWriter w =
                writer(
                        Map.of(
                                SinkConfig.MAX_IN_FLIGHT, "4",
                                CoreConfigDefs.RETRY_MAX_ATTEMPTS, "200",
                                CoreConfigDefs.HTTP_ADAPTIVE_THROTTLING, "false"));
        List<Outcome> outcomes = w.write(patches(ids, 10));
        assertThat(outcomes).hasSize(40).allMatch(Outcome::isSuccess);
        assertThat(snow.journal().maxInFlight()).isLessThanOrEqualTo(4);
        for (String id : ids) {
            assertThat(snow.tables().get("incident", id).orElseThrow().get("seq")).isEqualTo("9");
        }
    }

    @Test
    void retryableExhaustionStopsTheLaneAndIsMarkedRetryable() {
        snow.faults().serverError(100, 503);
        List<String> ids = seedRows(1);
        ServiceNowWriter w = writer(Map.of());
        List<Outcome> outcomes = w.write(patches(ids, 3));
        assertThat(outcomes).hasSize(1);
        Outcome o = outcomes.get(0);
        assertThat(o.isRetryable()).isTrue();
        assertThat(o.classification()).isEqualTo(Classification.RETRIES_EXHAUSTED);
        assertThat(o.status()).isEqualTo(503);
        assertThat(o.retries()).isEqualTo(3);
    }

    @Test
    void permanentErrorStopsTheLaneOnlyUnderFailBehavior() {
        List<String> ids = seedRows(1);
        String id = ids.get(0);
        List<SinkRecord> records =
                List.of(
                        TestSupport.record(0, 0, id, "not json"),
                        TestSupport.record(0, 1, id, Map.of("seq", 1)));
        ServiceNowWriter failing = writer(Map.of(SinkConfig.BEHAVIOR_ON_API_ERRORS, "fail"));
        List<Outcome> stopped = failing.write(records);
        assertThat(stopped).hasSize(1);
        assertThat(stopped.get(0).classification()).isEqualTo(Classification.RECORD_ERROR);
        failing.close();
        client.close();

        ServiceNowWriter logging = writer(Map.of(SinkConfig.BEHAVIOR_ON_API_ERRORS, "log"));
        List<Outcome> continued = logging.write(records);
        assertThat(continued).hasSize(2);
        assertThat(continued.get(0).isSuccess()).isFalse();
        assertThat(continued.get(1).isSuccess()).isTrue();
        assertThat(snow.tables().get("incident", id).orElseThrow().get("seq")).isEqualTo("1");
    }

    @Test
    void outcomesComeBackInInputOrderAcrossPartitions() {
        List<String> ids = seedRows(3);
        ServiceNowWriter w = writer(Map.of());
        List<SinkRecord> records = patches(ids, 2);
        List<Outcome> outcomes = w.write(records);
        assertThat(outcomes).extracting(Outcome::record).containsExactlyElementsOf(records);
        assertThat(w.write(List.of())).isEmpty();
    }
}
