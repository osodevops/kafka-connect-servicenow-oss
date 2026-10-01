package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

class MultiTableTaskTest {

    static final List<String> TABLES =
            List.of("incident", "problem", "change_request", "sc_task", "kb_knowledge");
    static final int ROWS_PER_TABLE = 20;

    MockServiceNowServer snow;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
        Instant base = snow.clock().instant().minusSeconds(100);
        for (String t : TABLES) {
            Fixtures.seed(snow.tables(), t, ROWS_PER_TABLE, base, 2);
        }
    }

    @AfterEach
    void tearDown() {
        snow.close();
    }

    private Map<String, String> props() {
        Map<String, String> p = new HashMap<>(snow.basicAuthProps());
        p.putAll(MockServiceNowServer.fastRetryProps());
        List<String> aliases = new ArrayList<>();
        for (int i = 0; i < TABLES.size(); i++) {
            String alias = "t" + (i + 1);
            aliases.add(alias);
            p.put(TableSpec.key(alias, TableSpec.NAME), TABLES.get(i));
            p.put(TableSpec.key(alias, TableSpec.TOPIC), "snow." + TABLES.get(i));
        }
        p.put(SourceConfig.TABLES, String.join(",", aliases));
        p.put(SourceConfig.POLL_INTERVAL_MS, "200");
        return p;
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5})
    void fiveTablesRunAcrossTasksWithEveryRowDeliveredOnce(int maxTasks) throws Exception {
        ServiceNowSourceConnector connector = new ServiceNowSourceConnector();
        connector.start(props());
        List<Map<String, String>> configs = connector.taskConfigs(maxTasks);
        assertThat(configs.size()).isBetween(1, maxTasks);

        List<SourceRecord> all = new ArrayList<>();
        for (Map<String, String> cfg : configs) {
            int owned = cfg.get(SourceConfig.TASK_TABLES).split(",").length;
            try (TaskHarness h = new TaskHarness(cfg)) {
                h.start();
                List<SourceRecord> got = h.pollUntil(owned * ROWS_PER_TABLE, 10_000);
                assertThat(got).hasSize(owned * ROWS_PER_TABLE);
                assertThat(h.task().metrics()).hasSize(owned);
                all.addAll(got);
            }
        }
        assertThat(all).hasSize(TABLES.size() * ROWS_PER_TABLE);
        Map<String, Long> perTopic =
                all.stream()
                        .collect(Collectors.groupingBy(SourceRecord::topic, Collectors.counting()));
        for (String t : TABLES) {
            assertThat(perTopic).containsEntry("snow." + t, (long) ROWS_PER_TABLE);
        }
        assertThat(all.stream().map(r -> r.topic() + "/" + r.key()).distinct().count())
                .isEqualTo(all.size());
    }

    @Test
    void aForbiddenTableDoesNotStopTheOthersAndTheFailureNamesIt() throws Exception {
        snow.faults().forbidTable("problem");
        Map<String, String> p = props();
        p.put(SourceConfig.STARTUP_PROBE, "false");
        try (TaskHarness h = new TaskHarness(p)) {
            h.start();
            List<SourceRecord> got = h.pollUntil(4 * ROWS_PER_TABLE, 10_000);
            assertThat(got).hasSize(4 * ROWS_PER_TABLE);
            assertThat(got).extracting(SourceRecord::topic).doesNotContain("snow.problem");
            assertThat(h.task().metrics().get("incident").recordsEmitted())
                    .isEqualTo(ROWS_PER_TABLE);
            assertThat(h.task().metrics().get("problem").recordsEmitted()).isZero();

            assertThatThrownBy(() -> h.pollFor(2_000))
                    .isInstanceOf(ConnectException.class)
                    .hasMessageContaining("problem")
                    .hasMessageContaining("403");
        }
    }
}
