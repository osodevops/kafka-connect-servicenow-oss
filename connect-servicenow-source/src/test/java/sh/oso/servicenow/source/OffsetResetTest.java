package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

/** Replaying from the same offset (or from no offset) yields the same record sequence. */
class OffsetResetTest {

    static final int ROWS = 40;

    MockServiceNowServer snow;
    Instant now;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
        now = snow.clock().instant();
        Fixtures.seed(snow.tables(), "incident", ROWS, now.minusSeconds(100), 2);
    }

    @AfterEach
    void tearDown() {
        snow.close();
    }

    private static List<String> sequence(List<SourceRecord> records) {
        return records.stream().map(Fixtures::versionKey).toList();
    }

    @Test
    void seedingTheSameOffsetTwiceReplaysAnIdenticalSequence() throws Exception {
        List<SourceRecord> full;
        Map<String, ?> partition;
        Map<String, Object> mid;
        try (TaskHarness a = new TaskHarness(snow).with(SourceConfig.BATCH_SIZE, "7")) {
            a.start();
            full = a.pollUntil(ROWS, 10_000);
            assertThat(full).hasSize(ROWS);
            SourceRecord r19 = full.get(19);
            assertThat(r19.key()).isEqualTo(Fixtures.sysId(19));
            partition = r19.sourcePartition();
            mid = new HashMap<>(r19.sourceOffset());
        }

        // cursor at second 9 (rows 18,19); overlap 2 re-reads seconds 8 and 9 -> rows 16..39
        int expected = ROWS - 16;
        List<String> s2;
        try (TaskHarness b = new TaskHarness(snow).with(SourceConfig.BATCH_SIZE, "7")) {
            b.seedOffset(partition, mid);
            b.start();
            s2 = sequence(b.pollUntil(expected, 10_000));
        }
        List<String> s3;
        try (TaskHarness c = new TaskHarness(snow).with(SourceConfig.BATCH_SIZE, "7")) {
            c.seedOffset(partition, mid);
            c.start();
            s3 = sequence(c.pollUntil(expected, 10_000));
        }
        assertThat(s2).hasSize(expected).isEqualTo(s3);
        assertThat(s2.get(0)).startsWith(Fixtures.sysId(16) + "@");
        assertThat(s2).isEqualTo(sequence(full).subList(16, ROWS));
    }

    @Test
    void droppingTheOffsetReplaysTheWholeTableDeterministically() throws Exception {
        List<String> first;
        try (TaskHarness a = new TaskHarness(snow).with(SourceConfig.BATCH_SIZE, "9")) {
            a.start();
            first = sequence(a.pollUntil(ROWS, 10_000));
        }
        List<String> second;
        try (TaskHarness b = new TaskHarness(snow).with(SourceConfig.BATCH_SIZE, "9")) {
            b.start();
            second = sequence(b.pollUntil(ROWS, 10_000));
        }
        assertThat(first).hasSize(ROWS).isEqualTo(second);
    }
}
