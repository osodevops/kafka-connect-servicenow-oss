package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

/** More than 10,000 rows sharing one {@code sys_updated_on} second with a 5,000-row batch. */
class SameSecondBucketTest {

    static final int ROWS = 12_000;

    MockServiceNowServer snow;
    Instant bucket;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
        bucket = snow.clock().instant().minusSeconds(10);
        for (int i = 0; i < ROWS; i++) {
            snow.tables().insert("incident", Fixtures.row(i, bucket));
        }
    }

    @AfterEach
    void tearDown() {
        snow.close();
    }

    @Test
    void everyRowOfTheBucketIsEmittedExactlyOnce() throws Exception {
        try (TaskHarness harness = new TaskHarness(snow).with(SourceConfig.BATCH_SIZE, "5000")) {
            harness.start();
            List<SourceRecord> records = harness.pollUntil(ROWS, 60_000);
            assertThat(records).hasSize(ROWS);
            List<String> keys = records.stream().map(r -> (String) r.key()).toList();
            assertThat(new HashSet<>(keys)).hasSize(ROWS);
            assertThat(keys).isSorted();
            assertThat(records)
                    .allSatisfy(
                            r ->
                                    assertThat(Fixtures.field(r, "sys_updated_on"))
                                            .isEqualTo(SnowTimestamp.format(bucket)));
            assertThat(harness.pollFor(500)).isEmpty();
            assertThat(harness.task().metrics().get("incident").recordsEmitted()).isEqualTo(ROWS);
        }
    }

    @Test
    void restartInTheMiddleOfTheBucketLosesNothing() throws Exception {
        try (TaskHarness harness = new TaskHarness(snow).with(SourceConfig.BATCH_SIZE, "5000")) {
            harness.start();
            List<SourceRecord> before = harness.pollUntil(5000, 30_000);
            assertThat(before).hasSize(5000);
            harness.restart();
            List<SourceRecord> after = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            before.forEach(r -> seen.add((String) r.key()));
            long deadline = System.currentTimeMillis() + 60_000;
            while (seen.size() < ROWS && System.currentTimeMillis() < deadline) {
                List<SourceRecord> page = harness.poll();
                after.addAll(page);
                page.forEach(r -> seen.add((String) r.key()));
            }
            assertThat(seen).hasSize(ROWS);
            long duplicates =
                    after.stream()
                            .filter(r -> before.stream().anyMatch(b -> b.key().equals(r.key())))
                            .count();
            assertThat(duplicates)
                    .as("only the already-emitted part of the bucket repeats")
                    .isLessThanOrEqualTo(5000);
            assertThat(harness.task().pollers().get(0).resumed()).isTrue();
        }
    }
}
