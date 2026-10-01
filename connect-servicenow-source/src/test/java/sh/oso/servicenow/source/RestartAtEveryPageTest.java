package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

/**
 * Kill and restart after every page of a backfill: the union of what was delivered before and after
 * each restart is the whole table, and duplicates never exceed the overlap window around the
 * committed cursor.
 */
class RestartAtEveryPageTest {

    static final int ROWS = 2_500;
    static final int PER_SECOND = 84; // about 30 seconds of rows
    static final int BATCH = 100;
    static final int OVERLAP = 2;

    MockServiceNowServer snow;
    Set<String> allVersions;
    List<Map<String, String>> rows;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
        Instant base = snow.clock().instant().minusSeconds(300);
        Fixtures.seed(snow.tables(), "incident", ROWS, base, PER_SECOND);
        rows = snow.tables().all("incident");
        allVersions = new HashSet<>();
        rows.forEach(r -> allVersions.add(Fixtures.versionKey(r)));
    }

    @AfterEach
    void tearDown() {
        snow.close();
    }

    private TaskHarness fresh() {
        return new TaskHarness(snow)
                .with(SourceConfig.BATCH_SIZE, Integer.toString(BATCH))
                .with(SourceConfig.OVERLAP_SECONDS, Integer.toString(OVERLAP))
                .with(SourceConfig.STARTUP_PROBE, "false");
    }

    /** Polls until the union of versions is complete or the deadline passes. */
    private static List<SourceRecord> pollToTheEnd(TaskHarness h, Set<String> seen, int total)
            throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 30_000;
        while (seen.size() < total && System.currentTimeMillis() < deadline) {
            List<SourceRecord> page = h.poll();
            out.addAll(page);
            page.forEach(r -> seen.add(Fixtures.versionKey(r)));
        }
        return out;
    }

    private long rowsInOverlapWindowOf(Instant cursorTs) {
        Instant from = cursorTs.minusSeconds(OVERLAP);
        return rows.stream()
                .filter(
                        r -> {
                            Instant ts = SnowTimestamp.parse(r.get("sys_updated_on"));
                            return ts.isAfter(from) && !ts.isAfter(cursorTs);
                        })
                .count();
    }

    @Test
    void restartAfterEveryPageLosesNothingAndBoundsDuplicates() throws Exception {
        int pages;
        try (TaskHarness baseline = fresh()) {
            baseline.start();
            Set<String> seen = new HashSet<>();
            pages = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (seen.size() < ROWS && System.currentTimeMillis() < deadline) {
                List<SourceRecord> page = baseline.poll();
                if (!page.isEmpty()) {
                    pages++;
                }
                page.forEach(r -> seen.add(Fixtures.versionKey(r)));
            }
            assertThat(seen).isEqualTo(allVersions);
        }
        assertThat(pages).isGreaterThanOrEqualTo(ROWS / BATCH);

        int worstDuplicates = 0;
        for (int k = 1; k < pages; k++) {
            try (TaskHarness h = fresh()) {
                h.start();
                List<SourceRecord> before = new ArrayList<>();
                int got = 0;
                long deadline = System.currentTimeMillis() + 30_000;
                while (got < k && System.currentTimeMillis() < deadline) {
                    List<SourceRecord> page = h.poll();
                    if (!page.isEmpty()) {
                        got++;
                        before.addAll(page);
                    }
                }
                SourceRecord lastCommitted = before.get(before.size() - 1);
                Instant cursorTs = SnowTimestamp.parse(Fixtures.offsetTimestamp(lastCommitted));

                h.restart();
                Set<String> seenBefore = new HashSet<>();
                before.forEach(r -> seenBefore.add(Fixtures.versionKey(r)));
                Set<String> union = new HashSet<>(seenBefore);
                List<SourceRecord> after = pollToTheEnd(h, union, ROWS);

                assertThat(union).as("restart after page " + k).isEqualTo(allVersions);
                long duplicates =
                        after.stream()
                                .filter(r -> seenBefore.contains(Fixtures.versionKey(r)))
                                .count();
                long bound = rowsInOverlapWindowOf(cursorTs);
                assertThat(duplicates)
                        .as("duplicates after restart at page " + k + " (cursor " + cursorTs + ")")
                        .isLessThanOrEqualTo(bound);
                worstDuplicates = (int) Math.max(worstDuplicates, duplicates);
                assertThat(Fixtures.offsetsMonotonic(after)).isTrue();
            }
        }
        assertThat(worstDuplicates).isLessThanOrEqualTo((OVERLAP + 1) * PER_SECOND);
    }
}
