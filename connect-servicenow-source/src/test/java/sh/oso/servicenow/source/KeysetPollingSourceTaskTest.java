package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;
import sh.oso.servicenow.testing.MutableClock;

class KeysetPollingSourceTaskTest {

    MockServiceNowServer snow;
    MutableClock clock;
    Instant now;
    TaskHarness harness;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
        clock = snow.clock();
        now = clock.instant();
        harness = new TaskHarness(snow);
    }

    @AfterEach
    void tearDown() {
        harness.close();
        snow.close();
    }

    @Test
    void backfillsFromTheStartTimestampInCursorOrder() throws Exception {
        Instant base = now.minusSeconds(200);
        Fixtures.seed(snow.tables(), "incident", 30, base, 3);
        Instant start = base.plusSeconds(3); // rows 9..29
        harness.with(TableSpec.key("t1", TableSpec.START_TIMESTAMP), SnowTimestamp.format(start))
                .with(SourceConfig.BATCH_SIZE, "7");
        harness.start();

        List<SourceRecord> records = harness.pollUntil(21, 10_000);
        assertThat(records).hasSize(21);
        List<String> expected = new ArrayList<>();
        for (int i = 9; i < 30; i++) {
            expected.add(Fixtures.sysId(i));
        }
        assertThat(records).extracting(SourceRecord::key).containsExactlyElementsOf(expected);
        assertThat(records)
                .allSatisfy(
                        r ->
                                assertThat(SnowTimestamp.parse(Fixtures.field(r, "sys_updated_on")))
                                        .isAfterOrEqualTo(start));
        assertThat(Fixtures.offsetsMonotonic(records)).isTrue();
        assertThat(Fixtures.offset(records.get(0))).containsEntry("phase", "backfill");
        assertThat(records).allSatisfy(r -> assertThat(r.topic()).isEqualTo(TaskHarness.TOPIC));
        assertThat(harness.pollFor(400)).isEmpty();
        assertThat(harness.task().pollers().get(0).phase()).isEqualTo("stream");
    }

    @Test
    void fullPageContinuesImmediatelyAndShortPageWaitsForThePollInterval() throws Exception {
        Fixtures.seed(snow.tables(), "incident", 25, now.minusSeconds(100), 5);
        harness.with(SourceConfig.BATCH_SIZE, "10").with(SourceConfig.POLL_INTERVAL_MS, "2000");
        harness.start();

        long t0 = System.currentTimeMillis();
        List<SourceRecord> records = harness.pollUntil(25, 5_000);
        long elapsed = System.currentTimeMillis() - t0;
        assertThat(records).hasSize(25);
        assertThat(elapsed).as("full pages must not wait for the poll interval").isLessThan(1500);

        snow.journal().reset();
        assertThat(harness.pollFor(600)).isEmpty();
        assertThat(snow.journal().count("GET", "incident"))
                .as("idle until the poll interval elapses")
                .isZero();

        harness.pollFor(2000);
        assertThat(snow.journal().count("GET", "incident")).isGreaterThan(0);
    }

    @Test
    void rowsNewerThanTheHighWaterMarkWaitUntilTheClockAdvances() throws Exception {
        harness.with(SourceConfig.SAFETY_LAG_SECONDS, "30");
        Fixtures.seed(snow.tables(), "incident", 5, now.minusSeconds(100), 1);
        for (int i = 100; i < 103; i++) {
            snow.tables().insert("incident", Fixtures.row(i, now));
        }
        harness.start();

        assertThat(harness.pollUntil(5, 5_000)).hasSize(5);
        assertThat(harness.pollFor(700)).as("rows inside the safety lag are not closed").isEmpty();

        clock.advance(Duration.ofSeconds(60));
        List<SourceRecord> fresh = harness.pollUntil(3, 5_000);
        assertThat(fresh)
                .extracting(SourceRecord::key)
                .containsExactly(Fixtures.sysId(100), Fixtures.sysId(101), Fixtures.sysId(102));
    }

    @Test
    void overlapReReadIsSuppressedByTheDedupCache() throws Exception {
        Fixtures.seed(snow.tables(), "incident", 10, now.minusSeconds(50), 1);
        harness.start();

        assertThat(harness.pollUntil(10, 5_000)).hasSize(10);
        assertThat(harness.pollFor(900)).isEmpty();

        TableMetrics m = harness.task().metrics().get("incident");
        assertThat(m.recordsEmitted()).isEqualTo(10);
        assertThat(m.duplicatesSuppressed())
                .as("overlap window re-read every sweep")
                .isGreaterThanOrEqualTo(2);
        assertThat(m.phase()).isEqualTo("stream");
        assertThat(m.pagesFetched()).isGreaterThan(1);
        assertThat(m.cursorTimestamp()).isEqualTo(SnowTimestamp.format(now.minusSeconds(41)));
        assertThat(m.lastSuccessfulRequestEpochMs()).isGreaterThan(0);
        assertThat(m.snapshot()).containsKeys("lagSeconds", "lastPollDurationMs", "retries");
    }

    @Test
    void rowUpdatedTwiceDuringBackfillIsEmittedInItsLatestState() throws Exception {
        Fixtures.seed(snow.tables(), "incident", 6, now.minusSeconds(100), 1);
        harness.with(SourceConfig.BATCH_SIZE, "2");
        harness.start();

        List<SourceRecord> first = harness.pollUntil(2, 5_000);
        assertThat(first)
                .extracting(SourceRecord::key)
                .containsExactly(Fixtures.sysId(0), Fixtures.sysId(1));

        snow.tables().update("incident", Fixtures.sysId(0), Map.of("short_description", "first"));
        snow.tables().update("incident", Fixtures.sysId(0), Map.of("short_description", "second"));
        clock.advance(Duration.ofSeconds(10));

        List<SourceRecord> rest = harness.pollUntil(5, 10_000);
        List<SourceRecord> all = new ArrayList<>(first);
        all.addAll(rest);
        List<SourceRecord> row0 =
                all.stream().filter(r -> Fixtures.sysId(0).equals(r.key())).toList();
        assertThat(row0).hasSize(2);
        assertThat(Fixtures.field(row0.get(0), "sys_mod_count")).isEqualTo("0");
        assertThat(Fixtures.field(row0.get(1), "sys_mod_count")).isEqualTo("2");
        assertThat(Fixtures.field(row0.get(1), "short_description")).isEqualTo("second");
        assertThat(all).extracting(r -> Fixtures.field(r, "sys_mod_count")).doesNotContain("1");
        assertThat(rest.get(rest.size() - 1).key()).isEqualTo(Fixtures.sysId(0));
        assertThat(Fixtures.offsetsMonotonic(all)).isTrue();
    }

    @Test
    void recordCarriesKeyHeadersTimestampPartitionAndOffset() throws Exception {
        Instant ts = now.minusSeconds(30);
        snow.tables().insert("incident", Fixtures.row(7, ts));
        harness.start();

        SourceRecord r = harness.pollUntil(1, 5_000).get(0);
        assertThat(r.key()).isEqualTo(Fixtures.sysId(7));
        assertThat(r.keySchema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(r.topic()).isEqualTo(TaskHarness.TOPIC);
        assertThat(r.timestamp()).isEqualTo(ts.toEpochMilli());
        assertThat(r.valueSchema()).isNull();
        assertThat(Fixtures.value(r))
                .containsEntry("short_description", "row 7")
                .containsEntry("sys_mod_count", "0")
                .containsEntry("sys_updated_on", SnowTimestamp.format(ts));
        assertThat(r.headers().lastWithName("snow.table").value()).isEqualTo("incident");
        assertThat(r.headers().lastWithName("snow.instance").value()).isEqualTo("localhost");
        assertThat(r.headers().lastWithName("snow.source.operation").value()).isEqualTo("UPSERT");
        assertThat(r.headers().lastWithName("snow.schema.mode").value()).isEqualTo("schemaless");
        assertThat((Long) r.headers().lastWithName("snow.extracted_at").value()).isGreaterThan(0L);
        assertThat(Fixtures.partition(r))
                .containsEntry("instance", "localhost")
                .containsEntry("table", "incident")
                .containsEntry("timestamp_field", "sys_updated_on");
        assertThat(String.valueOf(r.sourcePartition().get("query_fingerprint")))
                .startsWith("sha256:");
        assertThat(Fixtures.offset(r))
                .containsEntry("version", "1")
                .containsEntry("timestamp", SnowTimestamp.format(ts))
                .containsEntry("sys_id", Fixtures.sysId(7))
                .containsEntry("phase", "backfill")
                .containsEntry("fingerprint", r.sourcePartition().get("query_fingerprint"));
    }

    @Test
    void envelopeWrapsTheValueInSchemalessMode() throws Exception {
        Instant ts = now.minusSeconds(30);
        snow.tables().insert("incident", Fixtures.row(3, ts));
        harness.with(SourceConfig.EMIT_ENVELOPE, "true");
        harness.start();

        SourceRecord r = harness.pollUntil(1, 5_000).get(0);
        Map<String, Object> env = Fixtures.value(r);
        assertThat(env.keySet()).containsExactly("before", "after", "source", "op", "ts_ms");
        assertThat(env.get("before")).isNull();
        assertThat(Fixtures.map(env.get("after"))).containsEntry("short_description", "row 3");
        assertThat(Fixtures.map(env.get("source")))
                .containsEntry("table", "incident")
                .containsEntry("instance", "localhost");
        assertThat(env.get("op")).isEqualTo("u");
        assertThat(env.get("ts_ms")).isEqualTo(ts.toEpochMilli());
    }

    @Test
    void phaseBecomesStreamOnceCaughtUpAndNewRowsArriveOnTheNextSweep() throws Exception {
        Fixtures.seed(snow.tables(), "incident", 3, now.minusSeconds(100), 1);
        harness.start();
        assertThat(harness.pollUntil(3, 5_000)).hasSize(3);
        assertThat(harness.pollFor(300)).isEmpty();

        snow.tables().insert("incident", Fixtures.row(50, now.minusSeconds(5)));
        SourceRecord next = harness.pollUntil(1, 5_000).get(0);
        assertThat(next.key()).isEqualTo(Fixtures.sysId(50));
        assertThat(Fixtures.offset(next)).containsEntry("phase", "stream");
    }

    @Test
    void idleSweepsNeverDriftBelowTheStartTimestamp() throws Exception {
        // only rows older than the start timestamp exist; repeated overlap rewinds must not reach
        // them
        Instant start = now.minusSeconds(20);
        Fixtures.seed(snow.tables(), "incident", 5, now.minusSeconds(30), 1); // now-30 .. now-26
        harness.with(TableSpec.key("t1", TableSpec.START_TIMESTAMP), SnowTimestamp.format(start))
                .with(SourceConfig.POLL_INTERVAL_MS, "50")
                .with(SourceConfig.OVERLAP_SECONDS, "2");
        harness.start();
        assertThat(harness.pollFor(1_500)).as("no row at or after the start timestamp").isEmpty();
        TablePoller poller = harness.task().pollers().get(0);
        assertThat(poller.last().ts()).isEqualTo(start.minusSeconds(1));
        assertThat(poller.metrics().pagesFetched()).isGreaterThan(5);

        // a row at the start timestamp is picked up and anchors later rewinds
        snow.tables().insert("incident", Fixtures.row(9, start));
        assertThat(harness.pollUntil(1, 5_000))
                .extracting(SourceRecord::key)
                .containsExactly(Fixtures.sysId(9));
        assertThat(harness.pollFor(500)).isEmpty();
        assertThat(poller.last().ts()).isAfterOrEqualTo(start.minusSeconds(2));
    }

    @Test
    void emptyTaskAssignmentIdlesWithoutRequests() throws Exception {
        harness.with(SourceConfig.TASK_TABLES, "");
        harness.start();
        assertThat(harness.task().pollers()).hasSize(1);
        TaskHarness none = new TaskHarness(snow);
        none.table("t2", "problem", "snow.problem")
                .tables("t2")
                .with(SourceConfig.TASK_TABLES, "t2");
        none.start();
        assertThat(none.task().pollers()).extracting(TablePoller::table).containsExactly("problem");
        none.close();
    }
}
