package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

class ErrorHandlingSourceTest {

    MockServiceNowServer snow;
    Instant now;
    TaskHarness harness;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
        now = snow.clock().instant();
        Fixtures.seed(snow.tables(), "incident", 3, now.minusSeconds(50), 1);
        harness = new TaskHarness(snow);
    }

    @AfterEach
    void tearDown() {
        harness.close();
        snow.close();
    }

    /** Polls like the worker does: a RetriableException is logged and polling continues. */
    private List<SourceRecord> pollTolerating(int n, long timeoutMs) throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        int retriable = 0;
        while (out.size() < n && System.currentTimeMillis() < deadline) {
            try {
                out.addAll(harness.poll());
            } catch (RetriableException e) {
                retriable++;
            }
        }
        assertThat(retriable).as("retriable failures seen while polling").isGreaterThanOrEqualTo(0);
        return out;
    }

    @Test
    void unauthorizedOnceIsRefreshedAndTheRequestSucceeds() throws Exception {
        snow.faults().unauthorizedOnce();
        harness.start();
        assertThat(harness.pollUntil(3, 5_000)).hasSize(3);
    }

    @Test
    void aSecondUnauthorizedFails() {
        snow.faults().unauthorizedTimes(2);
        assertThatThrownBy(harness::start)
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasMessageContaining("401")
                .hasMessageContaining("incident");
    }

    @Test
    void forbiddenTableFailsAtStartNamingTheTable() {
        snow.faults().forbidTable("incident");
        assertThatThrownBy(harness::start)
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasMessageContaining("incident")
                .hasMessageContaining("403")
                .hasMessageContaining("ACL");
    }

    @Test
    void forbiddenTableFailsAtPollNamingTheTableWhenTheProbeIsOff() throws Exception {
        snow.faults().forbidTable("incident");
        harness.with(SourceConfig.STARTUP_PROBE, "false");
        harness.start();
        assertThatThrownBy(() -> harness.pollFor(1_000))
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasMessageContaining("incident")
                .hasMessageContaining("403");
    }

    @Test
    void missingTableFailsWith404() {
        snow.faults().notFoundTable("incident");
        assertThatThrownBy(harness::start)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("404")
                .hasMessageContaining("incident");
    }

    @Test
    void rateLimitIsRetriedThenSucceeds() throws Exception {
        snow.faults().rateLimit(2, Duration.ZERO);
        harness.start();
        assertThat(harness.pollUntil(3, 10_000)).hasSize(3);
        assertThat(snow.journal().count("GET", "incident")).isGreaterThanOrEqualTo(2);
    }

    @Test
    void serverErrorsAreRetriedThenSucceed() throws Exception {
        snow.faults().serverError(2, 503);
        harness.start();
        assertThat(harness.pollUntil(3, 10_000)).hasSize(3);
        snow.faults().serverError(3, 502);
        snow.tables().insert("incident", Fixtures.row(7, now.minusSeconds(40)));
        assertThat(harness.pollUntil(1, 10_000))
                .extracting(SourceRecord::key)
                .containsExactly(Fixtures.sysId(7));
    }

    @Test
    void exhaustedRetriesSurfaceAsRetriableAndThePollerRecovers() throws Exception {
        harness.with(SourceConfig.STARTUP_PROBE, "false");
        snow.faults().rateLimit(10, Duration.ZERO);
        harness.start();
        assertThatThrownBy(harness::poll)
                .isInstanceOf(RetriableException.class)
                .hasMessageContaining("incident");
        assertThat(pollTolerating(3, 10_000)).hasSize(3);
        assertThat(harness.task().metrics().get("incident").retries()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void malformedJsonIsRetriableAndTheNextPollSucceeds() throws Exception {
        harness.with(SourceConfig.STARTUP_PROBE, "false");
        snow.faults().malformedJsonOnce();
        harness.start();
        assertThatThrownBy(harness::poll)
                .isInstanceOf(RetriableException.class)
                .hasMessageContaining("incident");
        assertThat(harness.pollUntil(3, 5_000)).hasSize(3);
    }

    private void insertRowWithGarbageTimestamp() {
        Map<String, String> bad = Fixtures.row(9, now.minusSeconds(49));
        bad.put("sys_updated_on", SnowTimestamp.format(now.minusSeconds(49)) + " garbage");
        snow.tables().insert("incident", bad);
    }

    @Test
    void invalidTimestampRowFailsNamingTheRow() throws Exception {
        insertRowWithGarbageTimestamp();
        harness.with(SourceConfig.STARTUP_PROBE, "false");
        harness.start();
        assertThatThrownBy(() -> harness.pollFor(1_000))
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasMessageContaining(Fixtures.sysId(9))
                .hasMessageContaining("garbage")
                .hasMessageContaining("incident")
                .hasMessageContaining("snow.source.bad.row.behavior=skip");
    }

    @Test
    void invalidTimestampRowIsSkippedAndCountedUnderSkip() throws Exception {
        insertRowWithGarbageTimestamp();
        harness.with(SourceConfig.STARTUP_PROBE, "false")
                .with(SourceConfig.BAD_ROW_BEHAVIOR, "skip");
        harness.start();
        List<SourceRecord> records = harness.pollUntil(3, 5_000);
        assertThat(records).extracting(SourceRecord::key).doesNotContain(Fixtures.sysId(9));
        assertThat(records).hasSize(3);
        assertThat(harness.pollFor(400)).isEmpty();
        TableMetrics m = harness.task().metrics().get("incident");
        assertThat(m.rowsSkipped()).isGreaterThanOrEqualTo(1);
        assertThat(m.recordsEmitted()).isEqualTo(3);
    }

    @Test
    void hiddenCursorFieldIsCaughtByTheProbe() {
        snow.faults().hideField("incident", "sys_updated_on");
        assertThatThrownBy(harness::start)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("sys_updated_on")
                .hasMessageContaining("incident")
                .hasMessageContaining("ACL");
    }
}
