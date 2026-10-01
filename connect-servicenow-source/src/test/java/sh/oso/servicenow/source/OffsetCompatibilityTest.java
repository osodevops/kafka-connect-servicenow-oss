package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.cursor.Cursor;
import sh.oso.servicenow.cursor.SourceOffset;
import sh.oso.servicenow.cursor.SourcePartition;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

class OffsetCompatibilityTest {

    MockServiceNowServer snow;
    Instant now;
    TaskHarness harness;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
        now = snow.clock().instant();
        Fixtures.seed(snow.tables(), "incident", 10, now.minusSeconds(100), 1);
        harness = new TaskHarness(snow);
    }

    @AfterEach
    void tearDown() {
        harness.close();
        snow.close();
    }

    private Map<String, String> partitionFor(Map<String, String> props) {
        TableSpec spec = new SourceConfig(props).tables().get(0);
        return SourcePartition.of(
                "localhost", spec.name(), spec.fingerprint(), spec.timestampField());
    }

    @Test
    void changingTheQueryCreatesANewPartitionAndBackfillsAgain() throws Exception {
        harness.start();
        List<SourceRecord> first = harness.pollUntil(10, 5_000);
        assertThat(first).hasSize(10);
        Map<String, ?> partition1 = first.get(0).sourcePartition();
        assertThat(harness.pollFor(300)).isEmpty();

        harness.with(TableSpec.key("t1", TableSpec.QUERY), "priority>0");
        harness.restart();
        List<SourceRecord> second = harness.pollUntil(10, 5_000);
        assertThat(second).hasSize(10);
        Map<String, ?> partition2 = second.get(0).sourcePartition();
        assertThat(partition2).isNotEqualTo(partition1);
        assertThat(partition2.get("query_fingerprint"))
                .isNotEqualTo(partition1.get("query_fingerprint"));
        assertThat(partition2.get("table")).isEqualTo("incident");
        assertThat(harness.task().pollers().get(0).resumed()).isFalse();
        assertThat(harness.committedOffsets()).containsKeys(partition1, partition2);
    }

    @Test
    void storedOffsetWithAnotherFingerprintIsRefusedWithADiagnostic() {
        Map<String, String> partition = partitionFor(harness.props());
        String configured = partition.get("query_fingerprint");
        Map<String, Object> offset =
                new HashMap<>(
                        SourceOffset.of(
                                        new Cursor(now.minusSeconds(50), ""),
                                        "stream",
                                        "sha256:deadbeef")
                                .toMap());
        harness.seedOffset(partition, offset);

        assertThatThrownBy(harness::start)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("incident")
                .hasMessageContaining("sha256:deadbeef")
                .hasMessageContaining(configured)
                .hasMessageContaining("query_fingerprint")
                .hasMessageContaining("reset");
    }

    @Test
    void offsetFormatVersionTwoIsRejected() {
        Map<String, String> partition = partitionFor(harness.props());
        Map<String, Object> offset = new HashMap<>();
        offset.put("version", "2");
        offset.put("timestamp", "2026-01-01 00:00:00");
        offset.put("sys_id", "");
        offset.put("phase", "stream");
        offset.put("fingerprint", partition.get("query_fingerprint"));
        harness.seedOffset(partition, offset);

        assertThatThrownBy(harness::start)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("version 2")
                .hasMessageContaining("incident");
    }

    @Test
    void offsetMissingFieldsIsRejected() {
        Map<String, String> partition = partitionFor(harness.props());
        harness.seedOffset(partition, Map.of("version", "1"));
        assertThatThrownBy(harness::start)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void matchingOffsetResumesFromTheCommittedCursorMinusTheOverlap() throws Exception {
        Map<String, String> partition = partitionFor(harness.props());
        Instant ts5 = now.minusSeconds(95);
        Map<String, Object> offset =
                new HashMap<>(
                        SourceOffset.of(
                                        new Cursor(ts5, Fixtures.sysId(5)),
                                        "backfill",
                                        partition.get("query_fingerprint"))
                                .toMap());
        harness.seedOffset(partition, offset);
        harness.start();

        List<SourceRecord> records = harness.pollUntil(6, 5_000);
        assertThat(records)
                .extracting(SourceRecord::key)
                .containsExactly(
                        Fixtures.sysId(4),
                        Fixtures.sysId(5),
                        Fixtures.sysId(6),
                        Fixtures.sysId(7),
                        Fixtures.sysId(8),
                        Fixtures.sysId(9));
        assertThat(harness.task().pollers().get(0).resumed()).isTrue();
        assertThat(harness.pollFor(300)).isEmpty();
    }
}
