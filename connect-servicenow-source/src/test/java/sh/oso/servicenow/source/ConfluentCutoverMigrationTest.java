package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
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
import sh.oso.servicenow.testing.MutableClock;

/**
 * Executable proof of the documented Confluent to OSS cutover (docs: /migration/confluent): stop
 * the old connector, note its last successful poll, start this source with {@code start.timestamp =
 * last old poll minus a margin}. The guarantee under test: <b>no row version is lost across the
 * cutover</b>; rows the old connector already delivered are re-delivered only inside the margin
 * window, and offsets stay monotonic.
 *
 * <p>Every run writes a machine-readable evidence report to {@code target/migration-evidence.json}
 * (uploaded as a CI artefact).
 */
class ConfluentCutoverMigrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static final int OLD_ROWS = 200;
    static final int GAP_INSERTS = 50;
    static final int GAP_UPDATES = 20;
    static final long MARGIN_SECONDS = 60;

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
    void cutoverLosesNothingAndDuplicatesStayInsideTheMargin() throws Exception {
        // ---- Phase 1: the "Confluent era". Rows the old connector already delivered to Kafka.
        Instant oldBase = now.minusSeconds(900);
        Fixtures.seed(snow.tables(), "incident", OLD_ROWS, oldBase, 1); // ts now-900 .. now-701
        Instant lastOldPoll = now.minusSeconds(700);
        Set<String> deliveredByOldConnector = new HashSet<>();
        Map<String, Instant> oldTimestamps = new LinkedHashMap<>();
        for (Map<String, String> row : snow.tables().all("incident")) {
            deliveredByOldConnector.add(Fixtures.versionKey(row));
            oldTimestamps.put(row.get("sys_id"), SnowTimestamp.parse(row.get("sys_updated_on")));
        }

        // ---- Phase 2: the cutover gap. The old connector is stopped; ServiceNow keeps changing.
        Instant gapBase = now.minusSeconds(600);
        for (int i = 0; i < GAP_INSERTS; i++) {
            snow.tables().insert("incident", Fixtures.row(OLD_ROWS + i, gapBase.plusSeconds(i)));
        }
        clock.set(now.minusSeconds(500));
        for (int i = 0; i < GAP_UPDATES; i++) {
            snow.tables()
                    .update(
                            "incident",
                            Fixtures.sysId(i),
                            Map.of("short_description", "changed in gap"));
        }
        clock.set(now);
        Set<String> changedDuringGap = new HashSet<>();
        for (Map<String, String> row : snow.tables().all("incident")) {
            if (!deliveredByOldConnector.contains(Fixtures.versionKey(row))) {
                changedDuringGap.add(Fixtures.versionKey(row));
            }
        }
        assertThat(changedDuringGap).hasSize(GAP_INSERTS + GAP_UPDATES);

        // ---- Phase 3: the OSS connector starts from the last old poll minus the margin.
        Instant start = lastOldPoll.minusSeconds(MARGIN_SECONDS);
        harness.with(TableSpec.key("t1", TableSpec.START_TIMESTAMP), SnowTimestamp.format(start))
                .with(SourceConfig.BATCH_SIZE, "37");
        harness.start();
        long expectedOverlap =
                oldTimestamps.entrySet().stream()
                        .filter(e -> !e.getValue().isBefore(start))
                        .filter(e -> Integer.parseInt(e.getKey().substring(1), 16) >= GAP_UPDATES)
                        .count();
        int expected = (int) expectedOverlap + GAP_INSERTS + GAP_UPDATES;
        List<SourceRecord> records = harness.pollUntil(expected, 20_000);
        assertThat(harness.pollFor(400)).isEmpty();

        // ---- Verification.
        Set<String> seen = new HashSet<>();
        records.forEach(r -> seen.add(Fixtures.versionKey(r)));
        // 1. Nothing lost: every version created or changed while no connector ran is present.
        assertThat(seen).containsAll(changedDuringGap);
        // 2. Nothing stale: every emitted version is the row's current version.
        Set<String> current = new HashSet<>();
        snow.tables().all("incident").forEach(r -> current.add(Fixtures.versionKey(r)));
        assertThat(seen).isSubsetOf(current);
        // 3. Duplicates are exactly the old-era rows inside [start, lastOldPoll].
        List<SourceRecord> duplicates =
                records.stream()
                        .filter(r -> deliveredByOldConnector.contains(Fixtures.versionKey(r)))
                        .toList();
        assertThat(duplicates).hasSize((int) expectedOverlap);
        for (SourceRecord d : duplicates) {
            Instant ts = SnowTimestamp.parse(Fixtures.field(d, "sys_updated_on"));
            assertThat(ts).isAfterOrEqualTo(start).isBeforeOrEqualTo(lastOldPoll);
        }
        assertThat(records).hasSize(expected);
        // 4. Offsets are monotonic across the whole cutover.
        assertThat(Fixtures.offsetsMonotonic(records)).isTrue();

        writeEvidence(
                records,
                deliveredByOldConnector,
                changedDuringGap,
                duplicates.size(),
                start,
                lastOldPoll);
    }

    private void writeEvidence(
            List<SourceRecord> records,
            Set<String> preCutover,
            Set<String> gap,
            int duplicates,
            Instant start,
            Instant lastOldPoll)
            throws Exception {
        ObjectNode evidence = MAPPER.createObjectNode();
        evidence.put("report", "confluent-cutover-migration-evidence");
        evidence.put("schemaVersion", 1);
        evidence.put("generatedAt", Instant.now().toString());
        evidence.put(
                "procedure",
                "stop the Confluent connector -> note its last successful poll -> start"
                        + " sh.oso.servicenow.source.ServiceNowSourceConnector with"
                        + " snow.table.<alias>.start.timestamp = last old poll - margin");
        evidence.put("lastOldPoll", SnowTimestamp.format(lastOldPoll));
        evidence.put("marginSeconds", MARGIN_SECONDS);
        evidence.put("startTimestamp", SnowTimestamp.format(start));
        evidence.put("rowVersionsDeliveredByOldConnector", preCutover.size());
        evidence.put("rowVersionsChangedDuringGap", gap.size());
        evidence.put("recordsDeliveredAfterCutover", records.size());
        evidence.put("duplicates", duplicates);
        evidence.put("lost", 0);
        evidence.put(
                "verdict",
                "PASS: zero loss; duplicates limited to old-era rows inside [start, last old poll]");
        ArrayNode delivered = evidence.putArray("delivered");
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        for (SourceRecord record : records) {
            ObjectNode entry = delivered.addObject();
            entry.put("sys_id", (String) record.key());
            entry.put("sys_updated_on", Fixtures.field(record, "sys_updated_on"));
            entry.put("sys_mod_count", Fixtures.field(record, "sys_mod_count"));
            entry.put("offsetTimestamp", Fixtures.offsetTimestamp(record));
            entry.put("duplicateOfOldEra", preCutover.contains(Fixtures.versionKey(record)));
            entry.put(
                    "valueSha256",
                    HexFormat.of()
                            .formatHex(
                                    sha256.digest(
                                            String.valueOf(record.value())
                                                    .getBytes(StandardCharsets.UTF_8))));
        }
        ArrayNode mapping = evidence.putArray("keyMapping");
        keyMapping()
                .forEach(
                        (confluent, oss) -> {
                            ObjectNode m = mapping.addObject();
                            m.put("confluent", confluent);
                            m.put("oss", oss);
                        });
        Path out = Path.of("target", "migration-evidence.json");
        Files.createDirectories(out.getParent());
        Files.writeString(
                out, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
        assertThat(Files.size(out)).isGreaterThan(0);
    }

    /** The Confluent (Platform source, Cloud Source V2) to OSS key mapping, as documented. */
    static Map<String, String> keyMapping() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(
                "connector.class=io.confluent.connect.servicenow.ServiceNowSourceConnector",
                "connector.class=sh.oso.servicenow.source.ServiceNowSourceConnector");
        m.put("servicenow.url", "snow.url");
        m.put("servicenow.username", "snow.auth.username (snow.auth.type=basic)");
        m.put("servicenow.password", "snow.auth.password");
        m.put("servicenow.table / table{i}.name", "snow.table.<alias>.name");
        m.put("kafka.topic / table{i}.topic", "snow.table.<alias>.topic");
        m.put("servicenow.since / table{i}.start.timestamp", "snow.table.<alias>.start.timestamp");
        m.put("batch.max.rows / table{i}.batch.size", "snow.table.<alias>.batch.size");
        m.put(
                "poll.interval.s / table{i}.request.interval.ms",
                "snow.table.<alias>.poll.interval.ms");
        m.put("table{i}.timestamp.field", "snow.table.<alias>.timestamp.field");
        m.put(
                "servicenow.view.variable.prefix",
                "snow.table.<alias>.timestamp.field + snow.table.<alias>.sys.id.field");
        m.put(
                "table{i}.pagination.query",
                "snow.table.<alias>.query (MANUAL when it uses ${offset})");
        m.put("table{i}.allowlisted.fields", "snow.table.<alias>.fields");
        m.put("table{i}.display.value", "snow.table.<alias>.display.value");
        m.put("table{i}.exclude.reference.link", "snow.table.<alias>.exclude.reference.link");
        m.put("table{i}.query.category", "snow.table.<alias>.query.category");
        m.put("table{i}.query.domain", "snow.table.<alias>.query.domain");
        m.put("auth.type=BASIC / OAUTH2", "snow.auth.type=basic / oauth2");
        m.put("retry.max.times", "snow.retry.max.attempts");
        m.put(
                "connection.timeout.ms / request.timeout.ms",
                "snow.http.connect.timeout.ms / snow.http.request.timeout.ms");
        m.put("proxy.url", "snow.http.proxy.url");
        m.put(
                "header snow.operation (not emitted by the source)",
                "header snow.source.operation=UPSERT");
        return m;
    }
}
