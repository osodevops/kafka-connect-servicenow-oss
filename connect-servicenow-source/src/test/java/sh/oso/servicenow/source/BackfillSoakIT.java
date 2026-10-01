package sh.oso.servicenow.source;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.QueryParameter;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.config.CoreConfigDefs;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

/**
 * One-million-row backfill in a 256 MB heap ({@code -Psoak}). The in-memory fake cannot hold a
 * million rows inside that heap next to the connector, so this test serves a synthetic Table API
 * that answers the two keyset query shapes of ADR 0001 from a formula: row {@code i} has {@code
 * sys_updated_on = T0 + i / PER_SECOND} and {@code sys_id = a + hex(i)}. What is measured is the
 * connector side: every row emitted exactly once, the dedup cache bounded, nothing retained.
 */
class BackfillSoakIT {

    private static final Logger LOG = LoggerFactory.getLogger(BackfillSoakIT.class);

    static final int ROWS = Integer.getInteger("soak.rows", 1_000_000);
    static final int PER_SECOND = 2_000;
    static final int BATCH = 10_000;
    static final int DEDUP_WINDOW = 50_000;
    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    static final Instant SERVER_NOW = T0.plusSeconds(ROWS / PER_SECOND + 100);

    @Test
    void backfillsOneMillionRowsExactlyOnceInABoundedHeap() throws Exception {
        ConcurrencyLimiter.clearRegistry();
        LOG.info(
                "Soak: {} rows, heap max {} MB",
                ROWS,
                Runtime.getRuntime().maxMemory() / (1024 * 1024));
        WireMockServer server =
                new WireMockServer(
                        WireMockConfiguration.options()
                                .dynamicPort()
                                .extensions(new SyntheticTableApi())
                                .containerThreads(8)
                                .maxRequestJournalEntries(10));
        server.start();
        server.stubFor(
                any(urlPathMatching("/api/now/.*"))
                        .willReturn(aResponse().withTransformers(SyntheticTableApi.NAME)));
        try {
            Map<String, String> props = new HashMap<>();
            props.put(CoreConfigDefs.URL, server.baseUrl());
            props.put(CoreConfigDefs.AUTH_TYPE, CoreConfigDefs.AUTH_TYPE_BASIC);
            props.put(CoreConfigDefs.AUTH_USERNAME, "connect");
            props.put(CoreConfigDefs.AUTH_PASSWORD, "secret");
            props.putAll(MockServiceNowServer.fastRetryProps());
            props.put(SourceConfig.TABLES, "t1");
            props.put(TableSpec.key("t1", TableSpec.NAME), "incident");
            props.put(TableSpec.key("t1", TableSpec.TOPIC), "snow.incident");
            props.put(SourceConfig.BATCH_SIZE, Integer.toString(BATCH));
            props.put(SourceConfig.DEDUP_WINDOW_RECORDS, Integer.toString(DEDUP_WINDOW));
            props.put(SourceConfig.POLL_INTERVAL_MS, "100");

            try (TaskHarness harness = new TaskHarness(props)) {
                harness.start();
                BitSet seen = new BitSet(ROWS);
                long count = 0;
                long duplicates = 0;
                long polls = 0;
                long started = System.currentTimeMillis();
                long deadline = started + 20 * 60_000L;
                while (count < ROWS && System.currentTimeMillis() < deadline) {
                    List<SourceRecord> page = harness.poll();
                    polls++;
                    for (SourceRecord r : page) {
                        int i = index((String) r.key());
                        if (seen.get(i)) {
                            duplicates++;
                        } else {
                            seen.set(i);
                            count++;
                        }
                    }
                    if (polls % 50 == 0) {
                        LOG.info(
                                "Soak: {} rows after {} polls, {} MB used",
                                count,
                                polls,
                                (Runtime.getRuntime().totalMemory()
                                                - Runtime.getRuntime().freeMemory())
                                        / (1024 * 1024));
                    }
                }
                LOG.info(
                        "Soak: {} rows in {} ms ({} polls, {} duplicates)",
                        count,
                        System.currentTimeMillis() - started,
                        polls,
                        duplicates);
                assertThat(count).isEqualTo(ROWS);
                assertThat(seen.cardinality()).isEqualTo(ROWS);
                assertThat(duplicates).isZero();
                TablePoller poller = harness.task().pollers().get(0);
                assertThat(poller.dedupSize()).isLessThanOrEqualTo(DEDUP_WINDOW);
                assertThat(harness.task().metrics().get("incident").recordsEmitted())
                        .isEqualTo(ROWS);
                assertThat(harness.pollFor(300)).isEmpty();
            }
        } finally {
            server.stop();
        }
    }

    static int index(String sysId) {
        return Integer.parseInt(sysId.substring(1), 16);
    }

    /** A formula-backed Table API: no stored rows, just the two keyset query shapes. */
    static final class SyntheticTableApi implements ResponseDefinitionTransformerV2 {

        static final String NAME = "synthetic-table-api";
        private static final Pattern TERM = Pattern.compile("^([A-Za-z0-9_]+)(<=|>=|=|>|<)(.*)$");
        private static final DateTimeFormatter HTTP_DATE =
                DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneOffset.UTC);

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public boolean applyGlobally() {
            return false;
        }

        @Override
        public ResponseDefinition transform(ServeEvent serveEvent) {
            Request request = serveEvent.getRequest();
            if (request.getUrl().startsWith("/api/now/table/sys_user")) {
                return json("{\"result\":[]}");
            }
            String query = param(request, "sysparm_query");
            int limit =
                    Integer.parseInt(
                            param(request, "sysparm_limit") == null
                                    ? "1000"
                                    : param(request, "sysparm_limit"));
            Instant tsEq = null;
            Instant tsGt = null;
            Instant tsLe = null;
            String idGt = null;
            if (query != null && !query.isBlank()) {
                for (String term : query.split("\\^")) {
                    if (term.startsWith("ORDERBY")) {
                        continue;
                    }
                    Matcher m = TERM.matcher(term);
                    if (!m.matches()) {
                        throw new IllegalArgumentException("unexpected term " + term);
                    }
                    String field = m.group(1);
                    String op = m.group(2);
                    String value = m.group(3);
                    if (field.equals("sys_updated_on")) {
                        switch (op) {
                            case "=" -> tsEq = SnowTimestamp.parse(value);
                            case ">" -> tsGt = SnowTimestamp.parse(value);
                            case "<=" -> tsLe = SnowTimestamp.parse(value);
                            default -> throw new IllegalArgumentException("unexpected op " + term);
                        }
                    } else if (field.equals("sys_id") && op.equals(">")) {
                        idGt = value;
                    } else {
                        throw new IllegalArgumentException("unexpected term " + term);
                    }
                }
            }
            int from;
            int end;
            if (tsEq != null) {
                long k = tsEq.getEpochSecond() - T0.getEpochSecond();
                int bucketStart = (int) Math.max(0, Math.min(ROWS, k * PER_SECOND));
                int bucketEnd = (int) Math.max(0, Math.min(ROWS, (k + 1) * PER_SECOND));
                from =
                        idGt == null || idGt.isEmpty()
                                ? bucketStart
                                : Math.max(bucketStart, index(idGt) + 1);
                end = bucketEnd;
            } else if (tsGt != null) {
                long k = tsGt.getEpochSecond() - T0.getEpochSecond() + 1;
                from = (int) Math.max(0, Math.min(ROWS, k * PER_SECOND));
                long hiK =
                        tsLe == null
                                ? Long.MAX_VALUE / PER_SECOND
                                : tsLe.getEpochSecond() - T0.getEpochSecond();
                end = (int) Math.max(0, Math.min(ROWS, (hiK + 1) * PER_SECOND));
            } else {
                from = 0;
                end = ROWS;
            }
            int to = (int) Math.min(end, (long) from + limit);
            StringBuilder sb = new StringBuilder(Math.max(64, (to - from) * 140));
            sb.append("{\"result\":[");
            for (int i = from; i < to; i++) {
                if (i > from) {
                    sb.append(',');
                }
                sb.append("{\"sys_id\":\"").append(Fixtures.sysId(i)).append('"');
                sb.append(",\"sys_updated_on\":\"")
                        .append(SnowTimestamp.format(T0.plusSeconds(i / PER_SECOND)))
                        .append('"');
                sb.append(",\"sys_mod_count\":\"0\"");
                sb.append(",\"short_description\":\"row ").append(i).append("\"}");
            }
            sb.append("]}");
            return json(sb.toString());
        }

        private static ResponseDefinition json(String body) {
            return ResponseDefinitionBuilder.responseDefinition()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withHeader("Date", HTTP_DATE.format(SERVER_NOW))
                    .withBody(body)
                    .build();
        }

        private static String param(Request request, String name) {
            QueryParameter p = request.queryParameter(name);
            return p != null && p.isPresent() ? p.firstValue() : null;
        }
    }
}
