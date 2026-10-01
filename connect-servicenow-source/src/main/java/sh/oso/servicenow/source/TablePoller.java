package sh.oso.servicenow.source;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.common.IncompatibleOffsetException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.cursor.Cursor;
import sh.oso.servicenow.cursor.DedupCache;
import sh.oso.servicenow.cursor.DedupKey;
import sh.oso.servicenow.cursor.KeysetQueryBuilder;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.cursor.SourceOffset;
import sh.oso.servicenow.cursor.SourcePartition;
import sh.oso.servicenow.cursor.Watermark;
import sh.oso.servicenow.http.HttpStats;
import sh.oso.servicenow.schema.RecordSchemaMapper;
import sh.oso.servicenow.table.DisplayValue;
import sh.oso.servicenow.table.EncodedQuery;
import sh.oso.servicenow.table.FieldValue;
import sh.oso.servicenow.table.Page;
import sh.oso.servicenow.table.QueryRequest;
import sh.oso.servicenow.table.Record;
import sh.oso.servicenow.table.TableApiClient;

/**
 * The keyset poller for one table (ADR 0001). Each {@link #poll()} performs at most one Table API
 * request and returns the records of that page.
 *
 * <pre>
 *   INIT          decode the stored offset (refuse an incompatible one), rewind by the overlap
 *   beginSweep    sweepHi = serverNow - safetyLag; last.ts &gt; sweepHi ? IDLE : DRAIN or NEXT
 *   DRAIN_BUCKET  base ^ ts=T ^ sys_id&gt;S ^ ORDERBYsys_id          short page -&gt; NEXT_BUCKETS
 *   NEXT_BUCKETS  base ^ ts&gt;T ^ ts&lt;=sweepHi ^ ORDERBYts ^ ORDERBYsys_id
 *                 full page -&gt; DRAIN_BUCKET; short page -&gt; caught up ? IDLE : beginSweep()
 *   IDLE          no request until wakeAt; then rewind by the overlap and beginSweep()
 * </pre>
 *
 * Every row advances the cursor; a row version already in the {@link DedupCache} advances the
 * cursor without being emitted. Rows out of cursor order fail the table.
 */
final class TablePoller {

    private static final Logger LOG = LoggerFactory.getLogger(TablePoller.class);

    enum State {
        INIT,
        DRAIN_BUCKET,
        NEXT_BUCKETS,
        IDLE
    }

    private final TableSpec spec;
    private final TableApiClient api;
    private final HttpStats httpStats;
    private final Watermark watermark;
    private final Clock localClock;
    private final KeysetQueryBuilder queries;
    private final List<String> projection;
    private final DisplayValue wireDisplay;
    private final DedupCache dedup;
    private final SourceRecordFactory records;
    private final TableMetrics metrics;
    private final Map<String, String> partition;
    private final String fingerprint;
    private final Duration overlap;
    private final Duration safetyLag;
    private final Duration pollInterval;
    private final SourceConfig.BadRowBehavior badRow;

    private Cursor last;

    /** Last row observed (or the committed cursor on resume); overlap rewinds start here. */
    private Cursor anchor;

    private Instant sweepHi;
    private String phase = SourceOffset.PHASE_BACKFILL;
    private State state = State.INIT;
    private Instant wakeAt = Instant.EPOCH;
    private boolean resumed;

    TablePoller(
            TableSpec spec,
            SourceConfig config,
            ServiceNowClient client,
            Map<String, ?> storedOffset,
            Clock localClock) {
        this.spec = Objects.requireNonNull(spec, "spec");
        this.api = client.tableApi();
        this.httpStats = client.http().stats();
        this.watermark = new Watermark(client.serverClock());
        this.localClock = localClock == null ? Clock.systemUTC() : localClock;
        this.queries =
                new KeysetQueryBuilder(
                        EncodedQuery.of(spec.query()), spec.timestampField(), spec.sysIdField());
        this.projection = queries.projection(spec.fields());
        this.wireDisplay =
                spec.displayValue() == DisplayValue.TRUE ? DisplayValue.ALL : spec.displayValue();
        this.dedup = new DedupCache(config.dedupWindowRecords());
        this.metrics = new TableMetrics(spec.name());
        this.fingerprint = spec.fingerprint();
        String instanceHost = client.config().instanceHost();
        this.partition =
                SourcePartition.of(instanceHost, spec.name(), fingerprint, spec.timestampField());
        this.records =
                new SourceRecordFactory(
                        spec,
                        instanceHost,
                        partition,
                        new RecordSchemaMapper(
                                config.schemaMode(),
                                "sh.oso.servicenow." + spec.name(),
                                config.typedFields()),
                        config.emitEnvelope(),
                        this.localClock);
        this.overlap = config.overlap();
        this.safetyLag = config.safetyLag();
        this.pollInterval = Duration.ofMillis(spec.pollIntervalMs());
        this.badRow = config.badRowBehavior();
        this.last = initialCursor(storedOffset);
        metrics.phase(phase);
    }

    private Cursor initialCursor(Map<String, ?> stored) {
        if (stored == null || stored.isEmpty()) {
            Cursor start = new Cursor(spec.startTimestamp().minusSeconds(1), "");
            LOG.info(
                    "Table '{}' (alias {}): no stored offset, backfilling from {} on partition {}",
                    spec.name(),
                    spec.alias(),
                    SnowTimestamp.format(spec.startTimestamp()),
                    partition);
            return start;
        }
        SourceOffset offset;
        try {
            offset = SourceOffset.fromMap(stored);
        } catch (IncompatibleOffsetException e) {
            throw new ConnectException(
                    "Table '"
                            + spec.name()
                            + "' (alias "
                            + spec.alias()
                            + "): the stored offset for partition "
                            + partition
                            + " cannot be used: "
                            + e.getMessage()
                            + ". Reset or rewrite the offset (stored: "
                            + stored
                            + ")",
                    e);
        }
        if (!fingerprint.equals(offset.fingerprint())) {
            throw new ConnectException(
                    "Table '"
                            + spec.name()
                            + "' (alias "
                            + spec.alias()
                            + "): the stored offset carries query fingerprint "
                            + offset.fingerprint()
                            + " but the configured query has fingerprint "
                            + fingerprint
                            + " for partition "
                            + partition
                            + ". The offset store was edited or the configuration changed"
                            + " underneath it; reset the offset instead of reusing it");
        }
        resumed = true;
        Cursor committed = offset.cursor();
        anchor = committed;
        Cursor rewound = new Cursor(committed.ts().minus(overlap), "");
        LOG.info(
                "Table '{}' (alias {}): resuming from committed cursor {} (phase {}) re-reading"
                        + " from {} on partition {}",
                spec.name(),
                spec.alias(),
                committed,
                offset.phase(),
                rewound,
                partition);
        return rewound;
    }

    // ---------------------------------------------------------------- polling

    /** One unit of work: at most one request; the page's records, or nothing while idle. */
    List<SourceRecord> poll() {
        long t0 = System.nanoTime();
        // The task polls its tables one request at a time on one thread, so the throttled time
        // the shared HTTP client accumulates during this poll belongs to this table.
        long throttledBefore = httpStats.throttledMillis();
        try {
            if (state == State.INIT) {
                beginSweep();
            }
            if (state == State.IDLE) {
                if (localClock.instant().isBefore(wakeAt)) {
                    return List.of();
                }
                rewindForOverlap();
                beginSweep();
                if (state == State.IDLE) {
                    return List.of();
                }
            }
            return switch (state) {
                case DRAIN_BUCKET -> drainBucket();
                case NEXT_BUCKETS -> nextBuckets();
                default -> List.of();
            };
        } catch (ServiceNowException e) {
            if (e.isRetryable()) {
                metrics.retry();
            }
            throw e;
        } finally {
            metrics.throttled(httpStats.throttledMillis() - throttledBefore);
            metrics.pollDuration((System.nanoTime() - t0) / 1_000_000L);
        }
    }

    private void rewindForOverlap() {
        if (overlap.isZero()) {
            return;
        }
        if (anchor == null) {
            return; // nothing observed yet: stay at the start cursor, never drift below it
        }
        last = new Cursor(anchor.ts().minus(overlap), "");
    }

    private void beginSweep() {
        sweepHi = watermark.hi(safetyLag);
        boolean nothingBefore =
                last.ts().isAfter(sweepHi) || (last.ts().equals(sweepHi) && last.sysId().isEmpty());
        if (nothingBefore) {
            LOG.debug(
                    "Table '{}': cursor {} is at or past the high-water mark {}; idle",
                    spec.name(),
                    last,
                    SnowTimestamp.format(sweepHi));
            idle();
            return;
        }
        state = last.sysId().isEmpty() ? State.NEXT_BUCKETS : State.DRAIN_BUCKET;
        LOG.debug(
                "Table '{}': sweep from {} up to {} ({})",
                spec.name(),
                last,
                SnowTimestamp.format(sweepHi),
                state);
    }

    private List<SourceRecord> drainBucket() {
        Page<Record> page = fetch(queries.drainBucket(last));
        List<SourceRecord> out = emit(page);
        if (!page.isFull()) {
            state = State.NEXT_BUCKETS;
        }
        return out;
    }

    private List<SourceRecord> nextBuckets() {
        Page<Record> page = fetch(queries.nextBuckets(last.ts(), sweepHi));
        List<SourceRecord> out = emit(page);
        if (page.isFull()) {
            state = State.DRAIN_BUCKET;
        } else {
            endSweep();
        }
        return out;
    }

    private void endSweep() {
        Instant serverNow = watermark.now();
        Instant threshold =
                serverNow.minus(safetyLag).minus(pollInterval).truncatedTo(ChronoUnit.SECONDS);
        boolean caughtUp = !sweepHi.isBefore(threshold);
        if (caughtUp) {
            if (!SourceOffset.PHASE_STREAM.equals(phase)) {
                LOG.info(
                        "Table '{}' (alias {}): caught up at {}; now streaming every {} ms",
                        spec.name(),
                        spec.alias(),
                        SnowTimestamp.format(sweepHi),
                        pollInterval.toMillis());
            }
            phase = SourceOffset.PHASE_STREAM;
            metrics.phase(phase);
            idle();
        } else {
            phase = SourceOffset.PHASE_BACKFILL;
            metrics.phase(phase);
            beginSweep();
        }
    }

    private void idle() {
        state = State.IDLE;
        wakeAt = localClock.instant().plus(pollInterval);
    }

    private Page<Record> fetch(EncodedQuery query) {
        QueryRequest request =
                QueryRequest.builder(spec.name())
                        .query(query)
                        .fields(projection)
                        .limit(spec.batchSize())
                        .displayValue(wireDisplay)
                        .excludeReferenceLink(spec.excludeReferenceLink())
                        .queryCategory(spec.queryCategory())
                        .queryNoDomain(!spec.queryDomain())
                        .build();
        LOG.debug("Table '{}': {}", spec.name(), request);
        Page<Record> page = api.list(request);
        metrics.page();
        metrics.requestSucceeded(localClock.millis());
        return page;
    }

    private List<SourceRecord> emit(Page<Record> page) {
        List<SourceRecord> out = new ArrayList<>(page.size());
        Cursor before = last;
        for (Record raw : page.items()) {
            Record row = wireDisplay == spec.displayValue() ? raw : displayValues(raw);
            Cursor cursor;
            try {
                cursor = cursorOf(row);
            } catch (ServiceNowException e) {
                handleBadRow(row, e);
                continue;
            }
            if (!cursor.isAfter(last)) {
                throw new ConnectException(
                        "Table '"
                                + spec.name()
                                + "' (alias "
                                + spec.alias()
                                + "): rows arrived out of cursor order: "
                                + cursor
                                + " is not after "
                                + last
                                + ". The instance did not honour ORDERBY"
                                + spec.timestampField()
                                + "^ORDERBY"
                                + spec.sysIdField()
                                + ", or the integration user's time zone is not UTC");
            }
            last = cursor;
            DedupKey key = DedupKey.of(cursor, row.string(KeysetQueryBuilder.SYS_MOD_COUNT));
            if (!dedup.firstSeen(key)) {
                metrics.duplicate();
                continue;
            }
            out.add(records.create(row, cursor, SourceOffset.of(cursor, phase, fingerprint)));
        }
        if (page.isFull() && last.equals(before)) {
            throw new ConnectException(
                    "Table '"
                            + spec.name()
                            + "' (alias "
                            + spec.alias()
                            + "): every row of a full page of "
                            + page.size()
                            + " was skipped, so the cursor cannot advance past "
                            + last
                            + "; fix the rows or the cursor fields");
        }
        if (!last.equals(before)) {
            anchor = last;
        }
        metrics.emitted(out.size());
        metrics.cursor(last.ts(), last.formattedTs(), watermark.now());
        metrics.schemaVersion(records.schemaVersion());
        return out;
    }

    private Cursor cursorOf(Record row) {
        String sysId = row.string(spec.sysIdField());
        if (sysId == null || sysId.isBlank()) {
            throw new ServiceNowException("field '" + spec.sysIdField() + "' is missing or empty");
        }
        if (!row.has(spec.timestampField())) {
            throw new ServiceNowException(
                    "field '"
                            + spec.timestampField()
                            + "' is missing (hidden by an ACL or not a column of the table)");
        }
        return new Cursor(SnowTimestamp.parse(row.string(spec.timestampField())), sysId);
    }

    private void handleBadRow(Record row, ServiceNowException e) {
        String message =
                "Table '"
                        + spec.name()
                        + "' (alias "
                        + spec.alias()
                        + "): row sys_id="
                        + row.string(spec.sysIdField())
                        + " cannot be placed on the cursor: "
                        + e.getMessage()
                        + " ("
                        + spec.timestampField()
                        + "='"
                        + row.string(spec.timestampField())
                        + "')";
        if (badRow == SourceConfig.BadRowBehavior.SKIP) {
            metrics.skipped();
            LOG.warn("{}; skipped ({}={})", message, SourceConfig.BAD_ROW_BEHAVIOR, "skip");
            return;
        }
        throw new ConnectException(
                message
                        + "; set "
                        + SourceConfig.BAD_ROW_BEHAVIOR
                        + "="
                        + SourceConfig.BAD_ROW_SKIP
                        + " to skip such rows",
                e);
    }

    /**
     * {@code display.value=true} is fetched as {@code all}; every field takes its display value
     * except the cursor fields, which keep the raw value so the cursor stays parseable.
     */
    private Record displayValues(Record raw) {
        Map<String, FieldValue> out = new LinkedHashMap<>();
        for (Map.Entry<String, FieldValue> e : raw.fields().entrySet()) {
            String name = e.getKey();
            FieldValue v = e.getValue();
            if (v.isNull()) {
                out.put(name, FieldValue.NULL);
            } else if (name.equals(spec.sysIdField())
                    || name.equals(spec.timestampField())
                    || name.equals(KeysetQueryBuilder.SYS_MOD_COUNT)) {
                out.put(name, FieldValue.of(v.value()));
            } else {
                out.put(
                        name,
                        FieldValue.of(v.displayValue() != null ? v.displayValue() : v.value()));
            }
        }
        return Record.of(out);
    }

    // -------------------------------------------------------------- accessors

    TableSpec spec() {
        return spec;
    }

    String table() {
        return spec.name();
    }

    String alias() {
        return spec.alias();
    }

    Map<String, String> partition() {
        return partition;
    }

    String fingerprint() {
        return fingerprint;
    }

    TableMetrics metrics() {
        return metrics;
    }

    State state() {
        return state;
    }

    String phase() {
        return phase;
    }

    Cursor last() {
        return last;
    }

    Instant sweepHi() {
        return sweepHi;
    }

    boolean resumed() {
        return resumed;
    }

    int dedupSize() {
        return dedup.size();
    }
}
