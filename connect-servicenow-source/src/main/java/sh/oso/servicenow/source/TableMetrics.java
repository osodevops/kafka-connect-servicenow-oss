package sh.oso.servicenow.source;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Plain per-table counters and gauges kept by a {@link TablePoller}. Exposed through {@link
 * ServiceNowSourceTask#metrics()} and registered by the task as a {@link SourceTableMetricsMXBean}
 * (the {@code getX()} accessors are the JMX attribute view of the plain ones).
 */
public final class TableMetrics implements SourceTableMetricsMXBean {

    private final String table;
    private volatile String cursorTimestamp = "";
    private volatile long cursorEpochSeconds;
    private volatile long lagSeconds;
    private volatile String phase = "";
    private final AtomicLong recordsEmitted = new AtomicLong();
    private final AtomicLong pagesFetched = new AtomicLong();
    private final AtomicLong duplicatesSuppressed = new AtomicLong();
    private final AtomicLong rowsSkipped = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong throttledMillis = new AtomicLong();
    private volatile long lastPollDurationMs;
    private volatile long lastSuccessfulRequestEpochMs;
    private volatile int schemaVersion;

    public TableMetrics(String table) {
        this.table = table;
    }

    public String table() {
        return table;
    }

    /** Formatted cursor timestamp of the last emitted row ({@code yyyy-MM-dd HH:mm:ss}). */
    public String cursorTimestamp() {
        return cursorTimestamp;
    }

    public long cursorEpochSeconds() {
        return cursorEpochSeconds;
    }

    /** Instance clock minus the cursor timestamp, in seconds, after the last poll. */
    public long lagSeconds() {
        return lagSeconds;
    }

    public String phase() {
        return phase;
    }

    public long recordsEmitted() {
        return recordsEmitted.get();
    }

    public long pagesFetched() {
        return pagesFetched.get();
    }

    public long duplicatesSuppressed() {
        return duplicatesSuppressed.get();
    }

    /** Rows skipped under {@code snow.source.bad.row.behavior=skip}. */
    public long rowsSkipped() {
        return rowsSkipped.get();
    }

    /** Polls that ended in a retryable failure surfaced to the worker. */
    public long retries() {
        return retries.get();
    }

    /** Milliseconds this table's requests waited after {@code 429} responses. */
    public long throttledMillis() {
        return throttledMillis.get();
    }

    public long lastPollDurationMs() {
        return lastPollDurationMs;
    }

    public long lastSuccessfulRequestEpochMs() {
        return lastSuccessfulRequestEpochMs;
    }

    public int schemaVersion() {
        return schemaVersion;
    }

    void cursor(Instant ts, String formatted, Instant serverNow) {
        cursorTimestamp = formatted;
        cursorEpochSeconds = ts.getEpochSecond();
        lagSeconds = Math.max(0, serverNow.getEpochSecond() - ts.getEpochSecond());
    }

    void phase(String phase) {
        this.phase = phase;
    }

    void emitted(int n) {
        recordsEmitted.addAndGet(n);
    }

    void page() {
        pagesFetched.incrementAndGet();
    }

    void duplicate() {
        duplicatesSuppressed.incrementAndGet();
    }

    void skipped() {
        rowsSkipped.incrementAndGet();
    }

    void retry() {
        retries.incrementAndGet();
    }

    void throttled(long millis) {
        if (millis > 0) {
            throttledMillis.addAndGet(millis);
        }
    }

    void pollDuration(long millis) {
        lastPollDurationMs = millis;
    }

    void requestSucceeded(long epochMs) {
        lastSuccessfulRequestEpochMs = epochMs;
    }

    void schemaVersion(Integer version) {
        if (version != null) {
            schemaVersion = version;
        }
    }

    /** A point-in-time copy of every metric, keyed by MBean attribute name. */
    public Map<String, Object> snapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("table", table);
        m.put("cursorTimestamp", cursorTimestamp);
        m.put("cursorEpochSeconds", cursorEpochSeconds);
        m.put("lagSeconds", lagSeconds);
        m.put("phase", phase);
        m.put("recordsEmitted", recordsEmitted.get());
        m.put("pagesFetched", pagesFetched.get());
        m.put("duplicatesSuppressed", duplicatesSuppressed.get());
        m.put("rowsSkipped", rowsSkipped.get());
        m.put("retries", retries.get());
        m.put("throttledMillis", throttledMillis.get());
        m.put("lastPollDurationMs", lastPollDurationMs);
        m.put("lastSuccessfulRequestEpochMs", lastSuccessfulRequestEpochMs);
        m.put("schemaVersion", schemaVersion);
        return m;
    }

    // --- SourceTableMetricsMXBean ---

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String getCursorTimestamp() {
        return cursorTimestamp;
    }

    @Override
    public long getCursorEpochSeconds() {
        return cursorEpochSeconds;
    }

    @Override
    public long getLagSeconds() {
        return lagSeconds;
    }

    @Override
    public String getPhase() {
        return phase;
    }

    @Override
    public long getRecordsEmitted() {
        return recordsEmitted.get();
    }

    @Override
    public long getPagesFetched() {
        return pagesFetched.get();
    }

    @Override
    public long getDuplicatesSuppressed() {
        return duplicatesSuppressed.get();
    }

    @Override
    public long getRowsSkipped() {
        return rowsSkipped.get();
    }

    @Override
    public long getRetries() {
        return retries.get();
    }

    @Override
    public long getThrottledMillis() {
        return throttledMillis.get();
    }

    @Override
    public long getLastPollDurationMs() {
        return lastPollDurationMs;
    }

    @Override
    public long getLastSuccessfulRequestEpochMs() {
        return lastSuccessfulRequestEpochMs;
    }

    @Override
    public int getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public String toString() {
        return "TableMetrics" + snapshot();
    }
}
