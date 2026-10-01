package sh.oso.servicenow.source;

/**
 * JMX view of one polled table, registered as {@code
 * sh.oso.servicenow:type=source-table,connector=<name>,task=<n>,table=<table>}. Every attribute is
 * read-only; counters are cumulative since the task started.
 */
public interface SourceTableMetricsMXBean {

    /** ServiceNow table (or view) name. */
    String getTable();

    /** Timestamp of the last emitted row, formatted {@code yyyy-MM-dd HH:mm:ss} (instance time). */
    String getCursorTimestamp();

    /** The same cursor timestamp as epoch seconds; 0 before the first row. */
    long getCursorEpochSeconds();

    /** Instance clock minus the cursor timestamp after the last poll, in seconds. */
    long getLagSeconds();

    /** {@code backfill} or {@code stream}. */
    String getPhase();

    /** Records returned to the framework. */
    long getRecordsEmitted();

    /** Table API pages fetched. */
    long getPagesFetched();

    /** Row versions suppressed by the overlap dedup cache. */
    long getDuplicatesSuppressed();

    /** Rows skipped under {@code snow.source.bad.row.behavior=skip}. */
    long getRowsSkipped();

    /** Polls that ended in a retryable failure surfaced to the worker as a RetriableException. */
    long getRetries();

    /** Milliseconds this table's requests waited after {@code 429} responses. */
    long getThrottledMillis();

    /** Wall-clock duration of the last poll, in milliseconds. */
    long getLastPollDurationMs();

    /** Epoch milliseconds of the last successful Table API response for this table; 0 if none. */
    long getLastSuccessfulRequestEpochMs();

    /** Current value-schema version in {@code strings} and {@code typed} modes; 0 otherwise. */
    int getSchemaVersion();
}
