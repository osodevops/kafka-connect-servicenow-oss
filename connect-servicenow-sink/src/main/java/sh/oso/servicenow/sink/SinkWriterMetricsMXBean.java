package sh.oso.servicenow.sink;

import java.util.Map;

/**
 * JMX view of one sink task's writer, registered as {@code
 * sh.oso.servicenow:type=sink-writer,connector=<name>,task=<n>}. Every attribute is read-only;
 * counters are cumulative since the task started.
 */
public interface SinkWriterMetricsMXBean {

    /** Records written with a POST. */
    long getCreates();

    /** Records written with a PATCH (including correlation-lookup matches). */
    long getPatches();

    /** Records written with a PUT. */
    long getPuts();

    /** Records written with a DELETE. */
    long getDeletes();

    /**
     * Successful writes by operation: {@code CREATE}, {@code PATCH}, {@code PUT}, {@code DELETE}.
     */
    Map<String, Long> getWrittenByOperation();

    /** Successful writes, all operations. */
    long getWritten();

    /** Records that failed permanently and went to the error paths. */
    long getFailures();

    /** Records that failed with the {@code AMBIGUOUS} classification (subset of failures). */
    long getAmbiguous();

    /** Records whose transient failures outlived the retry budget; their batch is re-delivered. */
    long getRetriesExhausted();

    /** Requests on the wire right now. */
    int getInFlight();

    /** The highest value {@link #getInFlight()} has reached. */
    int getMaxInFlightObserved();

    /** Requests re-sent: snow-core retries plus ambiguous POST and DELETE re-sends by the sink. */
    long getRetries();

    /** Milliseconds this task's requests waited after {@code 429} responses. */
    long getThrottledMillis();

    /** Reports published to the success topic. */
    long getReporterSuccess();

    /** Reports published to the error topic. */
    long getReporterError();

    /** Epoch milliseconds of the last successful ServiceNow response for this task; 0 if none. */
    long getLastSuccessfulRequestEpochMs();
}
