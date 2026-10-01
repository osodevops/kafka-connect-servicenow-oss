package sh.oso.servicenow.sink;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import sh.oso.servicenow.http.HttpStats;

/**
 * Counters kept by one task's {@link ServiceNowWriter} and {@link Reporter}, read live through the
 * {@link SinkWriterMetricsMXBean} the task registers. Transport-level numbers (retries made by
 * snow-core, throttled time, last success) come from the task's {@link HttpStats}.
 */
final class SinkWriterMetrics implements SinkWriterMetricsMXBean {

    private final HttpStats http;
    private final AtomicLong creates = new AtomicLong();
    private final AtomicLong patches = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong ambiguous = new AtomicLong();
    private final AtomicLong retriesExhausted = new AtomicLong();
    private final AtomicLong resends = new AtomicLong();
    private final AtomicLong reporterSuccess = new AtomicLong();
    private final AtomicLong reporterError = new AtomicLong();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlightObserved = new AtomicInteger();

    SinkWriterMetrics(HttpStats http) {
        this.http = http == null ? new HttpStats() : http;
    }

    /** Accounts for one write outcome. */
    void recorded(Outcome o) {
        if (o.isSuccess()) {
            counter(o.operation()).incrementAndGet();
            resends.addAndGet(o.retries());
            return;
        }
        if (o.classification() == Classification.RETRIES_EXHAUSTED) {
            retriesExhausted.incrementAndGet();
            return;
        }
        failures.incrementAndGet();
        if (o.classification() == Classification.AMBIGUOUS) {
            ambiguous.incrementAndGet();
            resends.addAndGet(o.retries());
        }
    }

    void requestStarted() {
        int now = inFlight.incrementAndGet();
        maxInFlightObserved.accumulateAndGet(now, Math::max);
    }

    void requestFinished() {
        inFlight.decrementAndGet();
    }

    void reported(boolean success) {
        (success ? reporterSuccess : reporterError).incrementAndGet();
    }

    private AtomicLong counter(Operation op) {
        if (op == null) {
            return creates;
        }
        return switch (op) {
            case CREATE -> creates;
            case PATCH -> patches;
            case PUT -> puts;
            case DELETE -> deletes;
        };
    }

    // --- SinkWriterMetricsMXBean ---

    @Override
    public long getCreates() {
        return creates.get();
    }

    @Override
    public long getPatches() {
        return patches.get();
    }

    @Override
    public long getPuts() {
        return puts.get();
    }

    @Override
    public long getDeletes() {
        return deletes.get();
    }

    @Override
    public Map<String, Long> getWrittenByOperation() {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put(Operation.CREATE.name(), creates.get());
        m.put(Operation.PATCH.name(), patches.get());
        m.put(Operation.PUT.name(), puts.get());
        m.put(Operation.DELETE.name(), deletes.get());
        return m;
    }

    @Override
    public long getWritten() {
        return creates.get() + patches.get() + puts.get() + deletes.get();
    }

    @Override
    public long getFailures() {
        return failures.get();
    }

    @Override
    public long getAmbiguous() {
        return ambiguous.get();
    }

    @Override
    public long getRetriesExhausted() {
        return retriesExhausted.get();
    }

    @Override
    public int getInFlight() {
        return inFlight.get();
    }

    @Override
    public int getMaxInFlightObserved() {
        return maxInFlightObserved.get();
    }

    @Override
    public long getRetries() {
        return http.retries() + resends.get();
    }

    @Override
    public long getThrottledMillis() {
        return http.throttledMillis();
    }

    @Override
    public long getReporterSuccess() {
        return reporterSuccess.get();
    }

    @Override
    public long getReporterError() {
        return reporterError.get();
    }

    @Override
    public long getLastSuccessfulRequestEpochMs() {
        return http.lastSuccessfulRequestEpochMs();
    }

    @Override
    public String toString() {
        return "SinkWriterMetrics{written="
                + getWrittenByOperation()
                + ", failures="
                + failures.get()
                + ", ambiguous="
                + ambiguous.get()
                + ", retriesExhausted="
                + retriesExhausted.get()
                + ", inFlight="
                + inFlight.get()
                + ", maxInFlightObserved="
                + maxInFlightObserved.get()
                + ", retries="
                + getRetries()
                + ", throttledMillis="
                + getThrottledMillis()
                + ", reporterSuccess="
                + reporterSuccess.get()
                + ", reporterError="
                + reporterError.get()
                + '}';
    }
}
