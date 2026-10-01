package sh.oso.servicenow.http;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import sh.oso.servicenow.common.ServiceNowApiException;

/**
 * Transport-level counters kept by one {@link ServiceNowHttpClient}: attempts the retry policy
 * re-sent, time it waited on {@code 429} responses, and when the last request succeeded. The
 * connectors read these for their JMX MBeans; nothing here blocks or allocates on the request path.
 */
public final class HttpStats {

    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong throttledMillis = new AtomicLong();
    private volatile long lastSuccessfulRequestEpochMs;

    /** Attempts beyond the first that the retry policy made (every status and transport class). */
    public long retries() {
        return retries.get();
    }

    /** Milliseconds spent waiting after {@code 429} responses before re-sending. */
    public long throttledMillis() {
        return throttledMillis.get();
    }

    /** Epoch milliseconds of the last response below 400, or 0 before the first. */
    public long lastSuccessfulRequestEpochMs() {
        return lastSuccessfulRequestEpochMs;
    }

    void onRetry(String opName, int attempt, Duration wait, Exception failure) {
        retries.incrementAndGet();
        if (failure instanceof ServiceNowApiException api && api.isRateLimited()) {
            throttledMillis.addAndGet(Math.max(0, wait.toMillis()));
        }
    }

    void succeeded(long epochMs) {
        lastSuccessfulRequestEpochMs = epochMs;
    }

    @Override
    public String toString() {
        return "HttpStats{retries="
                + retries.get()
                + ", throttledMillis="
                + throttledMillis.get()
                + ", lastSuccessfulRequestEpochMs="
                + lastSuccessfulRequestEpochMs
                + '}';
    }
}
