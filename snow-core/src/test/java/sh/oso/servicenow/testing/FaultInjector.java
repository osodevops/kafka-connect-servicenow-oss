package sh.oso.servicenow.testing;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Armed faults the fake applies to Table API requests (never to the token endpoint). Counted faults
 * are consumed one per request in the order: unauthorized, rate limit, server error, malformed
 * JSON, truncated body. Table-scoped faults (forbid, not found, hidden fields, timeout after write)
 * apply to that table only.
 */
public final class FaultInjector {

    private final AtomicInteger unauthorized = new AtomicInteger();
    private final AtomicInteger rateLimited = new AtomicInteger();
    private final AtomicReference<Duration> rateLimitRetryAfter =
            new AtomicReference<>(Duration.ofSeconds(1));
    private final AtomicInteger serverErrors = new AtomicInteger();
    private final AtomicInteger serverErrorStatus = new AtomicInteger(503);
    private final AtomicInteger malformedJson = new AtomicInteger();
    private final AtomicInteger truncatedBody = new AtomicInteger();
    private final Map<String, TimeoutAfterWrite> timeoutAfterWrite = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> hiddenFields = new ConcurrentHashMap<>();
    private final Set<String> forbiddenTables = new CopyOnWriteArraySet<>();
    private final Set<String> notFoundTables = new CopyOnWriteArraySet<>();
    private final AtomicReference<Duration> latency = new AtomicReference<>(Duration.ZERO);

    private record TimeoutAfterWrite(AtomicInteger remaining, Duration delay) {}

    /** The next Table API request answers 401 once. */
    public void unauthorizedOnce() {
        unauthorized.set(1);
    }

    public void unauthorizedTimes(int times) {
        unauthorized.set(times);
    }

    /** The next {@code times} Table API requests answer 429 with {@code Retry-After} in seconds. */
    public void rateLimit(int times, Duration retryAfter) {
        rateLimitRetryAfter.set(retryAfter);
        rateLimited.set(times);
    }

    /** The next {@code times} Table API requests answer with {@code status} (5xx or 408/425). */
    public void serverError(int times, int status) {
        serverErrorStatus.set(status);
        serverErrors.set(times);
    }

    /**
     * The next write on {@code table} is applied, then the response is delayed by {@code delay}.
     */
    public void timeoutAfterWrite(String table, Duration delay) {
        timeoutAfterWrite(table, delay, 1);
    }

    public void timeoutAfterWrite(String table, Duration delay, int times) {
        timeoutAfterWrite.put(table, new TimeoutAfterWrite(new AtomicInteger(times), delay));
    }

    /** The next Table API response is HTTP 200 with a body that is not valid JSON. */
    public void malformedJsonOnce() {
        malformedJson.set(1);
    }

    /** The next Table API response is cut off mid-body (connection fault). */
    public void truncatedBodyOnce() {
        truncatedBody.set(1);
    }

    /** Strips {@code field} from every response for {@code table} (ACL simulation). */
    public void hideField(String table, String field) {
        hiddenFields.computeIfAbsent(table, t -> new CopyOnWriteArraySet<>()).add(field);
    }

    /** Every request on {@code table} answers 403. */
    public void forbidTable(String table) {
        forbiddenTables.add(table);
    }

    /** Every request on {@code table} answers 404 as if the table did not exist. */
    public void notFoundTable(String table) {
        notFoundTables.add(table);
    }

    /** Adds a fixed delay to every Table API request (for concurrency measurements). */
    public void latency(Duration delay) {
        latency.set(delay == null ? Duration.ZERO : delay);
    }

    public void clear() {
        unauthorized.set(0);
        rateLimited.set(0);
        serverErrors.set(0);
        malformedJson.set(0);
        truncatedBody.set(0);
        timeoutAfterWrite.clear();
        hiddenFields.clear();
        forbiddenTables.clear();
        notFoundTables.clear();
        latency.set(Duration.ZERO);
    }

    // --- consumed by the transformer ---

    boolean takeUnauthorized() {
        return take(unauthorized);
    }

    Optional<Duration> takeRateLimit() {
        return take(rateLimited) ? Optional.of(rateLimitRetryAfter.get()) : Optional.empty();
    }

    OptionalInt takeServerError() {
        return take(serverErrors) ? OptionalInt.of(serverErrorStatus.get()) : OptionalInt.empty();
    }

    boolean takeMalformedJson() {
        return take(malformedJson);
    }

    boolean takeTruncatedBody() {
        return take(truncatedBody);
    }

    Optional<Duration> takeTimeoutAfterWrite(String table) {
        TimeoutAfterWrite t = timeoutAfterWrite.get(table);
        if (t == null) {
            return Optional.empty();
        }
        return take(t.remaining()) ? Optional.of(t.delay()) : Optional.empty();
    }

    Set<String> hiddenFields(String table) {
        Set<String> s = hiddenFields.get(table);
        return s == null ? Set.of() : s;
    }

    boolean isForbidden(String table) {
        return forbiddenTables.contains(table);
    }

    boolean isNotFound(String table) {
        return notFoundTables.contains(table);
    }

    Duration latency() {
        return latency.get();
    }

    private static boolean take(AtomicInteger counter) {
        while (true) {
            int n = counter.get();
            if (n <= 0) {
                return false;
            }
            if (counter.compareAndSet(n, n - 1)) {
                return true;
            }
        }
    }
}
