package sh.oso.servicenow.testing;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What the connector actually sent to the fake: one {@link Entry} per Table API request (method,
 * table, sys_id, body), the number of token requests, and the in-flight high-water mark.
 */
public final class RequestJournal {

    /** One Table API request as the fake saw it. */
    public record Entry(
            String method,
            String path,
            String table,
            String sysId,
            String body,
            String requestId,
            Instant at) {}

    private final List<Entry> entries = new ArrayList<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();
    private final AtomicInteger tokenRequests = new AtomicInteger();

    void record(Entry entry) {
        synchronized (entries) {
            entries.add(entry);
        }
    }

    void enter() {
        int now = inFlight.incrementAndGet();
        maxInFlight.accumulateAndGet(now, Math::max);
    }

    void exit() {
        inFlight.decrementAndGet();
    }

    void tokenRequested() {
        tokenRequests.incrementAndGet();
    }

    public List<Entry> entries() {
        synchronized (entries) {
            return List.copyOf(entries);
        }
    }

    /** Requests with this method on this table (any sys_id). */
    public int count(String method, String table) {
        int n = 0;
        for (Entry e : entries()) {
            if (e.method().equalsIgnoreCase(method) && Objects.equals(e.table(), table)) {
                n++;
            }
        }
        return n;
    }

    public int count(String method) {
        int n = 0;
        for (Entry e : entries()) {
            if (e.method().equalsIgnoreCase(method)) {
                n++;
            }
        }
        return n;
    }

    /** Request bodies for this method, table and sys_id (null sys_id matches collection calls). */
    public List<String> bodies(String method, String table, String sysId) {
        List<String> out = new ArrayList<>();
        for (Entry e : entries()) {
            if (e.method().equalsIgnoreCase(method)
                    && Objects.equals(e.table(), table)
                    && Objects.equals(e.sysId(), sysId)) {
                out.add(e.body());
            }
        }
        return out;
    }

    /** Highest number of concurrently handled Table API requests since the last reset. */
    public int maxInFlight() {
        return maxInFlight.get();
    }

    public int inFlight() {
        return inFlight.get();
    }

    /** Calls to {@code POST /oauth_token.do}. */
    public int tokenRequests() {
        return tokenRequests.get();
    }

    public void reset() {
        synchronized (entries) {
            entries.clear();
        }
        maxInFlight.set(inFlight.get());
        tokenRequests.set(0);
    }
}
