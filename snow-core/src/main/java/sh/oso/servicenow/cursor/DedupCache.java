package sh.oso.servicenow.cursor;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded insertion-ordered set of recently emitted row versions. {@link #firstSeen(DedupKey)}
 * returns true the first time a key is offered and false afterwards, until it is evicted by newer
 * keys. Thread-safe.
 */
public final class DedupCache {

    private final int maxEntries;
    private final LinkedHashMap<DedupKey, Boolean> entries;

    public DedupCache(int maxEntries) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be >= 1");
        }
        this.maxEntries = maxEntries;
        this.entries =
                new LinkedHashMap<>(16, 0.75f, false) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<DedupKey, Boolean> eldest) {
                        return size() > DedupCache.this.maxEntries;
                    }
                };
    }

    public synchronized boolean firstSeen(DedupKey key) {
        return entries.put(key, Boolean.TRUE) == null;
    }

    public synchronized boolean contains(DedupKey key) {
        return entries.containsKey(key);
    }

    public synchronized int size() {
        return entries.size();
    }

    public int maxEntries() {
        return maxEntries;
    }

    public synchronized void clear() {
        entries.clear();
    }
}
