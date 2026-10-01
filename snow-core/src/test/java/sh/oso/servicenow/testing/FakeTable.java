package sh.oso.servicenow.testing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Rows of one fake table keyed by sys_id, in insertion order. Thread-safe. */
public final class FakeTable {

    private final String name;
    private final LinkedHashMap<String, LinkedHashMap<String, String>> rows = new LinkedHashMap<>();

    FakeTable(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public synchronized List<Map<String, String>> rows() {
        List<Map<String, String>> out = new ArrayList<>(rows.size());
        for (LinkedHashMap<String, String> r : rows.values()) {
            out.add(new LinkedHashMap<>(r));
        }
        return out;
    }

    public synchronized Optional<Map<String, String>> get(String sysId) {
        LinkedHashMap<String, String> r = rows.get(sysId);
        return r == null ? Optional.empty() : Optional.of(new LinkedHashMap<>(r));
    }

    synchronized void put(String sysId, Map<String, String> row) {
        rows.put(sysId, new LinkedHashMap<>(row));
    }

    synchronized boolean remove(String sysId) {
        return rows.remove(sysId) != null;
    }

    public synchronized int size() {
        return rows.size();
    }

    public synchronized void clear() {
        rows.clear();
    }
}
