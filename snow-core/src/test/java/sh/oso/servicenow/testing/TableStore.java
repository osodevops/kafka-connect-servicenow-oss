package sh.oso.servicenow.testing;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.table.PathSegments;

/**
 * In-memory tables behind the fake Table API. Writes maintain {@code sys_id}, {@code
 * sys_created_on}, {@code sys_updated_on} (second precision from the clock), {@code sys_mod_count},
 * {@code sys_created_by} and {@code sys_updated_by}. Seeding may supply its own {@code sys_id},
 * {@code sys_created_on}, {@code sys_updated_on} and {@code sys_mod_count} to build histories.
 */
public final class TableStore {

    public static final String USER = "connect";
    private static final Set<String> SYS_FIELDS =
            Set.of(
                    "sys_id",
                    "sys_created_on",
                    "sys_updated_on",
                    "sys_mod_count",
                    "sys_created_by",
                    "sys_updated_by");

    private final Clock clock;
    private final Map<String, FakeTable> tables = new ConcurrentHashMap<>();

    public TableStore(Clock clock) {
        this.clock = clock;
    }

    /** The table, created on demand. */
    public FakeTable table(String name) {
        return tables.computeIfAbsent(PathSegments.requireTable(name), FakeTable::new);
    }

    public Set<String> tableNames() {
        return Set.copyOf(tables.keySet());
    }

    public static String newSysId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /** Inserts and returns the sys_id (generated unless a valid one is supplied). */
    public String insert(String table, Map<String, String> fields) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        String supplied = fields.get("sys_id");
        String sysId = PathSegments.isSysId(supplied) ? supplied : newSysId();
        String now = now();
        row.put("sys_id", sysId);
        fields.forEach(
                (k, v) -> {
                    if (!"sys_id".equals(k)) {
                        row.put(k, v == null ? "" : v);
                    }
                });
        row.putIfAbsent("sys_created_on", now);
        row.putIfAbsent("sys_updated_on", now);
        row.putIfAbsent("sys_mod_count", "0");
        row.putIfAbsent("sys_created_by", USER);
        row.putIfAbsent("sys_updated_by", USER);
        table(table).put(sysId, row);
        return sysId;
    }

    /**
     * Merges {@code fields}, bumps {@code sys_updated_on} to the clock and {@code sys_mod_count}.
     */
    public void update(String table, String sysId, Map<String, String> fields) {
        FakeTable t = table(table);
        synchronized (t) {
            Map<String, String> row =
                    t.get(sysId)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "no row " + sysId + " in " + table));
            LinkedHashMap<String, String> merged = new LinkedHashMap<>(row);
            fields.forEach(
                    (k, v) -> {
                        if (!SYS_FIELDS.contains(k)) {
                            merged.put(k, v == null ? "" : v);
                        }
                    });
            touch(merged);
            t.put(sysId, merged);
        }
    }

    /** PUT semantics: user fields not supplied are cleared to the empty string, then merged. */
    public void replace(String table, String sysId, Map<String, String> fields) {
        FakeTable t = table(table);
        synchronized (t) {
            Map<String, String> row =
                    t.get(sysId)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "no row " + sysId + " in " + table));
            LinkedHashMap<String, String> merged = new LinkedHashMap<>(row);
            for (String k : row.keySet()) {
                if (!SYS_FIELDS.contains(k) && !fields.containsKey(k)) {
                    merged.put(k, "");
                }
            }
            fields.forEach(
                    (k, v) -> {
                        if (!SYS_FIELDS.contains(k)) {
                            merged.put(k, v == null ? "" : v);
                        }
                    });
            touch(merged);
            t.put(sysId, merged);
        }
    }

    public boolean delete(String table, String sysId) {
        return table(table).remove(sysId);
    }

    public Optional<Map<String, String>> get(String table, String sysId) {
        return table(table).get(sysId);
    }

    public List<Map<String, String>> all(String table) {
        return table(table).rows();
    }

    public int size(String table) {
        return table(table).size();
    }

    public boolean exists(String table, String sysId) {
        return table(table).get(sysId).isPresent();
    }

    /** Inserts {@code rows} rows generated by {@code gen(index)}. */
    public void seed(String table, int rows, Function<Integer, Map<String, String>> gen) {
        for (int i = 0; i < rows; i++) {
            insert(table, gen.apply(i));
        }
    }

    public void clear() {
        tables.clear();
    }

    private void touch(Map<String, String> row) {
        row.put("sys_updated_on", now());
        row.put("sys_updated_by", USER);
        long count;
        try {
            count = Long.parseLong(row.getOrDefault("sys_mod_count", "0"));
        } catch (NumberFormatException e) {
            count = 0;
        }
        row.put("sys_mod_count", Long.toString(count + 1));
    }

    private String now() {
        return SnowTimestamp.format(clock.instant().truncatedTo(ChronoUnit.SECONDS));
    }
}
