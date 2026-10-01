package sh.oso.servicenow.source;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.testing.TableStore;

/**
 * Synthetic rows for the fake ServiceNow. sys_ids start with a letter so they never look numeric.
 */
final class Fixtures {

    private Fixtures() {}

    /** A 32-character lower-case hex sys_id that sorts like {@code i}. */
    static String sysId(int i) {
        return String.format("a%031x", i);
    }

    static Map<String, String> row(int i, Instant ts) {
        LinkedHashMap<String, String> r = new LinkedHashMap<>();
        r.put("sys_id", sysId(i));
        r.put("sys_updated_on", SnowTimestamp.format(ts));
        r.put("sys_created_on", SnowTimestamp.format(ts));
        r.put("short_description", "row " + i);
        r.put("priority", Integer.toString((i % 5) + 1));
        return r;
    }

    /**
     * Inserts {@code rows} rows with {@code perSecond} rows per second starting at {@code base}.
     */
    static void seed(TableStore store, String table, int rows, Instant base, int perSecond) {
        for (int i = 0; i < rows; i++) {
            store.insert(table, row(i, base.plusSeconds(i / perSecond)));
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> value(SourceRecord r) {
        return (Map<String, Object>) r.value();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> map(Object o) {
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> partition(SourceRecord r) {
        return (Map<String, Object>) r.sourcePartition();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> offset(SourceRecord r) {
        return (Map<String, Object>) r.sourceOffset();
    }

    static String field(SourceRecord r, String name) {
        Object v = value(r).get(name);
        return v == null ? null : v.toString();
    }

    /** {@code sys_id@sys_updated_on@sys_mod_count}: the identity of one row version. */
    static String versionKey(SourceRecord r) {
        return r.key() + "@" + field(r, "sys_updated_on") + "@" + field(r, "sys_mod_count");
    }

    static String versionKey(Map<String, String> row) {
        return row.get("sys_id") + "@" + row.get("sys_updated_on") + "@" + row.get("sys_mod_count");
    }

    static String offsetTimestamp(SourceRecord r) {
        return String.valueOf(r.sourceOffset().get("timestamp"));
    }

    static String offsetSysId(SourceRecord r) {
        return String.valueOf(r.sourceOffset().get("sys_id"));
    }

    /** True when every record's offset tuple is at or after the previous record's. */
    static boolean offsetsMonotonic(List<SourceRecord> records) {
        String prevTs = "";
        String prevId = "";
        for (SourceRecord r : records) {
            String ts = offsetTimestamp(r);
            String id = offsetSysId(r);
            int c = ts.compareTo(prevTs);
            if (c < 0 || (c == 0 && id.compareTo(prevId) < 0)) {
                return false;
            }
            prevTs = ts;
            prevId = id;
        }
        return true;
    }
}
