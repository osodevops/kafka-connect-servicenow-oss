package sh.oso.servicenow.cursor;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Connect source partition for one table scan: {@code {instance, table, query_fingerprint,
 * timestamp_field}}. Credentials never influence partition identity.
 */
public final class SourcePartition {

    public static final String INSTANCE = "instance";
    public static final String TABLE = "table";
    public static final String QUERY_FINGERPRINT = "query_fingerprint";
    public static final String TIMESTAMP_FIELD = "timestamp_field";

    private SourcePartition() {}

    public static Map<String, String> of(
            String instanceHost, String table, String fingerprint, String timestampField) {
        LinkedHashMap<String, String> m = new LinkedHashMap<>();
        m.put(
                INSTANCE,
                Objects.requireNonNull(instanceHost, "instanceHost").toLowerCase(Locale.ROOT));
        m.put(TABLE, Objects.requireNonNull(table, "table"));
        m.put(QUERY_FINGERPRINT, Objects.requireNonNull(fingerprint, "fingerprint"));
        m.put(TIMESTAMP_FIELD, Objects.requireNonNull(timestampField, "timestampField"));
        return Map.copyOf(m);
    }
}
