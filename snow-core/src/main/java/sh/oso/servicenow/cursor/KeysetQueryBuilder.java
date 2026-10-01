package sh.oso.servicenow.cursor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.table.EncodedQuery;

/**
 * The two keyset query shapes of ADR 0001, applied to every {@code ^NQ} branch of the base query:
 *
 * <pre>
 *   drainBucket(last):           base ^ ts=T ^ sys_id&gt;S ^ ORDERBYsys_id
 *   nextBuckets(after, hi):      base ^ ts&gt;after ^ ts&lt;=hi ^ ORDERBYts ^ ORDERBYsys_id
 * </pre>
 *
 * The base query must not order; {@link #projection(List)} guarantees the cursor fields and {@code
 * sys_mod_count} are always selected.
 */
public final class KeysetQueryBuilder {

    public static final String SYS_MOD_COUNT = "sys_mod_count";

    private final EncodedQuery base;
    private final String tsField;
    private final String sysIdField;

    public KeysetQueryBuilder(EncodedQuery base, String tsField, String sysIdField) {
        this.base = base == null ? EncodedQuery.empty() : base;
        this.tsField = requireField(tsField, "timestamp field");
        this.sysIdField = requireField(sysIdField, "sys_id field");
        validateBase(this.base);
    }

    public KeysetQueryBuilder(EncodedQuery base) {
        this(base, "sys_updated_on", "sys_id");
    }

    public static void validateBase(EncodedQuery base) {
        if (base != null && base.containsOrderBy()) {
            throw new ServiceNowException(
                    "The base query must not contain ORDERBY/ORDERBYDESC; the connector orders by the"
                            + " cursor fields: "
                            + base.raw());
        }
    }

    public EncodedQuery base() {
        return base;
    }

    public String tsField() {
        return tsField;
    }

    public String sysIdField() {
        return sysIdField;
    }

    /** Rows in the same second as {@code last} with a greater sys_id. */
    public EncodedQuery drainBucket(Cursor last) {
        Objects.requireNonNull(last, "last");
        return base.and(tsField + "=" + SnowTimestamp.format(last.ts()))
                .and(sysIdField + ">" + last.sysId())
                .orderBy(sysIdField);
    }

    /** Rows strictly after {@code afterExclusive} and up to {@code hiInclusive}. */
    public EncodedQuery nextBuckets(Instant afterExclusive, Instant hiInclusive) {
        Objects.requireNonNull(afterExclusive, "afterExclusive");
        Objects.requireNonNull(hiInclusive, "hiInclusive");
        return base.and(tsField + ">" + SnowTimestamp.format(afterExclusive))
                .and(tsField + "<=" + SnowTimestamp.format(hiInclusive))
                .orderBy(tsField)
                .orderBy(sysIdField);
    }

    /**
     * The user's projection with the cursor fields appended when missing. An empty projection stays
     * empty (all fields).
     */
    public List<String> projection(List<String> userFields) {
        if (userFields == null || userFields.isEmpty()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String f : userFields) {
            String t = f == null ? "" : f.trim();
            if (!t.isEmpty() && !out.contains(t)) {
                out.add(t);
            }
        }
        for (String required : List.of(sysIdField, tsField, SYS_MOD_COUNT)) {
            if (!out.contains(required)) {
                out.add(required);
            }
        }
        return List.copyOf(out);
    }

    private static String requireField(String field, String what) {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException(what + " must not be blank");
        }
        return field.trim();
    }
}
