package sh.oso.servicenow.cursor;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;

/**
 * Keyset position {@code (timestamp, sys_id)}. Ordered by timestamp then sys_id, matching {@code
 * ORDERBY<ts>^ORDERBYsys_id}. A null sys_id is normalised to the empty string, which sorts before
 * every real sys_id (so {@code (T, "")} means "before the first row of second T").
 */
public record Cursor(Instant ts, String sysId) implements Comparable<Cursor> {

    private static final Comparator<Cursor> ORDER =
            Comparator.comparing(Cursor::ts).thenComparing(Cursor::sysId);

    public Cursor {
        if (ts == null) {
            throw new IllegalArgumentException("ts must not be null");
        }
        ts = ts.truncatedTo(ChronoUnit.SECONDS);
        sysId = sysId == null ? "" : sysId;
    }

    public static Cursor of(String timestamp, String sysId) {
        return new Cursor(SnowTimestamp.parse(timestamp), sysId);
    }

    /** Start of the given second with no sys_id. */
    public static Cursor atSecond(Instant ts) {
        return new Cursor(ts, "");
    }

    public String formattedTs() {
        return SnowTimestamp.format(ts);
    }

    @Override
    public int compareTo(Cursor o) {
        return ORDER.compare(this, o);
    }

    public boolean isAfter(Cursor o) {
        return compareTo(o) > 0;
    }

    @Override
    public String toString() {
        return "(" + formattedTs() + ", " + sysId + ")";
    }
}
