package sh.oso.servicenow.cursor;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import sh.oso.servicenow.common.IncompatibleOffsetException;
import sh.oso.servicenow.common.ServiceNowException;

/**
 * Versioned Connect source offset {@code {version, timestamp, sys_id, phase, fingerprint}}. Every
 * map value is a String so any offset backing store can hold it. {@link #fromMap(Map)} refuses
 * unknown versions and missing or malformed fields with {@link IncompatibleOffsetException}; the
 * caller compares {@code fingerprint} against its own query fingerprint.
 */
public record SourceOffset(
        int version, Instant timestamp, String sysId, String phase, String fingerprint) {

    public static final int CURRENT_VERSION = 1;
    public static final String PHASE_BACKFILL = "backfill";
    public static final String PHASE_STREAM = "stream";

    public static final String KEY_VERSION = "version";
    public static final String KEY_TIMESTAMP = "timestamp";
    public static final String KEY_SYS_ID = "sys_id";
    public static final String KEY_PHASE = "phase";
    public static final String KEY_FINGERPRINT = "fingerprint";

    public SourceOffset {
        Objects.requireNonNull(timestamp, "timestamp");
        timestamp = timestamp.truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        sysId = sysId == null ? "" : sysId;
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(fingerprint, "fingerprint");
    }

    public static SourceOffset of(Cursor cursor, String phase, String fingerprint) {
        return new SourceOffset(CURRENT_VERSION, cursor.ts(), cursor.sysId(), phase, fingerprint);
    }

    public Cursor cursor() {
        return new Cursor(timestamp, sysId);
    }

    public Map<String, ?> toMap() {
        LinkedHashMap<String, String> m = new LinkedHashMap<>();
        m.put(KEY_VERSION, Integer.toString(version));
        m.put(KEY_TIMESTAMP, SnowTimestamp.format(timestamp));
        m.put(KEY_SYS_ID, sysId);
        m.put(KEY_PHASE, phase);
        m.put(KEY_FINGERPRINT, fingerprint);
        return m;
    }

    public static SourceOffset fromMap(Map<String, ?> m) {
        if (m == null) {
            throw new IncompatibleOffsetException("offset map is null");
        }
        String version = string(m, KEY_VERSION);
        int v;
        try {
            v = Integer.parseInt(version);
        } catch (NumberFormatException e) {
            throw new IncompatibleOffsetException(
                    "offset version '" + version + "' is not a number");
        }
        if (v != CURRENT_VERSION) {
            throw new IncompatibleOffsetException(
                    "offset version " + v + " is not supported (expected " + CURRENT_VERSION + ")");
        }
        Instant ts;
        try {
            ts = SnowTimestamp.parse(string(m, KEY_TIMESTAMP));
        } catch (ServiceNowException e) {
            throw new IncompatibleOffsetException(
                    "offset timestamp is invalid: " + e.getMessage(), e);
        }
        String sysId = string(m, KEY_SYS_ID);
        String phase = string(m, KEY_PHASE);
        if (!PHASE_BACKFILL.equals(phase) && !PHASE_STREAM.equals(phase)) {
            throw new IncompatibleOffsetException("offset phase '" + phase + "' is unknown");
        }
        String fingerprint = string(m, KEY_FINGERPRINT);
        if (fingerprint.isBlank()) {
            throw new IncompatibleOffsetException("offset fingerprint is empty");
        }
        return new SourceOffset(v, ts, sysId, phase, fingerprint);
    }

    private static String string(Map<String, ?> m, String key) {
        Object value = m.get(key);
        if (value == null) {
            throw new IncompatibleOffsetException("offset is missing '" + key + "'");
        }
        if (!(value instanceof String s)) {
            throw new IncompatibleOffsetException(
                    "offset '"
                            + key
                            + "' must be a String but is "
                            + value.getClass().getSimpleName());
        }
        return s;
    }
}
