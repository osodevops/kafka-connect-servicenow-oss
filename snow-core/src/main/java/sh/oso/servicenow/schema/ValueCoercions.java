package sh.oso.servicenow.schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Date;
import java.util.Locale;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.cursor.SnowTimestamp;

/**
 * Conversions between ServiceNow's string representations and Connect values, in both directions.
 * ServiceNow sends every field as a string: {@code ""} is the empty value and maps to null for
 * every non-string schema; booleans are {@code true}/{@code false} (also {@code 1}/{@code 0});
 * dates are {@code yyyy-MM-dd} and datetimes {@code yyyy-MM-dd HH:mm:ss} UTC.
 */
public final class ValueCoercions {

    public static final DateTimeFormatter DATE = DateTimeFormatter.ISO_LOCAL_DATE;
    public static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");

    private ValueCoercions() {}

    /** ServiceNow string to the Connect value for {@code schema}; throws on malformed input. */
    public static Object toConnect(String raw, Schema schema) {
        if (raw == null) {
            return null;
        }
        try {
            return convert(raw, schema);
        } catch (IllegalArgumentException e) {
            throw new ServiceNowException(
                    "Cannot coerce '" + raw + "' to " + describe(schema) + ": " + e.getMessage(),
                    e);
        } catch (ServiceNowException e) {
            throw new ServiceNowException(
                    "Cannot coerce '" + raw + "' to " + describe(schema) + ": " + e.getMessage(),
                    e);
        }
    }

    private static Object convert(String raw, Schema schema) {
        String logical = schema.name();
        if (logical != null) {
            switch (logical) {
                case Timestamp.LOGICAL_NAME -> {
                    return raw.isEmpty() ? null : Date.from(SnowTimestamp.parse(raw));
                }
                case org.apache.kafka.connect.data.Date.LOGICAL_NAME -> {
                    return raw.isEmpty() ? null : parseDate(raw);
                }
                case Time.LOGICAL_NAME -> {
                    return raw.isEmpty() ? null : parseTime(raw);
                }
                case Decimal.LOGICAL_NAME -> {
                    if (raw.isEmpty()) {
                        return null;
                    }
                    int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
                    return parseDecimal(raw).setScale(scale, RoundingMode.HALF_UP);
                }
                default -> {
                    // fall through to the primitive type
                }
            }
        }
        {
            return switch (schema.type()) {
                case STRING -> raw;
                case BOOLEAN -> raw.isEmpty() ? null : parseBoolean(raw);
                case INT8 -> raw.isEmpty() ? null : (byte) parseLong(raw);
                case INT16 -> raw.isEmpty() ? null : (short) parseLong(raw);
                case INT32 -> raw.isEmpty() ? null : (int) parseLong(raw);
                case INT64 -> raw.isEmpty() ? null : parseLong(raw);
                case FLOAT32 -> raw.isEmpty() ? null : (float) Double.parseDouble(raw.trim());
                case FLOAT64 -> raw.isEmpty() ? null : Double.parseDouble(raw.trim());
                case BYTES -> raw.isEmpty() ? null : Base64.getDecoder().decode(raw);
                default ->
                        throw new IllegalArgumentException(
                                "unsupported Connect schema type " + schema.type());
            };
        }
    }

    /** Connect value to the string ServiceNow expects for the given (nullable) schema. */
    public static String toServiceNow(Object value, Schema schema) {
        if (value == null) {
            return null;
        }
        String logical = schema != null ? schema.name() : null;
        if (logical != null) {
            switch (logical) {
                case Timestamp.LOGICAL_NAME -> {
                    return SnowTimestamp.format(((Date) value).toInstant());
                }
                case org.apache.kafka.connect.data.Date.LOGICAL_NAME -> {
                    return DATE.format(
                            ((Date) value).toInstant().atOffset(ZoneOffset.UTC).toLocalDate());
                }
                case Time.LOGICAL_NAME -> {
                    return TIME.format(
                            ((Date) value).toInstant().atOffset(ZoneOffset.UTC).toLocalTime());
                }
                case Decimal.LOGICAL_NAME -> {
                    return ((BigDecimal) value).toPlainString();
                }
                default -> {
                    // fall through
                }
            }
        }
        return toServiceNow(value);
    }

    /** Schema-free rendering for schemaless sinks. */
    public static String toServiceNow(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof Boolean b) {
            return b ? "true" : "false";
        }
        if (value instanceof BigDecimal d) {
            return d.toPlainString();
        }
        if (value instanceof Date d) {
            return SnowTimestamp.format(d.toInstant());
        }
        if (value instanceof Instant i) {
            return SnowTimestamp.format(i);
        }
        if (value instanceof LocalDate ld) {
            return DATE.format(ld);
        }
        if (value instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof ByteBuffer buf) {
            ByteBuffer dup = buf.duplicate();
            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            if (d == Math.rint(d) && !Double.isInfinite(d) && Math.abs(d) < 1e15) {
                return Long.toString((long) d);
            }
            return BigDecimal.valueOf(d).toPlainString();
        }
        return String.valueOf(value);
    }

    static boolean parseBoolean(String raw) {
        String v = raw.trim().toLowerCase(Locale.ROOT);
        return switch (v) {
            case "true", "1", "yes" -> true;
            case "false", "0", "no" -> false;
            default -> throw new IllegalArgumentException("not a boolean");
        };
    }

    private static long parseLong(String raw) {
        String v = raw.trim();
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            // ServiceNow may render integers as "12.0" for some numeric field types.
            BigDecimal d = new BigDecimal(v);
            return d.longValueExact();
        }
    }

    private static BigDecimal parseDecimal(String raw) {
        return new BigDecimal(raw.trim().replace(",", ""));
    }

    private static Date parseDate(String raw) {
        try {
            LocalDate d = LocalDate.parse(raw.trim(), DATE);
            return Date.from(d.atStartOfDay(ZoneOffset.UTC).toInstant());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("not a yyyy-MM-dd date", e);
        }
    }

    private static Date parseTime(String raw) {
        try {
            java.time.LocalTime t = java.time.LocalTime.parse(raw.trim(), TIME);
            return Date.from(t.atDate(LocalDate.EPOCH).toInstant(ZoneOffset.UTC));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("not an HH:mm:ss time", e);
        }
    }

    private static String describe(Schema schema) {
        return schema.name() != null ? schema.name() : schema.type().getName();
    }
}
