package sh.oso.servicenow.cursor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoUnit;
import java.util.regex.Pattern;
import sh.oso.servicenow.common.ServiceNowException;

/**
 * Strict {@code yyyy-MM-dd HH:mm:ss} UTC, the format ServiceNow uses for glide_date_time values in
 * Table API responses and encoded queries (the integration user must be on UTC).
 */
public final class SnowTimestamp {

    public static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
                    .withZone(ZoneOffset.UTC)
                    .withResolverStyle(ResolverStyle.STRICT);

    private static final Pattern SHAPE =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    private SnowTimestamp() {}

    /** Parses strictly; blank or malformed input raises a non-retryable exception. */
    public static Instant parse(String text) {
        if (text == null || !SHAPE.matcher(text).matches()) {
            throw new ServiceNowException(
                    "Invalid ServiceNow timestamp '" + text + "': expected yyyy-MM-dd HH:mm:ss");
        }
        try {
            return LocalDateTime.parse(text, FORMAT).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            throw new ServiceNowException(
                    "Invalid ServiceNow timestamp '" + text + "': " + e.getMessage(), e);
        }
    }

    public static boolean isValid(String text) {
        try {
            parse(text);
            return true;
        } catch (ServiceNowException e) {
            return false;
        }
    }

    /** Formats at second precision (sub-second parts are truncated). */
    public static String format(Instant instant) {
        return FORMAT.format(instant.truncatedTo(ChronoUnit.SECONDS));
    }
}
