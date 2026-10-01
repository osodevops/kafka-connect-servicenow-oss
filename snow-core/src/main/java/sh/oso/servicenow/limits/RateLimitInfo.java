package sh.oso.servicenow.limits;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Throttling hints read from a response: {@code Retry-After} (delta-seconds or HTTP-date) and, when
 * an instance happens to send them, {@code X-RateLimit-Limit}, {@code X-RateLimit-Remaining} and
 * {@code X-RateLimit-Reset}. ServiceNow does not document the {@code X-RateLimit-*} headers
 * universally, so they are parsed opportunistically and never relied upon.
 */
public final class RateLimitInfo {

    private final Instant observedAt;
    private final Duration retryAfter;
    private final Long limit;
    private final Long remaining;
    private final Instant resetAt;

    private RateLimitInfo(
            Instant observedAt, Duration retryAfter, Long limit, Long remaining, Instant resetAt) {
        this.observedAt = observedAt;
        this.retryAfter = retryAfter;
        this.limit = limit;
        this.remaining = remaining;
        this.resetAt = resetAt;
    }

    /** Parses the headers; empty when none of the throttling headers is present. */
    public static Optional<RateLimitInfo> parse(Map<String, List<String>> headers, Instant now) {
        if (headers == null || headers.isEmpty()) {
            return Optional.empty();
        }
        Duration retryAfter =
                first(headers, "Retry-After").map(v -> parseRetryAfter(v, now)).orElse(null);
        Long limit =
                first(headers, "X-RateLimit-Limit").flatMap(RateLimitInfo::parseLong).orElse(null);
        Long remaining =
                first(headers, "X-RateLimit-Remaining")
                        .flatMap(RateLimitInfo::parseLong)
                        .orElse(null);
        Instant resetAt =
                first(headers, "X-RateLimit-Reset").flatMap(v -> parseReset(v, now)).orElse(null);
        if (retryAfter == null && limit == null && remaining == null && resetAt == null) {
            return Optional.empty();
        }
        return Optional.of(new RateLimitInfo(now, retryAfter, limit, remaining, resetAt));
    }

    public Instant observedAt() {
        return observedAt;
    }

    /** The {@code Retry-After} wait as it was when the response was observed. */
    public Optional<Duration> retryAfter() {
        return Optional.ofNullable(retryAfter);
    }

    public OptionalLong limit() {
        return limit == null ? OptionalLong.empty() : OptionalLong.of(limit);
    }

    public OptionalLong remaining() {
        return remaining == null ? OptionalLong.empty() : OptionalLong.of(remaining);
    }

    public Optional<Instant> resetAt() {
        return Optional.ofNullable(resetAt);
    }

    /**
     * How long to wait from {@code now}: the remainder of {@code Retry-After}, or the time until
     * the window resets when the remaining quota is exhausted. Never negative.
     */
    public Optional<Duration> waitFor(Instant now) {
        if (retryAfter != null) {
            Duration remainingWait = retryAfter.minus(Duration.between(observedAt, now));
            return Optional.of(remainingWait.isNegative() ? Duration.ZERO : remainingWait);
        }
        if (remaining != null && remaining <= 0 && resetAt != null) {
            Duration untilReset = Duration.between(now, resetAt);
            return Optional.of(untilReset.isNegative() ? Duration.ZERO : untilReset);
        }
        return Optional.empty();
    }

    /** Fraction of the quota left, when both limit and remaining are known. */
    public Optional<Double> remainingFraction() {
        if (limit == null || remaining == null || limit <= 0) {
            return Optional.empty();
        }
        return Optional.of(Math.max(0d, (double) remaining / (double) limit));
    }

    private static Optional<String> first(Map<String, List<String>> headers, String name) {
        for (Map.Entry<String, List<String>> e : headers.entrySet()) {
            if (e.getKey() != null && e.getKey().equalsIgnoreCase(name)) {
                List<String> values = e.getValue();
                if (values != null && !values.isEmpty() && values.get(0) != null) {
                    return Optional.of(values.get(0).trim());
                }
            }
        }
        return Optional.empty();
    }

    private static Duration parseRetryAfter(String value, Instant now) {
        Optional<Long> seconds = parseLong(value);
        if (seconds.isPresent()) {
            return Duration.ofSeconds(Math.max(0, seconds.get()));
        }
        try {
            ZonedDateTime date = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME);
            Duration until = Duration.between(now, date.toInstant());
            return until.isNegative() ? Duration.ZERO : until;
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static Optional<Instant> parseReset(String value, Instant now) {
        Optional<Long> n = parseLong(value);
        if (n.isEmpty()) {
            try {
                return Optional.of(
                        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                .toInstant());
            } catch (DateTimeParseException e) {
                return Optional.empty();
            }
        }
        long v = n.get();
        // Epoch seconds when it looks like one; otherwise delta seconds from now.
        return Optional.of(v > 1_000_000_000L ? Instant.ofEpochSecond(v) : now.plusSeconds(v));
    }

    private static Optional<Long> parseLong(String value) {
        try {
            return Optional.of(Long.parseLong(value.trim()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        return String.format(
                Locale.ROOT,
                "RateLimitInfo{retryAfter=%s, limit=%s, remaining=%s, resetAt=%s}",
                retryAfter,
                limit,
                remaining,
                resetAt);
    }
}
