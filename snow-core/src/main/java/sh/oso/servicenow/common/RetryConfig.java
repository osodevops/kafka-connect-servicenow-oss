package sh.oso.servicenow.common;

import java.time.Duration;

/**
 * Bounds for {@link RetryPolicy}: at most {@code maxAttempts} calls in total, never more than
 * {@code maxElapsed} of wall time including sleeps, full-jitter exponential backoff between {@code
 * initialBackoff} and {@code maxBackoff}.
 */
public record RetryConfig(
        int maxAttempts, Duration maxElapsed, Duration initialBackoff, Duration maxBackoff) {

    public RetryConfig {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (maxElapsed == null || maxElapsed.isNegative()) {
            throw new IllegalArgumentException("maxElapsed must be >= 0");
        }
        if (initialBackoff == null || initialBackoff.isNegative()) {
            throw new IllegalArgumentException("initialBackoff must be >= 0");
        }
        if (maxBackoff == null || maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException("maxBackoff must be >= initialBackoff");
        }
    }

    /** 5 attempts, 5 minutes elapsed, 500 ms to 30 s backoff. */
    public static RetryConfig defaults() {
        return new RetryConfig(
                5, Duration.ofMinutes(5), Duration.ofMillis(500), Duration.ofSeconds(30));
    }

    /** A policy that never retries. */
    public static RetryConfig none() {
        return new RetryConfig(1, Duration.ZERO, Duration.ZERO, Duration.ZERO);
    }
}
