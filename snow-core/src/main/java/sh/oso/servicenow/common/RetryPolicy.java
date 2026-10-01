package sh.oso.servicenow.common;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded retry with full-jitter exponential backoff.
 *
 * <p>Only failures that {@link ErrorClassifier#isRetryable(Throwable)} accepts are retried, and an
 * ambiguous failure is retried only when the caller declared the operation idempotent. A {@code
 * Retry-After} carried by a {@link ServiceNowApiException} replaces the computed backoff. Retrying
 * stops at {@code maxAttempts} or when the next sleep would exceed {@code maxElapsed}; both raise a
 * {@link RetryExhaustedException} (retryable, so the task boundary can surface a {@code
 * RetriableException}).
 *
 * <p>Time is injectable: {@link Sleeper} performs the wait and {@link Ticker} measures elapsed
 * time, so tests run on a virtual clock.
 */
public final class RetryPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(RetryPolicy.class);

    /** Performs a wait. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration d) throws InterruptedException;
    }

    /** Monotonic time source in nanoseconds. */
    @FunctionalInterface
    public interface Ticker {
        long nanos();
    }

    /** Told about every retry just before its wait, so callers can keep counters. */
    @FunctionalInterface
    public interface Observer {
        /**
         * @param opName the operation being retried
         * @param attempt the attempt that just failed (1-based)
         * @param wait how long the policy is about to wait before the next attempt
         * @param failure what the attempt failed with
         */
        void onRetry(String opName, int attempt, Duration wait, Exception failure);
    }

    public static final Observer NO_OBSERVER = (op, attempt, wait, failure) -> {};

    public static final Sleeper THREAD_SLEEPER =
            d -> {
                if (!d.isZero() && !d.isNegative()) {
                    Thread.sleep(d.toMillis(), d.toNanosPart() % 1_000_000);
                }
            };

    public static final Ticker SYSTEM_TICKER = System::nanoTime;

    private final RetryConfig config;
    private final Sleeper sleeper;
    private final Ticker ticker;
    private final Observer observer;

    public RetryPolicy(RetryConfig config) {
        this(config, THREAD_SLEEPER, SYSTEM_TICKER);
    }

    public RetryPolicy(RetryConfig config, Sleeper sleeper, Ticker ticker) {
        this(config, sleeper, ticker, NO_OBSERVER);
    }

    private RetryPolicy(RetryConfig config, Sleeper sleeper, Ticker ticker, Observer observer) {
        this.config = config;
        this.sleeper = sleeper;
        this.ticker = ticker;
        this.observer = observer == null ? NO_OBSERVER : observer;
    }

    /** The same policy (config, sleeper and ticker) reporting every retry to {@code observer}. */
    public RetryPolicy withObserver(Observer observer) {
        return new RetryPolicy(config, sleeper, ticker, observer);
    }

    public RetryConfig config() {
        return config;
    }

    /**
     * Runs {@code call} until it succeeds, fails with a non-retryable error, or the caps are hit.
     *
     * @param idempotent whether an ambiguous failure (request may have been applied) may be retried
     */
    public <T> T execute(String opName, Callable<T> call, boolean idempotent) {
        long start = ticker.nanos();
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return call.call();
            } catch (Exception e) {
                boolean retryable =
                        ErrorClassifier.isRetryable(e)
                                && (idempotent || !ErrorClassifier.isAmbiguous(e));
                if (!retryable) {
                    throw toRuntime(e);
                }
                Duration elapsed = Duration.ofNanos(ticker.nanos() - start);
                if (attempt >= config.maxAttempts()) {
                    LOG.warn(
                            "{} failed on attempt {}/{}; giving up: {}",
                            opName,
                            attempt,
                            config.maxAttempts(),
                            e.getMessage());
                    throw new RetryExhaustedException(
                            opName,
                            attempt,
                            elapsed,
                            "attempt limit " + config.maxAttempts() + " reached",
                            e);
                }
                final int attemptNo = attempt;
                Duration wait = retryAfter(e).orElseGet(() -> backoff(attemptNo));
                if (elapsed.plus(wait).compareTo(config.maxElapsed()) > 0) {
                    LOG.warn(
                            "{} failed on attempt {}; next wait of {} ms would exceed the {} ms budget: {}",
                            opName,
                            attempt,
                            wait.toMillis(),
                            config.maxElapsed().toMillis(),
                            e.getMessage());
                    throw new RetryExhaustedException(
                            opName,
                            attempt,
                            elapsed,
                            "next wait of "
                                    + wait.toMillis()
                                    + " ms would exceed the "
                                    + config.maxElapsed().toMillis()
                                    + " ms budget",
                            e);
                }
                LOG.info(
                        "{} failed on attempt {}/{}; retrying in {} ms: {}",
                        opName,
                        attempt,
                        config.maxAttempts(),
                        wait.toMillis(),
                        e.getMessage());
                observer.onRetry(opName, attempt, wait, e);
                sleep(wait);
            }
        }
    }

    /** Full jitter: uniform in [0, min(maxBackoff, initial * 2^(attempt-1))]. */
    Duration backoff(int attempt) {
        long initial = config.initialBackoff().toMillis();
        long cap = config.maxBackoff().toMillis();
        int exponent = Math.min(Math.max(attempt - 1, 0), 30);
        long ceiling = Math.min(cap, initial << exponent);
        if (initial > 0 && ceiling < initial) {
            ceiling = cap; // overflow guard
        }
        long millis = ceiling <= 0 ? 0 : ThreadLocalRandom.current().nextLong(0, ceiling + 1);
        return Duration.ofMillis(millis);
    }

    private static Optional<Duration> retryAfter(Exception e) {
        if (e instanceof ServiceNowApiException api) {
            return api.rateLimit().flatMap(r -> r.retryAfter());
        }
        return Optional.empty();
    }

    private void sleep(Duration wait) {
        try {
            sleeper.sleep(wait);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ServiceNowException("Interrupted during retry backoff", ie, false);
        }
    }

    private static RuntimeException toRuntime(Exception e) {
        return e instanceof RuntimeException re
                ? re
                : new ServiceNowException(e.getMessage(), e, false);
    }
}
