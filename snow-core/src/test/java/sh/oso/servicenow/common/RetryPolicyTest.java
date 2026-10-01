package sh.oso.servicenow.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.testing.VirtualTime;

class RetryPolicyTest {

    private final VirtualTime time = new VirtualTime();

    private static RetryConfig config(int attempts, Duration elapsed) {
        return new RetryConfig(attempts, elapsed, Duration.ofMillis(100), Duration.ofSeconds(1));
    }

    private static ServiceNowException transient_() {
        return new ServiceNowException("transient", null, true);
    }

    @Test
    void retriesRetryableFailuresUntilSuccess() {
        RetryPolicy policy = time.policy(config(5, Duration.ofMinutes(1)));
        AtomicInteger attempts = new AtomicInteger();
        String result =
                policy.execute(
                        "op",
                        () -> {
                            if (attempts.incrementAndGet() < 3) {
                                throw transient_();
                            }
                            return "ok";
                        },
                        true);
        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(3);
        assertThat(time.sleeps()).hasSize(2);
    }

    @Test
    void failsFastOnNonRetryableAndWrapsCheckedExceptions() {
        RetryPolicy policy = time.policy(config(5, Duration.ofMinutes(1)));
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(
                        () ->
                                policy.execute(
                                        "op",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new ServiceNowException("fatal");
                                        },
                                        true))
                .isInstanceOf(ServiceNowException.class)
                .hasMessage("fatal");
        assertThat(attempts.get()).isEqualTo(1);
        assertThatThrownBy(
                        () ->
                                policy.execute(
                                        "op",
                                        () -> {
                                            throw new Exception("checked");
                                        },
                                        true))
                .isInstanceOf(ServiceNowException.class)
                .hasMessage("checked");
        assertThat(time.sleeps()).isEmpty();
    }

    @Test
    void attemptsCapRaisesRetryExhaustedWithTheLastFailureAsCause() {
        RetryPolicy policy = time.policy(config(3, Duration.ofMinutes(1)));
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(
                        () ->
                                policy.execute(
                                        "op",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw transient_();
                                        },
                                        true))
                .isInstanceOf(RetryExhaustedException.class)
                .hasMessageContaining("3 attempt(s)")
                .satisfies(
                        e -> {
                            RetryExhaustedException ex = (RetryExhaustedException) e;
                            assertThat(ex.isRetryable()).isTrue();
                            assertThat(ex.attempts()).isEqualTo(3);
                            assertThat(ex.elapsed()).isEqualTo(time.totalSlept());
                            assertThat(ex.getCause()).hasMessage("transient");
                        });
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void elapsedCapStopsWhenTheNextSleepWouldExceedTheBudget() {
        RetryConfig cfg =
                new RetryConfig(
                        100,
                        Duration.ofMillis(250),
                        Duration.ofMillis(100),
                        Duration.ofMillis(100));
        RetryPolicy policy = time.policy(cfg);
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(
                        () ->
                                policy.execute(
                                        "op",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw transient_();
                                        },
                                        true))
                .isInstanceOf(RetryExhaustedException.class)
                .hasMessageContaining("budget");
        assertThat(time.totalSlept()).isLessThanOrEqualTo(Duration.ofMillis(250));
        assertThat(attempts.get()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void retryAfterOverridesTheBackoff() {
        RetryPolicy policy = time.policy(config(5, Duration.ofMinutes(1)));
        AtomicInteger attempts = new AtomicInteger();
        policy.execute(
                "op",
                () -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw ErrorClassifier.classify(
                                429, "", Map.of("Retry-After", List.of("3")), "r", Instant.EPOCH);
                    }
                    return null;
                },
                true);
        assertThat(time.sleeps()).containsExactly(Duration.ofSeconds(3));
    }

    @Test
    void ambiguousFailuresAreRetriedOnlyWhenIdempotent() {
        RetryPolicy policy = time.policy(config(5, Duration.ofMinutes(1)));
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(
                        () ->
                                policy.execute(
                                        "post",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new IOException("reset after send");
                                        },
                                        false))
                .isInstanceOf(ServiceNowException.class)
                .hasMessage("reset after send");
        assertThat(attempts.get()).isEqualTo(1);
        AtomicInteger again = new AtomicInteger();
        String ok =
                policy.execute(
                        "patch",
                        () -> {
                            if (again.incrementAndGet() == 1) {
                                throw new IOException("reset after send");
                            }
                            return "ok";
                        },
                        true);
        assertThat(ok).isEqualTo("ok");
        assertThat(again.get()).isEqualTo(2);
        AtomicInteger flagged = new AtomicInteger();
        assertThatThrownBy(
                        () ->
                                policy.execute(
                                        "post",
                                        () -> {
                                            flagged.incrementAndGet();
                                            throw new ServiceNowException(
                                                    "ambiguous", null, true, true);
                                        },
                                        false))
                .isInstanceOf(ServiceNowException.class);
        assertThat(flagged.get()).isEqualTo(1);
    }

    @Test
    void backoffIsFullJitterBoundedByTheExponentialCeiling() {
        RetryPolicy policy =
                new RetryPolicy(
                        new RetryConfig(
                                10,
                                Duration.ofMinutes(1),
                                Duration.ofMillis(100),
                                Duration.ofMillis(1000)));
        for (int attempt = 1; attempt <= 8; attempt++) {
            long ceiling = Math.min(1000, 100L << (attempt - 1));
            for (int i = 0; i < 200; i++) {
                Duration d = policy.backoff(attempt);
                assertThat(d.toMillis()).isBetween(0L, ceiling);
            }
        }
        assertThat(policy.backoff(40).toMillis()).isBetween(0L, 1000L);
        RetryPolicy none = new RetryPolicy(RetryConfig.none());
        assertThat(none.backoff(1)).isEqualTo(Duration.ZERO);
        assertThat(none.config().maxAttempts()).isEqualTo(1);
        assertThat(RetryConfig.defaults().maxAttempts()).isEqualTo(5);
        assertThatThrownBy(() -> new RetryConfig(0, Duration.ZERO, Duration.ZERO, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new RetryConfig(
                                        1,
                                        Duration.ZERO,
                                        Duration.ofSeconds(2),
                                        Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new RetryConfig(
                                        1, Duration.ofSeconds(-1), Duration.ZERO, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new RetryConfig(
                                        1, Duration.ZERO, Duration.ofSeconds(-1), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void interruptedSleepSurfacesAndRestoresTheFlag() {
        RetryPolicy policy =
                new RetryPolicy(
                        config(5, Duration.ofMinutes(1)),
                        d -> {
                            throw new InterruptedException();
                        },
                        time);
        assertThatThrownBy(
                        () ->
                                policy.execute(
                                        "op",
                                        () -> {
                                            throw transient_();
                                        },
                                        true))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("Interrupted");
        assertThat(Thread.interrupted()).isTrue();
    }

    @Test
    void realSleeperActuallyWaits() throws Exception {
        long start = System.nanoTime();
        RetryPolicy.THREAD_SLEEPER.sleep(Duration.ofMillis(20));
        RetryPolicy.THREAD_SLEEPER.sleep(Duration.ZERO);
        assertThat(Duration.ofNanos(System.nanoTime() - start))
                .isGreaterThanOrEqualTo(Duration.ofMillis(15));
        assertThat(RetryPolicy.SYSTEM_TICKER.nanos()).isNotZero();
    }
}
