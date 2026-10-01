package sh.oso.servicenow.limits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowException;

class ConcurrencyLimiterTest {

    @BeforeEach
    @AfterEach
    void clear() {
        ConcurrencyLimiter.clearRegistry();
    }

    @Test
    void boundsInFlightAndTimesOutWithARetryableFailure() {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(2);
        ConcurrencyLimiter.Permit a = limiter.acquire(Duration.ofSeconds(1));
        ConcurrencyLimiter.Permit b = limiter.acquire(Duration.ofSeconds(1));
        assertThat(limiter.inFlight()).isEqualTo(2);
        assertThatThrownBy(() -> limiter.acquire(Duration.ofMillis(50)))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("permit")
                .satisfies(e -> assertThat(((ServiceNowException) e).isRetryable()).isTrue());
        a.close();
        a.close();
        assertThat(limiter.inFlight()).isEqualTo(1);
        try (ConcurrencyLimiter.Permit c = limiter.acquire(Duration.ofMillis(50))) {
            assertThat(c).isNotNull();
            assertThat(limiter.inFlight()).isEqualTo(2);
        }
        b.close();
        assertThat(limiter.inFlight()).isZero();
        assertThatThrownBy(() -> new ConcurrencyLimiter(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter.resize(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void neverExceedsThePermitCountUnderContention() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger max = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(12);
        CountDownLatch done = new CountDownLatch(60);
        try {
            for (int i = 0; i < 60; i++) {
                pool.submit(
                        () -> {
                            try (ConcurrencyLimiter.Permit p =
                                    limiter.acquire(Duration.ofSeconds(5))) {
                                int now = concurrent.incrementAndGet();
                                max.accumulateAndGet(now, Math::max);
                                Thread.sleep(2);
                                concurrent.decrementAndGet();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                done.countDown();
                            }
                        });
            }
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
        assertThat(max.get()).isBetween(1, 3);
        assertThat(limiter.inFlight()).isZero();
    }

    @Test
    void aimdHalvesOnThrottleAndRecoversOnePermitPerFiftySuccesses() {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(8);
        limiter.onThrottled();
        assertThat(limiter.permits()).isEqualTo(4);
        limiter.onThrottled();
        limiter.onThrottled();
        limiter.onThrottled();
        assertThat(limiter.permits()).isEqualTo(1);
        for (int i = 0; i < ConcurrencyLimiter.SUCCESSES_PER_INCREMENT - 1; i++) {
            limiter.onSuccess();
        }
        assertThat(limiter.permits()).isEqualTo(1);
        limiter.onSuccess();
        assertThat(limiter.permits()).isEqualTo(2);
        for (int i = 0; i < ConcurrencyLimiter.SUCCESSES_PER_INCREMENT * 10; i++) {
            limiter.onSuccess();
        }
        assertThat(limiter.permits()).isEqualTo(8);
        assertThat(limiter.maxPermits()).isEqualTo(8);
        limiter.onSuccess();
        assertThat(limiter.permits()).isEqualTo(8);
    }

    @Test
    void reducedPermitsAreEnforcedImmediatelyAndResizeRestoresThem() {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(2);
        limiter.onThrottled();
        try (ConcurrencyLimiter.Permit p = limiter.acquire(Duration.ofMillis(50))) {
            assertThatThrownBy(() -> limiter.acquire(Duration.ofMillis(20)))
                    .isInstanceOf(ServiceNowException.class);
        }
        limiter.resize(3);
        assertThat(limiter.permits()).isEqualTo(3);
        ConcurrencyLimiter.Permit a = limiter.acquire(Duration.ofMillis(50));
        ConcurrencyLimiter.Permit b = limiter.acquire(Duration.ofMillis(50));
        ConcurrencyLimiter.Permit c = limiter.acquire(Duration.ofMillis(50));
        a.close();
        b.close();
        c.close();
    }

    @Test
    void registrySharesPerInstanceAndResizesOnDifferentConfig() {
        ConcurrencyLimiter a = ConcurrencyLimiter.forInstance("acme.service-now.com", 4);
        ConcurrencyLimiter b = ConcurrencyLimiter.forInstance("acme.service-now.com", 4);
        assertThat(a).isSameAs(b);
        ConcurrencyLimiter c = ConcurrencyLimiter.forInstance("acme.service-now.com", 6);
        assertThat(c).isSameAs(a);
        assertThat(a.maxPermits()).isEqualTo(6);
        assertThat(ConcurrencyLimiter.forInstance("other", 1)).isNotSameAs(a);
    }

    @Test
    void interruptedAcquireRestoresTheFlag() {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(1);
        ConcurrencyLimiter.Permit held = limiter.acquire(Duration.ofSeconds(1));
        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> limiter.acquire(Duration.ofSeconds(1)))
                .hasMessageContaining("Interrupted");
        assertThat(Thread.interrupted()).isTrue();
        held.close();
    }
}
