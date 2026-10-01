package sh.oso.servicenow.limits;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.common.ServiceNowException;

/**
 * Per-instance bound on in-flight requests, shared by every connector in the JVM that talks to the
 * same instance ({@link #forInstance(String, int)}).
 *
 * <p>Adaptive throttling (AIMD): {@link #onThrottled()} halves the current permits (never below
 * one); every {@value #SUCCESSES_PER_INCREMENT} consecutive successes ({@link #onSuccess()}) add
 * one permit back, never above the configured maximum. Callers decide whether to invoke these hooks
 * (the HTTP client only does so when {@code snow.http.adaptive.throttling=true}).
 */
public final class ConcurrencyLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(ConcurrencyLimiter.class);
    private static final ConcurrentHashMap<String, ConcurrencyLimiter> REGISTRY =
            new ConcurrentHashMap<>();

    public static final int SUCCESSES_PER_INCREMENT = 50;

    private final ResizableSemaphore semaphore;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Object adjustLock = new Object();
    private volatile int maxPermits;
    private volatile int currentPermits;
    private int successes;

    public ConcurrencyLimiter(int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be >= 1");
        }
        this.semaphore = new ResizableSemaphore(permits);
        this.maxPermits = permits;
        this.currentPermits = permits;
    }

    /**
     * The shared limiter for an instance (keyed by host or host:port). A later caller with a
     * different permit count resizes the shared limiter and logs the change.
     */
    public static ConcurrencyLimiter forInstance(String instanceKey, int permits) {
        ConcurrencyLimiter limiter =
                REGISTRY.computeIfAbsent(instanceKey, k -> new ConcurrencyLimiter(permits));
        if (limiter.maxPermits != permits) {
            LOG.info(
                    "Resizing shared concurrency limiter for {} from {} to {} permits",
                    instanceKey,
                    limiter.maxPermits,
                    permits);
            limiter.resize(permits);
        }
        return limiter;
    }

    /** Drops every shared limiter; for tests. */
    public static void clearRegistry() {
        REGISTRY.clear();
    }

    /**
     * Blocks until a permit is available or the timeout elapses. A timeout raises a retryable
     * {@link ServiceNowException}.
     */
    public Permit acquire(Duration timeout) {
        try {
            if (!semaphore.tryAcquire(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                throw new ServiceNowException(
                        "Timed out after "
                                + timeout.toMillis()
                                + " ms waiting for a request permit ("
                                + currentPermits
                                + " in use)",
                        null,
                        true);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceNowException("Interrupted while waiting for a request permit", e);
        }
        inFlight.incrementAndGet();
        return new Permit();
    }

    public int inFlight() {
        return inFlight.get();
    }

    /** Permits currently allowed (after adaptive reductions). */
    public int permits() {
        return currentPermits;
    }

    /** The configured ceiling. */
    public int maxPermits() {
        return maxPermits;
    }

    /** Sets both the ceiling and the current permits. */
    public void resize(int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be >= 1");
        }
        synchronized (adjustLock) {
            maxPermits = permits;
            setCurrent(permits);
            successes = 0;
        }
    }

    /** Multiplicative decrease after a 429: halves the permits, never below one. */
    public void onThrottled() {
        synchronized (adjustLock) {
            successes = 0;
            int next = Math.max(1, currentPermits / 2);
            if (next != currentPermits) {
                LOG.info(
                        "Throttled by the instance; reducing concurrency {} -> {}",
                        currentPermits,
                        next);
                setCurrent(next);
            }
        }
    }

    /** Additive increase: one permit back per {@value #SUCCESSES_PER_INCREMENT} successes. */
    public void onSuccess() {
        if (currentPermits >= maxPermits) {
            return;
        }
        synchronized (adjustLock) {
            if (currentPermits >= maxPermits) {
                return;
            }
            if (++successes >= SUCCESSES_PER_INCREMENT) {
                successes = 0;
                setCurrent(currentPermits + 1);
                LOG.debug("Recovered one permit; concurrency now {}", currentPermits);
            }
        }
    }

    private void setCurrent(int next) {
        int delta = next - currentPermits;
        if (delta > 0) {
            semaphore.release(delta);
        } else if (delta < 0) {
            semaphore.reducePermits(-delta);
        }
        currentPermits = next;
    }

    /** A held permit; {@link #close()} releases it exactly once. */
    public final class Permit implements AutoCloseable {
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit() {}

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                inFlight.decrementAndGet();
                semaphore.release();
            }
        }
    }

    private static final class ResizableSemaphore extends Semaphore {
        ResizableSemaphore(int permits) {
            super(permits, true);
        }

        @Override
        protected void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }
}
