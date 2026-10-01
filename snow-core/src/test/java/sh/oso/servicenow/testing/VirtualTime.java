package sh.oso.servicenow.testing;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import sh.oso.servicenow.common.RetryConfig;
import sh.oso.servicenow.common.RetryPolicy;

/**
 * A {@link RetryPolicy.Sleeper} that records sleeps instead of waiting and a {@link
 * RetryPolicy.Ticker} that advances by exactly those sleeps, so retry behaviour is deterministic.
 */
public final class VirtualTime implements RetryPolicy.Sleeper, RetryPolicy.Ticker {

    private final List<Duration> sleeps = new CopyOnWriteArrayList<>();
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public void sleep(Duration d) {
        sleeps.add(d);
        nanos.addAndGet(d.toNanos());
    }

    @Override
    public long nanos() {
        return nanos.get();
    }

    /** Moves the clock without recording a sleep. */
    public void advance(Duration d) {
        nanos.addAndGet(d.toNanos());
    }

    public List<Duration> sleeps() {
        return List.copyOf(sleeps);
    }

    public Duration totalSlept() {
        Duration total = Duration.ZERO;
        for (Duration d : sleeps) {
            total = total.plus(d);
        }
        return total;
    }

    public RetryPolicy policy(RetryConfig config) {
        return new RetryPolicy(config, this, this);
    }
}
