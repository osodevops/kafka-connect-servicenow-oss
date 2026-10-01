package sh.oso.servicenow.testing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * A clock tests move by hand; drives the fake's {@code sys_updated_on}, Date headers and tokens.
 */
public final class MutableClock extends Clock {

    private volatile Instant now;
    private final ZoneId zone;

    private MutableClock(Instant now, ZoneId zone) {
        this.now = Objects.requireNonNull(now, "now");
        this.zone = zone;
    }

    public static MutableClock at(Instant instant) {
        return new MutableClock(instant, ZoneOffset.UTC);
    }

    /** Starts at the real wall clock, truncated to the second. */
    public static MutableClock startingNow() {
        return at(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS));
    }

    public void set(Instant instant) {
        this.now = Objects.requireNonNull(instant, "instant");
    }

    public void advance(Duration d) {
        this.now = now.plus(d);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId newZone) {
        return new MutableClock(now, newZone);
    }

    @Override
    public Instant instant() {
        return now;
    }
}
