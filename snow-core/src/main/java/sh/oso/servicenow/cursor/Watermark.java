package sh.oso.servicenow.cursor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * The sweep high-water mark: server time minus a safety lag, truncated to the second so it is a
 * valid encoded-query timestamp and the current (still open) second is never closed early.
 */
public final class Watermark {

    private final Clock serverClock;

    public Watermark(Clock serverClock) {
        this.serverClock = Objects.requireNonNull(serverClock, "serverClock");
    }

    public Instant hi(Duration safetyLag) {
        Duration lag = safetyLag == null ? Duration.ZERO : safetyLag;
        return serverClock.instant().minus(lag).truncatedTo(ChronoUnit.SECONDS);
    }

    public Instant now() {
        return serverClock.instant();
    }
}
