package sh.oso.servicenow.http;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The instance's wall clock as inferred from response {@code Date} headers: local time plus the
 * last observed drift. Before any response is seen it equals the local clock. Watermarks are taken
 * from this clock so the safety lag is measured against the server, not the worker.
 */
public final class ServerClock extends Clock {

    private static final Logger LOG = LoggerFactory.getLogger(ServerClock.class);

    private final Clock local;
    private final ZoneId zone;
    private volatile long driftMillis;
    private volatile boolean observed;

    public ServerClock(Clock local) {
        this(local, ZoneOffset.UTC);
    }

    private ServerClock(Clock local, ZoneId zone) {
        this.local = local;
        this.zone = zone;
    }

    /** Records a {@code Date} header; malformed values are ignored. */
    public void observe(String dateHeader) {
        if (dateHeader == null || dateHeader.isBlank()) {
            return;
        }
        try {
            Instant server =
                    ZonedDateTime.parse(dateHeader.trim(), DateTimeFormatter.RFC_1123_DATE_TIME)
                            .toInstant();
            long drift = Duration.between(local.instant(), server).toMillis();
            if (!observed || Math.abs(drift - driftMillis) > 1500) {
                LOG.debug("Server clock drift now {} ms", drift);
            }
            driftMillis = drift;
            observed = true;
        } catch (DateTimeParseException e) {
            LOG.debug("Ignoring unparseable Date header: {}", dateHeader);
        }
    }

    /** Last observed server-minus-local drift in milliseconds (0 until observed). */
    public long driftMillis() {
        return driftMillis;
    }

    public boolean hasObserved() {
        return observed;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId newZone) {
        ServerClock copy = new ServerClock(local, newZone);
        copy.driftMillis = driftMillis;
        copy.observed = observed;
        return copy;
    }

    @Override
    public Instant instant() {
        return local.instant().plusMillis(driftMillis);
    }
}
