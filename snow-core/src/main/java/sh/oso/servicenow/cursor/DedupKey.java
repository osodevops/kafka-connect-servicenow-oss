package sh.oso.servicenow.cursor;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** Identity of one observed row version: {@code (sys_updated_on, sys_id, sys_mod_count)}. */
public record DedupKey(Instant ts, String sysId, String modCount) {

    public DedupKey {
        ts = ts == null ? Instant.EPOCH : ts.truncatedTo(ChronoUnit.SECONDS);
        sysId = sysId == null ? "" : sysId;
        modCount = modCount == null ? "" : modCount;
    }

    public static DedupKey of(Cursor cursor, String modCount) {
        return new DedupKey(cursor.ts(), cursor.sysId(), modCount);
    }
}
