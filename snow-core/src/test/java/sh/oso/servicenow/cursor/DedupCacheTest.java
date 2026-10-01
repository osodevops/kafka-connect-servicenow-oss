package sh.oso.servicenow.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class DedupCacheTest {

    private static DedupKey key(int i, String mod) {
        return new DedupKey(Instant.ofEpochSecond(i), "id" + i, mod);
    }

    @Test
    void firstSeenIsTrueOnceThenFalse() {
        DedupCache cache = new DedupCache(10);
        assertThat(cache.firstSeen(key(1, "0"))).isTrue();
        assertThat(cache.firstSeen(key(1, "0"))).isFalse();
        assertThat(cache.firstSeen(key(1, "1"))).isTrue();
        assertThat(cache.size()).isEqualTo(2);
        assertThat(cache.contains(key(1, "0"))).isTrue();
    }

    @Test
    void evictsTheOldestBeyondTheBound() {
        DedupCache cache = new DedupCache(3);
        for (int i = 0; i < 5; i++) {
            cache.firstSeen(key(i, "0"));
        }
        assertThat(cache.size()).isEqualTo(3);
        assertThat(cache.firstSeen(key(0, "0"))).isTrue();
        assertThat(cache.firstSeen(key(4, "0"))).isFalse();
        cache.clear();
        assertThat(cache.size()).isZero();
        assertThat(cache.maxEntries()).isEqualTo(3);
        assertThatThrownBy(() -> new DedupCache(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void keyNormalisesNullsAndSubSeconds() {
        DedupKey k = new DedupKey(Instant.parse("2026-09-29T07:30:40.900Z"), null, null);
        assertThat(k).isEqualTo(new DedupKey(Instant.parse("2026-09-29T07:30:40Z"), "", ""));
        assertThat(DedupKey.of(Cursor.of("2026-09-29 07:30:40", "x"), "3"))
                .isEqualTo(new DedupKey(Instant.parse("2026-09-29T07:30:40Z"), "x", "3"));
        assertThat(new DedupKey(null, "a", "b").ts()).isEqualTo(Instant.EPOCH);
    }
}
