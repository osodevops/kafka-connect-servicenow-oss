package sh.oso.servicenow.cursor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/** Total-order laws for {@link Cursor}. */
class CursorProperties {

    @Provide
    Arbitrary<Cursor> cursors() {
        Arbitrary<Instant> ts =
                Arbitraries.longs().between(0, 4_102_444_800L).map(Instant::ofEpochSecond);
        Arbitrary<String> sysId =
                Arbitraries.oneOf(
                        Arbitraries.strings()
                                .withCharRange('0', '9')
                                .withCharRange('a', 'f')
                                .ofLength(32),
                        Arbitraries.strings()
                                .withCharRange('0', '9')
                                .withCharRange('a', 'f')
                                .ofMinLength(0)
                                .ofMaxLength(3),
                        Arbitraries.just(""));
        return Combinators.combine(ts, sysId).as(Cursor::new);
    }

    @Property
    void antisymmetric(@ForAll("cursors") Cursor a, @ForAll("cursors") Cursor b) {
        assertThat(Integer.signum(a.compareTo(b))).isEqualTo(-Integer.signum(b.compareTo(a)));
    }

    @Property
    void transitive(
            @ForAll("cursors") Cursor a, @ForAll("cursors") Cursor b, @ForAll("cursors") Cursor c) {
        if (a.compareTo(b) <= 0 && b.compareTo(c) <= 0) {
            assertThat(a.compareTo(c)).isLessThanOrEqualTo(0);
        }
    }

    @Property
    void consistentWithEquals(@ForAll("cursors") Cursor a, @ForAll("cursors") Cursor b) {
        assertThat(a.compareTo(b) == 0).isEqualTo(a.equals(b));
        if (a.equals(b)) {
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }
    }

    @Provide
    Arbitrary<List<Cursor>> cursorLists() {
        return cursors().list().ofMaxSize(40);
    }

    @Property
    void sortOrderMatchesTheTimestampThenSysIdTuple(@ForAll("cursorLists") List<Cursor> cursors) {
        List<Cursor> natural = new ArrayList<>(cursors);
        natural.sort(Comparator.naturalOrder());
        List<Cursor> tuple = new ArrayList<>(cursors);
        tuple.sort(Comparator.comparing(Cursor::ts).thenComparing(Cursor::sysId));
        assertThat(natural).isEqualTo(tuple);
        for (int i = 1; i < natural.size(); i++) {
            Cursor prev = natural.get(i - 1);
            Cursor next = natural.get(i);
            boolean ordered =
                    prev.ts().isBefore(next.ts())
                            || (prev.ts().equals(next.ts())
                                    && prev.sysId().compareTo(next.sysId()) <= 0);
            assertThat(ordered).isTrue();
        }
    }

    @Property
    void secondOfTruncationNeverReorders(@ForAll("cursors") Cursor a) {
        Cursor withNanos = new Cursor(a.ts().plusMillis(999), a.sysId());
        assertThat(withNanos).isEqualByComparingTo(a).isEqualTo(a);
    }
}
