package sh.oso.servicenow.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import sh.oso.servicenow.common.IncompatibleOffsetException;

/** Round-trip and rejection laws for {@link SourceOffset}. */
class SourceOffsetCodecProperties {

    @Provide
    Arbitrary<SourceOffset> offsets() {
        Arbitrary<Instant> ts =
                Arbitraries.longs().between(0, 4_102_444_800L).map(Instant::ofEpochSecond);
        Arbitrary<String> sysId =
                Arbitraries.oneOf(
                        Arbitraries.strings()
                                .withCharRange('0', '9')
                                .withCharRange('a', 'f')
                                .ofLength(32),
                        Arbitraries.just(""));
        Arbitrary<String> phase =
                Arbitraries.of(SourceOffset.PHASE_BACKFILL, SourceOffset.PHASE_STREAM);
        Arbitrary<String> fingerprint =
                Arbitraries.strings()
                        .withCharRange('0', '9')
                        .withCharRange('a', 'f')
                        .ofLength(64)
                        .map(h -> "sha256:" + h);
        return Combinators.combine(ts, sysId, phase, fingerprint)
                .as((t, s, p, f) -> new SourceOffset(SourceOffset.CURRENT_VERSION, t, s, p, f));
    }

    @Property
    void roundTripsThroughTheMap(@ForAll("offsets") SourceOffset offset) {
        assertThat(SourceOffset.fromMap(offset.toMap())).isEqualTo(offset);
        assertThat(SourceOffset.of(offset.cursor(), offset.phase(), offset.fingerprint()))
                .isEqualTo(offset);
    }

    @Property
    void everyMapValueIsAString(@ForAll("offsets") SourceOffset offset) {
        Map<String, ?> map = offset.toMap();
        assertThat(map).hasSize(5);
        assertThat(map.values()).allSatisfy(v -> assertThat(v).isInstanceOf(String.class));
        assertThat(map.get(SourceOffset.KEY_VERSION)).isEqualTo("1");
    }

    @Property
    void mutatedMapsAreRejected(
            @ForAll("offsets") SourceOffset offset, @ForAll("mutations") String key) {
        Map<String, Object> map = new HashMap<>(offset.toMap());
        map.remove(key);
        assertThatThrownBy(() -> SourceOffset.fromMap(map))
                .isInstanceOf(IncompatibleOffsetException.class);
        Map<String, Object> wrongType = new HashMap<>(offset.toMap());
        wrongType.put(key, 42L);
        assertThatThrownBy(() -> SourceOffset.fromMap(wrongType))
                .isInstanceOf(IncompatibleOffsetException.class);
    }

    @Provide
    Arbitrary<String> mutations() {
        return Arbitraries.of(
                SourceOffset.KEY_VERSION,
                SourceOffset.KEY_TIMESTAMP,
                SourceOffset.KEY_SYS_ID,
                SourceOffset.KEY_PHASE,
                SourceOffset.KEY_FINGERPRINT);
    }

    @Property
    void unknownVersionsPhasesAndTimestampsAreRejected(@ForAll("offsets") SourceOffset offset) {
        Map<String, Object> v2 = new HashMap<>(offset.toMap());
        v2.put(SourceOffset.KEY_VERSION, "2");
        assertThatThrownBy(() -> SourceOffset.fromMap(v2))
                .isInstanceOf(IncompatibleOffsetException.class)
                .hasMessageContaining("version 2");
        Map<String, Object> badVersion = new HashMap<>(offset.toMap());
        badVersion.put(SourceOffset.KEY_VERSION, "one");
        assertThatThrownBy(() -> SourceOffset.fromMap(badVersion))
                .isInstanceOf(IncompatibleOffsetException.class);
        Map<String, Object> badPhase = new HashMap<>(offset.toMap());
        badPhase.put(SourceOffset.KEY_PHASE, "paused");
        assertThatThrownBy(() -> SourceOffset.fromMap(badPhase))
                .isInstanceOf(IncompatibleOffsetException.class);
        Map<String, Object> badTs = new HashMap<>(offset.toMap());
        badTs.put(SourceOffset.KEY_TIMESTAMP, "2026-09-29T07:30:40Z");
        assertThatThrownBy(() -> SourceOffset.fromMap(badTs))
                .isInstanceOf(IncompatibleOffsetException.class);
        Map<String, Object> blankFp = new HashMap<>(offset.toMap());
        blankFp.put(SourceOffset.KEY_FINGERPRINT, " ");
        assertThatThrownBy(() -> SourceOffset.fromMap(blankFp))
                .isInstanceOf(IncompatibleOffsetException.class);
        assertThatThrownBy(() -> SourceOffset.fromMap(null))
                .isInstanceOf(IncompatibleOffsetException.class);
    }
}
