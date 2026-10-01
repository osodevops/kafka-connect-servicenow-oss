package sh.oso.servicenow.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowException;

class SnowTimestampTest {

    @Test
    void parsesAndFormatsUtcAtSecondPrecision() {
        Instant t = SnowTimestamp.parse("2026-09-29 07:30:40");
        assertThat(t).isEqualTo(Instant.parse("2026-09-29T07:30:40Z"));
        assertThat(SnowTimestamp.format(t)).isEqualTo("2026-09-29 07:30:40");
        assertThat(SnowTimestamp.format(Instant.parse("2026-09-29T07:30:40.999Z")))
                .isEqualTo("2026-09-29 07:30:40");
        assertThat(SnowTimestamp.format(Instant.EPOCH)).isEqualTo("1970-01-01 00:00:00");
    }

    @Test
    void rejectsAnythingThatIsNotTheExactShape() {
        for (String bad :
                new String[] {
                    null,
                    "",
                    "2026-09-29T07:30:40Z",
                    "2026-09-29 07:30",
                    "2026-9-29 07:30:40",
                    "2026-02-30 00:00:00",
                    "2026-09-29 24:00:00",
                    " 2026-09-29 07:30:40",
                    "2026-09-29 07:30:40.123"
                }) {
            assertThatThrownBy(() -> SnowTimestamp.parse(bad))
                    .isInstanceOf(ServiceNowException.class);
            assertThat(SnowTimestamp.isValid(bad)).isFalse();
        }
        assertThat(SnowTimestamp.isValid("2026-12-31 23:59:59")).isTrue();
    }

    @Test
    void cursorNormalisesAndCompares() {
        Cursor a = Cursor.of("2026-09-29 07:30:40", "a");
        Cursor b = new Cursor(Instant.parse("2026-09-29T07:30:40.500Z"), "b");
        Cursor c = new Cursor(Instant.parse("2026-09-29T07:30:41Z"), null);
        assertThat(a).isLessThan(b);
        assertThat(b).isLessThan(c);
        assertThat(c.sysId()).isEmpty();
        assertThat(c.isAfter(b)).isTrue();
        assertThat(a.formattedTs()).isEqualTo("2026-09-29 07:30:40");
        assertThat(a).hasToString("(2026-09-29 07:30:40, a)");
        assertThatThrownBy(() -> new Cursor(null, "x"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
