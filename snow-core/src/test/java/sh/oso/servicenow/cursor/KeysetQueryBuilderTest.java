package sh.oso.servicenow.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.table.EncodedQuery;

class KeysetQueryBuilderTest {

    private static final Instant T0 = Instant.parse("2026-09-29T07:30:40Z");
    private static final Instant HI = Instant.parse("2026-09-29T07:31:00Z");
    private static final String SYS_ID = "abc0000000000000000000000000000f";

    private final KeysetQueryBuilder builder =
            new KeysetQueryBuilder(EncodedQuery.of("active=true"), "sys_updated_on", "sys_id");

    @Test
    void nextBucketsProducesTheExactWindowQuery() {
        assertThat(builder.nextBuckets(T0, HI).raw())
                .isEqualTo(
                        "active=true^sys_updated_on>2026-09-29 07:30:40^sys_updated_on<=2026-09-29 07:31:00^ORDERBYsys_updated_on^ORDERBYsys_id");
    }

    @Test
    void drainBucketProducesTheExactSameSecondQuery() {
        assertThat(builder.drainBucket(new Cursor(T0, SYS_ID)).raw())
                .isEqualTo(
                        "active=true^sys_updated_on=2026-09-29 07:30:40^sys_id>"
                                + SYS_ID
                                + "^ORDERBYsys_id");
    }

    @Test
    void everyNqBranchGetsThePredicatesAndOrderingIsAppendedOnce() {
        KeysetQueryBuilder nq =
                new KeysetQueryBuilder(
                        EncodedQuery.of("state=1^NQpriority=1"), "sys_updated_on", "sys_id");
        assertThat(nq.nextBuckets(T0, HI).raw())
                .isEqualTo(
                        "state=1^sys_updated_on>2026-09-29 07:30:40^sys_updated_on<=2026-09-29 07:31:00"
                                + "^NQpriority=1^sys_updated_on>2026-09-29 07:30:40^sys_updated_on<=2026-09-29 07:31:00"
                                + "^ORDERBYsys_updated_on^ORDERBYsys_id");
        assertThat(nq.drainBucket(new Cursor(T0, SYS_ID)).raw())
                .isEqualTo(
                        "state=1^sys_updated_on=2026-09-29 07:30:40^sys_id>"
                                + SYS_ID
                                + "^NQpriority=1^sys_updated_on=2026-09-29 07:30:40^sys_id>"
                                + SYS_ID
                                + "^ORDERBYsys_id");
    }

    @Test
    void emptyBaseAndCustomCursorFieldsWork() {
        KeysetQueryBuilder view = new KeysetQueryBuilder(EncodedQuery.empty(), "u_updated", "u_id");
        assertThat(view.nextBuckets(T0, HI).raw())
                .isEqualTo(
                        "u_updated>2026-09-29 07:30:40^u_updated<=2026-09-29 07:31:00^ORDERBYu_updated^ORDERBYu_id");
        assertThat(view.drainBucket(Cursor.atSecond(T0)).raw())
                .isEqualTo("u_updated=2026-09-29 07:30:40^u_id>^ORDERBYu_id");
        assertThat(new KeysetQueryBuilder(null).tsField()).isEqualTo("sys_updated_on");
    }

    @Test
    void orderByInTheBaseQueryIsRejected() {
        assertThatThrownBy(
                        () ->
                                new KeysetQueryBuilder(
                                        EncodedQuery.of("active=true^ORDERBYnumber"),
                                        "sys_updated_on",
                                        "sys_id"))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("ORDERBY");
        assertThatThrownBy(
                        () -> KeysetQueryBuilder.validateBase(EncodedQuery.of("ORDERBYDESCsys_id")))
                .isInstanceOf(ServiceNowException.class);
        assertThatThrownBy(() -> new KeysetQueryBuilder(EncodedQuery.empty(), " ", "sys_id"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void projectionAlwaysIncludesCursorFieldsAndModCount() {
        assertThat(builder.projection(List.of("number", "state")))
                .containsExactly("number", "state", "sys_id", "sys_updated_on", "sys_mod_count");
        assertThat(builder.projection(List.of("sys_mod_count", " sys_id ", "number", "number", "")))
                .containsExactly("sys_mod_count", "sys_id", "number", "sys_updated_on");
        assertThat(builder.projection(List.of())).isEmpty();
        assertThat(builder.projection(null)).isEmpty();
    }

    @Test
    void watermarkTruncatesToSecondsAfterSubtractingLag() {
        java.time.Clock clock =
                java.time.Clock.fixed(
                        Instant.parse("2026-09-29T07:31:00.750Z"), java.time.ZoneOffset.UTC);
        Watermark w = new Watermark(clock);
        assertThat(w.hi(Duration.ofSeconds(1))).isEqualTo(Instant.parse("2026-09-29T07:30:59Z"));
        assertThat(w.hi(null)).isEqualTo(Instant.parse("2026-09-29T07:31:00Z"));
        assertThat(w.now()).isEqualTo(Instant.parse("2026-09-29T07:31:00.750Z"));
    }
}
