package sh.oso.servicenow.limits;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RateLimitInfoTest {

    private static final Instant NOW = Instant.parse("2026-09-29T07:30:40Z");

    @Test
    void parsesRetryAfterAsDeltaSecondsOrHttpDate() {
        RateLimitInfo delta =
                RateLimitInfo.parse(Map.of("retry-after", List.of(" 5 ")), NOW).orElseThrow();
        assertThat(delta.retryAfter()).contains(Duration.ofSeconds(5));
        assertThat(delta.waitFor(NOW)).contains(Duration.ofSeconds(5));
        assertThat(delta.waitFor(NOW.plusSeconds(2))).contains(Duration.ofSeconds(3));
        assertThat(delta.waitFor(NOW.plusSeconds(9))).contains(Duration.ZERO);
        assertThat(delta.observedAt()).isEqualTo(NOW);
        RateLimitInfo date =
                RateLimitInfo.parse(
                                Map.of("Retry-After", List.of("Tue, 29 Sep 2026 07:30:50 GMT")),
                                NOW)
                        .orElseThrow();
        assertThat(date.retryAfter()).contains(Duration.ofSeconds(10));
        RateLimitInfo past =
                RateLimitInfo.parse(
                                Map.of("Retry-After", List.of("Tue, 29 Sep 2026 07:30:00 GMT")),
                                NOW)
                        .orElseThrow();
        assertThat(past.retryAfter()).contains(Duration.ZERO);
        assertThat(RateLimitInfo.parse(Map.of("Retry-After", List.of("soon")), NOW)).isEmpty();
        assertThat(
                        RateLimitInfo.parse(Map.of("Retry-After", List.of("-3")), NOW)
                                .orElseThrow()
                                .retryAfter())
                .contains(Duration.ZERO);
    }

    @Test
    void parsesOptionalRateLimitHeaders() {
        Map<String, List<String>> headers =
                Map.of(
                        "X-RateLimit-Limit", List.of("100"),
                        "X-RateLimit-Remaining", List.of("0"),
                        "X-RateLimit-Reset", List.of(Long.toString(NOW.getEpochSecond() + 30)));
        RateLimitInfo info = RateLimitInfo.parse(headers, NOW).orElseThrow();
        assertThat(info.limit()).hasValue(100);
        assertThat(info.remaining()).hasValue(0);
        assertThat(info.resetAt()).contains(NOW.plusSeconds(30));
        assertThat(info.remainingFraction()).contains(0d);
        assertThat(info.waitFor(NOW.plusSeconds(10))).contains(Duration.ofSeconds(20));
        assertThat(info.waitFor(NOW.plusSeconds(40))).contains(Duration.ZERO);
        assertThat(info.toString()).contains("limit=100");

        RateLimitInfo delta =
                RateLimitInfo.parse(
                                Map.of(
                                        "X-RateLimit-Reset",
                                        List.of("45"),
                                        "X-RateLimit-Remaining",
                                        List.of("7")),
                                NOW)
                        .orElseThrow();
        assertThat(delta.resetAt()).contains(NOW.plusSeconds(45));
        assertThat(delta.waitFor(NOW)).isEmpty();
        assertThat(delta.remainingFraction()).isEmpty();
        RateLimitInfo httpDate =
                RateLimitInfo.parse(
                                Map.of(
                                        "X-RateLimit-Reset",
                                        List.of("Tue, 29 Sep 2026 08:00:00 GMT")),
                                NOW)
                        .orElseThrow();
        assertThat(httpDate.resetAt()).contains(Instant.parse("2026-09-29T08:00:00Z"));
        assertThat(RateLimitInfo.parse(Map.of("X-RateLimit-Reset", List.of("later")), NOW))
                .isEmpty();
    }

    @Test
    void absentHeadersGiveEmpty() {
        assertThat(RateLimitInfo.parse(Map.of(), NOW)).isEmpty();
        assertThat(RateLimitInfo.parse(null, NOW)).isEmpty();
        assertThat(RateLimitInfo.parse(Map.of("Content-Type", List.of("application/json")), NOW))
                .isEmpty();
        Optional<RateLimitInfo> empty = RateLimitInfo.parse(Map.of("Retry-After", List.of()), NOW);
        assertThat(empty).isEmpty();
    }
}
