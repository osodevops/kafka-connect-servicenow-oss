package sh.oso.servicenow.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ErrorClassifierTest {

    @Test
    void statusClassificationFollowsTheContract() {
        for (int s : new int[] {429, 408, 425, 500, 502, 503, 504}) {
            assertThat(ErrorClassifier.isRetryableStatus(s, null)).as("%d", s).isTrue();
        }
        for (int s : new int[] {400, 401, 403, 404, 405, 422, 501}) {
            assertThat(ErrorClassifier.isRetryableStatus(s, "please try again"))
                    .as("%d", s)
                    .isFalse();
        }
        assertThat(ErrorClassifier.isRetryableStatus(409, "Record is locked, try again later"))
                .isTrue();
        assertThat(ErrorClassifier.isRetryableStatus(409, "Operation temporarily unavailable"))
                .isTrue();
        assertThat(ErrorClassifier.isRetryableStatus(409, "Duplicate record")).isFalse();
        assertThat(ErrorClassifier.isRetryableStatus(409, null)).isFalse();
    }

    @Test
    void classifyParsesServiceNowErrorBodiesAndRateLimitHeaders() {
        ServiceNowApiException e =
                ErrorClassifier.classify(
                        429,
                        "{\"error\":{\"message\":\"Too many\",\"detail\":\"slow down\"},\"status\":\"failure\"}",
                        Map.of("Retry-After", List.of("7")),
                        "rid",
                        Instant.EPOCH);
        assertThat(e.status()).isEqualTo(429);
        assertThat(e.isRetryable()).isTrue();
        assertThat(e.isRateLimited()).isTrue();
        assertThat(e.errorMessage()).isEqualTo("Too many");
        assertThat(e.errorDetail()).isEqualTo("slow down");
        assertThat(e.requestId()).isEqualTo("rid");
        assertThat(e.rateLimit()).isPresent();
        assertThat(e.rateLimit().get().retryAfter()).contains(java.time.Duration.ofSeconds(7));
        assertThat(e.getMessage())
                .isEqualTo("ServiceNow API error: HTTP 429 [Too many] slow down (request-id rid)");

        ServiceNowApiException plain =
                ErrorClassifier.classify(502, "<html>Bad gateway</html>", Map.of(), null);
        assertThat(plain.errorMessage()).isNull();
        assertThat(plain.responseExcerpt()).contains("Bad gateway");
        assertThat(plain.isRetryable()).isTrue();
        assertThat(plain.rateLimit()).isEmpty();

        ServiceNowApiException secret =
                ErrorClassifier.classify(
                        400,
                        "{\"error\":{\"message\":\"bad password=hunter2\",\"detail\":\"token=abc\"}}",
                        Map.of(),
                        "r");
        assertThat(secret.getMessage()).doesNotContain("hunter2").doesNotContain("abc");
        assertThat(secret.responseExcerpt()).doesNotContain("hunter2");
        assertThat(secret.isForbidden()).isFalse();
        assertThat(secret.isUnauthorized()).isFalse();
    }

    @Test
    void transportFailuresDependOnIdempotency() {
        ServiceNowException connect =
                ErrorClassifier.transportFailure(
                        new HttpConnectTimeoutException("ct"), "POST", "/p", false, "r");
        assertThat(connect.isRetryable()).isTrue();
        assertThat(connect.isAmbiguous()).isFalse();
        ServiceNowException refused =
                ErrorClassifier.transportFailure(
                        new ConnectException("refused"), "POST", "/p", false, null);
        assertThat(refused.isRetryable()).isTrue();
        ServiceNowException postTimeout =
                ErrorClassifier.transportFailure(
                        new HttpTimeoutException("t"), "POST", "/p", false, "r");
        assertThat(postTimeout.isRetryable()).isFalse();
        assertThat(postTimeout.isAmbiguous()).isTrue();
        assertThat(postTimeout.getMessage())
                .contains("may or may not have been applied")
                .contains("request-id r");
        ServiceNowException getTimeout =
                ErrorClassifier.transportFailure(
                        new HttpTimeoutException("t"), "GET", "/p", true, "r");
        assertThat(getTimeout.isRetryable()).isTrue();
        assertThat(getTimeout.isAmbiguous()).isFalse();
    }

    @Test
    void throwableClassification() {
        assertThat(ErrorClassifier.isRetryable(new ServiceNowException("x", null, true))).isTrue();
        assertThat(ErrorClassifier.isRetryable(new ServiceNowException("x"))).isFalse();
        assertThat(ErrorClassifier.isRetryable(new HttpConnectTimeoutException("x"))).isTrue();
        assertThat(ErrorClassifier.isRetryable(new IOException("x"))).isTrue();
        assertThat(ErrorClassifier.isRetryable(new IllegalStateException("x"))).isFalse();
        assertThat(ErrorClassifier.isAmbiguous(new IOException("x"))).isTrue();
        assertThat(ErrorClassifier.isAmbiguous(new HttpConnectTimeoutException("x"))).isFalse();
        assertThat(ErrorClassifier.isAmbiguous(new ServiceNowException("x", null, false, true)))
                .isTrue();
        assertThat(ErrorClassifier.isAmbiguous(new IllegalStateException("x"))).isFalse();
        assertThat(new IncompatibleOffsetException("o").isRetryable()).isFalse();
        assertThat(new IncompatibleOffsetException("o", new RuntimeException()).getCause())
                .isNotNull();
    }

    @Test
    void errorBodyParsing() {
        assertThat(ServiceNowErrorBody.parse(null)).isEmpty();
        assertThat(ServiceNowErrorBody.parse("[1,2]")).isEmpty();
        assertThat(ServiceNowErrorBody.parse("{nope")).isEmpty();
        ServiceNowErrorBody oauth =
                ServiceNowErrorBody.parse(
                                "{\"error\":\"invalid_client\",\"error_description\":\"bad\"}")
                        .orElseThrow();
        assertThat(oauth.message()).isEqualTo("invalid_client");
        assertThat(oauth.text()).isEqualTo("invalid_client: bad");
        ServiceNowErrorBody nullDetail =
                ServiceNowErrorBody.parse(
                                "{\"error\":{\"message\":\"m\",\"detail\":null},\"status\":\"failure\"}")
                        .orElseThrow();
        assertThat(nullDetail.text()).isEqualTo("m");
        assertThat(nullDetail.status()).isEqualTo("failure");
    }
}
