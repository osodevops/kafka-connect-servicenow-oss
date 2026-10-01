package sh.oso.servicenow.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import sh.oso.servicenow.auth.AuthConfig;
import sh.oso.servicenow.auth.BasicTokenProvider;
import sh.oso.servicenow.auth.OAuthTokenProvider;
import sh.oso.servicenow.auth.TokenProvider;
import sh.oso.servicenow.common.RetryConfig;
import sh.oso.servicenow.common.RetryExhaustedException;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;
import sh.oso.servicenow.testing.VirtualTime;

class ServiceNowHttpClientTest {

    private static final String INCIDENT = "/api/now/table/incident";

    private MockServiceNowServer snow;
    private VirtualTime time;
    private ConcurrencyLimiter limiter;
    private final HttpClient jdk =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @BeforeEach
    void setUp() {
        snow = MockServiceNowServer.start();
        time = new VirtualTime();
        limiter = new ConcurrencyLimiter(8);
    }

    @AfterEach
    void tearDown() {
        snow.close();
    }

    private TokenProvider basic() {
        return new BasicTokenProvider(MockServiceNowServer.USERNAME, MockServiceNowServer.PASSWORD);
    }

    private OAuthTokenProvider oauth() {
        AuthConfig cfg =
                AuthConfig.builder()
                        .type(AuthConfig.Type.OAUTH2)
                        .tokenUrl(URI.create(snow.baseUrl() + "/oauth_token.do"))
                        .clientId(MockServiceNowServer.CLIENT_ID)
                        .clientSecret(MockServiceNowServer.CLIENT_SECRET)
                        .build();
        return new OAuthTokenProvider(cfg, jdk, snow.clock(), Duration.ofSeconds(5));
    }

    private ServiceNowHttpClient client(TokenProvider tokens, RetryConfig retry, HttpConfig http) {
        return new ServiceNowHttpClient(
                URI.create(snow.baseUrl() + "/"), jdk, tokens, time.policy(retry), limiter, http);
    }

    private ServiceNowHttpClient client() {
        return client(basic(), retry(5), HttpConfig.defaults());
    }

    private static RetryConfig retry(int attempts) {
        return new RetryConfig(
                attempts, Duration.ofMinutes(5), Duration.ofMillis(100), Duration.ofSeconds(2));
    }

    @Test
    void refreshesTheTokenOnceAfter401AndRepeatsTheRequest() {
        ServiceNowHttpClient client = client(oauth(), retry(5), HttpConfig.defaults());
        assertThat(client.execute(RequestSpec.get(INCIDENT)).status()).isEqualTo(200);
        snow.faults().unauthorizedOnce();
        HttpResult result = client.execute(RequestSpec.get(INCIDENT));
        assertThat(result.status()).isEqualTo(200);
        snow.wireMock().verify(3, getRequestedFor(urlPathEqualTo(INCIDENT)));
        assertThat(snow.journal().tokenRequests()).isEqualTo(2);
        assertThat(time.sleeps()).isEmpty();
    }

    @Test
    void secondConsecutive401FailsWithoutRetry() {
        ServiceNowHttpClient client = client(oauth(), retry(5), HttpConfig.defaults());
        snow.faults().unauthorizedTimes(2);
        assertThatThrownBy(() -> client.execute(RequestSpec.get(INCIDENT)))
                .isInstanceOf(ServiceNowApiException.class)
                .satisfies(
                        e -> {
                            ServiceNowApiException api = (ServiceNowApiException) e;
                            assertThat(api.isUnauthorized()).isTrue();
                            assertThat(api.isRetryable()).isFalse();
                        });
        snow.wireMock().verify(2, getRequestedFor(urlPathEqualTo(INCIDENT)));
        assertThat(time.sleeps()).isEmpty();
    }

    @Test
    void rateLimitWaitsExactlyRetryAfterOnTheVirtualSleeper() {
        snow.faults().rateLimit(1, Duration.ofSeconds(2));
        ServiceNowHttpClient client = client();
        assertThat(client.stats().lastSuccessfulRequestEpochMs()).isZero();
        assertThat(client.execute(RequestSpec.get(INCIDENT)).status()).isEqualTo(200);
        assertThat(time.sleeps()).containsExactly(Duration.ofSeconds(2));
        snow.wireMock().verify(2, getRequestedFor(urlPathEqualTo(INCIDENT)));
        assertThat(client.stats().retries()).isEqualTo(1);
        assertThat(client.stats().throttledMillis()).isEqualTo(2000);
        assertThat(client.stats().lastSuccessfulRequestEpochMs()).isPositive();
    }

    /**
     * One injected fault, then success. The fake answers every injected fault with {@code
     * Connection: close}, so the retry always opens a fresh connection instead of racing a
     * keep-alive socket the server has dropped; the attempt budget ({@code faults + 2}) leaves room
     * for exactly the one retry the assertions count.
     */
    @ParameterizedTest
    @ValueSource(ints = {502, 503, 504, 408, 425, 500, 429})
    void retriesTransientStatuses(int status) {
        snow.faults().serverError(1, status);
        ServiceNowHttpClient client = client(basic(), retry(3), HttpConfig.defaults());
        assertThat(client.execute(RequestSpec.get(INCIDENT)).status()).isEqualTo(200);
        assertThat(time.sleeps()).hasSize(1);
        snow.wireMock().verify(2, getRequestedFor(urlPathEqualTo(INCIDENT)));
        assertThat(client.stats().retries()).isEqualTo(1);
        // Only a 429 counts as throttling; without Retry-After the wait is the jittered backoff.
        assertThat(client.stats().throttledMillis())
                .isEqualTo(status == 429 ? time.sleeps().get(0).toMillis() : 0L);
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 403, 404, 422, 409, 405})
    void neverRetriesClientErrors(int status) {
        snow.faults().serverError(1, status);
        assertThatThrownBy(() -> client().execute(RequestSpec.get(INCIDENT)))
                .isInstanceOf(ServiceNowApiException.class)
                .satisfies(
                        e -> assertThat(((ServiceNowApiException) e).status()).isEqualTo(status));
        assertThat(time.sleeps()).isEmpty();
        snow.wireMock().verify(1, getRequestedFor(urlPathEqualTo(INCIDENT)));
    }

    @Test
    void attemptsCapRaisesRetryExhaustedThatStaysRetryable() {
        snow.faults().serverError(10, 503);
        ServiceNowHttpClient client = client(basic(), retry(3), HttpConfig.defaults());
        assertThatThrownBy(() -> client.execute(RequestSpec.get(INCIDENT)))
                .isInstanceOf(RetryExhaustedException.class)
                .satisfies(
                        e -> {
                            RetryExhaustedException ex = (RetryExhaustedException) e;
                            assertThat(ex.isRetryable()).isTrue();
                            assertThat(ex.attempts()).isEqualTo(3);
                            assertThat(ex.getCause()).isInstanceOf(ServiceNowApiException.class);
                        });
        snow.wireMock().verify(3, getRequestedFor(urlPathEqualTo(INCIDENT)));
        assertThat(time.sleeps()).hasSize(2);
    }

    @Test
    void elapsedCapStopsBeforeAWaitThatWouldExceedTheBudget() {
        snow.faults().rateLimit(5, Duration.ofSeconds(10));
        RetryConfig cfg =
                new RetryConfig(
                        10, Duration.ofSeconds(5), Duration.ofMillis(1), Duration.ofMillis(10));
        ServiceNowHttpClient client = client(basic(), cfg, HttpConfig.defaults());
        assertThatThrownBy(() -> client.execute(RequestSpec.get(INCIDENT)))
                .isInstanceOf(RetryExhaustedException.class)
                .hasMessageContaining("budget");
        snow.wireMock().verify(1, getRequestedFor(urlPathEqualTo(INCIDENT)));
        assertThat(time.sleeps()).isEmpty();
    }

    @Test
    void postTimeoutAfterSendIsAmbiguousAndNotRetried() {
        snow.faults().timeoutAfterWrite("incident", Duration.ofMillis(1500));
        HttpConfig http = HttpConfig.builder().requestTimeout(Duration.ofMillis(300)).build();
        ServiceNowHttpClient client = client(basic(), retry(5), http);
        assertThatThrownBy(
                        () ->
                                client.execute(
                                        RequestSpec.post(
                                                INCIDENT, "{\"short_description\":\"x\"}")))
                .isInstanceOf(ServiceNowException.class)
                .satisfies(
                        e -> {
                            ServiceNowException ex = (ServiceNowException) e;
                            assertThat(ex.isAmbiguous()).isTrue();
                            assertThat(ex.isRetryable()).isFalse();
                        });
        assertThat(snow.tables().size("incident")).isEqualTo(1);
        assertThat(snow.journal().count("POST", "incident")).isEqualTo(1);
        assertThat(time.sleeps()).isEmpty();
    }

    @Test
    void patchTimeoutAfterSendIsRetried() {
        String id = snow.tables().insert("incident", Map.of("state", "1"));
        snow.faults().timeoutAfterWrite("incident", Duration.ofMillis(1500));
        HttpConfig http = HttpConfig.builder().requestTimeout(Duration.ofMillis(300)).build();
        ServiceNowHttpClient client = client(basic(), retry(5), http);
        HttpResult result =
                client.execute(RequestSpec.patch(INCIDENT + "/" + id, "{\"state\":\"2\"}"));
        assertThat(result.status()).isEqualTo(200);
        assertThat(snow.journal().count("PATCH", "incident")).isEqualTo(2);
        assertThat(time.sleeps()).hasSize(1);
    }

    @Test
    void truncatedBodyIsRetriedForIdempotentRequests() {
        snow.faults().truncatedBodyOnce();
        assertThat(client().execute(RequestSpec.get(INCIDENT)).status()).isEqualTo(200);
        assertThat(time.sleeps()).hasSize(1);
    }

    @Test
    void decodesGzipBodies() throws Exception {
        String payload = "{\"result\":[{\"sys_id\":\"zipped\"}]}";
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bytes)) {
            gz.write(payload.getBytes(StandardCharsets.UTF_8));
        }
        snow.wireMock()
                .stubFor(
                        get(urlPathEqualTo("/api/now/table/gz"))
                                .atPriority(1)
                                .willReturn(
                                        aResponse()
                                                .withStatus(200)
                                                .withHeader("Content-Type", "application/json")
                                                .withHeader("Content-Encoding", "gzip")
                                                .withBody(bytes.toByteArray())));
        HttpResult result = client().execute(RequestSpec.get("/api/now/table/gz"));
        assertThat(result.bodyAsString()).isEqualTo(payload);
    }

    @Test
    void bodyOverTheLimitFailsWithoutRetry() {
        String big = "x".repeat(200);
        for (int i = 0; i < 20; i++) {
            snow.tables().insert("incident", Map.of("description", big));
        }
        HttpConfig http = HttpConfig.builder().maxResponseBytes(1024).build();
        ServiceNowHttpClient client = client(basic(), retry(5), http);
        assertThatThrownBy(() -> client.execute(RequestSpec.get(INCIDENT)))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("snow.http.max.response.bytes")
                .satisfies(e -> assertThat(((ServiceNowException) e).isRetryable()).isFalse());
        assertThat(time.sleeps()).isEmpty();
    }

    @Test
    void requestIdTravelsWithTheRequestAndTheException() {
        String missing = "0123456789abcdef0123456789abcdef";
        assertThatThrownBy(() -> client().execute(RequestSpec.get(INCIDENT + "/" + missing)))
                .isInstanceOf(ServiceNowApiException.class)
                .satisfies(
                        e -> {
                            ServiceNowApiException api = (ServiceNowApiException) e;
                            assertThat(api.isNotFound()).isTrue();
                            assertThat(api.requestId()).isNotBlank();
                            assertThat(api.getMessage())
                                    .contains(api.requestId())
                                    .contains("No Record found");
                            assertThat(snow.journal().entries().get(0).requestId())
                                    .isEqualTo(api.requestId());
                        });
    }

    @Test
    void observesTheServerClockFromDateHeaders() {
        Instant serverTime =
                Instant.now()
                        .plus(Duration.ofHours(3))
                        .truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        snow.clock().set(serverTime);
        ServiceNowHttpClient client = client();
        assertThat(((ServerClock) client.serverClock()).hasObserved()).isFalse();
        client.execute(RequestSpec.get(INCIDENT));
        assertThat(((ServerClock) client.serverClock()).hasObserved()).isTrue();
        assertThat(Duration.between(serverTime, client.serverClock().instant()).abs())
                .isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void adaptiveThrottlingHalvesPermitsOn429AndOnlyWhenEnabled() {
        snow.faults().rateLimit(1, Duration.ofSeconds(1));
        client().execute(RequestSpec.get(INCIDENT));
        assertThat(limiter.permits()).isEqualTo(4);
        limiter.resize(8);
        snow.faults().rateLimit(1, Duration.ofSeconds(1));
        client(basic(), retry(5), HttpConfig.builder().adaptiveThrottling(false).build())
                .execute(RequestSpec.get(INCIDENT));
        assertThat(limiter.permits()).isEqualTo(8);
    }

    @Test
    void sendsStandardHeadersAndEncodedPath() {
        client().execute(RequestSpec.get(INCIDENT + "?sysparm_query=a%3D1").header("X-Extra", "1"));
        snow.wireMock()
                .verify(
                        getRequestedFor(urlPathEqualTo(INCIDENT))
                                .withQueryParam(
                                        "sysparm_query",
                                        com.github.tomakehurst.wiremock.client.WireMock.equalTo(
                                                "a=1"))
                                .withHeader(
                                        "Accept",
                                        com.github.tomakehurst.wiremock.client.WireMock.equalTo(
                                                "application/json"))
                                .withHeader(
                                        "Accept-Encoding",
                                        com.github.tomakehurst.wiremock.client.WireMock.equalTo(
                                                "gzip"))
                                .withHeader(
                                        "User-Agent",
                                        com.github.tomakehurst.wiremock.client.WireMock.containing(
                                                "kafka-connect-servicenow"))
                                .withHeader(
                                        "X-Extra",
                                        com.github.tomakehurst.wiremock.client.WireMock.equalTo(
                                                "1"))
                                .withHeader(
                                        "X-Request-Id",
                                        com.github.tomakehurst.wiremock.client.WireMock.matching(
                                                "[0-9a-f-]{36}")));
    }

    @Test
    void requestSpecDefaultsIdempotencyByMethod() {
        assertThat(RequestSpec.get("/x").idempotent()).isTrue();
        assertThat(RequestSpec.delete("/x").idempotent()).isTrue();
        assertThat(RequestSpec.put("/x", "{}").idempotent()).isTrue();
        assertThat(RequestSpec.patch("/x", "{}").idempotent()).isTrue();
        assertThat(RequestSpec.post("/x", "{}").idempotent()).isFalse();
        assertThat(RequestSpec.post("/x", "{}").idempotent(true).idempotent()).isTrue();
        assertThat(RequestSpec.post("/x?y=1", "{}").path()).isEqualTo("/x");
        assertThat(RequestSpec.post("/x", "{}").headers())
                .containsEntry("Content-Type", "application/json");
        HttpResult r =
                new HttpResult(
                        200,
                        Map.of("content-type", java.util.List.of("application/json")),
                        null,
                        "id");
        assertThat(r.header("Content-Type")).contains("application/json");
        assertThat(r.header("Missing")).isEmpty();
        assertThat(r.isSuccess()).isTrue();
        assertThat(r.bodyAsString()).isEmpty();
    }
}
