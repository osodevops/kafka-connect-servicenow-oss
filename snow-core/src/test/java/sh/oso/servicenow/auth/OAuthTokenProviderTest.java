package sh.oso.servicenow.auth;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.testing.MockServiceNowServer;

class OAuthTokenProviderTest {

    private MockServiceNowServer snow;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void setUp() {
        snow = MockServiceNowServer.start();
    }

    @AfterEach
    void tearDown() {
        snow.close();
    }

    private AuthConfig.Builder oauth(AuthConfig.GrantType grant) {
        return AuthConfig.builder()
                .type(AuthConfig.Type.OAUTH2)
                .grantType(grant)
                .tokenUrl(URI.create(snow.baseUrl() + "/oauth_token.do"))
                .clientId(MockServiceNowServer.CLIENT_ID)
                .clientSecret(MockServiceNowServer.CLIENT_SECRET)
                .username(MockServiceNowServer.USERNAME)
                .password(MockServiceNowServer.PASSWORD);
    }

    private OAuthTokenProvider provider(AuthConfig cfg) {
        return new OAuthTokenProvider(cfg, http, snow.clock(), Duration.ofSeconds(5));
    }

    @Test
    void fiftyConcurrentCallersTriggerOneTokenFetch() throws Exception {
        OAuthTokenProvider provider =
                provider(oauth(AuthConfig.GrantType.CLIENT_CREDENTIALS).build());
        int callers = 50;
        ExecutorService pool = Executors.newFixedThreadPool(callers);
        CountDownLatch go = new CountDownLatch(1);
        Set<String> seen = ConcurrentHashMap.newKeySet();
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < callers; i++) {
                futures.add(
                        pool.submit(
                                () -> {
                                    go.await();
                                    seen.add(provider.authorization());
                                    return null;
                                }));
            }
            go.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(seen).hasSize(1);
        assertThat(seen.iterator().next()).startsWith("Bearer tok-");
        assertThat(snow.journal().tokenRequests()).isEqualTo(1);
    }

    @Test
    void refreshesBeforeExpiryUsingTheRefreshTokenGrant() {
        snow.oauth().expiresIn(600);
        OAuthTokenProvider provider =
                provider(oauth(AuthConfig.GrantType.CLIENT_CREDENTIALS).build());
        String first = provider.authorization();
        snow.clock().advance(Duration.ofSeconds(600 - OAuthTokenProvider.REFRESH_SKEW_SECONDS - 1));
        assertThat(provider.authorization()).isEqualTo(first);
        snow.clock().advance(Duration.ofSeconds(2));
        String second = provider.authorization();
        assertThat(second).isNotEqualTo(first);
        assertThat(snow.journal().tokenRequests()).isEqualTo(2);
        snow.wireMock()
                .verify(
                        1,
                        postRequestedFor(urlPathEqualTo("/oauth_token.do"))
                                .withRequestBody(containing("grant_type=refresh_token")));
    }

    @Test
    void invalidateOnlyDropsTheTokenThatFailed() {
        OAuthTokenProvider provider =
                provider(oauth(AuthConfig.GrantType.CLIENT_CREDENTIALS).build());
        String first = provider.authorization();
        provider.invalidate("Bearer something-else");
        assertThat(provider.authorization()).isEqualTo(first);
        assertThat(snow.journal().tokenRequests()).isEqualTo(1);
        provider.invalidate(first);
        assertThat(provider.hasToken()).isFalse();
        assertThat(provider.authorization()).isNotEqualTo(first);
        assertThat(snow.journal().tokenRequests()).isEqualTo(2);
    }

    @Test
    void passwordGrantSendsUsernamePasswordClientAndScope() {
        OAuthTokenProvider provider =
                provider(oauth(AuthConfig.GrantType.PASSWORD).scope("useraccount").build());
        assertThat(provider.authorization()).startsWith("Bearer ");
        snow.wireMock()
                .verify(
                        postRequestedFor(urlPathEqualTo("/oauth_token.do"))
                                .withHeader(
                                        "Content-Type",
                                        containing("application/x-www-form-urlencoded"))
                                .withRequestBody(containing("grant_type=password"))
                                .withRequestBody(containing("client_id=client-id"))
                                .withRequestBody(containing("client_secret=client-secret"))
                                .withRequestBody(containing("username=connect"))
                                .withRequestBody(containing("password=secret"))
                                .withRequestBody(containing("scope=useraccount")));
    }

    @Test
    void tokenEndpointRejectionIsNotRetryableAndMentionsNoSecret() {
        OAuthTokenProvider provider =
                provider(
                        oauth(AuthConfig.GrantType.CLIENT_CREDENTIALS)
                                .clientSecret("wrong-secret-value")
                                .build());
        assertThatThrownBy(provider::authorization)
                .isInstanceOf(ServiceNowException.class)
                .satisfies(e -> assertThat(((ServiceNowException) e).isRetryable()).isFalse())
                .hasMessageContaining("HTTP 401")
                .hasMessageContaining("invalid_client")
                .extracting(
                        Throwable::toString, org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .doesNotContain("wrong-secret-value");
    }

    @Test
    void rejectedRefreshTokenFallsBackToThePrimaryGrant() {
        snow.oauth().expiresIn(100);
        OAuthTokenProvider provider =
                provider(oauth(AuthConfig.GrantType.CLIENT_CREDENTIALS).build());
        provider.authorization();
        snow.oauth().revokeAll();
        snow.clock().advance(Duration.ofSeconds(200));
        assertThat(provider.authorization()).startsWith("Bearer tok-");
        assertThat(snow.journal().tokenRequests()).isEqualTo(3);
        snow.wireMock()
                .verify(
                        2,
                        postRequestedFor(urlPathEqualTo("/oauth_token.do"))
                                .withRequestBody(containing("grant_type=client_credentials")));
    }

    @Test
    void basicConfigIsRejected() {
        AuthConfig basic = AuthConfig.builder().username("u").password("p").build();
        assertThatThrownBy(() -> new OAuthTokenProvider(basic, http))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(TokenProviders.from(basic, http)).isInstanceOf(BasicTokenProvider.class);
        assertThat(TokenProviders.from(oauth(AuthConfig.GrantType.PASSWORD).build(), http))
                .isInstanceOf(OAuthTokenProvider.class);
    }

    @Test
    void authConfigValidatesRequiredKeysAndRedactsToString() {
        assertThatThrownBy(
                        () ->
                                AuthConfig.builder()
                                        .type(AuthConfig.Type.BASIC)
                                        .username("u")
                                        .build())
                .hasMessageContaining("snow.auth.password");
        assertThatThrownBy(() -> oauth(AuthConfig.GrantType.PASSWORD).password(null).build())
                .hasMessageContaining("snow.auth.password");
        assertThatThrownBy(
                        () -> oauth(AuthConfig.GrantType.CLIENT_CREDENTIALS).clientId(null).build())
                .hasMessageContaining("snow.oauth.client.id");
        AuthConfig cfg = oauth(AuthConfig.GrantType.PASSWORD).build();
        assertThat(cfg.toString()).contains("username=connect").doesNotContain("secret");
        assertThat(AuthConfig.Type.fromConfig("OAuth2")).isEqualTo(AuthConfig.Type.OAUTH2);
        assertThat(AuthConfig.GrantType.fromConfig(null))
                .isEqualTo(AuthConfig.GrantType.CLIENT_CREDENTIALS);
        assertThatThrownBy(() -> AuthConfig.Type.fromConfig("jwt"))
                .hasMessageContaining("snow.auth.type");
    }
}
