package sh.oso.servicenow.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.common.ErrorClassifier;
import sh.oso.servicenow.common.ServiceNowErrorBody;
import sh.oso.servicenow.common.ServiceNowException;

/**
 * OAuth 2.0 bearer tokens from {@code POST {tokenUrl}} (form-encoded) using the {@code
 * client_credentials} or {@code password} grant.
 *
 * <p>The token is cached until {@code expires_in} minus a {@value #REFRESH_SKEW_SECONDS} second
 * skew. Refreshes are single-flight: concurrent callers with a stale or missing token wait for one
 * fetch. {@link #invalidate(String)} only drops the cached token if it is still the value the
 * caller saw fail. When the instance issued a {@code refresh_token}, refreshes use it first and
 * fall back to the primary grant if it is rejected.
 */
public final class OAuthTokenProvider implements TokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OAuthTokenProvider.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    static final int REFRESH_SKEW_SECONDS = 30;
    private static final long DEFAULT_EXPIRES_IN_SECONDS = 1800;

    private final AuthConfig cfg;
    private final HttpClient http;
    private final Clock clock;
    private final Duration requestTimeout;
    private final Object refreshLock = new Object();
    private volatile Token current;

    private record Token(String accessToken, String refreshToken, Instant expiresAt) {
        String header() {
            return "Bearer " + accessToken;
        }

        boolean freshAt(Instant now) {
            return now.isBefore(expiresAt.minusSeconds(REFRESH_SKEW_SECONDS));
        }
    }

    public OAuthTokenProvider(AuthConfig cfg, HttpClient http) {
        this(cfg, http, Clock.systemUTC(), Duration.ofSeconds(30));
    }

    public OAuthTokenProvider(
            AuthConfig cfg, HttpClient http, Clock clock, Duration requestTimeout) {
        if (cfg.type() != AuthConfig.Type.OAUTH2) {
            throw new IllegalArgumentException("OAuthTokenProvider needs snow.auth.type=oauth2");
        }
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.http = Objects.requireNonNull(http, "http");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    }

    @Override
    public String authorization() {
        Token t = current;
        Instant now = clock.instant();
        if (t != null && t.freshAt(now)) {
            return t.header();
        }
        synchronized (refreshLock) {
            t = current;
            now = clock.instant();
            if (t != null && t.freshAt(now)) {
                return t.header();
            }
            Token fresh = fetch(t);
            current = fresh;
            return fresh.header();
        }
    }

    @Override
    public void invalidate(String seen) {
        synchronized (refreshLock) {
            Token t = current;
            if (t != null && t.header().equals(seen)) {
                LOG.info("ServiceNow rejected the cached OAuth token; it will be refreshed");
                current = null;
            }
        }
    }

    /** Whether a token is currently cached (diagnostics and tests). */
    public boolean hasToken() {
        return current != null;
    }

    private Token fetch(Token previous) {
        if (previous != null && previous.refreshToken() != null) {
            try {
                return request(refreshForm(previous.refreshToken()));
            } catch (ServiceNowException e) {
                if (e.isRetryable()) {
                    throw e;
                }
                LOG.info(
                        "OAuth refresh_token grant was rejected; falling back to the primary grant");
            }
        }
        return request(primaryForm());
    }

    private Map<String, String> primaryForm() {
        Map<String, String> form = new LinkedHashMap<>();
        form.put("grant_type", cfg.grantType().wireValue());
        form.put("client_id", cfg.clientId());
        form.put("client_secret", cfg.clientSecret());
        if (cfg.grantType() == AuthConfig.GrantType.PASSWORD) {
            form.put("username", cfg.username());
            form.put("password", cfg.password());
        }
        if (cfg.scope() != null && !cfg.scope().isBlank()) {
            form.put("scope", cfg.scope());
        }
        return form;
    }

    private Map<String, String> refreshForm(String refreshToken) {
        Map<String, String> form = new LinkedHashMap<>();
        form.put("grant_type", "refresh_token");
        form.put("client_id", cfg.clientId());
        form.put("client_secret", cfg.clientSecret());
        form.put("refresh_token", refreshToken);
        return form;
    }

    private Token request(Map<String, String> form) {
        String grant = form.get("grant_type");
        HttpRequest request =
                HttpRequest.newBuilder(cfg.tokenUrl())
                        .timeout(requestTimeout)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .header("Accept", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(encode(form)))
                        .build();
        HttpResponse<String> response;
        try {
            LOG.debug("Requesting OAuth token ({} grant) from {}", grant, cfg.tokenUrl());
            response =
                    http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw ErrorClassifier.transportFailure(e, "POST", cfg.tokenUrl().getPath(), true, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceNowException("Interrupted while requesting an OAuth token", e);
        }
        int status = response.statusCode();
        if (status >= 400) {
            String code =
                    ServiceNowErrorBody.parse(response.body())
                            .map(ServiceNowErrorBody::message)
                            .orElse(null);
            boolean retryable = ErrorClassifier.isRetryableStatus(status, null);
            throw new ServiceNowException(
                    "OAuth token request ("
                            + grant
                            + " grant) to "
                            + cfg.tokenUrl()
                            + " failed: HTTP "
                            + status
                            + (code != null ? " " + code : ""),
                    null,
                    retryable);
        }
        return parse(response.body(), grant);
    }

    private Token parse(String body, String grant) {
        JsonNode node;
        try {
            node = MAPPER.readTree(body == null ? "" : body);
        } catch (IOException e) {
            throw new ServiceNowException(
                    "OAuth token response (" + grant + " grant) is not valid JSON", e, false);
        }
        String accessToken = node.path("access_token").asText(null);
        if (accessToken == null || accessToken.isBlank()) {
            throw new ServiceNowException(
                    "OAuth token response (" + grant + " grant) carries no access_token",
                    null,
                    false);
        }
        long expiresIn = node.path("expires_in").asLong(DEFAULT_EXPIRES_IN_SECONDS);
        String refreshToken = node.path("refresh_token").asText(null);
        Instant expiresAt =
                clock.instant().plusSeconds(Math.max(expiresIn, REFRESH_SKEW_SECONDS + 1));
        LOG.info("Obtained ServiceNow OAuth token ({} grant), expires in {} s", grant, expiresIn);
        return new Token(accessToken, refreshToken, expiresAt);
    }

    private static String encode(Map<String, String> form) {
        StringJoiner sj = new StringJoiner("&");
        for (Map.Entry<String, String> e : form.entrySet()) {
            sj.add(
                    URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8)
                            + "="
                            + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
        }
        return sj.toString();
    }

    @Override
    public String toString() {
        return "OAuthTokenProvider{tokenUrl=" + cfg.tokenUrl() + ", grant=" + cfg.grantType() + '}';
    }
}
