package sh.oso.servicenow.auth;

import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;

/** Chooses the {@link TokenProvider} for an {@link AuthConfig}. */
public final class TokenProviders {

    private TokenProviders() {}

    public static TokenProvider from(AuthConfig cfg, HttpClient http) {
        return from(cfg, http, Clock.systemUTC(), Duration.ofSeconds(30));
    }

    public static TokenProvider from(
            AuthConfig cfg, HttpClient http, Clock clock, Duration requestTimeout) {
        return switch (cfg.type()) {
            case BASIC -> new BasicTokenProvider(cfg.username(), cfg.password());
            case OAUTH2 -> new OAuthTokenProvider(cfg, http, clock, requestTimeout);
        };
    }
}
