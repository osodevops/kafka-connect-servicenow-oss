package sh.oso.servicenow.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import sh.oso.servicenow.auth.AuthConfig;
import sh.oso.servicenow.common.RetryConfig;
import sh.oso.servicenow.http.HttpConfig;

/**
 * Typed view over the shared {@code snow.*} keys with cross-field validation. Error messages name
 * keys, never values of PASSWORD settings.
 */
public final class CoreConfig {

    private final URI instanceUrl;
    private final AuthConfig authConfig;
    private final HttpConfig httpConfig;
    private final RetryConfig retryConfig;

    public CoreConfig(AbstractConfig config) {
        this.instanceUrl = normaliseUrl(config.getString(CoreConfigDefs.URL));
        this.authConfig = buildAuth(config, instanceUrl);
        this.httpConfig = buildHttp(config);
        this.retryConfig = buildRetry(config);
    }

    /** Convenience for tests and tools: parses raw properties against the core definition only. */
    public static CoreConfig fromProps(Map<String, String> props) {
        return new CoreConfig(new AbstractConfig(CoreConfigDefs.coreDef(), props, false));
    }

    public URI instanceUrl() {
        return instanceUrl;
    }

    /** Lower-cased host, for example {@code acme.service-now.com}; identifies the instance. */
    public String instanceHost() {
        return instanceUrl.getHost().toLowerCase(Locale.ROOT);
    }

    /** {@code host} or {@code host:port} when a non-default port is used; keys shared limiters. */
    public String instanceKey() {
        return instanceUrl.getPort() > 0
                ? instanceHost() + ":" + instanceUrl.getPort()
                : instanceHost();
    }

    public AuthConfig authConfig() {
        return authConfig;
    }

    public HttpConfig httpConfig() {
        return httpConfig;
    }

    public RetryConfig retryConfig() {
        return retryConfig;
    }

    static URI normaliseUrl(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new ConfigException(CoreConfigDefs.URL, raw, "must be set");
        }
        String trimmed = raw.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        URI uri;
        try {
            uri = new URI(trimmed);
        } catch (URISyntaxException e) {
            throw new ConfigException(CoreConfigDefs.URL, raw, "is not a valid URL");
        }
        String scheme = uri.getScheme() == null ? null : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!"https".equals(scheme) && !"http".equals(scheme)) {
            throw new ConfigException(
                    CoreConfigDefs.URL, raw, "must start with https:// or http://");
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new ConfigException(CoreConfigDefs.URL, raw, "must include a host");
        }
        if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new ConfigException(
                    CoreConfigDefs.URL, raw, "must not carry a query or fragment");
        }
        try {
            return new URI(
                    scheme,
                    null,
                    uri.getHost().toLowerCase(Locale.ROOT),
                    uri.getPort(),
                    uri.getPath() == null || uri.getPath().isEmpty() ? null : uri.getPath(),
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw new ConfigException(CoreConfigDefs.URL, raw, "is not a valid URL");
        }
    }

    private static AuthConfig buildAuth(AbstractConfig config, URI instanceUrl) {
        AuthConfig.Type type =
                AuthConfig.Type.fromConfig(config.getString(CoreConfigDefs.AUTH_TYPE));
        AuthConfig.GrantType grant =
                AuthConfig.GrantType.fromConfig(config.getString(CoreConfigDefs.OAUTH_GRANT_TYPE));
        String tokenUrl = config.getString(CoreConfigDefs.OAUTH_TOKEN_URL);
        URI tokenUri;
        if (tokenUrl == null || tokenUrl.isBlank()) {
            tokenUri = URI.create(instanceUrl + "/oauth_token.do");
        } else {
            try {
                tokenUri = new URI(tokenUrl.trim());
            } catch (URISyntaxException e) {
                throw new ConfigException(
                        CoreConfigDefs.OAUTH_TOKEN_URL, tokenUrl, "is not a valid URL");
            }
        }
        return AuthConfig.builder()
                .type(type)
                .grantType(grant)
                .username(config.getString(CoreConfigDefs.AUTH_USERNAME))
                .password(password(config, CoreConfigDefs.AUTH_PASSWORD))
                .tokenUrl(tokenUri)
                .clientId(config.getString(CoreConfigDefs.OAUTH_CLIENT_ID))
                .clientSecret(password(config, CoreConfigDefs.OAUTH_CLIENT_SECRET))
                .scope(config.getString(CoreConfigDefs.OAUTH_SCOPE))
                .build();
    }

    private static HttpConfig buildHttp(AbstractConfig config) {
        HttpConfig.Builder b =
                HttpConfig.builder()
                        .connectTimeout(
                                Duration.ofMillis(
                                        config.getInt(CoreConfigDefs.HTTP_CONNECT_TIMEOUT_MS)))
                        .requestTimeout(
                                Duration.ofMillis(
                                        config.getInt(CoreConfigDefs.HTTP_REQUEST_TIMEOUT_MS)))
                        .maxConcurrentRequests(
                                config.getInt(CoreConfigDefs.HTTP_MAX_CONCURRENT_REQUESTS))
                        .maxResponseBytes(config.getLong(CoreConfigDefs.HTTP_MAX_RESPONSE_BYTES))
                        .userAgent(config.getString(CoreConfigDefs.HTTP_USER_AGENT))
                        .adaptiveThrottling(
                                config.getBoolean(CoreConfigDefs.HTTP_ADAPTIVE_THROTTLING))
                        .truststore(
                                config.getString(CoreConfigDefs.TLS_TRUSTSTORE_PATH),
                                password(config, CoreConfigDefs.TLS_TRUSTSTORE_PASSWORD),
                                config.getString(CoreConfigDefs.TLS_TRUSTSTORE_TYPE))
                        .keystore(
                                config.getString(CoreConfigDefs.TLS_KEYSTORE_PATH),
                                password(config, CoreConfigDefs.TLS_KEYSTORE_PASSWORD),
                                config.getString(CoreConfigDefs.TLS_KEYSTORE_TYPE));
        String proxy = config.getString(CoreConfigDefs.HTTP_PROXY_URL);
        if (proxy != null && !proxy.isBlank()) {
            URI proxyUri;
            try {
                proxyUri = new URI(proxy.trim());
            } catch (URISyntaxException e) {
                throw new ConfigException(
                        CoreConfigDefs.HTTP_PROXY_URL, proxy, "is not a valid URL");
            }
            if (proxyUri.getHost() == null) {
                throw new ConfigException(
                        CoreConfigDefs.HTTP_PROXY_URL, proxy, "must include a host");
            }
            b.proxyUrl(proxyUri)
                    .proxyUsername(config.getString(CoreConfigDefs.HTTP_PROXY_USERNAME))
                    .proxyPassword(password(config, CoreConfigDefs.HTTP_PROXY_PASSWORD));
        }
        String keystore = config.getString(CoreConfigDefs.TLS_KEYSTORE_PATH);
        if (keystore != null
                && !keystore.isBlank()
                && password(config, CoreConfigDefs.TLS_KEYSTORE_PASSWORD) == null) {
            throw new ConfigException(
                    CoreConfigDefs.TLS_KEYSTORE_PASSWORD
                            + " is required when "
                            + CoreConfigDefs.TLS_KEYSTORE_PATH
                            + " is set");
        }
        return b.build();
    }

    private static RetryConfig buildRetry(AbstractConfig config) {
        long initial = config.getLong(CoreConfigDefs.RETRY_INITIAL_BACKOFF_MS);
        long max = config.getLong(CoreConfigDefs.RETRY_MAX_BACKOFF_MS);
        if (max < initial) {
            throw new ConfigException(
                    CoreConfigDefs.RETRY_MAX_BACKOFF_MS,
                    max,
                    "must be >= " + CoreConfigDefs.RETRY_INITIAL_BACKOFF_MS + " (" + initial + ")");
        }
        return new RetryConfig(
                config.getInt(CoreConfigDefs.RETRY_MAX_ATTEMPTS),
                Duration.ofMillis(config.getLong(CoreConfigDefs.RETRY_MAX_ELAPSED_MS)),
                Duration.ofMillis(initial),
                Duration.ofMillis(max));
    }

    private static String password(AbstractConfig config, String key) {
        Password p = config.getPassword(key);
        return p == null ? null : p.value();
    }

    @Override
    public String toString() {
        return "CoreConfig{instanceUrl="
                + instanceUrl
                + ", auth="
                + authConfig
                + ", http="
                + httpConfig
                + ", retry="
                + retryConfig
                + '}';
    }
}
