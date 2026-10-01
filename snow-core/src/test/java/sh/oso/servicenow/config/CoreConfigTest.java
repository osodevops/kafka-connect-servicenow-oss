package sh.oso.servicenow.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.auth.AuthConfig;

class CoreConfigTest {

    private static Map<String, String> basic() {
        Map<String, String> p = new HashMap<>();
        p.put(CoreConfigDefs.URL, "https://ACME.service-now.com/");
        p.put(CoreConfigDefs.AUTH_USERNAME, "connect");
        p.put(CoreConfigDefs.AUTH_PASSWORD, "hunter2");
        return p;
    }

    @Test
    void basicDefaultsAndUrlNormalisation() {
        CoreConfig cfg = CoreConfig.fromProps(basic());
        assertThat(cfg.instanceUrl()).isEqualTo(URI.create("https://acme.service-now.com"));
        assertThat(cfg.instanceHost()).isEqualTo("acme.service-now.com");
        assertThat(cfg.instanceKey()).isEqualTo("acme.service-now.com");
        assertThat(cfg.authConfig().type()).isEqualTo(AuthConfig.Type.BASIC);
        assertThat(cfg.authConfig().tokenUrl())
                .isEqualTo(URI.create("https://acme.service-now.com/oauth_token.do"));
        assertThat(cfg.httpConfig().connectTimeout()).isEqualTo(Duration.ofMillis(10_000));
        assertThat(cfg.httpConfig().requestTimeout()).isEqualTo(Duration.ofMillis(60_000));
        assertThat(cfg.httpConfig().maxConcurrentRequests()).isEqualTo(8);
        assertThat(cfg.httpConfig().maxResponseBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(cfg.httpConfig().userAgent()).startsWith("kafka-connect-servicenow/");
        assertThat(cfg.httpConfig().adaptiveThrottling()).isTrue();
        assertThat(cfg.httpConfig().proxyUrl()).isNull();
        assertThat(cfg.retryConfig().maxAttempts()).isEqualTo(5);
        assertThat(cfg.retryConfig().maxElapsed()).isEqualTo(Duration.ofMinutes(5));
        assertThat(cfg.retryConfig().initialBackoff()).isEqualTo(Duration.ofMillis(500));
        assertThat(cfg.retryConfig().maxBackoff()).isEqualTo(Duration.ofSeconds(30));
        assertThat(cfg.toString()).contains("acme.service-now.com").doesNotContain("hunter2");
    }

    @Test
    void portAndPathSurviveNormalisation() {
        Map<String, String> p = basic();
        p.put(CoreConfigDefs.URL, "http://localhost:8090/prefix/");
        CoreConfig cfg = CoreConfig.fromProps(p);
        assertThat(cfg.instanceUrl()).isEqualTo(URI.create("http://localhost:8090/prefix"));
        assertThat(cfg.instanceKey()).isEqualTo("localhost:8090");
        assertThat(cfg.authConfig().tokenUrl())
                .isEqualTo(URI.create("http://localhost:8090/prefix/oauth_token.do"));
    }

    @Test
    void oauthRequiresClientCredentialsAndHonoursOverrides() {
        Map<String, String> p = basic();
        p.put(CoreConfigDefs.AUTH_TYPE, "oauth2");
        assertThatThrownBy(() -> CoreConfig.fromProps(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CoreConfigDefs.OAUTH_CLIENT_ID)
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("hunter2"));
        p.put(CoreConfigDefs.OAUTH_CLIENT_ID, "id");
        p.put(CoreConfigDefs.OAUTH_CLIENT_SECRET, "s3cr3t");
        p.put(CoreConfigDefs.OAUTH_GRANT_TYPE, "password");
        p.put(CoreConfigDefs.OAUTH_TOKEN_URL, "https://sso.example.com/token");
        p.put(CoreConfigDefs.OAUTH_SCOPE, "useraccount");
        CoreConfig cfg = CoreConfig.fromProps(p);
        assertThat(cfg.authConfig().grantType()).isEqualTo(AuthConfig.GrantType.PASSWORD);
        assertThat(cfg.authConfig().tokenUrl())
                .isEqualTo(URI.create("https://sso.example.com/token"));
        assertThat(cfg.authConfig().scope()).isEqualTo("useraccount");
        assertThat(cfg.toString()).doesNotContain("s3cr3t").doesNotContain("hunter2");
        p.put(CoreConfigDefs.OAUTH_TOKEN_URL, "::bad::");
        assertThatThrownBy(() -> CoreConfig.fromProps(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CoreConfigDefs.OAUTH_TOKEN_URL);
        p.put(CoreConfigDefs.OAUTH_TOKEN_URL, "");
        p.put(CoreConfigDefs.AUTH_PASSWORD, "");
        assertThatThrownBy(() -> CoreConfig.fromProps(p))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CoreConfigDefs.AUTH_PASSWORD);
    }

    @Test
    void rejectsBadUrlsAndInvalidValues() {
        for (String bad :
                new String[] {
                    "ftp://acme",
                    "acme.service-now.com",
                    "https://",
                    "https://acme/x?y=1",
                    "https://acme/#f",
                    "http://[bad"
                }) {
            Map<String, String> p = basic();
            p.put(CoreConfigDefs.URL, bad);
            assertThatThrownBy(() -> CoreConfig.fromProps(p))
                    .as(bad)
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("snow.url");
        }
        Map<String, String> missingUser = basic();
        missingUser.remove(CoreConfigDefs.AUTH_USERNAME);
        assertThatThrownBy(() -> CoreConfig.fromProps(missingUser))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CoreConfigDefs.AUTH_USERNAME);
        Map<String, String> badType = basic();
        badType.put(CoreConfigDefs.AUTH_TYPE, "saml");
        assertThatThrownBy(() -> CoreConfig.fromProps(badType)).isInstanceOf(ConfigException.class);
        Map<String, String> backoff = basic();
        backoff.put(CoreConfigDefs.RETRY_INITIAL_BACKOFF_MS, "5000");
        backoff.put(CoreConfigDefs.RETRY_MAX_BACKOFF_MS, "1000");
        assertThatThrownBy(() -> CoreConfig.fromProps(backoff))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CoreConfigDefs.RETRY_MAX_BACKOFF_MS);
        Map<String, String> keystore = basic();
        keystore.put(CoreConfigDefs.TLS_KEYSTORE_PATH, "/k.p12");
        assertThatThrownBy(() -> CoreConfig.fromProps(keystore))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CoreConfigDefs.TLS_KEYSTORE_PASSWORD);
        Map<String, String> proxy = basic();
        proxy.put(CoreConfigDefs.HTTP_PROXY_URL, "::bad::");
        assertThatThrownBy(() -> CoreConfig.fromProps(proxy))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(CoreConfigDefs.HTTP_PROXY_URL);
        proxy.put(CoreConfigDefs.HTTP_PROXY_URL, "proxy-without-scheme");
        assertThatThrownBy(() -> CoreConfig.fromProps(proxy))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("host");
    }

    @Test
    void proxyTlsAndTuningKeysAreRead() {
        Map<String, String> p = basic();
        p.put(CoreConfigDefs.HTTP_PROXY_URL, "http://proxy.internal:3128");
        p.put(CoreConfigDefs.HTTP_PROXY_USERNAME, "pu");
        p.put(CoreConfigDefs.HTTP_PROXY_PASSWORD, "pp");
        p.put(CoreConfigDefs.TLS_TRUSTSTORE_PATH, "/t.p12");
        p.put(CoreConfigDefs.TLS_TRUSTSTORE_PASSWORD, "tp");
        p.put(CoreConfigDefs.TLS_TRUSTSTORE_TYPE, "PKCS12");
        p.put(CoreConfigDefs.TLS_KEYSTORE_PATH, "/k.p12");
        p.put(CoreConfigDefs.TLS_KEYSTORE_PASSWORD, "kp");
        p.put(CoreConfigDefs.HTTP_MAX_CONCURRENT_REQUESTS, "3");
        p.put(CoreConfigDefs.HTTP_ADAPTIVE_THROTTLING, "false");
        p.put(CoreConfigDefs.HTTP_MAX_RESPONSE_BYTES, "4096");
        p.put(CoreConfigDefs.HTTP_USER_AGENT, "custom/1");
        p.put(CoreConfigDefs.RETRY_MAX_ATTEMPTS, "2");
        CoreConfig cfg = CoreConfig.fromProps(p);
        assertThat(cfg.httpConfig().proxyUrl()).isEqualTo(URI.create("http://proxy.internal:3128"));
        assertThat(cfg.httpConfig().proxyUsername()).isEqualTo("pu");
        assertThat(cfg.httpConfig().proxyPassword()).isEqualTo("pp");
        assertThat(cfg.httpConfig().truststorePath()).isEqualTo("/t.p12");
        assertThat(cfg.httpConfig().truststoreType()).isEqualTo("PKCS12");
        assertThat(cfg.httpConfig().keystorePassword()).isEqualTo("kp");
        assertThat(cfg.httpConfig().keystoreType()).isEqualTo("JKS");
        assertThat(cfg.httpConfig().maxConcurrentRequests()).isEqualTo(3);
        assertThat(cfg.httpConfig().adaptiveThrottling()).isFalse();
        assertThat(cfg.httpConfig().maxResponseBytes()).isEqualTo(4096);
        assertThat(cfg.httpConfig().userAgent()).isEqualTo("custom/1");
        assertThat(cfg.retryConfig().maxAttempts()).isEqualTo(2);
        assertThat(cfg.toString()).doesNotContain("pp").doesNotContain("tp,").doesNotContain("kp");
    }

    @Test
    void definitionCoversEveryKeyOnceWithPasswordTypesForSecrets() {
        ConfigDef def = CoreConfigDefs.addCore(new ConfigDef());
        assertThat(CoreConfigDefs.keys())
                .contains(
                        CoreConfigDefs.URL,
                        CoreConfigDefs.AUTH_TYPE,
                        CoreConfigDefs.AUTH_USERNAME,
                        CoreConfigDefs.AUTH_PASSWORD,
                        CoreConfigDefs.OAUTH_GRANT_TYPE,
                        CoreConfigDefs.OAUTH_TOKEN_URL,
                        CoreConfigDefs.OAUTH_CLIENT_ID,
                        CoreConfigDefs.OAUTH_CLIENT_SECRET,
                        CoreConfigDefs.OAUTH_SCOPE,
                        CoreConfigDefs.HTTP_CONNECT_TIMEOUT_MS,
                        CoreConfigDefs.HTTP_REQUEST_TIMEOUT_MS,
                        CoreConfigDefs.HTTP_PROXY_URL,
                        CoreConfigDefs.HTTP_PROXY_USERNAME,
                        CoreConfigDefs.HTTP_PROXY_PASSWORD,
                        CoreConfigDefs.HTTP_MAX_CONCURRENT_REQUESTS,
                        CoreConfigDefs.HTTP_MAX_RESPONSE_BYTES,
                        CoreConfigDefs.HTTP_USER_AGENT,
                        CoreConfigDefs.HTTP_ADAPTIVE_THROTTLING,
                        CoreConfigDefs.TLS_TRUSTSTORE_PATH,
                        CoreConfigDefs.TLS_TRUSTSTORE_PASSWORD,
                        CoreConfigDefs.TLS_TRUSTSTORE_TYPE,
                        CoreConfigDefs.TLS_KEYSTORE_PATH,
                        CoreConfigDefs.TLS_KEYSTORE_PASSWORD,
                        CoreConfigDefs.TLS_KEYSTORE_TYPE,
                        CoreConfigDefs.RETRY_MAX_ATTEMPTS,
                        CoreConfigDefs.RETRY_MAX_ELAPSED_MS,
                        CoreConfigDefs.RETRY_INITIAL_BACKOFF_MS,
                        CoreConfigDefs.RETRY_MAX_BACKOFF_MS)
                .hasSize(28);
        for (String secret :
                new String[] {
                    CoreConfigDefs.AUTH_PASSWORD,
                    CoreConfigDefs.OAUTH_CLIENT_SECRET,
                    CoreConfigDefs.HTTP_PROXY_PASSWORD,
                    CoreConfigDefs.TLS_TRUSTSTORE_PASSWORD,
                    CoreConfigDefs.TLS_KEYSTORE_PASSWORD
                }) {
            assertThat(def.configKeys().get(secret).type)
                    .as(secret)
                    .isEqualTo(ConfigDef.Type.PASSWORD);
        }
        assertThat(def.configKeys().get(CoreConfigDefs.URL).hasDefault()).isFalse();
        assertThat(def.configKeys().get(CoreConfigDefs.AUTH_TYPE).defaultValue).isEqualTo("basic");
        assertThat(def.configKeys().values())
                .allSatisfy(k -> assertThat(k.documentation).isNotBlank());
        // adding to a def that already has keys does not clash
        ConfigDef combined =
                CoreConfigDefs.addCore(
                        new ConfigDef()
                                .define(
                                        "snow.tables",
                                        ConfigDef.Type.STRING,
                                        ConfigDef.Importance.HIGH,
                                        "x"));
        assertThat(combined.names()).hasSize(29);
    }
}
