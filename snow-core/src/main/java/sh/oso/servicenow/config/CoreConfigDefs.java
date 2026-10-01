package sh.oso.servicenow.config;

import java.util.List;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Width;
import sh.oso.servicenow.common.Version;

/**
 * The {@code snow.*} keys shared by the source and sink connectors: connection, authentication,
 * HTTP transport, TLS and retry. Defined once here so both connectors validate and document them
 * identically ({@link #addCore(ConfigDef)}).
 */
public final class CoreConfigDefs {

    public static final String GROUP_CONNECTION = "Connection";
    public static final String GROUP_AUTH = "Authentication";
    public static final String GROUP_HTTP = "HTTP";
    public static final String GROUP_TLS = "TLS";
    public static final String GROUP_RETRY = "Retry";

    public static final String URL = "snow.url";
    public static final String AUTH_TYPE = "snow.auth.type";
    public static final String AUTH_USERNAME = "snow.auth.username";
    public static final String AUTH_PASSWORD = "snow.auth.password";
    public static final String OAUTH_GRANT_TYPE = "snow.oauth.grant.type";
    public static final String OAUTH_TOKEN_URL = "snow.oauth.token.url";
    public static final String OAUTH_CLIENT_ID = "snow.oauth.client.id";
    public static final String OAUTH_CLIENT_SECRET = "snow.oauth.client.secret";
    public static final String OAUTH_SCOPE = "snow.oauth.scope";

    public static final String HTTP_CONNECT_TIMEOUT_MS = "snow.http.connect.timeout.ms";
    public static final String HTTP_REQUEST_TIMEOUT_MS = "snow.http.request.timeout.ms";
    public static final String HTTP_PROXY_URL = "snow.http.proxy.url";
    public static final String HTTP_PROXY_USERNAME = "snow.http.proxy.username";
    public static final String HTTP_PROXY_PASSWORD = "snow.http.proxy.password";
    public static final String HTTP_MAX_CONCURRENT_REQUESTS = "snow.http.max.concurrent.requests";
    public static final String HTTP_MAX_RESPONSE_BYTES = "snow.http.max.response.bytes";
    public static final String HTTP_USER_AGENT = "snow.http.user.agent";
    public static final String HTTP_ADAPTIVE_THROTTLING = "snow.http.adaptive.throttling";

    public static final String TLS_TRUSTSTORE_PATH = "snow.tls.truststore.path";
    public static final String TLS_TRUSTSTORE_PASSWORD = "snow.tls.truststore.password";
    public static final String TLS_TRUSTSTORE_TYPE = "snow.tls.truststore.type";
    public static final String TLS_KEYSTORE_PATH = "snow.tls.keystore.path";
    public static final String TLS_KEYSTORE_PASSWORD = "snow.tls.keystore.password";
    public static final String TLS_KEYSTORE_TYPE = "snow.tls.keystore.type";

    public static final String RETRY_MAX_ATTEMPTS = "snow.retry.max.attempts";
    public static final String RETRY_MAX_ELAPSED_MS = "snow.retry.max.elapsed.ms";
    public static final String RETRY_INITIAL_BACKOFF_MS = "snow.retry.initial.backoff.ms";
    public static final String RETRY_MAX_BACKOFF_MS = "snow.retry.max.backoff.ms";

    public static final String AUTH_TYPE_BASIC = "basic";
    public static final String AUTH_TYPE_OAUTH2 = "oauth2";
    public static final String GRANT_CLIENT_CREDENTIALS = "client_credentials";
    public static final String GRANT_PASSWORD = "password";

    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 60_000;
    public static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 8;
    public static final long DEFAULT_MAX_RESPONSE_BYTES = 64L * 1024 * 1024;
    public static final int DEFAULT_RETRY_MAX_ATTEMPTS = 5;
    public static final long DEFAULT_RETRY_MAX_ELAPSED_MS = 300_000L;
    public static final long DEFAULT_RETRY_INITIAL_BACKOFF_MS = 500L;
    public static final long DEFAULT_RETRY_MAX_BACKOFF_MS = 30_000L;
    public static final String DEFAULT_STORE_TYPE = "JKS";

    private CoreConfigDefs() {}

    /** All shared keys as a fresh {@link ConfigDef}. */
    public static ConfigDef coreDef() {
        return addCore(new ConfigDef());
    }

    /** Adds every shared key to {@code def} and returns it. */
    public static ConfigDef addCore(ConfigDef def) {
        int c = 0;
        def.define(
                URL,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                Importance.HIGH,
                "ServiceNow instance URL, for example https://acme.service-now.com. Only the scheme,"
                        + " host and port are used; the lower-cased host identifies the instance in"
                        + " source partitions.",
                GROUP_CONNECTION,
                ++c,
                Width.LONG,
                "Instance URL");

        int a = 0;
        def.define(
                AUTH_TYPE,
                Type.STRING,
                AUTH_TYPE_BASIC,
                ValidString.in(AUTH_TYPE_BASIC, AUTH_TYPE_OAUTH2),
                Importance.HIGH,
                "Authentication flow: `basic` (username and password on every request) or `oauth2`"
                        + " (bearer tokens from the instance token endpoint, see"
                        + " snow.oauth.grant.type).",
                GROUP_AUTH,
                ++a,
                Width.SHORT,
                "Auth type");
        def.define(
                AUTH_USERNAME,
                Type.STRING,
                null,
                Importance.HIGH,
                "Integration user. Required for `basic` and for the OAuth `password` grant. The user"
                        + " must have its time zone set to UTC.",
                GROUP_AUTH,
                ++a,
                Width.MEDIUM,
                "Username");
        def.define(
                AUTH_PASSWORD,
                Type.PASSWORD,
                null,
                Importance.HIGH,
                "Password for snow.auth.username. Required for `basic` and for the OAuth `password`"
                        + " grant.",
                GROUP_AUTH,
                ++a,
                Width.MEDIUM,
                "Password");
        def.define(
                OAUTH_GRANT_TYPE,
                Type.STRING,
                GRANT_CLIENT_CREDENTIALS,
                ValidString.in(GRANT_CLIENT_CREDENTIALS, GRANT_PASSWORD),
                Importance.MEDIUM,
                "OAuth 2.0 grant when snow.auth.type=oauth2: `client_credentials` (needs the instance"
                        + " property glide.oauth.inbound.client.credential.grant_type.enabled and an"
                        + " application user) or `password` (resource owner password, reusing"
                        + " snow.auth.username and snow.auth.password).",
                GROUP_AUTH,
                ++a,
                Width.SHORT,
                "OAuth grant type");
        def.define(
                OAUTH_TOKEN_URL,
                Type.STRING,
                null,
                Importance.LOW,
                "Token endpoint override. Defaults to {snow.url}/oauth_token.do.",
                GROUP_AUTH,
                ++a,
                Width.LONG,
                "OAuth token URL");
        def.define(
                OAUTH_CLIENT_ID,
                Type.STRING,
                null,
                Importance.MEDIUM,
                "OAuth application registry client id. Required when snow.auth.type=oauth2.",
                GROUP_AUTH,
                ++a,
                Width.MEDIUM,
                "OAuth client id");
        def.define(
                OAUTH_CLIENT_SECRET,
                Type.PASSWORD,
                null,
                Importance.MEDIUM,
                "OAuth application registry client secret. Required when snow.auth.type=oauth2.",
                GROUP_AUTH,
                ++a,
                Width.MEDIUM,
                "OAuth client secret");
        def.define(
                OAUTH_SCOPE,
                Type.STRING,
                null,
                Importance.LOW,
                "Optional OAuth scope sent with the token request.",
                GROUP_AUTH,
                ++a,
                Width.MEDIUM,
                "OAuth scope");

        int h = 0;
        def.define(
                HTTP_CONNECT_TIMEOUT_MS,
                Type.INT,
                DEFAULT_CONNECT_TIMEOUT_MS,
                Range.atLeast(1),
                Importance.LOW,
                "TCP connect timeout in milliseconds.",
                GROUP_HTTP,
                ++h,
                Width.SHORT,
                "Connect timeout (ms)");
        def.define(
                HTTP_REQUEST_TIMEOUT_MS,
                Type.INT,
                DEFAULT_REQUEST_TIMEOUT_MS,
                Range.atLeast(1),
                Importance.LOW,
                "Per-request timeout in milliseconds (time to response headers). Also bounds the wait"
                        + " for a concurrency permit.",
                GROUP_HTTP,
                ++h,
                Width.SHORT,
                "Request timeout (ms)");
        def.define(
                HTTP_PROXY_URL,
                Type.STRING,
                null,
                Importance.LOW,
                "HTTP proxy, for example http://proxy.internal:3128.",
                GROUP_HTTP,
                ++h,
                Width.LONG,
                "Proxy URL");
        def.define(
                HTTP_PROXY_USERNAME,
                Type.STRING,
                null,
                Importance.LOW,
                "Proxy username, when the proxy requires authentication.",
                GROUP_HTTP,
                ++h,
                Width.MEDIUM,
                "Proxy username");
        def.define(
                HTTP_PROXY_PASSWORD,
                Type.PASSWORD,
                null,
                Importance.LOW,
                "Proxy password.",
                GROUP_HTTP,
                ++h,
                Width.MEDIUM,
                "Proxy password");
        def.define(
                HTTP_MAX_CONCURRENT_REQUESTS,
                Type.INT,
                DEFAULT_MAX_CONCURRENT_REQUESTS,
                Range.atLeast(1),
                Importance.MEDIUM,
                "Maximum requests in flight to this instance from this worker JVM, shared by every"
                        + " connector that targets the same instance.",
                GROUP_HTTP,
                ++h,
                Width.SHORT,
                "Max concurrent requests");
        def.define(
                HTTP_MAX_RESPONSE_BYTES,
                Type.LONG,
                DEFAULT_MAX_RESPONSE_BYTES,
                Range.atLeast(1024L),
                Importance.LOW,
                "Responses larger than this many bytes (after decompression) fail the request.",
                GROUP_HTTP,
                ++h,
                Width.SHORT,
                "Max response bytes");
        def.define(
                HTTP_USER_AGENT,
                Type.STRING,
                "kafka-connect-servicenow/" + Version.get(),
                Importance.LOW,
                "User-Agent header sent with every request.",
                GROUP_HTTP,
                ++h,
                Width.MEDIUM,
                "User agent");
        def.define(
                HTTP_ADAPTIVE_THROTTLING,
                Type.BOOLEAN,
                true,
                Importance.LOW,
                "Halve the in-flight limit when the instance answers 429 and recover one permit per"
                        + " 50 successful requests, never above snow.http.max.concurrent.requests.",
                GROUP_HTTP,
                ++h,
                Width.SHORT,
                "Adaptive throttling");

        int t = 0;
        def.define(
                TLS_TRUSTSTORE_PATH,
                Type.STRING,
                null,
                Importance.LOW,
                "Truststore file for the instance certificate chain, when the JDK defaults are not"
                        + " enough.",
                GROUP_TLS,
                ++t,
                Width.LONG,
                "Truststore path");
        def.define(
                TLS_TRUSTSTORE_PASSWORD,
                Type.PASSWORD,
                null,
                Importance.LOW,
                "Truststore password.",
                GROUP_TLS,
                ++t,
                Width.MEDIUM,
                "Truststore password");
        def.define(
                TLS_TRUSTSTORE_TYPE,
                Type.STRING,
                DEFAULT_STORE_TYPE,
                Importance.LOW,
                "Truststore type (JKS or PKCS12).",
                GROUP_TLS,
                ++t,
                Width.SHORT,
                "Truststore type");
        def.define(
                TLS_KEYSTORE_PATH,
                Type.STRING,
                null,
                Importance.LOW,
                "Keystore holding the client certificate for mutual TLS.",
                GROUP_TLS,
                ++t,
                Width.LONG,
                "Keystore path");
        def.define(
                TLS_KEYSTORE_PASSWORD,
                Type.PASSWORD,
                null,
                Importance.LOW,
                "Keystore password.",
                GROUP_TLS,
                ++t,
                Width.MEDIUM,
                "Keystore password");
        def.define(
                TLS_KEYSTORE_TYPE,
                Type.STRING,
                DEFAULT_STORE_TYPE,
                Importance.LOW,
                "Keystore type (JKS or PKCS12).",
                GROUP_TLS,
                ++t,
                Width.SHORT,
                "Keystore type");

        int r = 0;
        def.define(
                RETRY_MAX_ATTEMPTS,
                Type.INT,
                DEFAULT_RETRY_MAX_ATTEMPTS,
                Range.atLeast(1),
                Importance.MEDIUM,
                "Total attempts per request for retryable failures (429, 408, 425, 5xx, transient"
                        + " I/O) before the failure surfaces to the task as retriable.",
                GROUP_RETRY,
                ++r,
                Width.SHORT,
                "Max attempts");
        def.define(
                RETRY_MAX_ELAPSED_MS,
                Type.LONG,
                DEFAULT_RETRY_MAX_ELAPSED_MS,
                Range.atLeast(0L),
                Importance.MEDIUM,
                "Retrying stops once the next wait would take the request past this many"
                        + " milliseconds in total.",
                GROUP_RETRY,
                ++r,
                Width.SHORT,
                "Max elapsed (ms)");
        def.define(
                RETRY_INITIAL_BACKOFF_MS,
                Type.LONG,
                DEFAULT_RETRY_INITIAL_BACKOFF_MS,
                Range.atLeast(0L),
                Importance.LOW,
                "Ceiling of the first full-jitter backoff in milliseconds; doubles per attempt.",
                GROUP_RETRY,
                ++r,
                Width.SHORT,
                "Initial backoff (ms)");
        def.define(
                RETRY_MAX_BACKOFF_MS,
                Type.LONG,
                DEFAULT_RETRY_MAX_BACKOFF_MS,
                Range.atLeast(0L),
                Importance.LOW,
                "Largest backoff ceiling in milliseconds. A Retry-After header overrides the backoff.",
                GROUP_RETRY,
                ++r,
                Width.SHORT,
                "Max backoff (ms)");
        return def;
    }

    /** Every key defined by {@link #addCore(ConfigDef)}, for docs and tests. */
    public static List<String> keys() {
        return List.copyOf(coreDef().names());
    }
}
