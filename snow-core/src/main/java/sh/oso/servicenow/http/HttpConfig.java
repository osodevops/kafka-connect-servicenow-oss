package sh.oso.servicenow.http;

import java.net.URI;
import java.time.Duration;
import sh.oso.servicenow.common.Redaction;

/** Transport settings: timeouts, proxy, TLS stores, response bound, concurrency. */
public final class HttpConfig {

    public static final long DEFAULT_MAX_RESPONSE_BYTES = 64L * 1024 * 1024;

    private final Duration connectTimeout;
    private final Duration requestTimeout;
    private final URI proxyUrl;
    private final String proxyUsername;
    private final String proxyPassword;
    private final int maxConcurrentRequests;
    private final long maxResponseBytes;
    private final String userAgent;
    private final String truststorePath;
    private final String truststorePassword;
    private final String truststoreType;
    private final String keystorePath;
    private final String keystorePassword;
    private final String keystoreType;
    private final boolean adaptiveThrottling;

    private HttpConfig(Builder b) {
        this.connectTimeout = b.connectTimeout;
        this.requestTimeout = b.requestTimeout;
        this.proxyUrl = b.proxyUrl;
        this.proxyUsername = b.proxyUsername;
        this.proxyPassword = b.proxyPassword;
        this.maxConcurrentRequests = b.maxConcurrentRequests;
        this.maxResponseBytes = b.maxResponseBytes;
        this.userAgent = b.userAgent;
        this.truststorePath = b.truststorePath;
        this.truststorePassword = b.truststorePassword;
        this.truststoreType = b.truststoreType;
        this.keystorePath = b.keystorePath;
        this.keystorePassword = b.keystorePassword;
        this.keystoreType = b.keystoreType;
        this.adaptiveThrottling = b.adaptiveThrottling;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static HttpConfig defaults() {
        return builder().build();
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    public URI proxyUrl() {
        return proxyUrl;
    }

    public String proxyUsername() {
        return proxyUsername;
    }

    public String proxyPassword() {
        return proxyPassword;
    }

    public int maxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public long maxResponseBytes() {
        return maxResponseBytes;
    }

    public String userAgent() {
        return userAgent;
    }

    public String truststorePath() {
        return truststorePath;
    }

    public String truststorePassword() {
        return truststorePassword;
    }

    public String truststoreType() {
        return truststoreType;
    }

    public String keystorePath() {
        return keystorePath;
    }

    public String keystorePassword() {
        return keystorePassword;
    }

    public String keystoreType() {
        return keystoreType;
    }

    public boolean adaptiveThrottling() {
        return adaptiveThrottling;
    }

    @Override
    public String toString() {
        return "HttpConfig{connectTimeout="
                + connectTimeout
                + ", requestTimeout="
                + requestTimeout
                + ", proxyUrl="
                + proxyUrl
                + ", proxyUsername="
                + proxyUsername
                + ", proxyPassword="
                + Redaction.mask(proxyPassword)
                + ", maxConcurrentRequests="
                + maxConcurrentRequests
                + ", maxResponseBytes="
                + maxResponseBytes
                + ", userAgent="
                + userAgent
                + ", truststorePath="
                + truststorePath
                + ", truststorePassword="
                + Redaction.mask(truststorePassword)
                + ", truststoreType="
                + truststoreType
                + ", keystorePath="
                + keystorePath
                + ", keystorePassword="
                + Redaction.mask(keystorePassword)
                + ", keystoreType="
                + keystoreType
                + ", adaptiveThrottling="
                + adaptiveThrottling
                + '}';
    }

    public static final class Builder {
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(60);
        private URI proxyUrl;
        private String proxyUsername;
        private String proxyPassword;
        private int maxConcurrentRequests = 8;
        private long maxResponseBytes = DEFAULT_MAX_RESPONSE_BYTES;
        private String userAgent = "kafka-connect-servicenow";
        private String truststorePath;
        private String truststorePassword;
        private String truststoreType = "JKS";
        private String keystorePath;
        private String keystorePassword;
        private String keystoreType = "JKS";
        private boolean adaptiveThrottling = true;

        private Builder() {}

        public Builder connectTimeout(Duration v) {
            this.connectTimeout = v;
            return this;
        }

        public Builder requestTimeout(Duration v) {
            this.requestTimeout = v;
            return this;
        }

        public Builder proxyUrl(URI v) {
            this.proxyUrl = v;
            return this;
        }

        public Builder proxyUsername(String v) {
            this.proxyUsername = v;
            return this;
        }

        public Builder proxyPassword(String v) {
            this.proxyPassword = v;
            return this;
        }

        public Builder maxConcurrentRequests(int v) {
            this.maxConcurrentRequests = v;
            return this;
        }

        public Builder maxResponseBytes(long v) {
            this.maxResponseBytes = v;
            return this;
        }

        public Builder userAgent(String v) {
            this.userAgent = v;
            return this;
        }

        public Builder truststore(String path, String password, String type) {
            this.truststorePath = path;
            this.truststorePassword = password;
            if (type != null) {
                this.truststoreType = type;
            }
            return this;
        }

        public Builder keystore(String path, String password, String type) {
            this.keystorePath = path;
            this.keystorePassword = password;
            if (type != null) {
                this.keystoreType = type;
            }
            return this;
        }

        public Builder adaptiveThrottling(boolean v) {
            this.adaptiveThrottling = v;
            return this;
        }

        public HttpConfig build() {
            return new HttpConfig(this);
        }
    }
}
