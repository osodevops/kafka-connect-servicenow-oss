package sh.oso.servicenow.auth;

import java.net.URI;
import java.util.Locale;
import java.util.Objects;
import org.apache.kafka.common.config.ConfigException;
import sh.oso.servicenow.common.Redaction;

/**
 * Credentials and flow selection. {@code snow.auth.type=basic|oauth2}; for OAuth 2.0 the grant is
 * {@code client_credentials} (default) or {@code password} (which reuses the Basic username and
 * password together with the client id and secret). {@link #toString()} masks every secret.
 */
public final class AuthConfig {

    public enum Type {
        BASIC,
        OAUTH2;

        public static Type fromConfig(String value) {
            if (value == null) {
                return BASIC;
            }
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "basic" -> BASIC;
                case "oauth2" -> OAUTH2;
                default ->
                        throw new ConfigException(
                                "snow.auth.type", value, "must be one of [basic, oauth2]");
            };
        }
    }

    public enum GrantType {
        CLIENT_CREDENTIALS,
        PASSWORD;

        public static GrantType fromConfig(String value) {
            if (value == null) {
                return CLIENT_CREDENTIALS;
            }
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "client_credentials" -> CLIENT_CREDENTIALS;
                case "password" -> PASSWORD;
                default ->
                        throw new ConfigException(
                                "snow.oauth.grant.type",
                                value,
                                "must be one of [client_credentials, password]");
            };
        }

        public String wireValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final Type type;
    private final GrantType grantType;
    private final String username;
    private final String password;
    private final URI tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final String scope;

    private AuthConfig(Builder b) {
        this.type = Objects.requireNonNull(b.type, "type");
        this.grantType = b.grantType == null ? GrantType.CLIENT_CREDENTIALS : b.grantType;
        this.username = b.username;
        this.password = b.password;
        this.tokenUrl = b.tokenUrl;
        this.clientId = b.clientId;
        this.clientSecret = b.clientSecret;
        this.scope = b.scope;
        validate();
    }

    private void validate() {
        switch (type) {
            case BASIC -> {
                require(username, "snow.auth.username");
                require(password, "snow.auth.password");
            }
            case OAUTH2 -> {
                Objects.requireNonNull(tokenUrl, "tokenUrl");
                require(clientId, "snow.oauth.client.id");
                require(clientSecret, "snow.oauth.client.secret");
                if (grantType == GrantType.PASSWORD) {
                    require(username, "snow.auth.username");
                    require(password, "snow.auth.password");
                }
            }
        }
    }

    private void require(String value, String key) {
        if (value == null || value.isBlank()) {
            throw new ConfigException(
                    key
                            + " is required for snow.auth.type="
                            + type.name().toLowerCase(Locale.ROOT)
                            + (type == Type.OAUTH2
                                    ? " with snow.oauth.grant.type=" + grantType.wireValue()
                                    : ""));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public Type type() {
        return type;
    }

    public GrantType grantType() {
        return grantType;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public URI tokenUrl() {
        return tokenUrl;
    }

    public String clientId() {
        return clientId;
    }

    public String clientSecret() {
        return clientSecret;
    }

    public String scope() {
        return scope;
    }

    @Override
    public String toString() {
        return "AuthConfig{type="
                + type
                + ", grantType="
                + grantType
                + ", username="
                + username
                + ", password="
                + Redaction.mask(password)
                + ", tokenUrl="
                + tokenUrl
                + ", clientId="
                + clientId
                + ", clientSecret="
                + Redaction.mask(clientSecret)
                + ", scope="
                + scope
                + '}';
    }

    public static final class Builder {
        private Type type = Type.BASIC;
        private GrantType grantType = GrantType.CLIENT_CREDENTIALS;
        private String username;
        private String password;
        private URI tokenUrl;
        private String clientId;
        private String clientSecret;
        private String scope;

        private Builder() {}

        public Builder type(Type type) {
            this.type = type;
            return this;
        }

        public Builder grantType(GrantType grantType) {
            this.grantType = grantType;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder tokenUrl(URI tokenUrl) {
            this.tokenUrl = tokenUrl;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder clientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
            return this;
        }

        public Builder scope(String scope) {
            this.scope = scope;
            return this;
        }

        public AuthConfig build() {
            return new AuthConfig(this);
        }
    }
}
