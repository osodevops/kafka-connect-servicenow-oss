package sh.oso.servicenow.http;

import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;

/** Builds the JDK {@link HttpClient} from an {@link HttpConfig}. */
public final class HttpClientFactory {

    private HttpClientFactory() {}

    public static HttpClient create(HttpConfig cfg) {
        HttpClient.Builder builder =
                HttpClient.newBuilder()
                        .connectTimeout(cfg.connectTimeout())
                        // Never follow redirects: they would replay the Authorization header.
                        .followRedirects(HttpClient.Redirect.NEVER);
        URI proxy = cfg.proxyUrl();
        if (proxy != null) {
            int port = proxy.getPort() > 0 ? proxy.getPort() : 3128;
            builder.proxy(ProxySelector.of(new InetSocketAddress(proxy.getHost(), port)));
            if (cfg.proxyUsername() != null && !cfg.proxyUsername().isBlank()) {
                builder.authenticator(
                        new ProxyAuthenticator(cfg.proxyUsername(), cfg.proxyPassword()));
            }
        }
        SslContexts.forConfig(cfg).ifPresent(builder::sslContext);
        return builder.build();
    }

    /** Answers proxy challenges only; server 401s are handled by the token provider. */
    static final class ProxyAuthenticator extends Authenticator {
        private final String username;
        private final char[] password;

        ProxyAuthenticator(String username, String password) {
            this.username = username;
            this.password = password == null ? new char[0] : password.toCharArray();
        }

        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
            if (getRequestorType() == RequestorType.PROXY) {
                return new PasswordAuthentication(username, password.clone());
            }
            return null;
        }
    }
}
