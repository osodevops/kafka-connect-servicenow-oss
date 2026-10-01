package sh.oso.servicenow.http;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import sh.oso.servicenow.common.ServiceNowException;

/** Builds an {@link SSLContext} from the configured truststore and keystore (mTLS). */
public final class SslContexts {

    private SslContexts() {}

    /** Empty when neither store is configured, so the JDK defaults apply. */
    public static Optional<SSLContext> forConfig(HttpConfig cfg) {
        boolean hasTrust = cfg.truststorePath() != null && !cfg.truststorePath().isBlank();
        boolean hasKey = cfg.keystorePath() != null && !cfg.keystorePath().isBlank();
        if (!hasTrust && !hasKey) {
            return Optional.empty();
        }
        try {
            TrustManager[] trustManagers = null;
            if (hasTrust) {
                KeyStore trust =
                        load(cfg.truststorePath(), cfg.truststorePassword(), cfg.truststoreType());
                TrustManagerFactory tmf =
                        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trust);
                trustManagers = tmf.getTrustManagers();
            }
            KeyManager[] keyManagers = null;
            if (hasKey) {
                KeyStore keys =
                        load(cfg.keystorePath(), cfg.keystorePassword(), cfg.keystoreType());
                KeyManagerFactory kmf =
                        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keys, chars(cfg.keystorePassword()));
                keyManagers = kmf.getKeyManagers();
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagers, trustManagers, null);
            return Optional.of(context);
        } catch (GeneralSecurityException | IOException e) {
            throw new ServiceNowException(
                    "Failed to initialise TLS from snow.tls.*: " + e.getMessage(), e);
        }
    }

    private static KeyStore load(String path, String password, String type)
            throws GeneralSecurityException, IOException {
        KeyStore store = KeyStore.getInstance(type == null ? KeyStore.getDefaultType() : type);
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            store.load(in, chars(password));
        }
        return store;
    }

    private static char[] chars(String s) {
        return s == null ? null : s.toCharArray();
    }
}
