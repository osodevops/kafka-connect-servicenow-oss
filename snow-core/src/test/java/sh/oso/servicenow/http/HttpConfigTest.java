package sh.oso.servicenow.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowException;

class HttpConfigTest {

    @Test
    void toStringMasksEverySecret() {
        HttpConfig cfg =
                HttpConfig.builder()
                        .proxyUrl(URI.create("http://proxy:3128"))
                        .proxyUsername("pu")
                        .proxyPassword("proxy-pw")
                        .truststore("/t.jks", "trust-pw", "PKCS12")
                        .keystore("/k.jks", "key-pw", null)
                        .build();
        assertThat(cfg.toString())
                .contains("proxyUsername=pu")
                .contains("truststoreType=PKCS12")
                .contains("keystoreType=JKS")
                .doesNotContain("proxy-pw")
                .doesNotContain("trust-pw")
                .doesNotContain("key-pw");
    }

    @Test
    void factoryBuildsClientsWithAndWithoutProxy() {
        HttpClient plain = HttpClientFactory.create(HttpConfig.defaults());
        assertThat(plain.connectTimeout()).contains(Duration.ofSeconds(10));
        assertThat(plain.followRedirects()).isEqualTo(HttpClient.Redirect.NEVER);
        HttpClient proxied =
                HttpClientFactory.create(
                        HttpConfig.builder()
                                .proxyUrl(URI.create("http://proxy.internal"))
                                .proxyUsername("u")
                                .proxyPassword("p")
                                .build());
        assertThat(proxied.proxy()).isPresent();
        assertThat(proxied.authenticator()).isPresent();
        HttpClientFactory.ProxyAuthenticator auth =
                new HttpClientFactory.ProxyAuthenticator("u", null);
        assertThat(auth).isNotNull();
    }

    @Test
    void sslContextIsEmptyWithoutStoresAndFailsClearlyOnMissingFiles() {
        assertThat(SslContexts.forConfig(HttpConfig.defaults())).isEmpty();
        HttpConfig missing =
                HttpConfig.builder().truststore("/no/such/file.jks", "x", "JKS").build();
        assertThatThrownBy(() -> SslContexts.forConfig(missing))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("snow.tls");
    }

    @Test
    void serverClockIgnoresGarbageAndSupportsZones() {
        ServerClock clock = new ServerClock(java.time.Clock.systemUTC());
        clock.observe("not a date");
        assertThat(clock.hasObserved()).isFalse();
        clock.observe(null);
        clock.observe("Tue, 29 Sep 2026 07:30:40 GMT");
        assertThat(clock.hasObserved()).isTrue();
        assertThat(clock.withZone(java.time.ZoneId.of("Europe/London")).getZone().getId())
                .isEqualTo("Europe/London");
        assertThat(
                        ((ServerClock) clock.withZone(java.time.ZoneId.of("Europe/London")))
                                .driftMillis())
                .isEqualTo(clock.driftMillis());
        assertThat(clock.getZone()).isEqualTo(java.time.ZoneOffset.UTC);
    }
}
