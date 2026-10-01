package sh.oso.servicenow.common;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.auth.AuthConfig;
import sh.oso.servicenow.auth.OAuthTokenProvider;
import sh.oso.servicenow.config.CoreConfig;
import sh.oso.servicenow.config.CoreConfigDefs;
import sh.oso.servicenow.http.RequestSpec;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

/** Captures every log line at TRACE across auth, 401 refresh, 5xx retries and failures. */
class NoCredentialLeakTest {

    private MockServiceNowServer snow;
    private ListAppender<ILoggingEvent> appender;
    private Logger root;
    private Level previous;

    @BeforeEach
    void setUp() {
        snow = MockServiceNowServer.start();
        root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        previous = root.getLevel();
        root.setLevel(Level.TRACE);
        appender = new ListAppender<>();
        appender.start();
        root.addAppender(appender);
        ConcurrencyLimiter.clearRegistry();
    }

    @AfterEach
    void tearDown() {
        root.detachAppender(appender);
        root.setLevel(previous);
        snow.close();
    }

    @Test
    void noPasswordSecretOrTokenAppearsInLogsOrExceptions() {
        List<String> captured = new ArrayList<>();
        Map<String, String> props = new LinkedHashMap<>(snow.oauthPasswordProps());
        props.putAll(MockServiceNowServer.fastRetryProps());
        CoreConfig cfg = CoreConfig.fromProps(props);
        captured.add(cfg.toString());
        captured.add(cfg.authConfig().toString());
        captured.add(cfg.httpConfig().toString());

        try (ServiceNowClient client = ServiceNowClient.create(cfg)) {
            client.http().execute(RequestSpec.get("/api/now/table/incident"));
            snow.faults().unauthorizedOnce();
            client.http().execute(RequestSpec.get("/api/now/table/incident"));
            snow.faults().serverError(1, 503);
            client.http().execute(RequestSpec.get("/api/now/table/incident"));
            snow.faults().serverError(1, 400);
            try {
                client.http().execute(RequestSpec.get("/api/now/table/incident"));
            } catch (ServiceNowException e) {
                captured.add(render(e));
            }
            snow.faults().unauthorizedTimes(2);
            try {
                client.http().execute(RequestSpec.get("/api/now/table/incident"));
            } catch (ServiceNowException e) {
                captured.add(render(e));
            }
            captured.add(client.toString());
            captured.add(client.http().toString());
        }

        AuthConfig wrong =
                AuthConfig.builder()
                        .type(AuthConfig.Type.OAUTH2)
                        .grantType(AuthConfig.GrantType.PASSWORD)
                        .tokenUrl(URI.create(snow.baseUrl() + "/oauth_token.do"))
                        .clientId(MockServiceNowServer.CLIENT_ID)
                        .clientSecret("wrong-" + MockServiceNowServer.CLIENT_SECRET)
                        .username(MockServiceNowServer.USERNAME)
                        .password(MockServiceNowServer.PASSWORD)
                        .build();
        OAuthTokenProvider provider =
                new OAuthTokenProvider(
                        wrong, HttpClient.newHttpClient(), snow.clock(), Duration.ofSeconds(5));
        try {
            provider.authorization();
        } catch (ServiceNowException e) {
            captured.add(render(e));
        }
        captured.add(provider.toString());
        captured.add(wrong.toString());

        try {
            CoreConfig.fromProps(
                    Map.of(
                            CoreConfigDefs.URL,
                            snow.baseUrl(),
                            CoreConfigDefs.AUTH_USERNAME,
                            "connect"));
        } catch (RuntimeException e) {
            captured.add(render(e));
        }

        for (ILoggingEvent event : appender.list) {
            captured.add(event.getFormattedMessage());
            if (event.getThrowableProxy() != null) {
                captured.add(event.getThrowableProxy().getMessage());
            }
        }
        assertThat(appender.list).isNotEmpty();
        assertThat(snow.oauth().issuedAccessTokens()).isNotEmpty();

        List<String> forbidden = new ArrayList<>();
        forbidden.add(MockServiceNowServer.PASSWORD);
        forbidden.add(MockServiceNowServer.CLIENT_SECRET);
        forbidden.add(
                Base64.getEncoder()
                        .encodeToString(
                                (MockServiceNowServer.USERNAME
                                                + ":"
                                                + MockServiceNowServer.PASSWORD)
                                        .getBytes(StandardCharsets.UTF_8)));
        forbidden.addAll(snow.oauth().issuedAccessTokens());
        for (String line : captured) {
            for (String secret : forbidden) {
                assertThat(line)
                        .as("captured output must not contain '%s'", secret)
                        .doesNotContain(secret);
            }
        }
    }

    private static String render(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return t + "\n" + sw;
    }
}
