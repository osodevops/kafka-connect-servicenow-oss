package sh.oso.servicenow.testing;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.LinkedHashMap;
import java.util.Map;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.config.CoreConfig;
import sh.oso.servicenow.config.CoreConfigDefs;

/**
 * A local fake of the ServiceNow HTTP surface: Basic auth, {@code POST /oauth_token.do}
 * (client_credentials, password, refresh_token) and a stateful Table API over an in-memory {@link
 * TableStore}, with {@link FaultInjector}, {@link MutableClock} and {@link RequestJournal}.
 *
 * <p>Credentials: Basic {@value #USERNAME}/{@value #PASSWORD}; OAuth client {@value #CLIENT_ID}/
 * {@value #CLIENT_SECRET}. {@link #basicAuthProps()} and friends return ready-to-merge connector
 * properties including {@code snow.url}.
 */
public final class MockServiceNowServer implements AutoCloseable {

    public static final String USERNAME = "connect";
    public static final String PASSWORD = "secret";
    public static final String CLIENT_ID = "client-id";
    public static final String CLIENT_SECRET = "client-secret";

    private final WireMockServer server;
    private final MutableClock clock;
    private final TableStore tables;
    private final FaultInjector faults;
    private final RequestJournal journal;
    private final OAuthTokenStore oauth;
    private volatile String advertisedBaseUrl;
    private volatile ServiceNowClient client;

    private MockServiceNowServer(int port, MutableClock clock) {
        this.clock = clock;
        this.tables = new TableStore(clock);
        this.faults = new FaultInjector();
        this.journal = new RequestJournal();
        this.oauth = new OAuthTokenStore(clock);
        TableApiTransformer transformer =
                new TableApiTransformer(
                        tables,
                        faults,
                        journal,
                        oauth,
                        clock,
                        this::linkBaseUrl,
                        USERNAME,
                        PASSWORD,
                        CLIENT_ID,
                        CLIENT_SECRET);
        WireMockConfiguration options =
                WireMockConfiguration.options()
                        .extensions(transformer)
                        .containerThreads(64)
                        .maxRequestJournalEntries(5000);
        if (port > 0) {
            options.port(port);
        } else {
            options.dynamicPort();
        }
        this.server = new WireMockServer(options);
    }

    /** Starts on a dynamic port with the clock at the current second. */
    public static MockServiceNowServer start() {
        return start(0, MutableClock.startingNow());
    }

    public static MockServiceNowServer start(int port) {
        return start(port, MutableClock.startingNow());
    }

    public static MockServiceNowServer start(int port, MutableClock clock) {
        MockServiceNowServer s = new MockServiceNowServer(port, clock);
        s.server.start();
        s.stubRoutes();
        return s;
    }

    private void stubRoutes() {
        server.stubFor(
                any(urlPathMatching("/api/now/.*"))
                        .atPriority(10)
                        .willReturn(aResponse().withTransformers(TableApiTransformer.NAME)));
        server.stubFor(
                post(urlPathEqualTo("/oauth_token.do"))
                        .atPriority(10)
                        .willReturn(aResponse().withTransformers(TableApiTransformer.NAME)));
    }

    public String baseUrl() {
        return server.baseUrl();
    }

    public int port() {
        return server.port();
    }

    /** Base URL used in reference links, for clients inside containers. */
    public void advertise(String externalBaseUrl) {
        this.advertisedBaseUrl = externalBaseUrl;
    }

    private String linkBaseUrl() {
        String a = advertisedBaseUrl;
        return a != null ? a : baseUrl();
    }

    public TableStore tables() {
        return tables;
    }

    public FaultInjector faults() {
        return faults;
    }

    public MutableClock clock() {
        return clock;
    }

    public RequestJournal journal() {
        return journal;
    }

    public OAuthTokenStore oauth() {
        return oauth;
    }

    public WireMockServer wireMock() {
        return server;
    }

    /** Connector properties for Basic auth against this fake. */
    public Map<String, String> basicAuthProps() {
        LinkedHashMap<String, String> p = new LinkedHashMap<>();
        p.put(CoreConfigDefs.URL, baseUrl());
        p.put(CoreConfigDefs.AUTH_TYPE, CoreConfigDefs.AUTH_TYPE_BASIC);
        p.put(CoreConfigDefs.AUTH_USERNAME, USERNAME);
        p.put(CoreConfigDefs.AUTH_PASSWORD, PASSWORD);
        return p;
    }

    /** Connector properties for the OAuth client_credentials grant against this fake. */
    public Map<String, String> oauthClientCredentialsProps() {
        LinkedHashMap<String, String> p = new LinkedHashMap<>();
        p.put(CoreConfigDefs.URL, baseUrl());
        p.put(CoreConfigDefs.AUTH_TYPE, CoreConfigDefs.AUTH_TYPE_OAUTH2);
        p.put(CoreConfigDefs.OAUTH_GRANT_TYPE, CoreConfigDefs.GRANT_CLIENT_CREDENTIALS);
        p.put(CoreConfigDefs.OAUTH_CLIENT_ID, CLIENT_ID);
        p.put(CoreConfigDefs.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
        return p;
    }

    /** Connector properties for the OAuth password grant against this fake. */
    public Map<String, String> oauthPasswordProps() {
        LinkedHashMap<String, String> p = new LinkedHashMap<>();
        p.put(CoreConfigDefs.URL, baseUrl());
        p.put(CoreConfigDefs.AUTH_TYPE, CoreConfigDefs.AUTH_TYPE_OAUTH2);
        p.put(CoreConfigDefs.OAUTH_GRANT_TYPE, CoreConfigDefs.GRANT_PASSWORD);
        p.put(CoreConfigDefs.OAUTH_CLIENT_ID, CLIENT_ID);
        p.put(CoreConfigDefs.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
        p.put(CoreConfigDefs.AUTH_USERNAME, USERNAME);
        p.put(CoreConfigDefs.AUTH_PASSWORD, PASSWORD);
        return p;
    }

    /** Retry settings that keep fault-injection tests fast (1 to 10 ms backoff). */
    public static Map<String, String> fastRetryProps() {
        LinkedHashMap<String, String> p = new LinkedHashMap<>();
        p.put(CoreConfigDefs.RETRY_INITIAL_BACKOFF_MS, "1");
        p.put(CoreConfigDefs.RETRY_MAX_BACKOFF_MS, "10");
        p.put(CoreConfigDefs.RETRY_MAX_ATTEMPTS, "4");
        p.put(CoreConfigDefs.RETRY_MAX_ELAPSED_MS, "5000");
        return p;
    }

    /** Basic-auth {@link CoreConfig} with fast retries. */
    public CoreConfig coreConfig() {
        LinkedHashMap<String, String> p = new LinkedHashMap<>(basicAuthProps());
        p.putAll(fastRetryProps());
        return CoreConfig.fromProps(p);
    }

    /** A shared Basic-auth client against this fake; closed with the server. */
    public ServiceNowClient client() {
        ServiceNowClient c = client;
        if (c == null) {
            synchronized (this) {
                if (client == null) {
                    client = ServiceNowClient.create(coreConfig());
                }
                c = client;
            }
        }
        return c;
    }

    /** Drops rows, faults, journal and issued tokens; keeps the clock. */
    public void reset() {
        tables.clear();
        faults.clear();
        journal.reset();
        server.resetRequests();
    }

    @Override
    public void close() {
        ServiceNowClient c = client;
        if (c != null) {
            c.close();
        }
        server.stop();
    }
}
