package sh.oso.servicenow;

import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.auth.TokenProvider;
import sh.oso.servicenow.auth.TokenProviders;
import sh.oso.servicenow.common.RetryPolicy;
import sh.oso.servicenow.config.CoreConfig;
import sh.oso.servicenow.http.HttpClientFactory;
import sh.oso.servicenow.http.ServiceNowHttpClient;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.schema.TableMetadataClient;
import sh.oso.servicenow.table.TableApiClient;

/**
 * One-stop factory for everything a connector needs to talk to an instance: the JDK HTTP client,
 * the token provider, the shared per-instance {@link ConcurrencyLimiter}, the retrying {@link
 * ServiceNowHttpClient}, the {@link TableApiClient} and a TTL-cached {@link TableMetadataClient}.
 */
public final class ServiceNowClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceNowClient.class);
    public static final Duration DEFAULT_METADATA_TTL = Duration.ofMinutes(10);

    private final CoreConfig config;
    private final ServiceNowHttpClient http;
    private final TableApiClient tableApi;
    private final TableMetadataClient metadata;

    private ServiceNowClient(CoreConfig config, ServiceNowHttpClient http) {
        this.config = config;
        this.http = http;
        this.tableApi = new TableApiClient(http);
        this.metadata = new TableMetadataClient(tableApi, DEFAULT_METADATA_TTL);
    }

    public static ServiceNowClient create(CoreConfig cfg) {
        return create(cfg, new RetryPolicy(cfg.retryConfig()));
    }

    /** As {@link #create(CoreConfig)} with an explicit retry policy (virtual time in tests). */
    public static ServiceNowClient create(CoreConfig cfg, RetryPolicy retry) {
        Objects.requireNonNull(cfg, "cfg");
        HttpClient jdk = HttpClientFactory.create(cfg.httpConfig());
        TokenProvider tokens =
                TokenProviders.from(
                        cfg.authConfig(),
                        jdk,
                        Clock.systemUTC(),
                        cfg.httpConfig().requestTimeout());
        ConcurrencyLimiter limiter =
                ConcurrencyLimiter.forInstance(
                        cfg.instanceKey(), cfg.httpConfig().maxConcurrentRequests());
        ServiceNowHttpClient http =
                new ServiceNowHttpClient(
                        cfg.instanceUrl(), jdk, tokens, retry, limiter, cfg.httpConfig());
        LOG.info(
                "ServiceNow client ready for {} (auth={}, maxConcurrent={}, retry={})",
                cfg.instanceUrl(),
                cfg.authConfig().type(),
                cfg.httpConfig().maxConcurrentRequests(),
                cfg.retryConfig());
        return new ServiceNowClient(cfg, http);
    }

    public CoreConfig config() {
        return config;
    }

    public TableApiClient tableApi() {
        return tableApi;
    }

    public TableMetadataClient metadata() {
        return metadata;
    }

    public ServiceNowHttpClient http() {
        return http;
    }

    /** Instance wall clock inferred from response {@code Date} headers. */
    public Clock serverClock() {
        return http.serverClock();
    }

    @Override
    public void close() {
        http.close();
    }
}
