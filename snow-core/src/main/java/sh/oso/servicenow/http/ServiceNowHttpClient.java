package sh.oso.servicenow.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.auth.TokenProvider;
import sh.oso.servicenow.common.ErrorClassifier;
import sh.oso.servicenow.common.RetryPolicy;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.limits.ConcurrencyLimiter;

/**
 * Authenticated transport for the ServiceNow REST API.
 *
 * <p>Every request gets {@code Authorization} from the {@link TokenProvider}, {@code Accept:
 * application/json}, {@code Accept-Encoding: gzip} (decoded here), a {@code User-Agent} and an
 * {@code X-Request-Id} UUID that also appears in exceptions. A 401 invalidates the token and the
 * request is repeated once with a fresh one; a second 401 fails. Every attempt runs inside the
 * {@link RetryPolicy} (the {@link RequestSpec#idempotent()} flag decides whether a failure after
 * sending may be retried) and holds a {@link ConcurrencyLimiter} permit while on the wire. Response
 * bodies are bounded by {@link HttpConfig#maxResponseBytes()}; the {@code Date} header feeds the
 * {@link #serverClock()}. Retries, throttled time and the last success are counted in {@link
 * #stats()}.
 */
public final class ServiceNowHttpClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceNowHttpClient.class);

    private final URI baseUrl;
    private final HttpClient http;
    private final TokenProvider tokens;
    private final RetryPolicy retry;
    private final ConcurrencyLimiter limiter;
    private final HttpConfig cfg;
    private final ServerClock serverClock;
    private final Clock localClock;
    private final HttpStats stats = new HttpStats();

    public ServiceNowHttpClient(
            URI baseUrl,
            HttpClient http,
            TokenProvider tokens,
            RetryPolicy retry,
            ConcurrencyLimiter limiter,
            HttpConfig cfg) {
        this(baseUrl, http, tokens, retry, limiter, cfg, Clock.systemUTC());
    }

    public ServiceNowHttpClient(
            URI baseUrl,
            HttpClient http,
            TokenProvider tokens,
            RetryPolicy retry,
            ConcurrencyLimiter limiter,
            HttpConfig cfg,
            Clock localClock) {
        this.baseUrl = stripTrailingSlash(Objects.requireNonNull(baseUrl, "baseUrl"));
        this.http = Objects.requireNonNull(http, "http");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.retry = Objects.requireNonNull(retry, "retry").withObserver(stats::onRetry);
        this.limiter = Objects.requireNonNull(limiter, "limiter");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.localClock = localClock == null ? Clock.systemUTC() : localClock;
        this.serverClock = new ServerClock(this.localClock);
    }

    public URI baseUrl() {
        return baseUrl;
    }

    public Clock serverClock() {
        return serverClock;
    }

    public ConcurrencyLimiter limiter() {
        return limiter;
    }

    public HttpConfig config() {
        return cfg;
    }

    /** Retry, throttling and last-success counters for this client. */
    public HttpStats stats() {
        return stats;
    }

    /**
     * Executes with retries; any status of 400 or above raises a {@link ServiceNowApiException}.
     */
    public HttpResult execute(RequestSpec spec) {
        String requestId = UUID.randomUUID().toString();
        return retry.execute(
                spec.method() + " " + spec.path(),
                () -> attempt(spec, requestId),
                spec.idempotent());
    }

    private HttpResult attempt(RequestSpec spec, String requestId) {
        String auth = tokens.authorization();
        HttpResult result = send(spec, auth, requestId);
        if (result.status() == 401) {
            LOG.info(
                    "{} {} returned 401; refreshing credentials and retrying once (request-id {})",
                    spec.method(),
                    spec.path(),
                    requestId);
            tokens.invalidate(auth);
            String fresh = tokens.authorization();
            result = send(spec, fresh, requestId);
        }
        if (result.status() >= 400) {
            ServiceNowApiException failure =
                    ErrorClassifier.classify(
                            result.status(), result.bodyAsString(), result.headers(), requestId);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "{} {} -> HTTP {} (request-id {}, retryable={})",
                        spec.method(),
                        spec.path(),
                        result.status(),
                        requestId,
                        failure.isRetryable());
            }
            throw failure;
        }
        stats.succeeded(localClock.millis());
        return result;
    }

    private HttpResult send(RequestSpec spec, String authorization, String requestId) {
        HttpRequest request = build(spec, authorization, requestId);
        try (ConcurrencyLimiter.Permit ignored = limiter.acquire(cfg.requestTimeout())) {
            HttpResponse<InputStream> response;
            try {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "{} {} (request-id {})", spec.method(), spec.pathAndQuery(), requestId);
                }
                response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            } catch (IOException e) {
                throw ErrorClassifier.transportFailure(
                        e, spec.method(), spec.path(), spec.idempotent(), requestId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ServiceNowException(
                        "Interrupted during " + spec.method() + " " + spec.path(), e);
            }
            byte[] body = readBody(response, spec, requestId);
            Map<String, java.util.List<String>> headers = response.headers().map();
            response.headers().firstValue("Date").ifPresent(serverClock::observe);
            if (cfg.adaptiveThrottling()) {
                if (response.statusCode() == 429) {
                    limiter.onThrottled();
                } else if (response.statusCode() < 400) {
                    limiter.onSuccess();
                }
            }
            return new HttpResult(response.statusCode(), headers, body, requestId);
        }
    }

    private HttpRequest build(RequestSpec spec, String authorization, String requestId) {
        HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(baseUrl + spec.pathAndQuery()))
                        .timeout(cfg.requestTimeout())
                        .header("Authorization", authorization)
                        .header("Accept", "application/json")
                        .header("Accept-Encoding", "gzip")
                        .header("User-Agent", cfg.userAgent())
                        .header("X-Request-Id", requestId);
        for (Map.Entry<String, String> h : spec.headers().entrySet()) {
            builder.setHeader(h.getKey(), h.getValue());
        }
        HttpRequest.BodyPublisher publisher =
                spec.hasBody()
                        ? HttpRequest.BodyPublishers.ofByteArray(spec.body())
                        : HttpRequest.BodyPublishers.noBody();
        switch (spec.method()) {
            case "GET" -> builder.GET();
            case "DELETE" -> builder.DELETE();
            default -> builder.method(spec.method(), publisher);
        }
        return builder.build();
    }

    private byte[] readBody(
            HttpResponse<InputStream> response, RequestSpec spec, String requestId) {
        long max = cfg.maxResponseBytes();
        boolean gzip =
                response.headers()
                        .firstValue("Content-Encoding")
                        .map(v -> v.toLowerCase(java.util.Locale.ROOT).contains("gzip"))
                        .orElse(false);
        try (InputStream raw = response.body()) {
            byte[] bytes = readBounded(raw, max, spec, requestId);
            if (gzip && bytes.length > 0) {
                try (InputStream in =
                        new GZIPInputStream(new java.io.ByteArrayInputStream(bytes))) {
                    return readBounded(in, max, spec, requestId);
                }
            }
            return bytes;
        } catch (IOException e) {
            throw ErrorClassifier.transportFailure(
                    e, spec.method(), spec.path(), spec.idempotent(), requestId);
        }
    }

    private static byte[] readBounded(InputStream in, long max, RequestSpec spec, String requestId)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        long total = 0;
        int n;
        while ((n = in.read(buf)) >= 0) {
            total += n;
            if (total > max) {
                throw new ServiceNowException(
                        spec.method()
                                + " "
                                + spec.path()
                                + " response exceeded snow.http.max.response.bytes="
                                + max
                                + " (request-id "
                                + requestId
                                + ")",
                        null,
                        false);
            }
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    private static URI stripTrailingSlash(URI uri) {
        String s = uri.toString();
        while (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }
        return URI.create(s);
    }

    @Override
    public void close() {
        tokens.close();
    }
}
