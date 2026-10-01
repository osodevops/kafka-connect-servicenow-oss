package sh.oso.servicenow.http;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A request relative to the instance URL. {@code pathAndQuery} must already be URL-encoded (see
 * {@link sh.oso.servicenow.table.QueryRequest}). {@link #idempotent()} defaults to true for GET,
 * PUT, PATCH and DELETE and false for POST; it decides whether a failure after the request was sent
 * may be retried.
 */
public final class RequestSpec {

    private final String method;
    private final String pathAndQuery;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private final byte[] body;
    private boolean idempotent;

    private RequestSpec(String method, String pathAndQuery, byte[] body, boolean idempotent) {
        this.method = Objects.requireNonNull(method, "method");
        this.pathAndQuery = Objects.requireNonNull(pathAndQuery, "pathAndQuery");
        this.body = body;
        this.idempotent = idempotent;
        if (body != null) {
            headers.put("Content-Type", "application/json");
        }
    }

    public static RequestSpec get(String pathAndQuery) {
        return new RequestSpec("GET", pathAndQuery, null, true);
    }

    public static RequestSpec delete(String pathAndQuery) {
        return new RequestSpec("DELETE", pathAndQuery, null, true);
    }

    public static RequestSpec post(String pathAndQuery, String jsonBody) {
        return new RequestSpec("POST", pathAndQuery, bytes(jsonBody), false);
    }

    public static RequestSpec patch(String pathAndQuery, String jsonBody) {
        return new RequestSpec("PATCH", pathAndQuery, bytes(jsonBody), true);
    }

    public static RequestSpec put(String pathAndQuery, String jsonBody) {
        return new RequestSpec("PUT", pathAndQuery, bytes(jsonBody), true);
    }

    private static byte[] bytes(String json) {
        return json == null ? new byte[0] : json.getBytes(StandardCharsets.UTF_8);
    }

    public RequestSpec header(String name, String value) {
        headers.put(name, value);
        return this;
    }

    public RequestSpec idempotent(boolean idempotent) {
        this.idempotent = idempotent;
        return this;
    }

    public boolean idempotent() {
        return idempotent;
    }

    public String method() {
        return method;
    }

    public String pathAndQuery() {
        return pathAndQuery;
    }

    /** Path without the query string, for logging. */
    public String path() {
        int q = pathAndQuery.indexOf('?');
        return q < 0 ? pathAndQuery : pathAndQuery.substring(0, q);
    }

    public Map<String, String> headers() {
        return Collections.unmodifiableMap(headers);
    }

    public byte[] body() {
        return body;
    }

    public boolean hasBody() {
        return body != null;
    }

    @Override
    public String toString() {
        return method + " " + pathAndQuery;
    }
}
