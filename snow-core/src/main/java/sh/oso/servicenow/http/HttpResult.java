package sh.oso.servicenow.http;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/** A decoded (un-gzipped, bounded) successful response. Header lookup is case-insensitive. */
public record HttpResult(
        int status, Map<String, List<String>> headers, byte[] body, String requestId) {

    public HttpResult {
        TreeMap<String, List<String>> ci = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (headers != null) {
            headers.forEach(
                    (k, v) -> {
                        if (k != null) {
                            ci.put(k, List.copyOf(v));
                        }
                    });
        }
        headers = java.util.Collections.unmodifiableMap(ci);
        body = body == null ? new byte[0] : body;
    }

    public String bodyAsString() {
        return new String(body, StandardCharsets.UTF_8);
    }

    public Optional<String> header(String name) {
        List<String> values = headers.get(name);
        return values == null || values.isEmpty()
                ? Optional.empty()
                : Optional.ofNullable(values.get(0));
    }

    public boolean isSuccess() {
        return status >= 200 && status < 300;
    }

    @Override
    public String toString() {
        return "HttpResult{status="
                + status
                + ", bytes="
                + body.length
                + ", requestId="
                + requestId
                + '}';
    }
}
