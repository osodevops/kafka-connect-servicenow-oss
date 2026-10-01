package sh.oso.servicenow.common;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Secret masking for log lines, exception messages, {@code toString()} output and reporter
 * payloads. Everything that could carry a credential passes through here before it is rendered.
 */
public final class Redaction {

    public static final String MASK = "***";

    private static final Set<String> SENSITIVE_HEADERS =
            Set.of(
                    "authorization",
                    "proxy-authorization",
                    "cookie",
                    "set-cookie",
                    "x-auth-token",
                    "x-usertoken");

    /** JSON members whose name mentions a credential: {@code "client_secret": "..."}. */
    private static final Pattern JSON_SECRET =
            Pattern.compile(
                    "(\"[^\"]*(?:password|passwd|secret|token|authorization|credential)[^\"]*\"\\s*:\\s*)\"(?:[^\"\\\\]|\\\\.)*\"",
                    Pattern.CASE_INSENSITIVE);

    /** Form fields whose name mentions a credential: {@code client_secret=...&}. */
    private static final Pattern FORM_SECRET =
            Pattern.compile(
                    "((?:^|[&?\\s])[^&=\\s]*(?:password|passwd|secret|token|authorization|credential)[^&=\\s]*=)[^&\\s]*",
                    Pattern.CASE_INSENSITIVE);

    /** Inline credentials such as {@code Bearer abc.def} or {@code Basic Zm9v}. */
    private static final Pattern SCHEME_SECRET =
            Pattern.compile("\\b(Bearer|Basic)\\s+[A-Za-z0-9._~+/=-]+", Pattern.CASE_INSENSITIVE);

    private Redaction() {}

    /** Masks a secret value for display; {@code null} stays {@code null}, empty stays empty. */
    public static String mask(String secret) {
        if (secret == null) {
            return null;
        }
        return secret.isEmpty() ? "" : MASK;
    }

    /** Redacts credential-bearing members of a JSON, form-encoded or free-text body. */
    public static String redactBody(String body) {
        if (body == null || body.isEmpty()) {
            return body;
        }
        String out = JSON_SECRET.matcher(body).replaceAll("$1\"" + MASK + "\"");
        out = FORM_SECRET.matcher(out).replaceAll("$1" + MASK);
        out = SCHEME_SECRET.matcher(out).replaceAll("$1 " + MASK);
        return out;
    }

    /** Renders headers with sensitive values masked, sorted by name. */
    public static String redactHeaders(Map<String, List<String>> headers) {
        if (headers == null) {
            return "{}";
        }
        TreeMap<String, String> rendered = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, List<String>> e : headers.entrySet()) {
            String name = e.getKey();
            if (name == null) {
                continue;
            }
            if (SENSITIVE_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                rendered.put(name, "[" + MASK + "]");
            } else {
                rendered.put(name, String.valueOf(e.getValue()));
            }
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : rendered.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.append('}').toString();
    }

    /** Shortens a body excerpt for diagnostics after redaction. */
    public static String excerpt(String body, int maxChars) {
        if (body == null) {
            return "";
        }
        String redacted = redactBody(body);
        return redacted.length() > maxChars ? redacted.substring(0, maxChars) + "..." : redacted;
    }
}
