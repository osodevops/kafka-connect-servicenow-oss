package sh.oso.servicenow.common;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import sh.oso.servicenow.limits.RateLimitInfo;

/**
 * Retry contract for ServiceNow responses and transport failures.
 *
 * <ul>
 *   <li>429, 408, 425, 500, 502, 503, 504: retryable.
 *   <li>409: retryable only when the error text carries a transient hint.
 *   <li>400, 401, 403, 404, 422 and every other status: never retried here (401 is handled once by
 *       the token refresh in the HTTP client).
 *   <li>Connect failures (never sent): retryable.
 *   <li>Timeouts and I/O failures after the request may have been sent: retryable when the request
 *       is idempotent, otherwise ambiguous and not retried.
 * </ul>
 */
public final class ErrorClassifier {

    private static final Set<Integer> RETRYABLE_STATUSES =
            Set.of(408, 425, 429, 500, 502, 503, 504);
    private static final Pattern TRANSIENT_HINT =
            Pattern.compile(
                    "try again|temporar|busy|lock|in progress|timed? ?out|unavailable|overload",
                    Pattern.CASE_INSENSITIVE);
    private static final int EXCERPT_CHARS = 500;

    private ErrorClassifier() {}

    /** Status-code classification, with the error text consulted only for 409. */
    public static boolean isRetryableStatus(int status, String errorText) {
        if (RETRYABLE_STATUSES.contains(status)) {
            return true;
        }
        if (status == 409) {
            return errorText != null && TRANSIENT_HINT.matcher(errorText).find();
        }
        return false;
    }

    public static boolean isRetryable(Throwable t) {
        if (t instanceof ServiceNowException se) {
            return se.isRetryable();
        }
        if (t instanceof HttpConnectTimeoutException
                || t instanceof ConnectException
                || t instanceof UnknownHostException) {
            return true;
        }
        if (t instanceof IOException) {
            // Timeouts, resets, broken pipes: assumed idempotent when not wrapped; the HTTP client
            // wraps them with the request's idempotency so this branch is only a fallback.
            return true;
        }
        return false;
    }

    /** True when the request may have been applied although no usable response arrived. */
    public static boolean isAmbiguous(Throwable t) {
        if (t instanceof ServiceNowException se) {
            return se.isAmbiguous();
        }
        if (t instanceof HttpConnectTimeoutException
                || t instanceof ConnectException
                || t instanceof UnknownHostException) {
            return false;
        }
        return t instanceof IOException;
    }

    /** Builds the exception for an HTTP error response. */
    public static ServiceNowApiException classify(
            int status, String body, Map<String, List<String>> headers, String requestId) {
        return classify(status, body, headers, requestId, Instant.now());
    }

    public static ServiceNowApiException classify(
            int status,
            String body,
            Map<String, List<String>> headers,
            String requestId,
            Instant now) {
        ServiceNowErrorBody parsed = ServiceNowErrorBody.parse(body).orElse(null);
        String message = parsed != null ? parsed.message() : null;
        String detail = parsed != null ? parsed.detail() : null;
        String text = parsed != null ? parsed.text() : (body == null ? "" : body);
        RateLimitInfo rateLimit = RateLimitInfo.parse(headers, now).orElse(null);
        boolean retryable = isRetryableStatus(status, text);
        return new ServiceNowApiException(
                status,
                message,
                detail,
                Redaction.excerpt(body, EXCERPT_CHARS),
                requestId,
                rateLimit,
                retryable);
    }

    /**
     * Wraps a transport failure. Connect-phase failures were never sent and are retryable. Anything
     * else is retryable for idempotent requests and ambiguous (not retried) otherwise.
     */
    public static ServiceNowException transportFailure(
            IOException cause, String method, String path, boolean idempotent, String requestId) {
        boolean neverSent =
                cause instanceof HttpConnectTimeoutException
                        || cause instanceof ConnectException
                        || cause instanceof UnknownHostException;
        String where =
                method + " " + path + (requestId != null ? " (request-id " + requestId + ")" : "");
        if (neverSent) {
            return new ServiceNowException(
                    where + " could not connect: " + cause.getMessage(), cause, true, false);
        }
        if (idempotent) {
            return new ServiceNowException(
                    where + " failed after sending: " + cause.getMessage(), cause, true, false);
        }
        return new ServiceNowException(
                where
                        + " failed after sending and may or may not have been applied: "
                        + cause.getMessage(),
                cause,
                false,
                true);
    }
}
