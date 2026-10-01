package sh.oso.servicenow.common;

import java.util.Optional;
import sh.oso.servicenow.limits.RateLimitInfo;

/** An HTTP error response from a ServiceNow endpoint, already classified and redacted. */
public class ServiceNowApiException extends ServiceNowException {

    private final int status;
    private final String errorMessage;
    private final String errorDetail;
    private final String requestId;
    private final String responseExcerpt;
    private final transient RateLimitInfo rateLimit;

    public ServiceNowApiException(
            int status,
            String errorMessage,
            String errorDetail,
            String responseExcerpt,
            String requestId,
            RateLimitInfo rateLimit,
            boolean retryable) {
        super(describe(status, errorMessage, errorDetail, requestId), null, retryable, false);
        this.status = status;
        this.errorMessage = errorMessage;
        this.errorDetail = errorDetail;
        this.responseExcerpt = responseExcerpt == null ? "" : Redaction.redactBody(responseExcerpt);
        this.requestId = requestId;
        this.rateLimit = rateLimit;
    }

    private static String describe(int status, String message, String detail, String requestId) {
        StringBuilder sb = new StringBuilder("ServiceNow API error: HTTP ").append(status);
        if (message != null && !message.isBlank()) {
            sb.append(" [").append(Redaction.redactBody(message)).append(']');
        }
        if (detail != null && !detail.isBlank() && !"null".equals(detail)) {
            sb.append(' ').append(Redaction.redactBody(detail));
        }
        if (requestId != null) {
            sb.append(" (request-id ").append(requestId).append(')');
        }
        return sb.toString();
    }

    public int status() {
        return status;
    }

    /** ServiceNow {@code error.message}; may be null for non-JSON bodies. */
    public String errorMessage() {
        return errorMessage;
    }

    /** ServiceNow {@code error.detail}; may be null. */
    public String errorDetail() {
        return errorDetail;
    }

    /** The {@code X-Request-Id} sent with the failing request; may be null. */
    public String requestId() {
        return requestId;
    }

    /** Redacted, truncated response body for reporters and diagnostics. */
    public String responseExcerpt() {
        return responseExcerpt;
    }

    public Optional<RateLimitInfo> rateLimit() {
        return Optional.ofNullable(rateLimit);
    }

    public boolean isUnauthorized() {
        return status == 401;
    }

    public boolean isForbidden() {
        return status == 403;
    }

    public boolean isNotFound() {
        return status == 404;
    }

    public boolean isRateLimited() {
        return status == 429;
    }
}
