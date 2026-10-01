package sh.oso.servicenow.sink;

import org.apache.kafka.connect.errors.ConnectException;
import sh.oso.servicenow.common.RetryExhaustedException;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.common.ServiceNowException;

/**
 * A failure attributed to one record: routing, identifier, operation or mapping problems raised by
 * the sink itself, and ServiceNow responses classified at the write boundary. {@link
 * #isRetryable()} is true only for {@link Classification#RETRIES_EXHAUSTED}, which the task turns
 * into a {@code RetriableException}; every other classification is permanent and goes to the error
 * paths.
 */
public final class RecordError extends ConnectException {

    private final Classification classification;
    private final Integer status;
    private final String requestId;
    private final String responseExcerpt;
    private final int retries;

    public RecordError(Classification classification, String message) {
        this(classification, message, null, null, null, null, 0);
    }

    public RecordError(Classification classification, String message, Throwable cause) {
        this(classification, message, cause, null, null, null, 0);
    }

    public RecordError(
            Classification classification,
            String message,
            Throwable cause,
            Integer status,
            String requestId,
            String responseExcerpt,
            int retries) {
        super(message, cause);
        this.classification = classification;
        this.status = status;
        this.requestId = requestId;
        this.responseExcerpt = responseExcerpt;
        this.retries = retries;
    }

    /** Classifies a snow-core failure raised while writing {@code op} to {@code table}. */
    public static RecordError from(
            ServiceNowException e, Operation op, String table, String sysId, String user) {
        String where = op + " " + table + (sysId != null ? "/" + sysId : "");
        if (e instanceof RetryExhaustedException rx) {
            Integer status =
                    rx.getCause() instanceof ServiceNowApiException api ? api.status() : null;
            String requestId =
                    rx.getCause() instanceof ServiceNowApiException api ? api.requestId() : null;
            String excerpt =
                    rx.getCause() instanceof ServiceNowApiException api
                            ? api.responseExcerpt()
                            : null;
            return new RecordError(
                    Classification.RETRIES_EXHAUSTED,
                    where + ": " + e.getMessage(),
                    e,
                    status,
                    requestId,
                    excerpt,
                    Math.max(0, rx.attempts() - 1));
        }
        if (e instanceof ServiceNowApiException api) {
            Classification c;
            String message;
            if (api.isForbidden()) {
                c = Classification.PERMISSION;
                message =
                        "HTTP 403 on "
                                + where
                                + ": the integration user "
                                + (user != null ? "'" + user + "' " : "")
                                + "lacks write access to table "
                                + table
                                + " (check the ACLs, domain and roles granted to the user)";
            } else if (api.isNotFound()) {
                c = Classification.NOT_FOUND;
                message = "HTTP 404 on " + where + ": no such row";
            } else if (api.status() == 409) {
                c = Classification.CONFLICT;
                message = "HTTP 409 on " + where + ": " + api.getMessage();
            } else {
                c = Classification.RECORD_ERROR;
                message = where + ": " + api.getMessage();
            }
            return new RecordError(
                    c, message, e, api.status(), api.requestId(), api.responseExcerpt(), 0);
        }
        if (e.isAmbiguous()) {
            return new RecordError(
                    Classification.AMBIGUOUS,
                    where + " may or may not have been applied: " + e.getMessage(),
                    e);
        }
        if (e.isRetryable()) {
            // A retryable failure that escaped the retry policy; treat it as exhausted so the
            // framework re-delivers rather than failing the record permanently.
            return new RecordError(
                    Classification.RETRIES_EXHAUSTED, where + ": " + e.getMessage(), e);
        }
        return new RecordError(Classification.RECORD_ERROR, where + ": " + e.getMessage(), e);
    }

    public Classification classification() {
        return classification;
    }

    /** HTTP status of the failing response, or null when no response was involved. */
    public Integer status() {
        return status;
    }

    public String requestId() {
        return requestId;
    }

    /** Redacted, truncated response body; null when no response was involved. */
    public String responseExcerpt() {
        return responseExcerpt;
    }

    /** Retries the sink made beyond the first attempt, where known. */
    public int retries() {
        return retries;
    }

    public boolean isRetryable() {
        return classification == Classification.RETRIES_EXHAUSTED;
    }
}
