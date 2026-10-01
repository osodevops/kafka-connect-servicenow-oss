package sh.oso.servicenow.common;

/**
 * Base exception for every failure raised by snow-core.
 *
 * <p>{@link #isRetryable()} says whether the same call may succeed if repeated; {@link
 * #isAmbiguous()} says whether a non-idempotent request may already have been applied by the
 * instance (for example a POST whose response never arrived). Ambiguous failures are never retried
 * by {@link RetryPolicy}; the caller decides.
 */
public class ServiceNowException extends RuntimeException {

    private final boolean retryable;
    private final boolean ambiguous;

    public ServiceNowException(String message) {
        this(message, null, false, false);
    }

    public ServiceNowException(String message, Throwable cause) {
        this(message, cause, false, false);
    }

    public ServiceNowException(String message, Throwable cause, boolean retryable) {
        this(message, cause, retryable, false);
    }

    public ServiceNowException(
            String message, Throwable cause, boolean retryable, boolean ambiguous) {
        super(message, cause);
        this.retryable = retryable;
        this.ambiguous = ambiguous;
    }

    /** True when the failure is transient and the operation may succeed on retry. */
    public boolean isRetryable() {
        return retryable;
    }

    /** True when a non-idempotent request may have been applied although no response arrived. */
    public boolean isAmbiguous() {
        return ambiguous;
    }
}
