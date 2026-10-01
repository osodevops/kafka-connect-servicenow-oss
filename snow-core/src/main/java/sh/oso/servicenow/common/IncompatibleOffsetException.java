package sh.oso.servicenow.common;

/**
 * A stored Connect offset cannot be used by this connector version or configuration (unknown
 * version, missing fields, or a query fingerprint that no longer matches). Never retryable.
 */
public final class IncompatibleOffsetException extends ServiceNowException {

    public IncompatibleOffsetException(String message) {
        super(message, null, false, false);
    }

    public IncompatibleOffsetException(String message, Throwable cause) {
        super(message, cause, false, false);
    }
}
