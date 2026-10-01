package sh.oso.servicenow.common;

import java.time.Duration;

/**
 * {@link RetryPolicy} gave up: the attempt or elapsed-time cap was reached while the failure was
 * still retryable. Remains retryable so the task boundary surfaces a {@code RetriableException};
 * the last failure is the cause (a {@link ServiceNowApiException} keeps its status there).
 */
public final class RetryExhaustedException extends ServiceNowException {

    private final int attempts;
    private final Duration elapsed;

    public RetryExhaustedException(
            String opName, int attempts, Duration elapsed, Throwable lastFailure) {
        this(opName, attempts, elapsed, "attempt limit reached", lastFailure);
    }

    public RetryExhaustedException(
            String opName, int attempts, Duration elapsed, String reason, Throwable lastFailure) {
        super(
                "Retries exhausted for "
                        + opName
                        + " after "
                        + attempts
                        + " attempt(s) and "
                        + elapsed.toMillis()
                        + " ms ("
                        + reason
                        + "): "
                        + (lastFailure != null ? lastFailure.getMessage() : "unknown failure"),
                lastFailure,
                true,
                false);
        this.attempts = attempts;
        this.elapsed = elapsed;
    }

    public int attempts() {
        return attempts;
    }

    public Duration elapsed() {
        return elapsed;
    }
}
