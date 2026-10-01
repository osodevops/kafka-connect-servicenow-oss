package sh.oso.servicenow.common;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

/**
 * Maps snow-core failures to Kafka Connect exceptions at the task boundary: retryable failures
 * become {@link RetriableException} (the worker retries the poll or put), everything else a fatal
 * {@link ConnectException}. Connect exceptions pass through unchanged.
 */
public final class ConnectExceptions {

    private ConnectExceptions() {}

    public static RuntimeException toConnect(Throwable t) {
        if (t instanceof ConnectException ce) {
            return ce;
        }
        String message = t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
        if (ErrorClassifier.isRetryable(t)) {
            return new RetriableException(message, t);
        }
        return new ConnectException(message, t);
    }
}
