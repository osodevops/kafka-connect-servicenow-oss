package sh.oso.servicenow.testing;

/** The fake's encoded-query evaluator met syntax it does not support. */
public final class InvalidQueryException extends RuntimeException {

    public InvalidQueryException(String message) {
        super(message);
    }
}
