package sh.oso.servicenow.sink;

/** Why a record failed, as reported on the error topic and in log lines. */
public enum Classification {
    /** Invalid payload, mapping or routing failure, 400/422 and any other permanent status. */
    RECORD_ERROR,
    /** 403: the integration user lacks access to the table or row. */
    PERMISSION,
    /** 404 on an update or delete, under {@code snow.sink.not.found.behavior=fail}. */
    NOT_FOUND,
    /** 409 that the instance did not mark as transient. */
    CONFLICT,
    /** A write was sent but its outcome is unknown and the policy did not resolve it. */
    AMBIGUOUS,
    /** A transient failure outlived the retry budget; the task re-delivers the batch. */
    RETRIES_EXHAUSTED
}
