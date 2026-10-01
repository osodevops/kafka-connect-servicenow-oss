package sh.oso.servicenow.sink;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * The result of writing one record: what was attempted, what the instance answered and, on failure,
 * the {@link RecordError}. {@code droppedFields} lists dictionary-unknown fields removed under
 * {@code snow.sink.unknown.field.behavior=report}, which the task reports separately.
 */
public record Outcome(
        SinkRecord record,
        Operation operation,
        String table,
        String sysId,
        Integer status,
        String requestId,
        int retries,
        Map<String, Object> requestBody,
        List<String> droppedFields,
        RecordError error) {

    public Outcome {
        requestBody = requestBody == null ? Map.of() : Map.copyOf(requestBody);
        droppedFields = droppedFields == null ? List.of() : List.copyOf(droppedFields);
    }

    static Outcome success(
            SinkRecord record,
            Operation operation,
            String table,
            String sysId,
            int status,
            String requestId,
            int retries,
            Map<String, Object> requestBody,
            List<String> droppedFields) {
        return new Outcome(
                record,
                operation,
                table,
                sysId,
                status,
                requestId,
                retries,
                requestBody,
                droppedFields,
                null);
    }

    static Outcome failure(
            SinkRecord record,
            Operation operation,
            String table,
            String sysId,
            Map<String, Object> requestBody,
            RecordError error) {
        return new Outcome(
                record,
                operation,
                table,
                sysId,
                error.status(),
                error.requestId(),
                error.retries(),
                requestBody,
                List.of(),
                error);
    }

    public boolean isSuccess() {
        return error == null;
    }

    /** True when the task must re-deliver the batch instead of absorbing the failure. */
    public boolean isRetryable() {
        return error != null && error.isRetryable();
    }

    public Classification classification() {
        return error == null ? null : error.classification();
    }

    /** {@code topic-partition@offset}, for log lines and exception messages. */
    public String coordinates() {
        return record.topic() + "-" + record.kafkaPartition() + "@" + record.kafkaOffset();
    }
}
