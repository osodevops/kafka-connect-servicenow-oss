package sh.oso.servicenow.sink;

import java.util.Locale;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Picks the operation for a record, in priority order: the operation header when present, the fixed
 * operation under {@code snow.sink.operation.mode=fixed}, then key/value inference (null value is
 * DELETE, no sys_id is CREATE, otherwise the configured update method).
 *
 * <p>Header values are case-insensitive: {@code CREATE} or {@code POST}, {@code PATCH}, {@code
 * PUT}, {@code UPDATE} (the configured update method), {@code DELETE} and {@code UPSERT} (the
 * update method when a sys_id is present, otherwise CREATE; this is what a source topic would carry
 * if it were replayed with its header renamed).
 */
final class OperationResolver {

    private final SinkConfig.OperationMode mode;
    private final Operation fixed;
    private final String headerName;
    private final boolean headerRequired;
    private final Operation updateMethod;

    OperationResolver(SinkConfig config) {
        this.mode = config.operationMode();
        this.fixed = config.fixedOperation();
        this.headerName = config.operationHeader();
        this.headerRequired = config.operationHeaderRequired();
        this.updateMethod = config.updateMethod();
    }

    Operation resolve(SinkRecord record, String sysId) {
        Operation op = fromHeader(record, sysId);
        if (op == null && mode == SinkConfig.OperationMode.HEADER && headerRequired) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "Record has no '"
                            + headerName
                            + "' header and "
                            + SinkConfig.OPERATION_HEADER_REQUIRED
                            + "=true");
        }
        if (op == null && mode == SinkConfig.OperationMode.FIXED) {
            op = fixed;
        }
        if (op == null) {
            op = infer(record, sysId);
        }
        check(op, record, sysId);
        return op;
    }

    private Operation fromHeader(SinkRecord record, String sysId) {
        Header header = record.headers().lastWithName(headerName);
        if (header == null || header.value() == null) {
            return null;
        }
        String value = TableRouter.headerText(header).trim().toUpperCase(Locale.ROOT);
        return switch (value) {
            case "CREATE", "POST" -> Operation.CREATE;
            case "PATCH" -> Operation.PATCH;
            case "PUT" -> Operation.PUT;
            case "UPDATE" -> updateMethod;
            case "DELETE" -> Operation.DELETE;
            case "UPSERT" -> sysId == null ? Operation.CREATE : updateMethod;
            default ->
                    throw new RecordError(
                            Classification.RECORD_ERROR,
                            "Header '"
                                    + headerName
                                    + "' value '"
                                    + TableRouter.headerText(header)
                                    + "' is not one of CREATE, POST, PATCH, PUT, UPDATE, DELETE,"
                                    + " UPSERT");
        };
    }

    private Operation infer(SinkRecord record, String sysId) {
        if (record.value() == null) {
            return Operation.DELETE;
        }
        return sysId == null ? Operation.CREATE : updateMethod;
    }

    private static void check(Operation op, SinkRecord record, String sysId) {
        if (op.needsSysId() && sysId == null) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    op
                            + " needs a sys_id but the record carries none (key, "
                            + SinkConfig.SYS_ID_VALUE_FIELD
                            + " or the "
                            + IdExtractor.SYS_ID_HEADER
                            + " header)");
        }
        if (op != Operation.DELETE && record.value() == null) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    op + " needs a record value but the record is a tombstone");
        }
    }
}
