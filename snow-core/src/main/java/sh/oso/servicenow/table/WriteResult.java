package sh.oso.servicenow.table;

import java.util.Optional;

/** Outcome of a create, patch, put or delete: HTTP status, returned record (none for DELETE). */
public record WriteResult(int status, Optional<Record> record, String requestId) {

    public Optional<String> sysId() {
        return record.map(Record::sysId);
    }
}
