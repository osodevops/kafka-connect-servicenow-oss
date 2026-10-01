package sh.oso.servicenow.sink;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import sh.oso.servicenow.table.PathSegments;

/**
 * Chooses the target table for a record: a fixed table, a per-topic map, or a trusted record header
 * validated against an allowlist and the table-name pattern (table names become URL path segments,
 * so a record can never choose an unlisted or malformed one).
 */
final class TableRouter {

    private final SinkConfig.RoutingMode mode;
    private final String fixedTable;
    private final Map<String, String> topicTables;
    private final String headerName;
    private final Set<String> allowlist;

    TableRouter(SinkConfig config) {
        this.mode = config.routingMode();
        this.fixedTable = config.fixedTable();
        this.topicTables = config.topicTables();
        this.headerName = config.tableHeader();
        this.allowlist = Set.copyOf(config.tableAllowlist());
    }

    String route(SinkRecord record) {
        return switch (mode) {
            case FIXED -> fixedTable;
            case TOPIC_MAP -> {
                String table = topicTables.get(record.topic());
                if (table == null) {
                    throw new RecordError(
                            Classification.RECORD_ERROR,
                            "No "
                                    + SinkConfig.topicTableKey(record.topic())
                                    + " mapping for topic "
                                    + record.topic());
                }
                yield table;
            }
            case HEADER -> fromHeader(record);
        };
    }

    private String fromHeader(SinkRecord record) {
        Header header = record.headers().lastWithName(headerName);
        if (header == null || header.value() == null) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "Record has no '" + headerName + "' header to select the target table");
        }
        String table = headerText(header).trim();
        if (!PathSegments.isTable(table)) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "Header '"
                            + headerName
                            + "' value '"
                            + table
                            + "' is not a valid table name (must match "
                            + PathSegments.TABLE_NAME.pattern()
                            + ")");
        }
        if (!allowlist.contains(table)) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "Header '"
                            + headerName
                            + "' selects table '"
                            + table
                            + "', which is not in "
                            + SinkConfig.TABLE_ALLOWLIST);
        }
        return table;
    }

    static String headerText(Header header) {
        Object v = header.value();
        if (v instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(v);
    }
}
