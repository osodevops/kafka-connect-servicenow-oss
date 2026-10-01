package sh.oso.servicenow.sink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import sh.oso.servicenow.config.CoreConfig;
import sh.oso.servicenow.config.CoreConfigDefs;
import sh.oso.servicenow.table.PathSegments;

/**
 * Sink connector configuration: routing, operation resolution, identifier extraction, field
 * mapping, ambiguous-write and not-found policies, the reporters and the shared {@code snow.*} keys
 * from {@link CoreConfigDefs}. Dynamic {@code snow.sink.topic.<topic>.table} keys are read from the
 * raw originals.
 */
public class SinkConfig extends AbstractConfig {

    public static final String GROUP_ROUTING = "Routing";
    public static final String GROUP_OPERATION = "Operation";
    public static final String GROUP_MAPPING = "Mapping";
    public static final String GROUP_WRITES = "Writes";
    public static final String GROUP_REPORTING = "Reporting";

    public static final String ROUTING_MODE = "snow.sink.routing.mode";
    public static final String TABLE = "snow.sink.table";
    public static final String TOPIC_TABLE_PREFIX = "snow.sink.topic.";
    public static final String TOPIC_TABLE_SUFFIX = ".table";
    public static final String TABLE_HEADER = "snow.sink.table.header";
    public static final String TABLE_ALLOWLIST = "snow.sink.table.allowlist";

    public static final String OPERATION_MODE = "snow.sink.operation.mode";
    public static final String OPERATION_FIXED = "snow.sink.operation.fixed";
    public static final String OPERATION_HEADER = "snow.sink.operation.header";
    public static final String OPERATION_HEADER_REQUIRED = "snow.sink.operation.header.required";
    public static final String UPDATE_METHOD = "snow.sink.update.method";
    public static final String SYS_ID_KEY_FIELD = "snow.sink.sys.id.key.field";
    public static final String SYS_ID_VALUE_FIELD = "snow.sink.sys.id.value.field";
    public static final String SYS_ID_PRECEDENCE = "snow.sink.sys.id.precedence";

    public static final String MAX_IN_FLIGHT = "snow.sink.max.in.flight";
    public static final String NULL_BEHAVIOR = "snow.sink.null.behavior";
    public static final String UNKNOWN_FIELD_BEHAVIOR = "snow.sink.unknown.field.behavior";
    public static final String NESTED_BEHAVIOR = "snow.sink.nested.behavior";
    public static final String NESTED_FLATTEN_DELIMITER = "snow.sink.nested.flatten.delimiter";
    public static final String FIELD_RENAME = "snow.sink.field.rename";
    public static final String FIELD_ALLOWLIST = "snow.sink.field.allowlist";
    public static final String FIELD_DENYLIST = "snow.sink.field.denylist";

    public static final String CREATE_AMBIGUOUS_BEHAVIOR = "snow.sink.create.ambiguous.behavior";
    public static final String CORRELATION_FIELD = "snow.sink.correlation.field";
    public static final String NOT_FOUND_BEHAVIOR = "snow.sink.not.found.behavior";

    public static final String REPORTER_BOOTSTRAP_SERVERS = "snow.sink.reporter.bootstrap.servers";
    public static final String REPORTER_SUCCESS_TOPIC = "snow.sink.reporter.success.topic";
    public static final String REPORTER_ERROR_TOPIC = "snow.sink.reporter.error.topic";
    public static final String REPORTER_PRODUCER_PREFIX = "snow.sink.reporter.producer.";
    public static final String REPORTER_INCLUDE_REQUEST_BODY =
            "snow.sink.reporter.include.request.body";
    public static final String BEHAVIOR_ON_API_ERRORS = "behavior.on.api.errors";

    public static final String DEFAULT_TABLE_HEADER = "snow.table";
    public static final String DEFAULT_OPERATION_HEADER = "snow.operation";
    public static final String DEFAULT_SYS_ID_FIELD = "sys_id";
    public static final int DEFAULT_MAX_IN_FLIGHT = 8;
    public static final String DEFAULT_FLATTEN_DELIMITER = "_";

    public enum RoutingMode {
        FIXED,
        TOPIC_MAP,
        HEADER
    }

    public enum OperationMode {
        KEY_VALUE,
        HEADER,
        FIXED
    }

    public enum SysIdPrecedence {
        REJECT,
        KEY,
        VALUE
    }

    public enum NullBehavior {
        OMIT,
        CLEAR,
        REJECT
    }

    public enum UnknownFieldBehavior {
        FAIL,
        DROP,
        REPORT,
        PASSTHROUGH
    }

    public enum NestedBehavior {
        REJECT,
        FLATTEN,
        STRINGIFY
    }

    public enum CreateAmbiguousBehavior {
        RETRY,
        FAIL_AMBIGUOUS,
        CORRELATION_LOOKUP
    }

    public enum NotFoundBehavior {
        FAIL,
        IGNORE,
        CREATE
    }

    public enum ErrorBehavior {
        FAIL,
        LOG,
        IGNORE
    }

    private final Map<String, String> topicTables;
    private final Map<String, String> renames;
    private final CoreConfig core;

    public SinkConfig(Map<String, String> originals) {
        super(configDef(), originals);
        this.core = new CoreConfig(this);
        this.topicTables = parseTopicTables(originalsStrings());
        this.renames = parseRenames(getList(FIELD_RENAME));
        validate();
    }

    /** The static keys; the per-topic key is documented by {@link #topicDef(String)}. */
    public static ConfigDef configDef() {
        ConfigDef def = new ConfigDef();
        int r = 0;
        def.define(
                ROUTING_MODE,
                Type.STRING,
                "fixed",
                new OneOf("fixed", "topic_map", "header"),
                Importance.HIGH,
                "How the target table is chosen: `fixed` (snow.sink.table for every record),"
                        + " `topic_map` (snow.sink.topic.<topic>.table per input topic) or `header`"
                        + " (the record header named by snow.sink.table.header, validated against"
                        + " snow.sink.table.allowlist).",
                GROUP_ROUTING,
                ++r,
                Width.SHORT,
                "Routing mode");
        def.define(
                TABLE,
                Type.STRING,
                null,
                new TableName(),
                Importance.HIGH,
                "Target table for `fixed` routing, for example incident.",
                GROUP_ROUTING,
                ++r,
                Width.MEDIUM,
                "Table");
        def.define(
                TABLE_HEADER,
                Type.STRING,
                DEFAULT_TABLE_HEADER,
                new ConfigDef.NonEmptyString(),
                Importance.LOW,
                "Record header carrying the target table name under `header` routing.",
                GROUP_ROUTING,
                ++r,
                Width.MEDIUM,
                "Table header");
        def.define(
                TABLE_ALLOWLIST,
                Type.LIST,
                Collections.emptyList(),
                new TableNameList(),
                Importance.MEDIUM,
                "Tables a record header may select under `header` routing. Required in that mode:"
                        + " table names become URL path segments, so records never choose a table"
                        + " that is not listed here.",
                GROUP_ROUTING,
                ++r,
                Width.LONG,
                "Table allowlist");

        int o = 0;
        def.define(
                OPERATION_MODE,
                Type.STRING,
                "key_value",
                new OneOf("key_value", "header", "fixed"),
                Importance.HIGH,
                "How the operation is chosen when no operation header is present: `key_value`"
                        + " (null value is DELETE, no sys_id is CREATE, otherwise"
                        + " snow.sink.update.method), `header` (the header is expected; see"
                        + " snow.sink.operation.header.required) or `fixed`"
                        + " (snow.sink.operation.fixed). A present operation header always wins.",
                GROUP_OPERATION,
                ++o,
                Width.SHORT,
                "Operation mode");
        def.define(
                OPERATION_FIXED,
                Type.STRING,
                null,
                new OneOf(true, "create", "patch", "put", "delete"),
                Importance.MEDIUM,
                "Operation for every record under snow.sink.operation.mode=fixed.",
                GROUP_OPERATION,
                ++o,
                Width.SHORT,
                "Fixed operation");
        def.define(
                OPERATION_HEADER,
                Type.STRING,
                DEFAULT_OPERATION_HEADER,
                new ConfigDef.NonEmptyString(),
                Importance.LOW,
                "Record header carrying the operation: CREATE or POST, PATCH, PUT, UPDATE (the"
                        + " configured update method), DELETE, or UPSERT (update when a sys_id is"
                        + " present, otherwise create). Case-insensitive. The source connector"
                        + " writes snow.source.operation, a different header, so replaying a source"
                        + " topic never issues commands by accident.",
                GROUP_OPERATION,
                ++o,
                Width.MEDIUM,
                "Operation header");
        def.define(
                OPERATION_HEADER_REQUIRED,
                Type.BOOLEAN,
                false,
                Importance.LOW,
                "Under snow.sink.operation.mode=header, fail the record when the header is absent"
                        + " instead of falling back to key/value inference.",
                GROUP_OPERATION,
                ++o,
                Width.SHORT,
                "Operation header required");
        def.define(
                UPDATE_METHOD,
                Type.STRING,
                "PATCH",
                new OneOf("PATCH", "PUT"),
                Importance.MEDIUM,
                "HTTP method for updates: `PATCH` sends only the mapped fields and leaves the rest"
                        + " untouched; `PUT` asks the instance to replace the record. PATCH is the"
                        + " default because Kafka payloads are often partial.",
                GROUP_OPERATION,
                ++o,
                Width.SHORT,
                "Update method");
        def.define(
                SYS_ID_KEY_FIELD,
                Type.STRING,
                DEFAULT_SYS_ID_FIELD,
                new ConfigDef.NonEmptyString(),
                Importance.LOW,
                "Field of a Struct or Map record key holding the sys_id. A String key is the"
                        + " sys_id itself.",
                GROUP_OPERATION,
                ++o,
                Width.MEDIUM,
                "sys_id key field");
        def.define(
                SYS_ID_VALUE_FIELD,
                Type.STRING,
                DEFAULT_SYS_ID_FIELD,
                new ConfigDef.NonEmptyString(),
                Importance.LOW,
                "Field of the record value holding the sys_id.",
                GROUP_OPERATION,
                ++o,
                Width.MEDIUM,
                "sys_id value field");
        def.define(
                SYS_ID_PRECEDENCE,
                Type.STRING,
                "reject",
                new OneOf("reject", "key", "value"),
                Importance.LOW,
                "When the key, the value and the snow.sys_id header carry different sys_ids:"
                        + " `reject` the record, or let the `key` or the `value` win.",
                GROUP_OPERATION,
                ++o,
                Width.SHORT,
                "sys_id precedence");

        int m = 0;
        def.define(
                NULL_BEHAVIOR,
                Type.STRING,
                "omit",
                new OneOf("omit", "clear", "reject"),
                Importance.MEDIUM,
                "Null field values: `omit` them from the body (a PATCH leaves the field"
                        + " untouched), `clear` the field by sending an empty string, or `reject`"
                        + " the record.",
                GROUP_MAPPING,
                ++m,
                Width.SHORT,
                "Null behavior");
        def.define(
                UNKNOWN_FIELD_BEHAVIOR,
                Type.STRING,
                "fail",
                new OneOf("fail", "drop", "report", "passthrough"),
                Importance.MEDIUM,
                "Fields missing from the target table's dictionary (sys_dictionary, cached):"
                        + " `fail` the record, `drop` them, `report` them to the error reporter"
                        + " while the rest of the record is written, or `passthrough` send them as"
                        + " is. Only `passthrough` avoids the dictionary lookup, which needs read"
                        + " access to sys_db_object and sys_dictionary.",
                GROUP_MAPPING,
                ++m,
                Width.SHORT,
                "Unknown field behavior");
        def.define(
                NESTED_BEHAVIOR,
                Type.STRING,
                "reject",
                new OneOf("reject", "flatten", "stringify"),
                Importance.MEDIUM,
                "Struct, Map or array field values: `reject` the record naming the field path,"
                        + " `flatten` nested keys with snow.sink.nested.flatten.delimiter, or"
                        + " `stringify` the value as a JSON string. Never silently stringified:"
                        + " ServiceNow would store an unrecognised object as a Java map string.",
                GROUP_MAPPING,
                ++m,
                Width.SHORT,
                "Nested behavior");
        def.define(
                NESTED_FLATTEN_DELIMITER,
                Type.STRING,
                DEFAULT_FLATTEN_DELIMITER,
                new ConfigDef.NonEmptyString(),
                Importance.LOW,
                "Joins nested keys under snow.sink.nested.behavior=flatten, so location.city"
                        + " becomes location_city.",
                GROUP_MAPPING,
                ++m,
                Width.SHORT,
                "Flatten delimiter");
        def.define(
                FIELD_RENAME,
                Type.LIST,
                Collections.emptyList(),
                new RenameList(),
                Importance.LOW,
                "Top-level field renames as from:to pairs, applied before the allow and deny"
                        + " lists.",
                GROUP_MAPPING,
                ++m,
                Width.LONG,
                "Field renames");
        def.define(
                FIELD_ALLOWLIST,
                Type.LIST,
                Collections.emptyList(),
                Importance.LOW,
                "When set, only these top-level fields (after renames) are written. Naming a"
                        + " reserved field such as sys_id here keeps it in the body.",
                GROUP_MAPPING,
                ++m,
                Width.LONG,
                "Field allowlist");
        def.define(
                FIELD_DENYLIST,
                Type.LIST,
                Collections.emptyList(),
                Importance.LOW,
                "Top-level fields (after renames) never written. Cannot be combined with the"
                        + " allowlist.",
                GROUP_MAPPING,
                ++m,
                Width.LONG,
                "Field denylist");

        int w = 0;
        def.define(
                MAX_IN_FLIGHT,
                Type.INT,
                DEFAULT_MAX_IN_FLIGHT,
                Range.atLeast(1),
                Importance.MEDIUM,
                "Kafka partitions written concurrently by one task; each partition is written in"
                        + " order, one request at a time. Keep it at or below"
                        + " snow.http.max.concurrent.requests.",
                GROUP_WRITES,
                ++w,
                Width.SHORT,
                "Max in flight");
        def.define(
                CREATE_AMBIGUOUS_BEHAVIOR,
                Type.STRING,
                "fail_ambiguous",
                new OneOf("retry", "fail_ambiguous", "correlation_lookup"),
                Importance.MEDIUM,
                "A create whose response never arrived may or may not exist: `retry` re-sends"
                        + " the POST (a duplicate row is possible), `fail_ambiguous` fails the"
                        + " record with classification AMBIGUOUS, `correlation_lookup` queries"
                        + " snow.sink.correlation.field before every create and after an"
                        + " ambiguous one, then creates, patches the single match, or fails on"
                        + " two or more matches.",
                GROUP_WRITES,
                ++w,
                Width.SHORT,
                "Create ambiguous behavior");
        def.define(
                CORRELATION_FIELD,
                Type.STRING,
                null,
                Importance.MEDIUM,
                "Field unique per business object, present in every create payload; required"
                        + " by snow.sink.create.ambiguous.behavior=correlation_lookup.",
                GROUP_WRITES,
                ++w,
                Width.MEDIUM,
                "Correlation field");
        def.define(
                NOT_FOUND_BEHAVIOR,
                Type.STRING,
                "fail",
                new OneOf("fail", "ignore", "create"),
                Importance.MEDIUM,
                "A 404 on PATCH, PUT or DELETE: `fail` the record (classification NOT_FOUND),"
                        + " `ignore` it, or `create` the row with the same sys_id (updates only;"
                        + " a missing row on DELETE is ignored).",
                GROUP_WRITES,
                ++w,
                Width.SHORT,
                "Not found behavior");
        def.define(
                BEHAVIOR_ON_API_ERRORS,
                Type.STRING,
                "fail",
                new OneOf("fail", "log", "ignore"),
                Importance.MEDIUM,
                "After a permanent record failure has been offered to the dead letter queue and"
                        + " the error reporter: `fail` the task, `log` the failure and continue, or"
                        + " `ignore` it and continue. Retryable failures never take this path;"
                        + " they re-deliver the batch.",
                GROUP_WRITES,
                ++w,
                Width.SHORT,
                "Behavior on API errors");

        int p = 0;
        def.define(
                REPORTER_BOOTSTRAP_SERVERS,
                Type.STRING,
                null,
                Importance.MEDIUM,
                "Kafka bootstrap servers for the reporter producer. Required when a reporter"
                        + " topic is set; a task cannot see the worker's own bootstrap servers.",
                GROUP_REPORTING,
                ++p,
                Width.LONG,
                "Reporter bootstrap servers");
        def.define(
                REPORTER_SUCCESS_TOPIC,
                Type.STRING,
                null,
                Importance.LOW,
                "Topic receiving a JSON report for every successful write; empty disables it.",
                GROUP_REPORTING,
                ++p,
                Width.MEDIUM,
                "Success topic");
        def.define(
                REPORTER_ERROR_TOPIC,
                Type.STRING,
                null,
                Importance.LOW,
                "Topic receiving a JSON report for every failed write; empty disables it.",
                GROUP_REPORTING,
                ++p,
                Width.MEDIUM,
                "Error topic");
        def.define(
                REPORTER_INCLUDE_REQUEST_BODY,
                Type.BOOLEAN,
                false,
                Importance.LOW,
                "Include the request body in reports. Off by default because bodies can carry"
                        + " personal data.",
                GROUP_REPORTING,
                ++p,
                Width.SHORT,
                "Include request body");
        return CoreConfigDefs.addCore(def);
    }

    /** The dynamic per-topic key, for the documentation generator. */
    public static ConfigDef topicDef(String topic) {
        return new ConfigDef()
                .define(
                        topicTableKey(topic),
                        Type.STRING,
                        null,
                        new TableName(),
                        Importance.HIGH,
                        "Target table for records from this topic under"
                                + " snow.sink.routing.mode=topic_map.",
                        GROUP_ROUTING,
                        1,
                        Width.MEDIUM,
                        "Table for topic");
    }

    public static String topicTableKey(String topic) {
        return TOPIC_TABLE_PREFIX + topic + TOPIC_TABLE_SUFFIX;
    }

    private static Map<String, String> parseTopicTables(Map<String, String> originals) {
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : originals.entrySet()) {
            String key = e.getKey();
            if (key.startsWith(TOPIC_TABLE_PREFIX)
                    && key.endsWith(TOPIC_TABLE_SUFFIX)
                    && key.length() > TOPIC_TABLE_PREFIX.length() + TOPIC_TABLE_SUFFIX.length()) {
                String topic =
                        key.substring(
                                TOPIC_TABLE_PREFIX.length(),
                                key.length() - TOPIC_TABLE_SUFFIX.length());
                String table = e.getValue() == null ? "" : e.getValue().trim();
                if (!PathSegments.isTable(table)) {
                    throw new ConfigException(
                            key,
                            e.getValue(),
                            "must be a table name matching " + PathSegments.TABLE_NAME.pattern());
                }
                out.put(topic, table);
            }
        }
        return Collections.unmodifiableMap(out);
    }

    private static Map<String, String> parseRenames(List<String> pairs) {
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        for (String pair : pairs) {
            int colon = pair.indexOf(':');
            out.put(pair.substring(0, colon).trim(), pair.substring(colon + 1).trim());
        }
        return Collections.unmodifiableMap(out);
    }

    private void validate() {
        switch (routingMode()) {
            case FIXED -> {
                if (fixedTable() == null) {
                    throw new ConfigException(
                            TABLE, null, "is required when " + ROUTING_MODE + "=fixed");
                }
            }
            case TOPIC_MAP -> {
                if (topicTables.isEmpty()) {
                    throw new ConfigException(
                            ROUTING_MODE,
                            "topic_map",
                            "requires at least one "
                                    + TOPIC_TABLE_PREFIX
                                    + "<topic>"
                                    + TOPIC_TABLE_SUFFIX
                                    + " mapping");
                }
            }
            case HEADER -> {
                if (tableAllowlist().isEmpty()) {
                    throw new ConfigException(
                            TABLE_ALLOWLIST,
                            "",
                            "must list the tables a header may select when "
                                    + ROUTING_MODE
                                    + "=header");
                }
            }
        }
        if (operationMode() == OperationMode.FIXED && fixedOperation() == null) {
            throw new ConfigException(
                    OPERATION_FIXED, null, "is required when " + OPERATION_MODE + "=fixed");
        }
        if (createAmbiguousBehavior() == CreateAmbiguousBehavior.CORRELATION_LOOKUP
                && correlationField() == null) {
            throw new ConfigException(
                    CORRELATION_FIELD,
                    null,
                    "is required when " + CREATE_AMBIGUOUS_BEHAVIOR + "=correlation_lookup");
        }
        if ((successTopic() != null || errorTopic() != null)
                && reporterBootstrapServers() == null) {
            throw new ConfigException(
                    REPORTER_BOOTSTRAP_SERVERS,
                    null,
                    "is required when "
                            + REPORTER_SUCCESS_TOPIC
                            + " or "
                            + REPORTER_ERROR_TOPIC
                            + " is set");
        }
        if (!fieldAllowlist().isEmpty() && !fieldDenylist().isEmpty()) {
            throw new ConfigException(
                    FIELD_DENYLIST,
                    getList(FIELD_DENYLIST),
                    "cannot be combined with " + FIELD_ALLOWLIST);
        }
        if (maxInFlight() > core.httpConfig().maxConcurrentRequests()) {
            throw new ConfigException(
                    MAX_IN_FLIGHT,
                    maxInFlight(),
                    "must not exceed "
                            + CoreConfigDefs.HTTP_MAX_CONCURRENT_REQUESTS
                            + " ("
                            + core.httpConfig().maxConcurrentRequests()
                            + "), the per-instance request bound");
        }
    }

    public CoreConfig coreConfig() {
        return core;
    }

    public RoutingMode routingMode() {
        return RoutingMode.valueOf(upper(getString(ROUTING_MODE)));
    }

    /** {@code snow.sink.table}, or null when blank. */
    public String fixedTable() {
        return blankToNull(getString(TABLE));
    }

    public String tableHeader() {
        return getString(TABLE_HEADER);
    }

    public List<String> tableAllowlist() {
        return trimmed(getList(TABLE_ALLOWLIST));
    }

    /** Topic to table, from the {@code snow.sink.topic.<topic>.table} keys. */
    public Map<String, String> topicTables() {
        return topicTables;
    }

    public OperationMode operationMode() {
        return OperationMode.valueOf(upper(getString(OPERATION_MODE)));
    }

    /** {@code snow.sink.operation.fixed}, or null when unset. */
    public Operation fixedOperation() {
        String v = blankToNull(getString(OPERATION_FIXED));
        return v == null ? null : Operation.fromConfig(v);
    }

    public String operationHeader() {
        return getString(OPERATION_HEADER);
    }

    public boolean operationHeaderRequired() {
        return getBoolean(OPERATION_HEADER_REQUIRED);
    }

    public Operation updateMethod() {
        return Operation.fromConfig(getString(UPDATE_METHOD));
    }

    public String sysIdKeyField() {
        return getString(SYS_ID_KEY_FIELD);
    }

    public String sysIdValueField() {
        return getString(SYS_ID_VALUE_FIELD);
    }

    public SysIdPrecedence sysIdPrecedence() {
        return SysIdPrecedence.valueOf(upper(getString(SYS_ID_PRECEDENCE)));
    }

    public int maxInFlight() {
        return getInt(MAX_IN_FLIGHT);
    }

    public NullBehavior nullBehavior() {
        return NullBehavior.valueOf(upper(getString(NULL_BEHAVIOR)));
    }

    public UnknownFieldBehavior unknownFieldBehavior() {
        return UnknownFieldBehavior.valueOf(upper(getString(UNKNOWN_FIELD_BEHAVIOR)));
    }

    public NestedBehavior nestedBehavior() {
        return NestedBehavior.valueOf(upper(getString(NESTED_BEHAVIOR)));
    }

    public String flattenDelimiter() {
        return getString(NESTED_FLATTEN_DELIMITER);
    }

    public Map<String, String> fieldRenames() {
        return renames;
    }

    public Set<String> fieldAllowlist() {
        return Set.copyOf(trimmed(getList(FIELD_ALLOWLIST)));
    }

    public Set<String> fieldDenylist() {
        return Set.copyOf(trimmed(getList(FIELD_DENYLIST)));
    }

    public CreateAmbiguousBehavior createAmbiguousBehavior() {
        return CreateAmbiguousBehavior.valueOf(upper(getString(CREATE_AMBIGUOUS_BEHAVIOR)));
    }

    public String correlationField() {
        return blankToNull(getString(CORRELATION_FIELD));
    }

    public NotFoundBehavior notFoundBehavior() {
        return NotFoundBehavior.valueOf(upper(getString(NOT_FOUND_BEHAVIOR)));
    }

    public ErrorBehavior errorBehavior() {
        return ErrorBehavior.valueOf(upper(getString(BEHAVIOR_ON_API_ERRORS)));
    }

    public String reporterBootstrapServers() {
        return blankToNull(getString(REPORTER_BOOTSTRAP_SERVERS));
    }

    public String successTopic() {
        return blankToNull(getString(REPORTER_SUCCESS_TOPIC));
    }

    public String errorTopic() {
        return blankToNull(getString(REPORTER_ERROR_TOPIC));
    }

    /** {@code snow.sink.reporter.producer.*} with the prefix stripped. */
    public Map<String, Object> reporterProducerProps() {
        return originalsWithPrefix(REPORTER_PRODUCER_PREFIX);
    }

    public boolean includeRequestBody() {
        return getBoolean(REPORTER_INCLUDE_REQUEST_BODY);
    }

    /** {@code snow.auth.username}, for permission diagnostics; may be null. */
    public String username() {
        return blankToNull(getString(CoreConfigDefs.AUTH_USERNAME));
    }

    private static String upper(String v) {
        return v.trim().toUpperCase(Locale.ROOT);
    }

    private static String blankToNull(String v) {
        return v == null || v.isBlank() ? null : v.trim();
    }

    private static List<String> trimmed(List<String> in) {
        List<String> out = new ArrayList<>(in.size());
        for (String s : in) {
            if (s != null && !s.isBlank()) {
                out.add(s.trim());
            }
        }
        return out;
    }

    /** Case-insensitive enumeration validator; {@code nullable} accepts an unset value. */
    static final class OneOf implements ConfigDef.Validator {
        private final boolean nullable;
        private final List<String> values;

        OneOf(String... values) {
            this(false, values);
        }

        OneOf(boolean nullable, String... values) {
            this.nullable = nullable;
            this.values = List.of(values);
        }

        @Override
        public void ensureValid(String name, Object value) {
            if (value == null || value.toString().isBlank()) {
                if (nullable) {
                    return;
                }
                throw new ConfigException(name, value, "must be one of " + this);
            }
            String v = value.toString().trim();
            for (String allowed : values) {
                if (allowed.equalsIgnoreCase(v)) {
                    return;
                }
            }
            throw new ConfigException(name, value, "must be one of " + this);
        }

        @Override
        public String toString() {
            return "[" + String.join(", ", values) + "]";
        }
    }

    /** A nullable table name matching {@link PathSegments#TABLE_NAME}. */
    static final class TableName implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null || value.toString().isBlank()) {
                return;
            }
            if (!PathSegments.isTable(value.toString().trim())) {
                throw new ConfigException(
                        name,
                        value,
                        "must be a table name matching " + PathSegments.TABLE_NAME.pattern());
            }
        }

        @Override
        public String toString() {
            return "a table name matching " + PathSegments.TABLE_NAME.pattern();
        }
    }

    /** A list of table names, each matching {@link PathSegments#TABLE_NAME}. */
    static final class TableNameList implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (!(value instanceof List<?> list)) {
                return;
            }
            for (Object o : list) {
                String s = o == null ? "" : o.toString().trim();
                if (s.isEmpty()) {
                    continue;
                }
                if (!PathSegments.isTable(s)) {
                    throw new ConfigException(
                            name,
                            value,
                            "entry '"
                                    + s
                                    + "' must be a table name matching "
                                    + PathSegments.TABLE_NAME.pattern());
                }
            }
        }

        @Override
        public String toString() {
            return "table names matching " + PathSegments.TABLE_NAME.pattern();
        }
    }

    /** {@code from:to} pairs. */
    static final class RenameList implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (!(value instanceof List<?> list)) {
                return;
            }
            for (Object o : list) {
                String s = o == null ? "" : o.toString().trim();
                int colon = s.indexOf(':');
                if (colon <= 0 || colon == s.length() - 1) {
                    throw new ConfigException(
                            name, value, "entry '" + s + "' must have the form from:to");
                }
            }
        }

        @Override
        public String toString() {
            return "from:to pairs";
        }
    }
}
