package sh.oso.servicenow.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.schema.TableMetadataClient;
import sh.oso.servicenow.schema.ValueCoercions;

/**
 * Turns a record value (Struct, Map, or a String holding a JSON object) into the Table API body.
 * Rules apply in order: rename, allow or deny list, reserved-field strip, null policy, nested
 * policy, scalar rendering through {@link ValueCoercions}, then the unknown-field policy against
 * the table dictionary. Every body value is a string, which is what the Table API expects; {@code
 * u_} custom field names pass through untouched.
 */
final class RecordMapper {

    static final Set<String> RESERVED_FIELDS =
            Set.of(
                    "sys_id",
                    "sys_created_on",
                    "sys_updated_on",
                    "sys_mod_count",
                    "sys_created_by",
                    "sys_updated_by");

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, Object>> OBJECT =
            new TypeReference<>() {};

    /** The body plus any dictionary-unknown fields removed under the {@code report} policy. */
    record Mapped(Map<String, Object> body, List<String> droppedUnknownFields) {
        static Mapped empty() {
            return new Mapped(Map.of(), List.of());
        }
    }

    private final Map<String, String> renames;
    private final Set<String> allowlist;
    private final Set<String> denylist;
    private final Set<String> reserved;
    private final SinkConfig.NullBehavior nulls;
    private final SinkConfig.NestedBehavior nested;
    private final String delimiter;
    private final SinkConfig.UnknownFieldBehavior unknown;
    private final TableMetadataClient metadata;

    RecordMapper(SinkConfig config, TableMetadataClient metadata) {
        this.renames = config.fieldRenames();
        this.allowlist = config.fieldAllowlist();
        this.denylist = config.fieldDenylist();
        Set<String> r = new java.util.HashSet<>(RESERVED_FIELDS);
        r.add(config.operationHeader());
        r.add(config.tableHeader());
        r.add(IdExtractor.SYS_ID_HEADER);
        this.reserved = Set.copyOf(r);
        this.nulls = config.nullBehavior();
        this.nested = config.nestedBehavior();
        this.delimiter = config.flattenDelimiter();
        this.unknown = config.unknownFieldBehavior();
        this.metadata = metadata;
        if (unknown != SinkConfig.UnknownFieldBehavior.PASSTHROUGH && metadata == null) {
            throw new IllegalArgumentException(
                    "TableMetadataClient is required unless "
                            + SinkConfig.UNKNOWN_FIELD_BEHAVIOR
                            + "=passthrough");
        }
    }

    Mapped map(SinkRecord record, String table) {
        Map<String, Object> body = new LinkedHashMap<>();
        for (Entry e : entries(record)) {
            String name = renames.getOrDefault(e.name(), e.name());
            if (!allowlist.isEmpty() && !allowlist.contains(name)) {
                continue;
            }
            if (denylist.contains(name)) {
                continue;
            }
            if (reserved.contains(name) && !allowlist.contains(name)) {
                continue;
            }
            put(body, name, e.value(), e.schema());
        }
        return applyUnknownPolicy(body, table);
    }

    private void put(Map<String, Object> body, String path, Object value, Schema schema) {
        if (value == null) {
            switch (nulls) {
                case OMIT -> {}
                case CLEAR -> body.put(path, "");
                case REJECT ->
                        throw new RecordError(
                                Classification.RECORD_ERROR,
                                "Field '"
                                        + path
                                        + "' is null and "
                                        + SinkConfig.NULL_BEHAVIOR
                                        + "=reject");
            }
            return;
        }
        if (isNested(value)) {
            switch (nested) {
                case REJECT ->
                        throw new RecordError(
                                Classification.RECORD_ERROR,
                                "Field '"
                                        + path
                                        + "' is "
                                        + kind(value)
                                        + "; ServiceNow fields are scalar. Set "
                                        + SinkConfig.NESTED_BEHAVIOR
                                        + " to flatten or stringify, or drop the field");
                case FLATTEN -> flatten(body, path, value, schema);
                case STRINGIFY -> body.put(path, stringify(path, value, schema));
            }
            return;
        }
        body.put(path, scalar(path, value, schema));
    }

    private void flatten(Map<String, Object> body, String prefix, Object value, Schema schema) {
        if (value instanceof Struct struct) {
            for (Field f : struct.schema().fields()) {
                put(body, prefix + delimiter + f.name(), struct.get(f), f.schema());
            }
            return;
        }
        if (value instanceof Map<?, ?> map) {
            Schema valueSchema =
                    schema != null && schema.type() == Schema.Type.MAP
                            ? schema.valueSchema()
                            : null;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                put(body, prefix + delimiter + e.getKey(), e.getValue(), valueSchema);
            }
            return;
        }
        throw new RecordError(
                Classification.RECORD_ERROR,
                "Field '"
                        + prefix
                        + "' is an array and cannot be flattened; set "
                        + SinkConfig.NESTED_BEHAVIOR
                        + "=stringify or drop the field");
    }

    private static String stringify(String path, Object value, Schema schema) {
        try {
            return JSON.writeValueAsString(tree(value, schema));
        } catch (JsonProcessingException e) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "Field '" + path + "' cannot be rendered as JSON: " + e.getOriginalMessage(),
                    e);
        }
    }

    /** Jackson-friendly tree with logical types rendered the way ServiceNow expects. */
    private static Object tree(Object value, Schema schema) {
        if (value == null) {
            return null;
        }
        if (value instanceof Struct struct) {
            LinkedHashMap<String, Object> out = new LinkedHashMap<>();
            for (Field f : struct.schema().fields()) {
                out.put(f.name(), tree(struct.get(f), f.schema()));
            }
            return out;
        }
        if (value instanceof Map<?, ?> map) {
            Schema vs =
                    schema != null && schema.type() == Schema.Type.MAP
                            ? schema.valueSchema()
                            : null;
            LinkedHashMap<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.put(String.valueOf(e.getKey()), tree(e.getValue(), vs));
            }
            return out;
        }
        if (value instanceof Collection<?> list) {
            Schema vs =
                    schema != null && schema.type() == Schema.Type.ARRAY
                            ? schema.valueSchema()
                            : null;
            List<Object> out = new ArrayList<>(list.size());
            for (Object o : list) {
                out.add(tree(o, vs));
            }
            return out;
        }
        if (value instanceof Object[] array) {
            List<Object> out = new ArrayList<>(array.length);
            for (Object o : array) {
                out.add(tree(o, null));
            }
            return out;
        }
        if ((value instanceof Number || value instanceof Boolean)
                && (schema == null || schema.name() == null)) {
            return value;
        }
        return ValueCoercions.toServiceNow(value, schema);
    }

    private static String scalar(String path, Object value, Schema schema) {
        try {
            return ValueCoercions.toServiceNow(value, schema);
        } catch (ClassCastException | ServiceNowException e) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "Field '" + path + "' cannot be rendered: " + e.getMessage(),
                    e);
        }
    }

    private Mapped applyUnknownPolicy(Map<String, Object> body, String table) {
        if (unknown == SinkConfig.UnknownFieldBehavior.PASSTHROUGH || body.isEmpty()) {
            return new Mapped(body, List.of());
        }
        Set<String> columns = columns(table);
        List<String> unknownFields = new ArrayList<>();
        for (String name : body.keySet()) {
            if (!columns.contains(name)) {
                unknownFields.add(name);
            }
        }
        if (unknownFields.isEmpty()) {
            return new Mapped(body, List.of());
        }
        switch (unknown) {
            case FAIL ->
                    throw new RecordError(
                            Classification.RECORD_ERROR,
                            "Field(s) "
                                    + unknownFields
                                    + " are not in the dictionary of table "
                                    + table
                                    + "; rename or drop them, or set "
                                    + SinkConfig.UNKNOWN_FIELD_BEHAVIOR
                                    + " to drop, report or passthrough");
            case DROP -> {
                unknownFields.forEach(body::remove);
                return new Mapped(body, List.of());
            }
            case REPORT -> {
                unknownFields.forEach(body::remove);
                return new Mapped(body, unknownFields);
            }
            default -> throw new IllegalStateException(unknown.toString());
        }
    }

    private Set<String> columns(String table) {
        try {
            return metadata.columns(table);
        } catch (ServiceNowApiException e) {
            Classification c =
                    e.isForbidden() ? Classification.PERMISSION : Classification.RECORD_ERROR;
            throw new RecordError(
                    c,
                    "Cannot read the dictionary of table "
                            + table
                            + " (needed by "
                            + SinkConfig.UNKNOWN_FIELD_BEHAVIOR
                            + "="
                            + unknown.name().toLowerCase(java.util.Locale.ROOT)
                            + "; grant read on sys_db_object and sys_dictionary or use"
                            + " passthrough): "
                            + e.getMessage(),
                    e,
                    e.status(),
                    e.requestId(),
                    e.responseExcerpt(),
                    0);
        } catch (ServiceNowException e) {
            if (e.isRetryable()) {
                throw e;
            }
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "Cannot read the dictionary of table " + table + ": " + e.getMessage(),
                    e);
        }
    }

    private record Entry(String name, Object value, Schema schema) {}

    private static List<Entry> entries(SinkRecord record) {
        Object value = record.value();
        if (value == null) {
            throw new RecordError(
                    Classification.RECORD_ERROR, "Record value is null; nothing to write");
        }
        if (value instanceof byte[] bytes) {
            value = new String(bytes, StandardCharsets.UTF_8);
        }
        List<Entry> out = new ArrayList<>();
        if (value instanceof Struct struct) {
            for (Field f : struct.schema().fields()) {
                out.add(new Entry(f.name(), struct.get(f), f.schema()));
            }
            return out;
        }
        if (value instanceof Map<?, ?> map) {
            Schema schema = record.valueSchema();
            Schema vs =
                    schema != null && schema.type() == Schema.Type.MAP
                            ? schema.valueSchema()
                            : null;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.add(new Entry(String.valueOf(e.getKey()), e.getValue(), vs));
            }
            return out;
        }
        if (value instanceof String json) {
            Map<String, Object> parsed;
            try {
                parsed = JSON.readValue(json, OBJECT);
            } catch (JsonProcessingException e) {
                throw new RecordError(
                        Classification.RECORD_ERROR,
                        "String record value is not a JSON object: " + e.getOriginalMessage(),
                        e);
            }
            if (parsed == null) {
                throw new RecordError(
                        Classification.RECORD_ERROR, "String record value is JSON null");
            }
            for (Map.Entry<String, Object> e : parsed.entrySet()) {
                out.add(new Entry(e.getKey(), e.getValue(), null));
            }
            return out;
        }
        throw new RecordError(
                Classification.RECORD_ERROR,
                "Unsupported record value type "
                        + value.getClass().getName()
                        + ": expected a Struct, a Map or a String holding a JSON object");
    }

    private static boolean isNested(Object v) {
        return v instanceof Struct
                || v instanceof Map
                || v instanceof Collection
                || v instanceof Object[];
    }

    private static String kind(Object v) {
        if (v instanceof Struct) {
            return "a Struct";
        }
        if (v instanceof Map) {
            return "a Map";
        }
        return "an array";
    }
}
