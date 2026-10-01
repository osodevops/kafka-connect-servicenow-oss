package sh.oso.servicenow.schema;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.table.FieldValue;
import sh.oso.servicenow.table.Record;

/**
 * Shapes a Table API {@link Record} as a Connect value.
 *
 * <ul>
 *   <li>{@link SchemaMode#SCHEMALESS}: a {@code Map<String,Object>} mirroring the response. Absent
 *       fields are absent, JSON nulls are nulls, and {@code display_value=all} fields become {@code
 *       {value, display_value, link}} maps.
 *   <li>{@link SchemaMode#STRINGS}: a Struct whose fields are all optional strings. The schema only
 *       grows: a new field bumps the version and is appended; fields are never removed. A rich
 *       field contributes {@code <name>} (raw value) and {@code <name>_display_value}.
 *   <li>{@link SchemaMode#TYPED}: a Struct from the explicit {@link TypedFieldMapping}; unmapped
 *       fields and coercion failures follow {@link SchemaEvolution}.
 * </ul>
 *
 * Thread-safe; one instance per table keeps the growing schema.
 */
public final class RecordSchemaMapper {

    private static final Logger LOG = LoggerFactory.getLogger(RecordSchemaMapper.class);
    public static final String DISPLAY_SUFFIX = "_display_value";

    private final SchemaMode mode;
    private final String schemaName;
    private final TypedFieldMapping typed;
    private final Object lock = new Object();
    private final Map<String, Schema> knownFields = new LinkedHashMap<>();
    private Schema currentSchema;
    private int version;

    public RecordSchemaMapper(SchemaMode mode, String schemaName, TypedFieldMapping typed) {
        this.mode = Objects.requireNonNull(mode, "mode");
        this.schemaName = schemaName;
        this.typed = typed;
        if (mode == SchemaMode.TYPED) {
            if (typed == null || typed.isEmpty()) {
                throw new IllegalArgumentException(
                        "Typed schema mode needs a non-empty field mapping");
            }
            knownFields.putAll(typed.fields());
            currentSchema = build();
        }
    }

    public SchemaMode mode() {
        return mode;
    }

    /** The current Struct schema (null in schemaless mode). */
    public Schema currentSchema() {
        synchronized (lock) {
            return currentSchema;
        }
    }

    public SchemaAndValue map(Record record) {
        return switch (mode) {
            case SCHEMALESS -> new SchemaAndValue(null, schemaless(record));
            case STRINGS -> strings(record);
            case TYPED -> typed(record);
        };
    }

    private Map<String, Object> schemaless(Record record) {
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, FieldValue> e : record.fields().entrySet()) {
            FieldValue v = e.getValue();
            if (v.isRich()) {
                LinkedHashMap<String, Object> rich = new LinkedHashMap<>();
                rich.put("value", v.value());
                rich.put("display_value", v.displayValue());
                rich.put("link", v.link());
                out.put(e.getKey(), rich);
            } else {
                out.put(e.getKey(), v.value());
            }
        }
        return out;
    }

    private SchemaAndValue strings(Record record) {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        for (Map.Entry<String, FieldValue> e : record.fields().entrySet()) {
            FieldValue v = e.getValue();
            values.put(e.getKey(), v.value());
            if (v.displayValue() != null) {
                values.put(e.getKey() + DISPLAY_SUFFIX, v.displayValue());
            }
        }
        Schema schema;
        synchronized (lock) {
            boolean grew = false;
            for (String name : values.keySet()) {
                if (!knownFields.containsKey(name)) {
                    knownFields.put(name, Schema.OPTIONAL_STRING_SCHEMA);
                    grew = true;
                }
            }
            if (grew || currentSchema == null) {
                currentSchema = build();
            }
            schema = currentSchema;
        }
        Struct struct = new Struct(schema);
        values.forEach(struct::put);
        return new SchemaAndValue(schema, struct);
    }

    private SchemaAndValue typed(Record record) {
        SchemaEvolution evolution = typed.evolution();
        Set<String> unknown = new LinkedHashSet<>();
        synchronized (lock) {
            for (String name : record.fieldNames()) {
                if (!knownFields.containsKey(name)) {
                    unknown.add(name);
                }
            }
            if (!unknown.isEmpty()) {
                if (evolution == SchemaEvolution.FAIL) {
                    throw new ServiceNowException(
                            "Record carries field(s) not in the typed mapping: "
                                    + unknown
                                    + " (schema evolution is FAIL)");
                }
                for (String name : unknown) {
                    knownFields.put(name, Schema.OPTIONAL_STRING_SCHEMA);
                }
                LOG.info("Typed schema {} grew by {} ({})", schemaName, unknown, evolution);
                currentSchema = build();
            }
        }
        Schema schema = currentSchema();
        Struct struct = new Struct(schema);
        for (Map.Entry<String, FieldValue> e : record.fields().entrySet()) {
            Schema fieldSchema = schema.field(e.getKey()).schema();
            String raw = e.getValue().value();
            Object value;
            try {
                value = ValueCoercions.toConnect(raw, fieldSchema);
            } catch (ServiceNowException ex) {
                if (evolution == SchemaEvolution.PERMISSIVE) {
                    LOG.warn("Field {} set to null: {}", e.getKey(), ex.getMessage());
                    value = null;
                } else {
                    throw new ServiceNowException(
                            "Field '" + e.getKey() + "': " + ex.getMessage(), ex);
                }
            }
            struct.put(e.getKey(), value);
        }
        return new SchemaAndValue(schema, struct);
    }

    private Schema build() {
        version++;
        SchemaBuilder b = SchemaBuilder.struct().optional().version(version);
        if (schemaName != null && !schemaName.isBlank()) {
            b.name(schemaName);
        }
        knownFields.forEach(b::field);
        return b.build();
    }
}
