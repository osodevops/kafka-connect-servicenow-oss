package sh.oso.servicenow.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

/**
 * Explicit {@code field:type} mapping for {@link SchemaMode#TYPED}. Types: {@code string, int8,
 * int16, int32, int64, float32, float64, boolean, timestamp, date, time, decimal, decimal(scale)}
 * (decimal defaults to scale 2). Every field schema is optional.
 */
public final class TypedFieldMapping {

    public static final int DEFAULT_DECIMAL_SCALE = 2;

    private static final Pattern ENTRY =
            Pattern.compile("^\\s*([A-Za-z0-9_.]+)\\s*:\\s*([a-z0-9]+)(?:\\((\\d{1,2})\\))?\\s*$");

    private final Map<String, Schema> fields;
    private final SchemaEvolution evolution;

    private TypedFieldMapping(Map<String, Schema> fields, SchemaEvolution evolution) {
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        this.evolution = evolution;
    }

    public static TypedFieldMapping parse(List<String> fieldColonType) {
        return parse(fieldColonType, SchemaEvolution.FAIL);
    }

    public static TypedFieldMapping parse(List<String> fieldColonType, SchemaEvolution evolution) {
        LinkedHashMap<String, Schema> out = new LinkedHashMap<>();
        if (fieldColonType != null) {
            for (String entry : fieldColonType) {
                if (entry == null || entry.isBlank()) {
                    continue;
                }
                Matcher m = ENTRY.matcher(entry);
                if (!m.matches()) {
                    throw new IllegalArgumentException(
                            "Typed field mapping entry '" + entry + "' must look like field:type");
                }
                String field = m.group(1);
                if (out.containsKey(field)) {
                    throw new IllegalArgumentException(
                            "Typed field '" + field + "' is mapped twice");
                }
                out.put(field, schemaFor(m.group(2), m.group(3), entry));
            }
        }
        return new TypedFieldMapping(out, Objects.requireNonNull(evolution, "evolution"));
    }

    public static TypedFieldMapping of(Map<String, Schema> fields, SchemaEvolution evolution) {
        return new TypedFieldMapping(fields, evolution);
    }

    static Schema schemaFor(String type, String scaleArg, String entry) {
        String t = type.toLowerCase(Locale.ROOT);
        if (scaleArg != null && !"decimal".equals(t)) {
            throw new IllegalArgumentException("Only decimal takes a scale: '" + entry + "'");
        }
        return switch (t) {
            case "string" -> Schema.OPTIONAL_STRING_SCHEMA;
            case "int8" -> Schema.OPTIONAL_INT8_SCHEMA;
            case "int16" -> Schema.OPTIONAL_INT16_SCHEMA;
            case "int32" -> Schema.OPTIONAL_INT32_SCHEMA;
            case "int64" -> Schema.OPTIONAL_INT64_SCHEMA;
            case "float32" -> Schema.OPTIONAL_FLOAT32_SCHEMA;
            case "float64" -> Schema.OPTIONAL_FLOAT64_SCHEMA;
            case "boolean" -> Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case "bytes" -> Schema.OPTIONAL_BYTES_SCHEMA;
            case "timestamp" -> Timestamp.builder().optional().build();
            case "date" -> org.apache.kafka.connect.data.Date.builder().optional().build();
            case "time" -> Time.builder().optional().build();
            case "decimal" ->
                    Decimal.builder(
                                    scaleArg == null
                                            ? DEFAULT_DECIMAL_SCALE
                                            : Integer.parseInt(scaleArg))
                            .optional()
                            .build();
            default ->
                    throw new IllegalArgumentException(
                            "Unknown type '"
                                    + type
                                    + "' in '"
                                    + entry
                                    + "'; expected one of string, int8,"
                                    + " int16, int32, int64, float32, float64, boolean, bytes, timestamp,"
                                    + " date, time, decimal, decimal(scale)");
        };
    }

    public Map<String, Schema> fields() {
        return fields;
    }

    public SchemaEvolution evolution() {
        return evolution;
    }

    public TypedFieldMapping withEvolution(SchemaEvolution evolution) {
        return new TypedFieldMapping(fields, evolution);
    }

    public boolean isEmpty() {
        return fields.isEmpty();
    }

    /** Applies the mapping to a schema builder (all fields optional, in mapping order). */
    public void addTo(SchemaBuilder builder) {
        fields.forEach(builder::field);
    }

    @Override
    public String toString() {
        return "TypedFieldMapping{fields=" + fields.keySet() + ", evolution=" + evolution + '}';
    }
}
