package sh.oso.servicenow.table;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** An immutable Table API record: insertion-ordered field name to {@link FieldValue}. */
public final class Record {

    private final Map<String, FieldValue> fields;

    private Record(Map<String, FieldValue> fields) {
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    public static Record of(Map<String, FieldValue> fields) {
        return new Record(fields);
    }

    /** Builds a record from plain string values. */
    public static Record ofStrings(Map<String, String> values) {
        LinkedHashMap<String, FieldValue> out = new LinkedHashMap<>();
        values.forEach((k, v) -> out.put(k, v == null ? FieldValue.NULL : FieldValue.of(v)));
        return new Record(out);
    }

    /** Parses one element of a Table API {@code result}. */
    public static Record fromJson(JsonNode node) {
        LinkedHashMap<String, FieldValue> out = new LinkedHashMap<>();
        if (node != null && node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                out.put(e.getKey(), parseValue(e.getValue()));
            }
        }
        return new Record(out);
    }

    private static FieldValue parseValue(JsonNode v) {
        if (v == null || v.isNull() || v.isMissingNode()) {
            return FieldValue.NULL;
        }
        if (v.isObject()) {
            return new FieldValue(
                    text(v.get("value")), text(v.get("display_value")), text(v.get("link")));
        }
        return FieldValue.of(v.isTextual() ? v.asText() : v.toString());
    }

    private static String text(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode()) {
            return null;
        }
        return n.isTextual() ? n.asText() : n.toString();
    }

    public Map<String, FieldValue> fields() {
        return fields;
    }

    /** Raw value, or null when the field is absent or null. */
    public String string(String name) {
        FieldValue v = fields.get(name);
        return v == null ? null : v.value();
    }

    public Optional<String> opt(String name) {
        return Optional.ofNullable(string(name));
    }

    /** Display value when present, else the raw value. */
    public String display(String name) {
        FieldValue v = fields.get(name);
        if (v == null) {
            return null;
        }
        return v.displayValue() != null ? v.displayValue() : v.value();
    }

    public FieldValue field(String name) {
        return fields.get(name);
    }

    /** True when the field is present, even if its value is null. */
    public boolean has(String name) {
        return fields.containsKey(name);
    }

    public Set<String> fieldNames() {
        return fields.keySet();
    }

    public int size() {
        return fields.size();
    }

    public String sysId() {
        return string("sys_id");
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Record r && fields.equals(r.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    @Override
    public String toString() {
        return "Record" + fields;
    }
}
