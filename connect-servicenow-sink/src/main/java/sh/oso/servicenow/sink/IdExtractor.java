package sh.oso.servicenow.sink;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import sh.oso.servicenow.table.PathSegments;

/**
 * Finds the sys_id a record addresses: a String (or bytes) key, the configured field of a Struct or
 * Map key, the configured field of the value, or the {@value #SYS_ID_HEADER} header. When the
 * sources disagree the record is rejected unless {@code snow.sink.sys.id.precedence} names a
 * winner. Values are trimmed and lower-cased, then must match the 32-hex sys_id shape.
 */
final class IdExtractor {

    static final String SYS_ID_HEADER = "snow.sys_id";

    private final String keyField;
    private final String valueField;
    private final SinkConfig.SysIdPrecedence precedence;

    IdExtractor(SinkConfig config) {
        this.keyField = config.sysIdKeyField();
        this.valueField = config.sysIdValueField();
        this.precedence = config.sysIdPrecedence();
    }

    Optional<String> extract(SinkRecord record) {
        Map<String, String> found = new LinkedHashMap<>();
        String fromKey = fromKey(record.key());
        if (fromKey != null) {
            found.put("key", fromKey);
        }
        String fromValue = fromField(record.value(), valueField);
        if (fromValue != null) {
            found.put("value", fromValue);
        }
        Header header = record.headers().lastWithName(SYS_ID_HEADER);
        if (header != null && header.value() != null) {
            String h = normalise(TableRouter.headerText(header));
            if (h != null) {
                found.put("header", h);
            }
        }
        if (found.isEmpty()) {
            return Optional.empty();
        }
        String chosen;
        switch (precedence) {
            case KEY -> chosen = found.containsKey("key") ? found.get("key") : agree(found);
            case VALUE -> chosen = found.containsKey("value") ? found.get("value") : agree(found);
            default -> chosen = agree(found);
        }
        if (!PathSegments.isSysId(chosen)) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "sys_id '" + chosen + "' is not a 32-character hex identifier");
        }
        return Optional.of(chosen);
    }

    private String agree(Map<String, String> found) {
        Set<String> distinct = new LinkedHashSet<>(found.values());
        if (distinct.size() > 1) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "Record carries different sys_ids "
                            + found
                            + "; set "
                            + SinkConfig.SYS_ID_PRECEDENCE
                            + " to key or value to choose one");
        }
        return distinct.iterator().next();
    }

    private String fromKey(Object key) {
        if (key == null) {
            return null;
        }
        if (key instanceof Struct || key instanceof Map) {
            return fromField(key, keyField);
        }
        return normalise(scalarText(key));
    }

    private static String fromField(Object container, String field) {
        if (container instanceof Struct struct) {
            Field f = struct.schema().field(field);
            return f == null ? null : normalise(scalarText(struct.get(f)));
        }
        if (container instanceof Map<?, ?> map) {
            return normalise(scalarText(map.get(field)));
        }
        return null;
    }

    private static String scalarText(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (v instanceof ByteBuffer buf) {
            ByteBuffer dup = buf.duplicate();
            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (v instanceof CharSequence s) {
            return s.toString();
        }
        return null; // numbers, booleans, structs: never a sys_id
    }

    private static String normalise(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim().toLowerCase(Locale.ROOT);
        return t.isEmpty() ? null : t;
    }
}
