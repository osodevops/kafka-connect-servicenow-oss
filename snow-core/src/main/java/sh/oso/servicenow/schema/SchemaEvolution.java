package sh.oso.servicenow.schema;

import java.util.Locale;

/**
 * What the typed mapper does with a field that is not in the explicit mapping or whose value does
 * not coerce: FAIL rejects the record, BACKWARD adds unknown fields as optional strings but still
 * rejects bad values, PERMISSIVE adds unknown fields and nulls bad values.
 */
public enum SchemaEvolution {
    FAIL,
    BACKWARD,
    PERMISSIVE;

    public static SchemaEvolution fromConfig(String value) {
        if (value == null) {
            return FAIL;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "fail" -> FAIL;
            case "backward" -> BACKWARD;
            case "permissive" -> PERMISSIVE;
            default ->
                    throw new IllegalArgumentException(
                            "schema evolution must be one of [fail, backward, permissive]: "
                                    + value);
        };
    }
}
