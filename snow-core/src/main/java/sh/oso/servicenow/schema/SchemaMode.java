package sh.oso.servicenow.schema;

import java.util.Locale;

/** How source records are shaped: schemaless maps, an all-string Struct, or a typed Struct. */
public enum SchemaMode {
    SCHEMALESS,
    STRINGS,
    TYPED;

    public static SchemaMode fromConfig(String value) {
        if (value == null) {
            return SCHEMALESS;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "schemaless" -> SCHEMALESS;
            case "strings" -> STRINGS;
            case "typed" -> TYPED;
            default ->
                    throw new IllegalArgumentException(
                            "schema mode must be one of [schemaless, strings, typed]: " + value);
        };
    }
}
