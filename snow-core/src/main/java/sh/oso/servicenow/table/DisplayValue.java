package sh.oso.servicenow.table;

import java.util.Locale;

/** {@code sysparm_display_value}: raw values, display values, or both. */
public enum DisplayValue {
    FALSE,
    TRUE,
    ALL;

    public String wireValue() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static DisplayValue fromConfig(String value) {
        if (value == null) {
            return FALSE;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "false" -> FALSE;
            case "true" -> TRUE;
            case "all" -> ALL;
            default ->
                    throw new IllegalArgumentException(
                            "display value must be one of [false, true, all]: " + value);
        };
    }
}
