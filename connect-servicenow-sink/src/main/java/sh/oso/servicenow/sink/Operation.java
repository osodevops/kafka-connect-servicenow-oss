package sh.oso.servicenow.sink;

import java.util.Locale;

/** The four Table API write operations the sink performs. */
public enum Operation {
    CREATE,
    PATCH,
    PUT,
    DELETE;

    /**
     * Parses a configuration value ({@code create}, {@code patch}, {@code put}, {@code delete}).
     */
    public static Operation fromConfig(String value) {
        return valueOf(value.trim().toUpperCase(Locale.ROOT));
    }

    /** True for PATCH and PUT. */
    public boolean isUpdate() {
        return this == PATCH || this == PUT;
    }

    /** True when the operation addresses an existing row and therefore needs a sys_id. */
    public boolean needsSysId() {
        return this != CREATE;
    }
}
