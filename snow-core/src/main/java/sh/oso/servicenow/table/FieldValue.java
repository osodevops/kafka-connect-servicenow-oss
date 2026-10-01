package sh.oso.servicenow.table;

/**
 * One field of a Table API record. Plain responses carry only {@code value}; {@code
 * sysparm_display_value=all} adds {@code displayValue}, and reference fields add {@code link}
 * unless {@code sysparm_exclude_reference_link=true}. A JSON {@code null} is a {@code FieldValue}
 * whose three members are null; an absent field is not in the record at all.
 */
public record FieldValue(String value, String displayValue, String link) {

    public static final FieldValue NULL = new FieldValue(null, null, null);

    public static FieldValue of(String value) {
        return new FieldValue(value, null, null);
    }

    /** True when the response carried a display value or link, i.e. the object shape. */
    public boolean isRich() {
        return displayValue != null || link != null;
    }

    public boolean isNull() {
        return value == null && displayValue == null && link == null;
    }
}
