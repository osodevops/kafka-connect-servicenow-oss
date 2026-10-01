package sh.oso.servicenow.table;

import java.util.regex.Pattern;
import sh.oso.servicenow.common.ServiceNowException;

/** Validation for values interpolated into Table API paths. */
public final class PathSegments {

    public static final Pattern TABLE_NAME = Pattern.compile("^[a-z0-9_]+$");
    public static final Pattern SYS_ID = Pattern.compile("^[0-9a-f]{32}$");

    private PathSegments() {}

    public static boolean isTable(String table) {
        return table != null && TABLE_NAME.matcher(table).matches();
    }

    public static boolean isSysId(String sysId) {
        return sysId != null && SYS_ID.matcher(sysId).matches();
    }

    public static String requireTable(String table) {
        if (!isTable(table)) {
            throw new ServiceNowException(
                    "Invalid table name '" + table + "': must match " + TABLE_NAME.pattern());
        }
        return table;
    }

    public static String requireSysId(String sysId) {
        if (!isSysId(sysId)) {
            throw new ServiceNowException(
                    "Invalid sys_id '" + sysId + "': must match " + SYS_ID.pattern());
        }
        return sysId;
    }
}
