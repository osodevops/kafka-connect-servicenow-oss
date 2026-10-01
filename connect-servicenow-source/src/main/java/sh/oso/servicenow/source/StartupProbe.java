package sh.oso.servicenow.source;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.common.ConnectExceptions;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.cursor.KeysetQueryBuilder;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.table.DisplayValue;
import sh.oso.servicenow.table.Page;
import sh.oso.servicenow.table.QueryRequest;
import sh.oso.servicenow.table.Record;

/**
 * Pre-flight checks run once when a task starts: one-row read per table proving the integration
 * user can read it and that the cursor fields are present and parseable, plus a best-effort look at
 * the user's {@code sys_user.time_zone} (encoded-query timestamps are interpreted in the session
 * user's time zone, so anything but UTC skews the cursor). Failures name the table, alias and
 * field; the time-zone check only warns.
 */
final class StartupProbe {

    private static final Logger LOG = LoggerFactory.getLogger(StartupProbe.class);
    static final Set<String> UTC_ZONES = Set.of("utc", "gmt", "etc/utc", "etc/gmt", "universal");

    private final ServiceNowClient client;

    StartupProbe(ServiceNowClient client) {
        this.client = client;
    }

    /** Reads one row of {@code spec} and verifies access and the cursor fields. */
    void probe(TableSpec spec) {
        List<String> fields =
                List.of(spec.sysIdField(), spec.timestampField(), KeysetQueryBuilder.SYS_MOD_COUNT);
        QueryRequest request =
                QueryRequest.builder(spec.name())
                        .query(spec.query())
                        .fields(fields)
                        .limit(1)
                        .displayValue(DisplayValue.FALSE)
                        .excludeReferenceLink(true)
                        .queryCategory(spec.queryCategory())
                        .queryNoDomain(!spec.queryDomain())
                        .build();
        Page<Record> page;
        try {
            page = client.tableApi().list(request);
        } catch (ServiceNowApiException e) {
            throw describe(spec, e);
        } catch (ServiceNowException e) {
            throw ConnectExceptions.toConnect(e);
        }
        if (page.isEmpty()) {
            LOG.info(
                    "Startup probe: table '{}' (alias {}) is readable and currently returns no rows"
                            + " for query '{}'",
                    spec.name(),
                    spec.alias(),
                    spec.query());
            return;
        }
        Record row = page.items().get(0);
        for (String field : List.of(spec.sysIdField(), spec.timestampField())) {
            if (!row.has(field)) {
                throw new ConnectException(
                        "Table '"
                                + spec.name()
                                + "' (alias "
                                + spec.alias()
                                + "): cursor field '"
                                + field
                                + "' is missing from the response; it is hidden by an ACL or is"
                                + " not a column of this table or view (fields seen: "
                                + row.fieldNames()
                                + ")");
            }
        }
        String ts = row.string(spec.timestampField());
        if (!SnowTimestamp.isValid(ts)) {
            throw new ConnectException(
                    "Table '"
                            + spec.name()
                            + "' (alias "
                            + spec.alias()
                            + "): timestamp field '"
                            + spec.timestampField()
                            + "' returned '"
                            + ts
                            + "', expected yyyy-MM-dd HH:mm:ss; check the integration user's date"
                            + " format and time zone (must be UTC) or the field type");
        }
        LOG.info(
                "Startup probe: table '{}' (alias {}) readable, cursor fields {} and {} present",
                spec.name(),
                spec.alias(),
                spec.sysIdField(),
                spec.timestampField());
    }

    /**
     * Warns when the integration user's time zone is readable and not UTC. Never fails: the {@code
     * sys_user} table may be hidden from the user, and OAuth client credentials have no username to
     * look up.
     */
    void checkUserTimeZone() {
        String username = client.config().authConfig().username();
        if (username == null || username.isBlank()) {
            LOG.debug("Startup probe: no username configured; skipping the time zone check");
            return;
        }
        try {
            Page<Record> page =
                    client.tableApi()
                            .list(
                                    QueryRequest.builder("sys_user")
                                            .query("user_name=" + username)
                                            .fields(List.of("user_name", "time_zone"))
                                            .limit(1)
                                            .build());
            if (page.isEmpty()) {
                LOG.debug(
                        "Startup probe: sys_user row for '{}' not visible; cannot verify time zone",
                        username);
                return;
            }
            String tz = page.items().get(0).string("time_zone");
            if (tz == null || tz.isBlank()) {
                LOG.debug(
                        "Startup probe: user '{}' has no explicit time zone (instance default"
                                + " applies; it must be UTC)",
                        username);
                return;
            }
            if (!isUtc(tz)) {
                LOG.warn(
                        "Integration user '{}' has time zone '{}', not UTC. Encoded-query"
                                + " timestamps follow the session user's time zone, so the cursor"
                                + " will be skewed; set the user's time zone to UTC.",
                        username,
                        tz);
            } else {
                LOG.info("Startup probe: integration user '{}' time zone is {}", username, tz);
            }
        } catch (RuntimeException e) {
            LOG.debug(
                    "Startup probe: could not read sys_user for '{}': {}",
                    username,
                    e.getMessage());
        }
    }

    static boolean isUtc(String tz) {
        return tz != null && UTC_ZONES.contains(tz.trim().toLowerCase(Locale.ROOT));
    }

    private static RuntimeException describe(TableSpec spec, ServiceNowApiException e) {
        String where = "Table '" + spec.name() + "' (alias " + spec.alias() + ")";
        if (e.isForbidden()) {
            return new ConnectException(
                    where
                            + ": HTTP 403, the integration user cannot read this table or one of the"
                            + " queried fields (ACL). Grant read access or remove the table. "
                            + e.getMessage(),
                    e);
        }
        if (e.isNotFound()) {
            return new ConnectException(
                    where
                            + ": HTTP 404, the table or view does not exist or is not exposed to the"
                            + " Table API. "
                            + e.getMessage(),
                    e);
        }
        if (e.isUnauthorized()) {
            return new ConnectException(
                    where + ": HTTP 401, the credentials were rejected. " + e.getMessage(), e);
        }
        if (e.status() == 400) {
            return new ConnectException(
                    where
                            + ": HTTP 400, the base query or a projected field was rejected by the"
                            + " instance. "
                            + e.getMessage(),
                    e);
        }
        RuntimeException mapped = ConnectExceptions.toConnect(e);
        if (mapped instanceof ConnectException
                && !(mapped instanceof org.apache.kafka.connect.errors.RetriableException)) {
            return new ConnectException(where + ": " + e.getMessage(), e);
        }
        return mapped;
    }
}
