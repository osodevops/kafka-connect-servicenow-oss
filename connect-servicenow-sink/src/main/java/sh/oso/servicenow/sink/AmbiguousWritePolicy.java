package sh.oso.servicenow.sink;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.common.RetryExhaustedException;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.http.HttpResult;
import sh.oso.servicenow.http.RequestSpec;
import sh.oso.servicenow.table.EncodedQuery;
import sh.oso.servicenow.table.Page;
import sh.oso.servicenow.table.PathSegments;
import sh.oso.servicenow.table.QueryRequest;
import sh.oso.servicenow.table.Record;
import sh.oso.servicenow.table.TableApiClient;
import sh.oso.servicenow.table.WriteResult;

/**
 * Performs the four write operations with the policies for outcomes the Table API cannot make safe
 * on its own.
 *
 * <ul>
 *   <li>CREATE after an ambiguous failure (sent, no response): {@code retry} re-sends the POST up
 *       to the retry attempt cap and accepts a possible duplicate; {@code fail_ambiguous} fails the
 *       record with {@link Classification#AMBIGUOUS}; {@code correlation_lookup} queries the
 *       correlation field ({@code sysparm_limit=2}) before every create and again after an
 *       ambiguous one: no match creates, one match patches that row, two or more fail.
 *   <li>PATCH and PUT are idempotent by sys_id, so snow-core retries them itself.
 *   <li>DELETE is sent as non-idempotent so an ambiguous failure surfaces here; it is re-sent, and
 *       a 404 on a re-sent DELETE means the first attempt was applied, which is success.
 * </ul>
 */
final class AmbiguousWritePolicy {

    private static final Logger LOG = LoggerFactory.getLogger(AmbiguousWritePolicy.class);

    /** What was done and what the instance answered. */
    record Result(Operation operation, int status, String sysId, String requestId, int retries) {}

    private final SinkConfig.CreateAmbiguousBehavior createMode;
    private final String correlationField;
    private final TableApiClient api;
    private final int maxAttempts;

    AmbiguousWritePolicy(SinkConfig config, TableApiClient api) {
        this.createMode = config.createAmbiguousBehavior();
        this.correlationField = config.correlationField();
        this.api = api;
        this.maxAttempts = Math.max(1, config.coreConfig().retryConfig().maxAttempts());
    }

    Result create(String table, Map<String, Object> body) {
        return switch (createMode) {
            case RETRY -> createWithRetry(table, body);
            case FAIL_AMBIGUOUS -> createOnce(table, body);
            case CORRELATION_LOOKUP -> createByCorrelation(table, body);
        };
    }

    Result update(Operation op, String table, String sysId, Map<String, Object> body) {
        WriteResult w =
                op == Operation.PUT ? api.put(table, sysId, body) : api.patch(table, sysId, body);
        return new Result(op, w.status(), sysId, w.requestId(), 0);
    }

    Result delete(String table, String sysId) {
        String path =
                "/api/now/table/"
                        + PathSegments.requireTable(table)
                        + "/"
                        + PathSegments.requireSysId(sysId);
        int retries = 0;
        ServiceNowException last = null;
        while (retries < maxAttempts) {
            try {
                HttpResult r = api.http().execute(RequestSpec.delete(path).idempotent(false));
                return new Result(Operation.DELETE, r.status(), sysId, r.requestId(), retries);
            } catch (ServiceNowApiException e) {
                if (e.isNotFound() && retries > 0) {
                    LOG.info(
                            "DELETE {}/{} answered 404 after an ambiguous attempt; the row is gone,"
                                    + " treating it as deleted (request-id {})",
                            table,
                            sysId,
                            e.requestId());
                    return new Result(Operation.DELETE, e.status(), sysId, e.requestId(), retries);
                }
                throw e;
            } catch (ServiceNowException e) {
                if (!e.isAmbiguous()) {
                    throw e;
                }
                last = e;
                retries++;
                LOG.warn(
                        "DELETE {}/{} outcome unknown ({}); re-sending (attempt {}/{})",
                        table,
                        sysId,
                        e.getMessage(),
                        retries + 1,
                        maxAttempts);
            }
        }
        throw new RetryExhaustedException(
                "DELETE " + table + "/" + sysId, maxAttempts, Duration.ZERO, last);
    }

    private Result createOnce(String table, Map<String, Object> body) {
        try {
            WriteResult w = api.create(table, body);
            return new Result(
                    Operation.CREATE, w.status(), w.sysId().orElse(null), w.requestId(), 0);
        } catch (ServiceNowException e) {
            if (e.isAmbiguous()) {
                throw ambiguous(table, 0, e);
            }
            throw e;
        }
    }

    private Result createWithRetry(String table, Map<String, Object> body) {
        int retries = 0;
        while (true) {
            try {
                WriteResult w = api.create(table, body);
                return new Result(
                        Operation.CREATE,
                        w.status(),
                        w.sysId().orElse(null),
                        w.requestId(),
                        retries);
            } catch (ServiceNowException e) {
                if (!e.isAmbiguous() || retries >= maxAttempts - 1) {
                    throw e.isAmbiguous() ? ambiguous(table, retries, e) : e;
                }
                retries++;
                LOG.warn(
                        "POST {} outcome unknown ({}); re-sending under"
                                + " snow.sink.create.ambiguous.behavior=retry, a duplicate row is"
                                + " possible (attempt {}/{})",
                        table,
                        e.getMessage(),
                        retries + 1,
                        maxAttempts);
            }
        }
    }

    private Result createByCorrelation(String table, Map<String, Object> body) {
        Object value = body.get(correlationField);
        String correlation = value == null ? "" : value.toString();
        if (correlation.isEmpty()) {
            throw new RecordError(
                    Classification.RECORD_ERROR,
                    "Create payload has no value for "
                            + SinkConfig.CORRELATION_FIELD
                            + "="
                            + correlationField
                            + ", which correlation_lookup needs to make the create safe");
        }
        int retries = 0;
        while (true) {
            List<Record> matches = lookup(table, correlation);
            if (matches.size() >= 2) {
                throw new RecordError(
                        Classification.AMBIGUOUS,
                        "Correlation lookup "
                                + correlationField
                                + "="
                                + correlation
                                + " on "
                                + table
                                + " matched "
                                + matches.size()
                                + " rows; the field must be unique per business object",
                        null,
                        null,
                        null,
                        null,
                        retries);
            }
            if (matches.size() == 1) {
                String sysId = matches.get(0).sysId();
                LOG.info(
                        "Correlation lookup {}={} found {}/{}; patching it instead of creating",
                        correlationField,
                        correlation,
                        table,
                        sysId);
                WriteResult w = api.patch(table, sysId, body);
                return new Result(Operation.PATCH, w.status(), sysId, w.requestId(), retries);
            }
            try {
                WriteResult w = api.create(table, body);
                return new Result(
                        Operation.CREATE,
                        w.status(),
                        w.sysId().orElse(null),
                        w.requestId(),
                        retries);
            } catch (ServiceNowException e) {
                if (!e.isAmbiguous() || retries >= maxAttempts - 1) {
                    throw e.isAmbiguous() ? ambiguous(table, retries, e) : e;
                }
                retries++;
                LOG.warn(
                        "POST {} outcome unknown ({}); looking up {}={} before trying again",
                        table,
                        e.getMessage(),
                        correlationField,
                        correlation);
            }
        }
    }

    private List<Record> lookup(String table, String correlation) {
        Page<Record> page =
                api.list(
                        QueryRequest.builder(table)
                                .query(EncodedQuery.of(correlationField + "=" + correlation))
                                .fields(List.of("sys_id"))
                                .limit(2)
                                .build());
        return page.items();
    }

    private static RecordError ambiguous(String table, int retries, ServiceNowException e) {
        return new RecordError(
                Classification.AMBIGUOUS,
                "POST "
                        + table
                        + " was sent but no response arrived; the row may or may not exist ("
                        + e.getMessage()
                        + ")",
                e,
                null,
                null,
                null,
                retries);
    }
}
