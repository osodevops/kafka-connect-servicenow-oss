package sh.oso.servicenow.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.http.HttpResult;
import sh.oso.servicenow.http.RequestSpec;
import sh.oso.servicenow.http.ServiceNowHttpClient;

/**
 * The six Table API operations on {@code /api/now/table/{table}[/{sys_id}]}. Table names and
 * sys_ids are validated before any request is built.
 */
public final class TableApiClient {

    private static final Logger LOG = LoggerFactory.getLogger(TableApiClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ServiceNowHttpClient http;

    public TableApiClient(ServiceNowHttpClient http) {
        this.http = Objects.requireNonNull(http, "http");
    }

    public ServiceNowHttpClient http() {
        return http;
    }

    /** {@code GET /api/now/table/{table}?sysparm_...}. */
    public Page<Record> list(QueryRequest q) {
        HttpResult result = http.execute(RequestSpec.get(q.pathAndQuery()));
        JsonNode root = json(result, "GET", q.table());
        JsonNode array = root.path("result");
        List<Record> records = new ArrayList<>();
        if (array.isArray()) {
            for (JsonNode n : array) {
                records.add(Record.fromJson(n));
            }
        } else if (array.isObject()) {
            records.add(Record.fromJson(array));
        }
        LOG.debug("GET {} returned {} row(s) (limit {})", q.table(), records.size(), q.limit());
        return new Page<>(records, q.limit());
    }

    /** {@code GET /api/now/table/{table}/{sys_id}}; empty on 404. */
    public Optional<Record> get(String table, String sysId, GetOptions opts) {
        String path = recordPath(table, sysId);
        GetOptions o = opts == null ? GetOptions.defaults() : opts;
        LinkedHashMap<String, String> params = new LinkedHashMap<>();
        if (!o.fields().isEmpty()) {
            params.put("sysparm_fields", String.join(",", o.fields()));
        }
        params.put("sysparm_display_value", o.displayValue().wireValue());
        params.put("sysparm_exclude_reference_link", Boolean.toString(o.excludeReferenceLink()));
        try {
            HttpResult result =
                    http.execute(RequestSpec.get(path + "?" + QueryRequest.encode(params)));
            return Optional.of(Record.fromJson(json(result, "GET", table).path("result")));
        } catch (ServiceNowApiException e) {
            if (e.isNotFound()) {
                return Optional.empty();
            }
            throw e;
        }
    }

    /** {@code POST /api/now/table/{table}}; 201 with the created record. Not idempotent. */
    public WriteResult create(String table, Map<String, ?> fields) {
        String path = "/api/now/table/" + PathSegments.requireTable(table);
        HttpResult result = http.execute(RequestSpec.post(path, toJson(fields)));
        return writeResult(result, "POST", table);
    }

    /** {@code PATCH /api/now/table/{table}/{sys_id}}; 200 with the merged record. */
    public WriteResult patch(String table, String sysId, Map<String, ?> fields) {
        HttpResult result =
                http.execute(RequestSpec.patch(recordPath(table, sysId), toJson(fields)));
        return writeResult(result, "PATCH", table);
    }

    /** {@code PUT /api/now/table/{table}/{sys_id}}; 200 with the updated record. */
    public WriteResult put(String table, String sysId, Map<String, ?> fields) {
        HttpResult result = http.execute(RequestSpec.put(recordPath(table, sysId), toJson(fields)));
        return writeResult(result, "PUT", table);
    }

    /** {@code DELETE /api/now/table/{table}/{sys_id}}; 204 with no body. 404 raises. */
    public WriteResult delete(String table, String sysId) {
        HttpResult result = http.execute(RequestSpec.delete(recordPath(table, sysId)));
        return new WriteResult(result.status(), Optional.empty(), result.requestId());
    }

    private static String recordPath(String table, String sysId) {
        return "/api/now/table/"
                + PathSegments.requireTable(table)
                + "/"
                + PathSegments.requireSysId(sysId);
    }

    private WriteResult writeResult(HttpResult result, String method, String table) {
        if (result.body().length == 0) {
            return new WriteResult(result.status(), Optional.empty(), result.requestId());
        }
        JsonNode root = json(result, method, table);
        JsonNode record = root.path("result");
        Optional<Record> parsed =
                record.isObject() ? Optional.of(Record.fromJson(record)) : Optional.empty();
        return new WriteResult(result.status(), parsed, result.requestId());
    }

    private static JsonNode json(HttpResult result, String method, String table) {
        try {
            JsonNode node = MAPPER.readTree(result.body());
            if (node == null || node.isMissingNode()) {
                throw new ServiceNowException(
                        method
                                + " "
                                + table
                                + " returned an empty body (request-id "
                                + result.requestId()
                                + ")",
                        null,
                        true);
            }
            return node;
        } catch (IOException e) {
            throw new ServiceNowException(
                    method
                            + " "
                            + table
                            + " returned malformed JSON (request-id "
                            + result.requestId()
                            + "): "
                            + e.getMessage(),
                    e,
                    true);
        }
    }

    private static String toJson(Map<String, ?> fields) {
        try {
            return MAPPER.writeValueAsString(fields == null ? Map.of() : fields);
        } catch (JsonProcessingException e) {
            throw new ServiceNowException(
                    "Cannot serialise request body: " + e.getOriginalMessage(), e);
        }
    }
}
