package sh.oso.servicenow.testing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.http.QueryParameter;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import sh.oso.servicenow.table.PathSegments;

/**
 * WireMock transformer implementing the fake's {@code /oauth_token.do} and the stateful Table API
 * over {@link TableStore} with {@link FaultInjector}, {@link RequestJournal} and {@link
 * MutableClock}. Pattern after the Salesforce fake's Bulk API transformer.
 */
final class TableApiTransformer implements ResponseDefinitionTransformerV2 {

    static final String NAME = "snow-table-api";
    static final Set<String> REFERENCE_FIELDS = Set.of("assigned_to", "caller_id", "cmdb_ci");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern TABLE_ROUTE =
            Pattern.compile("^/api/now/table/([^/?]+)(?:/([^/?]+))?/?(?:\\?.*)?$");
    private static final DateTimeFormatter HTTP_DATE =
            DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneOffset.UTC);

    private final TableStore store;
    private final FaultInjector faults;
    private final RequestJournal journal;
    private final OAuthTokenStore oauth;
    private final Clock clock;
    private final Supplier<String> baseUrl;
    private final String basicUser;
    private final String basicPassword;
    private final String clientId;
    private final String clientSecret;

    TableApiTransformer(
            TableStore store,
            FaultInjector faults,
            RequestJournal journal,
            OAuthTokenStore oauth,
            Clock clock,
            Supplier<String> baseUrl,
            String basicUser,
            String basicPassword,
            String clientId,
            String clientSecret) {
        this.store = store;
        this.faults = faults;
        this.journal = journal;
        this.oauth = oauth;
        this.clock = clock;
        this.baseUrl = baseUrl;
        this.basicUser = basicUser;
        this.basicPassword = basicPassword;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean applyGlobally() {
        return false;
    }

    @Override
    public ResponseDefinition transform(ServeEvent serveEvent) {
        Request request = serveEvent.getRequest();
        String url = request.getUrl();
        String requestId =
                Optional.ofNullable(request.getHeader("X-Request-Id"))
                        .orElse(UUID.randomUUID().toString());
        if (url.startsWith("/oauth_token.do")) {
            return token(request, requestId);
        }
        Matcher m = TABLE_ROUTE.matcher(url);
        if (!m.matches()) {
            return error(404, "Not found: " + url, null, requestId);
        }
        journal.enter();
        try {
            sleep(faults.latency());
            return table(request, m.group(1), m.group(2), requestId);
        } finally {
            journal.exit();
        }
    }

    // ---------------------------------------------------------------- OAuth

    private ResponseDefinition token(Request request, String requestId) {
        journal.tokenRequested();
        Map<String, String> form = parseForm(request.getBodyAsString());
        String grant = form.getOrDefault("grant_type", "");
        if (!clientId.equals(form.get("client_id"))
                || !clientSecret.equals(form.get("client_secret"))) {
            return oauthError(401, "invalid_client", "Client authentication failed", requestId);
        }
        switch (grant) {
            case "client_credentials" -> {
                return issue(requestId);
            }
            case "password" -> {
                if (!basicUser.equals(form.get("username"))
                        || !basicPassword.equals(form.get("password"))) {
                    return oauthError(401, "access_denied", "access_denied", requestId);
                }
                return issue(requestId);
            }
            case "refresh_token" -> {
                if (!oauth.knowsRefreshToken(form.get("refresh_token"))) {
                    return oauthError(401, "invalid_grant", "Refresh token is invalid", requestId);
                }
                return issue(requestId);
            }
            default -> {
                return oauthError(
                        400, "unsupported_grant_type", "grant_type is not supported", requestId);
            }
        }
    }

    private ResponseDefinition issue(String requestId) {
        OAuthTokenStore.Issued issued = oauth.issue();
        ObjectNode body = MAPPER.createObjectNode();
        body.put("access_token", issued.accessToken());
        body.put("refresh_token", issued.refreshToken());
        body.put("scope", "useraccount");
        body.put("token_type", "Bearer");
        body.put("expires_in", issued.expiresIn());
        return json(200, body.toString(), requestId).build();
    }

    private ResponseDefinition oauthError(
            int status, String error, String description, String requestId) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("error", error);
        body.put("error_description", description);
        return json(status, body.toString(), requestId).build();
    }

    // ------------------------------------------------------------- Table API

    private ResponseDefinition table(
            Request request, String table, String sysId, String requestId) {
        String method = request.getMethod().getName();
        if (!authorised(request)) {
            return error(
                    401,
                    "User Not Authenticated",
                    "Required to provide Auth information",
                    requestId);
        }
        if (faults.takeUnauthorized()) {
            return error(401, "User Not Authenticated", "Injected 401", requestId);
        }
        if (!PathSegments.isTable(table)) {
            return error(400, "Invalid table " + table, null, requestId);
        }
        if (faults.isForbidden(table)) {
            return error(
                    403,
                    "Insufficient rights to query records",
                    "Field(s) present in the query do not have permission to be read",
                    requestId);
        }
        if (faults.isNotFound(table)) {
            return error(404, "Invalid table " + table, null, requestId);
        }
        Optional<Duration> rateLimit = faults.takeRateLimit();
        if (rateLimit.isPresent()) {
            return json(429, errorBody("Too many requests", "Rate limit exceeded"), requestId)
                    .withHeader("Retry-After", Long.toString(rateLimit.get().toSeconds()))
                    .build();
        }
        OptionalInt serverError = faults.takeServerError();
        if (serverError.isPresent()) {
            return error(serverError.getAsInt(), "Injected server error", null, requestId);
        }
        if (faults.takeMalformedJson()) {
            return json(200, "{\"result\": [{\"sys_id\": \"abc", requestId).build();
        }
        if (faults.takeTruncatedBody()) {
            return ResponseDefinitionBuilder.responseDefinition()
                    .withFault(Fault.MALFORMED_RESPONSE_CHUNK)
                    .build();
        }
        if (sysId != null && !PathSegments.isSysId(sysId)) {
            return error(
                    404,
                    "No Record found",
                    "Record doesn't exist or ACL restricts the record retrieval",
                    requestId);
        }
        String body = request.getBodyAsString();
        journal.record(
                new RequestJournal.Entry(
                        method, request.getUrl(), table, sysId, body, requestId, clock.instant()));
        try {
            return switch (method) {
                case "GET" ->
                        sysId == null
                                ? list(request, table, requestId)
                                : getOne(request, table, sysId, requestId);
                case "POST" ->
                        sysId == null
                                ? create(request, table, requestId)
                                : error(405, "Method not allowed", null, requestId);
                case "PATCH" ->
                        sysId == null
                                ? error(405, "Method not allowed", null, requestId)
                                : patch(request, table, sysId, requestId);
                case "PUT" ->
                        sysId == null
                                ? error(405, "Method not allowed", null, requestId)
                                : put(request, table, sysId, requestId);
                case "DELETE" ->
                        sysId == null
                                ? error(405, "Method not allowed", null, requestId)
                                : delete(table, sysId, requestId);
                default -> error(405, "Method not allowed", null, requestId);
            };
        } catch (BadRequest e) {
            return error(400, e.getMessage(), null, requestId);
        }
    }

    private ResponseDefinition list(Request request, String table, String requestId) {
        String query = param(request, "sysparm_query").orElse("");
        List<Map<String, String>> rows = store.all(table);
        Predicate<Map<String, String>> predicate;
        try {
            predicate = EncodedQueryEvaluator.compile(query);
        } catch (InvalidQueryException e) {
            if (EncodedQueryEvaluator.invalidQueryReturnsNoRows) {
                predicate = row -> false;
            } else {
                predicate = EncodedQueryEvaluator.compileLenient(query);
            }
        }
        Comparator<Map<String, String>> ordering = EncodedQueryEvaluator.ordering(query);
        List<Map<String, String>> matched = new ArrayList<>();
        for (Map<String, String> row : rows) {
            if (predicate.test(row)) {
                matched.add(row);
            }
        }
        matched.sort(ordering);
        int offset = param(request, "sysparm_offset").map(Integer::parseInt).orElse(0);
        int limit = param(request, "sysparm_limit").map(Integer::parseInt).orElse(10000);
        ArrayNode result = MAPPER.createArrayNode();
        for (int i = Math.max(0, offset); i < matched.size() && result.size() < limit; i++) {
            result.add(shape(matched.get(i), table, request));
        }
        ObjectNode root = MAPPER.createObjectNode();
        root.set("result", result);
        return json(200, root.toString(), requestId).build();
    }

    private ResponseDefinition getOne(
            Request request, String table, String sysId, String requestId) {
        Optional<Map<String, String>> row = store.get(table, sysId);
        if (row.isEmpty()) {
            return notFoundRecord(requestId);
        }
        return result(200, shape(row.get(), table, request), requestId);
    }

    private ResponseDefinition create(Request request, String table, String requestId) {
        Map<String, String> fields = parseBody(request.getBodyAsString());
        String sysId = store.insert(table, fields);
        ResponseDefinition response =
                result(
                        201,
                        shape(store.get(table, sysId).orElseThrow(), table, request),
                        requestId);
        faults.takeTimeoutAfterWrite(table).ifPresent(this::sleep);
        return response;
    }

    private ResponseDefinition patch(
            Request request, String table, String sysId, String requestId) {
        if (!store.exists(table, sysId)) {
            return notFoundRecord(requestId);
        }
        store.update(table, sysId, parseBody(request.getBodyAsString()));
        ResponseDefinition response =
                result(
                        200,
                        shape(store.get(table, sysId).orElseThrow(), table, request),
                        requestId);
        faults.takeTimeoutAfterWrite(table).ifPresent(this::sleep);
        return response;
    }

    private ResponseDefinition put(Request request, String table, String sysId, String requestId) {
        if (!store.exists(table, sysId)) {
            return notFoundRecord(requestId);
        }
        store.replace(table, sysId, parseBody(request.getBodyAsString()));
        ResponseDefinition response =
                result(
                        200,
                        shape(store.get(table, sysId).orElseThrow(), table, request),
                        requestId);
        faults.takeTimeoutAfterWrite(table).ifPresent(this::sleep);
        return response;
    }

    private ResponseDefinition delete(String table, String sysId, String requestId) {
        if (!store.delete(table, sysId)) {
            return notFoundRecord(requestId);
        }
        ResponseDefinition response =
                ResponseDefinitionBuilder.responseDefinition()
                        .withStatus(204)
                        .withHeader("Date", HTTP_DATE.format(clock.instant()))
                        .withHeader("X-Request-Id", requestId)
                        .build();
        faults.takeTimeoutAfterWrite(table).ifPresent(this::sleep);
        return response;
    }

    // --------------------------------------------------------------- shaping

    private ObjectNode shape(Map<String, String> row, String table, Request request) {
        String display = param(request, "sysparm_display_value").orElse("false");
        boolean excludeLink =
                param(request, "sysparm_exclude_reference_link")
                        .map(Boolean::parseBoolean)
                        .orElse(false);
        List<String> fields =
                param(request, "sysparm_fields")
                        .filter(s -> !s.isBlank())
                        .map(s -> List.of(s.split(",")))
                        .orElse(List.of());
        Set<String> hidden = faults.hiddenFields(table);
        ObjectNode out = MAPPER.createObjectNode();
        Iterable<String> names = fields.isEmpty() ? row.keySet() : fields;
        for (String name : names) {
            String key = name.trim();
            if (hidden.contains(key) || !row.containsKey(key)) {
                continue;
            }
            String value = row.get(key);
            boolean reference = key.endsWith("_ref") || REFERENCE_FIELDS.contains(key);
            String link = baseUrl.get() + "/api/now/table/" + key + "/" + value;
            switch (display) {
                case "all" -> {
                    ObjectNode rich = MAPPER.createObjectNode();
                    rich.put("display_value", value.isEmpty() ? "" : value + " (display)");
                    rich.put("value", value);
                    if (reference && !excludeLink && !value.isEmpty()) {
                        rich.put("link", link);
                    }
                    out.set(key, rich);
                }
                case "true" -> out.put(key, value.isEmpty() ? "" : value + " (display)");
                default -> {
                    if (reference && !excludeLink && !value.isEmpty()) {
                        ObjectNode ref = MAPPER.createObjectNode();
                        ref.put("link", link);
                        ref.put("value", value);
                        out.set(key, ref);
                    } else {
                        out.put(key, value);
                    }
                }
            }
        }
        return out;
    }

    // --------------------------------------------------------------- helpers

    private boolean authorised(Request request) {
        String auth = request.getHeader("Authorization");
        if (auth == null) {
            return false;
        }
        if (auth.startsWith("Basic ")) {
            try {
                String decoded =
                        new String(
                                Base64.getDecoder().decode(auth.substring(6)),
                                StandardCharsets.UTF_8);
                return decoded.equals(basicUser + ":" + basicPassword);
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
        if (auth.startsWith("Bearer ")) {
            return oauth.isValid(auth.substring(7));
        }
        return false;
    }

    private static final class BadRequest extends RuntimeException {
        BadRequest(String message) {
            super(message);
        }
    }

    private static Map<String, String> parseBody(String body) {
        if (body == null || body.isBlank()) {
            return Map.of();
        }
        JsonNode node;
        try {
            node = MAPPER.readTree(body);
        } catch (Exception e) {
            throw new BadRequest("Invalid JSON body");
        }
        if (!node.isObject()) {
            throw new BadRequest("Request body must be a JSON object");
        }
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode v = e.getValue();
            if (v.isNull()) {
                out.put(e.getKey(), "");
            } else if (v.isTextual()) {
                out.put(e.getKey(), v.asText());
            } else if (v.isContainerNode()) {
                throw new BadRequest("Field '" + e.getKey() + "' must be a scalar");
            } else {
                out.put(e.getKey(), v.asText());
            }
        }
        return out;
    }

    private static Map<String, String> parseForm(String body) {
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        if (body == null || body.isBlank()) {
            return out;
        }
        for (String pair : body.split("&")) {
            int eq = pair.indexOf('=');
            String k = eq < 0 ? pair : pair.substring(0, eq);
            String v = eq < 0 ? "" : pair.substring(eq + 1);
            out.put(
                    URLDecoder.decode(k, StandardCharsets.UTF_8),
                    URLDecoder.decode(v, StandardCharsets.UTF_8));
        }
        return out;
    }

    private static Optional<String> param(Request request, String name) {
        QueryParameter p = request.queryParameter(name);
        return p != null && p.isPresent() ? Optional.ofNullable(p.firstValue()) : Optional.empty();
    }

    private ResponseDefinition result(int status, JsonNode record, String requestId) {
        ObjectNode root = MAPPER.createObjectNode();
        root.set("result", record);
        return json(status, root.toString(), requestId).build();
    }

    private ResponseDefinition notFoundRecord(String requestId) {
        return error(
                404,
                "No Record found",
                "Record doesn't exist or ACL restricts the record retrieval",
                requestId);
    }

    private ResponseDefinition error(int status, String message, String detail, String requestId) {
        return json(status, errorBody(message, detail), requestId).build();
    }

    private static String errorBody(String message, String detail) {
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode error = root.putObject("error");
        error.put("message", message);
        if (detail == null) {
            error.putNull("detail");
        } else {
            error.put("detail", detail);
        }
        root.put("status", "failure");
        return root.toString();
    }

    private ResponseDefinitionBuilder json(int status, String body, String requestId) {
        return ResponseDefinitionBuilder.responseDefinition()
                .withStatus(status)
                .withHeader("Content-Type", "application/json;charset=UTF-8")
                .withHeader("Date", HTTP_DATE.format(clock.instant()))
                .withHeader("X-Request-Id", requestId)
                .withBody(body);
    }

    private void sleep(Duration d) {
        if (d == null || d.isZero() || d.isNegative()) {
            return;
        }
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
