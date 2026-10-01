package sh.oso.servicenow.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.table.DisplayValue;
import sh.oso.servicenow.table.GetOptions;
import sh.oso.servicenow.table.Page;
import sh.oso.servicenow.table.QueryRequest;
import sh.oso.servicenow.table.Record;
import sh.oso.servicenow.table.TableApiClient;
import sh.oso.servicenow.table.WriteResult;

/** Self-test of the fake ServiceNow. */
class MockServiceNowServerTest {

    private MockServiceNowServer snow;
    private TableApiClient api;
    private final HttpClient raw = HttpClient.newHttpClient();

    @BeforeEach
    void setUp() {
        snow =
                MockServiceNowServer.start(
                        0, MutableClock.at(Instant.parse("2026-09-29T07:30:40Z")));
        api = snow.client().tableApi();
    }

    @AfterEach
    void tearDown() {
        snow.close();
    }

    @Test
    void insertSetsSystemFieldsFromTheClock() {
        String id = snow.tables().insert("incident", Map.of("short_description", "a"));
        Map<String, String> row = snow.tables().get("incident", id).orElseThrow();
        assertThat(id).matches("[0-9a-f]{32}");
        assertThat(row)
                .containsEntry("sys_created_on", "2026-09-29 07:30:40")
                .containsEntry("sys_updated_on", "2026-09-29 07:30:40")
                .containsEntry("sys_mod_count", "0")
                .containsEntry("sys_created_by", "connect");
    }

    @Test
    void writesAdvanceUpdatedOnAndModCount() {
        String id = snow.tables().insert("incident", Map.of("short_description", "a"));
        snow.clock().advance(Duration.ofSeconds(5));
        WriteResult r = api.patch("incident", id, Map.of("short_description", "b"));
        assertThat(r.status()).isEqualTo(200);
        Record rec = r.record().orElseThrow();
        assertThat(rec.string("sys_updated_on")).isEqualTo("2026-09-29 07:30:45");
        assertThat(rec.string("sys_mod_count")).isEqualTo("1");
        assertThat(rec.string("short_description")).isEqualTo("b");
    }

    @Test
    void putClearsUnsuppliedUserFieldsButKeepsSystemFields() {
        String id =
                snow.tables().insert("incident", Map.of("short_description", "a", "state", "1"));
        Record rec = api.put("incident", id, Map.of("state", "2")).record().orElseThrow();
        assertThat(rec.string("short_description")).isEmpty();
        assertThat(rec.string("state")).isEqualTo("2");
        assertThat(rec.string("sys_created_on")).isEqualTo("2026-09-29 07:30:40");
        assertThat(rec.string("sys_mod_count")).isEqualTo("1");
    }

    @Test
    void deleteAnswers204ThenNotFound() {
        String id = snow.tables().insert("incident", Map.of());
        assertThat(api.delete("incident", id).status()).isEqualTo(204);
        assertThatThrownBy(() -> api.delete("incident", id))
                .isInstanceOf(ServiceNowApiException.class)
                .satisfies(e -> assertThat(((ServiceNowApiException) e).isNotFound()).isTrue());
    }

    @Test
    void listAppliesQueryOrderingProjectionOffsetAndLimit() {
        for (int i = 0; i < 10; i++) {
            snow.tables()
                    .insert(
                            "incident",
                            Map.of("number", "INC" + i, "state", i % 2 == 0 ? "1" : "2"));
        }
        Page<Record> page =
                api.list(
                        QueryRequest.builder("incident")
                                .query("state=1^ORDERBYDESCnumber")
                                .fields(List.of("number", "sys_id"))
                                .limit(2)
                                .offset(1)
                                .build());
        assertThat(page.items())
                .extracting(r -> r.string("number"))
                .containsExactly("INC6", "INC4");
        assertThat(page.items().get(0).fieldNames()).containsExactly("number", "sys_id");
        assertThat(page.isFull()).isTrue();
    }

    @Test
    void displayValueAllWrapsEveryFieldAndLinksReferences() {
        String id = snow.tables().insert("incident", Map.of("caller_id", "abc", "state", "1"));
        Record rec =
                api.get("incident", id, new GetOptions(List.of(), DisplayValue.ALL, false))
                        .orElseThrow();
        assertThat(rec.field("state").displayValue()).isEqualTo("1 (display)");
        assertThat(rec.field("state").link()).isNull();
        assertThat(rec.field("caller_id").link())
                .isEqualTo(snow.baseUrl() + "/api/now/table/caller_id/abc");
        Record plain =
                api.get("incident", id, new GetOptions(List.of(), DisplayValue.FALSE, false))
                        .orElseThrow();
        assertThat(plain.field("caller_id").link()).isNotNull();
        assertThat(plain.field("caller_id").displayValue()).isNull();
        Record excluded = api.get("incident", id, GetOptions.defaults()).orElseThrow();
        assertThat(excluded.field("caller_id").isRich()).isFalse();
    }

    @Test
    void wrongBasicPasswordIs401WithServiceNowErrorBody() throws Exception {
        HttpResponse<String> response =
                rawGet("/api/now/table/incident", "Basic " + basic("connect", "nope"));
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body())
                .contains("\"status\":\"failure\"")
                .contains("User Not Authenticated");
    }

    @Test
    void everyResponseCarriesDateFromTheClockAndEchoesRequestId() throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(URI.create(snow.baseUrl() + "/api/now/table/incident"))
                        .header("Authorization", "Basic " + basic("connect", "secret"))
                        .header("X-Request-Id", "req-123")
                        .GET()
                        .build();
        HttpResponse<String> response = raw.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.headers().firstValue("X-Request-Id")).contains("req-123");
        assertThat(response.headers().firstValue("Date")).contains("Tue, 29 Sep 2026 07:30:40 GMT");
    }

    @Test
    void oauthTokensExpireWithTheClock() throws Exception {
        snow.oauth().expiresIn(60);
        HttpResponse<String> token =
                raw.send(
                        HttpRequest.newBuilder(URI.create(snow.baseUrl() + "/oauth_token.do"))
                                .header("Content-Type", "application/x-www-form-urlencoded")
                                .POST(
                                        HttpRequest.BodyPublishers.ofString(
                                                "grant_type=client_credentials&client_id=client-id&client_secret=client-secret"))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(token.statusCode()).isEqualTo(200);
        String access = token.body().replaceAll(".*\"access_token\":\"([^\"]+)\".*", "$1");
        assertThat(rawGet("/api/now/table/incident", "Bearer " + access).statusCode())
                .isEqualTo(200);
        snow.clock().advance(Duration.ofSeconds(61));
        assertThat(rawGet("/api/now/table/incident", "Bearer " + access).statusCode())
                .isEqualTo(401);
        assertThat(snow.journal().tokenRequests()).isEqualTo(1);
    }

    @Test
    void oauthRejectsBadClientAndBadUserCredentials() throws Exception {
        assertThat(
                        tokenStatus(
                                "grant_type=client_credentials&client_id=client-id&client_secret=wrong"))
                .isEqualTo(401);
        assertThat(
                        tokenStatus(
                                "grant_type=password&client_id=client-id&client_secret=client-secret&username=connect&password=wrong"))
                .isEqualTo(401);
        assertThat(
                        tokenStatus(
                                "grant_type=implicit&client_id=client-id&client_secret=client-secret"))
                .isEqualTo(400);
        assertThat(
                        tokenStatus(
                                "grant_type=refresh_token&client_id=client-id&client_secret=client-secret&refresh_token=nope"))
                .isEqualTo(401);
    }

    @Test
    void journalRecordsBodiesAndInFlightHighWaterMark() {
        String id = snow.tables().insert("incident", Map.of());
        api.patch("incident", id, Map.of("state", "3"));
        assertThat(snow.journal().count("PATCH", "incident")).isEqualTo(1);
        assertThat(snow.journal().bodies("PATCH", "incident", id))
                .containsExactly("{\"state\":\"3\"}");
        assertThat(snow.journal().maxInFlight()).isGreaterThanOrEqualTo(1);
        snow.journal().reset();
        assertThat(snow.journal().entries()).isEmpty();
    }

    @Test
    void faultsHideFieldsAndForbidOrHideTables() {
        String id = snow.tables().insert("incident", Map.of("secret_field", "x", "state", "1"));
        snow.faults().hideField("incident", "secret_field");
        assertThat(api.get("incident", id, GetOptions.defaults()).orElseThrow().has("secret_field"))
                .isFalse();
        snow.faults().forbidTable("incident");
        assertThatThrownBy(() -> api.list(QueryRequest.builder("incident").build()))
                .isInstanceOf(ServiceNowApiException.class)
                .satisfies(e -> assertThat(((ServiceNowApiException) e).isForbidden()).isTrue());
        snow.faults().clear();
        snow.faults().notFoundTable("incident");
        assertThatThrownBy(() -> api.list(QueryRequest.builder("incident").build()))
                .isInstanceOf(ServiceNowApiException.class)
                .satisfies(e -> assertThat(((ServiceNowApiException) e).isNotFound()).isTrue());
    }

    @Test
    void seedGeneratesRowsAndPropsAreReadyToMerge() {
        FakeServiceNowMain.seedIncidents(snow, 25);
        assertThat(snow.tables().size("incident")).isEqualTo(25);
        assertThat(snow.basicAuthProps())
                .containsEntry("snow.url", snow.baseUrl())
                .containsEntry("snow.auth.type", "basic");
        assertThat(snow.oauthClientCredentialsProps())
                .containsEntry("snow.oauth.grant.type", "client_credentials");
        assertThat(snow.oauthPasswordProps())
                .containsEntry("snow.oauth.grant.type", "password")
                .containsEntry("snow.auth.username", "connect");
        assertThat(snow.coreConfig().instanceHost()).isEqualTo("localhost");
    }

    @Test
    void invalidQueryReturnsNoRowsUnlessLenient() {
        snow.tables().insert("incident", Map.of("state", "1"));
        Optional<String> none = Optional.empty();
        assertThat(none).isEmpty();
        assertThat(
                        api.list(
                                        QueryRequest.builder("incident")
                                                .query("state=1^javascript:gs.now()")
                                                .build())
                                .items())
                .isEmpty();
        EncodedQueryEvaluator.invalidQueryReturnsNoRows = false;
        try {
            assertThat(
                            api.list(
                                            QueryRequest.builder("incident")
                                                    .query("state=1^BOGUS")
                                                    .build())
                                    .items())
                    .hasSize(1);
        } finally {
            EncodedQueryEvaluator.invalidQueryReturnsNoRows = true;
        }
    }

    private int tokenStatus(String form) throws Exception {
        return raw.send(
                        HttpRequest.newBuilder(URI.create(snow.baseUrl() + "/oauth_token.do"))
                                .header("Content-Type", "application/x-www-form-urlencoded")
                                .POST(HttpRequest.BodyPublishers.ofString(form))
                                .build(),
                        HttpResponse.BodyHandlers.ofString())
                .statusCode();
    }

    private HttpResponse<String> rawGet(String path, String authorization) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(URI.create(snow.baseUrl() + path))
                        .header("Authorization", authorization)
                        .GET()
                        .build();
        return raw.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String basic(String user, String password) {
        return Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }
}
