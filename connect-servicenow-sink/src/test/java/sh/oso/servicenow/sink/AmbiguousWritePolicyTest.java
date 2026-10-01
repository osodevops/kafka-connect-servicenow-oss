package sh.oso.servicenow.sink;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.common.RetryExhaustedException;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.config.CoreConfigDefs;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

/**
 * Timeout after the write was applied, for every ambiguous-create mode and for PATCH and DELETE.
 */
class AmbiguousWritePolicyTest {

    private static final Duration DELAY = Duration.ofMillis(1500);
    private MockServiceNowServer snow;
    private ServiceNowClient client;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        snow.close();
    }

    private AmbiguousWritePolicy policy(String mode, Map<String, String> extra) {
        Map<String, String> p = new HashMap<>(TestSupport.props(snow));
        p.put(CoreConfigDefs.HTTP_REQUEST_TIMEOUT_MS, "300");
        p.put(SinkConfig.CREATE_AMBIGUOUS_BEHAVIOR, mode);
        p.putAll(extra);
        SinkConfig config = new SinkConfig(p);
        client = ServiceNowClient.create(config.coreConfig());
        return new AmbiguousWritePolicy(config, client.tableApi());
    }

    private static Map<String, Object> body() {
        return Map.of("short_description", "x", "correlation_id", "ORD-1");
    }

    @Test
    void retryResendsThePostAndAcceptsADuplicate() {
        snow.faults().timeoutAfterWrite("incident", DELAY);
        AmbiguousWritePolicy p = policy("retry", Map.of());
        AmbiguousWritePolicy.Result r = p.create("incident", body());
        assertThat(r.status()).isEqualTo(201);
        assertThat(r.operation()).isEqualTo(Operation.CREATE);
        assertThat(r.retries()).isEqualTo(1);
        assertThat(snow.journal().count("POST", "incident")).isEqualTo(2);
        assertThat(snow.tables().size("incident")).isEqualTo(2);
    }

    @Test
    void retryGivesUpAsAmbiguousAfterTheAttemptCap() {
        snow.faults().timeoutAfterWrite("incident", DELAY, 10);
        AmbiguousWritePolicy p = policy("retry", Map.of(CoreConfigDefs.RETRY_MAX_ATTEMPTS, "2"));
        assertThatThrownBy(() -> p.create("incident", body()))
                .isInstanceOf(RecordError.class)
                .satisfies(
                        e ->
                                assertThat(((RecordError) e).classification())
                                        .isEqualTo(Classification.AMBIGUOUS))
                .satisfies(e -> assertThat(((RecordError) e).retries()).isEqualTo(1));
        assertThat(snow.journal().count("POST", "incident")).isEqualTo(2);
    }

    @Test
    void failAmbiguousDoesNotRetryAndClassifiesAmbiguous() {
        snow.faults().timeoutAfterWrite("incident", DELAY);
        AmbiguousWritePolicy p = policy("fail_ambiguous", Map.of());
        assertThatThrownBy(() -> p.create("incident", body()))
                .isInstanceOf(RecordError.class)
                .satisfies(
                        e ->
                                assertThat(((RecordError) e).classification())
                                        .isEqualTo(Classification.AMBIGUOUS))
                .satisfies(e -> assertThat(((RecordError) e).retries()).isZero())
                .hasMessageContaining("may or may not exist");
        assertThat(snow.journal().count("POST", "incident")).isEqualTo(1);
        assertThat(snow.tables().size("incident")).isEqualTo(1);
    }

    @Test
    void correlationLookupWithNoMatchCreates() {
        AmbiguousWritePolicy p =
                policy(
                        "correlation_lookup",
                        Map.of(SinkConfig.CORRELATION_FIELD, "correlation_id"));
        AmbiguousWritePolicy.Result r = p.create("incident", body());
        assertThat(r.operation()).isEqualTo(Operation.CREATE);
        assertThat(r.status()).isEqualTo(201);
        assertThat(snow.journal().count("GET", "incident")).isEqualTo(1);
        assertThat(snow.journal().entries().get(0).path())
                .contains("sysparm_query=correlation_id%3DORD-1")
                .contains("sysparm_fields=sys_id")
                .contains("sysparm_limit=2");
        assertThat(snow.tables().size("incident")).isEqualTo(1);
    }

    @Test
    void correlationLookupAfterAmbiguousPostFindsOneRowAndPatchesIt() {
        snow.faults().timeoutAfterWrite("incident", DELAY);
        AmbiguousWritePolicy p =
                policy(
                        "correlation_lookup",
                        Map.of(SinkConfig.CORRELATION_FIELD, "correlation_id"));
        AmbiguousWritePolicy.Result r = p.create("incident", body());
        assertThat(r.operation()).isEqualTo(Operation.PATCH);
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.retries()).isEqualTo(1);
        assertThat(snow.tables().size("incident")).isEqualTo(1);
        assertThat(snow.tables().all("incident").get(0).get("sys_id")).isEqualTo(r.sysId());
        assertThat(snow.journal().count("POST", "incident")).isEqualTo(1);
        assertThat(snow.journal().count("PATCH", "incident")).isEqualTo(1);
        assertThat(snow.journal().count("GET", "incident")).isEqualTo(2);
    }

    @Test
    void correlationLookupAfterAmbiguousPostWithNoRowRetriesThePost() {
        // The first POST fails after sending without the fake applying it (connection fault);
        // the lookup then finds nothing and the POST is re-sent exactly once.
        snow.wireMock()
                .stubFor(
                        post(urlPathEqualTo("/api/now/table/incident"))
                                .atPriority(1)
                                .inScenario("lost-post")
                                .whenScenarioStateIs(Scenario.STARTED)
                                .willReturn(aResponse().withFault(Fault.MALFORMED_RESPONSE_CHUNK))
                                .willSetStateTo("recovered"));
        AmbiguousWritePolicy p =
                policy(
                        "correlation_lookup",
                        Map.of(SinkConfig.CORRELATION_FIELD, "correlation_id"));
        AmbiguousWritePolicy.Result r = p.create("incident", body());
        assertThat(r.operation()).isEqualTo(Operation.CREATE);
        assertThat(r.retries()).isEqualTo(1);
        assertThat(snow.tables().size("incident")).isEqualTo(1);
        assertThat(snow.journal().count("POST", "incident")).isEqualTo(1);
        assertThat(snow.journal().count("GET", "incident")).isEqualTo(2);
    }

    @Test
    void correlationLookupWithTwoMatchesIsAmbiguousWithoutWriting() {
        snow.tables().insert("incident", Map.of("correlation_id", "ORD-1"));
        snow.tables().insert("incident", Map.of("correlation_id", "ORD-1"));
        AmbiguousWritePolicy p =
                policy(
                        "correlation_lookup",
                        Map.of(SinkConfig.CORRELATION_FIELD, "correlation_id"));
        assertThatThrownBy(() -> p.create("incident", body()))
                .isInstanceOf(RecordError.class)
                .satisfies(
                        e ->
                                assertThat(((RecordError) e).classification())
                                        .isEqualTo(Classification.AMBIGUOUS))
                .hasMessageContaining("matched 2 rows");
        assertThat(snow.journal().count("POST", "incident")).isZero();
        assertThat(snow.tables().size("incident")).isEqualTo(2);
    }

    @Test
    void correlationLookupRequiresTheFieldInThePayload() {
        AmbiguousWritePolicy p =
                policy(
                        "correlation_lookup",
                        Map.of(SinkConfig.CORRELATION_FIELD, "correlation_id"));
        assertThatThrownBy(() -> p.create("incident", Map.of("short_description", "x")))
                .isInstanceOf(RecordError.class)
                .satisfies(
                        e ->
                                assertThat(((RecordError) e).classification())
                                        .isEqualTo(Classification.RECORD_ERROR))
                .hasMessageContaining("correlation_id");
        assertThat(snow.journal().entries()).isEmpty();
    }

    @Test
    void patchTimeoutIsRetriedByTheCoreAndLeavesASingleRow() {
        String id = snow.tables().insert("incident", Map.of("state", "1"));
        snow.faults().timeoutAfterWrite("incident", DELAY);
        AmbiguousWritePolicy p = policy("fail_ambiguous", Map.of());
        AmbiguousWritePolicy.Result r =
                p.update(Operation.PATCH, "incident", id, Map.of("state", "2"));
        assertThat(r.status()).isEqualTo(200);
        assertThat(snow.journal().count("PATCH", "incident")).isEqualTo(2);
        assertThat(snow.tables().size("incident")).isEqualTo(1);
        assertThat(snow.tables().get("incident", id).orElseThrow().get("state")).isEqualTo("2");
        AmbiguousWritePolicy.Result put =
                p.update(Operation.PUT, "incident", id, Map.of("state", "3"));
        assertThat(put.operation()).isEqualTo(Operation.PUT);
        assertThat(snow.tables().get("incident", id).orElseThrow().get("state")).isEqualTo("3");
    }

    @Test
    void deleteRetryHitting404IsSuccess() {
        String id = snow.tables().insert("incident", Map.of("state", "1"));
        snow.faults().timeoutAfterWrite("incident", DELAY);
        AmbiguousWritePolicy p = policy("fail_ambiguous", Map.of());
        AmbiguousWritePolicy.Result r = p.delete("incident", id);
        assertThat(r.operation()).isEqualTo(Operation.DELETE);
        assertThat(r.status()).isEqualTo(404);
        assertThat(r.retries()).isEqualTo(1);
        assertThat(snow.journal().count("DELETE", "incident")).isEqualTo(2);
        assertThat(snow.tables().exists("incident", id)).isFalse();
    }

    @Test
    void deleteWithoutAmbiguitySurfaces404() {
        AmbiguousWritePolicy p = policy("fail_ambiguous", Map.of());
        assertThatThrownBy(() -> p.delete("incident", "0123456789abcdef0123456789abcdef"))
                .isInstanceOf(ServiceNowApiException.class)
                .satisfies(e -> assertThat(((ServiceNowApiException) e).isNotFound()).isTrue());
        assertThat(snow.journal().count("DELETE", "incident")).isEqualTo(1);
    }

    @Test
    void deleteThatStaysAmbiguousExhaustsAsRetryable() {
        // Every DELETE fails after sending without reaching the fake's store.
        snow.wireMock()
                .stubFor(
                        delete(urlPathMatching("/api/now/table/incident/.*"))
                                .atPriority(1)
                                .willReturn(aResponse().withFault(Fault.MALFORMED_RESPONSE_CHUNK)));
        AmbiguousWritePolicy p =
                policy("fail_ambiguous", Map.of(CoreConfigDefs.RETRY_MAX_ATTEMPTS, "2"));
        assertThatThrownBy(() -> p.delete("incident", "0123456789abcdef0123456789abcdef"))
                .isInstanceOf(RetryExhaustedException.class)
                .satisfies(e -> assertThat(((RetryExhaustedException) e).isRetryable()).isTrue());
        assertThat(
                        snow.wireMock()
                                .countRequestsMatching(
                                        deleteRequestedFor(
                                                        urlPathMatching(
                                                                "/api/now/table/incident/.*"))
                                                .build())
                                .getCount())
                .isEqualTo(2);
    }
}
