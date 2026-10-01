package sh.oso.servicenow.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.testing.MockServiceNowServer;

class TableApiClientTest {

    private static final String SYS_ID = "0123456789abcdef0123456789abcdef";

    private MockServiceNowServer snow;
    private TableApiClient api;

    @BeforeEach
    void setUp() {
        snow = MockServiceNowServer.start();
        api = snow.client().tableApi();
    }

    @AfterEach
    void tearDown() {
        snow.close();
    }

    @Test
    void createReturns201WithGeneratedSysId() {
        java.util.LinkedHashMap<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("short_description", "Printer on fire");
        body.put("urgency", 2);
        WriteResult r = api.create("incident", body);
        assertThat(r.status()).isEqualTo(201);
        assertThat(r.sysId()).isPresent();
        assertThat(r.record().orElseThrow().string("urgency")).isEqualTo("2");
        assertThat(snow.tables().get("incident", r.sysId().get())).isPresent();
        assertThat(snow.journal().bodies("POST", "incident", null))
                .containsExactly("{\"short_description\":\"Printer on fire\",\"urgency\":2}");
    }

    @Test
    void getReturnsTheRecordOrEmptyOn404() {
        String id = snow.tables().insert("incident", Map.of("state", "1"));
        Optional<Record> found =
                api.get(
                        "incident",
                        id,
                        GetOptions.defaults().withFields(List.of("state", "sys_id")));
        assertThat(found).isPresent();
        assertThat(found.get().fieldNames()).containsExactly("state", "sys_id");
        assertThat(api.get("incident", SYS_ID, GetOptions.defaults())).isEmpty();
    }

    @Test
    void listEncodesEverySysparmParameter() {
        snow.tables()
                .insert(
                        "incident",
                        Map.of("active", "true", "sys_updated_on", "2026-09-29 07:30:41"));
        snow.tables()
                .insert(
                        "incident",
                        Map.of("active", "false", "sys_updated_on", "2026-09-29 07:30:41"));
        QueryRequest q =
                QueryRequest.builder("incident")
                        .query(EncodedQuery.of("active=true^sys_updated_on>2026-09-29 07:30:40"))
                        .fields(List.of("sys_id", "active"))
                        .limit(5)
                        .displayValue(DisplayValue.ALL)
                        .excludeReferenceLink(true)
                        .queryCategory("kafka")
                        .queryNoDomain(true)
                        .build();
        Page<Record> page = api.list(q);
        assertThat(page.items()).hasSize(1);
        assertThat(page.limit()).isEqualTo(5);
        assertThat(page.isFull()).isFalse();
        String path = snow.journal().entries().get(0).path();
        assertThat(path)
                .startsWith("/api/now/table/incident?")
                .contains(
                        "sysparm_query=active%3Dtrue%5Esys_updated_on%3E2026-09-29%2007%3A30%3A40")
                .contains("sysparm_fields=sys_id%2Cactive")
                .contains("sysparm_limit=5")
                .contains("sysparm_display_value=all")
                .contains("sysparm_exclude_reference_link=true")
                .contains("sysparm_query_category=kafka")
                .contains("sysparm_query_no_domain=true")
                .contains("sysparm_no_count=true")
                .contains("sysparm_suppress_pagination_header=true")
                .doesNotContain("sysparm_offset");
    }

    @Test
    void patchPutAndDeleteRoundTrip() {
        String id =
                snow.tables().insert("incident", Map.of("state", "1", "short_description", "a"));
        assertThat(
                        api.patch("incident", id, Map.of("state", "2"))
                                .record()
                                .orElseThrow()
                                .string("short_description"))
                .isEqualTo("a");
        assertThat(
                        api.put("incident", id, Map.of("state", "3"))
                                .record()
                                .orElseThrow()
                                .string("short_description"))
                .isEmpty();
        WriteResult deleted = api.delete("incident", id);
        assertThat(deleted.status()).isEqualTo(204);
        assertThat(deleted.record()).isEmpty();
        assertThat(snow.tables().exists("incident", id)).isFalse();
        assertThatThrownBy(() -> api.patch("incident", id, Map.of("state", "4")))
                .isInstanceOf(ServiceNowApiException.class)
                .satisfies(e -> assertThat(((ServiceNowApiException) e).isNotFound()).isTrue());
    }

    @Test
    void invalidTableAndSysIdAreRejectedBeforeAnyRequest() {
        assertThatThrownBy(() -> api.list(QueryRequest.builder("Incident; drop").build()))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("Invalid table name");
        assertThatThrownBy(() -> api.get("incident", "../sys_user", GetOptions.defaults()))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("Invalid sys_id");
        assertThatThrownBy(() -> api.create("../sys_user", Map.of()))
                .hasMessageContaining("Invalid table name");
        assertThatThrownBy(() -> api.delete("incident", "ABCDEF"))
                .hasMessageContaining("Invalid sys_id");
        assertThat(snow.journal().entries()).isEmpty();
        assertThat(snow.wireMock().getAllServeEvents()).isEmpty();
    }

    @Test
    void malformedJsonIsReportedAsRetryable() {
        snow.faults().malformedJsonOnce();
        assertThatThrownBy(() -> api.list(QueryRequest.builder("incident").build()))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("malformed JSON")
                .satisfies(e -> assertThat(((ServiceNowException) e).isRetryable()).isTrue());
    }

    @Test
    void recordParsesPlainRichAndNullValues() throws Exception {
        com.fasterxml.jackson.databind.JsonNode node =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readTree(
                                "{\"a\":\"1\",\"b\":null,\"c\":{\"value\":\"v\",\"display_value\":\"d\",\"link\":\"l\"},\"n\":5}");
        Record r = Record.fromJson(node);
        assertThat(r.string("a")).isEqualTo("1");
        assertThat(r.has("b")).isTrue();
        assertThat(r.string("b")).isNull();
        assertThat(r.field("b").isNull()).isTrue();
        assertThat(r.has("missing")).isFalse();
        assertThat(r.opt("missing")).isEmpty();
        assertThat(r.field("c")).isEqualTo(new FieldValue("v", "d", "l"));
        assertThat(r.display("c")).isEqualTo("d");
        assertThat(r.display("a")).isEqualTo("1");
        assertThat(r.string("n")).isEqualTo("5");
        assertThat(r.fieldNames()).containsExactly("a", "b", "c", "n");
        assertThat(Record.ofStrings(Map.of("x", "y")).string("x")).isEqualTo("y");
        assertThat(r).isEqualTo(Record.fromJson(node)).hasSameHashCodeAs(Record.fromJson(node));
    }
}
