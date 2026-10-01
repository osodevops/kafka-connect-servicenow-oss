package sh.oso.servicenow.table;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.config.CoreConfig;
import sh.oso.servicenow.config.CoreConfigDefs;

/**
 * The six Table API operations against a real instance ({@code SNOW_URL}, {@code SNOW_USERNAME},
 * {@code SNOW_PASSWORD}). Scratch {@code incident} rows carry a UUID marker in {@code
 * short_description} and are deleted afterwards. Runs only under {@code -Pcontract}.
 */
@EnabledIfEnvironmentVariable(named = "SNOW_URL", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TableApiContractIT {

    private static final String MARKER = "kafka-connect-servicenow contract " + UUID.randomUUID();
    private static ServiceNowClient client;
    private static TableApiClient api;
    private static String sysId;

    @BeforeAll
    static void connect() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(CoreConfigDefs.URL, System.getenv("SNOW_URL"));
        props.put(CoreConfigDefs.AUTH_TYPE, CoreConfigDefs.AUTH_TYPE_BASIC);
        props.put(CoreConfigDefs.AUTH_USERNAME, System.getenv("SNOW_USERNAME"));
        props.put(CoreConfigDefs.AUTH_PASSWORD, System.getenv("SNOW_PASSWORD"));
        client = ServiceNowClient.create(CoreConfig.fromProps(props));
        api = client.tableApi();
    }

    @AfterAll
    static void cleanUp() {
        if (api == null) {
            return;
        }
        try {
            Page<Record> leftovers =
                    api.list(
                            QueryRequest.builder("incident")
                                    .query(EncodedQuery.of("short_descriptionLIKE" + MARKER))
                                    .fields(List.of("sys_id"))
                                    .limit(100)
                                    .build());
            for (Record r : leftovers.items()) {
                api.delete("incident", r.sysId());
            }
        } finally {
            client.close();
        }
    }

    @Test
    @Order(1)
    void createInsertsAScratchIncident() {
        WriteResult r = api.create("incident", Map.of("short_description", MARKER, "urgency", "3"));
        assertThat(r.status()).isEqualTo(201);
        sysId = r.sysId().orElseThrow();
        assertThat(sysId).matches("[0-9a-f]{32}");
    }

    @Test
    @Order(2)
    void getReturnsTheRow() {
        Optional<Record> row =
                api.get(
                        "incident",
                        sysId,
                        GetOptions.defaults()
                                .withFields(
                                        List.of("sys_id", "short_description", "sys_updated_on")));
        assertThat(row).isPresent();
        assertThat(row.get().string("short_description")).isEqualTo(MARKER);
        assertThat(row.get().string("sys_updated_on"))
                .matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
    }

    @Test
    @Order(3)
    void listFindsTheRowByEncodedQuery() {
        Page<Record> page =
                api.list(
                        QueryRequest.builder("incident")
                                .query(
                                        EncodedQuery.of("short_description=" + MARKER)
                                                .orderBy("sys_updated_on")
                                                .orderBy("sys_id"))
                                .fields(List.of("sys_id", "sys_updated_on", "sys_mod_count"))
                                .limit(10)
                                .build());
        assertThat(page.items()).extracting(Record::sysId).containsExactly(sysId);
    }

    @Test
    @Order(4)
    void patchUpdatesOnlyTheGivenField() {
        Record r = api.patch("incident", sysId, Map.of("urgency", "2")).record().orElseThrow();
        assertThat(r.string("urgency")).isEqualTo("2");
        assertThat(r.string("short_description")).isEqualTo(MARKER);
    }

    @Test
    @Order(5)
    void putUpdatesTheRow() {
        Record r =
                api.put("incident", sysId, Map.of("short_description", MARKER, "urgency", "1"))
                        .record()
                        .orElseThrow();
        assertThat(r.string("urgency")).isEqualTo("1");
    }

    @Test
    @Order(6)
    void deleteRemovesTheRow() {
        assertThat(api.delete("incident", sysId).status()).isEqualTo(204);
        assertThat(api.get("incident", sysId, GetOptions.defaults())).isEmpty();
    }
}
