package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.sink.SinkRecord;
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
import sh.oso.servicenow.table.EncodedQuery;
import sh.oso.servicenow.table.GetOptions;
import sh.oso.servicenow.table.Page;
import sh.oso.servicenow.table.QueryRequest;
import sh.oso.servicenow.table.Record;
import sh.oso.servicenow.table.TableApiClient;

/**
 * Sink CRUD against a real instance ({@code SNOW_URL}, {@code SNOW_USERNAME}, {@code
 * SNOW_PASSWORD}): create, patch, put, get and delete on {@code incident} through the task. Rows
 * carry a UUID marker in {@code short_description} and are deleted afterwards. Runs only under
 * {@code -Pcontract}.
 */
@EnabledIfEnvironmentVariable(named = "SNOW_URL", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SinkContractIT {

    private static final String MARKER =
            "kafka-connect-servicenow sink contract " + UUID.randomUUID();
    private static final String TOPIC = "contract-incidents";

    private static ServiceNowClient client;
    private static TableApiClient api;
    private static ServiceNowSinkTask task;
    private static MockProducer<byte[], byte[]> reports;
    private static String sysId;

    private static Map<String, String> props() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(CoreConfigDefs.URL, System.getenv("SNOW_URL"));
        p.put(CoreConfigDefs.AUTH_TYPE, CoreConfigDefs.AUTH_TYPE_BASIC);
        p.put(CoreConfigDefs.AUTH_USERNAME, System.getenv("SNOW_USERNAME"));
        p.put(CoreConfigDefs.AUTH_PASSWORD, System.getenv("SNOW_PASSWORD"));
        p.put(SinkConfig.TABLE, "incident");
        p.put(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "fail");
        p.put(SinkConfig.REPORTER_BOOTSTRAP_SERVERS, "unused:9092");
        p.put(SinkConfig.REPORTER_SUCCESS_TOPIC, "ok");
        return p;
    }

    @BeforeAll
    static void connect() {
        client = ServiceNowClient.create(CoreConfig.fromProps(props()));
        api = client.tableApi();
        reports = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        task = new ServiceNowSinkTask(p -> reports);
        task.start(props());
    }

    @AfterAll
    static void cleanUp() {
        try {
            if (task != null) {
                task.stop();
            }
            if (api != null) {
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
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static SinkRecord record(long offset, String key, Object value) {
        return TestSupport.record(TOPIC, 0, offset, key, value, null);
    }

    @Test
    @Order(1)
    void createInsertsAnIncidentAndReportsItsSysId() {
        task.put(List.of(record(0, null, Map.of("short_description", MARKER, "urgency", "3"))));
        assertThat(reports.history()).hasSize(1);
        sysId = new String(reports.history().get(0).key(), StandardCharsets.UTF_8);
        assertThat(sysId).matches("[0-9a-f]{32}");
        Optional<Record> row = api.get("incident", sysId, GetOptions.defaults());
        assertThat(row).isPresent();
        assertThat(row.get().string("short_description")).isEqualTo(MARKER);
    }

    @Test
    @Order(2)
    void patchUpdatesOnlyTheGivenField() {
        task.put(List.of(record(1, sysId, Map.of("urgency", "2"))));
        Record row = api.get("incident", sysId, GetOptions.defaults()).orElseThrow();
        assertThat(row.string("urgency")).isEqualTo("2");
        assertThat(row.string("short_description")).isEqualTo(MARKER);
    }

    @Test
    @Order(3)
    void putUpdatesTheRow() {
        task.put(
                List.of(
                        TestSupport.record(
                                TOPIC,
                                0,
                                2,
                                sysId,
                                Map.of("short_description", MARKER, "urgency", "1"),
                                TestSupport.headers("snow.operation", "PUT"))));
        Record row = api.get("incident", sysId, GetOptions.defaults()).orElseThrow();
        assertThat(row.string("urgency")).isEqualTo("1");
    }

    @Test
    @Order(4)
    void tombstoneDeletesTheRow() {
        task.put(List.of(record(3, sysId, null)));
        assertThat(api.get("incident", sysId, GetOptions.defaults())).isEmpty();
    }
}
