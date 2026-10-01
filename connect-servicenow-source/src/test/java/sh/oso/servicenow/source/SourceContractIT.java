package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.config.CoreConfig;
import sh.oso.servicenow.config.CoreConfigDefs;
import sh.oso.servicenow.cursor.SnowTimestamp;
import sh.oso.servicenow.table.WriteResult;

/**
 * Contract tests against a real instance ({@code -Pcontract}; needs {@code SNOW_URL}, {@code
 * SNOW_USERNAME}, {@code SNOW_PASSWORD}). Scratch rows in {@code incident} carry a unique marker in
 * {@code short_description} and are deleted afterwards.
 */
@EnabledIfEnvironmentVariable(named = "SNOW_URL", matches = ".+")
class SourceContractIT {

    static final String MARKER = "kc-snow-source-contract-" + UUID.randomUUID();
    static final int ROWS = 3;

    static ServiceNowClient client;
    static List<String> created = new ArrayList<>();

    static Map<String, String> coreProps() {
        Map<String, String> p = new HashMap<>();
        p.put(CoreConfigDefs.URL, System.getenv("SNOW_URL"));
        p.put(CoreConfigDefs.AUTH_TYPE, CoreConfigDefs.AUTH_TYPE_BASIC);
        p.put(CoreConfigDefs.AUTH_USERNAME, System.getenv("SNOW_USERNAME"));
        p.put(CoreConfigDefs.AUTH_PASSWORD, System.getenv("SNOW_PASSWORD"));
        return p;
    }

    @BeforeAll
    static void createScratchRows() {
        client = ServiceNowClient.create(CoreConfig.fromProps(coreProps()));
        for (int i = 0; i < ROWS; i++) {
            Map<String, String> fields = new HashMap<>();
            fields.put("short_description", MARKER);
            fields.put("description", "row " + i);
            fields.put("urgency", "3");
            WriteResult result = client.tableApi().create("incident", fields);
            created.add(result.record().orElseThrow().sysId());
        }
    }

    @AfterAll
    static void deleteScratchRows() {
        try {
            for (String sysId : created) {
                try {
                    client.tableApi().delete("incident", sysId);
                } catch (RuntimeException e) {
                    System.err.println("cleanup of " + sysId + " failed: " + e.getMessage());
                }
            }
        } finally {
            client.close();
        }
    }

    private static TaskHarness harness() {
        Map<String, String> p = coreProps();
        p.put(SourceConfig.TABLES, "t1");
        p.put(TableSpec.key("t1", TableSpec.NAME), "incident");
        p.put(TableSpec.key("t1", TableSpec.TOPIC), "snow.incident");
        p.put(TableSpec.key("t1", TableSpec.QUERY), "short_description=" + MARKER);
        p.put(SourceConfig.POLL_INTERVAL_MS, "500");
        p.put(SourceConfig.SAFETY_LAG_SECONDS, "0");
        return new TaskHarness(p);
    }

    @Test
    void baseQueryFiltersToTheScratchRowsInCursorOrder() throws Exception {
        try (TaskHarness h = harness()) {
            h.start();
            List<SourceRecord> records = h.pollUntil(ROWS, 60_000);
            assertThat(records)
                    .extracting(SourceRecord::key)
                    .containsExactlyInAnyOrderElementsOf(created);
            assertThat(records)
                    .allSatisfy(
                            r ->
                                    assertThat(Fixtures.field(r, "short_description"))
                                            .isEqualTo(MARKER));
            assertThat(Fixtures.offsetsMonotonic(records)).isTrue();
            assertThat(records)
                    .allSatisfy(r -> SnowTimestamp.parse(Fixtures.field(r, "sys_updated_on")));
        }
    }

    @Test
    void projectionReturnsOnlyTheRequestedFieldsPlusSysModCount() throws Exception {
        try (TaskHarness h =
                harness()
                        .with(
                                TableSpec.key("t1", TableSpec.FIELDS),
                                "sys_id,sys_updated_on,short_description")) {
            h.start();
            List<SourceRecord> records = h.pollUntil(ROWS, 60_000);
            assertThat(records).hasSize(ROWS);
            assertThat(Fixtures.value(records.get(0)).keySet())
                    .containsExactlyInAnyOrder(
                            "sys_id", "sys_updated_on", "short_description", "sys_mod_count");
        }
    }

    @Test
    void displayValueAllReturnsRichFields() throws Exception {
        try (TaskHarness h = harness().with(TableSpec.key("t1", TableSpec.DISPLAY_VALUE), "all")) {
            h.start();
            List<SourceRecord> records = h.pollUntil(ROWS, 60_000);
            assertThat(records).hasSize(ROWS);
            Map<String, Object> value = Fixtures.value(records.get(0));
            assertThat(Fixtures.map(value.get("urgency")))
                    .containsEntry("value", "3")
                    .containsEntry("display_value", "3 - Low");
            assertThat(records.get(0).timestamp()).isGreaterThan(0L);
        }
    }

    @Test
    void displayValueTrueKeepsTheCursorFieldsRaw() throws Exception {
        try (TaskHarness h = harness().with(TableSpec.key("t1", TableSpec.DISPLAY_VALUE), "true")) {
            h.start();
            List<SourceRecord> records = h.pollUntil(ROWS, 60_000);
            assertThat(records).hasSize(ROWS);
            assertThat(Fixtures.field(records.get(0), "urgency")).isEqualTo("3 - Low");
            assertThat(SnowTimestamp.isValid(Fixtures.field(records.get(0), "sys_updated_on")))
                    .isTrue();
        }
    }

    @Test
    void documentedViewUsesPrefixedCursorFields() throws Exception {
        Map<String, String> p = coreProps();
        p.put(SourceConfig.TABLES, "v");
        p.put(TableSpec.key("v", TableSpec.NAME), "incident_sla");
        p.put(TableSpec.key("v", TableSpec.TOPIC), "snow.incident_sla");
        p.put(TableSpec.key("v", TableSpec.TIMESTAMP_FIELD), "inc_sys_updated_on");
        p.put(TableSpec.key("v", TableSpec.SYS_ID_FIELD), "inc_sys_id");
        p.put(
                TableSpec.key("v", TableSpec.START_TIMESTAMP),
                SnowTimestamp.format(Instant.now().minusSeconds(7 * 24 * 3600)));
        p.put(SourceConfig.POLL_INTERVAL_MS, "500");
        try (TaskHarness h = new TaskHarness(p)) {
            try {
                h.start();
            } catch (ConnectException e) {
                assumeTrue(
                        false,
                        "incident_sla view not readable on this instance: " + e.getMessage());
            }
            List<SourceRecord> records = h.pollFor(5_000);
            for (SourceRecord r : records) {
                assertThat(r.key()).isNotNull();
                assertThat(Fixtures.partition(r))
                        .containsEntry("timestamp_field", "inc_sys_updated_on");
                assertThat(Fixtures.value(r)).containsKey("inc_sys_id");
            }
        }
    }

    @Test
    void queryDomainFalseSendsTheNoDomainParameter() throws Exception {
        try (TaskHarness h = harness().with(TableSpec.key("t1", TableSpec.QUERY_DOMAIN), "false")) {
            try {
                h.start();
            } catch (ConnectException e) {
                assumeTrue(
                        !e.getMessage().contains("403"),
                        "integration user lacks query_no_domain_table_api: " + e.getMessage());
                throw e;
            }
            assertThat(h.pollUntil(ROWS, 60_000)).hasSize(ROWS);
        }
    }
}
