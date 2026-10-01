package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A PATCH carries exactly the mapped fields: no nulls under omit, no reserved or header names. */
class PatchSendsOnlyIntendedFieldsTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private SinkTaskHarness h;

    @BeforeEach
    void setUp() {
        h = new SinkTaskHarness();
    }

    @AfterEach
    void tearDown() {
        h.close();
    }

    @Test
    void patchBodyContainsOnlyTheIntendedFields() throws Exception {
        String sysId =
                h.snow.tables()
                        .insert(
                                "incident",
                                Map.of(
                                        "short_description",
                                        "old",
                                        "urgency",
                                        "3",
                                        "comments",
                                        "keep"));
        h.start(h.props(Map.of()));
        Map<String, Object> value = new HashMap<>();
        value.put("short_description", "new");
        value.put("urgency", 1);
        value.put("comments", null);
        value.put("sys_id", sysId);
        value.put("sys_updated_on", "2026-01-01 00:00:00");
        value.put("sys_mod_count", "99");
        value.put("snow.operation", "DELETE");
        h.put(TestSupport.record(0, 7, sysId, value));

        List<String> bodies = h.snow.journal().bodies("PATCH", "incident", sysId);
        assertThat(bodies).hasSize(1);
        JsonNode body = JSON.readTree(bodies.get(0));
        assertThat(body.fieldNames())
                .toIterable()
                .containsExactlyInAnyOrder("short_description", "urgency");
        assertThat(body.get("urgency").asText()).isEqualTo("1");
        Map<String, String> row = h.snow.tables().get("incident", sysId).orElseThrow();
        assertThat(row).containsEntry("short_description", "new").containsEntry("comments", "keep");
        assertThat(row.get("sys_mod_count")).isEqualTo("1");
        assertThat(h.snow.journal().count("POST", "incident")).isZero();
    }
}
