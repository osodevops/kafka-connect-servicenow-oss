package sh.oso.servicenow.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.testing.MockServiceNowServer;
import sh.oso.servicenow.testing.TableStore;

class TableMetadataClientTest {

    private MockServiceNowServer snow;
    private TableMetadataClient metadata;

    @BeforeEach
    void setUp() {
        snow = MockServiceNowServer.start();
        TableStore t = snow.tables();
        String taskId = t.insert("sys_db_object", Map.of("name", "task", "super_class", ""));
        String incidentId =
                t.insert("sys_db_object", Map.of("name", "incident", "super_class", taskId));
        t.insert("sys_db_object", Map.of("name", "incident_task", "super_class", incidentId));
        t.insert("sys_dictionary", Map.of("name", "task", "element", "number"));
        t.insert("sys_dictionary", Map.of("name", "task", "element", "short_description"));
        t.insert("sys_dictionary", Map.of("name", "task", "element", ""));
        t.insert("sys_dictionary", Map.of("name", "incident", "element", "caller_id"));
        t.insert("sys_dictionary", Map.of("name", "incident", "element", "severity"));
        t.insert("sys_dictionary", Map.of("name", "problem", "element", "known_error"));
        metadata =
                new TableMetadataClient(
                        snow.client().tableApi(), Duration.ofMinutes(5), snow.clock());
    }

    @AfterEach
    void tearDown() {
        snow.close();
    }

    @Test
    void walksTheSuperClassChainAndAlwaysIncludesSysId() {
        assertThat(metadata.tableChain("incident_task"))
                .containsExactly("incident_task", "incident", "task");
        assertThat(metadata.columns("incident"))
                .containsExactlyInAnyOrder(
                        "sys_id", "caller_id", "severity", "number", "short_description");
        assertThat(metadata.columns("task"))
                .containsExactlyInAnyOrder("sys_id", "number", "short_description");
    }

    @Test
    void cachesUntilTheTtlExpiresOrEvicted() {
        metadata.columns("incident");
        int requests = snow.journal().count("GET");
        metadata.columns("incident");
        assertThat(snow.journal().count("GET")).isEqualTo(requests);
        snow.clock().advance(Duration.ofMinutes(6));
        metadata.columns("incident");
        assertThat(snow.journal().count("GET")).isGreaterThan(requests);
        int afterReload = snow.journal().count("GET");
        metadata.evict("incident");
        metadata.columns("incident");
        assertThat(snow.journal().count("GET")).isGreaterThan(afterReload);
        metadata.evictAll();
    }

    @Test
    void unknownTableFailsClearly() {
        assertThatThrownBy(() -> metadata.columns("nope"))
                .isInstanceOf(ServiceNowException.class)
                .hasMessageContaining("sys_db_object");
        assertThatThrownBy(() -> metadata.columns("Bad Name"))
                .isInstanceOf(ServiceNowException.class);
    }
}
