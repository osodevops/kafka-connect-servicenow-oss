package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class ServiceNowSinkConnectorTest {

    @Test
    void startsFailsFastAndHandsEveryTaskTheSameConfig() {
        ServiceNowSinkConnector c = new ServiceNowSinkConnector();
        Map<String, String> props = TestSupport.offlineProps();
        c.start(props);
        List<Map<String, String>> tasks = c.taskConfigs(3);
        assertThat(tasks).hasSize(3);
        assertThat(tasks).allSatisfy(t -> assertThat(t).isEqualTo(props));
        assertThat(c.taskClass()).isEqualTo(ServiceNowSinkTask.class);
        assertThat(c.config().names()).contains(SinkConfig.TABLE, "snow.url");
        assertThat(c.version()).isNotBlank();
        assertThat(new ServiceNowSinkTask().version()).isEqualTo(c.version());
        c.stop();

        Map<String, String> bad = TestSupport.offlineProps(Map.of(SinkConfig.ROUTING_MODE, "nope"));
        assertThatThrownBy(() -> new ServiceNowSinkConnector().start(bad))
                .isInstanceOf(ConfigException.class);
    }
}
