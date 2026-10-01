package sh.oso.servicenow.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Connect sink connector for ServiceNow: create, patch, put and delete through the Table API.
 * Every task receives the same configuration; the framework partitions the input topics.
 */
public class ServiceNowSinkConnector extends SinkConnector {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceNowSinkConnector.class);

    private Map<String, String> originals;

    @Override
    public void start(Map<String, String> props) {
        this.originals = Map.copyOf(props);
        SinkConfig config = new SinkConfig(props); // fail fast
        LOG.info(
                "ServiceNow sink connector started for {} ({} routing)",
                config.coreConfig().instanceHost(),
                config.routingMode());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ServiceNowSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(originals);
        }
        return configs;
    }

    @Override
    public void stop() {}

    @Override
    public ConfigDef config() {
        return SinkConfig.configDef();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
