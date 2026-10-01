package sh.oso.servicenow.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ServiceNow source connector: historical backfill plus continuous keyset polling of any Table API
 * table or view. Tables are assigned whole to tasks by rendezvous hashing; delivery is
 * at-least-once and exactly-once is declared unsupported.
 */
public class ServiceNowSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceNowSourceConnector.class);

    private Map<String, String> originals;

    @Override
    public void start(Map<String, String> props) {
        this.originals = Map.copyOf(props);
        SourceConfig config = new SourceConfig(props); // fail fast on invalid config
        LOG.info(
                "ServiceNow source connector configured for {} table(s) on {}: {}",
                config.tables().size(),
                config.core().instanceHost(),
                config.aliases());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ServiceNowSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> aliases = new SourceConfig(originals).aliases();
        int taskCount = Math.max(1, Math.min(maxTasks, aliases.size()));
        if (maxTasks > aliases.size()) {
            LOG.info(
                    "tasks.max={} exceeds the table count {}; at most {} task(s) will be created",
                    maxTasks,
                    aliases.size(),
                    taskCount);
        }
        List<List<String>> assignment = TaskAssignment.assign(aliases, taskCount);
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < assignment.size(); i++) {
            List<String> tables = assignment.get(i);
            if (tables.isEmpty()) {
                LOG.info("Task slot {} owns no table under rendezvous hashing; not created", i);
                continue;
            }
            Map<String, String> taskConfig = new HashMap<>(originals);
            taskConfig.put(SourceConfig.TASK_TABLES, String.join(",", tables));
            configs.add(taskConfig);
            LOG.info("Task {} owns tables {}", configs.size() - 1, tables);
        }
        return configs;
    }

    @Override
    public void stop() {}

    /** The static definition; per-alias keys are validated by {@link #validate(Map)}. */
    @Override
    public ConfigDef config() {
        return SourceConfig.configDef();
    }

    /**
     * Validates against the definition that includes every {@code snow.table.<alias>.*} key named
     * by {@code snow.tables}, then runs the cross-field checks and attaches their message to the
     * offending key.
     */
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigDef def = SourceConfig.configDef(connectorConfigs);
        List<ConfigValue> values = def.validate(connectorConfigs);
        boolean hasErrors = false;
        for (ConfigValue v : values) {
            if (!v.errorMessages().isEmpty()) {
                hasErrors = true;
                break;
            }
        }
        if (!hasErrors) {
            try {
                new SourceConfig(connectorConfigs);
            } catch (ConfigException e) {
                attach(values, e.getMessage());
            }
        }
        return new Config(values);
    }

    private static void attach(List<ConfigValue> values, String message) {
        ConfigValue target = null;
        for (ConfigValue v : values) {
            if (message != null
                    && message.contains(v.name())
                    && (target == null || v.name().length() > target.name().length())) {
                target = v;
            }
        }
        if (target == null) {
            for (ConfigValue v : values) {
                if (SourceConfig.TABLES.equals(v.name())) {
                    target = v;
                    break;
                }
            }
        }
        if (target == null) {
            target = new ConfigValue(SourceConfig.TABLES);
            values.add(target);
        }
        target.addErrorMessage(message);
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        return ExactlyOnceSupport.UNSUPPORTED;
    }

    @Override
    public String version() {
        return Version.get();
    }
}
