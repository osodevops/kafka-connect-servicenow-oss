package sh.oso.servicenow.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import sh.oso.servicenow.testing.MockServiceNowServer;

/**
 * Drives a {@link ServiceNowSourceTask} against the fake ServiceNow (or any explicit properties)
 * while simulating Connect's offset storage: the offset of every polled record becomes visible to a
 * restarted task, exactly as the framework commits them.
 */
final class TaskHarness implements AutoCloseable {

    static final String ALIAS = "t1";
    static final String TABLE = "incident";
    static final String TOPIC = "snow.incident";

    private final Map<String, String> props = new LinkedHashMap<>();
    private final Map<Map<String, ?>, Map<String, Object>> committed = new HashMap<>();
    private ServiceNowSourceTask task;

    /** Basic auth against the fake, fast retries, one table {@code t1 -> incident}. */
    TaskHarness(MockServiceNowServer snow) {
        props.putAll(snow.basicAuthProps());
        props.putAll(MockServiceNowServer.fastRetryProps());
        props.put(SourceConfig.TABLES, ALIAS);
        props.put(TableSpec.key(ALIAS, TableSpec.NAME), TABLE);
        props.put(TableSpec.key(ALIAS, TableSpec.TOPIC), TOPIC);
        props.put(SourceConfig.POLL_INTERVAL_MS, "200");
    }

    /** Explicit properties, for example from {@code taskConfigs} or a real instance. */
    TaskHarness(Map<String, String> explicit) {
        props.putAll(explicit);
    }

    TaskHarness with(String key, String value) {
        props.put(key, value);
        return this;
    }

    /** Adds (or redefines) a table alias. */
    TaskHarness table(String alias, String name, String topic) {
        List<String> aliases = new ArrayList<>(SourceConfig.rawAliases(props));
        if (!aliases.contains(alias)) {
            aliases.add(alias);
        }
        props.put(SourceConfig.TABLES, String.join(",", aliases));
        props.put(TableSpec.key(alias, TableSpec.NAME), name);
        props.put(TableSpec.key(alias, TableSpec.TOPIC), topic);
        return this;
    }

    /** Replaces the alias list (used to drop {@code t1}). */
    TaskHarness tables(String aliases) {
        props.put(SourceConfig.TABLES, aliases);
        return this;
    }

    Map<String, String> props() {
        return props;
    }

    ServiceNowSourceTask start() {
        task = new ServiceNowSourceTask();
        task.initialize(context());
        task.start(props);
        return task;
    }

    SourceTaskContext context() {
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return props;
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return committed.get(partition);
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                            Collection<Map<String, T>> partitions) {
                        Map<Map<String, T>, Map<String, Object>> out = new HashMap<>();
                        for (Map<String, T> partition : partitions) {
                            out.put(partition, committed.get(partition));
                        }
                        return out;
                    }
                };
            }
        };
    }

    /** One task poll; offsets of returned records are "committed" like the framework would. */
    @SuppressWarnings("unchecked")
    List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = task.poll();
        if (records != null) {
            for (SourceRecord record : records) {
                committed.put(
                        (Map<String, ?>) record.sourcePartition(),
                        (Map<String, Object>) record.sourceOffset());
            }
        }
        return records == null ? List.of() : records;
    }

    /** Polls until at least {@code n} records accumulate or the deadline passes. */
    List<SourceRecord> pollUntil(int n, long timeoutMillis) throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (out.size() < n && System.currentTimeMillis() < deadline) {
            out.addAll(poll());
        }
        return out;
    }

    /** Polls for a fixed duration and returns everything delivered. */
    List<SourceRecord> pollFor(long millis) throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            out.addAll(poll());
        }
        return out;
    }

    /** Pre-loads an offset as if a previous run had committed it. */
    void seedOffset(Map<String, ?> partition, Map<String, Object> offset) {
        committed.put(partition, offset);
    }

    Map<Map<String, ?>, Map<String, Object>> committedOffsets() {
        return committed;
    }

    void restart() {
        if (task != null) {
            task.stop();
        }
        start();
    }

    ServiceNowSourceTask task() {
        return task;
    }

    @Override
    public void close() {
        if (task != null) {
            task.stop();
            task = null;
        }
    }
}
