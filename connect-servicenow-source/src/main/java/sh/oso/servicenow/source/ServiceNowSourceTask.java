package sh.oso.servicenow.source;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.common.ConnectExceptions;
import sh.oso.servicenow.cursor.SourcePartition;

/**
 * Runs one {@link TablePoller} per assigned table over a shared {@link ServiceNowClient}. Pollers
 * are visited round-robin, one request each per {@link #poll()}; a table that fails permanently is
 * parked and its failure is raised once the other tables have nothing to deliver, so one broken
 * table never hides data from the others but is never hidden either. Retryable failures surface as
 * {@link RetriableException} and the poller retries the same request on the next poll.
 */
public class ServiceNowSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceNowSourceTask.class);
    static final long IDLE_SLEEP_MS = 100;

    private SourceConfig config;
    private ServiceNowClient client;
    private final List<TablePoller> pollers = new ArrayList<>();
    private final Set<TablePoller> parked = new HashSet<>();
    private RuntimeException pendingFailure;
    private int next;

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new SourceConfig(props);
        client = ServiceNowClient.create(config.core());
        try {
            List<TableSpec> specs = config.taskTables();
            if (config.startupProbe()) {
                StartupProbe probe = new StartupProbe(client);
                for (TableSpec spec : specs) {
                    probe.probe(spec);
                }
                probe.checkUserTimeZone();
            }
            String host = config.core().instanceHost();
            List<Map<String, String>> partitions = new ArrayList<>();
            for (TableSpec spec : specs) {
                partitions.add(
                        SourcePartition.of(
                                host, spec.name(), spec.fingerprint(), spec.timestampField()));
            }
            Map<Map<String, String>, Map<String, Object>> offsets =
                    context == null || context.offsetStorageReader() == null
                            ? Map.of()
                            : context.offsetStorageReader().offsets(partitions);
            for (int i = 0; i < specs.size(); i++) {
                TableSpec spec = specs.get(i);
                Map<String, String> partition = partitions.get(i);
                Map<String, Object> stored = offsets == null ? null : offsets.get(partition);
                TablePoller poller =
                        new TablePoller(spec, config, client, stored, Clock.systemUTC());
                pollers.add(poller);
                LOG.info(
                        "Table '{}' (alias {}) -> topic {} on partition {} ({})",
                        spec.name(),
                        spec.alias(),
                        spec.topic(),
                        partition,
                        stored == null ? "no stored offset" : "stored offset " + stored);
            }
            LOG.info(
                    "ServiceNow source task started for {} table(s) on {}: {}",
                    pollers.size(),
                    host,
                    aliases());
        } catch (RuntimeException e) {
            client.close();
            throw ConnectExceptions.toConnect(e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        int n = pollers.size();
        if (n == 0) {
            Thread.sleep(IDLE_SLEEP_MS);
            return null;
        }
        List<SourceRecord> out = new ArrayList<>();
        RuntimeException retriable = null;
        for (int i = 0; i < n; i++) {
            TablePoller poller = pollers.get((next + i) % n);
            if (parked.contains(poller)) {
                continue;
            }
            try {
                out.addAll(poller.poll());
            } catch (RuntimeException e) {
                RuntimeException mapped = ConnectExceptions.toConnect(e);
                String message = describe(poller, mapped);
                if (mapped instanceof RetriableException) {
                    LOG.warn("{}; will retry", message);
                    if (retriable == null) {
                        retriable = new RetriableException(message, mapped);
                    }
                } else {
                    LOG.error("{}; table parked", message, mapped);
                    parked.add(poller);
                    if (pendingFailure == null) {
                        pendingFailure = new ConnectException(message, mapped);
                    }
                }
            }
        }
        next = (next + 1) % n;
        if (!out.isEmpty()) {
            return out;
        }
        if (pendingFailure != null) {
            throw pendingFailure;
        }
        if (retriable != null) {
            throw retriable;
        }
        Thread.sleep(IDLE_SLEEP_MS);
        return null;
    }

    @Override
    public void stop() {
        pollers.clear();
        parked.clear();
        if (client != null) {
            client.close();
            client = null;
        }
        LOG.info("ServiceNow source task stopped");
    }

    /** Per-table metrics keyed by table name, for JMX registration. */
    public Map<String, TableMetrics> metrics() {
        Map<String, TableMetrics> out = new LinkedHashMap<>();
        for (TablePoller p : pollers) {
            out.put(p.table(), p.metrics());
        }
        return Collections.unmodifiableMap(out);
    }

    List<TablePoller> pollers() {
        return Collections.unmodifiableList(pollers);
    }

    /** Prefixes the failure with the table unless the poller already named it. */
    private static String describe(TablePoller poller, RuntimeException failure) {
        String message = failure.getMessage() == null ? failure.toString() : failure.getMessage();
        if (message.startsWith("Table '")) {
            return message;
        }
        return "Table '" + poller.table() + "' (alias " + poller.alias() + "): " + message;
    }

    private List<String> aliases() {
        List<String> out = new ArrayList<>();
        for (TablePoller p : pollers) {
            out.add(p.alias());
        }
        return out;
    }
}
