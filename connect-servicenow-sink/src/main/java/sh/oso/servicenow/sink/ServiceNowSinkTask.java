package sh.oso.servicenow.sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.management.ObjectName;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.metrics.ServiceNowMetrics;

/**
 * Writes Kafka records to ServiceNow tables. {@link #put} hands the batch to the {@link
 * ServiceNowWriter} and then applies the error paths to every outcome: successes go to the success
 * reporter; permanent failures go to the error reporter and the errant record reporter (when the
 * worker offers one) and then follow {@code behavior.on.api.errors}; a retryable exhaustion throws
 * {@link RetriableException} so the framework re-delivers the batch. {@link #preCommit} flushes the
 * reporter and commits only offsets of records that succeeded or were accepted by an error path.
 *
 * <p>The task registers a {@link SinkWriterMetricsMXBean} under {@code
 * sh.oso.servicenow:type=sink-writer,connector=<name>,task=<n>} for its lifetime.
 */
public class ServiceNowSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceNowSinkTask.class);

    private final Reporter.ProducerFactory producerFactory;
    private SinkConfig config;
    private ServiceNowClient client;
    private ServiceNowWriter writer;
    private Reporter reporter;
    private ErrantRecordReporter errantReporter;
    private SinkWriterMetrics metrics;
    private ObjectName mbean;
    private final Map<TopicPartition, Long> completed = new HashMap<>();

    public ServiceNowSinkTask() {
        this(Reporter.KAFKA);
    }

    ServiceNowSinkTask(Reporter.ProducerFactory producerFactory) {
        this.producerFactory = producerFactory;
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new SinkConfig(props);
        client = ServiceNowClient.create(config.coreConfig());
        metrics = new SinkWriterMetrics(client.http().stats());
        writer = new ServiceNowWriter(config, client, metrics);
        reporter = new Reporter(config, producerFactory, metrics);
        registerMetrics(props);
        errantReporter = null;
        try {
            errantReporter = context != null ? context.errantRecordReporter() : null;
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            LOG.info("Worker predates the errant record reporter (Connect 2.6); DLQ unavailable");
        }
        LOG.info(
                "ServiceNow sink task started: routing={} table={} operation={} update={}"
                        + " inFlight={} ambiguousCreate={} notFound={} onErrors={} dlq={} reporter={}",
                config.routingMode(),
                config.routingMode() == SinkConfig.RoutingMode.FIXED
                        ? config.fixedTable()
                        : config.routingMode() == SinkConfig.RoutingMode.TOPIC_MAP
                                ? config.topicTables()
                                : config.tableAllowlist(),
                config.operationMode(),
                config.updateMethod(),
                config.maxInFlight(),
                config.createAmbiguousBehavior(),
                config.notFoundBehavior(),
                config.errorBehavior(),
                errantReporter != null,
                reporter.enabled());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        handleOutcomes(writer.write(records));
    }

    void handleOutcomes(List<Outcome> outcomes) {
        List<Future<Void>> dlq = new ArrayList<>();
        Outcome firstFatal = null;
        Outcome firstRetryable = null;
        int failures = 0;
        for (Outcome o : outcomes) {
            if (o.isSuccess()) {
                reporter.report(o);
                markCompleted(o);
                continue;
            }
            reporter.report(o);
            if (o.isRetryable()) {
                LOG.warn(
                        "Retries exhausted for {} ({} {}): {}",
                        o.coordinates(),
                        o.operation(),
                        o.table(),
                        o.error().getMessage());
                if (firstRetryable == null) {
                    firstRetryable = o;
                }
                continue;
            }
            failures++;
            if (errantReporter != null) {
                dlq.add(errantReporter.report(o.record(), o.error()));
            }
            switch (config.errorBehavior()) {
                case FAIL -> {
                    if (firstFatal == null) {
                        firstFatal = o;
                    }
                }
                case LOG -> {
                    LOG.error(
                            "Record {} failed permanently ({} {} {}): {}",
                            o.coordinates(),
                            o.classification(),
                            o.operation(),
                            o.table(),
                            o.error().getMessage());
                    markCompleted(o);
                }
                case IGNORE -> markCompleted(o);
            }
        }
        awaitDlq(dlq);
        if (firstFatal != null) {
            throw new ConnectException(
                    failures
                            + " record(s) failed permanently; first at "
                            + firstFatal.coordinates()
                            + " ("
                            + firstFatal.classification()
                            + "): "
                            + firstFatal.error().getMessage(),
                    firstFatal.error());
        }
        if (firstRetryable != null) {
            throw new RetriableException(
                    "Retries exhausted at "
                            + firstRetryable.coordinates()
                            + "; the batch will be re-delivered: "
                            + firstRetryable.error().getMessage(),
                    firstRetryable.error());
        }
    }

    private void awaitDlq(List<Future<Void>> futures) {
        for (Future<Void> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException(
                        "Interrupted while waiting for the dead letter queue", e);
            } catch (ExecutionException e) {
                throw new ConnectException(
                        "The dead letter queue did not accept a failed record", e.getCause());
            }
        }
    }

    private void markCompleted(Outcome o) {
        TopicPartition tp = new TopicPartition(o.record().topic(), o.record().kafkaPartition());
        long next = o.record().kafkaOffset() + 1;
        completed.merge(tp, next, Math::max);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        reporter.flush();
        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : currentOffsets.entrySet()) {
            Long done = completed.get(e.getKey());
            if (done == null) {
                continue;
            }
            out.put(e.getKey(), new OffsetAndMetadata(Math.min(done, e.getValue().offset())));
        }
        return out;
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        partitions.forEach(completed::remove);
    }

    /** The MBean this task registered, or null when the platform server refused it. */
    ObjectName mbeanName() {
        return mbean;
    }

    SinkWriterMetrics metrics() {
        return metrics;
    }

    private void registerMetrics(Map<String, String> props) {
        ObjectName name =
                ServiceNowMetrics.sinkWriterName(
                        ServiceNowMetrics.connectorName(props), ServiceNowMetrics.taskId(props));
        mbean =
                ServiceNowMetrics.register(name, metrics, SinkWriterMetricsMXBean.class)
                        ? name
                        : null;
    }

    @Override
    public void stop() {
        ServiceNowMetrics.unregister(mbean);
        mbean = null;
        if (reporter != null) {
            reporter.close();
        }
        if (writer != null) {
            writer.close();
        }
        if (client != null) {
            client.close();
        }
    }
}
