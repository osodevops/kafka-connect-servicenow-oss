package sh.oso.servicenow.sink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;

/**
 * Runs a {@link ServiceNowSinkTask} against the fake ServiceNow with a fake {@link
 * SinkTaskContext}: captured errant-record reports, an optional simulated pre-2.6 worker, and a
 * {@link MockProducer} behind the reporters.
 */
final class SinkTaskHarness implements AutoCloseable {

    record Reported(SinkRecord record, Throwable error) {}

    final MockServiceNowServer snow;

    /** Sends complete only on flush, so {@code flushed()} proves preCommit flushed the reporter. */
    final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());

    final List<Reported> dlq = new CopyOnWriteArrayList<>();
    final FakeContext context = new FakeContext();
    ServiceNowSinkTask task;
    boolean oldWorker;
    boolean dlqConfigured = true;

    SinkTaskHarness() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
    }

    Map<String, String> props(Map<String, String> overrides) {
        return TestSupport.props(snow, overrides);
    }

    void start(Map<String, String> props) {
        task = new ServiceNowSinkTask(p -> producer);
        task.initialize(context);
        task.start(props);
    }

    void put(SinkRecord... records) {
        task.put(Arrays.asList(records));
    }

    /** Calls preCommit with the framework's view: offset + 1 of the last record per partition. */
    Map<TopicPartition, OffsetAndMetadata> preCommit(SinkRecord... delivered) {
        Map<TopicPartition, OffsetAndMetadata> current = new HashMap<>();
        for (SinkRecord r : delivered) {
            TopicPartition tp = new TopicPartition(r.topic(), r.kafkaPartition());
            long next = r.kafkaOffset() + 1;
            OffsetAndMetadata prev = current.get(tp);
            if (prev == null || prev.offset() < next) {
                current.put(tp, new OffsetAndMetadata(next));
            }
        }
        return task.preCommit(current);
    }

    @Override
    public void close() {
        if (task != null) {
            task.stop();
        }
        snow.close();
    }

    final class FakeContext implements SinkTaskContext {
        final Set<TopicPartition> assignment = new HashSet<>();
        final Map<TopicPartition, Long> offsets = new HashMap<>();
        final Set<TopicPartition> paused = new HashSet<>();
        long timeout = -1;
        int commitRequests;

        @Override
        public Map<String, String> configs() {
            return Map.of();
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
            this.offsets.putAll(offsets);
        }

        @Override
        public void offset(TopicPartition tp, long offset) {
            offsets.put(tp, offset);
        }

        @Override
        public void timeout(long timeoutMs) {
            this.timeout = timeoutMs;
        }

        @Override
        public Set<TopicPartition> assignment() {
            return assignment;
        }

        @Override
        public void pause(TopicPartition... partitions) {
            paused.addAll(Arrays.asList(partitions));
        }

        @Override
        public void resume(TopicPartition... partitions) {
            Arrays.asList(partitions).forEach(paused::remove);
        }

        @Override
        public void requestCommit() {
            commitRequests++;
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            if (oldWorker) {
                throw new NoSuchMethodError("errantRecordReporter");
            }
            if (!dlqConfigured) {
                return null;
            }
            return new ErrantRecordReporter() {
                @Override
                public Future<Void> report(SinkRecord record, Throwable error) {
                    dlq.add(new Reported(record, error));
                    return CompletableFuture.completedFuture(null);
                }
            };
        }
    }
}
