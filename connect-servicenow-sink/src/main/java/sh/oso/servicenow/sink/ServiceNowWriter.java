package sh.oso.servicenow.sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.ServiceNowClient;
import sh.oso.servicenow.common.ServiceNowApiException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.table.TableApiClient;
import sh.oso.servicenow.table.WriteResult;

/**
 * Writes a batch with per-partition ordering and bounded concurrency. Records are grouped by {@link
 * TopicPartition} in arrival order; each group is a lane written sequentially, one request at a
 * time, on a pool of {@code snow.sink.max.in.flight} threads with a {@link Semaphore} of the same
 * size around every request. A lane stops at the first retryable failure (so later records of the
 * partition are not written ahead of an earlier one) and, under {@code
 * behavior.on.api.errors=fail}, at the first permanent one. {@link #write} blocks until every lane
 * has finished and returns one {@link Outcome} per processed record, in input order.
 */
final class ServiceNowWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceNowWriter.class);

    private final TableRouter router;
    private final IdExtractor ids;
    private final OperationResolver operations;
    private final RecordMapper mapper;
    private final AmbiguousWritePolicy writes;
    private final TableApiClient api;
    private final SinkConfig.NotFoundBehavior notFound;
    private final boolean stopLaneOnRecordError;
    private final String username;
    private final ExecutorService executor;
    private final Semaphore inFlight;
    private final SinkWriterMetrics metrics;

    ServiceNowWriter(SinkConfig config, ServiceNowClient client) {
        this(config, client, new SinkWriterMetrics(client.http().stats()));
    }

    ServiceNowWriter(SinkConfig config, ServiceNowClient client, SinkWriterMetrics metrics) {
        this.metrics = metrics;
        this.router = new TableRouter(config);
        this.ids = new IdExtractor(config);
        this.operations = new OperationResolver(config);
        this.mapper =
                new RecordMapper(
                        config,
                        config.unknownFieldBehavior() == SinkConfig.UnknownFieldBehavior.PASSTHROUGH
                                ? null
                                : client.metadata());
        this.api = client.tableApi();
        this.writes = new AmbiguousWritePolicy(config, api);
        this.notFound = config.notFoundBehavior();
        this.stopLaneOnRecordError = config.errorBehavior() == SinkConfig.ErrorBehavior.FAIL;
        this.username = config.username();
        int lanes = config.maxInFlight();
        this.executor = Executors.newFixedThreadPool(lanes, new LaneThreads());
        this.inFlight = new Semaphore(lanes);
    }

    List<Outcome> write(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return List.of();
        }
        Map<TopicPartition, List<SinkRecord>> lanes = new LinkedHashMap<>();
        for (SinkRecord r : records) {
            lanes.computeIfAbsent(
                            new TopicPartition(r.topic(), r.kafkaPartition()),
                            k -> new ArrayList<>())
                    .add(r);
        }
        List<Future<List<Outcome>>> futures = new ArrayList<>(lanes.size());
        for (List<SinkRecord> lane : lanes.values()) {
            futures.add(executor.submit(() -> runLane(lane)));
        }
        Map<SinkRecord, Outcome> byRecord = new IdentityHashMap<>();
        try {
            for (Future<List<Outcome>> f : futures) {
                for (Outcome o : f.get()) {
                    byRecord.put(o.record(), o);
                }
            }
        } catch (InterruptedException e) {
            futures.forEach(f -> f.cancel(true));
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while writing to ServiceNow", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new ConnectException("Writer lane failed", cause);
        }
        List<Outcome> out = new ArrayList<>(byRecord.size());
        for (SinkRecord r : records) {
            Outcome o = byRecord.get(r);
            if (o != null) {
                out.add(o);
            }
        }
        return out;
    }

    private List<Outcome> runLane(List<SinkRecord> lane) {
        List<Outcome> out = new ArrayList<>(lane.size());
        for (SinkRecord r : lane) {
            Outcome o = writeOne(r);
            out.add(o);
            if (o.isRetryable() || (!o.isSuccess() && stopLaneOnRecordError)) {
                LOG.debug(
                        "Lane {}-{} stopped at offset {} ({})",
                        r.topic(),
                        r.kafkaPartition(),
                        r.kafkaOffset(),
                        o.classification());
                break;
            }
        }
        return out;
    }

    SinkWriterMetrics metrics() {
        return metrics;
    }

    Outcome writeOne(SinkRecord record) {
        Outcome outcome = attempt(record);
        metrics.recorded(outcome);
        return outcome;
    }

    private Outcome attempt(SinkRecord record) {
        String table = null;
        String sysId = null;
        Operation op = null;
        Map<String, Object> body = Map.of();
        try {
            table = router.route(record);
            sysId = ids.extract(record).orElse(null);
            op = operations.resolve(record, sysId);
            RecordMapper.Mapped mapped =
                    op == Operation.DELETE
                            ? RecordMapper.Mapped.empty()
                            : mapper.map(record, table);
            body = mapped.body();
            AmbiguousWritePolicy.Result result = execute(op, table, sysId, body);
            return Outcome.success(
                    record,
                    result.operation(),
                    table,
                    result.sysId(),
                    result.status(),
                    result.requestId(),
                    result.retries(),
                    body,
                    mapped.droppedUnknownFields());
        } catch (RecordError e) {
            return Outcome.failure(record, op, table, sysId, body, e);
        } catch (ServiceNowException e) {
            return Outcome.failure(
                    record,
                    op,
                    table,
                    sysId,
                    body,
                    RecordError.from(e, op, table, sysId, username));
        } catch (RuntimeException e) {
            LOG.error("Unexpected failure writing {}", coordinates(record), e);
            return Outcome.failure(
                    record,
                    op,
                    table,
                    sysId,
                    body,
                    new RecordError(
                            Classification.RECORD_ERROR,
                            "Unexpected failure: " + e.getMessage(),
                            e));
        }
    }

    private AmbiguousWritePolicy.Result execute(
            Operation op, String table, String sysId, Map<String, Object> body) {
        acquire();
        metrics.requestStarted();
        try {
            switch (op) {
                case CREATE -> {
                    return writes.create(table, body);
                }
                case PATCH, PUT -> {
                    try {
                        return writes.update(op, table, sysId, body);
                    } catch (ServiceNowApiException e) {
                        if (e.isNotFound()) {
                            return notFound(op, table, sysId, body, e);
                        }
                        throw e;
                    }
                }
                case DELETE -> {
                    try {
                        return writes.delete(table, sysId);
                    } catch (ServiceNowApiException e) {
                        if (e.isNotFound()) {
                            return notFound(op, table, sysId, body, e);
                        }
                        throw e;
                    }
                }
                default -> throw new IllegalStateException(op.toString());
            }
        } finally {
            metrics.requestFinished();
            inFlight.release();
        }
    }

    private AmbiguousWritePolicy.Result notFound(
            Operation op,
            String table,
            String sysId,
            Map<String, Object> body,
            ServiceNowApiException e) {
        switch (notFound) {
            case FAIL -> throw RecordError.from(e, op, table, sysId, username);
            case IGNORE -> {
                LOG.info(
                        "{} {}/{} answered 404; ignored under {}=ignore (request-id {})",
                        op,
                        table,
                        sysId,
                        SinkConfig.NOT_FOUND_BEHAVIOR,
                        e.requestId());
                return new AmbiguousWritePolicy.Result(op, e.status(), sysId, e.requestId(), 0);
            }
            case CREATE -> {
                if (op == Operation.DELETE) {
                    LOG.info(
                            "DELETE {}/{} answered 404; nothing to delete (request-id {})",
                            table,
                            sysId,
                            e.requestId());
                    return new AmbiguousWritePolicy.Result(op, e.status(), sysId, e.requestId(), 0);
                }
                LOG.info(
                        "{} {}/{} answered 404; creating the row with that sys_id under {}=create",
                        op,
                        table,
                        sysId,
                        SinkConfig.NOT_FOUND_BEHAVIOR);
                Map<String, Object> withId = new LinkedHashMap<>(body);
                withId.put("sys_id", sysId);
                WriteResult w = api.create(table, withId);
                return new AmbiguousWritePolicy.Result(
                        Operation.CREATE, w.status(), w.sysId().orElse(sysId), w.requestId(), 0);
            }
            default -> throw new IllegalStateException(notFound.toString());
        }
    }

    private void acquire() {
        try {
            inFlight.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while waiting to write to ServiceNow", e);
        }
    }

    private static String coordinates(SinkRecord r) {
        return r.topic() + "-" + r.kafkaPartition() + "@" + r.kafkaOffset();
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private static final class LaneThreads implements ThreadFactory {
        private static final AtomicInteger COUNTER = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "snow-sink-writer-" + COUNTER.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
