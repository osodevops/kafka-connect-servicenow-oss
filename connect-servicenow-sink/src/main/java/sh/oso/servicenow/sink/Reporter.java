package sh.oso.servicenow.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.common.Redaction;

/**
 * Publishes one JSON report per write to the success and error topics with its own producer. The
 * key is the sys_id (null when none is known); headers carry the Kafka coordinates, the table and
 * the operation. {@link #flush()} runs in {@code preCommit} so every report is durable before the
 * offset it describes is committed; a failed send fails that commit.
 */
final class Reporter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Reporter.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    static final String HEADER_TOPIC = "snow.source.topic";
    static final String HEADER_PARTITION = "snow.source.partition";
    static final String HEADER_OFFSET = "snow.source.offset";
    static final String HEADER_TABLE = "snow.table";
    static final String HEADER_OPERATION = "snow.operation";
    static final String HEADER_CLASSIFICATION = "snow.classification";

    /** Creates the producer; tests supply a {@code MockProducer}. */
    @FunctionalInterface
    interface ProducerFactory {
        Producer<byte[], byte[]> create(Map<String, Object> props);
    }

    static final ProducerFactory KAFKA = KafkaProducer::new;

    private final String successTopic;
    private final String errorTopic;
    private final boolean includeBody;
    private final Producer<byte[], byte[]> producer;
    private final SinkWriterMetrics metrics;
    private final AtomicReference<Exception> sendFailure = new AtomicReference<>();

    Reporter(SinkConfig config, ProducerFactory factory) {
        this(config, factory, null);
    }

    Reporter(SinkConfig config, ProducerFactory factory, SinkWriterMetrics metrics) {
        this.metrics = metrics;
        this.successTopic = config.successTopic();
        this.errorTopic = config.errorTopic();
        this.includeBody = config.includeRequestBody();
        if (successTopic == null && errorTopic == null) {
            this.producer = null;
            return;
        }
        Map<String, Object> props = new HashMap<>(config.reporterProducerProps());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.reporterBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        props.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "servicenow-sink-reporter");
        this.producer = factory.create(props);
        LOG.info(
                "Reporter enabled (success topic {}, error topic {}, request body {})",
                successTopic,
                errorTopic,
                includeBody ? "included" : "excluded");
    }

    boolean enabled() {
        return producer != null;
    }

    void report(Outcome outcome) {
        if (producer == null) {
            return;
        }
        if (outcome.isSuccess()) {
            if (successTopic != null) {
                send(successTopic, outcome, body(outcome));
            }
            if (errorTopic != null && !outcome.droppedFields().isEmpty()) {
                send(errorTopic, outcome, droppedFieldsBody(outcome));
            }
        } else if (errorTopic != null) {
            send(errorTopic, outcome, body(outcome));
        }
    }

    private ObjectNode body(Outcome o) {
        ObjectNode node = JSON.createObjectNode();
        node.put("operation", o.operation() == null ? null : o.operation().name());
        node.put("table", o.table());
        node.put("sys_id", o.sysId());
        if (o.status() == null) {
            node.putNull("status");
        } else {
            node.put("status", o.status());
        }
        node.put("request_id", o.requestId());
        ObjectNode source = node.putObject("source");
        source.put("topic", o.record().topic());
        source.put("partition", o.record().kafkaPartition());
        source.put("offset", o.record().kafkaOffset());
        if (!o.isSuccess()) {
            RecordError e = o.error();
            node.put("classification", e.classification().name());
            node.put("retries", e.retries());
            node.put(
                    "response_excerpt",
                    e.responseExcerpt() == null ? null : Redaction.redactBody(e.responseExcerpt()));
            node.put(
                    "exception",
                    e.getCause() != null
                            ? e.getCause().getClass().getName()
                            : e.getClass().getName());
            node.put("message", Redaction.redactBody(e.getMessage()));
        }
        if (includeBody) {
            node.set("request_body", JSON.valueToTree(o.requestBody()));
        }
        return node;
    }

    private ObjectNode droppedFieldsBody(Outcome o) {
        ObjectNode node = body(o);
        node.put("classification", Classification.RECORD_ERROR.name());
        node.put("retries", 0);
        node.put(
                "response_excerpt",
                "Fields " + o.droppedFields() + " are not in the dictionary of " + o.table());
        node.put("exception", RecordError.class.getName());
        node.put(
                "message",
                "Unknown field(s) "
                        + o.droppedFields()
                        + " dropped under "
                        + SinkConfig.UNKNOWN_FIELD_BEHAVIOR
                        + "=report; the rest of the record was written");
        return node;
    }

    private void send(String topic, Outcome o, ObjectNode body) {
        byte[] key = o.sysId() == null ? null : o.sysId().getBytes(StandardCharsets.UTF_8);
        byte[] value;
        try {
            value = JSON.writeValueAsBytes(body);
        } catch (JsonProcessingException e) {
            throw new ConnectException("Cannot render reporter payload", e);
        }
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);
        if (metrics != null) {
            metrics.reported(topic.equals(successTopic));
        }
        record.headers()
                .add(HEADER_TOPIC, utf8(o.record().topic()))
                .add(HEADER_PARTITION, utf8(String.valueOf(o.record().kafkaPartition())))
                .add(HEADER_OFFSET, utf8(String.valueOf(o.record().kafkaOffset())))
                .add(HEADER_TABLE, utf8(o.table()))
                .add(HEADER_OPERATION, utf8(o.operation() == null ? null : o.operation().name()));
        if (!o.isSuccess()) {
            record.headers().add(HEADER_CLASSIFICATION, utf8(o.classification().name()));
        }
        producer.send(
                record,
                (metadata, exception) -> {
                    if (exception != null) {
                        LOG.error("Reporter send to {} failed", topic, exception);
                        sendFailure.compareAndSet(null, exception);
                    }
                });
    }

    /** Waits for every report to be acknowledged; throws if any send failed. */
    void flush() {
        if (producer == null) {
            return;
        }
        producer.flush();
        Exception failure = sendFailure.getAndSet(null);
        if (failure != null) {
            throw new ConnectException(
                    "A reporter record could not be published; offsets are not committed until"
                            + " reports are durable",
                    failure);
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            try {
                producer.close();
            } catch (RuntimeException e) {
                LOG.warn("Reporter producer close failed: {}", e.getMessage());
            }
        }
    }

    private static byte[] utf8(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }
}
