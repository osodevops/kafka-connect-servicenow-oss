package sh.oso.servicenow.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class ReporterTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String ID = "8f4bc0d1c611227a0100e2d3f8a6b9e1";

    private final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

    private Reporter reporter(Map<String, String> overrides) {
        Map<String, String> p = TestSupport.offlineProps(overrides);
        p.put(SinkConfig.REPORTER_BOOTSTRAP_SERVERS, "kafka:9092");
        p.putIfAbsent(SinkConfig.REPORTER_SUCCESS_TOPIC, "ok");
        p.putIfAbsent(SinkConfig.REPORTER_ERROR_TOPIC, "errors");
        return new Reporter(new SinkConfig(p), props -> producer);
    }

    private static SinkRecord source() {
        return TestSupport.record("incident-updates", 3, 42, ID, Map.of("state", "2"), null);
    }

    private static Outcome success() {
        return Outcome.success(
                source(),
                Operation.PATCH,
                "incident",
                ID,
                200,
                "req-1",
                0,
                Map.of("state", "2"),
                List.of());
    }

    private static Outcome failure() {
        RecordError e =
                new RecordError(
                        Classification.RECORD_ERROR,
                        "PATCH incident/" + ID + ": invalid field urgencyy",
                        new IllegalStateException("cause"),
                        400,
                        "req-2",
                        "{\"error\":{\"message\":\"Invalid field: urgencyy\"},\"password\":\"hunter2\"}",
                        1);
        return Outcome.failure(
                source(), Operation.PATCH, "incident", ID, Map.of("urgencyy", "1"), e);
    }

    private static String header(ProducerRecord<byte[], byte[]> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @Test
    void successReportBindsTheKafkaCoordinates() throws Exception {
        Reporter r = reporter(Map.of());
        assertThat(r.enabled()).isTrue();
        r.report(success());
        assertThat(producer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo("ok");
        assertThat(new String(sent.key(), StandardCharsets.UTF_8)).isEqualTo(ID);
        JsonNode body = JSON.readTree(sent.value());
        assertThat(body.get("operation").asText()).isEqualTo("PATCH");
        assertThat(body.get("table").asText()).isEqualTo("incident");
        assertThat(body.get("sys_id").asText()).isEqualTo(ID);
        assertThat(body.get("status").asInt()).isEqualTo(200);
        assertThat(body.get("request_id").asText()).isEqualTo("req-1");
        assertThat(body.get("source").get("topic").asText()).isEqualTo("incident-updates");
        assertThat(body.get("source").get("partition").asInt()).isEqualTo(3);
        assertThat(body.get("source").get("offset").asLong()).isEqualTo(42);
        assertThat(body.has("classification")).isFalse();
        assertThat(body.has("request_body")).isFalse();
        assertThat(header(sent, Reporter.HEADER_TOPIC)).isEqualTo("incident-updates");
        assertThat(header(sent, Reporter.HEADER_PARTITION)).isEqualTo("3");
        assertThat(header(sent, Reporter.HEADER_OFFSET)).isEqualTo("42");
        assertThat(header(sent, Reporter.HEADER_TABLE)).isEqualTo("incident");
        assertThat(header(sent, Reporter.HEADER_OPERATION)).isEqualTo("PATCH");
    }

    @Test
    void errorReportCarriesClassificationRetriesAndARedactedExcerpt() throws Exception {
        Reporter r = reporter(Map.of());
        r.report(failure());
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo("errors");
        JsonNode body = JSON.readTree(sent.value());
        assertThat(body.get("classification").asText()).isEqualTo("RECORD_ERROR");
        assertThat(body.get("retries").asInt()).isEqualTo(1);
        assertThat(body.get("status").asInt()).isEqualTo(400);
        assertThat(body.get("exception").asText()).isEqualTo(IllegalStateException.class.getName());
        assertThat(body.get("response_excerpt").asText())
                .contains("Invalid field: urgencyy")
                .contains("\"password\":\"***\"")
                .doesNotContain("hunter2");
        assertThat(body.get("message").asText()).contains("urgencyy");
        assertThat(header(sent, Reporter.HEADER_CLASSIFICATION)).isEqualTo("RECORD_ERROR");
    }

    @Test
    void requestBodyIsIncludedOnlyWhenConfigured() throws Exception {
        Reporter r = reporter(Map.of(SinkConfig.REPORTER_INCLUDE_REQUEST_BODY, "true"));
        r.report(success());
        JsonNode body = JSON.readTree(producer.history().get(0).value());
        assertThat(body.get("request_body").get("state").asText()).isEqualTo("2");
    }

    @Test
    void nullSysIdGivesANullKeyAndStatus() throws Exception {
        Reporter r = reporter(Map.of());
        RecordError e = new RecordError(Classification.AMBIGUOUS, "POST incident unknown outcome");
        r.report(
                Outcome.failure(
                        TestSupport.record(0, 1, null, Map.of()),
                        Operation.CREATE,
                        "incident",
                        null,
                        Map.of(),
                        e));
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.key()).isNull();
        JsonNode body = JSON.readTree(sent.value());
        assertThat(body.get("sys_id").isNull()).isTrue();
        assertThat(body.get("status").isNull()).isTrue();
        assertThat(body.get("classification").asText()).isEqualTo("AMBIGUOUS");
    }

    @Test
    void droppedUnknownFieldsAreReportedOnTheErrorTopicNextToTheSuccess() throws Exception {
        Reporter r = reporter(Map.of());
        r.report(
                Outcome.success(
                        source(),
                        Operation.PATCH,
                        "incident",
                        ID,
                        200,
                        "req-1",
                        0,
                        Map.of(),
                        List.of("urgencyy")));
        assertThat(producer.history())
                .extracting(ProducerRecord::topic)
                .containsExactly("ok", "errors");
        JsonNode body = JSON.readTree(producer.history().get(1).value());
        assertThat(body.get("message").asText()).contains("[urgencyy]");
        assertThat(body.get("status").asInt()).isEqualTo(200);
    }

    @Test
    void onlyConfiguredTopicsReceiveReports() {
        Map<String, String> p = TestSupport.offlineProps();
        p.put(SinkConfig.REPORTER_BOOTSTRAP_SERVERS, "kafka:9092");
        p.put(SinkConfig.REPORTER_ERROR_TOPIC, "errors");
        Reporter errorsOnly = new Reporter(new SinkConfig(p), props -> producer);
        errorsOnly.report(success());
        assertThat(producer.history()).isEmpty();
        errorsOnly.report(failure());
        assertThat(producer.history()).hasSize(1);

        Reporter disabled = new Reporter(TestSupport.config(Map.of()), props -> producer);
        assertThat(disabled.enabled()).isFalse();
        disabled.report(failure());
        disabled.flush();
        disabled.close();
        assertThat(producer.history()).hasSize(1);
    }

    @Test
    void flushWaitsForAcknowledgementAndSurfacesSendFailures() {
        Reporter r = reporter(Map.of());
        r.report(success());
        r.flush();
        assertThat(producer.flushed()).isTrue();

        MockProducer<byte[], byte[]> failing =
                new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        Map<String, String> p = TestSupport.offlineProps();
        p.put(SinkConfig.REPORTER_BOOTSTRAP_SERVERS, "kafka:9092");
        p.put(SinkConfig.REPORTER_SUCCESS_TOPIC, "ok");
        Reporter r2 = new Reporter(new SinkConfig(p), props -> failing);
        r2.report(success());
        failing.errorNext(new RuntimeException("broker down"));
        assertThatThrownBy(r2::flush)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("reporter");
        r2.close();
        assertThat(failing.closed()).isTrue();
    }

    @Test
    void producerIsBuiltFromTheReporterKeys() {
        Map<String, String> p = TestSupport.offlineProps();
        p.put(SinkConfig.REPORTER_BOOTSTRAP_SERVERS, "kafka:9092");
        p.put(SinkConfig.REPORTER_SUCCESS_TOPIC, "ok");
        p.put("snow.sink.reporter.producer.security.protocol", "SASL_SSL");
        p.put("snow.sink.reporter.producer.acks", "1");
        java.util.concurrent.atomic.AtomicReference<Map<String, Object>> seen =
                new java.util.concurrent.atomic.AtomicReference<>();
        new Reporter(
                new SinkConfig(p),
                props -> {
                    seen.set(props);
                    return producer;
                });
        assertThat(seen.get())
                .containsEntry("bootstrap.servers", "kafka:9092")
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("acks", "1")
                .containsEntry("key.serializer", ByteArraySerializer.class.getName());
    }
}
