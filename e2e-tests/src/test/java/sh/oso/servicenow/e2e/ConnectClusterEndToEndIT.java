package sh.oso.servicenow.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;
import sh.oso.servicenow.testing.MockServiceNowServer;
import sh.oso.servicenow.testing.TableStore;

/**
 * True end-to-end test: a real Kafka broker and a real Kafka Connect worker (Docker) load the
 * packaged plugin directories of both connectors with {@code plugin.discovery=service_load} and run
 * them against the fake ServiceNow on the host. It proves the plugin packaging and the KIP-898
 * service manifests, classloader isolation, config plumbing, the offset shape the worker exposes
 * and the full data path in both directions.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConnectClusterEndToEndIT {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectClusterEndToEndIT.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    // apache/kafka:3.9.0's docker init validates advertised.listeners before Testcontainers'
    // starter script exports them; 3.7.0 is the default and CI also runs a 4.x tag.
    private static final String KAFKA_IMAGE =
            System.getProperty("e2e.kafka.image", "apache/kafka:3.7.0");
    private static final String HOST_ALIAS = "host.testcontainers.internal";
    private static final String VERSION = System.getProperty("project.version", "0.0.1-SNAPSHOT");
    private static final String SOURCE_CLASS = "sh.oso.servicenow.source.ServiceNowSourceConnector";
    private static final String SINK_CLASS = "sh.oso.servicenow.sink.ServiceNowSinkConnector";
    private static final String SOURCE_NAME = "servicenow-source";
    private static final String SINK_NAME = "servicenow-sink";
    private static final String SOURCE_TOPIC = "servicenow.incident";
    private static final String SINK_TOPIC = "incident-updates";
    private static final String SNOW_TIMESTAMP = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";
    private static final Duration LIMIT = Duration.ofMinutes(2);
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    static MockServiceNowServer snow;
    static String snowUrl;
    static ScheduledExecutorService clockTicker;
    static Network network;
    static KafkaContainer kafka;
    static GenericContainer<?> connect;
    static String connectUrl;

    @BeforeAll
    static void setUp() {
        snow = MockServiceNowServer.start();
        // The fake's clock only moves by hand. Against a real worker sys_updated_on and the Date
        // header must follow wall-clock time, or the source's watermark never closes the rows
        // this test writes.
        clockTicker =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "fake-servicenow-clock");
                            t.setDaemon(true);
                            return t;
                        });
        clockTicker.scheduleAtFixedRate(
                ConnectClusterEndToEndIT::syncFakeClock, 0, 250, TimeUnit.MILLISECONDS);
        Testcontainers.exposeHostPorts(snow.port());
        snowUrl = "http://" + HOST_ALIAS + ":" + snow.port();
        snow.advertise(snowUrl);

        network = Network.newNetwork();
        kafka =
                new KafkaContainer(KAFKA_IMAGE)
                        .withNetwork(network)
                        .withNetworkAliases("kafka")
                        .withListener("kafka:19092")
                        .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("kafka"));
        kafka.start();

        Path repoRoot = Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
        connect =
                new GenericContainer<>(KAFKA_IMAGE)
                        .withNetwork(network)
                        .withExposedPorts(8083)
                        .withAccessToHost(true)
                        .withCreateContainerCmdModifier(
                                cmd ->
                                        cmd.withEntrypoint(
                                                "/opt/kafka/bin/connect-distributed.sh",
                                                "/connect.properties"))
                        .withCopyToContainer(
                                Transferable.of(workerProperties()), "/connect.properties")
                        .waitingFor(
                                Wait.forHttp("/connectors")
                                        .forPort(8083)
                                        .withStartupTimeout(Duration.ofMinutes(3)))
                        .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("connect"));
        for (String module : List.of("connect-servicenow-source", "connect-servicenow-sink")) {
            Path plugin =
                    repoRoot.resolve(module)
                            .resolve("target")
                            .resolve(module + "-" + VERSION + "-kafka-connect-plugin")
                            .resolve(module + "-" + VERSION);
            if (!Files.isDirectory(plugin)) {
                throw new IllegalStateException(
                        "Plugin dir missing (run mvn package first): " + plugin);
            }
            // Copy rather than bind-mount: Docker Desktop's host-path cache goes stale when
            // `mvn clean` recreates target directories just before container start.
            connect.withCopyFileToContainer(
                    MountableFile.forHostPath(plugin), "/plugins/" + module);
        }
        connect.start();
        connectUrl = "http://" + connect.getHost() + ":" + connect.getMappedPort(8083);
    }

    @AfterAll
    static void tearDown() {
        if (connect != null) {
            connect.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (network != null) {
            network.close();
        }
        if (clockTicker != null) {
            clockTicker.shutdownNow();
        }
        if (snow != null) {
            snow.close();
        }
    }

    private static void syncFakeClock() {
        snow.clock().set(Instant.now().truncatedTo(ChronoUnit.SECONDS));
    }

    private static String workerProperties() {
        return """
                bootstrap.servers=kafka:19092
                group.id=snow-e2e
                key.converter=org.apache.kafka.connect.storage.StringConverter
                value.converter=org.apache.kafka.connect.json.JsonConverter
                value.converter.schemas.enable=false
                config.storage.topic=_connect_configs
                offset.storage.topic=_connect_offsets
                status.storage.topic=_connect_status
                config.storage.replication.factor=1
                offset.storage.replication.factor=1
                status.storage.replication.factor=1
                offset.flush.interval.ms=2000
                plugin.path=/plugins
                plugin.discovery=service_load
                listeners=HTTP://0.0.0.0:8083
                """;
    }

    @Test
    @Order(1)
    void workerListsBothConnectorsThroughServiceLoadDiscovery() throws Exception {
        // service_load reads only META-INF/services; a plugin without a manifest is invisible.
        JsonNode plugins = getJson("/connector-plugins");
        Map<String, String> versions = new LinkedHashMap<>();
        for (JsonNode plugin : plugins) {
            versions.put(plugin.path("class").asText(), plugin.path("version").asText());
        }
        assertThat(versions).containsKeys(SOURCE_CLASS, SINK_CLASS);
        assertThat(versions.get(SOURCE_CLASS)).isEqualTo(VERSION);
        assertThat(versions.get(SINK_CLASS)).isEqualTo(VERSION);
    }

    @Test
    @Order(2)
    void sourceConnectorStreamsRowsAndLaterUpdatesIntoKafka() throws Exception {
        List<String> sysIds = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            sysIds.add(
                    snow.tables()
                            .insert(
                                    "incident",
                                    Map.of(
                                            "short_description",
                                            "e2e incident " + i,
                                            "state",
                                            "1")));
        }

        Map<String, String> config = new LinkedHashMap<>();
        config.put("connector.class", SOURCE_CLASS);
        config.put("tasks.max", "1");
        config.putAll(connectionProps());
        config.put("snow.tables", "incident");
        config.put("snow.table.incident.name", "incident");
        config.put("snow.table.incident.topic", SOURCE_TOPIC);
        config.put("snow.source.poll.interval.ms", "1000");
        registerConnector(SOURCE_NAME, config);
        awaitConnectorRunning(SOURCE_NAME, 1);

        try (KafkaConsumer<String, String> consumer = consumer(SOURCE_TOPIC)) {
            List<ConsumerRecord<String, String>> records =
                    awaitRecords(consumer, r -> sysIds.contains(r.key()), 3);
            assertThat(records)
                    .extracting(ConsumerRecord::key)
                    .containsExactlyInAnyOrderElementsOf(sysIds);
            for (ConsumerRecord<String, String> record : records) {
                JsonNode value = MAPPER.readTree(record.value());
                assertThat(value.path("sys_id").asText()).isEqualTo(record.key());
                assertThat(value.path("short_description").asText()).startsWith("e2e incident ");
                assertThat(value.path("sys_updated_on").asText()).matches(SNOW_TIMESTAMP);
                assertThat(header(record, "snow.source.operation")).isEqualTo("UPSERT");
                assertThat(header(record, "snow.table")).isEqualTo("incident");
            }

            String updatedId = sysIds.get(0);
            snow.tables()
                    .update(
                            "incident",
                            updatedId,
                            Map.of("short_description", "e2e incident 1 updated"));
            ConsumerRecord<String, String> updated =
                    awaitRecords(consumer, r -> updatedId.equals(r.key()), 1).get(0);
            assertThat(MAPPER.readTree(updated.value()).path("short_description").asText())
                    .isEqualTo("e2e incident 1 updated");
            assertThat(header(updated, "snow.source.operation")).isEqualTo("UPSERT");
        }

        // Kafka Connect 3.5+ exposes committed source offsets; the shape is part of the contract
        // documented for offset resets.
        await().atMost(LIMIT)
                .untilAsserted(
                        () -> {
                            JsonNode body = getJson("/connectors/" + SOURCE_NAME + "/offsets");
                            String shape = "offsets response: " + body;
                            JsonNode offsets = body.path("offsets");
                            assertThat(offsets).as(shape).isNotEmpty();
                            JsonNode partition = offsets.get(0).path("partition");
                            JsonNode offset = offsets.get(0).path("offset");
                            assertThat(partition.path("table").asText())
                                    .as(shape)
                                    .isEqualTo("incident");
                            assertThat(partition.path("instance").asText())
                                    .as(shape)
                                    .contains(HOST_ALIAS);
                            assertThat(partition.path("timestamp_field").asText())
                                    .as(shape)
                                    .isEqualTo("sys_updated_on");
                            assertThat(partition.path("query_fingerprint").asText())
                                    .as(shape)
                                    .startsWith("sha256:");
                            assertThat(offset.path("version").asText()).as(shape).isEqualTo("1");
                            assertThat(offset.path("timestamp").asText())
                                    .as(shape)
                                    .matches(SNOW_TIMESTAMP);
                            assertThat(offset.has("sys_id")).as(shape).isTrue();
                            assertThat(offset.path("phase").asText())
                                    .as(shape)
                                    .isIn("backfill", "stream");
                        });
    }

    @Test
    @Order(3)
    void sinkConnectorCreatesPatchesAndDeletesRowsThroughTheTableApi() throws Exception {
        // The default unknown-field policy (fail) looks the table up in sys_db_object and
        // sys_dictionary, so register incident the way a real instance would expose it.
        seedDictionary("incident", "short_description", "state", "number");
        try (Admin admin =
                Admin.create(
                        Map.of(
                                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                                kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(SINK_TOPIC, 2, (short) 1))).all().get();
        }

        Map<String, String> config = new LinkedHashMap<>();
        config.put("connector.class", SINK_CLASS);
        config.put("tasks.max", "2");
        config.put("topics", SINK_TOPIC);
        config.putAll(connectionProps());
        config.put("snow.sink.table", "incident");
        config.put("snow.sink.operation.mode", "key_value");
        config.put("snow.sink.update.method", "PATCH");
        registerConnector(SINK_NAME, config);
        awaitConnectorRunning(SINK_NAME, 2);

        try (KafkaProducer<String, String> producer = producer()) {
            // null key, no sys_id in the value: CREATE
            producer.send(
                            new ProducerRecord<>(
                                    SINK_TOPIC, null, "{\"short_description\":\"from kafka 1\"}"))
                    .get();
            producer.send(
                            new ProducerRecord<>(
                                    SINK_TOPIC, null, "{\"short_description\":\"from kafka 2\"}"))
                    .get();
            await().atMost(LIMIT)
                    .untilAsserted(
                            () -> {
                                List<String> descriptions =
                                        snow.tables().all("incident").stream()
                                                .map(r -> r.get("short_description"))
                                                .toList();
                                assertThat(descriptions)
                                        .containsOnlyOnce("from kafka 1", "from kafka 2");
                            });

            // String key = sys_id, value present: PATCH leaves the other fields untouched
            String target =
                    snow.tables()
                            .insert(
                                    "incident",
                                    Map.of("short_description", "e2e patch target", "state", "1"));
            producer.send(new ProducerRecord<>(SINK_TOPIC, target, "{\"state\":\"6\"}")).get();
            await().atMost(LIMIT)
                    .untilAsserted(
                            () -> {
                                Map<String, String> row =
                                        snow.tables().get("incident", target).orElseThrow();
                                assertThat(row.get("state")).isEqualTo("6");
                                assertThat(row.get("short_description"))
                                        .isEqualTo("e2e patch target");
                            });

            // tombstone: DELETE
            producer.send(new ProducerRecord<>(SINK_TOPIC, target, null)).get();
            await().atMost(LIMIT).until(() -> !snow.tables().exists("incident", target));
        }

        JsonNode status = getJson("/connectors/" + SINK_NAME + "/status");
        assertThat(status.path("tasks")).hasSize(2);
        for (JsonNode task : status.path("tasks")) {
            assertThat(task.path("state").asText())
                    .withFailMessage("sink task not running after writes: %s", status)
                    .isEqualTo("RUNNING");
        }
    }

    // ---------------------------------------------------------------- helpers

    /** Basic auth and fast retries against the fake, reached through the Testcontainers alias. */
    private static Map<String, String> connectionProps() {
        Map<String, String> props = new LinkedHashMap<>(snow.basicAuthProps());
        props.put("snow.url", snowUrl);
        props.putAll(MockServiceNowServer.fastRetryProps());
        return props;
    }

    private static void seedDictionary(String table, String... columns) {
        TableStore tables = snow.tables();
        tables.insert("sys_db_object", Map.of("name", table, "super_class", ""));
        for (String column : columns) {
            tables.insert("sys_dictionary", Map.of("name", table, "element", column));
        }
    }

    private static void registerConnector(String name, Map<String, String> config)
            throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("name", name, "config", config));
        HttpResponse<String> response =
                HTTP.send(
                        HttpRequest.newBuilder(URI.create(connectUrl + "/connectors"))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(body))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .withFailMessage("Connector registration failed: %s", response.body())
                .isIn(200, 201);
    }

    private static void awaitConnectorRunning(String name, int expectedTasks) {
        await().atMost(LIMIT)
                .untilAsserted(
                        () -> {
                            JsonNode status = getJson("/connectors/" + name + "/status");
                            assertThat(status.path("connector").path("state").asText())
                                    .withFailMessage("connector not running: %s", status)
                                    .isEqualTo("RUNNING");
                            assertThat(status.path("tasks")).hasSize(expectedTasks);
                            for (JsonNode task : status.path("tasks")) {
                                assertThat(task.path("state").asText())
                                        .withFailMessage("task not running: %s", status)
                                        .isEqualTo("RUNNING");
                            }
                        });
    }

    private static JsonNode getJson(String path) throws Exception {
        HttpResponse<String> response =
                HTTP.send(
                        HttpRequest.newBuilder(URI.create(connectUrl + path)).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .withFailMessage("GET %s failed: %s", path, response.body())
                .isEqualTo(200);
        return MAPPER.readTree(response.body());
    }

    private static KafkaConsumer<String, String> consumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-assert-" + topic);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private static KafkaProducer<String, String> producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /** Polls until {@code count} records matching {@code matching} have arrived. */
    private static List<ConsumerRecord<String, String>> awaitRecords(
            KafkaConsumer<String, String> consumer,
            Predicate<ConsumerRecord<String, String>> matching,
            int count) {
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        long deadline = System.nanoTime() + LIMIT.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                if (matching.test(record)) {
                    out.add(record);
                }
                if (out.size() >= count) {
                    return out;
                }
            }
        }
        throw new AssertionError(
                "Expected "
                        + count
                        + " matching record(s) within "
                        + LIMIT
                        + ", saw "
                        + out.size());
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
