package sh.oso.servicenow.sink;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import sh.oso.servicenow.config.CoreConfigDefs;
import sh.oso.servicenow.testing.MockServiceNowServer;
import sh.oso.servicenow.testing.TableStore;

/** Shared builders for sink tests: connector properties against the fake, records and headers. */
final class TestSupport {

    static final String TOPIC = "incident-updates";

    private TestSupport() {}

    /** Basic auth, fast retries, fixed routing to {@code incident}, no dictionary lookup. */
    static Map<String, String> props(MockServiceNowServer snow) {
        Map<String, String> p = new LinkedHashMap<>(snow.basicAuthProps());
        p.putAll(MockServiceNowServer.fastRetryProps());
        p.put(SinkConfig.TABLE, "incident");
        p.put(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "passthrough");
        return p;
    }

    static Map<String, String> props(MockServiceNowServer snow, Map<String, String> overrides) {
        Map<String, String> p = props(snow);
        p.putAll(overrides);
        return p;
    }

    /** Properties with no instance at all, for pure mapping and config tests. */
    static Map<String, String> offlineProps() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(CoreConfigDefs.URL, "https://acme.service-now.com");
        p.put(CoreConfigDefs.AUTH_USERNAME, "connect");
        p.put(CoreConfigDefs.AUTH_PASSWORD, "secret");
        p.put(SinkConfig.TABLE, "incident");
        p.put(SinkConfig.UNKNOWN_FIELD_BEHAVIOR, "passthrough");
        return p;
    }

    static Map<String, String> offlineProps(Map<String, String> overrides) {
        Map<String, String> p = offlineProps();
        p.putAll(overrides);
        return p;
    }

    static SinkConfig config(Map<String, String> overrides) {
        return new SinkConfig(offlineProps(overrides));
    }

    /** Registers {@code table} and its columns in the fake's sys_db_object and sys_dictionary. */
    static void seedDictionary(MockServiceNowServer snow, String table, String... columns) {
        TableStore t = snow.tables();
        t.insert("sys_db_object", Map.of("name", table, "super_class", ""));
        for (String c : columns) {
            t.insert("sys_dictionary", Map.of("name", table, "element", c));
        }
    }

    static Headers headers(String... nameValuePairs) {
        ConnectHeaders h = new ConnectHeaders();
        for (int i = 0; i + 1 < nameValuePairs.length; i += 2) {
            h.addString(nameValuePairs[i], nameValuePairs[i + 1]);
        }
        return h;
    }

    static SinkRecord record(int partition, long offset, Object key, Object value) {
        return record(TOPIC, partition, offset, key, value, null);
    }

    static SinkRecord record(int partition, long offset, Object key, Object value, Headers h) {
        return record(TOPIC, partition, offset, key, value, h);
    }

    static SinkRecord record(
            String topic, int partition, long offset, Object key, Object value, Headers h) {
        Schema keySchema = key instanceof String ? Schema.OPTIONAL_STRING_SCHEMA : null;
        return new SinkRecord(topic, partition, keySchema, key, null, value, offset, null, null, h);
    }

    static SinkRecord struct(
            int partition, long offset, Object key, Schema valueSchema, Object value, Headers h) {
        Schema keySchema = key instanceof String ? Schema.OPTIONAL_STRING_SCHEMA : null;
        return new SinkRecord(
                TOPIC, partition, keySchema, key, valueSchema, value, offset, null, null, h);
    }
}
