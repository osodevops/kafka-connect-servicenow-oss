package sh.oso.servicenow.source;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.servicenow.cursor.Cursor;
import sh.oso.servicenow.cursor.SourceOffset;
import sh.oso.servicenow.schema.RecordSchemaMapper;
import sh.oso.servicenow.schema.SchemaMode;
import sh.oso.servicenow.table.Record;

/**
 * Builds {@link SourceRecord}s for one table: key is the sys_id field as a string, the value comes
 * from the table's {@link RecordSchemaMapper}, the record timestamp is the cursor timestamp, and
 * every record carries the {@code snow.*} headers. The source never claims deletes, so the
 * operation header is always {@value #OPERATION_UPSERT}.
 */
final class SourceRecordFactory {

    static final String HEADER_TABLE = "snow.table";
    static final String HEADER_INSTANCE = "snow.instance";
    static final String HEADER_OPERATION = "snow.source.operation";
    static final String HEADER_EXTRACTED_AT = "snow.extracted_at";
    static final String HEADER_SCHEMA_MODE = "snow.schema.mode";
    static final String OPERATION_UPSERT = "UPSERT";

    static final String ENVELOPE_BEFORE = "before";
    static final String ENVELOPE_AFTER = "after";
    static final String ENVELOPE_SOURCE = "source";
    static final String ENVELOPE_OP = "op";
    static final String ENVELOPE_TS_MS = "ts_ms";
    static final String OP_UPDATE = "u";

    static final Schema SOURCE_SCHEMA =
            SchemaBuilder.struct()
                    .name("sh.oso.servicenow.Source")
                    .field("table", Schema.STRING_SCHEMA)
                    .field("instance", Schema.STRING_SCHEMA)
                    .build();

    private final TableSpec spec;
    private final String instanceHost;
    private final Map<String, String> partition;
    private final RecordSchemaMapper mapper;
    private final boolean envelope;
    private final Clock clock;
    private final String schemaModeName;
    private Schema envelopeAfterSchema;
    private Schema envelopeSchema;

    SourceRecordFactory(
            TableSpec spec,
            String instanceHost,
            Map<String, String> partition,
            RecordSchemaMapper mapper,
            boolean envelope,
            Clock clock) {
        this.spec = Objects.requireNonNull(spec, "spec");
        this.instanceHost = Objects.requireNonNull(instanceHost, "instanceHost");
        this.partition = Map.copyOf(partition);
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.envelope = envelope;
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.schemaModeName = mapper.mode().name().toLowerCase(java.util.Locale.ROOT);
    }

    SchemaMode mode() {
        return mapper.mode();
    }

    /** Version of the current value schema, or null in schemaless mode. */
    Integer schemaVersion() {
        Schema s = mapper.currentSchema();
        return s == null ? null : s.version();
    }

    SourceRecord create(Record row, Cursor cursor, SourceOffset offset) {
        SchemaAndValue value = mapper.map(row);
        Schema valueSchema = value.schema();
        Object valueObject = value.value();
        long tsMs = cursor.ts().toEpochMilli();
        if (envelope) {
            if (valueSchema == null) {
                valueObject = schemalessEnvelope(valueObject, tsMs);
            } else {
                Schema env = envelopeSchemaFor(valueSchema);
                Struct source =
                        new Struct(SOURCE_SCHEMA)
                                .put("table", spec.name())
                                .put("instance", instanceHost);
                valueObject =
                        new Struct(env)
                                .put(ENVELOPE_BEFORE, null)
                                .put(ENVELOPE_AFTER, valueObject)
                                .put(ENVELOPE_SOURCE, source)
                                .put(ENVELOPE_OP, OP_UPDATE)
                                .put(ENVELOPE_TS_MS, tsMs);
                valueSchema = env;
            }
        }
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString(HEADER_TABLE, spec.name());
        headers.addString(HEADER_INSTANCE, instanceHost);
        headers.addString(HEADER_OPERATION, OPERATION_UPSERT);
        headers.addLong(HEADER_EXTRACTED_AT, clock.millis());
        headers.addString(HEADER_SCHEMA_MODE, schemaModeName);
        return new SourceRecord(
                partition,
                offset.toMap(),
                spec.topic(),
                null,
                Schema.STRING_SCHEMA,
                row.string(spec.sysIdField()),
                valueSchema,
                valueObject,
                tsMs,
                headers);
    }

    private Map<String, Object> schemalessEnvelope(Object after, long tsMs) {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("table", spec.name());
        source.put("instance", instanceHost);
        Map<String, Object> env = new LinkedHashMap<>();
        env.put(ENVELOPE_BEFORE, null);
        env.put(ENVELOPE_AFTER, after);
        env.put(ENVELOPE_SOURCE, source);
        env.put(ENVELOPE_OP, OP_UPDATE);
        env.put(ENVELOPE_TS_MS, tsMs);
        return env;
    }

    private synchronized Schema envelopeSchemaFor(Schema after) {
        if (envelopeSchema == null || envelopeAfterSchema != after) {
            SchemaBuilder b =
                    SchemaBuilder.struct()
                            .name("sh.oso.servicenow." + spec.name() + ".Envelope")
                            .field(ENVELOPE_BEFORE, after)
                            .field(ENVELOPE_AFTER, after)
                            .field(ENVELOPE_SOURCE, SOURCE_SCHEMA)
                            .field(ENVELOPE_OP, Schema.STRING_SCHEMA)
                            .field(ENVELOPE_TS_MS, Schema.INT64_SCHEMA);
            if (after.version() != null) {
                b.version(after.version());
            }
            envelopeSchema = b.build();
            envelopeAfterSchema = after;
        }
        return envelopeSchema;
    }
}
