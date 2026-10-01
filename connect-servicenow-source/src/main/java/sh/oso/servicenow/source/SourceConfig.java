package sh.oso.servicenow.source;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.config.CoreConfig;
import sh.oso.servicenow.config.CoreConfigDefs;
import sh.oso.servicenow.cursor.KeysetQueryBuilder;
import sh.oso.servicenow.metrics.ServiceNowMetrics;
import sh.oso.servicenow.schema.SchemaEvolution;
import sh.oso.servicenow.schema.SchemaMode;
import sh.oso.servicenow.schema.TypedFieldMapping;
import sh.oso.servicenow.table.EncodedQuery;
import sh.oso.servicenow.table.PathSegments;

/**
 * Source connector configuration: the global {@code snow.source.*} keys, the shared {@code snow.*}
 * core keys and one {@link TableSpec} per alias in {@code snow.tables}. The per-alias keys are
 * defined dynamically by {@link #configDef(Map)} so they are validated and documented like any
 * other key.
 */
public class SourceConfig extends AbstractConfig {

    public static final String GROUP_TABLES = "Tables";
    public static final String GROUP_SOURCE = "Source";
    public static final String GROUP_SCHEMA = "Schema";

    public static final String TABLES = "snow.tables";
    public static final String SCHEMA_MODE = "snow.source.schema.mode";
    public static final String POLL_INTERVAL_MS = "snow.source.poll.interval.ms";
    public static final String BATCH_SIZE = "snow.source.batch.size";
    public static final String OVERLAP_SECONDS = "snow.source.overlap.seconds";
    public static final String SAFETY_LAG_SECONDS = "snow.source.safety.lag.seconds";
    public static final String DEDUP_WINDOW_RECORDS = "snow.source.dedup.window.records";
    public static final String EMIT_ENVELOPE = "snow.source.emit.envelope";
    public static final String SCHEMA_EVOLUTION = "snow.source.schema.evolution";
    public static final String TYPED_FIELDS = "snow.source.typed.fields";
    public static final String BAD_ROW_BEHAVIOR = "snow.source.bad.row.behavior";
    public static final String STARTUP_PROBE = "snow.source.startup.probe";

    /** Internal: aliases assigned to one task by the connector. */
    public static final String TASK_TABLES = "snow.task.tables";

    /** Internal: the task's index, set by the connector; names the task's JMX MBeans. */
    public static final String TASK_ID = ServiceNowMetrics.TASK_ID_KEY;

    public static final String SCHEMA_MODE_SCHEMALESS = "schemaless";
    public static final String SCHEMA_MODE_STRINGS = "strings";
    public static final String SCHEMA_MODE_TYPED = "typed";
    public static final String BAD_ROW_FAIL = "fail";
    public static final String BAD_ROW_SKIP = "skip";

    public static final int DEFAULT_POLL_INTERVAL_MS = 5000;
    public static final int DEFAULT_BATCH_SIZE = 5000;
    public static final int DEFAULT_OVERLAP_SECONDS = 2;
    public static final int DEFAULT_SAFETY_LAG_SECONDS = 1;
    public static final int DEFAULT_DEDUP_WINDOW_RECORDS = 50_000;
    public static final int MAX_BATCH_SIZE = 50_000;

    private static final Pattern FIELD_NAME = Pattern.compile("^[A-Za-z0-9_]+$");

    /** What to do with a row whose cursor fields cannot be read. */
    public enum BadRowBehavior {
        FAIL,
        SKIP;

        static BadRowBehavior fromConfig(String v) {
            return valueOf(v.trim().toUpperCase(Locale.ROOT));
        }
    }

    private final List<String> aliases;
    private final List<TableSpec> tables;
    private final CoreConfig core;

    public SourceConfig(Map<String, String> originals) {
        super(configDef(originals), originals, false);
        this.aliases = validateAliases(getList(TABLES));
        this.core = new CoreConfig(this);
        this.tables = buildTables();
        validateSchemaSettings();
    }

    /** The static definition: globals plus the shared core keys (no per-alias keys). */
    public static ConfigDef configDef() {
        ConfigDef def = new ConfigDef();
        int t = 0;
        def.define(
                TABLES,
                Type.LIST,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                "Comma-separated aliases, one per table to poll. Each alias is configured with"
                        + " `snow.table.<alias>.*` keys; aliases are lower-case letters, digits and"
                        + " underscores. Tables are assigned whole to tasks by rendezvous hashing.",
                GROUP_TABLES,
                ++t,
                Width.LONG,
                "Tables");
        def.define(
                TASK_TABLES,
                Type.LIST,
                "",
                Importance.LOW,
                "Internal: aliases assigned to this task by the connector. Do not set.",
                GROUP_TABLES,
                ++t,
                Width.LONG,
                "Task tables");
        def.define(
                TASK_ID,
                Type.INT,
                -1,
                Importance.LOW,
                "Internal: the task index the connector assigned, used in the `task=` key of the"
                        + " task's JMX MBeans. Do not set.",
                GROUP_TABLES,
                ++t,
                Width.SHORT,
                "Task id");

        int s = 0;
        def.define(
                POLL_INTERVAL_MS,
                Type.INT,
                DEFAULT_POLL_INTERVAL_MS,
                Range.atLeast(1),
                Importance.MEDIUM,
                "Milliseconds a table waits after a sweep that ended on a short page before"
                        + " polling again. A full page continues immediately. Overridable per alias.",
                GROUP_SOURCE,
                ++s,
                Width.SHORT,
                "Poll interval (ms)");
        def.define(
                BATCH_SIZE,
                Type.INT,
                DEFAULT_BATCH_SIZE,
                Range.between(1, MAX_BATCH_SIZE),
                Importance.MEDIUM,
                "Rows requested per Table API call (`sysparm_limit`). Overridable per alias.",
                GROUP_SOURCE,
                ++s,
                Width.SHORT,
                "Batch size");
        def.define(
                OVERLAP_SECONDS,
                Type.INT,
                DEFAULT_OVERLAP_SECONDS,
                Range.atLeast(0),
                Importance.LOW,
                "Seconds re-read before the committed cursor on restart and at the start of every"
                        + " streaming sweep, so rows committed late with an earlier timestamp are"
                        + " still seen. Re-read rows are suppressed by the dedup cache; after a"
                        + " restart they are delivered again (at-least-once).",
                GROUP_SOURCE,
                ++s,
                Width.SHORT,
                "Overlap (s)");
        def.define(
                SAFETY_LAG_SECONDS,
                Type.INT,
                DEFAULT_SAFETY_LAG_SECONDS,
                Range.atLeast(0),
                Importance.LOW,
                "Seconds subtracted from the instance clock to form a sweep's high-water mark, so"
                        + " the second still being written is never closed early.",
                GROUP_SOURCE,
                ++s,
                Width.SHORT,
                "Safety lag (s)");
        def.define(
                DEDUP_WINDOW_RECORDS,
                Type.INT,
                DEFAULT_DEDUP_WINDOW_RECORDS,
                Range.atLeast(1),
                Importance.LOW,
                "Per-table bound on the in-memory cache of emitted (timestamp, sys_id,"
                        + " sys_mod_count) versions used to suppress overlap re-reads.",
                GROUP_SOURCE,
                ++s,
                Width.SHORT,
                "Dedup window (records)");
        def.define(
                BAD_ROW_BEHAVIOR,
                Type.STRING,
                BAD_ROW_FAIL,
                ValidString.in(BAD_ROW_FAIL, BAD_ROW_SKIP),
                Importance.LOW,
                "A row whose sys_id or timestamp field is missing or unparseable either fails the"
                        + " task naming the row (`fail`) or is logged, counted and skipped (`skip`).",
                GROUP_SOURCE,
                ++s,
                Width.SHORT,
                "Bad row behaviour");
        def.define(
                STARTUP_PROBE,
                Type.BOOLEAN,
                true,
                Importance.LOW,
                "On task start, read one row per table to verify access and the cursor fields, and"
                        + " warn when the integration user's time zone is not UTC.",
                GROUP_SOURCE,
                ++s,
                Width.SHORT,
                "Startup probe");

        int m = 0;
        def.define(
                SCHEMA_MODE,
                Type.STRING,
                SCHEMA_MODE_SCHEMALESS,
                ValidString.in(SCHEMA_MODE_SCHEMALESS, SCHEMA_MODE_STRINGS, SCHEMA_MODE_TYPED),
                Importance.HIGH,
                "Record value shape: `schemaless` (a map, for the JSON converter without schemas),"
                        + " `strings` (a Struct of optional strings whose schema only grows) or"
                        + " `typed` (a Struct from snow.source.typed.fields; experimental).",
                GROUP_SCHEMA,
                ++m,
                Width.SHORT,
                "Schema mode");
        def.define(
                TYPED_FIELDS,
                Type.LIST,
                "",
                Importance.MEDIUM,
                "`field:type` entries for `typed` mode, for example"
                        + " `sys_id:string,priority:int32,opened_at:timestamp,active:boolean`. Types:"
                        + " string, int8, int16, int32, int64, float32, float64, boolean, bytes,"
                        + " timestamp, date, time, decimal, decimal(scale).",
                GROUP_SCHEMA,
                ++m,
                Width.LONG,
                "Typed fields");
        def.define(
                SCHEMA_EVOLUTION,
                Type.STRING,
                "fail",
                ValidString.in("fail", "backward", "permissive"),
                Importance.LOW,
                "In `typed` mode, what happens with a field that is not in the mapping or does"
                        + " not coerce: `fail` rejects the record, `backward` adds unknown fields as"
                        + " optional strings but still rejects bad values, `permissive` adds unknown"
                        + " fields and nulls bad values.",
                GROUP_SCHEMA,
                ++m,
                Width.SHORT,
                "Schema evolution");
        def.define(
                EMIT_ENVELOPE,
                Type.BOOLEAN,
                false,
                Importance.LOW,
                "Wrap each value as `{before: null, after, source: {table, instance}, op: \"u\","
                        + " ts_ms}`. `before` is always null because the Table API has no previous"
                        + " image.",
                GROUP_SCHEMA,
                ++m,
                Width.SHORT,
                "Emit envelope");

        return CoreConfigDefs.addCore(def);
    }

    /**
     * The static definition plus the {@code snow.table.<alias>.*} keys for every alias in {@code
     * originals}.
     */
    public static ConfigDef configDef(Map<String, ?> originals) {
        ConfigDef def = configDef();
        for (String alias : rawAliases(originals)) {
            if (TableSpec.ALIAS.matcher(alias).matches()) {
                TableSpec.addTo(def, alias);
            }
        }
        return def;
    }

    /** Aliases as written in {@code snow.tables}, before validation. */
    public static List<String> rawAliases(Map<String, ?> originals) {
        Object raw = originals == null ? null : originals.get(TABLES);
        if (raw == null) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        Iterable<?> items =
                raw instanceof List<?> l ? l : List.of(String.valueOf(raw).split(",", -1));
        for (Object o : items) {
            String s = o == null ? "" : o.toString().trim();
            if (!s.isEmpty() && !out.contains(s)) {
                out.add(s);
            }
        }
        return out;
    }

    private static List<String> validateAliases(List<String> raw) {
        List<String> out = new ArrayList<>();
        for (String a : raw) {
            String alias = a == null ? "" : a.trim();
            if (alias.isEmpty()) {
                continue;
            }
            if (!TableSpec.ALIAS.matcher(alias).matches()) {
                throw new ConfigException(
                        TABLES,
                        raw,
                        "alias '"
                                + alias
                                + "' must contain only lower-case letters, digits and underscores");
            }
            if (out.contains(alias)) {
                throw new ConfigException(TABLES, raw, "alias '" + alias + "' is listed twice");
            }
            out.add(alias);
        }
        if (out.isEmpty()) {
            throw new ConfigException(TABLES, raw, "at least one table alias is required");
        }
        return List.copyOf(out);
    }

    private List<TableSpec> buildTables() {
        TableSpec.Defaults defaults =
                new TableSpec.Defaults(getInt(BATCH_SIZE), getInt(POLL_INTERVAL_MS));
        List<TableSpec> out = new ArrayList<>();
        for (String alias : aliases) {
            out.add(validate(buildTable(alias, defaults)));
        }
        return List.copyOf(out);
    }

    private TableSpec buildTable(String alias, TableSpec.Defaults defaults) {
        try {
            return TableSpec.from(alias, this, defaults);
        } catch (ServiceNowException e) {
            throw new ConfigException(
                    TableSpec.key(alias, TableSpec.START_TIMESTAMP),
                    getString(TableSpec.key(alias, TableSpec.START_TIMESTAMP)),
                    e.getMessage());
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                    TableSpec.key(alias, TableSpec.DISPLAY_VALUE),
                    getString(TableSpec.key(alias, TableSpec.DISPLAY_VALUE)),
                    e.getMessage());
        }
    }

    private static TableSpec validate(TableSpec spec) {
        String alias = spec.alias();
        if (!PathSegments.isTable(spec.name())) {
            throw new ConfigException(
                    spec.key(TableSpec.NAME),
                    spec.name(),
                    "must be a ServiceNow table or view name: lower-case letters, digits and"
                            + " underscores");
        }
        requireFieldName(spec.key(TableSpec.TIMESTAMP_FIELD), spec.timestampField());
        requireFieldName(spec.key(TableSpec.SYS_ID_FIELD), spec.sysIdField());
        if (spec.timestampField().equals(spec.sysIdField())) {
            throw new ConfigException(
                    spec.key(TableSpec.TIMESTAMP_FIELD),
                    spec.timestampField(),
                    "must differ from " + spec.key(TableSpec.SYS_ID_FIELD));
        }
        if (!spec.fields().isEmpty()) {
            Set<String> fields = new LinkedHashSet<>();
            for (String f : spec.fields()) {
                fields.add(f.trim());
            }
            List<String> missing = new ArrayList<>();
            if (!fields.contains(spec.sysIdField())) {
                missing.add(spec.sysIdField());
            }
            if (!fields.contains(spec.timestampField())) {
                missing.add(spec.timestampField());
            }
            if (!missing.isEmpty()) {
                throw new ConfigException(
                        spec.key(TableSpec.FIELDS),
                        spec.fields(),
                        "must include the cursor field(s) "
                                + missing
                                + " for alias '"
                                + alias
                                + "'");
            }
        }
        try {
            KeysetQueryBuilder.validateBase(EncodedQuery.of(spec.query()));
        } catch (ServiceNowException e) {
            throw new ConfigException(spec.key(TableSpec.QUERY), spec.query(), e.getMessage());
        }
        if (spec.queryCategory() != null && spec.queryCategory().contains("&")) {
            throw new ConfigException(
                    spec.key(TableSpec.QUERY_CATEGORY),
                    spec.queryCategory(),
                    "must not contain '&'");
        }
        return spec;
    }

    private static void requireFieldName(String key, String value) {
        if (value == null || !FIELD_NAME.matcher(value).matches()) {
            throw new ConfigException(
                    key, value, "must be a field name (letters, digits and underscores)");
        }
    }

    private void validateSchemaSettings() {
        SchemaMode mode = schemaMode();
        List<String> typed = getList(TYPED_FIELDS);
        if (mode == SchemaMode.TYPED && typed.isEmpty()) {
            throw new ConfigException(
                    TYPED_FIELDS,
                    typed,
                    "must list at least one field:type entry when "
                            + SCHEMA_MODE
                            + "="
                            + SCHEMA_MODE_TYPED);
        }
        if (!typed.isEmpty()) {
            try {
                TypedFieldMapping.parse(typed, schemaEvolution());
            } catch (IllegalArgumentException e) {
                throw new ConfigException(TYPED_FIELDS, typed, e.getMessage());
            }
        }
    }

    // --- accessors ---

    public CoreConfig core() {
        return core;
    }

    /** Every alias in {@code snow.tables}, in configured order. */
    public List<String> aliases() {
        return aliases;
    }

    /** Every table spec, in configured order. */
    public List<TableSpec> tables() {
        return tables;
    }

    public TableSpec table(String alias) {
        for (TableSpec t : tables) {
            if (t.alias().equals(alias)) {
                return t;
            }
        }
        throw new ConfigException(TASK_TABLES, alias, "is not an alias in " + TABLES);
    }

    /** The specs assigned to this task ({@code snow.task.tables}), or every table when unset. */
    public List<TableSpec> taskTables() {
        List<String> assigned = getList(TASK_TABLES);
        if (assigned.isEmpty()) {
            return tables;
        }
        List<TableSpec> out = new ArrayList<>();
        for (String alias : assigned) {
            out.add(table(alias.trim()));
        }
        return List.copyOf(out);
    }

    public SchemaMode schemaMode() {
        return SchemaMode.fromConfig(getString(SCHEMA_MODE));
    }

    public SchemaEvolution schemaEvolution() {
        return SchemaEvolution.fromConfig(getString(SCHEMA_EVOLUTION));
    }

    /** The typed mapping, or null outside typed mode. */
    public TypedFieldMapping typedFields() {
        List<String> typed = getList(TYPED_FIELDS);
        return typed.isEmpty() ? null : TypedFieldMapping.parse(typed, schemaEvolution());
    }

    public Duration overlap() {
        return Duration.ofSeconds(getInt(OVERLAP_SECONDS));
    }

    public Duration safetyLag() {
        return Duration.ofSeconds(getInt(SAFETY_LAG_SECONDS));
    }

    public int dedupWindowRecords() {
        return getInt(DEDUP_WINDOW_RECORDS);
    }

    public boolean emitEnvelope() {
        return getBoolean(EMIT_ENVELOPE);
    }

    public BadRowBehavior badRowBehavior() {
        return BadRowBehavior.fromConfig(getString(BAD_ROW_BEHAVIOR));
    }

    public boolean startupProbe() {
        return getBoolean(STARTUP_PROBE);
    }
}
