package sh.oso.servicenow.source;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Width;
import sh.oso.servicenow.cursor.QueryFingerprint;
import sh.oso.servicenow.table.DisplayValue;

/**
 * One table (or view) to poll, resolved from the {@code snow.table.<alias>.*} keys plus the global
 * defaults. {@link #defFor(String)} is the per-alias {@link ConfigDef} reused by the connector's
 * {@code validate()} and by the documentation generator.
 */
public record TableSpec(
        String alias,
        String name,
        String topic,
        Instant startTimestamp,
        String timestampField,
        String sysIdField,
        String query,
        List<String> fields,
        DisplayValue displayValue,
        boolean excludeReferenceLink,
        boolean queryDomain,
        String queryCategory,
        int batchSize,
        long pollIntervalMs) {

    public static final String PREFIX = "snow.table.";
    public static final Pattern ALIAS = Pattern.compile("^[a-z0-9_]+$");

    public static final String NAME = "name";
    public static final String TOPIC = "topic";
    public static final String START_TIMESTAMP = "start.timestamp";
    public static final String TIMESTAMP_FIELD = "timestamp.field";
    public static final String SYS_ID_FIELD = "sys.id.field";
    public static final String QUERY = "query";
    public static final String FIELDS = "fields";
    public static final String DISPLAY_VALUE = "display.value";
    public static final String EXCLUDE_REFERENCE_LINK = "exclude.reference.link";
    public static final String QUERY_DOMAIN = "query.domain";
    public static final String QUERY_CATEGORY = "query.category";
    public static final String BATCH_SIZE = "batch.size";
    public static final String POLL_INTERVAL_MS = "poll.interval.ms";

    public static final String DEFAULT_START_TIMESTAMP = "1970-01-01 00:00:00";
    public static final String DEFAULT_TIMESTAMP_FIELD = "sys_updated_on";
    public static final String DEFAULT_SYS_ID_FIELD = "sys_id";

    public TableSpec {
        Objects.requireNonNull(alias, "alias");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(startTimestamp, "startTimestamp");
        Objects.requireNonNull(timestampField, "timestampField");
        Objects.requireNonNull(sysIdField, "sysIdField");
        query = query == null ? "" : query.trim();
        fields = fields == null ? List.of() : List.copyOf(fields);
        displayValue = displayValue == null ? DisplayValue.FALSE : displayValue;
        queryCategory = queryCategory == null || queryCategory.isBlank() ? null : queryCategory;
    }

    /** The full key for {@code suffix} under this alias. */
    public static String key(String alias, String suffix) {
        return PREFIX + alias + "." + suffix;
    }

    public String key(String suffix) {
        return key(alias, suffix);
    }

    /** {@code sha256:} over table, normalised base query and the domain flag (ADR 0001). */
    public String fingerprint() {
        return QueryFingerprint.of(name, query, queryDomain);
    }

    /** Group name used in the {@link ConfigDef} for this alias. */
    public static String group(String alias) {
        return "Table " + alias;
    }

    /** The {@code snow.table.<alias>.*} definitions for one alias. */
    public static ConfigDef defFor(String alias) {
        return addTo(new ConfigDef(), alias);
    }

    /** Adds the {@code snow.table.<alias>.*} definitions to {@code def} and returns it. */
    public static ConfigDef addTo(ConfigDef def, String alias) {
        String group = group(alias);
        int order = 0;
        def.define(
                key(alias, NAME),
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                Importance.HIGH,
                "ServiceNow table or database view to poll, for example `incident`. Lower-case"
                        + " letters, digits and underscores only. The partition is keyed on this name,"
                        + " not on the alias.",
                group,
                ++order,
                Width.MEDIUM,
                "Table");
        def.define(
                key(alias, TOPIC),
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                Importance.HIGH,
                "Kafka topic that receives this table's records.",
                group,
                ++order,
                Width.MEDIUM,
                "Topic");
        def.define(
                key(alias, START_TIMESTAMP),
                Type.STRING,
                DEFAULT_START_TIMESTAMP,
                Importance.MEDIUM,
                "Where the backfill starts when no offset is stored for this table: rows whose"
                        + " timestamp field is at or after this UTC `yyyy-MM-dd HH:mm:ss` value are"
                        + " emitted. Ignored once an offset exists.",
                group,
                ++order,
                Width.MEDIUM,
                "Start timestamp");
        def.define(
                key(alias, TIMESTAMP_FIELD),
                Type.STRING,
                DEFAULT_TIMESTAMP_FIELD,
                new ConfigDef.NonEmptyString(),
                Importance.MEDIUM,
                "Cursor timestamp field. `sys_updated_on` captures creates and updates;"
                        + " `sys_created_on` captures creates only. For a database view use the"
                        + " prefixed column, for example `inc_sys_updated_on`.",
                group,
                ++order,
                Width.MEDIUM,
                "Timestamp field");
        def.define(
                key(alias, SYS_ID_FIELD),
                Type.STRING,
                DEFAULT_SYS_ID_FIELD,
                new ConfigDef.NonEmptyString(),
                Importance.LOW,
                "Cursor identity field and record key. Only database views need to change it, to"
                        + " the prefixed column such as `inc_sys_id`.",
                group,
                ++order,
                Width.MEDIUM,
                "sys_id field");
        def.define(
                key(alias, QUERY),
                Type.STRING,
                "",
                Importance.MEDIUM,
                "Base encoded query ANDed with the cursor predicates, for example"
                        + " `active=true^priority<=2`. Must not contain ORDERBY; the connector orders"
                        + " by the cursor fields. Changing it changes the partition (the table starts"
                        + " again from its start timestamp).",
                group,
                ++order,
                Width.LONG,
                "Base query");
        def.define(
                key(alias, FIELDS),
                Type.LIST,
                "",
                Importance.MEDIUM,
                "Projection (`sysparm_fields`). Empty means every field. When set it must include"
                        + " the sys_id field and the timestamp field; `sys_mod_count` is added"
                        + " automatically.",
                group,
                ++order,
                Width.LONG,
                "Fields");
        def.define(
                key(alias, DISPLAY_VALUE),
                Type.STRING,
                "false",
                ValidString.in("false", "true", "all"),
                Importance.LOW,
                "`sysparm_display_value`: `false` emits raw values, `all` emits `{value,"
                        + " display_value, link}` per field, and `true` emits display values for"
                        + " every field except the cursor fields, which keep their raw values so the"
                        + " cursor stays parseable (fetched as `all` on the wire).",
                group,
                ++order,
                Width.SHORT,
                "Display value");
        def.define(
                key(alias, EXCLUDE_REFERENCE_LINK),
                Type.BOOLEAN,
                true,
                Importance.LOW,
                "`sysparm_exclude_reference_link`: drop the `link` of reference fields.",
                group,
                ++order,
                Width.SHORT,
                "Exclude reference link");
        def.define(
                key(alias, QUERY_DOMAIN),
                Type.BOOLEAN,
                true,
                Importance.LOW,
                "`true` restricts rows to the integration user's domains (ServiceNow default)."
                        + " `false` sends `sysparm_query_no_domain=true`, which needs the"
                        + " `query_no_domain_table_api` role. Part of the partition fingerprint.",
                group,
                ++order,
                Width.SHORT,
                "Query domain");
        def.define(
                key(alias, QUERY_CATEGORY),
                Type.STRING,
                null,
                Importance.LOW,
                "`sysparm_query_category` for instance-side query routing and auditing.",
                group,
                ++order,
                Width.MEDIUM,
                "Query category");
        def.define(
                key(alias, BATCH_SIZE),
                Type.INT,
                null,
                Importance.LOW,
                "Rows per request for this table; overrides snow.source.batch.size.",
                group,
                ++order,
                Width.SHORT,
                "Batch size");
        def.define(
                key(alias, POLL_INTERVAL_MS),
                Type.INT,
                null,
                Importance.LOW,
                "Milliseconds to wait after a sweep that found no full page; overrides"
                        + " snow.source.poll.interval.ms.",
                group,
                ++order,
                Width.SHORT,
                "Poll interval (ms)");
        return def;
    }

    /** Defaults applied to the batch size and poll interval when the alias does not set them. */
    public static final class Defaults {
        final int batchSize;
        final long pollIntervalMs;

        public Defaults(int batchSize, long pollIntervalMs) {
            this.batchSize = batchSize;
            this.pollIntervalMs = pollIntervalMs;
        }
    }

    /** Reads one alias from a parsed config whose definition already includes the alias keys. */
    static TableSpec from(
            String alias, org.apache.kafka.common.config.AbstractConfig cfg, Defaults d) {
        Integer batch = cfg.getInt(key(alias, BATCH_SIZE));
        Integer interval = cfg.getInt(key(alias, POLL_INTERVAL_MS));
        Range.atLeast(1).ensureValid(key(alias, BATCH_SIZE), batch == null ? 1 : batch);
        Range.atLeast(1).ensureValid(key(alias, POLL_INTERVAL_MS), interval == null ? 1 : interval);
        return new TableSpec(
                alias,
                cfg.getString(key(alias, NAME)).trim(),
                cfg.getString(key(alias, TOPIC)).trim(),
                sh.oso.servicenow.cursor.SnowTimestamp.parse(
                        cfg.getString(key(alias, START_TIMESTAMP)).trim()),
                cfg.getString(key(alias, TIMESTAMP_FIELD)).trim(),
                cfg.getString(key(alias, SYS_ID_FIELD)).trim(),
                cfg.getString(key(alias, QUERY)),
                cfg.getList(key(alias, FIELDS)),
                DisplayValue.fromConfig(cfg.getString(key(alias, DISPLAY_VALUE))),
                cfg.getBoolean(key(alias, EXCLUDE_REFERENCE_LINK)),
                cfg.getBoolean(key(alias, QUERY_DOMAIN)),
                cfg.getString(key(alias, QUERY_CATEGORY)),
                batch == null ? d.batchSize : batch,
                interval == null ? d.pollIntervalMs : interval);
    }

    @Override
    public String toString() {
        return "TableSpec{"
                + alias
                + " -> "
                + name
                + " => "
                + topic
                + ", ts="
                + timestampField
                + ", id="
                + sysIdField
                + ", query='"
                + query
                + "', fields="
                + fields
                + ", display="
                + displayValue
                + ", domain="
                + queryDomain
                + ", batch="
                + batchSize
                + ", interval="
                + pollIntervalMs
                + "ms}";
    }
}
