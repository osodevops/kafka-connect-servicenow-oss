package sh.oso.servicenow.schema;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.common.ServiceNowException;
import sh.oso.servicenow.table.DisplayValue;
import sh.oso.servicenow.table.EncodedQuery;
import sh.oso.servicenow.table.Page;
import sh.oso.servicenow.table.PathSegments;
import sh.oso.servicenow.table.QueryRequest;
import sh.oso.servicenow.table.Record;
import sh.oso.servicenow.table.TableApiClient;

/**
 * Column names of a table from {@code sys_dictionary}, walking the super-class chain through {@code
 * sys_db_object.super_class}, cached per table for a TTL. Used by the sink's unknown-field policy.
 * {@code sys_id} is always included.
 */
public final class TableMetadataClient {

    private static final Logger LOG = LoggerFactory.getLogger(TableMetadataClient.class);
    private static final int PAGE = 1000;
    private static final int MAX_DEPTH = 32;

    private final TableApiClient api;
    private final Duration ttl;
    private final Clock clock;
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    private record Cached(Set<String> columns, Instant expiresAt) {}

    public TableMetadataClient(TableApiClient api, Duration ttl) {
        this(api, ttl, Clock.systemUTC());
    }

    public TableMetadataClient(TableApiClient api, Duration ttl, Clock clock) {
        this.api = Objects.requireNonNull(api, "api");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Column names of {@code table} including inherited ones; cached. */
    public Set<String> columns(String table) {
        PathSegments.requireTable(table);
        Instant now = clock.instant();
        Cached hit = cache.get(table);
        if (hit != null && now.isBefore(hit.expiresAt())) {
            return hit.columns();
        }
        Set<String> columns = load(table);
        cache.put(table, new Cached(columns, now.plus(ttl)));
        return columns;
    }

    /** The table and its ancestors, nearest first. */
    public List<String> tableChain(String table) {
        PathSegments.requireTable(table);
        List<String> chain = new ArrayList<>();
        String current = table;
        for (int depth = 0; current != null && depth < MAX_DEPTH; depth++) {
            chain.add(current);
            current = superClassOf(current, depth == 0);
            if (chain.contains(current)) {
                break;
            }
        }
        return chain;
    }

    public void evict(String table) {
        cache.remove(table);
    }

    public void evictAll() {
        cache.clear();
    }

    private Set<String> load(String table) {
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        columns.add("sys_id");
        for (String t : tableChain(table)) {
            columns.addAll(dictionaryColumns(t));
        }
        LOG.debug("Loaded {} column(s) for {}", columns.size(), table);
        return Set.copyOf(columns);
    }

    private String superClassOf(String table, boolean required) {
        Page<Record> page =
                api.list(
                        QueryRequest.builder("sys_db_object")
                                .query(EncodedQuery.of("name=" + table))
                                .fields(List.of("name", "super_class"))
                                .limit(1)
                                .displayValue(DisplayValue.FALSE)
                                .excludeReferenceLink(true)
                                .build());
        if (page.isEmpty()) {
            if (required) {
                throw new ServiceNowException(
                        "Table '" + table + "' is not defined in sys_db_object");
            }
            return null;
        }
        String parentSysId = page.items().get(0).string("super_class");
        if (parentSysId == null || parentSysId.isBlank()) {
            return null;
        }
        Page<Record> parent =
                api.list(
                        QueryRequest.builder("sys_db_object")
                                .query(EncodedQuery.of("sys_id=" + parentSysId))
                                .fields(List.of("name"))
                                .limit(1)
                                .excludeReferenceLink(true)
                                .build());
        return parent.isEmpty() ? null : parent.items().get(0).string("name");
    }

    private List<String> dictionaryColumns(String table) {
        List<String> out = new ArrayList<>();
        int offset = 0;
        while (true) {
            Page<Record> page =
                    api.list(
                            QueryRequest.builder("sys_dictionary")
                                    .query(EncodedQuery.of("name=" + table + "^elementISNOTEMPTY"))
                                    .fields(List.of("element"))
                                    .limit(PAGE)
                                    .offset(offset)
                                    .excludeReferenceLink(true)
                                    .build());
            for (Record r : page.items()) {
                String element = r.string("element");
                if (element != null && !element.isBlank()) {
                    out.add(element);
                }
            }
            if (!page.isFull()) {
                return out;
            }
            offset += page.size();
        }
    }
}
