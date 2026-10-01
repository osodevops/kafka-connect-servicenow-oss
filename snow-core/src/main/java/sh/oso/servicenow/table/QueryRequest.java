package sh.oso.servicenow.table;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A Table API list request. Renders {@code sysparm_*} parameters with strict percent-encoding (the
 * {@code ^}, space, {@code =}, {@code >} and {@code <} characters of encoded queries included).
 * {@code sysparm_no_count} and {@code sysparm_suppress_pagination_header} default to true.
 */
public final class QueryRequest {

    private final String table;
    private final EncodedQuery query;
    private final List<String> fields;
    private final int limit;
    private final Integer offset;
    private final DisplayValue displayValue;
    private final boolean excludeReferenceLink;
    private final String queryCategory;
    private final boolean queryNoDomain;
    private final boolean noCount;
    private final boolean suppressPaginationHeader;

    private QueryRequest(Builder b) {
        this.table = PathSegments.requireTable(b.table);
        this.query = b.query;
        this.fields = List.copyOf(b.fields);
        this.limit = b.limit;
        this.offset = b.offset;
        this.displayValue = b.displayValue;
        this.excludeReferenceLink = b.excludeReferenceLink;
        this.queryCategory = b.queryCategory;
        this.queryNoDomain = b.queryNoDomain;
        this.noCount = b.noCount;
        this.suppressPaginationHeader = b.suppressPaginationHeader;
    }

    public static Builder builder(String table) {
        return new Builder(table);
    }

    public String table() {
        return table;
    }

    public EncodedQuery query() {
        return query;
    }

    public List<String> fields() {
        return fields;
    }

    public int limit() {
        return limit;
    }

    public Integer offset() {
        return offset;
    }

    public DisplayValue displayValue() {
        return displayValue;
    }

    public boolean excludeReferenceLink() {
        return excludeReferenceLink;
    }

    public String queryCategory() {
        return queryCategory;
    }

    public boolean queryNoDomain() {
        return queryNoDomain;
    }

    public boolean noCount() {
        return noCount;
    }

    public boolean suppressPaginationHeader() {
        return suppressPaginationHeader;
    }

    /** Ordered parameters before encoding. */
    public Map<String, String> parameters() {
        LinkedHashMap<String, String> p = new LinkedHashMap<>();
        if (!query.isEmpty()) {
            p.put("sysparm_query", query.raw());
        }
        if (!fields.isEmpty()) {
            p.put("sysparm_fields", String.join(",", fields));
        }
        p.put("sysparm_limit", Integer.toString(limit));
        if (offset != null) {
            p.put("sysparm_offset", Integer.toString(offset));
        }
        p.put("sysparm_display_value", displayValue.wireValue());
        p.put("sysparm_exclude_reference_link", Boolean.toString(excludeReferenceLink));
        if (queryCategory != null && !queryCategory.isBlank()) {
            p.put("sysparm_query_category", queryCategory);
        }
        if (queryNoDomain) {
            p.put("sysparm_query_no_domain", "true");
        }
        if (noCount) {
            p.put("sysparm_no_count", "true");
        }
        if (suppressPaginationHeader) {
            p.put("sysparm_suppress_pagination_header", "true");
        }
        return p;
    }

    /** {@code /api/now/table/{table}?sysparm_...} fully encoded. */
    public String pathAndQuery() {
        return "/api/now/table/" + table + "?" + encode(parameters());
    }

    static String encode(Map<String, String> params) {
        StringJoiner sj = new StringJoiner("&");
        params.forEach((k, v) -> sj.add(percentEncode(k) + "=" + percentEncode(v)));
        return sj.toString();
    }

    /** RFC 3986 unreserved characters pass; everything else is percent-encoded (space is %20). */
    public static String percentEncode(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder(bytes.length * 3);
        for (byte b : bytes) {
            int c = b & 0xff;
            if ((c >= 'A' && c <= 'Z')
                    || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9')
                    || c == '-'
                    || c == '_'
                    || c == '.'
                    || c == '~') {
                sb.append((char) c);
            } else {
                sb.append('%');
                sb.append(Character.toUpperCase(Character.forDigit(c >> 4, 16)));
                sb.append(Character.toUpperCase(Character.forDigit(c & 0xf, 16)));
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "QueryRequest{" + pathAndQuery() + '}';
    }

    public static final class Builder {
        private final String table;
        private EncodedQuery query = EncodedQuery.empty();
        private List<String> fields = List.of();
        private int limit = 1000;
        private Integer offset;
        private DisplayValue displayValue = DisplayValue.FALSE;
        private boolean excludeReferenceLink = true;
        private String queryCategory;
        private boolean queryNoDomain;
        private boolean noCount = true;
        private boolean suppressPaginationHeader = true;

        private Builder(String table) {
            this.table = Objects.requireNonNull(table, "table");
        }

        public Builder query(EncodedQuery query) {
            this.query = query == null ? EncodedQuery.empty() : query;
            return this;
        }

        public Builder query(String rawQuery) {
            return query(EncodedQuery.of(rawQuery));
        }

        public Builder fields(List<String> fields) {
            this.fields = fields == null ? List.of() : List.copyOf(fields);
            return this;
        }

        public Builder limit(int limit) {
            if (limit < 1) {
                throw new IllegalArgumentException("limit must be >= 1");
            }
            this.limit = limit;
            return this;
        }

        public Builder offset(int offset) {
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be >= 0");
            }
            this.offset = offset;
            return this;
        }

        public Builder displayValue(DisplayValue displayValue) {
            this.displayValue = displayValue == null ? DisplayValue.FALSE : displayValue;
            return this;
        }

        public Builder excludeReferenceLink(boolean v) {
            this.excludeReferenceLink = v;
            return this;
        }

        public Builder queryCategory(String v) {
            this.queryCategory = v;
            return this;
        }

        public Builder queryNoDomain(boolean v) {
            this.queryNoDomain = v;
            return this;
        }

        public Builder noCount(boolean v) {
            this.noCount = v;
            return this;
        }

        public Builder suppressPaginationHeader(boolean v) {
            this.suppressPaginationHeader = v;
            return this;
        }

        public QueryRequest build() {
            return new QueryRequest(this);
        }
    }
}
