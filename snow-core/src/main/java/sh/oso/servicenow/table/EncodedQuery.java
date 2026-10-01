package sh.oso.servicenow.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A ServiceNow encoded query ({@code sysparm_query}). Immutable; {@link #and(String)} appends a
 * predicate to every {@code ^NQ} branch so a keyset window applies to each top-level OR branch, and
 * {@link #orderBy(String)} appends a single {@code ^ORDERBY} term to the whole query.
 */
public final class EncodedQuery {

    private static final Pattern ORDER_BY = Pattern.compile("(^|\\^)ORDERBY(DESC)?");
    private static final String NQ = "^NQ";
    private static final EncodedQuery EMPTY = new EncodedQuery("");

    private final String raw;

    private EncodedQuery(String raw) {
        this.raw = raw;
    }

    public static EncodedQuery of(String raw) {
        if (raw == null) {
            return EMPTY;
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? EMPTY : new EncodedQuery(trimmed);
    }

    public static EncodedQuery empty() {
        return EMPTY;
    }

    public String raw() {
        return raw;
    }

    public boolean isEmpty() {
        return raw.isEmpty();
    }

    public boolean containsOrderBy() {
        return ORDER_BY.matcher(raw).find();
    }

    /** The {@code ^NQ}-separated top-level OR branches (one element when there is none). */
    public List<String> nqBranches() {
        if (raw.isEmpty()) {
            return List.of("");
        }
        return Collections.unmodifiableList(
                new ArrayList<>(Arrays.asList(raw.split(Pattern.quote(NQ), -1))));
    }

    /** ANDs {@code clause} onto every {@code ^NQ} branch. */
    public EncodedQuery and(String clause) {
        Objects.requireNonNull(clause, "clause");
        if (clause.isBlank()) {
            return this;
        }
        if (raw.isEmpty()) {
            return new EncodedQuery(clause);
        }
        List<String> out = new ArrayList<>();
        for (String branch : nqBranches()) {
            out.add(branch.isEmpty() ? clause : branch + "^" + clause);
        }
        return new EncodedQuery(String.join(NQ, out));
    }

    /** Appends {@code ^ORDERBY<field>} once, at the end. */
    public EncodedQuery orderBy(String field) {
        Objects.requireNonNull(field, "field");
        return new EncodedQuery(raw.isEmpty() ? "ORDERBY" + field : raw + "^ORDERBY" + field);
    }

    /** Appends {@code ^ORDERBYDESC<field>} once, at the end. */
    public EncodedQuery orderByDesc(String field) {
        Objects.requireNonNull(field, "field");
        return new EncodedQuery(
                raw.isEmpty() ? "ORDERBYDESC" + field : raw + "^ORDERBYDESC" + field);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof EncodedQuery q && raw.equals(q.raw);
    }

    @Override
    public int hashCode() {
        return raw.hashCode();
    }

    @Override
    public String toString() {
        return raw;
    }
}
