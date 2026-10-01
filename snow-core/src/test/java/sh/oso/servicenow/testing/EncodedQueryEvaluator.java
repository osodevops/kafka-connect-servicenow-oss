package sh.oso.servicenow.testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The subset of ServiceNow's encoded query language the fake understands.
 *
 * <p>Operators: {@code = != > >= < <= LIKE STARTSWITH ENDSWITH IN NOT IN ISEMPTY ISNOTEMPTY}.
 * {@code ^} joins terms with AND; {@code ^OR} ORs with the previous term; {@code ^NQ} starts a new
 * top-level OR branch; {@code ORDERBY}/{@code ORDERBYDESC} order (collected from anywhere in the
 * query); {@code ^EQ} is ignored. Values compare numerically when both sides are numbers and as
 * strings otherwise, so the fixed {@code yyyy-MM-dd HH:mm:ss} format compares chronologically.
 * Anything else raises {@link InvalidQueryException}; the fake then returns no rows when {@link
 * #invalidQueryReturnsNoRows} is true (the recommended instance property) or drops the invalid
 * terms otherwise (ServiceNow's default).
 */
public final class EncodedQueryEvaluator {

    /** Mimics {@code glide.invalid_query.returns_no_rows}. */
    public static volatile boolean invalidQueryReturnsNoRows = true;

    private static final Pattern TERM =
            Pattern.compile(
                    "^([A-Za-z0-9_.]+?)(ISNOTEMPTY|ISEMPTY|NOT IN|IN|LIKE|STARTSWITH|ENDSWITH|!=|>=|<=|=|>|<)(.*)$",
                    Pattern.DOTALL);

    private EncodedQueryEvaluator() {}

    /** Strict compilation; unsupported syntax raises {@link InvalidQueryException}. */
    public static Predicate<Map<String, String>> compile(String encodedQuery) {
        return parse(encodedQuery, false).predicate();
    }

    /** Lenient compilation: invalid terms are dropped, mimicking ServiceNow's default. */
    public static Predicate<Map<String, String>> compileLenient(String encodedQuery) {
        return parse(encodedQuery, true).predicate();
    }

    /** Ordering from the {@code ORDERBY} terms; insertion order when there are none. */
    public static Comparator<Map<String, String>> ordering(String encodedQuery) {
        Parsed parsed;
        try {
            parsed = parse(encodedQuery, true);
        } catch (InvalidQueryException e) {
            return (a, b) -> 0;
        }
        Comparator<Map<String, String>> cmp = (a, b) -> 0;
        for (Order o : parsed.orders()) {
            Comparator<Map<String, String>> c =
                    (a, b) -> compareValues(valueOf(a, o.field()), valueOf(b, o.field()));
            cmp = cmp.thenComparing(o.desc() ? c.reversed() : c);
        }
        return cmp;
    }

    private record Order(String field, boolean desc) {}

    private record Term(String field, String op, String value) {
        private boolean comparable(String actual) {
            return !actual.isEmpty() && !value.isEmpty();
        }

        boolean test(Map<String, String> row) {
            String actual = valueOf(row, field);
            switch (op) {
                case "=":
                    return value.isEmpty() ? actual.isEmpty() : compareValues(actual, value) == 0;
                case "!=":
                    return value.isEmpty() ? !actual.isEmpty() : compareValues(actual, value) != 0;
                case ">":
                    return comparable(actual) && compareValues(actual, value) > 0;
                case ">=":
                    return comparable(actual) && compareValues(actual, value) >= 0;
                case "<":
                    return comparable(actual) && compareValues(actual, value) < 0;
                case "<=":
                    return comparable(actual) && compareValues(actual, value) <= 0;
                case "LIKE":
                    return lower(actual).contains(lower(value));
                case "STARTSWITH":
                    return lower(actual).startsWith(lower(value));
                case "ENDSWITH":
                    return lower(actual).endsWith(lower(value));
                case "IN":
                    return Arrays.asList(value.split(",")).contains(actual);
                case "NOT IN":
                    return !Arrays.asList(value.split(",")).contains(actual);
                case "ISEMPTY":
                    return actual.isEmpty();
                case "ISNOTEMPTY":
                    return !actual.isEmpty();
                default:
                    throw new InvalidQueryException("unsupported operator " + op);
            }
        }
    }

    private record Parsed(List<List<List<Term>>> branches, List<Order> orders) {
        Predicate<Map<String, String>> predicate() {
            return row -> {
                for (List<List<Term>> groups : branches) {
                    if (matchesBranch(groups, row)) {
                        return true;
                    }
                }
                return false;
            };
        }

        private static boolean matchesBranch(List<List<Term>> groups, Map<String, String> row) {
            for (List<Term> group : groups) {
                boolean any = false;
                for (Term t : group) {
                    if (t.test(row)) {
                        any = true;
                        break;
                    }
                }
                if (!any) {
                    return false;
                }
            }
            return true;
        }
    }

    private static Parsed parse(String encodedQuery, boolean lenient) {
        List<List<List<Term>>> branches = new ArrayList<>();
        List<Order> orders = new ArrayList<>();
        List<List<Term>> groups = new ArrayList<>();
        branches.add(groups);
        if (encodedQuery == null || encodedQuery.isBlank()) {
            return new Parsed(branches, orders);
        }
        if (encodedQuery.contains("javascript:")) {
            if (!lenient) {
                throw new InvalidQueryException("javascript: expressions are not supported");
            }
        }
        List<Term> current = null;
        for (String rawTerm : encodedQuery.split("\\^", -1)) {
            String term = rawTerm;
            if (term.isEmpty() || term.equals("EQ")) {
                continue;
            }
            if (term.startsWith("ORDERBYDESC")) {
                orders.add(new Order(term.substring("ORDERBYDESC".length()), true));
                continue;
            }
            if (term.startsWith("ORDERBY")) {
                orders.add(new Order(term.substring("ORDERBY".length()), false));
                continue;
            }
            if (term.startsWith("NQ")) {
                groups = new ArrayList<>();
                branches.add(groups);
                current = null;
                term = term.substring(2);
                if (term.isEmpty()) {
                    continue;
                }
            }
            boolean or = false;
            if (term.startsWith("OR")) {
                or = true;
                term = term.substring(2);
            }
            Term parsed = parseTerm(term, lenient);
            if (parsed == null) {
                continue;
            }
            if (or && current != null) {
                current.add(parsed);
            } else {
                current = new ArrayList<>();
                current.add(parsed);
                groups.add(current);
            }
        }
        return new Parsed(branches, orders);
    }

    private static Term parseTerm(String term, boolean lenient) {
        Matcher m = TERM.matcher(term);
        if (!m.matches() || term.contains("javascript:")) {
            if (lenient) {
                return null;
            }
            throw new InvalidQueryException("unsupported encoded query term: " + term);
        }
        return new Term(m.group(1), m.group(2), m.group(3));
    }

    private static String valueOf(Map<String, String> row, String field) {
        String v = row.get(field);
        return v == null ? "" : v;
    }

    private static String lower(String s) {
        return s.toLowerCase(Locale.ROOT);
    }

    /** Numeric when both sides parse as numbers, else string order. */
    static int compareValues(String a, String b) {
        Double da = number(a);
        Double db = number(b);
        if (da != null && db != null) {
            return Double.compare(da, db);
        }
        return a.compareTo(b);
    }

    private static Double number(String s) {
        if (s == null || s.isEmpty()) {
            return null;
        }
        try {
            return Double.valueOf(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
