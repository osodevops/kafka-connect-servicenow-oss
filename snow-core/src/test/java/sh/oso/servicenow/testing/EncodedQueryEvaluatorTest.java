package sh.oso.servicenow.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

class EncodedQueryEvaluatorTest {

    private static final Map<String, String> ROW =
            Map.of(
                    "state", "2",
                    "priority", "10",
                    "short_description", "Printer on fire",
                    "sys_updated_on", "2026-09-29 07:30:40",
                    "sys_id", "0000000000000000000000000000000b",
                    "active", "true",
                    "assigned_to", "");

    private static boolean matches(String query) {
        return EncodedQueryEvaluator.compile(query).test(ROW);
    }

    @Test
    void comparisonOperatorsCompareNumbersNumericallyAndStringsLexically() {
        assertThat(matches("state=2")).isTrue();
        assertThat(matches("state!=2")).isFalse();
        assertThat(matches("priority>9")).isTrue();
        assertThat(matches("priority>=10")).isTrue();
        assertThat(matches("priority<9")).isFalse();
        assertThat(matches("priority<=10")).isTrue();
        assertThat(matches("sys_updated_on>2026-09-29 07:30:39")).isTrue();
        assertThat(matches("sys_updated_on<=2026-09-29 07:30:40")).isTrue();
        assertThat(matches("sys_updated_on>2026-09-29 07:30:40")).isFalse();
        assertThat(matches("sys_id>0000000000000000000000000000000a")).isTrue();
    }

    @Test
    void textOperatorsAreCaseInsensitive() {
        assertThat(matches("short_descriptionLIKEprinter")).isTrue();
        assertThat(matches("short_descriptionSTARTSWITHPRINT")).isTrue();
        assertThat(matches("short_descriptionENDSWITHfire")).isTrue();
        assertThat(matches("short_descriptionLIKEwater")).isFalse();
    }

    @Test
    void inAndEmptyOperators() {
        assertThat(matches("stateIN1,2,3")).isTrue();
        assertThat(matches("stateNOT IN1,2,3")).isFalse();
        assertThat(matches("assigned_toISEMPTY")).isTrue();
        assertThat(matches("assigned_toISNOTEMPTY")).isFalse();
        assertThat(matches("missingISEMPTY")).isTrue();
        assertThat(matches("assigned_to=")).isTrue();
        assertThat(matches("priority>")).isFalse();
    }

    @Test
    void orBindsToThePreviousTermAndNqStartsANewBranch() {
        assertThat(matches("state=1^ORstate=2^active=true")).isTrue();
        assertThat(matches("state=1^ORstate=3^active=true")).isFalse();
        assertThat(matches("state=1^NQactive=true")).isTrue();
        assertThat(matches("state=1^NQactive=false")).isFalse();
        assertThat(matches("state=2^NQactive=false^NQpriority=10")).isTrue();
        assertThat(matches("^EQ")).isTrue();
        assertThat(matches("")).isTrue();
    }

    @Test
    void orderingFollowsOrderByTermsAnywhereInTheQuery() {
        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(Map.of("ts", "2026-01-01 00:00:02", "sys_id", "b"));
        rows.add(Map.of("ts", "2026-01-01 00:00:01", "sys_id", "c"));
        rows.add(Map.of("ts", "2026-01-01 00:00:02", "sys_id", "a"));
        rows.sort(EncodedQueryEvaluator.ordering("active=true^ORDERBYts^ORDERBYsys_id"));
        assertThat(rows).extracting(r -> r.get("sys_id")).containsExactly("c", "a", "b");
        rows.sort(EncodedQueryEvaluator.ordering("ORDERBYDESCts^ORDERBYsys_id"));
        assertThat(rows).extracting(r -> r.get("sys_id")).containsExactly("a", "b", "c");
    }

    @Test
    void unknownSyntaxRaisesAndLenientModeDropsIt() {
        assertThatThrownBy(() -> EncodedQueryEvaluator.compile("state=1^BOGUS"))
                .isInstanceOf(InvalidQueryException.class);
        assertThatThrownBy(
                        () -> EncodedQueryEvaluator.compile("sys_updated_on>javascript:gs.now()"))
                .isInstanceOf(InvalidQueryException.class);
        Predicate<Map<String, String>> lenient =
                EncodedQueryEvaluator.compileLenient("state=2^BOGUS");
        assertThat(lenient.test(ROW)).isTrue();
    }
}
