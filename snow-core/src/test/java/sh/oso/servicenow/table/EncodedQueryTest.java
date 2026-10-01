package sh.oso.servicenow.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class EncodedQueryTest {

    @Test
    void andAppendsToEveryNqBranch() {
        EncodedQuery q = EncodedQuery.of("a=1^NQb=2^c=3^NQd=4");
        assertThat(q.nqBranches()).containsExactly("a=1", "b=2^c=3", "d=4");
        assertThat(q.and("ts>T").raw()).isEqualTo("a=1^ts>T^NQb=2^c=3^ts>T^NQd=4^ts>T");
        assertThat(EncodedQuery.empty().and("ts>T").raw()).isEqualTo("ts>T");
        assertThat(EncodedQuery.of("  ").isEmpty()).isTrue();
        assertThat(EncodedQuery.of(null).nqBranches()).containsExactly("");
        assertThat(q.and(" ").raw()).isEqualTo(q.raw());
    }

    @Test
    void orderByIsAppendedOnceAndDetected() {
        assertThat(EncodedQuery.of("a=1").orderBy("ts").orderBy("sys_id").raw())
                .isEqualTo("a=1^ORDERBYts^ORDERBYsys_id");
        assertThat(EncodedQuery.empty().orderBy("ts").raw()).isEqualTo("ORDERBYts");
        assertThat(EncodedQuery.empty().orderByDesc("ts").raw()).isEqualTo("ORDERBYDESCts");
        assertThat(EncodedQuery.of("a=1^ORDERBYDESCts").containsOrderBy()).isTrue();
        assertThat(EncodedQuery.of("ORDERBYts").containsOrderBy()).isTrue();
        assertThat(EncodedQuery.of("nameLIKEORDERBY").containsOrderBy()).isFalse();
        assertThat(EncodedQuery.of("a=1")).isEqualTo(EncodedQuery.of("a=1")).hasToString("a=1");
    }

    @Test
    void queryRequestValidatesAndEncodes() {
        assertThatThrownBy(() -> QueryRequest.builder("incident").limit(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueryRequest.builder("incident").offset(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(QueryRequest.percentEncode("a b^c=d>e,f/g~h"))
                .isEqualTo("a%20b%5Ec%3Dd%3Ee%2Cf%2Fg~h");
        QueryRequest q =
                QueryRequest.builder("incident")
                        .noCount(false)
                        .suppressPaginationHeader(false)
                        .offset(10)
                        .build();
        assertThat(q.pathAndQuery())
                .isEqualTo(
                        "/api/now/table/incident?sysparm_limit=1000&sysparm_offset=10&sysparm_display_value=false&sysparm_exclude_reference_link=true");
        assertThat(q.toString()).contains("sysparm_limit=1000");
        assertThat(DisplayValue.fromConfig(" All ")).isEqualTo(DisplayValue.ALL);
        assertThatThrownBy(() -> DisplayValue.fromConfig("maybe"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new GetOptions(null, null, true).displayValue()).isEqualTo(DisplayValue.FALSE);
        assertThat(new Page<>(java.util.List.of(1, 2), 2).last()).contains(2);
        assertThat(new Page<>(java.util.List.of(), 2).isEmpty()).isTrue();
    }
}
