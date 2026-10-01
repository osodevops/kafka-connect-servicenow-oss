package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

class TaskAssignmentProperties {

    @Provide
    Arbitrary<List<String>> tableSets() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .ofMinLength(1)
                .ofMaxLength(12)
                .list()
                .ofMinSize(1)
                .ofMaxSize(20)
                .uniqueElements();
    }

    @Property
    void everyTableGoesToExactlyOneTask(
            @ForAll("tableSets") List<String> tables,
            @ForAll @IntRange(min = 1, max = 20) int tasks) {
        Assume.that(tasks <= tables.size());
        List<List<String>> assignment = TaskAssignment.assign(tables, tasks);
        assertThat(assignment).hasSize(tasks);
        List<String> all = new ArrayList<>();
        assignment.forEach(all::addAll);
        assertThat(all).containsExactlyInAnyOrderElementsOf(tables);
        for (int i = 0; i < tasks; i++) {
            for (String t : assignment.get(i)) {
                assertThat(TaskAssignment.owner(t, tasks)).isEqualTo(i);
            }
        }
    }

    @Property
    void assignmentIsAPureFunctionOfNamesAndTaskCount(
            @ForAll("tableSets") List<String> tables,
            @ForAll @IntRange(min = 1, max = 5) int tasks) {
        assertThat(TaskAssignment.assign(tables, tasks))
                .isEqualTo(TaskAssignment.assign(new ArrayList<>(tables), tasks));
    }

    @Property
    void growingTheTaskCountByOneMovesTablesOnlyToTheNewTask(
            @ForAll("tableSets") List<String> tables, @ForAll @IntRange(min = 1, max = 19) int n) {
        for (String t : tables) {
            int before = TaskAssignment.owner(t, n);
            int after = TaskAssignment.owner(t, n + 1);
            assertThat(after).isIn(before, n);
        }
    }
}
