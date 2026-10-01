package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class TaskAssignmentTest {

    static final List<String> FIVE =
            List.of("incident", "problem", "change_request", "sc_task", "kb_knowledge");

    @Test
    void fiveTablesAcrossOneToFiveTasksIsStableAndComplete() {
        for (int tasks = 1; tasks <= 5; tasks++) {
            List<List<String>> first = TaskAssignment.assign(FIVE, tasks);
            List<List<String>> again = TaskAssignment.assign(FIVE, tasks);
            assertThat(again).isEqualTo(first);
            assertThat(first).hasSize(tasks);
            List<String> all = new ArrayList<>();
            first.forEach(all::addAll);
            assertThat(all).containsExactlyInAnyOrderElementsOf(FIVE);
            for (String t : FIVE) {
                assertThat(first.get(TaskAssignment.owner(t, tasks))).contains(t);
            }
        }
    }

    @Test
    void oneTaskOwnsEverythingInInputOrder() {
        assertThat(TaskAssignment.assign(FIVE, 1)).containsExactly(FIVE);
    }

    @Test
    void addingATaskOnlyMovesTablesToTheNewTask() {
        for (int n = 1; n < 8; n++) {
            for (String t : FIVE) {
                int before = TaskAssignment.owner(t, n);
                int after = TaskAssignment.owner(t, n + 1);
                assertThat(after).isIn(before, n);
            }
        }
    }

    @Test
    void rejectsZeroTasks() {
        assertThatThrownBy(() -> TaskAssignment.owner("incident", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TaskAssignment.assign(FIVE, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void murmur3MatchesTheReferenceVectors() {
        assertThat(TaskAssignment.murmur3x86_32(new byte[0], 0)).isEqualTo(0);
        assertThat(TaskAssignment.murmur3x86_32("hello".getBytes(StandardCharsets.UTF_8), 0))
                .isEqualTo(0x248bfa47);
        assertThat(
                        TaskAssignment.murmur3x86_32(
                                "The quick brown fox jumps over the lazy dog"
                                        .getBytes(StandardCharsets.UTF_8),
                                0))
                .isEqualTo(0x2e4ff723);
        assertThat(TaskAssignment.score("incident", 0)).isBetween(0L, 0xffffffffL);
    }
}
