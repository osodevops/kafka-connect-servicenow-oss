package sh.oso.servicenow.table;

import java.util.List;
import java.util.Optional;

/** One page of results; {@link #isFull()} means the caller should fetch another page. */
public record Page<T>(List<T> items, int limit) {

    public Page {
        items = List.copyOf(items);
    }

    public boolean isFull() {
        return limit > 0 && items.size() >= limit;
    }

    public int size() {
        return items.size();
    }

    public boolean isEmpty() {
        return items.isEmpty();
    }

    public Optional<T> last() {
        return items.isEmpty() ? Optional.empty() : Optional.of(items.get(items.size() - 1));
    }
}
