package sh.oso.servicenow.source;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Rendezvous (highest random weight) hashing of tables onto task indexes. Every table goes to
 * exactly one task, the result is a pure function of the table names and the task count, and going
 * from {@code n} to {@code n + 1} tasks moves only the tables that the new task wins. The score is
 * MurmurHash3 (x86, 32-bit) of {@code table + "#" + taskIndex}.
 */
public final class TaskAssignment {

    private static final int SEED = 0x5f3759df;

    private TaskAssignment() {}

    /** The task index that owns {@code table} among {@code taskCount} tasks. */
    public static int owner(String table, int taskCount) {
        Objects.requireNonNull(table, "table");
        if (taskCount < 1) {
            throw new IllegalArgumentException("taskCount must be >= 1");
        }
        int best = 0;
        long bestScore = Long.MIN_VALUE;
        for (int i = 0; i < taskCount; i++) {
            long score = score(table, i);
            if (score > bestScore) {
                bestScore = score;
                best = i;
            }
        }
        return best;
    }

    /**
     * Tables grouped by owning task: index {@code i} of the result holds the tables of task {@code
     * i} in their input order. Lists may be empty when there are more tasks than tables win.
     */
    public static List<List<String>> assign(List<String> tables, int taskCount) {
        Objects.requireNonNull(tables, "tables");
        if (taskCount < 1) {
            throw new IllegalArgumentException("taskCount must be >= 1");
        }
        List<List<String>> out = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            out.add(new ArrayList<>());
        }
        for (String table : tables) {
            out.get(owner(table, taskCount)).add(table);
        }
        List<List<String>> frozen = new ArrayList<>(taskCount);
        for (List<String> l : out) {
            frozen.add(Collections.unmodifiableList(l));
        }
        return Collections.unmodifiableList(frozen);
    }

    static long score(String table, int taskIndex) {
        byte[] key = (table + "#" + taskIndex).getBytes(StandardCharsets.UTF_8);
        return murmur3x86_32(key, SEED) & 0xffffffffL;
    }

    /** MurmurHash3 x86 32-bit (Austin Appleby, public domain). */
    static int murmur3x86_32(byte[] data, int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        int h1 = seed;
        int len = data.length;
        int blocks = len / 4;
        for (int i = 0; i < blocks; i++) {
            int o = i * 4;
            int k1 =
                    (data[o] & 0xff)
                            | ((data[o + 1] & 0xff) << 8)
                            | ((data[o + 2] & 0xff) << 16)
                            | ((data[o + 3] & 0xff) << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }
        int k1 = 0;
        int tail = blocks * 4;
        switch (len & 3) {
            case 3:
                k1 ^= (data[tail + 2] & 0xff) << 16;
            // fall through
            case 2:
                k1 ^= (data[tail + 1] & 0xff) << 8;
            // fall through
            case 1:
                k1 ^= (data[tail] & 0xff);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
                break;
            default:
                break;
        }
        h1 ^= len;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
