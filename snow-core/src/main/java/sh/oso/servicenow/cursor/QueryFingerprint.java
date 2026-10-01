package sh.oso.servicenow.cursor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * {@code sha256:<hex>} over {@code table\n<normalised base query>\n<queryDomain>}. Whitespace in
 * the query is trimmed and collapsed so cosmetic edits keep the same partition; projection and
 * display options are deliberately excluded because they do not change record identity.
 */
public final class QueryFingerprint {

    public static final String PREFIX = "sha256:";

    private QueryFingerprint() {}

    public static String of(String table, String baseQuery, boolean queryDomain) {
        String material = table + "\n" + normalise(baseQuery) + "\n" + queryDomain;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(material.getBytes(StandardCharsets.UTF_8));
            return PREFIX + HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public static String normalise(String query) {
        if (query == null) {
            return "";
        }
        return query.trim().replaceAll("\\s+", " ");
    }
}
