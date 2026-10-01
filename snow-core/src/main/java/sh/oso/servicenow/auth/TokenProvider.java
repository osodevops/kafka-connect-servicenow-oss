package sh.oso.servicenow.auth;

/**
 * Supplies the {@code Authorization} header value for ServiceNow requests.
 *
 * <p>Implementations cache credentials as they see fit. When a request fails with 401 the HTTP
 * client calls {@link #invalidate(String)} with the exact header value it used and asks again;
 * providers drop the cached value only if it is still the one that failed, so concurrent callers do
 * not trigger redundant refreshes.
 */
public interface TokenProvider extends AutoCloseable {

    /** The full header value, for example {@code Bearer abc} or {@code Basic Zm9v}. */
    String authorization();

    /** Drops the cached value if it is still {@code seen}. */
    void invalidate(String seen);

    @Override
    default void close() {}
}
