package sh.oso.servicenow.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/** Constant HTTP Basic header; {@link #invalidate(String)} is a no-op. */
public final class BasicTokenProvider implements TokenProvider {

    private final String header;

    public BasicTokenProvider(String username, String password) {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        this.header =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + password)
                                                .getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String authorization() {
        return header;
    }

    @Override
    public void invalidate(String seen) {
        // Basic credentials cannot be refreshed; a 401 surfaces to the caller.
    }

    @Override
    public String toString() {
        return "BasicTokenProvider{header=***}";
    }
}
