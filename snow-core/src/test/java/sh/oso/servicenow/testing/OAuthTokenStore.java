package sh.oso.servicenow.testing;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Bearer tokens issued by the fake's {@code /oauth_token.do}; validity follows the clock. */
public final class OAuthTokenStore {

    /** A token response as the fake issued it. */
    public record Issued(String accessToken, String refreshToken, long expiresIn) {}

    private final Clock clock;
    private final Map<String, Instant> accessTokens = new ConcurrentHashMap<>();
    private final Set<String> refreshTokens = ConcurrentHashMap.newKeySet();
    private final AtomicLong expiresIn = new AtomicLong(1800);

    OAuthTokenStore(Clock clock) {
        this.clock = clock;
    }

    /** Lifetime reported in {@code expires_in} for tokens issued from now on. */
    public void expiresIn(long seconds) {
        expiresIn.set(seconds);
    }

    public long expiresIn() {
        return expiresIn.get();
    }

    Issued issue() {
        String access = "tok-" + UUID.randomUUID();
        String refresh = "ref-" + UUID.randomUUID();
        long ttl = expiresIn.get();
        accessTokens.put(access, clock.instant().plusSeconds(ttl));
        refreshTokens.add(refresh);
        return new Issued(access, refresh, ttl);
    }

    boolean isValid(String accessToken) {
        Instant expiry = accessTokens.get(accessToken);
        return expiry != null && clock.instant().isBefore(expiry);
    }

    boolean knowsRefreshToken(String refreshToken) {
        return refreshToken != null && refreshTokens.contains(refreshToken);
    }

    /** Every access token ever issued (for leak checks). */
    public Set<String> issuedAccessTokens() {
        return Set.copyOf(accessTokens.keySet());
    }

    /** Expires every issued token immediately. */
    public void revokeAll() {
        accessTokens.replaceAll((k, v) -> Instant.EPOCH);
        refreshTokens.clear();
    }
}
