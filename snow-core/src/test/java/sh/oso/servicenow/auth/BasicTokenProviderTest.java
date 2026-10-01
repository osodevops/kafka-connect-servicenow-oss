package sh.oso.servicenow.auth;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BasicTokenProviderTest {

    @Test
    void encodesUserAndPasswordAndNeverChanges() {
        BasicTokenProvider p = new BasicTokenProvider("connect", "secret");
        assertThat(p.authorization()).isEqualTo("Basic Y29ubmVjdDpzZWNyZXQ=");
        p.invalidate(p.authorization());
        assertThat(p.authorization()).isEqualTo("Basic Y29ubmVjdDpzZWNyZXQ=");
        assertThat(p.toString()).doesNotContain("secret").doesNotContain("Y29ubmVjdDpzZWNyZXQ=");
    }
}
