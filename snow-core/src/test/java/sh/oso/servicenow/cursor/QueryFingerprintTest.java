package sh.oso.servicenow.cursor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class QueryFingerprintTest {

    @Test
    void isStableAcrossWhitespaceAndChangesWithInputs() {
        String a = QueryFingerprint.of("incident", "active=true", true);
        assertThat(a).startsWith("sha256:").hasSize(7 + 64);
        assertThat(QueryFingerprint.of("incident", "  active=true \n", true)).isEqualTo(a);
        assertThat(QueryFingerprint.of("incident", "active=true", false)).isNotEqualTo(a);
        assertThat(QueryFingerprint.of("problem", "active=true", true)).isNotEqualTo(a);
        assertThat(QueryFingerprint.of("incident", "active=false", true)).isNotEqualTo(a);
        assertThat(QueryFingerprint.of("incident", null, true))
                .isEqualTo(QueryFingerprint.of("incident", "", true));
        assertThat(QueryFingerprint.normalise("a   b\tc")).isEqualTo("a b c");
    }

    @Test
    void sourcePartitionLowerCasesTheHostAndHasFixedKeys() {
        Map<String, String> p =
                SourcePartition.of(
                        "ACME.service-now.com", "incident", "sha256:x", "sys_updated_on");
        assertThat(p)
                .containsEntry("instance", "acme.service-now.com")
                .containsEntry("table", "incident")
                .containsEntry("query_fingerprint", "sha256:x")
                .containsEntry("timestamp_field", "sys_updated_on")
                .hasSize(4);
    }
}
