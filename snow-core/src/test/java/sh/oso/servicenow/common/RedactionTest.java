package sh.oso.servicenow.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RedactionTest {

    @Test
    void masksValues() {
        assertThat(Redaction.mask("hunter2")).isEqualTo("***");
        assertThat(Redaction.mask("")).isEmpty();
        assertThat(Redaction.mask(null)).isNull();
    }

    @Test
    void redactsJsonFormAndSchemeCredentials() {
        String json =
                "{\"username\":\"u\",\"password\":\"hunter2\",\"client_secret\":\"s3\",\"access_token\":\"t\\\"k\",\"Authorization\":\"Bearer x\"}";
        String redacted = Redaction.redactBody(json);
        assertThat(redacted)
                .contains("\"username\":\"u\"")
                .contains("\"password\":\"***\"")
                .contains("\"client_secret\":\"***\"")
                .contains("\"access_token\":\"***\"")
                .doesNotContain("hunter2")
                .doesNotContain("s3")
                .doesNotContain("Bearer x");
        String form =
                "grant_type=password&client_id=id&client_secret=s3&username=u&password=hunter2&refresh_token=r1";
        assertThat(Redaction.redactBody(form))
                .isEqualTo(
                        "grant_type=password&client_id=id&client_secret=***&username=u&password=***&refresh_token=***");
        assertThat(Redaction.redactBody("Authorization: Basic Zm9vOmJhcg== and Bearer abc.def-ghi"))
                .isEqualTo("Authorization: Basic *** and Bearer ***");
        assertThat(Redaction.redactBody(null)).isNull();
        assertThat(Redaction.redactBody("")).isEmpty();
        assertThat(Redaction.excerpt("x".repeat(20), 5)).isEqualTo("xxxxx...");
        assertThat(Redaction.excerpt(null, 5)).isEmpty();
    }

    @Test
    void redactsSensitiveHeadersOnly() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put("Authorization", List.of("Bearer abc"));
        headers.put("Proxy-Authorization", List.of("Basic x"));
        headers.put("Cookie", List.of("JSESSIONID=1"));
        headers.put("Accept", List.of("application/json"));
        headers.put(null, List.of("ignored"));
        String rendered = Redaction.redactHeaders(headers);
        assertThat(rendered)
                .isEqualTo(
                        "{Accept=[application/json], Authorization=[***], Cookie=[***], Proxy-Authorization=[***]}");
        assertThat(Redaction.redactHeaders(null)).isEqualTo("{}");
    }
}
