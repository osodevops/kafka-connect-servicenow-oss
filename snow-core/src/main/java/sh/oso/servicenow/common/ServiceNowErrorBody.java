package sh.oso.servicenow.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;

/**
 * The ServiceNow REST error envelope: {@code {"error":{"message":..,"detail":..},"status":
 * "failure"}}. Non-JSON bodies yield an empty message and detail.
 */
public record ServiceNowErrorBody(String message, String detail, String status) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Parses the envelope; returns empty when the body is not a JSON object. */
    public static Optional<ServiceNowErrorBody> parse(String body) {
        if (body == null || body.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode node = MAPPER.readTree(body);
            if (!node.isObject()) {
                return Optional.empty();
            }
            JsonNode error = node.path("error");
            String message = null;
            String detail = null;
            if (error.isObject()) {
                message = error.path("message").asText(null);
                detail = error.path("detail").asText(null);
            } else if (error.isTextual()) {
                // OAuth style: {"error":"invalid_client","error_description":"..."}
                message = error.asText();
                detail = node.path("error_description").asText(null);
            }
            return Optional.of(
                    new ServiceNowErrorBody(message, detail, node.path("status").asText(null)));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /** Message and detail joined for classification and diagnostics. */
    public String text() {
        StringBuilder sb = new StringBuilder();
        if (message != null) {
            sb.append(message);
        }
        if (detail != null && !detail.isBlank() && !"null".equals(detail)) {
            if (sb.length() > 0) {
                sb.append(": ");
            }
            sb.append(detail);
        }
        return sb.toString();
    }
}
