package sh.oso.servicenow.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;
import sh.oso.servicenow.common.ServiceNowException;

class ValueCoercionsTest {

    @Test
    void stringsToConnectValues() {
        assertThat(ValueCoercions.toConnect(null, Schema.OPTIONAL_STRING_SCHEMA)).isNull();
        assertThat(ValueCoercions.toConnect("", Schema.OPTIONAL_STRING_SCHEMA)).isEqualTo("");
        assertThat(ValueCoercions.toConnect("", Schema.OPTIONAL_INT32_SCHEMA)).isNull();
        assertThat(ValueCoercions.toConnect("12.0", Schema.OPTIONAL_INT64_SCHEMA)).isEqualTo(12L);
        assertThat(ValueCoercions.toConnect("7", Schema.OPTIONAL_INT8_SCHEMA)).isEqualTo((byte) 7);
        assertThat(ValueCoercions.toConnect("7", Schema.OPTIONAL_INT16_SCHEMA))
                .isEqualTo((short) 7);
        assertThat(ValueCoercions.toConnect("1.5", Schema.OPTIONAL_FLOAT32_SCHEMA)).isEqualTo(1.5f);
        assertThat(ValueCoercions.toConnect("1.5", Schema.OPTIONAL_FLOAT64_SCHEMA)).isEqualTo(1.5d);
        assertThat(ValueCoercions.toConnect("1", Schema.OPTIONAL_BOOLEAN_SCHEMA)).isEqualTo(true);
        assertThat(ValueCoercions.toConnect("No", Schema.OPTIONAL_BOOLEAN_SCHEMA)).isEqualTo(false);
        assertThat((byte[]) ValueCoercions.toConnect("aGk=", Schema.OPTIONAL_BYTES_SCHEMA))
                .isEqualTo("hi".getBytes());
        assertThat(
                        ValueCoercions.toConnect(
                                "2026-09-29 07:30:40", Timestamp.builder().optional().build()))
                .isEqualTo(Date.from(Instant.parse("2026-09-29T07:30:40Z")));
        assertThat(ValueCoercions.toConnect("", Timestamp.builder().optional().build())).isNull();
        assertThat(ValueCoercions.toConnect("07:30:40", Time.builder().optional().build()))
                .isEqualTo(Date.from(Instant.parse("1970-01-01T07:30:40Z")));
        assertThat(ValueCoercions.toConnect("1,234.5", Decimal.builder(1).optional().build()))
                .isEqualTo(new BigDecimal("1234.5"));
        assertThatThrownBy(() -> ValueCoercions.toConnect("x", Schema.OPTIONAL_INT32_SCHEMA))
                .isInstanceOf(ServiceNowException.class);
        assertThatThrownBy(() -> ValueCoercions.toConnect("maybe", Schema.OPTIONAL_BOOLEAN_SCHEMA))
                .isInstanceOf(ServiceNowException.class);
        assertThatThrownBy(
                        () ->
                                ValueCoercions.toConnect(
                                        "nope",
                                        org.apache.kafka.connect.data.Date.builder()
                                                .optional()
                                                .build()))
                .isInstanceOf(ServiceNowException.class);
        assertThatThrownBy(
                        () -> ValueCoercions.toConnect("nope", Time.builder().optional().build()))
                .isInstanceOf(ServiceNowException.class);
        assertThatThrownBy(
                        () ->
                                ValueCoercions.toConnect(
                                        "x",
                                        org.apache.kafka.connect.data.SchemaBuilder.array(
                                                        Schema.STRING_SCHEMA)
                                                .build()))
                .isInstanceOf(ServiceNowException.class);
    }

    @Test
    void connectValuesToServiceNowStrings() {
        Date ts = Date.from(Instant.parse("2026-09-29T07:30:40Z"));
        assertThat(ValueCoercions.toServiceNow(ts, Timestamp.SCHEMA))
                .isEqualTo("2026-09-29 07:30:40");
        assertThat(ValueCoercions.toServiceNow(ts, org.apache.kafka.connect.data.Date.SCHEMA))
                .isEqualTo("2026-09-29");
        assertThat(ValueCoercions.toServiceNow(ts, Time.SCHEMA)).isEqualTo("07:30:40");
        assertThat(ValueCoercions.toServiceNow(new BigDecimal("1E+2"), Decimal.schema(0)))
                .isEqualTo("100");
        assertThat(ValueCoercions.toServiceNow(null, Schema.STRING_SCHEMA)).isNull();
        assertThat(ValueCoercions.toServiceNow(true, Schema.BOOLEAN_SCHEMA)).isEqualTo("true");
        assertThat(ValueCoercions.toServiceNow(42, null)).isEqualTo("42");
        assertThat(ValueCoercions.toServiceNow(2.0d)).isEqualTo("2");
        assertThat(ValueCoercions.toServiceNow(2.5f)).isEqualTo("2.5");
        assertThat(ValueCoercions.toServiceNow(ts)).isEqualTo("2026-09-29 07:30:40");
        assertThat(ValueCoercions.toServiceNow(Instant.parse("2026-09-29T07:30:40Z")))
                .isEqualTo("2026-09-29 07:30:40");
        assertThat(ValueCoercions.toServiceNow(LocalDate.of(2026, 9, 29))).isEqualTo("2026-09-29");
        assertThat(ValueCoercions.toServiceNow("hi".getBytes())).isEqualTo("aGk=");
        assertThat(ValueCoercions.toServiceNow(ByteBuffer.wrap("hi".getBytes()))).isEqualTo("aGk=");
        assertThat(ValueCoercions.toServiceNow(new BigDecimal("1.50"))).isEqualTo("1.50");
        assertThat(ValueCoercions.toServiceNow(false)).isEqualTo("false");
    }
}
