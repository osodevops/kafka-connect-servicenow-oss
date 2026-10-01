package sh.oso.servicenow.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Test;

class ConnectExceptionsTest {

    @Test
    void mapsRetryableToRetriableAndTheRestToConnectException() {
        RuntimeException retriable =
                ConnectExceptions.toConnect(new ServiceNowException("busy", null, true));
        assertThat(retriable).isInstanceOf(RetriableException.class).hasMessage("busy");
        assertThat(retriable.getCause()).isInstanceOf(ServiceNowException.class);
        assertThat(ConnectExceptions.toConnect(new ServiceNowException("fatal")))
                .isExactlyInstanceOf(ConnectException.class);
        assertThat(ConnectExceptions.toConnect(new IOException("io")))
                .isInstanceOf(RetriableException.class);
        assertThat(ConnectExceptions.toConnect(new IllegalStateException()))
                .isExactlyInstanceOf(ConnectException.class)
                .hasMessage("IllegalStateException");
        ConnectException passthrough = new RetriableException("x");
        assertThat(ConnectExceptions.toConnect(passthrough)).isSameAs(passthrough);
        assertThat(new ServiceNowException("m", new IOException()).getCause())
                .isInstanceOf(IOException.class);
        assertThat(Version.get()).isNotBlank();
    }
}
