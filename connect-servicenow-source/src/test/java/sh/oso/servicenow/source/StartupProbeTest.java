package sh.oso.servicenow.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import sh.oso.servicenow.limits.ConcurrencyLimiter;
import sh.oso.servicenow.testing.MockServiceNowServer;
import sh.oso.servicenow.testing.RequestJournal;

class StartupProbeTest {

    MockServiceNowServer snow;
    Instant now;
    TaskHarness harness;
    ListAppender<ILoggingEvent> log;
    Logger probeLogger;

    @BeforeEach
    void setUp() {
        ConcurrencyLimiter.clearRegistry();
        snow = MockServiceNowServer.start();
        now = snow.clock().instant();
        harness = new TaskHarness(snow);
        probeLogger = (Logger) LoggerFactory.getLogger(StartupProbe.class);
        log = new ListAppender<>();
        log.start();
        probeLogger.addAppender(log);
    }

    @AfterEach
    void tearDown() {
        probeLogger.detachAppender(log);
        harness.close();
        snow.close();
    }

    private List<String> warnings() {
        return log.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    @Test
    void probeReadsOneRowWithTheBaseQueryAndCursorFields() throws Exception {
        Fixtures.seed(snow.tables(), "incident", 3, now.minusSeconds(50), 1);
        harness.with(TableSpec.key("t1", TableSpec.QUERY), "priority>0");
        harness.start();
        List<RequestJournal.Entry> incident =
                snow.journal().entries().stream()
                        .filter(e -> "incident".equals(e.table()))
                        .toList();
        assertThat(incident).isNotEmpty();
        RequestJournal.Entry probe = incident.get(0);
        assertThat(probe.method()).isEqualTo("GET");
        assertThat(probe.path())
                .contains("sysparm_limit=1")
                .contains("sysparm_query=priority%3E0")
                .contains("sysparm_fields=sys_id%2Csys_updated_on%2Csys_mod_count");
        assertThat(harness.pollUntil(3, 5_000)).hasSize(3);
        assertThat(warnings()).isEmpty();
    }

    @Test
    void probePassesOnAnEmptyTable() throws Exception {
        harness.start();
        assertThat(harness.pollFor(300)).isEmpty();
        assertThat(warnings()).isEmpty();
    }

    @Test
    void missingCursorFieldFailsNamingTheFieldAndTable() {
        snow.tables().insert("incident", Fixtures.row(1, now.minusSeconds(10)));
        snow.faults().hideField("incident", "sys_id");
        assertThatThrownBy(harness::start)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'sys_id'")
                .hasMessageContaining("incident")
                .hasMessageContaining("t1");
    }

    @Test
    void viewWithPrefixedCursorFieldsIsProbedWithThoseFields() throws Exception {
        Map<String, String> row =
                Map.of(
                        "inc_sys_id",
                        Fixtures.sysId(4),
                        "inc_sys_updated_on",
                        sh.oso.servicenow.cursor.SnowTimestamp.format(now.minusSeconds(10)),
                        "inc_number",
                        "INC0001");
        snow.tables().insert("incident_sla", row);
        harness.table("v", "incident_sla", "snow.incident_sla")
                .tables("v")
                .with(TableSpec.key("v", TableSpec.TIMESTAMP_FIELD), "inc_sys_updated_on")
                .with(TableSpec.key("v", TableSpec.SYS_ID_FIELD), "inc_sys_id");
        harness.start();
        List<org.apache.kafka.connect.source.SourceRecord> records = harness.pollUntil(1, 5_000);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo(Fixtures.sysId(4));
        assertThat(Fixtures.partition(records.get(0)))
                .containsEntry("timestamp_field", "inc_sys_updated_on");
        assertThat(Fixtures.offset(records.get(0))).containsEntry("sys_id", Fixtures.sysId(4));
    }

    @Test
    void warnsWhenTheIntegrationUserIsNotOnUtc() {
        snow.tables()
                .insert(
                        "sys_user",
                        Map.of(
                                "user_name",
                                MockServiceNowServer.USERNAME,
                                "time_zone",
                                "Europe/London"));
        harness.start();
        assertThat(warnings())
                .anySatisfy(w -> assertThat(w).contains("Europe/London").contains("UTC"));
    }

    @Test
    void noWarningForAUtcUserOrAnInvisibleUser() {
        snow.tables()
                .insert(
                        "sys_user",
                        Map.of("user_name", MockServiceNowServer.USERNAME, "time_zone", "UTC"));
        harness.start();
        assertThat(warnings()).isEmpty();
        harness.close();

        snow.tables().clear();
        TaskHarness other = new TaskHarness(snow);
        other.start();
        assertThat(warnings()).isEmpty();
        other.close();
    }

    @Test
    void disabledProbeMakesNoRequestBeforeTheFirstPoll() {
        harness.with(SourceConfig.STARTUP_PROBE, "false");
        harness.start();
        assertThat(snow.journal().count("GET")).isZero();
    }

    @Test
    void recognisesUtcSpellings() {
        assertThat(StartupProbe.isUtc("UTC")).isTrue();
        assertThat(StartupProbe.isUtc("GMT")).isTrue();
        assertThat(StartupProbe.isUtc("Etc/UTC")).isTrue();
        assertThat(StartupProbe.isUtc(" utc ")).isTrue();
        assertThat(StartupProbe.isUtc("Europe/London")).isFalse();
        assertThat(StartupProbe.isUtc(null)).isFalse();
    }
}
