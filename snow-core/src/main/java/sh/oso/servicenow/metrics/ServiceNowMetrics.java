package sh.oso.servicenow.metrics;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers the connectors' MXBeans on the platform MBean server under the {@value #DOMAIN} domain.
 *
 * <ul>
 *   <li>{@code sh.oso.servicenow:type=source-table,connector=<name>,task=<n>,table=<table>}
 *   <li>{@code sh.oso.servicenow:type=sink-writer,connector=<name>,task=<n>}
 * </ul>
 *
 * <p>Values that JMX would otherwise misparse (commas, equals signs, colons, quotes, wildcards,
 * whitespace) are quoted with {@link ObjectName#quote}. Registration never throws: a failure is
 * logged at WARN and the task runs without metrics. Registering a name that is already registered
 * replaces the previous bean, so a task restarted inside the same worker JVM does not fail.
 *
 * <p>The connector name comes from the {@code name} key that Connect copies into every task
 * configuration. Connect does not pass the task index to tasks, so both connectors set the internal
 * {@value #TASK_ID_KEY} key in {@code taskConfigs}; a task started without it (outside a connector)
 * gets a per-JVM sequence number instead so names stay unique.
 */
public final class ServiceNowMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceNowMetrics.class);

    public static final String DOMAIN = "sh.oso.servicenow";
    public static final String TYPE_SOURCE_TABLE = "source-table";
    public static final String TYPE_SINK_WRITER = "sink-writer";

    /** The key under which Connect injects the connector name into task configurations. */
    public static final String CONNECTOR_NAME_KEY = "name";

    /** Internal key set by the connectors' {@code taskConfigs}: the task index. */
    public static final String TASK_ID_KEY = "snow.task.id";

    static final String UNKNOWN_CONNECTOR = "unknown";

    private static final AtomicInteger FALLBACK_TASK_IDS = new AtomicInteger();

    private ServiceNowMetrics() {}

    public static ObjectName sourceTableName(String connector, int task, String table) {
        return name(
                "type="
                        + TYPE_SOURCE_TABLE
                        + ",connector="
                        + value(connector)
                        + ",task="
                        + task
                        + ",table="
                        + value(table));
    }

    public static ObjectName sinkWriterName(String connector, int task) {
        return name(
                "type=" + TYPE_SINK_WRITER + ",connector=" + value(connector) + ",task=" + task);
    }

    /** The connector name Connect put in the task configuration, or {@code unknown}. */
    public static String connectorName(Map<String, String> taskProps) {
        String name = taskProps == null ? null : taskProps.get(CONNECTOR_NAME_KEY);
        return name == null || name.isBlank() ? UNKNOWN_CONNECTOR : name.trim();
    }

    /**
     * The task index from {@value #TASK_ID_KEY}, or the next per-JVM sequence number when the key
     * is absent or not a non-negative integer.
     */
    public static int taskId(Map<String, String> taskProps) {
        String raw = taskProps == null ? null : taskProps.get(TASK_ID_KEY);
        if (raw != null && !raw.isBlank()) {
            try {
                int id = Integer.parseInt(raw.trim());
                if (id >= 0) {
                    return id;
                }
            } catch (NumberFormatException ignored) {
                // fall through to the sequence number
            }
        }
        return FALLBACK_TASK_IDS.getAndIncrement();
    }

    /**
     * Registers {@code bean} as an MXBean of {@code mxbeanInterface} under {@code name}, replacing
     * any bean already registered there.
     *
     * @return true when the bean is registered, false when the platform server refused it
     */
    public static <T> boolean register(ObjectName name, T bean, Class<T> mxbeanInterface) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(bean, "bean");
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            if (server.isRegistered(name)) {
                server.unregisterMBean(name);
            }
            server.registerMBean(new StandardMBean(bean, mxbeanInterface, true), name);
            LOG.debug("Registered MBean {}", name);
            return true;
        } catch (JMException | RuntimeException e) {
            LOG.warn("Could not register MBean {}: {}", name, e.toString());
            return false;
        }
    }

    /** Unregisters {@code name} if it is registered; never throws. */
    public static void unregister(ObjectName name) {
        if (name == null) {
            return;
        }
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            if (server.isRegistered(name)) {
                server.unregisterMBean(name);
                LOG.debug("Unregistered MBean {}", name);
            }
        } catch (JMException | RuntimeException e) {
            LOG.warn("Could not unregister MBean {}: {}", name, e.toString());
        }
    }

    public static boolean isRegistered(ObjectName name) {
        return name != null && ManagementFactory.getPlatformMBeanServer().isRegistered(name);
    }

    private static ObjectName name(String keyProperties) {
        try {
            return new ObjectName(DOMAIN + ":" + keyProperties);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException("Invalid MBean name " + keyProperties, e);
        }
    }

    /** Quotes a key value only when JMX would otherwise misparse it. */
    static String value(String raw) {
        String v = raw == null || raw.isBlank() ? UNKNOWN_CONNECTOR : raw;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c == ','
                    || c == '='
                    || c == ':'
                    || c == '"'
                    || c == '*'
                    || c == '?'
                    || c == '\n'
                    || Character.isWhitespace(c)) {
                return ObjectName.quote(v);
            }
        }
        return v;
    }
}
