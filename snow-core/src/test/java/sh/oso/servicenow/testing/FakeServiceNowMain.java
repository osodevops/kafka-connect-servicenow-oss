package sh.oso.servicenow.testing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs the fake as a standalone process for {@code docker compose --profile fake}: {@code --port
 * 8090} (default), seeded with 25 {@code incident} rows plus the {@code sys_db_object} and {@code
 * sys_dictionary} rows that describe the table, so the sink's default {@code
 * snow.sink.unknown.field.behavior=fail} can look the dictionary up. Blocks until killed.
 */
public final class FakeServiceNowMain {

    /** Columns the fake declares for {@code incident} in {@code sys_dictionary}. */
    public static final List<String> INCIDENT_COLUMNS =
            List.of(
                    "sys_id",
                    "sys_created_on",
                    "sys_updated_on",
                    "sys_mod_count",
                    "number",
                    "short_description",
                    "description",
                    "state",
                    "priority",
                    "category",
                    "caller_id",
                    "assigned_to",
                    "correlation_id",
                    "u_external_ref");

    private FakeServiceNowMain() {}

    public static void main(String[] args) throws InterruptedException {
        int port = 8090;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--port".equals(args[i])) {
                port = Integer.parseInt(args[i + 1]);
            }
        }
        MockServiceNowServer server = MockServiceNowServer.start(port);
        seedDictionary(server, "incident", INCIDENT_COLUMNS);
        seedIncidents(server, 25);
        System.out.println(
                "Fake ServiceNow listening on "
                        + server.baseUrl()
                        + " (Basic "
                        + MockServiceNowServer.USERNAME
                        + "/"
                        + MockServiceNowServer.PASSWORD
                        + ", OAuth client "
                        + MockServiceNowServer.CLIENT_ID
                        + "), "
                        + server.tables().size("incident")
                        + " incident rows seeded, "
                        + server.tables().size("sys_dictionary")
                        + " dictionary rows");
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }

    /**
     * Registers {@code table} (no super class) in {@code sys_db_object} and one {@code
     * sys_dictionary} row per column, in the shape {@code TableMetadataClient} reads.
     */
    public static void seedDictionary(
            MockServiceNowServer server, String table, List<String> columns) {
        TableStore t = server.tables();
        t.insert("sys_db_object", Map.of("name", table, "super_class", ""));
        for (String column : columns) {
            t.insert("sys_dictionary", Map.of("name", table, "element", column));
        }
    }

    /** Seeds {@code count} incident rows with sequential numbers and states. */
    public static void seedIncidents(MockServiceNowServer server, int count) {
        server.tables()
                .seed(
                        "incident",
                        count,
                        i -> {
                            Map<String, String> row = new LinkedHashMap<>();
                            row.put("number", String.format("INC%07d", i + 1));
                            row.put("short_description", "Seeded incident " + (i + 1));
                            row.put("state", Integer.toString(1 + (i % 3)));
                            row.put("priority", Integer.toString(1 + (i % 5)));
                            row.put("active", i % 3 == 2 ? "false" : "true");
                            row.put("caller_id", TableStore.newSysId());
                            return row;
                        });
    }
}
