package sh.oso.servicenow.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Checks the packaged plugin ZIPs: the connector jar, the shared {@code snow-core} library and the
 * licence are present; nothing the Connect runtime already provides (and no test jar) is bundled;
 * and the connector jar carries the KIP-898 service manifest and a versioned MANIFEST.MF.
 */
class PluginZipContentsTest {

    private static final String VERSION = System.getProperty("project.version", "0.0.1-SNAPSHOT");
    private static final Path REPO_ROOT =
            Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
    private static final List<String> PROVIDED_BY_RUNTIME =
            List.of(
                    "connect-api",
                    "kafka-clients",
                    "connect-transforms",
                    "connect-json",
                    "connect-runtime");

    @ParameterizedTest(name = "{0}")
    @CsvSource({
        "connect-servicenow-source, org.apache.kafka.connect.source.SourceConnector,"
                + " sh.oso.servicenow.source.ServiceNowSourceConnector",
        "connect-servicenow-sink, org.apache.kafka.connect.sink.SinkConnector,"
                + " sh.oso.servicenow.sink.ServiceNowSinkConnector"
    })
    void pluginZipShipsTheConnectorAndNothingTheRuntimeProvides(
            String module, String service, String connectorClass) throws IOException {
        Path zipPath =
                REPO_ROOT
                        .resolve(module)
                        .resolve("target")
                        .resolve(module + "-" + VERSION + "-kafka-connect-plugin.zip");
        if (!Files.isRegularFile(zipPath)) {
            throw new IllegalStateException(
                    "Plugin ZIP missing (run mvn package first): " + zipPath);
        }
        String base = module + "-" + VERSION;
        String lib = base + "/lib/";
        String connectorJar = lib + base + ".jar";

        try (ZipFile zip = new ZipFile(zipPath.toFile())) {
            List<String> entries = zip.stream().map(ZipEntry::getName).toList();
            assertThat(entries)
                    .contains(
                            connectorJar,
                            lib + "snow-core-" + VERSION + ".jar",
                            base + "/doc/LICENSE");
            assertThat(entries)
                    .withFailMessage("every entry must live under %s/: %s", base, entries)
                    .allMatch(e -> e.startsWith(base + "/"));

            List<String> jars =
                    entries.stream()
                            .filter(e -> e.startsWith(lib) && e.endsWith(".jar"))
                            .map(e -> e.substring(lib.length()))
                            .toList();
            for (String provided : PROVIDED_BY_RUNTIME) {
                assertThat(jars)
                        .withFailMessage(
                                "%s is provided by the worker and must not be in lib/", provided)
                        .noneMatch(jar -> jar.contains(provided));
            }
            assertThat(jars).noneMatch(jar -> jar.endsWith("-tests.jar"));

            try (InputStream in = zip.getInputStream(zip.getEntry(connectorJar))) {
                NestedJar jar = NestedJar.read(in, "META-INF/services/" + service);
                assertThat(jar.manifest)
                        .withFailMessage("%s has no MANIFEST.MF", connectorJar)
                        .isNotNull();
                assertThat(jar.manifest.getMainAttributes().getValue("Implementation-Version"))
                        .isEqualTo(VERSION);
                assertThat(jar.serviceFile)
                        .withFailMessage(
                                "%s lacks META-INF/services/%s (plugin.discovery=service_load"
                                        + " would not see it)",
                                connectorJar, service)
                        .isNotNull();
                assertThat(
                                jar.serviceFile
                                        .lines()
                                        .map(String::trim)
                                        .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                                        .toList())
                        .containsExactly(connectorClass);
            }
        }
    }

    /** The manifest and one service file of a jar read from a stream, wherever they sit. */
    private record NestedJar(Manifest manifest, String serviceFile) {
        static NestedJar read(InputStream in, String servicePath) throws IOException {
            Manifest manifest = null;
            String serviceFile = null;
            try (ZipInputStream jar = new ZipInputStream(in)) {
                for (ZipEntry entry = jar.getNextEntry();
                        entry != null;
                        entry = jar.getNextEntry()) {
                    if ("META-INF/MANIFEST.MF".equals(entry.getName())) {
                        manifest = new Manifest(jar);
                    } else if (servicePath.equals(entry.getName())) {
                        serviceFile = new String(jar.readAllBytes(), StandardCharsets.UTF_8);
                    }
                }
            }
            return new NestedJar(manifest, serviceFile);
        }
    }
}
