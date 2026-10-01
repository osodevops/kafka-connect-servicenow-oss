package sh.oso.servicenow.source;

/** Connector version, read from the jar manifest ({@code Implementation-Version}). */
final class Version {

    static final String VERSION = resolve();

    private Version() {}

    static String get() {
        return VERSION;
    }

    private static String resolve() {
        Package pkg = Version.class.getPackage();
        String version = pkg != null ? pkg.getImplementationVersion() : null;
        return version != null && !version.isBlank() ? version : "unknown";
    }
}
