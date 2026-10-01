package sh.oso.servicenow.common;

/** Library version, read from the jar manifest ({@code Implementation-Version}). */
public final class Version {

    private static final String VERSION = load();

    private Version() {}

    public static String get() {
        return VERSION;
    }

    private static String load() {
        Package pkg = Version.class.getPackage();
        String v = pkg != null ? pkg.getImplementationVersion() : null;
        return v != null && !v.isBlank() ? v : "unknown";
    }
}
