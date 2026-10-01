---
title: "Building"
description: "JDK 17, Maven, quality gates and plugin ZIPs."
sidebar_position: 1
---

# Building

Requirements: **JDK 17 or later** and **Maven 3.6.3 or later**. The code is compiled with
`maven.compiler.release=17` and CI runs the build on Temurin 17 and 21.

```bash
git clone https://github.com/osodevops/kafka-connect-servicenow-oss.git
cd kafka-connect-servicenow-oss
mvn clean verify
```

`verify` compiles everything, runs the unit and fake-harness tests against the local fake
ServiceNow, runs the quality gates, packages each connector as a Kafka Connect plugin
ZIP, and then runs the Docker end-to-end tests in `e2e-tests`:

```text
connect-servicenow-source/target/connect-servicenow-source-<version>-kafka-connect-plugin.zip
connect-servicenow-sink/target/connect-servicenow-sink-<version>-kafka-connect-plugin.zip
```

Each ZIP contains a `lib/` directory with the connector, `snow-core` and their runtime
dependencies (`connect-api` and `kafka-clients` excluded, the worker provides them),
`doc/LICENSE`, and the KIP-898 service-loader manifests
(`META-INF/services/org.apache.kafka.connect.source.SourceConnector` and
`...sink.SinkConnector`) so `plugin.discovery=service_load` works. Unzip onto
`plugin.path`.

## Useful flags

```bash
mvn clean package -DskipTests          # just build the ZIPs
mvn clean verify -DskipE2E             # everything except the Docker e2e (no Docker needed)
mvn verify -DskipUnitTests=true        # only the failsafe integration tiers (e2e, contract, soak)
mvn -pl snow-core test                 # one module's tests
mvn spotless:apply                     # fix formatting before committing
mvn verify -De2e.kafka.image=apache/kafka:4.1.0   # e2e on a Kafka 4.x worker image
```

`-DskipE2E` is the everyday flag on a laptop without Docker; `-DskipUnitTests=true` is
what the Kafka 4.x CI job and the contract and soak workflows use so that they run only
the failsafe tier they are interested in.

## Quality gates

All of these run in `verify` and fail the build:

| Gate | What it checks | Fix |
|---|---|---|
| Spotless (`google-java-format` 1.36, AOSP style, 4-space indent) | Formatting of every Java file; needs JDK 21 or newer at runtime, see below | `mvn spotless:apply` |
| JaCoCo `check` | Line coverage floor per module: **70%** in `snow-core`, **60%** in each connector (`jacoco.line.minimum`); `e2e-tests` is exempt | Add tests; the HTML report is in `<module>/target/site/jacoco/` |
| Maven Enforcer | Maven 3.6.3 or later, Java 17 or later, and `dependencyConvergence` (no two versions of the same artefact on the classpath) | Pin the version in the parent's `dependencyManagement` |
| `PluginZipContentsTest` (`e2e-tests`, surefire) | Each ZIP has both service-loader manifests, `doc/LICENSE`, and no `connect-api`, `kafka-clients` or `connect-transforms` jar in `lib/` | Mark the dependency `provided` |
| `ConfigDocsGeneratorTest` (`e2e-tests`, surefire) | Regenerates `website/docs/reference/configuration/{source,sink}.md` from each `ConfigDef`; CI then runs `git diff --exit-code` on that directory | Commit the regenerated files with the config change |

Spotless and JaCoCo run before packaging, so a formatting slip fails fast. The
generated reference pages are the only part of `website/` that a Java change must
update; never edit them by hand.

:::note Spotless needs JDK 21
`google-java-format` 1.36 runs only on JDK 21 or newer. On JDK 17 the
`spotless-needs-jdk21` Maven profile activates automatically and sets
`spotless.check.skip=true`, so the build still passes but formatting is not checked.
CI enforces formatting on its JDK 21 job, so if you develop on JDK 17 run
`mvn spotless:apply` with a JDK 21 or newer (for example `JAVA_HOME=/path/to/jdk21 mvn spotless:apply`)
before opening a pull request, or the JDK 21 job will fail on formatting alone.
:::

## Module layout

| Module | Contents |
|---|---|
| `snow-core` | Shared ServiceNow client library (config, auth, HTTP, Table API, cursor engine, schema, retries, limits) plus the fake ServiceNow test harness, published as a `test-jar` |
| `connect-servicenow-source` | Source connector and its plugin ZIP |
| `connect-servicenow-sink` | Sink connector and its plugin ZIP |
| `e2e-tests` | Docker end-to-end tests, the configuration reference generator and the ZIP contents test; never published |
| `tools/confluent-migration` | `confluent2oss.py`, `verify_cutover.py` and their `selftest.py`, which CI runs |
| `servicenow-event-bridge` | Placeholder README for the optional ServiceNow-side component; not part of any release |
| `website` | This documentation site (Docusaurus); `cd website && npm ci && npm run build` |

Property-based tests use jqwik on the JUnit Platform, so `mvn test` runs them alongside
the JUnit 5 tests with no extra configuration.
