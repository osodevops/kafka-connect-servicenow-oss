<p align="center">
  <h1 align="center">kafka-connect-servicenow</h1>
  <p align="center">
    Open-source ServiceNow connectors for Apache Kafka: Table API source and sink, Kafka Connect
  </p>
</p>

<p align="center">
  <a href="https://github.com/osodevops/kafka-connect-servicenow-oss/actions/workflows/ci.yml">
    <img src="https://github.com/osodevops/kafka-connect-servicenow-oss/actions/workflows/ci.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://github.com/osodevops/kafka-connect-servicenow-oss/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0">
  </a>
  <a href="https://github.com/osodevops/kafka-connect-servicenow-oss/releases">
    <img src="https://img.shields.io/github/v/release/osodevops/kafka-connect-servicenow-oss" alt="Release">
  </a>
</p>

---

**kafka-connect-servicenow** is a production-grade, Apache-2.0 pair of ServiceNow connectors for Kafka Connect, the **open-source alternative to Confluent's proprietary ServiceNow connectors**. A source and a sink deliver parity with Confluent's Platform and Cloud (V2) connectors, built clean-room on ServiceNow's public **REST Table API** with no ServiceNow-side application to install.

> **Running it in production?** OSO, who build and maintain the connectors, offer an [Enterprise support subscription](https://servicenowkafkaconnector.com/enterprise-support): maintained releases and security patches, ServiceNow family release readiness, a 60-minute P1 response from the engineers who write the code, and an onboarding review. What it covers is on the [Enterprise Support page](https://servicenowkafkaconnector.com/enterprise-support) and in [SUPPORT.md](SUPPORT.md).

## Features

- **Correct under one-second timestamps**: a `(sys_updated_on, sys_id)` keyset cursor with overlap and version dedup, so 10,000 rows updated in the same second are never skipped and restarts never lose a row
- **Historical backfill plus continuous polling**: start from any UTC timestamp, catch up at full page rate, then stream at the poll interval
- **Multi-table, multi-task**: any Table API table or view, whole tables assigned to tasks by rendezvous hashing, per-table query, projection, display value and domain settings
- **ServiceNow sink**: create, patch, put and delete through the Table API with per-partition ordering, bounded concurrency, explicit policies for ambiguous writes, and success/error reporter topics plus the native dead letter queue
- **Basic and OAuth 2.0**: client credentials or password grant, single-flight token refresh, secrets never logged
- **Rate-limit aware**: honours `Retry-After`, backs off with jitter, bounds retries by attempts and elapsed time, caps in-flight requests per instance
- **Runs anywhere**: Apache Kafka 3.x and 4.x, MSK Connect, Strimzi or Confluent Platform; any converter (JSON, Avro, Protobuf); `plugin.discovery=service_load` ready
- **Verified migration path**: scripted Confluent config translation with a CI-proven, evidence-generating cutover

## Installation

Download a connector plugin ZIP from [GitHub Releases](https://github.com/osodevops/kafka-connect-servicenow-oss/releases), unzip it onto your Connect worker's `plugin.path`, and restart the worker. Each release also carries a SHA-256 checksum per ZIP and a CycloneDX SBOM.

Or build from source (JDK 17+, Maven 3.6.3+):

```bash
mvn clean package -DskipTests
# ZIPs land in */target/*-kafka-connect-plugin.zip
```

Library artifacts are published to Maven Central under the `sh.oso` group.

## Quick Start

The quickstart runs with **no ServiceNow credentials**: the `fake` profile starts the same fake ServiceNow the test suite uses.

```bash
cd examples
mkdir -p plugins && for z in ../*/target/*-kafka-connect-plugin.zip; do unzip -o "$z" -d plugins/; done
docker compose --profile fake up -d

curl -s -X POST -H 'Content-Type: application/json' \
  --data @source-connector-fake.json localhost:8083/connectors
```

Watch the seeded incidents arrive:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic servicenow.incident --from-beginning
```

Point it at a real instance by registering `source-connector.json` instead. Full walkthrough (integration user, OAuth application registry, connector configs): **[the documentation](https://servicenowkafkaconnector.com/)**.

## Example configuration

Backfill and then poll three tables into per-table topics:

```json
{
  "name": "servicenow-source",
  "config": {
    "connector.class": "sh.oso.servicenow.source.ServiceNowSourceConnector",
    "tasks.max": "3",
    "snow.url": "https://acme.service-now.com",
    "snow.auth.type": "oauth2",
    "snow.oauth.grant.type": "client_credentials",
    "snow.oauth.client.id": "${file:/secrets/snow.properties:client.id}",
    "snow.oauth.client.secret": "${file:/secrets/snow.properties:client.secret}",
    "snow.tables": "incident,change,cmdb",
    "snow.table.incident.name": "incident",
    "snow.table.incident.topic": "servicenow.incident",
    "snow.table.incident.start.timestamp": "2026-01-01 00:00:00",
    "snow.table.incident.query": "active=true",
    "snow.table.change.name": "change_request",
    "snow.table.change.topic": "servicenow.change_request",
    "snow.table.cmdb.name": "cmdb_ci_server",
    "snow.table.cmdb.topic": "servicenow.cmdb_ci_server",
    "snow.table.cmdb.fields": "sys_id,sys_updated_on,name,ip_address,os",
    "snow.source.schema.mode": "schemaless"
  }
}
```

Write Kafka records back into ServiceNow, patching by `sys_id` and deleting on tombstones:

```json
{
  "name": "servicenow-incident-sink",
  "config": {
    "connector.class": "sh.oso.servicenow.sink.ServiceNowSinkConnector",
    "tasks.max": "2",
    "topics": "incident-updates",
    "snow.url": "https://acme.service-now.com",
    "snow.auth.type": "basic",
    "snow.auth.username": "${file:/secrets/snow.properties:username}",
    "snow.auth.password": "${file:/secrets/snow.properties:password}",
    "snow.sink.table": "incident",
    "snow.sink.operation.mode": "key_value",
    "snow.sink.update.method": "PATCH",
    "snow.sink.create.ambiguous.behavior": "correlation_lookup",
    "snow.sink.correlation.field": "correlation_id",
    "snow.sink.reporter.bootstrap.servers": "kafka:19092",
    "snow.sink.reporter.error.topic": "servicenow-incident-sink-errors",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-servicenow-incident-sink"
  }
}
```

Every property is documented in the **[configuration reference](https://servicenowkafkaconnector.com/reference/configuration/source)**, generated from the connectors' `ConfigDef`, so it always matches the release.

## The connectors

| Connector | Direction | ServiceNow APIs |
|---|---|---|
| **Source** ([docs](https://servicenowkafkaconnector.com/connectors/source)) | ServiceNow to Kafka | REST Table API (`GET`), keyset polling on `sys_updated_on` or `sys_created_on` |
| **Sink** ([docs](https://servicenowkafkaconnector.com/connectors/sink)) | Kafka to ServiceNow | REST Table API (`POST`, `PATCH`, `PUT`, `DELETE`) |

All ServiceNow protocol logic lives in the shared **`snow-core`** library; the connectors stay thin. Deletes cannot be observed through the Table API; the optional, separately governed [event bridge](servicenow-event-bridge/README.md) is the documented path for them.

## Migrating from Confluent

A scripted path with verifiable evidence:

```bash
# translate your existing connector config
curl -s http://connect:8083/connectors/servicenow-incidents \
  | tools/confluent-migration/confluent2oss.py - -o servicenow-incidents-oss.json

# after cutover, prove nothing was lost
tools/confluent-migration/verify_cutover.py --table incident \
  --instance-url https://acme.service-now.com --username "$USER" --password "$PASS" \
  --since "$T0" --topic-dump dump.tsv
```

The zero-loss cutover procedure is executed in CI on every build, publishing a `migration-evidence` artifact (row counts, SHA-256 digests, cursor continuity). See the **[migration guide](https://servicenowkafkaconnector.com/migration/confluent)**.

## Testing

Everything runs locally with **no ServiceNow instance**: the `snow-core` test-jar ships a fake ServiceNow (a WireMock Table API with an in-memory table store, encoded query evaluation, OAuth token endpoint and fault injection).

```bash
mvn clean verify              # unit + fake-harness + docker e2e (Kafka + Connect worker)
mvn clean verify -DskipE2E    # without Docker
mvn verify -Pcontract -DskipE2E -DskipUnitTests=true   # against a real instance (SNOW_URL, SNOW_USERNAME, SNOW_PASSWORD)
```

## Documentation

Docs are built with Docusaurus from [`website/`](website/) and served at **[servicenowkafkaconnector.com](https://servicenowkafkaconnector.com/)**. Design and clean-room documents live in [`docs/`](docs/).

## Contributing

We welcome contributions of all kinds!

- **Report Bugs:** Found a bug? Open an [issue on GitHub](https://github.com/osodevops/kafka-connect-servicenow-oss/issues).
- **Suggest Features:** Have an idea? [Open a feature request](https://github.com/osodevops/kafka-connect-servicenow-oss/issues/new).
- **Contribute Code:** Check out our [good first issues](https://github.com/osodevops/kafka-connect-servicenow-oss/labels/good%20first%20issue) for beginner-friendly tasks. The [local testing guide](https://servicenowkafkaconnector.com/development/local-testing) gets you a full fake-ServiceNow dev loop with no instance.
- **Improve Docs:** The site lives in [`website/`](website/); docs pull requests are very welcome.

Releases use [conventional commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, ...); release-please turns them into versions and changelogs automatically.

## Commercial Support

[OSO](https://oso.sh), who build and maintain these connectors, offer an annual **Enterprise support subscription**: maintained releases and security patches, ServiceNow family release readiness, production support with a 60-minute P1 response in UK business hours, an onboarding review, and help migrating from Confluent's connectors. What is covered, the response targets, the supported-version window and the maintenance commitments are on the [Enterprise Support page](https://servicenowkafkaconnector.com/enterprise-support) and in [SUPPORT.md](SUPPORT.md). Contact [sales@oso.sh](mailto:sales@oso.sh) or [book a call](https://meetings-eu1.hubspot.com/sion-smith).

## License

kafka-connect-servicenow is licensed under the [Apache License 2.0](LICENSE) © [OSO](https://oso.sh).

This is an independent open-source project, not affiliated with, endorsed, or sponsored by ServiceNow, Inc. or the Apache Software Foundation. ServiceNow is a trademark of ServiceNow, Inc. Apache, Apache Kafka, and Kafka are trademarks of the Apache Software Foundation.

---

<p align="center">
  Made with ❤️ by <a href="https://oso.sh">OSO</a>
</p>
