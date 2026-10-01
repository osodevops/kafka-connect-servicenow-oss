---
title: "Getting Started"
description: "Install the plugins and prepare a ServiceNow integration user."
sidebar_position: 1
---

# Getting Started

Three steps take you from nothing to ServiceNow rows flowing into Kafka:

1. **[Set up ServiceNow](servicenow-setup.md)**: create a dedicated integration user with
   the roles the connectors need, set its time zone to UTC and, if you want OAuth 2.0,
   register an OAuth client in the application registry.
2. **Install the plugins**: download a connector ZIP from
   [GitHub Releases](https://github.com/osodevops/kafka-connect-servicenow-oss/releases)
   (or build with `mvn clean package -DskipTests`), unzip it onto your Connect worker's
   `plugin.path`, and restart the worker.
3. **[Run the quickstart](quickstart.md)**: a docker compose stack with a bundled fake
   ServiceNow, so the first run needs no instance and no credentials.

## Requirements

| Requirement | Version |
|---|---|
| Apache Kafka / Kafka Connect | 3.x or 4.x (2.6 or later for dead letter queue support); built against `connect-api` 3.9 and tested on 3.x and 4.x workers |
| Java (Connect worker) | 17 or 21 (both in CI) |
| ServiceNow | Any instance that exposes the REST Table API (`/api/now/table`); a Personal Developer Instance is enough for evaluation |
| ServiceNow integration user | Read access to the source tables, or create, write and delete access to the sink tables, with its time zone set to UTC |

Nothing is installed on the ServiceNow instance. The connectors use only the public Table
API and, for OAuth, the instance's `/oauth_token.do` endpoint.

## Install the plugin ZIPs

Each release publishes two plugin ZIPs, one per connector, with a SHA-256 checksum file
next to each and a CycloneDX SBOM for the release:

```bash
VERSION=0.1.0
BASE=https://github.com/osodevops/kafka-connect-servicenow-oss/releases/download/v$VERSION
for c in source sink; do
  curl -sSLO "$BASE/connect-servicenow-$c-$VERSION-kafka-connect-plugin.zip"
  curl -sSLO "$BASE/connect-servicenow-$c-$VERSION-kafka-connect-plugin.zip.sha256"
  sha256sum -c "connect-servicenow-$c-$VERSION-kafka-connect-plugin.zip.sha256"
  unzip -o "connect-servicenow-$c-$VERSION-kafka-connect-plugin.zip" -d /opt/kafka/plugins/
done
```

Point the worker's `plugin.path` at the directory that now contains the two unzipped
plugin folders and restart the worker. Each ZIP carries a `lib/` directory with the
connector and its runtime dependencies, `doc/` with the licence, and the KIP-898
service-loader manifests under `META-INF/services`, so workers running with
`plugin.discovery=service_load` load the connectors without scanning.

:::tip Amazon MSK Connect and Strimzi
MSK Connect takes the ZIP as a custom plugin as-is. For Strimzi, add the ZIP as a
`plugins[].artifacts[]` entry of type `zip` on the `KafkaConnect` resource and let the
operator build the image.
:::

## Verify the plugin installation

After restarting the worker, both connector classes should appear in the plugin list:

```bash
curl -s localhost:8083/connector-plugins | jq '.[].class' | grep servicenow
"sh.oso.servicenow.sink.ServiceNowSinkConnector"
"sh.oso.servicenow.source.ServiceNowSourceConnector"
```

If only one class is listed, check that the other ZIP was unzipped into a directory on
`plugin.path` and that the worker was restarted afterwards.

## Next steps

- [ServiceNow setup](servicenow-setup.md): the integration user, roles, UTC time zone,
  OAuth application registry and recommended instance properties.
- [Quickstart](quickstart.md): run both connectors against the bundled fake ServiceNow,
  then switch to a real instance.
- [Source connector](../connectors/source.md) and [Sink connector](../connectors/sink.md):
  what each connector does and how to configure it.
