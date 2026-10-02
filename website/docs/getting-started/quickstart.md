---
title: "Quickstart"
description: "Run the connectors with docker compose against the bundled fake ServiceNow."
sidebar_position: 3
---

# Quickstart

The repository ships a docker compose stack under
[`examples/`](https://github.com/osodevops/kafka-connect-servicenow-oss/tree/main/examples)
with a single-node Kafka (KRaft) broker, a Connect worker and, behind the `fake` profile,
the same fake ServiceNow the test suite uses. The first run therefore needs **no
ServiceNow instance and no credentials**.

## 1. Build and unpack the plugins

```bash
git clone https://github.com/osodevops/kafka-connect-servicenow-oss.git
cd kafka-connect-servicenow-oss
mvn clean package -DskipTests

cd examples
mkdir -p plugins
for z in ../*/target/*-kafka-connect-plugin.zip; do unzip -o "$z" -d plugins/; done
```

You can use the ZIPs from
[GitHub Releases](https://github.com/osodevops/kafka-connect-servicenow-oss/releases)
instead of building; unzip them into `examples/plugins/` the same way.

## 2. Start the stack with the fake ServiceNow

The fake is packaged as a small image built from the `snow-core` test-jar:

```bash
./fake-servicenow/build.sh
docker compose --profile fake up -d
curl -s localhost:8083/connector-plugins | jq '.[].class' | grep servicenow
```

The fake listens on `localhost:8090` and comes seeded with a few `incident` rows. It
speaks the real Table API, so you can query it exactly as you would an instance:

```bash
curl -s -u connect:secret 'http://localhost:8090/api/now/table/incident?sysparm_limit=2' | jq
```

The fake accepts Basic auth with the built-in user `connect` and password `secret`, the same
credentials the `*-fake.json` connector configs use.

## 3. Register the source connector

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  --data @source-connector-fake.json localhost:8083/connectors | jq
curl -s localhost:8083/connectors/servicenow-source/status | jq
```

`source-connector-fake.json` points the connector at the fake with Basic authentication
and polls the seeded table into `servicenow.incident`:

```json
{
  "name": "servicenow-source",
  "config": {
    "connector.class": "sh.oso.servicenow.source.ServiceNowSourceConnector",
    "tasks.max": "1",
    "snow.url": "http://fake-servicenow:8090",
    "snow.auth.type": "basic",
    "snow.auth.username": "admin",
    "snow.auth.password": "admin",
    "snow.tables": "incident",
    "snow.table.incident.name": "incident",
    "snow.table.incident.topic": "servicenow.incident",
    "snow.source.schema.mode": "schemaless"
  }
}
```

## 4. Watch records arrive

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 \
  --topic servicenow.incident --from-beginning \
  --property print.key=true
```

The key is the row's `sys_id`; the value is the row as the Table API returned it:

```json
"9c573169c611228700193229fff72400"	{"sys_id":"9c573169c611228700193229fff72400","number":"INC0000001","short_description":"Unable to connect to email","sys_updated_on":"2026-09-29 07:30:42","sys_mod_count":"3", ...}
```

Create another incident in the fake and it appears within the poll interval:

```bash
curl -s -u connect:secret -X POST -H 'Content-Type: application/json' \
  http://localhost:8090/api/now/table/incident \
  -d '{"short_description":"Printer on fire","urgency":"1"}' | jq .result.sys_id
```

## 5. Register the sink connector

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  --data @sink-connector-fake.json localhost:8083/connectors | jq
```

`sink-connector-fake.json` consumes `incident-updates` and writes to the fake's `incident`
table in `key_value` mode: a record without a key or `sys_id` is created, a keyed record
with a value is patched, and a keyed record with a null value is deleted.

Produce a record without a key to create an incident:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic incident-updates
>{"short_description":"Created from Kafka","urgency":"2"}
```

Then see it in the fake:

```bash
curl -s -u connect:secret \
  'http://localhost:8090/api/now/table/incident?sysparm_query=short_descriptionLIKEKafka' | jq
```

To patch that row, produce a keyed record whose key is its `sys_id`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic incident-updates \
  --property parse.key=true --property key.separator='|'
>9c573169c611228700193229fff72400|{"urgency":"1","state":"2"}
```

A tombstone (the same key with a null value, for example
`--property null.marker=NULL` and `9c57...|NULL`) deletes it.

## 6. Switch to a real instance

Once the loop works, point the connectors at your instance:

1. Prepare the integration user and, optionally, the OAuth client as described in
   [ServiceNow setup](servicenow-setup.md).
2. Create `examples/secrets/snow.properties` with the values the production configs
   reference through the worker's `FileConfigProvider`:

   ```properties
   url=https://acme.service-now.com
   username=kafka.integration
   password=...
   client.id=...
   client.secret=...
   ```

   The compose file mounts `./secrets` at `/secrets` inside the worker and enables the
   file config provider, so the secrets never appear in connector JSON or in the Connect
   REST API.
3. Start the stack without the `fake` profile and register `source-connector.json`
   (and `sink-connector.json`) instead of the `-fake` variants:

   ```bash
   docker compose up -d
   curl -s -X POST -H 'Content-Type: application/json' \
     --data @source-connector.json localhost:8083/connectors | jq
   ```

`source-connector.json` is the production shape: OAuth client credentials, several
tables with per-table topics and queries, and a `start.timestamp` so the first run
backfills from a known point rather than from 1970.

:::warning Sink against a real instance
`sink-connector.json` writes to whatever table it is configured for. Point it at a
sub-production instance or a scratch table first, and keep `snow.sink.not.found.behavior`
and `snow.sink.create.ambiguous.behavior` at their defaults until you have read
[Error handling](../reference/error-handling.md).
:::

## Next steps

- Tune the [source connector configuration](../connectors/source.md) and read the
  generated [source reference](../reference/configuration/source.md).
- Write records back with the [sink connector](../connectors/sink.md).
- Understand [delivery semantics](../concepts/delivery-semantics.md) and
  [offsets and recovery](../concepts/offsets-and-recovery.md) before going to production.
