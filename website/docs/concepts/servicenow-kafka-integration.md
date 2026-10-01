---
title: "ServiceNow to Kafka integration: streaming tables with the Table API"
description: "How to stream ServiceNow tables into Apache Kafka with the REST Table API: what the API can and cannot do, why keyset polling on sys_updated_on and sys_id is the correct cursor, record shape, and when a ServiceNow-side event bridge is needed."
slug: "/servicenow-kafka-integration"
sidebar_label: "ServiceNow to Kafka"
keywords:
  - servicenow kafka
  - servicenow kafka connector
  - servicenow kafka integration
  - servicenow table api kafka
  - stream servicenow to kafka
  - servicenow change data capture
sidebar_position: 5
---

# ServiceNow to Kafka integration

**ServiceNow has no change feed.** The platform exposes its tables through the REST
**Table API**, which lists, creates, updates and deletes rows but never pushes a change
to a subscriber and never lists what was deleted. Streaming ServiceNow into Apache Kafka
therefore means polling tables correctly: a cursor that cannot skip a row when ten
thousand incidents are stamped with the same second, a restart that cannot lose a row,
and a clear statement of what polling cannot see.

This page explains what the Table API offers, why the
[source connector](../connectors/source.md) polls it with a keyset cursor rather than
offsets or a bare timestamp, and when you need the optional ServiceNow-side event bridge.

## What the Table API can and cannot do

| Capability | Table API | What the connectors do with it |
|---|---|---|
| List rows with an encoded query, ordering and a field projection | Yes (`GET /api/now/table/{table}`) | The source's two query shapes, bounded by `sysparm_limit` and ordered by the cursor fields |
| Read a single row, create, patch, put, delete by `sys_id` | Yes | The [sink connector](../connectors/sink.md): `create`, `patch`, `put`, `delete` |
| Display values and reference links | Yes (`sysparm_display_value`, `sysparm_exclude_reference_link`) | Per-table `display.value` and `exclude.reference.link` settings |
| Domain and category hints | Yes (`sysparm_query_no_domain`, `sysparm_query_category`) | Per-table `query.domain` and `query.category` |
| A change feed or server push | No | Polling at `poll.interval.ms`; the event bridge for lower latency |
| A list of deleted rows | No | The source never emits deletes or tombstones; see below |
| Row history or a before-image | No | A row changed twice between polls is seen once, in its latest state; the envelope's `before` is always `null` |
| Sub-second timestamps | No: `sys_updated_on` has one-second precision | The `(sys_updated_on, sys_id)` keyset cursor below |

Every table that the integration user can read through its ACLs is in scope, including
database views when `sys.id.field` and `timestamp.field` name the view's prefixed
columns. The [setup guide](../getting-started/servicenow-setup.md) covers the integration
user, roles, the UTC time zone requirement and tables that are not suitable.

## Why keyset polling

Three ways to page a table through the Table API, and why only one is safe:

- **Offset paging** (`sysparm_offset`) numbers the rows of a result set that is being
  changed while you read it. A row updated behind the cursor shifts every later row by
  one, so a page boundary skips a row or repeats one. Confluent's connectors page this way.
- **Timestamp watermarks** (`sys_updated_on>T`) cannot finish a second: when a bulk update
  stamps 12,000 rows with the same `sys_updated_on` and the page size is 5,000, a
  `>T` predicate either re-reads the whole second forever or jumps past the rows it has
  not seen.
- **Keyset paging** on the pair `(sys_updated_on, sys_id)` drains a second with
  `sys_updated_on=T^sys_id>S^ORDERBYsys_id`, then moves on with
  `sys_updated_on>T^sys_updated_on<=HI^ORDERBYsys_updated_on^ORDERBYsys_id`. Every row
  advances the cursor and no page boundary can skip or repeat a row.

The source adds two guards. A **safety lag** (`snow.source.safety.lag.seconds`, default
`1`) keeps the current instance second out of every sweep, measured against the `Date`
header of the instance's own responses, so a second is read only after ServiceNow has
finished writing it. An **overlap** (`snow.source.overlap.seconds`, default `2`) is
re-read on every streaming sweep and every restart to pick up rows committed late by long
transactions; a bounded cache of emitted `(sys_updated_on, sys_id, sys_mod_count)`
versions (`snow.source.dedup.window.records`, default `50000`) suppresses the re-read
rows within a run. The result is at-least-once delivery with a
[documented duplicate bound](delivery-semantics.md) and the same-second case is
[proven in the test suite](../connectors/source.md#same-second-correctness).

## Streaming ServiceNow tables into Kafka

Each table (an *alias* in the configuration) lands on its own topic with `sys_id` as the
record key. The connector backfills from `start.timestamp` at full page rate, then
switches to polling at the configured interval once it is within a poll interval of the
instance clock:

```json
{
  "name": "servicenow-source",
  "config": {
    "connector.class": "sh.oso.servicenow.source.ServiceNowSourceConnector",
    "tasks.max": "2",
    "snow.url": "https://acme.service-now.com",
    "snow.auth.type": "oauth2",
    "snow.oauth.grant.type": "client_credentials",
    "snow.oauth.client.id": "${file:/secrets/snow.properties:client.id}",
    "snow.oauth.client.secret": "${file:/secrets/snow.properties:client.secret}",
    "snow.tables": "incident,change",
    "snow.table.incident.name": "incident",
    "snow.table.incident.topic": "servicenow.incident",
    "snow.table.incident.start.timestamp": "2026-01-01 00:00:00",
    "snow.table.incident.query": "active=true",
    "snow.table.change.name": "change_request",
    "snow.table.change.topic": "servicenow.change_request",
    "snow.source.schema.mode": "schemaless"
  }
}
```

- `servicenow.incident` and `servicenow.change_request` receive one record per observed
  row version, keyed by `sys_id`, with the headers `snow.table`, `snow.instance`,
  `snow.source.operation=UPSERT`, `snow.extracted_at` and `snow.schema.mode`.
- Choose a [schema mode](../reference/schemas.md): `schemaless` for plain JSON, `strings`
  for Confluent Platform's all-string shape, or `typed` with a declared field mapping for
  Avro and Protobuf converters.
- Whole tables are assigned to tasks by rendezvous hashing, so `tasks.max` up to the
  number of tables spreads the load without splitting a table's ordering.
- Offsets are the cursor tuple plus a fingerprint of the query, so a changed filter or
  projection is detected on restart; see [Offsets and recovery](offsets-and-recovery.md).

To write Kafka records back, the [sink connector](../connectors/sink.md) resolves the
table, the `sys_id` and the operation per record, patches by default, deletes on
tombstones, and reports every outcome to success and error topics or the Connect dead
letter queue.

## Polling versus the alternatives

| Approach | Latency | Deletes visible | Same-second safe | Installs on the instance |
|---|---|---|---|---|
| **Keyset polling of the Table API** (this connector) | Poll interval (seconds) | No | Yes | Nothing |
| Offset paging of the Table API (Confluent style) | Poll interval | No | No: page boundaries can skip or repeat | Nothing |
| ServiceNow-side event bridge (Business Rule plus outbox) | Sub-second to seconds | Yes | Not applicable: push, not scan | A scoped application and an HTTPS gateway |
| Scheduled exports or MID Server jobs | Minutes to hours | Only by full comparison | Not applicable | Scheduled jobs and credentials |

## When to use the event bridge

Polling cannot observe a delete, and its latency floor is the poll interval plus the safety
lag. Both limits are properties of the Table API, not of this connector, so closing them
needs code on the ServiceNow side. The repository reserves an optional, separately governed
[event bridge](https://github.com/osodevops/kafka-connect-servicenow-oss/tree/main/servicenow-event-bridge)
for exactly that: a Business Rule writes to an outbox table, an asynchronous dispatcher
posts to an HTTPS gateway, and the polling source stays authoritative and reconciles
against it. It is not part of any connector release. Until you need deletes or sub-poll
latency, the polling source alone is the simpler and cheaper system: nothing to install,
nothing to upgrade with the instance, and a correctness story that is tested without a
ServiceNow instance.

## Operational notes

- **UTC.** Encoded-query timestamps are interpreted in the integration user's time zone, so
  the user must be set to UTC or the cursor drifts by the offset. The source's startup probe
  checks for this.
- **Rate limits.** ServiceNow enforces per-user and per-instance REST rate limit rules and
  answers `429` with `Retry-After`. The connectors honour it, back off with jitter and cap
  in-flight requests per instance; sizing guidance is in [Rate limits](rate-limits.md).
- **Projections.** `snow.table.<alias>.fields` keeps payloads small; the cursor fields are
  always included.
- **Monitoring.** Each table exposes a JMX MBean with its cursor position, lag behind the
  instance clock and throttled time; see [Metrics](../reference/metrics.md).
- **Try it without an instance.** The [quickstart](../getting-started/quickstart.md)
  runs the whole pipeline against the bundled fake ServiceNow.
