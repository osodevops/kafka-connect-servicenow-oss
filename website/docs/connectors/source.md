---
title: "Source Connector"
description: "ServiceNow to Kafka: backfill plus continuous keyset polling."
sidebar_position: 1
---

# Source Connector

`sh.oso.servicenow.source.ServiceNowSourceConnector`

Copies creations and updates from any Table API readable table or view into Kafka: a
historical backfill from a configured start timestamp, then continuous polling on a
`(sys_updated_on, sys_id)` keyset cursor. Delivery is at-least-once with a documented
duplicate bound; deletes are not captured and exactly-once is declared unsupported.

| Capability | Behaviour |
|---|---|
| **Backfill** | Rows whose timestamp field is at or after `start.timestamp`, in cursor order, page after page without waiting |
| **Streaming** | Once caught up, one sweep per `poll.interval.ms`, closing each second only after the safety lag |
| **Multi-table** | Any number of tables or views, each with its own topic, query, projection and cursor; whole tables are assigned to tasks by rendezvous hashing |
| **Restarts** | Resume from the committed cursor minus an overlap; nothing is lost, duplicates are bounded and identifiable |

## How it works

### Backfill and keyset polling

The connector never uses `sysparm_offset`, which can skip or repeat rows when earlier rows
change mid-scan. Each sweep fixes a high-water mark `HI = instance time - safety lag` and
issues two query shapes against the Table API, both ordered by the cursor fields:

| Step | Encoded query | Purpose |
|---|---|---|
| Next buckets | `<base>^sys_updated_on>T^sys_updated_on<=HI^ORDERBYsys_updated_on^ORDERBYsys_id` | Move to later seconds |
| Drain bucket | `<base>^sys_updated_on=T^sys_id>S^ORDERBYsys_id` | Finish the second the last page ended in |

A full page continues immediately with the next request; a short page ends the sweep. If
the sweep's high-water mark is still older than `now - safety lag - poll interval` the
connector is in the `backfill` phase and starts the next sweep at once. Otherwise it is in
the `stream` phase and waits `poll.interval.ms`.

The instance time comes from the `Date` header of every response, so the safety lag is
measured against the ServiceNow clock, not the worker's.

### Same-second correctness

`sys_updated_on` has one-second precision and a bulk update can stamp tens of thousands of
rows with the same second. Because the cursor is the pair `(timestamp, sys_id)` and a bucket
is drained with `sys_id>S`, a second with more rows than `batch.size` is paged through
without loss and without re-reading. The `SameSecondBucketTest` in the repository proves
12,000 rows in one second with a 5,000-row batch arrive exactly once, including across a
restart in the middle of the bucket.

Rows committed late (a long transaction whose `sys_updated_on` is earlier than the sweep's
high-water mark) are covered by the overlap: every streaming sweep and every restart re-reads
`overlap.seconds` before the cursor. A bounded cache of emitted
`(sys_updated_on, sys_id, sys_mod_count)` versions suppresses the re-read rows within a run;
after a restart the cache is empty and the overlap rows are delivered again.

### What a row update looks like

A row updated twice between two polls is seen once, in its latest state. The Table API has
no history, so the intermediate state is not promised and the envelope's `before` is always
`null`.

## Record shape

- **Key**: the `sys_id` field as a string (`snow.table.<alias>.sys.id.field` for views).
- **Topic**: `snow.table.<alias>.topic`.
- **Timestamp**: the cursor timestamp (`sys_updated_on`) in epoch milliseconds.
- **Value**: one of three shapes selected by `snow.source.schema.mode`:

| Mode | Value | Use with |
|---|---|---|
| `schemaless` (default) | A map mirroring the Table API response: absent fields absent, JSON nulls null, `display.value=all` fields as `{value, display_value, link}` | `JsonConverter` with `schemas.enable=false` |
| `strings` | A Struct of optional strings whose schema only grows (new fields are appended with a version bump, fields are never removed) | Schema Registry converters |
| `typed` | A Struct from the explicit `snow.source.typed.fields` mapping, with `snow.source.schema.evolution` deciding what happens to unmapped or uncoercible fields (experimental) | Typed consumers |

- **Headers** on every record:

| Header | Value |
|---|---|
| `snow.table` | ServiceNow table or view name |
| `snow.instance` | Instance host, for example `acme.service-now.com` |
| `snow.source.operation` | Always `UPSERT`. The name differs from the sink's `snow.operation` header on purpose, so a replayed source topic can never be read as a sink command |
| `snow.extracted_at` | Epoch milliseconds when the connector read the row |
| `snow.schema.mode` | `schemaless`, `strings` or `typed` |

### Optional envelope

With `snow.source.emit.envelope=true` each value is wrapped as

```json
{
  "before": null,
  "after": { "sys_id": "8f4bc0d1c611227a0100e2d3f8a6b9e1", "number": "INC0010042", "...": "..." },
  "source": { "table": "incident", "instance": "acme.service-now.com" },
  "op": "u",
  "ts_ms": 1790667042000
}
```

In `schemaless` mode the envelope is a map; in `strings` and `typed` modes it is a Struct named
`sh.oso.servicenow.<table>.Envelope` whose `after` field carries the table schema.

### Display values and projections

- `display.value=false` emits raw values; `all` emits `{value, display_value, link}` per
  field; `true` emits the display value of every field except the cursor fields, which keep
  their raw values so the cursor stays parseable (the request is sent as `all`).
- `fields` limits the projection. When set it must include the sys_id field and the
  timestamp field; `sys_mod_count` is added automatically because it is part of the
  deduplication key.
- `exclude.reference.link=true` (default) drops the `link` of reference fields.

## Multi-table and tasks

`snow.tables` lists aliases; each alias has its own `snow.table.<alias>.*` keys. A table is
owned by exactly one task and is never split. `taskConfigs(maxTasks)` creates at most
`min(tasks.max, number of tables)` tasks and assigns tables with rendezvous hashing, so the
assignment is a pure function of the table names and the task count, and changing
`tasks.max` by one moves only the tables the new task wins. Offsets are per table, not per
task, so a reassignment never loses a cursor.

Within a task, tables are polled round-robin, one request each. A table that fails
permanently (for example a 403 on its ACL) is parked and the failure is raised with the
table name once the other tables have nothing to deliver, so one broken table neither hides
data from the others nor goes unnoticed. Retryable failures (429, 5xx, timeouts) are retried
inside `snow-core` with `Retry-After` honoured, then surface as a retriable poll failure and
the same request is repeated on the next poll.

### Startup probe

On start each task reads one row per table with the base query and the cursor fields. A
403, a 404, a hidden cursor field or an unparseable timestamp fails the task with a message
naming the table, alias and field. The probe also looks up the integration user's
`sys_user.time_zone` and warns if it is not UTC, because encoded-query timestamps follow the
session user's time zone. Disable it with `snow.source.startup.probe=false`.

## Offsets and restarts

Each table has one Connect source partition
`{instance, table, query_fingerprint, timestamp_field}` and one offset
`{version, timestamp, sys_id, phase, fingerprint}`. The fingerprint is a SHA-256 over the
table name, the normalised base query and the `query.domain` flag, so changing which rows the
cursor walks over starts a new partition, while changing only how rows look (projection,
display values, topic, batch size) keeps the cursor. A stored offset whose fingerprint or
format version does not match is refused with a diagnostic naming the partition rather than
silently reused.

On restart the table resumes from the committed cursor minus `snow.source.overlap.seconds`.
Rows inside that window are delivered again; consumers that need idempotence can
deduplicate on `(sys_id, sys_updated_on, sys_mod_count)`. Inspecting, rewinding and resetting
offsets through the Connect REST API is described in
[Offsets and recovery](../concepts/offsets-and-recovery.md).

## What it does not do

- **Deletes.** The Table API cannot list deleted rows, so the source never emits a delete or
  a tombstone and `snow.source.operation` is always `UPSERT`. The optional ServiceNow-side
  event bridge is the planned path for deletes.
- **Exactly-once.** `exactlyOnceSupport()` returns `UNSUPPORTED`; delivery is at-least-once
  with duplicates bounded by the overlap window plus the in-flight page.
- **Intermediate states.** A row changed several times between polls is seen in its latest
  state only.
- **Attachments and binary bodies.**

## Error handling

| Failure | Behaviour |
|---|---|
| 401 | Credentials refreshed (OAuth) and the request repeated once; a second 401 fails the task |
| 403 | Fails with the table, alias and an ACL hint; other tables keep running until they go idle |
| 404 on a table | Fails naming the missing table or view |
| 429, 408, 425, 5xx, I/O | Retried inside `snow-core` with jittered backoff and `Retry-After`, then retried on the next poll |
| Row without a readable cursor | `snow.source.bad.row.behavior=fail` (default) fails naming the `sys_id`; `skip` logs, counts and skips it |
| Rows out of cursor order | Fails naming the two tuples (the instance did not honour `ORDERBY`, or the user's time zone is not UTC) |

## Configuration

:::tip Full property reference
Every property with type, default and valid values, generated from the connector's
`ConfigDef`: [Source configuration](../reference/configuration/source.md).
:::

### Global

| Property | Default | Description |
|---|---|---|
| `snow.tables` | required | Comma-separated aliases, one per table |
| `snow.source.schema.mode` | `schemaless` | `schemaless`, `strings` or `typed` |
| `snow.source.poll.interval.ms` | `5000` | Wait after a sweep that ended on a short page |
| `snow.source.batch.size` | `5000` | Rows per request (`sysparm_limit`) |
| `snow.source.overlap.seconds` | `2` | Seconds re-read before the cursor on restart and at each streaming sweep |
| `snow.source.safety.lag.seconds` | `1` | Seconds subtracted from the instance clock to form the high-water mark |
| `snow.source.dedup.window.records` | `50000` | Bound of the per-table cache of emitted row versions |
| `snow.source.emit.envelope` | `false` | Wrap values in the `before`, `after`, `source`, `op`, `ts_ms` envelope |
| `snow.source.schema.evolution` | `fail` | `fail`, `backward` or `permissive` (typed mode) |
| `snow.source.typed.fields` | empty | `field:type` entries for typed mode |
| `snow.source.bad.row.behavior` | `fail` | `fail` or `skip` for rows without a readable cursor |
| `snow.source.startup.probe` | `true` | One-row access and cursor check per table on start |

### Per alias (`snow.table.<alias>.`)

| Property | Default | Description |
|---|---|---|
| `name` | required | ServiceNow table or view |
| `topic` | required | Kafka topic |
| `start.timestamp` | `1970-01-01 00:00:00` | Backfill start (UTC) when no offset exists |
| `timestamp.field` | `sys_updated_on` | Cursor timestamp field (`sys_created_on` for creates only, prefixed name for views) |
| `sys.id.field` | `sys_id` | Cursor identity field and record key (prefixed name for views) |
| `query` | empty | Base encoded query; must not contain `ORDERBY`; part of the fingerprint |
| `fields` | empty (all) | Projection; must include the cursor fields when set |
| `display.value` | `false` | `false`, `true` or `all` |
| `exclude.reference.link` | `true` | Drop reference `link` values |
| `query.domain` | `true` | `false` sends `sysparm_query_no_domain=true` (needs `query_no_domain_table_api`); part of the fingerprint |
| `query.category` | empty | `sysparm_query_category` |
| `batch.size` | global | Per-table override |
| `poll.interval.ms` | global | Per-table override |

Connection, authentication, HTTP, TLS and retry keys (`snow.url`, `snow.auth.*`,
`snow.oauth.*`, `snow.http.*`, `snow.tls.*`, `snow.retry.*`) are shared with the sink and
described in [Authentication](../reference/authentication.md).

## Example

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
    "snow.tables": "inc,chg",
    "snow.table.inc.name": "incident",
    "snow.table.inc.topic": "snow.incident",
    "snow.table.inc.start.timestamp": "2026-01-01 00:00:00",
    "snow.table.inc.query": "active=true",
    "snow.table.inc.fields": "sys_id,sys_updated_on,number,short_description,priority,state,assignment_group",
    "snow.table.chg.name": "change_request",
    "snow.table.chg.topic": "snow.change_request",
    "snow.table.chg.display.value": "all",
    "snow.source.schema.mode": "strings",
    "snow.source.batch.size": "5000",
    "snow.source.poll.interval.ms": "5000",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
  }
}
```

Two tables and `tasks.max=2` give each table its own task. Migrating an existing Confluent
configuration, including the cutover procedure that keeps the overlap window small, is
covered in [Migrating from Confluent](../migration/confluent.md).
