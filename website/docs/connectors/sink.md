---
title: "Sink Connector"
description: "Kafka to ServiceNow: create, patch, put and delete through the Table API."
sidebar_position: 2
---

# Sink Connector

`sh.oso.servicenow.sink.ServiceNowSinkConnector`

Writes Kafka records to ServiceNow tables with the public Table API: `POST` to create,
`PATCH` or `PUT` to update, `DELETE` to remove. Every task writes each Kafka partition in
order, one request at a time, and writes up to `snow.sink.max.in.flight` partitions
concurrently. Nothing is installed on the instance.

## How it works

Each record passes through four steps before the HTTP request is built.

### 1. Routing: which table

`snow.sink.routing.mode` picks the target table:

| Mode | Table comes from | Use when |
|---|---|---|
| `fixed` (default) | `snow.sink.table` | One topic, one table |
| `topic_map` | `snow.sink.topic.<topic>.table` for each input topic | Several topics, one table each |
| `header` | The record header named by `snow.sink.table.header` (default `snow.table`) | A producer decides per record |

Table names become URL path segments, so `header` routing is only accepted together with
`snow.sink.table.allowlist`: a header value that is not listed, or that does not match
`^[a-z0-9_]+$`, fails the record. A record can never select `../sys_user` or `Sys_User`.

### 2. Identifier: which row

The sys_id is read from, in this order of discovery, the record key (a `String` or bytes
key is the sys_id itself; a `Struct` or `Map` key supplies `snow.sink.sys.id.key.field`),
the value field `snow.sink.sys.id.value.field` (default `sys_id`), and the `snow.sys_id`
record header. Values are trimmed and lower-cased and must be 32 hex characters. When the
sources disagree the record is rejected, unless `snow.sink.sys.id.precedence` is `key` or
`value`.

### 3. Operation: what to do

Priority order, as in the PRD:

1. The operation header, `snow.sink.operation.header` (default `snow.operation`), when
   present. Values are case-insensitive: `CREATE` or `POST`, `PATCH`, `PUT`, `UPDATE`
   (whatever `snow.sink.update.method` says), `DELETE`, and `UPSERT` (the update method
   when a sys_id is present, otherwise create).
2. `snow.sink.operation.fixed`, when `snow.sink.operation.mode=fixed`.
3. Key/value inference, the Confluent-compatible default (`snow.sink.operation.mode=key_value`):
   a null value (tombstone) is `DELETE`; no sys_id is `CREATE`; a sys_id with a value is
   `snow.sink.update.method` (`PATCH` by default, `PUT` on request).

`snow.sink.operation.mode=header` behaves like `key_value` when the header is absent, unless
`snow.sink.operation.header.required=true`, which fails such records instead.

:::note Replaying a source topic is safe
The [source connector](source.md) marks its records `snow.source.operation=UPSERT`, a
different header from the sink's `snow.operation`, so a replayed source topic is never read as
a stream of commands. If you rename that header to `snow.operation` on purpose, `UPSERT`
updates the row by sys_id when the record has one and creates it otherwise.
:::

An update or delete without a sys_id, and a create with a null value, are record errors.

### 4. Mapping: the request body

The value may be a Connect `Struct` (any Schema Registry converter, or `JsonConverter` with
`schemas.enable=true`), a `Map` (`JsonConverter` with `schemas.enable=false`), or a `String`
holding a JSON object. Fields are processed in this order:

1. `snow.sink.field.rename` (`from:to` pairs), then `snow.sink.field.allowlist` or
   `snow.sink.field.denylist`. `u_` custom field names pass through unchanged.
2. Reserved fields are stripped: `sys_id` (it is the path, not the body), `sys_created_on`,
   `sys_updated_on`, `sys_mod_count`, `sys_created_by`, `sys_updated_by`, and the operation,
   table and `snow.sys_id` header names if they appear as fields. Naming `sys_id` in the
   allowlist keeps it in a create body, for instances that honour a supplied sys_id.
3. `snow.sink.null.behavior`: `omit` (default, a `PATCH` leaves the field untouched),
   `clear` (send an empty string, which clears the field), or `reject`.
4. `snow.sink.nested.behavior`: `reject` (default) fails the record naming the field;
   `flatten` joins nested keys with `snow.sink.nested.flatten.delimiter` (`location.city`
   becomes `location_city`; arrays cannot be flattened); `stringify` sends the nested value as
   a JSON string. Nested values are never silently stringified, because ServiceNow stores an
   unrecognised object as a Java map string.
5. `snow.sink.unknown.field.behavior`: fields missing from the table's dictionary are a
   record error (`fail`, default), `drop`ped, `report`ed to the error reporter while the rest
   of the record is written, or sent as is (`passthrough`). Every mode except `passthrough`
   reads `sys_db_object` and `sys_dictionary` once per table and caches the result for ten
   minutes, so the integration user needs read access to both; use `passthrough` when it
   cannot have it.

Logical types render as ServiceNow expects: `Timestamp` as `yyyy-MM-dd HH:mm:ss` UTC, `Date`
as `yyyy-MM-dd`, `Time` as `HH:mm:ss`, `Decimal` as a plain decimal string, booleans as
`true`/`false`, bytes as Base64. [Schemas](../reference/schemas.md) has the full converter
matrix.

A `PATCH` body therefore contains exactly the mapped fields: no nulls under `omit`, no
reserved names, no headers. `PUT` sends the same body but asks the instance to replace the
record, so fields you do not send may be cleared; `PATCH` is the default for that reason.

## Delivery and ordering

Records in a `put()` batch are grouped by Kafka partition, in arrival order. Each group is a
lane written sequentially; lanes run on `snow.sink.max.in.flight` threads, so a task never has
more than that many requests in flight and never reorders a partition. With several tasks,
each partition belongs to one task, so per-partition order holds across the connector. The
per-instance bound `snow.http.max.concurrent.requests` (shared by every connector on the
worker that targets the same instance) must be at least `snow.sink.max.in.flight`; the
configuration is rejected otherwise.

Delivery is at-least-once. Offsets are committed only for records that succeeded or were
accepted by an error path (the dead letter queue, the error reporter and
`behavior.on.api.errors=log|ignore`). When a transient failure outlives the retry budget
(`snow.retry.*`), the lane stops at that record, the task throws `RetriableException`, and
the framework re-delivers the batch; records written before the stop are written again, which
is harmless for `PATCH`, `PUT` and `DELETE` (idempotent by sys_id) and creates a duplicate for
`CREATE` unless `correlation_lookup` is on. A 429 is retried honouring `Retry-After` and
never adds concurrency: the lane that was throttled waits, the bound stays where it was.

## Ambiguous writes

A request can be sent and never answered: the connection drops, the proxy times out, the
instance is restarted. For `PATCH`, `PUT` and `DELETE` the connector re-sends, because the
sys_id makes them idempotent; a `404` on a re-sent `DELETE` means the first attempt was
applied and counts as success. A `POST` has no idempotency key, so
`snow.sink.create.ambiguous.behavior` decides:

| Mode | What happens | Duplicates |
|---|---|---|
| `retry` | Re-send the `POST`, up to `snow.retry.max.attempts` times | Possible |
| `fail_ambiguous` (default) | Fail the record with classification `AMBIGUOUS`; it goes to the DLQ or error reporter, then `behavior.on.api.errors` | None; a human or a downstream process decides |
| `correlation_lookup` | `GET ?sysparm_query=<field>=<value>&sysparm_fields=sys_id&sysparm_limit=2` on `snow.sink.correlation.field`, before every create and again after an ambiguous one: no match creates, one match is patched instead, two or more fail the record | None, provided the field is unique |

`correlation_lookup` costs one `GET` per create and needs the field in every create payload,
but it is the only mode that also makes a re-delivered create (after a task restart, say)
land on the existing row. Pick a field that is unique per business object: an external id, a
`correlation_id`, the ticket number of the upstream system.

A `404` on `PATCH`, `PUT` or `DELETE` follows `snow.sink.not.found.behavior`: `fail`
(default, classification `NOT_FOUND`), `ignore`, or `create` the row with the same sys_id (for
updates; a missing row on `DELETE` is ignored).

## Reporters and the dead letter queue

Every write can produce a JSON report with the Kafka coordinates of the record, the table,
the operation, the sys_id, the HTTP status and the `X-Request-Id` the connector sent, so a
report can be matched to the instance's transaction log:

```properties
snow.sink.reporter.bootstrap.servers=kafka:19092
snow.sink.reporter.success.topic=servicenow-sink-success
snow.sink.reporter.error.topic=servicenow-sink-errors
snow.sink.reporter.producer.security.protocol=SASL_SSL
snow.sink.reporter.include.request.body=false
```

The reporter has its own producer (`snow.sink.reporter.producer.*` keys pass through to it)
and is flushed in `preCommit`, so a report is durable before the offset it describes is
committed. Error reports add `classification` (`RECORD_ERROR`, `PERMISSION`, `NOT_FOUND`,
`CONFLICT`, `AMBIGUOUS` or `RETRIES_EXHAUSTED`), `retries`, a redacted `response_excerpt`
and the `exception` class. The request body is excluded unless
`snow.sink.reporter.include.request.body=true`, because it can carry personal data. A
`RETRIES_EXHAUSTED` report is informational: that record is re-delivered, not dropped.

Permanent failures are also handed to Kafka Connect's errant record reporter when the worker
offers one (`errors.tolerance=all` plus `errors.deadletterqueue.topic.name`); the original
record lands on the DLQ unchanged, in plaintext. Then `behavior.on.api.errors` decides what
the task does: `fail` (default) stops the task naming the topic, partition and offset; `log`
logs at `ERROR` and continues; `ignore` continues silently. To keep a connector running
through bad records, combine a DLQ or error reporter with `log`. Record shapes, the
classification table and the DLQ warning are in
[Error handling](../reference/error-handling.md).

## Schema handling

The sink accepts whatever the value converter produces and never needs a schema of its own.
`Struct` values (Avro, JSON Schema, Protobuf and `JsonConverter` with `schemas.enable=true`)
and schemaless `Map` values are handled by the same mapping rules; the only difference is
that logical types are known for a `Struct` and inferred from the runtime type for a `Map`.
The Confluent converters are not bundled in the plugin ZIP; install them on the worker or
use Apicurio's Apache-2.0 converters. See [Schemas](../reference/schemas.md).

## Example

Incidents from a `incident-updates` topic, keyed by sys_id, with a DLQ, an error reporter
and safe creates:

```json
{
  "name": "servicenow-incident-sink",
  "config": {
    "connector.class": "sh.oso.servicenow.sink.ServiceNowSinkConnector",
    "tasks.max": "2",
    "topics": "incident-updates",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "snow.url": "https://acme.service-now.com",
    "snow.auth.type": "oauth2",
    "snow.oauth.grant.type": "client_credentials",
    "snow.oauth.client.id": "${file:/secrets/snow.properties:client.id}",
    "snow.oauth.client.secret": "${file:/secrets/snow.properties:client.secret}",

    "snow.sink.routing.mode": "fixed",
    "snow.sink.table": "incident",
    "snow.sink.operation.mode": "key_value",
    "snow.sink.update.method": "PATCH",
    "snow.sink.max.in.flight": "8",

    "snow.sink.null.behavior": "omit",
    "snow.sink.nested.behavior": "reject",
    "snow.sink.unknown.field.behavior": "fail",
    "snow.sink.field.rename": "description:short_description",
    "snow.sink.field.denylist": "internal_notes",

    "snow.sink.create.ambiguous.behavior": "correlation_lookup",
    "snow.sink.correlation.field": "correlation_id",
    "snow.sink.not.found.behavior": "fail",

    "snow.sink.reporter.bootstrap.servers": "kafka:19092",
    "snow.sink.reporter.error.topic": "servicenow-sink-errors",
    "behavior.on.api.errors": "log",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-servicenow-incident-sink",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```

For a producer that chooses the table and operation per record:

```properties
snow.sink.routing.mode=header
snow.sink.table.allowlist=incident,problem,change_request
snow.sink.operation.mode=header
snow.sink.operation.header.required=true
```

## Configuration reference

:::tip Full property reference
Every property with types, defaults and valid values, generated from the connector's
ConfigDef: [Sink configuration](../reference/configuration/sink.md).
:::

Connection, authentication, HTTP, TLS and retry properties (`snow.url`, `snow.auth.*`,
`snow.oauth.*`, `snow.http.*`, `snow.tls.*`, `snow.retry.*`) are shared with the source
connector and described in [Authentication](../reference/authentication.md).
