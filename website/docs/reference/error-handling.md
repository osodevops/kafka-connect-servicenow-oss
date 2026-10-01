---
title: "Error handling"
description: "Retries, RetriableException, reporters and the dead letter queue."
sidebar_position: 4
---

# Error handling

Every HTTP response and transport failure is classified once, in `snow-core`'s
`ErrorClassifier`, and both connectors act on the classification. This page lists the
classes, what each connector does with them, and the record shapes written to the
reporter topics and the dead letter queue.

## Status codes

| Status | Class | Source | Sink |
|---|---|---|---|
| `401` | Auth | Invalidate the token, refresh once, retry once; a second `401` fails the task | Same |
| `403` | Permission | Fail the table with a diagnostic naming the table, the user and the missing access (ACL, domain or role); other tables keep polling | Fail the record as a permanent error with the same diagnostic |
| `404` | Not found | Missing table: fail the table at startup. Missing row on a probe: warn | `PATCH`/`PUT`/`DELETE` on a missing `sys_id`: `snow.sink.not.found.behavior` (`fail`, `ignore`, `create`, default `fail`). A `404` on a **retried** `DELETE` is success |
| `409` | Conflict | Retried only when the body indicates a transient conflict, otherwise a permanent error | Same |
| `429` | Rate limited | Retried, honouring `Retry-After` (see [Rate limits](../concepts/rate-limits.md)) | Same |
| `408`, `425`, `500`, `502`, `503`, `504` | Transient | Retried with exponential backoff and full jitter | Same |
| `400`, `422` | Record error | Invalid query or field: the table fails naming the query. A row with an unparseable timestamp or missing `sys_id`: `snow.source.bad.row.behavior` | Permanent record error: DLQ or reporter, then `behavior.on.api.errors` |
| Other `4xx` | Permanent | Fail the table | Permanent record error |
| Other `5xx` | Permanent | Fail the table (configurable retry is not offered for unknown 5xx) | Permanent record error |
| Connect timeout, connection reset before the request is sent | Transient | Retried | Retried |
| Timeout **after** the request was sent | Ambiguous | Retried (`GET` is idempotent) | `PATCH`, `PUT`, `DELETE` retried (idempotent by `sys_id`); `CREATE` goes to `snow.sink.create.ambiguous.behavior` |
| Malformed or truncated JSON body | Transient | Retried once, then the table fails | Retried once, then a permanent record error |

Retries are bounded by `snow.retry.max.attempts` and `snow.retry.max.elapsed.ms`. When
they are exhausted on a transient class, the task throws `RetriableException` and the
Connect framework retries the whole `poll()` or `put()`; the task stays `RUNNING`. Permanent
classes surface as `ConnectException` (task `FAILED`) unless a per-record path below
absorbs them.

## Source: bad rows

`snow.source.bad.row.behavior` (default `fail`) decides what happens when a row on a page
cannot be turned into a record: its timestamp field is missing or not a valid
`yyyy-MM-dd HH:mm:ss` value, its `sys_id` is missing, or a typed-mode coercion fails under
`snow.source.schema.evolution=fail`.

- `fail`: the table fails with a message naming the table, the row's `sys_id` (when it has
  one) and the field. Nothing after that row is emitted, so the cursor cannot advance past
  unreported data.
- `skip`: the row is logged at `WARN` with the same detail, counted in the table's
  metrics, and the cursor moves past it. Use this only when a consumer can tolerate a
  missing row and you have a way to find out which one it was; skipping is data loss.

The source does not use the Connect dead letter queue: `errors.tolerance=all` on a source
connector applies to the converter and transformation stages only, and can drop records
there. Prefer fixing the converter.

## Sink: `behavior.on.api.errors`

After a record has failed permanently and has been offered to the error paths below,
`behavior.on.api.errors` decides what the task does next:

- `fail` (default): the task stops with a `ConnectException` naming the topic, partition
  and offset. Nothing is silently dropped.
- `log`: log at `ERROR` and continue with the next record; the offset advances.
- `ignore`: continue silently; the offset advances.

`log` and `ignore` only make sense with a dead letter queue or an error reporter
configured, otherwise the failed record leaves no trace but a log line.

## Sink: ambiguous creates

A `POST` is ambiguous when the request was sent but no response arrived (timeout, reset,
proxy failure): the row may or may not exist. The Table API has no idempotency key, so
`snow.sink.create.ambiguous.behavior` decides:

| Mode | Behaviour | Outcome |
|---|---|---|
| `retry` | Re-send the `POST` under the normal retry policy | At-least-once; a duplicate row is possible |
| `fail_ambiguous` (default) | Do not retry; the record is a permanent error with classification `AMBIGUOUS` and goes to the DLQ or error reporter, then `behavior.on.api.errors` | No duplicate; a human or a downstream process decides |
| `correlation_lookup` | `GET ?sysparm_query=<field>=<value>&sysparm_limit=2` on `snow.sink.correlation.field`; zero rows: retry the `POST`; one row: `PATCH` it instead; two or more: permanent error | Exactly one row, provided the correlation field is unique |

`correlation_lookup` requires `snow.sink.correlation.field` to name a field that is
present in every create payload and unique per business object (an external id, a
`correlation_id`, a ticket number from the upstream system). It also handles redelivered
creates after a task restart, which `retry` and `fail_ambiguous` cannot, because those
never see the original request.

## Reporters

The sink can publish a JSON record for every write to a success topic, an error topic or
both, with its own producer:

```properties
snow.sink.reporter.bootstrap.servers=kafka:19092
snow.sink.reporter.success.topic=servicenow-sink-success
snow.sink.reporter.error.topic=servicenow-sink-errors
snow.sink.reporter.producer.security.protocol=SASL_SSL
snow.sink.reporter.include.request.body=false
```

`snow.sink.reporter.bootstrap.servers` is required because a task cannot see the
worker's bootstrap servers; `snow.sink.reporter.producer.*` keys are passed through to
the producer for security settings. Topics are not created by the connector; create them
with the retention you want. The reporter is flushed in `preCommit`, so a report is
durable before the offset it describes is committed.

Success record (key: the `sys_id` as a string, value: JSON):

```json
{
  "operation": "PATCH",
  "table": "incident",
  "sys_id": "8f4bc0d1c611227a0100e2d3f8a6b9e1",
  "status": 200,
  "request_id": "6d1e1a0c-4b3e-4c5f-9c2a-1f0e8d7c6b5a",
  "source": {"topic": "incident-updates", "partition": 0, "offset": 42}
}
```

Error record (same key when a `sys_id` is known, otherwise null):

```json
{
  "operation": "CREATE",
  "table": "incident",
  "sys_id": null,
  "status": 400,
  "request_id": "6d1e1a0c-4b3e-4c5f-9c2a-1f0e8d7c6b5a",
  "source": {"topic": "incident-updates", "partition": 0, "offset": 43},
  "classification": "RECORD_ERROR",
  "retries": 0,
  "response_excerpt": "{\"error\":{\"message\":\"Invalid field: urgencyy\"}}",
  "exception": "sh.oso.servicenow.common.ServiceNowApiException"
}
```

`request_id` is the `X-Request-Id` the connector sent, so a report can be matched to the
instance's transaction log. `classification` is one of `RECORD_ERROR`, `PERMISSION`,
`NOT_FOUND`, `CONFLICT`, `AMBIGUOUS` or `RETRIES_EXHAUSTED`. `response_excerpt` is
truncated and passed through `Redaction`. The request body is included only with
`snow.sink.reporter.include.request.body=true`, because it can contain personal data.

## The Connect dead letter queue

The sink also supports Kafka Connect's native errant record reporter (Connect 2.6 or
later):

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dlq-servicenow-incident-sink",
"errors.deadletterqueue.context.headers.enable": "true",
"errors.deadletterqueue.topic.replication.factor": "3"
```

Every permanently failed record lands on the DLQ with its **original key, value and
headers**; with context headers enabled, the failure reason travels in the
`__connect.errors.*` headers. On workers older than 2.6 the reporter is absent and the
task falls back to the error reporter topic or `behavior.on.api.errors` alone.

:::warning DLQ records are plaintext copies
A DLQ record is the original Kafka record, exactly as the sink received it after
conversion. If the payload contains personal data or secrets, they are on the DLQ topic
in plaintext with whatever retention and ACLs that topic has. Encryption of DLQ payloads
is the converter's or the platform's responsibility; the connector does not encrypt them.
Apply topic ACLs and retention to the DLQ deliberately, and prefer the error reporter
(which carries an excerpt, not the payload) when the payload itself is sensitive.
:::

With `errors.tolerance=none` (the default) the errant record reporter is not used and a
permanent record error follows `behavior.on.api.errors` directly.

## `RetriableException` and task status

A `RetriableException` from `poll()` or `put()` tells the framework to call the method
again after `errors.retry.timeout` and `errors.retry.delay.max.ms` (worker or connector
level). The task status stays `RUNNING`, and the JMX `retries` counter increments. The
connector throws it only after its own bounded retries are exhausted on a **transient**
class, so a `RetriableException` in the log means the instance has been unavailable or
throttled for at least `snow.retry.max.elapsed.ms`. Permanent failures never take this
path; they fail the task or the record so that nothing is retried forever.
