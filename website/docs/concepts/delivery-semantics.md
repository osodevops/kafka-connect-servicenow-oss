---
title: "Delivery semantics"
description: "At-least-once delivery, the duplicate bound and ordering."
sidebar_position: 2
---

# Delivery semantics

Both connectors are **at-least-once**, matching the guarantees of Confluent's ServiceNow
connectors and of the Table API itself. This page states exactly what that means for
each direction, what the duplicate bound is, and what is deliberately not promised.

## Source connector

- **Offsets are committed after records are produced.** The cursor tuple travels in each
  `SourceRecord`'s offset and the Connect framework commits it after the record has been
  written to Kafka. A crash between produce and commit re-delivers those records on
  restart: duplicates are possible, loss is not.
- **Every restart re-reads an overlap window.** On start, the poller moves the committed
  timestamp back by `snow.source.overlap.seconds` (default `2`) and re-scans from there.
  Rows already emitted are suppressed by the dedup cache, keyed on
  `(timestamp, sys_id, sys_mod_count)` and bounded by
  `snow.source.dedup.window.records` (default `50000`). The cache is in memory, so after a
  restart it is empty and the overlap window is re-emitted.
- **The duplicate bound.** After a restart, a consumer may see again at most the rows whose
  timestamp falls within `overlap.seconds` of the committed cursor, plus any rows on the
  page that was in flight when the task stopped. In steady state the safety lag keeps the
  current second out of the sweep, so a second is only ever read after the instance has
  finished writing it, and duplicates do not occur without a restart.
- **A row updated twice is delivered in its latest observable state.** If a row changes
  again while a page is being read, its `sys_updated_on` moves later and it is read again
  in a later bucket, with a higher `sys_mod_count`, so the dedup cache does not suppress
  it. The intermediate state may or may not have been emitted; only the Table API's view at
  read time is promised.
- **No deletes.** The Table API cannot report rows that no longer exist, and the source
  never claims otherwise: every record carries the header `snow.source.operation=UPSERT`
  and there are no tombstones. The header is deliberately named so a replayed source topic
  can never be mistaken for a sink command stream. Delete capture needs a ServiceNow-side
  component; the repository reserves the optional, separately governed
  [event bridge](https://github.com/osodevops/kafka-connect-servicenow-oss/tree/main/servicenow-event-bridge)
  for it.

**Consumer guidance:** deduplicate on the record key (`sys_id`) plus `sys_updated_on` and
`sys_mod_count` from the value, or make downstream processing idempotent. A compacted
topic keyed by `sys_id` converges to the latest state on its own.

## Ordering

- Within one table, records are emitted in `(timestamp, sys_id)` order: ascending by the
  timestamp field, then by `sys_id` within a second. That is the order the Table API
  returns them in, and the order the cursor advances in.
- A table maps to one topic. With the default key-based partitioner, all versions of a
  row land in the same partition in order, because the key is its `sys_id`.
- Across tables there is no ordering; each table is polled independently, and different
  tables may be owned by different tasks.
- Rows that share a second are ordered by `sys_id`, which is a random hexadecimal string,
  not by the order in which they were written. Nothing in ServiceNow exposes write order
  inside a second.

## Sink connector

- **Offsets advance only after success or a durable error path.** `put()` returns when
  every record in the batch has either been accepted by ServiceNow, been handed to the
  dead letter queue or the error reporter, or failed permanently under
  `behavior.on.api.errors=fail`. `preCommit` returns only the offsets of completed
  records. Redelivery after a crash can re-write records that had already been applied.
- **Per-partition ordering is preserved.** Records from one Kafka partition are written
  sequentially in offset order through a single lane; only records from different
  partitions are written concurrently, up to `snow.sink.max.in.flight`. Two updates to
  the same `sys_id` arriving on the same partition are therefore applied in order, which
  is what a keyed producer gives you.
- **Idempotency by operation.** `PATCH`, `PUT` and `DELETE` are addressed by an immutable
  `sys_id`, so re-delivering them converges: patching the same fields twice or deleting a
  row that is already gone (a `404` on a retried `DELETE` counts as success) does no harm.
  `CREATE` is a `POST` without an idempotency key, so a redelivered create can insert a
  duplicate row. If your pipeline can redeliver creates, set
  `snow.sink.create.ambiguous.behavior=correlation_lookup` with a unique correlation field
  so the sink looks the row up before posting again; see
  [Error handling](../reference/error-handling.md).

## Exactly-once is not supported

The source connector's `exactlyOnceSupport()` returns `UNSUPPORTED`, so a worker with
`exactly.once.source.support=required` refuses to start it. Kafka Connect's exactly-once
source framework (KIP-618) can make the produce-and-commit step atomic, but it cannot make
the read from ServiceNow transactional: the Table API offers no snapshot, no change log
and no way to observe deletes, and an instance can be updating rows while a page is read.
Declaring exactly-once would promise something the upstream API cannot back. The
at-least-once contract above, with a stated duplicate bound and a stable key, is honest
and sufficient for idempotent consumers.
