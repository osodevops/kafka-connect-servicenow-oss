---
title: "Introduction"
description: "Open-source, Table API-based ServiceNow connectors for Apache Kafka Connect."
slug: "/"
sidebar_position: 1
---

# Kafka Connect ServiceNow

**OSO Kafka Connect ServiceNow** is an open-source (Apache-2.0) pair of ServiceNow
connectors for Apache Kafka Connect. It is the **open-source alternative to Confluent's
proprietary ServiceNow connectors**: two connectors deliver functional parity with
Confluent's five, built clean-room on ServiceNow's public **REST Table API** with no
ServiceNow-side application to install and no licence keys.

## The connectors

| Connector | Direction | ServiceNow API | Replaces (Confluent) |
|---|---|---|---|
| [Source](connectors/source.md) | ServiceNow to Kafka | REST Table API (`GET`), keyset polling on `sys_updated_on` or `sys_created_on` | Platform Source, Cloud Source Legacy, Cloud Source V2 |
| [Sink](connectors/sink.md) | Kafka to ServiceNow | REST Table API (`POST`, `PATCH`, `PUT`, `DELETE`) | Platform Sink, Cloud Sink |

Both connectors share one library, `snow-core`, which owns authentication, the HTTP
transport, the Table API client, the cursor engine, schema mapping, retries and the
per-instance concurrency limit. The connectors themselves stay thin.

## Why the keyset cursor matters

ServiceNow timestamps such as `sys_updated_on` have **one-second precision**, and a bulk
update or an import set can touch more than 10,000 rows in the same second. A connector
that checkpoints only a timestamp has to choose between two failure modes at that
boundary: skip rows that share a second with the checkpoint, or re-emit the whole second
on every poll. Paging through a second with `sysparm_offset` is no better, because rows
that change while the scan runs shift the page boundaries and rows are skipped or
repeated. Confluent's own Source V2 documentation warns about loss or duplicates when
polling near that boundary.

The source connector avoids the problem instead of tuning around it:

- The cursor is the tuple `(timestamp, sys_id)`, ordered by both, so the connector always
  knows exactly where it stopped inside a second.
- Two query shapes do the work: one drains the rest of the current second by `sys_id`,
  the other moves to later seconds up to a high-water mark held below the server clock by
  a safety lag, so rows still being written are picked up by the next sweep.
- Every restart re-reads a short overlap window before the committed cursor and suppresses
  versions it has already emitted, using `(timestamp, sys_id, sys_mod_count)`.

The result is at-least-once delivery with no lost rows and a duplicate bound you can state
in seconds. The design is recorded in the repository as an architecture decision record
and proved by tests that put 12,000 rows in one second and restart the task after every
page. Details are in [Delivery semantics](concepts/delivery-semantics.md) and
[Offsets and recovery](concepts/offsets-and-recovery.md).

## Highlights

- **Historical backfill plus continuous polling.** Start any table from any UTC
  timestamp, catch up at full page rate, then stream at the poll interval.
- **Multi-table, multi-task.** Any Table API table or database view, whole tables assigned
  to tasks by rendezvous hashing, per-table query, projection, display value and domain
  settings. There is no five-table cap.
- **A sink with explicit policies.** Create, patch, put and delete through the Table API
  with per-partition ordering, bounded concurrency, three policies for writes whose
  outcome is unknown, success and error reporter topics, and the native Connect dead
  letter queue.
- **Basic and OAuth 2.0.** Client credentials or password grant, single-flight token
  refresh, secrets declared as Connect `PASSWORD` types and never logged.
- **Rate-limit aware.** Honours `Retry-After`, backs off with jitter, bounds retries by
  attempts and elapsed time, caps in-flight requests per instance.
- **Runs anywhere.** Apache Kafka 3.x and 4.x, Amazon MSK Connect, Strimzi or Confluent
  Platform; any converter (JSON, Avro, Protobuf); `plugin.discovery=service_load` ready.
- **Fully testable locally.** The repository ships a fake ServiceNow (a WireMock Table API
  over an in-memory table store, with an OAuth token endpoint and fault injection) so
  every behaviour is verified without an instance.
- **Verified migration path.** A scripted Confluent configuration translator and a cutover
  procedure that is executed in CI on every build, producing a machine-readable evidence
  artefact.

## Where to go next

- **New here?** Start with [Getting Started](getting-started/index.md).
- **Configuring a connector?** Read the [Source](connectors/source.md) and
  [Sink](connectors/sink.md) pages, then the generated configuration reference for the
  [source](reference/configuration/source.md) and the [sink](reference/configuration/sink.md).
- **Coming from Confluent?** See [Migrating from Confluent](migration/confluent.md).
- **Running it in production?** OSO, who build the connectors, offer an
  [Enterprise support subscription](enterprise-support.md): maintained releases,
  ServiceNow family release readiness, a 60-minute P1 response in business hours, and an
  onboarding review.
- **Contributing?** See [Building](development/building.md) and
  [Local testing](development/local-testing.md).
