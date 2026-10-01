---
title: "Offsets and recovery"
description: "Cursor tuples, query fingerprints, restarts and offset resets."
sidebar_position: 3
---

# Offsets and recovery

The source connector keeps one offset per table in Kafka Connect's offset store. Nothing
else is persisted: no state topic, no files on the worker. This page shows what the
offset looks like, what makes it incompatible, what happens on restart, and how to
inspect, reset or rewind it with the Connect REST API.

## What is stored

### Source partition

The partition identifies the stream a cursor belongs to. It is derived from configuration
only; credentials never influence it.

```json
{
  "instance": "acme.service-now.com",
  "table": "incident",
  "query_fingerprint": "sha256:5c8b1f7e0a3d...",
  "timestamp_field": "sys_updated_on"
}
```

- `instance` is the normalised host of `snow.url` (lower case, no scheme, no trailing
  slash), so a sandbox and a production instance never share offsets.
- `table` is the ServiceNow table or view name (`snow.table.<alias>.name`), not the alias,
  so renaming an alias in the configuration does not lose the cursor.
- `query_fingerprint` is a SHA-256 over the table name, the normalised base query and the
  `query.domain` flag.
- `timestamp_field` is the cursor field (`sys_updated_on` by default, or the prefixed name
  for a view).

### Source offset

```json
{
  "version": "1",
  "timestamp": "2026-09-29 07:30:42",
  "sys_id": "8f4bc0d1c611227a0100e2d3f8a6b9e1",
  "phase": "stream",
  "fingerprint": "sha256:5c8b1f7e0a3d..."
}
```

- `version` is the offset format version. A future format change bumps it; an offset with
  an unknown version is refused, never guessed at.
- `timestamp` and `sys_id` are the cursor tuple: the last row emitted, in UTC with
  second precision.
- `phase` is `backfill` while the poller is catching up (a sweep ended on a full page) and
  `stream` once it has reached the high-water mark and sleeps between polls. It is
  informational; the poller recomputes it on the next sweep.
- `fingerprint` repeats the partition's fingerprint so an offset can be checked on its own.

The offset is committed with every record, so a restart resumes from the last row that
reached Kafka, not from the start of a page.

## What changes the fingerprint

| Change | Fingerprint | Effect |
|---|---|---|
| `snow.table.<alias>.name` | changes (and the partition's `table`) | New partition; the table starts from its `start.timestamp` |
| `snow.table.<alias>.query` (base encoded query) | changes | New partition; the old offset is left in place and ignored |
| `snow.table.<alias>.query.domain` | changes | New partition, because the row set visible to the cursor changes |
| `snow.table.<alias>.timestamp.field` | partition changes (`timestamp_field`) | New partition |
| `snow.url` host | partition changes (`instance`) | New partition |
| `snow.table.<alias>.fields` (projection) | unchanged | Same cursor; new fields appear on new records only |
| `snow.table.<alias>.display.value`, `exclude.reference.link`, `query.category` | unchanged | Same cursor; record shape changes from the next record |
| `batch.size`, `poll.interval.ms`, `overlap.seconds`, `safety.lag.seconds` | unchanged | Same cursor |
| Alias name, topic name | unchanged | Same cursor; records go to the new topic from the next poll |

The rule: anything that changes **which rows** the cursor walks over changes the
fingerprint, anything that changes only **what each row looks like** does not. Whitespace
and the order of independent `^` terms are normalised before hashing, so reformatting a
query is safe.

When the connector finds an offset whose `fingerprint` does not match the partition it
computed (which can only happen if the offset store was edited by hand), it fails the
table with a message naming the partition rather than silently reusing the cursor.

## Restart behaviour

| Scenario | Behaviour |
|---|---|
| Worker restart, rebalance, task moved to another worker | Resume from the committed tuple minus `snow.source.overlap.seconds`; rows in the overlap are re-emitted, then suppressed as the cache fills |
| Restart in the middle of a page | The offset of the last produced record is committed; the rest of the page is re-read. No loss; duplicates limited to the overlap plus the in-flight page |
| Restart during backfill | `phase=backfill`; sweeps continue back to back from the cursor until the high-water mark is reached |
| Long outage (hours or days) | The cursor is a timestamp, not a retention-bound position; polling resumes from it and catches up at full page rate. Rows updated more than once during the outage are seen once, in their latest state |
| Table added to `snow.tables` | New partition, starts from its `start.timestamp` |
| Table removed | Its offset stays in the store, unused; re-adding the same table and query resumes from it |
| `tasks.max` changed | Rendezvous hashing reassigns tables with minimal movement; offsets are per table, not per task, so nothing is lost |
| Incompatible offset (`version` or `fingerprint` mismatch) | The table fails with a diagnostic; reset the offset (below) |

`start.timestamp` is read **only when there is no offset** for the partition. Changing it
for a table that already has a cursor has no effect until the offset is removed.

## Inspecting offsets

Kafka Connect 3.6 and later exposes offsets through the REST API (KIP-875):

```bash
curl -s localhost:8083/connectors/servicenow-source/offsets | jq
```

```json
{
  "offsets": [
    {
      "partition": {
        "instance": "acme.service-now.com",
        "table": "incident",
        "query_fingerprint": "sha256:5c8b1f7e0a3d...",
        "timestamp_field": "sys_updated_on"
      },
      "offset": {
        "version": "1",
        "timestamp": "2026-09-29 07:30:42",
        "sys_id": "8f4bc0d1c611227a0100e2d3f8a6b9e1",
        "phase": "stream",
        "fingerprint": "sha256:5c8b1f7e0a3d..."
      }
    }
  ]
}
```

The task also logs each table's partition map at `INFO` on start, so the partition you
need for a `PATCH` is in the worker log. On older workers, read the offset storage topic
(`offset.storage.topic`) directly; the partition and offset are stored as JSON keyed by
connector name.

## Resetting and rewinding offsets

Offsets can only be changed while the connector is **stopped** (not paused):

```bash
# 1. stop the connector; tasks are shut down, configuration is kept
curl -s -X PUT localhost:8083/connectors/servicenow-source/stop

# 2a. rewind one table to a point in time
curl -s -X PATCH -H 'Content-Type: application/json' \
  localhost:8083/connectors/servicenow-source/offsets -d '{
  "offsets": [{
    "partition": {
      "instance": "acme.service-now.com",
      "table": "incident",
      "query_fingerprint": "sha256:5c8b1f7e0a3d...",
      "timestamp_field": "sys_updated_on"
    },
    "offset": {
      "version": "1",
      "timestamp": "2026-09-01 00:00:00",
      "sys_id": "",
      "phase": "backfill",
      "fingerprint": "sha256:5c8b1f7e0a3d..."
    }
  }]
}'

# 2b. or drop one table's offset (a null offset deletes the partition)
curl -s -X PATCH -H 'Content-Type: application/json' \
  localhost:8083/connectors/servicenow-source/offsets \
  -d '{"offsets":[{"partition":{...},"offset":null}]}'

# 2c. or reset every offset of the connector
curl -s -X DELETE localhost:8083/connectors/servicenow-source/offsets

# 3. resume
curl -s -X PUT localhost:8083/connectors/servicenow-source/resume
```

An empty `sys_id` means "before the first row of that second", so a rewritten offset with
`"sys_id": ""` re-reads the whole of `timestamp`. Keep `fingerprint` equal to the
partition's `query_fingerprint`; a mismatch is refused on start.

## Backfilling again

To replay a table from an earlier point, or from the beginning, do not edit the cursor
by hand; change the configured start and clear the offset, so the configuration stays
the source of truth:

1. Set `snow.table.<alias>.start.timestamp` to the point you want (for example
   `2026-01-01 00:00:00`, or leave the default `1970-01-01 00:00:00` for everything).
2. Stop the connector, delete the table's offset with a null-offset `PATCH` (or `DELETE`
   all offsets if every table should replay), and resume.
3. The poller reads no offset, starts from `start.timestamp` in the `backfill` phase and
   emits every row with `timestamp` at or after that point, in cursor order.

The replay is deterministic: the same start point, the same query and the same instance
content produce the same records in the same order. Consumers see every replayed row
again, so treat a backfill as a planned duplicate window.

:::tip Changing the query is a reset
Because the base query is part of the partition, changing `snow.table.<alias>.query`
starts the table again from `start.timestamp` under a new partition without any REST
call. Set `start.timestamp` to the intended point **before** changing the query, or the
new partition will backfill from 1970.
:::
