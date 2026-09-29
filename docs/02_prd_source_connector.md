# PRD-01: ServiceNow Source Connector

## Objective

Continuously copy creations and updates from one or more ServiceNow tables or views to Kafka with historical backfill, deterministic restarts, and at-least-once delivery. Achieve parity with Confluent Source V2 and preserve migration options from the legacy connector.

## Scope

### In scope

- Any Table API-readable table or view except explicitly denylisted system/audit tables.
- Historical start timestamp and continuous polling.
- Multiple tables and multiple tasks.
- Per-table topic, timestamp field, query, projection, display/reference/domain settings.
- Basic and OAuth2 client-credentials authentication.
- Schemaless, string-schema, and typed-schema modes.
- Custom offset inspection/reset through standard Kafka Connect offset APIs.
- At-least-once delivery, retries, proxy, TLS/mTLS, metrics.

### Out of scope

- Deletes from polling.
- Attachments and binary bodies.
- Journaling every field revision.
- Guaranteed exactly-once delivery.
- Installing ServiceNow-side code.

## Task assignment

The connector accepts an arbitrary list of table specifications. `taskConfigs(maxTasks)` assigns whole tables consistently using rendezvous hashing. A table is owned by exactly one task; one table is never split across tasks in v1. Reconfiguration should minimize table movement.

## Polling algorithm

For each table:

1. Load the source offset or configured start timestamp.
2. Query with the user's base filter plus a connector cursor predicate.
3. Order by timestamp field and `sys_id`.
4. Request `batch.size` records and all required cursor fields.
5. Suppress exact row versions already emitted in the overlap, ideally using `sys_mod_count`; do not discard all rows earlier than the last emitted tuple within an unclosed timestamp bucket.
6. Emit `SourceRecord`s in tuple order with a versioned offset representing a safely closed watermark and in-flight cursor state.
7. Continue immediately for a full page; otherwise sleep for the per-table interval.

ServiceNow's Table API supports encoded filters, ordering, projection, display values, limit and offset ([Table API](https://www.servicenow.com/docs/r/zurich/api-reference/rest-apis/c_TableAPI.html)). The connector should prefer keyset behavior because `sysparm_offset` can shift under concurrent mutation. A lagged high-water mark closes a timestamp bucket only after the configured safety interval; neither this nor a tuple cursor can recover intermediate states overwritten between polls.

## Record model

Default:

- Key: `sys_id` string.
- Value: ServiceNow fields.
- Topic: configured per table.
- Timestamp: parsed cursor timestamp.
- Headers:
  - `snow.table`
  - `snow.instance`
  - `snow.operation=UPSERT`
  - `snow.extracted_at`
  - `snow.schema.mode`

Optional envelope:

```json
{
  "before": null,
  "after": {"sys_id": "..."},
  "source": {"table": "incident", "instance": "..."},
  "op": "u",
  "ts_ms": 1790667042000
}
```

The Table API cannot supply `before`, so it remains null.

## Configuration

Global:

| Property | Default |
|---|---:|
| `snow.tables` | required comma-separated aliases |
| `snow.source.schema.mode` | `schemaless` |
| `snow.source.poll.interval.ms` | `5000` |
| `snow.source.batch.size` | `5000` |
| `snow.source.overlap.seconds` | `2` |
| `snow.source.safety.lag.seconds` | `1` |
| `snow.source.dedup.window.records` | `50000` |
| `snow.source.emit.envelope` | `false` |

Per alias:

| Property | Meaning |
|---|---|
| `snow.table.<alias>.name` | ServiceNow table/view |
| `snow.table.<alias>.topic` | Kafka topic |
| `snow.table.<alias>.start.timestamp` | UTC timestamp |
| `snow.table.<alias>.timestamp.field` | Default `sys_updated_on` |
| `snow.table.<alias>.query` | Base encoded query |
| `snow.table.<alias>.fields` | Projection |
| `snow.table.<alias>.display.value` | `false`, `true`, `all` |
| `snow.table.<alias>.exclude.reference.link` | Boolean |
| `snow.table.<alias>.query.domain` | Boolean |
| `snow.table.<alias>.batch.size` | Override |
| `snow.table.<alias>.poll.interval.ms` | Override |

Validate that projected fields include `sys_id` and the timestamp field. Fingerprint query-affecting configuration and refuse to silently reuse an incompatible offset.

## Schema behavior

- `schemaless`: JSON-compatible map.
- `strings`: optional strings for all fields.
- `typed`: explicit user mapping plus optional metadata snapshot.

Field disappearance must not immediately remove a field from a schema. In typed mode, schema changes are controlled by `snow.source.schema.evolution=fail|backward|permissive`.

## Errors and operations

- 401: refresh OAuth once, then fail.
- 403: fail with table/field/ACL diagnostic.
- 404: fail for missing table; optionally skip missing row lookups.
- 429/5xx: retry through `snow-core`.
- Invalid cursor/timestamp: report exact row and fail by default.
- Bad row: `errors.tolerance=all` may route it to a source error topic, but the offset must not advance past unreported data without an explicit data-loss policy.

Metrics include per-table cursor time, wall-clock lag, poll duration, records, pages, duplicates suppressed, retries, throttled time, schema versions, and last successful request.

## Acceptance criteria

- Backfills at least one million rows without unbounded memory.
- No missing rows when over 10,000 stable records share one timestamp in the test fixture.
- Kill/restart at every point in a page yields no loss; duplicates remain within documented at-least-once bounds.
- A row updated twice during backfill is eventually emitted in its latest observable state; the intermediate state is not promised.
- Five tables run across 1–5 tasks with stable assignment.
- Query, projection, display, view, and domain settings pass contract tests.
- Offset reset replays deterministically.
- Source does not claim to emit deletes.

## Confluent migration mapping

| Confluent | OSS |
|---|---|
| `servicenow.url` | `snow.url` |
| `servicenow.table` | `snow.table.<alias>.name` |
| `kafka.topic` / `table{i}.topic` | `snow.table.<alias>.topic` |
| `servicenow.since` / `table{i}.start.timestamp` | `snow.table.<alias>.start.timestamp` |
| `batch.max.rows` / `table{i}.batch.size` | per-table batch size |
| `poll.interval.s` / request interval | per-table poll interval |
| `table{i}.pagination.query` | `snow.table.<alias>.query` |
| `table{i}.allowlisted.fields` | `snow.table.<alias>.fields` |
