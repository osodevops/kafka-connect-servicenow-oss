---
title: "Architecture"
description: "How snow-core, the source and the sink fit together."
sidebar_position: 1
---

# Architecture

```text
                   +--------------------------------------------------+
                   |          kafka-connect-servicenow (repo)          |
                   +--------------------------------------------------+
  ServiceNow       |  snow-core (shared library)                      |      Kafka
  ----------       |   config   CoreConfigDefs: snow.url, auth, http, |      -----
  REST Table API   |            tls, retry keys defined once          |
  GET      <-------+   auth     Basic, OAuth2 client credentials and  +---> SourceRecord
  POST/PATCH/      |            password grant (TokenProvider)        |      (any converter)
  PUT/DELETE <-----+   http     Java HttpClient, gzip, X-Request-Id,  |
  /oauth_token.do  |            proxy, TLS/mTLS, 401 refresh-once     |
                   |   table    TableApiClient: list/get/create/      |
                   |            patch/put/delete                      |
                   |   cursor   (timestamp, sys_id) keyset queries,   |
                   |            offset codec, query fingerprint,      |
                   |            dedup cache, watermark                |
                   |   schema   schemaless / strings / typed mapping, |
                   |            sys_dictionary metadata client        |
                   |   common   error classifier, RetryPolicy,        |
                   |            redaction, Connect exception mapping  |
                   |   limits   per-instance ConcurrencyLimiter,      |
                   |            Retry-After parsing                   |
                   +--------------------------------------------------+
                   |  Connectors (thin):                              |
                   |   ServiceNowSourceConnector / Task               |
                   |   ServiceNowSinkConnector / Task                 |<--- SinkRecord
                   +--------------------------------------------------+
                   |  Test harness (snow-core test-jar):              |
                   |   MockServiceNowServer, TableStore, FaultInjector|
                   +--------------------------------------------------+
```

## Design principles

- **One shared `snow-core` library** owns every piece of ServiceNow protocol logic:
  authentication and token refresh, the HTTP transport, the Table API client, the cursor
  engine and offset codec, schema mapping, error classification and bounded retries, and
  the per-instance concurrency limit. Both connectors depend on the same artefact, so
  there are no shaded copies and no divergent behaviour.
- **Configuration defined once.** `CoreConfigDefs` declares the `snow.url`, `snow.auth.*`,
  `snow.oauth.*`, `snow.http.*`, `snow.tls.*` and `snow.retry.*` keys, and both connectors
  add them to their own `ConfigDef`. Secrets are `PASSWORD` types.
- **Checkpointing through Kafka Connect's offset store.** The source's cursor tuple lives in
  each record's source offset, exactly as the framework intends. There is no external
  state, no state topic and nothing to back up separately.
- **Per-table isolation.** Each table has its own poller with its own cursor, dedup cache
  and error state, so a table that returns 403 does not stop the others owned by the same
  task.
- **Bounded everything.** Retries are capped by attempts and elapsed time; in-flight
  requests are capped per instance; the dedup cache and response bodies are capped in
  size; a backfill of a million rows runs in a 256 MB heap.
- **Clean room.** Everything is implemented from ServiceNow's public Table API and OAuth
  documentation, with an independent `snow.*` namespace. Confluent's documentation was
  read only to specify the externally visible compatibility recorded in the
  [migration guide](../migration/confluent.md).

## snow-core packages

| Package | Contents |
|---|---|
| `config` | `CoreConfigDefs` (shared keys), `CoreConfig` (validated view, builds the auth, HTTP and retry configs, normalises the instance host) |
| `auth` | `TokenProvider` interface, `BasicTokenProvider`, `OAuthTokenProvider` (client credentials and password grants, cached token, single-flight refresh) |
| `http` | `ServiceNowHttpClient` over the Java 11 `HttpClient`, `HttpConfig` (timeouts, proxy, TLS, response size and concurrency caps), `RequestSpec`, `HttpResult` |
| `table` | `TableApiClient` and the value types (`QueryRequest`, `EncodedQuery`, `Page`, `Record`, `FieldValue`, `WriteResult`, `ServiceNowErrorBody`); validates table names and `sys_id` path segments |
| `cursor` | `Cursor`, `SnowTimestamp`, `KeysetQueryBuilder`, `SourceOffset`, `SourcePartition`, `CursorCodec`, `QueryFingerprint`, `DedupCache`, `Watermark` |
| `schema` | `SchemaMode`, `RecordSchemaMapper`, `TypedFieldMapping`, `ValueCoercions`, `TableMetadataClient` (`sys_db_object` and `sys_dictionary` lookups with a TTL cache) |
| `common` | `ServiceNowException` and `ServiceNowApiException` (with `retryable` and `ambiguous` flags), `ErrorClassifier`, `RetryPolicy`, `Redaction`, `ConnectExceptions` |
| `limits` | `ConcurrencyLimiter` (per-instance semaphore, resizable), `RateLimitInfo` (`Retry-After` parsing) |
| `testing` (test-jar) | The fake ServiceNow: `MockServiceNowServer`, `TableApiTransformer`, `TableStore`, `EncodedQueryEvaluator`, `FaultInjector`, `MutableClock`, `RequestJournal`, `FakeServiceNowMain` |

## The source pipeline

```text
ServiceNowSourceConnector
   |  validates SourceConfig (fails fast), expands snow.table.<alias>.* specs
   |  taskConfigs(maxTasks): TaskAssignment (rendezvous hashing) -> min(maxTasks, tables) tasks
   v
ServiceNowSourceTask (one per task config)
   |  round-robins its TablePollers; one failing table is isolated from the rest
   v
TablePoller (one per table)              StartupProbe (once per table)
   |  INIT -> DRAIN_BUCKET | NEXT_BUCKETS -> IDLE
   |  owns Cursor, sweep high-water mark, DedupCache, phase, metrics
   v
SourceRecordFactory
      key sys_id (STRING), value via RecordSchemaMapper, timestamp = cursor time,
      headers snow.table, snow.instance, snow.source.operation=UPSERT,
              snow.extracted_at, snow.schema.mode; optional envelope
```

**Task assignment.** `taskConfigs` hashes every `table#taskIndex` pair with murmur3 and
gives each table to the task with the highest score. Adding or removing a task moves as
few tables as possible, and a table is owned by exactly one task; tables are never split.

**The poll loop.** A sweep starts by fixing `HI = serverNow - safety.lag.seconds`. From the
committed cursor `(T, S)` the poller alternates between two query shapes:

- `DRAIN_BUCKET`: `<query>^<ts>=T^sys_id>S^ORDERBYsys_id`, page by page until a short page
  says the second is exhausted.
- `NEXT_BUCKETS`: `<query>^<ts>>T^<ts><=HI^ORDERBY<ts>^ORDERBYsys_id`. A full page means
  the last second on it may continue, so the poller switches back to `DRAIN_BUCKET`; a
  short page means the sweep has reached `HI`.

When a sweep ends in the `backfill` phase the next one starts immediately; in the
`stream` phase the poller sleeps for the poll interval. Every emitted row updates the
cursor and carries the full offset, so a restart at any page resumes correctly (see
[Offsets and recovery](offsets-and-recovery.md)).

## The sink pipeline

```text
ServiceNowSinkTask.put(records)
   |
   v
TableRouter          fixed | topic_map | header (allowlist + path-segment validation)
IdExtractor          sys_id from primitive key, key field, value field or header
OperationResolver    header -> fixed operation -> inference (null value = DELETE,
                     no sys_id = CREATE, key + value = update method)
RecordMapper         Struct / Map / primitive -> JSON body; rename, allow, deny lists;
                     null and nested policies; logical types; reserved fields stripped;
                     unknown-field policy via TableMetadataClient
   |
   v
ServiceNowWriter     groups by TopicPartition, one sequential lane per partition,
                     lanes on a bounded executor: Semaphore(snow.sink.max.in.flight)
                     plus the core ConcurrencyLimiter; returns an Outcome per record
   |   AmbiguousWritePolicy   retry | fail_ambiguous | correlation_lookup for CREATE
   |                          whose outcome is unknown; PATCH/PUT retried; DELETE
   |                          retry hitting 404 counts as success
   v
Outcomes             success -> Reporter success topic
                     permanent error -> DLQ (errant record reporter) and/or Reporter
                     error topic, then behavior.on.api.errors
                     retryable exhaustion -> RetriableException
preCommit            returns only offsets whose records completed
```

Ordering is preserved within a Kafka partition because a partition's records always go
through one lane in order; concurrency comes from running lanes for different partitions
side by side, up to `snow.sink.max.in.flight`. The `Reporter` publishes with its own
`KafkaProducer` to `snow.sink.reporter.bootstrap.servers` and is flushed in `preCommit`,
so a success report is never visible before the offset that covers it can be committed.

## The fake ServiceNow

`snow-core` publishes a test-jar containing a fake instance built on WireMock. It
implements Basic auth and `/oauth_token.do`, a stateful Table API over an in-memory
`TableStore` that maintains `sys_created_on`, `sys_updated_on` and `sys_mod_count`, an
`EncodedQueryEvaluator` for the query subset the connectors use, and a `FaultInjector`
for 401s, 429 storms, 5xx, malformed bodies, timeouts after a write has been applied, and
ACL simulation. Every connector test runs against it over real HTTP, and the same fake
runs standalone (`FakeServiceNowMain`) for the [quickstart](../getting-started/quickstart.md)
and the Docker end-to-end tests. See [Local testing](../development/local-testing.md).

## Table API cheat sheet

| Operation | Request | Used by |
|---|---|---|
| List | `GET /api/now/table/{table}?sysparm_query=...&sysparm_limit=...&sysparm_fields=...` | Source polling, sink `correlation_lookup` |
| Get | `GET /api/now/table/{table}/{sys_id}` | Startup probe, contract tests |
| Create | `POST /api/now/table/{table}` | Sink `CREATE` |
| Partial update | `PATCH /api/now/table/{table}/{sys_id}` | Sink `PATCH` (default update method) |
| Update | `PUT /api/now/table/{table}/{sys_id}` | Sink `PUT` (Confluent parity) |
| Delete | `DELETE /api/now/table/{table}/{sys_id}` | Sink `DELETE` |
| Token | `POST /oauth_token.do` | OAuth grants and refresh |

The source always sends `sysparm_no_count=true` and
`sysparm_suppress_pagination_header=true`, never uses `sysparm_offset`, and adds
`sysparm_display_value`, `sysparm_exclude_reference_link`, `sysparm_query_category` and
`sysparm_query_no_domain` from the per-table configuration.
