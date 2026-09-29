# Open-Source ServiceNow ↔ Kafka Connectors: Feasibility and Clean-Room Strategy

## Executive summary

An open-source replacement is technically straightforward and legally low-risk when implemented from public documentation and ServiceNow's public APIs. Confluent's products are wrappers around ServiceNow's Table API: the self-managed source polls one table, the newer Cloud Source V2 polls up to five tables, and the sink maps Kafka records to Table API create, update, and delete operations ([Confluent Platform source](https://docs.confluent.io/kafka-connectors/servicenow/current/source-connector/overview.html), [Confluent Source V2](https://docs.confluent.io/cloud/current/connectors/cc-servicenow-source-v2.html), [Confluent Platform sink](https://docs.confluent.io/kafka-connectors/servicenow/current/sink-connector/overview.html)).

The recommended OSS product is smaller than Confluent's deployment-specific catalog:

| OSS component | Purpose | Confluent parity target |
|---|---|---|
| `snow-core` | Authentication, Table API, cursoring, schema mapping, throttling | Shared behavior across every plugin |
| ServiceNow Source | Multi-table backfill plus continuous polling | Platform Source, Cloud Legacy Source, Cloud Source V2 |
| ServiceNow Sink | Create, update, patch, and delete | Platform Sink and Cloud Sink |
| Event Bridge, optional | Near-real-time outbound changes and deletes | Differentiation beyond Confluent parity |

Confluent deprecated its Cloud legacy source on April 6, 2026, with end of life on April 6, 2027, and directs customers to Source V2 ([Cloud connector catalog](https://docs.confluent.io/cloud/current/connectors/overview.html)). Therefore, V2 is the primary parity target, while legacy property mappings should exist only as a migration guide.

## Product recommendation

### Repository and modules

Use one Gradle multi-module monorepo:

```text
kafka-connect-servicenow-oss/
  snow-core/
    auth/
    http/
    table-api/
    cursor/
    schema/
    telemetry/
  connect-servicenow-source/
  connect-servicenow-sink/
  servicenow-event-bridge/       # optional ServiceNow app
  integration-tests/
  packaging/
  docs/
```

Publish source and sink as independent plugin ZIPs. Keep the optional ServiceNow-side application out of the baseline connector ZIP and release it independently.

### Language

Use **Kotlin or Java**. Kafka Connect plugins execute in the JVM and implement Kafka's Connect SPI. Java provides the widest contributor pool; Kotlin improves safety and ergonomics while retaining Java interoperability. Scala is viable but supplies no protocol advantage and narrows the contributor pool.

The archived IBM connector is Java, Apache-2.0, and demonstrates a multi-table source architecture with source tasks and table subtasks, but it was archived on November 1, 2024 and has no published releases ([IBM repository](https://github.com/IBM/kafka-connect-servicenow)). It may be studied as permissively licensed prior art, but the new implementation should not inherit its design blindly.

### License

Use Apache-2.0 for the repository. Apache Kafka's Connect API and common JVM HTTP/JSON libraries support a permissive dependency set. Avoid bundling Confluent serializers or proprietary client-side encryption libraries; converters should remain user-supplied Kafka Connect components.

## Clean-room position

The implementation should target ServiceNow's public interfaces, not Confluent's implementation:

- Use the documented Table API endpoints for list, get, insert, update, patch, and delete ([ServiceNow Table API](https://www.servicenow.com/docs/r/zurich/api-reference/rest-apis/c_TableAPI.html)).
- Read Confluent's public documentation only to specify externally observable compatibility.
- Never decompile or inspect Confluent connector JARs.
- Use a new configuration namespace such as `snow.*`.
- Write descriptions independently and provide Confluent-to-OSS mappings in a migration appendix.
- Keep an architecture decision record and source-linked PRDs showing independent derivation.

This is interoperability engineering. The implementation does not need Confluent code, private protocols, or undisclosed ServiceNow behavior.

## Current Confluent product inventory

| Product | Scope | Important behavior |
|---|---|---|
| Platform Source | One table, one task | Polling, historical start date, at-least-once, Basic auth, proxy/mTLS |
| Cloud Source Legacy | One table, one task | Deprecated; timestamp polling and schemaless JSON option |
| Cloud Source V2 | Up to five tables and multiple tasks | Basic or OAuth client credentials, custom pagination query, field allowlist, custom offsets |
| Platform Sink | One table, multiple tasks | POST/PUT/DELETE selected from record key/value, result/error reporter |
| Cloud Sink | One table per connector configuration | Schema Registry formats, success/error/DLQ topics, Basic auth |

There is no Confluent ServiceNow source-side delete capture. Source V2 explicitly emits creations and updates based on `sys_updated_on` or `sys_created_on`, not deletes ([Confluent Source V2](https://docs.confluent.io/cloud/current/connectors/cc-servicenow-source-v2.html)).

## Feature-parity matrix

| Capability | Required OSS behavior | Priority |
|---|---|---|
| Historical source | Start at configured UTC timestamp and page until caught up | P0 |
| Continuous source | Poll creations and updates | P0 |
| Multi-table | Arbitrary table list, task assignment, one topic per table | P0 |
| At-least-once | Persist source partition and tuple offset | P0 |
| Same-second correctness | Timestamp plus `sys_id` cursor, overlap and dedup | P0 |
| Custom queries | User filter combined safely with connector cursor | P0 |
| Field projection | `sysparm_fields`; always include cursor fields | P0 |
| Display/reference options | Actual/display/both and reference-link suppression | P1 |
| Views | Source accessible database views with configured cursor fields | P1 |
| Basic auth | Username/password | P0 |
| OAuth | Client credentials; extensible token provider | P0 |
| Proxy and TLS | HTTPS proxy, truststore, keystore/mTLS | P1 |
| Source schema modes | Schemaless JSON, string schema, metadata-assisted typed schema | P0/P1 |
| Sink create/update/delete | POST, PATCH or PUT, DELETE | P0 |
| Sink reporter/DLQ | Success, error, and Connect DLQ paths | P0 |
| Retry and rate limits | Retry-After/reset-aware backoff, jitter and circuit breaking | P0 |
| Source deletes | Optional ServiceNow event bridge | P2, differentiation |
| CSFLE/CSPE | Integrate through converters/rules, do not clone proprietary implementation | Deferred |

## Key technical decision: tuple cursoring

ServiceNow timestamps provide one-second precision, while many records can change in one second. A timestamp-only cursor can either miss records or repeatedly emit records. The source offset must therefore include:

```json
{
  "table": "incident",
  "timestamp_field": "sys_updated_on",
  "timestamp": "2026-09-29 07:30:42",
  "sys_id": "8f4b...",
  "query_fingerprint": "sha256:..."
}
```

Requests must order by timestamp and `sys_id`. The task should hold a safety-lagged watermark, re-read an overlap window, and deduplicate by `(timestamp, sys_id, sys_mod_count)` where the field is available, rather than discard every tuple below the last emitted tuple. A tuple high-water mark can close a stable timestamp bucket only after the safety interval. Advance committed offsets through Kafka Connect's source-offset mechanism; simply returning a record from `poll()` is not a durability guarantee. Even this cannot recover intermediate updates to a row that changes multiple times between polls, or changes overwritten in the same second with no distinguishable version. That requires the optional event bridge.

This is more robust than simple `sysparm_offset` paging. Offset paging can shift when records are inserted or updated during a backfill. Keyset-like progression and overlap improve restart behavior, but should not be marketed as full CDC.

## ServiceNow API fit

The Table API supplies every baseline operation:

- `GET /api/now/table/{tableName}` for list and polling.
- `GET /api/now/table/{tableName}/{sys_id}` for lookup.
- `POST /api/now/table/{tableName}` for insert.
- `PATCH` or `PUT /api/now/table/{tableName}/{sys_id}` for update.
- `DELETE /api/now/table/{tableName}/{sys_id}` for delete.

The list endpoint supports encoded queries, ordering, `sysparm_limit`, `sysparm_offset`, field projection, display-value selection, domain behavior, and optional pagination headers ([ServiceNow Table API](https://www.servicenow.com/docs/r/zurich/api-reference/rest-apis/c_TableAPI.html)).

ServiceNow permits administrators to create per-hour inbound REST rate-limit rules for a resource, user, role, or all users. Limits are instance configuration, not one universal quota, so the connector must react to headers and 429 responses rather than hard-code a number ([ServiceNow rate limiting](https://www.servicenow.com/docs/r/api-reference/rest-api-explorer/inbound-REST-API-rate-limiting.html)).

## OSS landscape

| Project | Relevance | Gap |
|---|---|---|
| IBM `kafka-connect-servicenow` | Apache-2.0 Java source; arbitrary tables; timestamp plus identifier model | Archived in 2024, no releases, source only |
| Apache Camel ServiceNow | Maintained JVM REST producer supporting Table API CRUD | Not a Kafka Connect source; producer-only |
| Airbyte ServiceNow | Batch/ELT source and useful API behavior reference | Not a Kafka Connect plugin |
| Confluent ServiceNow | Full parity target | Proprietary subscription |

Apache Camel exposes ServiceNow Table API create, retrieve, modify, update, and delete operations, but its ServiceNow component is producer-only and is not a Kafka Connect source ([Apache Camel ServiceNow](https://camel.apache.org/components/4.22.x/servicenow-component.html)). This leaves room for a maintained, standalone Kafka Connect suite.

## Risks and mitigations

| Risk | Effect | Mitigation |
|---|---|---|
| One-second timestamp precision | Misses or duplicates | Lagged watermark, tuple cursor, overlap, version-aware dedup; document unobservable intermediate mutations |
| Concurrent mutation during paging | Offset pages shift | Keyset progression; avoid offset pagination for steady state |
| ACL and domain filtering | Silent partial datasets | Startup access probe, diagnostic metrics, explicit domain option |
| Dynamic response fields | Schema churn | Field allowlist; schemaless default; optional metadata snapshots |
| Display values | Locale/time-zone instability and extra cost | Default to actual values; document `all` carefully |
| Deletes absent from polling | No delete events | Clearly document; optional event bridge |
| User custom query excludes cursor rows | Data loss | Parse/validate known hazards; combine with cursor using parentheses or separate clauses; fingerprint query |
| 429/custom rate rules | Task failure | Header-aware throttle and jitter; expose remaining/reset metrics |
| Sink retries after ambiguous timeout | Duplicate inserts | Idempotency mode, correlation field, optional preflight/upsert pattern |
| Nested sink objects | ServiceNow stringifies maps | Reject, flatten, or explicitly stringify with policy |

## Delivery phases

1. **Foundation:** `snow-core`, auth, HTTP, Table API CRUD/list, tuple cursor, test instance harness.
2. **Source MVP:** one table, historical plus polling, schemaless JSON, restart tests.
3. **Source V2 parity:** multiple tables/tasks, projection, views, OAuth, custom query and typed schemas.
4. **Sink:** create/update/delete, reporters, DLQ, retry/idempotency policies.
5. **Operational hardening:** proxy/mTLS, adaptive throttling, JMX/OpenTelemetry, compatibility tests.
6. **Differentiation:** optional event bridge for deletes and sub-poll-interval latency.

## Verdict

Build it. The technical surface is much smaller than Salesforce: one primary REST API, no proprietary wire protocol, and an abandoned Apache-2.0 implementation that confirms demand and feasibility. The product opportunity is a maintained, well-tested connector with correct tuple cursoring, OAuth, multi-table support, and operational transparency.
