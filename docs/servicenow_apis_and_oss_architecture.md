# ServiceNow APIs and OSS Connector Architecture

## Table API

The public Table API is sufficient for the baseline source and sink. ServiceNow documents both unversioned and versioned paths ([Table API](https://www.servicenow.com/docs/r/zurich/api-reference/rest-apis/c_TableAPI.html)):

| Method | Path | Use |
|---|---|---|
| GET | `/api/now/table/{tableName}` | Query/list records |
| GET | `/api/now/table/{tableName}/{sys_id}` | Retrieve one |
| POST | `/api/now/table/{tableName}` | Insert one |
| PATCH | `/api/now/table/{tableName}/{sys_id}` | Partial update |
| PUT | `/api/now/table/{tableName}/{sys_id}` | Update |
| DELETE | `/api/now/table/{tableName}/{sys_id}` | Delete |

Important query parameters include:

- `sysparm_query`: encoded filter and ordering.
- `sysparm_limit`: page size, default 10000.
- `sysparm_offset`: index-based pagination, default zero.
- `sysparm_fields`: projection.
- `sysparm_display_value`: `false`, `true`, or `all`.
- `sysparm_exclude_reference_link`: omit reference URLs.
- `sysparm_query_category`: apply a query category.
- `sysparm_no_count`: suppress the count query.
- `sysparm_suppress_pagination_header`: suppress Link.
- Domain-aware query options where enabled.

Encoded queries support comparison predicates, `^` for AND, `^OR`, and `ORDERBY`/`ORDERBYDESC`. Invalid query portions can be ignored by default, which is dangerous: the connector should run a startup probe and encourage the instance property that causes invalid queries to return no rows.

## Incremental source algorithm

### Source partition

The Connect source partition identifies the instance, table, and stable query:

```json
{
  "instance": "acme.service-now.com",
  "table": "incident",
  "query_fingerprint": "sha256:...",
  "timestamp_field": "sys_updated_on"
}
```

Credentials must not influence partition identity. The instance URL must be normalized. Any change to table, query, cursor field, domain behavior, or projection that affects record identity should change or explicitly migrate the partition.

### Offset

```json
{
  "version": 1,
  "timestamp": "2026-09-29 07:30:42",
  "primary_key": "8f4bc...",
  "phase": "stream"
}
```

Never checkpoint only the timestamp. ServiceNow supplies one-second timestamp precision, and Confluent itself warns about duplicates or loss near that boundary ([Source V2](https://docs.confluent.io/cloud/current/connectors/cc-servicenow-source-v2.html)).

### Poll sequence

1. Read the committed tuple.
2. Query from an overlap boundary, normally one to two seconds before the committed timestamp.
3. Order by cursor field and `sys_id`.
4. Decode each row into `(timestamp, sys_id)`.
5. Suppress exact row versions already seen in the bounded cache; retain a lagged watermark and avoid prematurely closing the current timestamp second.
6. Emit records in tuple order.
7. Attach a versioned cursor/checkpoint state to each `SourceRecord` offset and use Connect-managed source offset commits.
8. When the page is full, immediately fetch the next keyset page; otherwise wait for the poll interval.

Use `sysparm_offset` only when an unavoidable custom query cannot support keyset progression. During offset paging, hold the high-water mark fixed and overlap/deduplicate at the end.

### Concurrent changes

A row can be updated again while a page is being read and move later in sort order. Deduplicating only by `sys_id` would suppress legitimate updates, so the bounded cache should use `sys_mod_count` with `(timestamp, sys_id)` where accessible. Overlap and safety lag reduce missed observable states but cannot reconstruct overwritten intermediate states or deleted rows. For busy tables, expose:

- `snow.source.overlap.seconds`
- `snow.source.dedup.window.records`
- `snow.source.lag.seconds`, a deliberate safety delay behind wall clock

The safety delay trades latency for more stable pages; it is not a transaction snapshot or a true CDC guarantee. The offset envelope may need a bounded recent-version set when replaying the overlap after restart; test the serialized size and move dedup to a compacted state topic if it cannot fit safely in Connect offsets.

## Authentication

Baseline modes:

- Basic authentication.
- OAuth 2 client credentials.
- Extensible OAuth token provider for JWT bearer or instance-specific flows.

ServiceNow documents client credentials for automated system integrations and supports OAuth scopes that restrict access to APIs ([OAuth inbound](https://www.servicenow.com/docs/r/platform-security/authentication/oauth-inbound.html)). Tokens should be cached until shortly before expiry and refreshed under a single-flight lock. Never log credentials or access tokens.

## ACLs, domains, and data semantics

Table API results reflect the integration user's ACLs and domain access. A successful response does not prove the result set is complete. Startup checks should verify:

- Read access to the configured table/view.
- Read access to cursor field and `sys_id`.
- Write/delete permission for sink operations.
- Visibility of projected fields.
- Domain filtering configuration.

Default `sysparm_display_value=false`. Display values depend on locale, time zone, references, and encryption context and can require additional work in the instance. `all` should be represented with an explicit, stable structure rather than flattening actual and display values ambiguously.

## Schema strategy

Offer three modes:

1. **Schemaless:** emit a map matching the API response. Best compatibility and least schema churn.
2. **Strings:** stable Connect schema where every selected field is optional string. Matches Platform behavior.
3. **Typed:** use configured mappings and optional metadata from accessible dictionary tables; snapshot the mapping and evolve schemas deliberately.

Do not infer a durable type solely from one row. Sparse fields and display values make response inference unstable. Always emit `sys_id`, cursor field, source table, extraction timestamp, and operation in headers or an optional envelope.

## Sink design

The Table API inserts one record per POST; it is not a bulk endpoint. Parallelize across Kafka partitions while bounding per-instance concurrency.

Operation selection should be explicit first, compatibility-based second:

- `snow.sink.operation.mode=header|key-value|fixed`
- `fixed=create|patch|put|delete`
- In compatibility mode: tombstone value deletes; missing `sys_id` creates; key plus value updates.

PATCH is the recommended update default because it expresses partial mutation. PUT remains available for Confluent compatibility.

Create retries are ambiguous after timeouts. Offer:

- `at_least_once`: retry, accepting possible duplicates.
- `lookup_before_retry`: query a configured correlation field.
- `external_id`: resolve/update by a unique configured field.
- `fail_ambiguous`: send to reporter/DLQ rather than retry POST.

## Rate limiting and reliability

ServiceNow rate-limit rules are administrator-defined per hour and may apply by user, role, or resource. Counts are persisted periodically and reset headers expose the next window; no single global request quota can be assumed ([rate-limit documentation](https://www.servicenow.com/docs/r/api-reference/rest-api-explorer/inbound-REST-API-rate-limiting.html)).

Retry:

- 429 using `Retry-After` or rate-limit reset information.
- 408, 425, 502, 503, and 504 with exponential jitter.
- Other 5xx according to configurable policy.
- 401 once after token refresh.
- Do not retry most 4xx.
- Bound retries by elapsed time as well as attempt count.

Expose request totals, latency histograms, response codes, retries, throttled time, rows emitted/written, cursor lag, duplicates suppressed, and reporter outcomes.

## Delete capture

Polling live tables cannot discover deleted rows. The baseline must say this plainly. The optional event bridge installs a ServiceNow Business Rule or equivalent application artifact that emits an outbound event to an authenticated endpoint or Kafka-facing gateway for insert/update/delete. ServiceNow documents these three Business Rule triggers and outbound REST scripting, but this creates an instance-side deployment dependency ([Business Rules](https://www.servicenow.com/docs/r/build-workflows/business-rules-classic/c_BusinessRules.html), [outbound REST](https://www.servicenow.com/docs/r/api-reference/web-services/c_ScriptingOutboundREST.html)).

## OSS survey

The IBM Java source is the closest prior implementation. It supports arbitrary tables, source tasks/subtasks, a sortable timestamp plus sortable identifier, OAuth, configurable polling, and Apache-2.0 licensing, but is archived ([IBM connector](https://github.com/IBM/kafka-connect-servicenow)).

Apache Camel's ServiceNow component is maintained and implements REST CRUD, but it is producer-only and not a Kafka Connect source ([Camel component](https://camel.apache.org/components/4.22.x/servicenow-component.html)). Airbyte has a ServiceNow source useful for connector testing patterns, but it is not Kafka Connect ([Airbyte ServiceNow](https://docs.airbyte.com/integrations/sources/service-now)).
