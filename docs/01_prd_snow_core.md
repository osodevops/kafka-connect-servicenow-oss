# PRD-00: `snow-core` Shared ServiceNow Client Library

## Objective

Provide one tested, dependency-light library for all ServiceNow protocol behavior so connector modules remain thin Kafka Connect adapters.

## Modules

### Auth

Support Basic and OAuth2 client credentials at launch. Define a `TokenProvider` interface for JWT bearer and custom flows. Cache tokens, refresh before expiry, use single-flight refresh, redact all secrets, and support Kafka Connect `ConfigProvider` references.

### HTTP

Provide pooled HTTP/2-capable connections, proxy support, connect/read/write/request timeouts, truststore and keystore/mTLS, request correlation IDs, gzip, bounded response bodies, and redacted structured logging.

### Table API

Implement:

```text
list(table, query, fields, page)
get(table, sysId)
create(table, payload)
patch(table, sysId, payload)
put(table, sysId, payload)
delete(table, sysId)
```

Paths and semantics must follow ServiceNow's documented Table API ([ServiceNow Table API](https://www.servicenow.com/docs/r/zurich/api-reference/rest-apis/c_TableAPI.html)).

### Cursor engine

Own tuple comparison, timestamp parsing, overlap windows, dedup caching, keyset query construction, high-water marks, query fingerprints, and versioned offset serialization.

### Schema

Implement schemaless, all-string, and typed modes. Preserve null versus absent. Normalize actual/display/reference values according to explicit policy. Never infer a stable schema from a single response.

### Throttle and retry

Classify responses and exceptions, parse throttling headers, implement exponential backoff with full jitter, cap retries by attempts and elapsed time, and expose a per-instance concurrency limiter. ServiceNow rate limits are configured per instance/resource/user, so adaptive behavior is required ([rate limits](https://www.servicenow.com/docs/r/api-reference/rest-api-explorer/inbound-REST-API-rate-limiting.html)).

## Public interfaces

```kotlin
interface ServiceNowClient {
  suspend fun query(request: QueryRequest): Page<Record>
  suspend fun get(table: String, sysId: String): Record?
  suspend fun create(table: String, value: Record): WriteResult
  suspend fun patch(table: String, sysId: String, value: Record): WriteResult
  suspend fun put(table: String, sysId: String, value: Record): WriteResult
  suspend fun delete(table: String, sysId: String): WriteResult
}

data class Cursor(val timestamp: Instant, val sysId: String)
```

## Configuration namespace

- `snow.url`
- `snow.auth.type`
- `snow.auth.username`, `snow.auth.password`
- `snow.oauth.token.url`, `snow.oauth.client.id`, `snow.oauth.client.secret`, `snow.oauth.scope`
- `snow.http.connect.timeout.ms`, `snow.http.request.timeout.ms`
- `snow.http.proxy.url`
- `snow.tls.truststore.*`, `snow.tls.keystore.*`
- `snow.retry.max.attempts`, `snow.retry.max.elapsed.ms`
- `snow.retry.initial.backoff.ms`, `snow.retry.max.backoff.ms`
- `snow.http.max.concurrent.requests`

## Non-functional requirements

- Java 17 baseline.
- No dependency on Confluent proprietary libraries.
- Secrets never appear in `toString`, logs, metrics, exceptions, or reporter payloads.
- Unit tests use deterministic virtual time.
- Contract tests run against a disposable or dedicated ServiceNow developer instance.
- WireMock tests cover pagination, malformed JSON, 401 refresh, 429, timeout-after-write, and partial responses.
- Semantic versioning and binary API compatibility checks.

## Acceptance criteria

- All six Table API operations pass contract tests.
- OAuth refresh remains correct under 50 concurrent requests.
- Retry honors Retry-After/reset information and never retries disallowed 4xx.
- Cursor comparator and offset codec pass property-based tests.
- No credential appears in captured logs or exception snapshots.
- Source and sink plugins share the same client artifact without shaded class conflicts.
