# PRD-02: ServiceNow Sink Connector

## Objective

Write Kafka records to ServiceNow tables using Table API create, partial update, full update, and delete operations with bounded concurrency, operational reporters, and explicit ambiguous-write handling.

## Scope

- One or many input topics.
- Fixed table, topic-to-table map, or record-header table routing.
- POST create, PATCH partial update, PUT update, DELETE.
- Confluent-compatible key/value operation inference.
- Explicit operation header or fixed operation mode.
- Basic and OAuth2 client credentials.
- Schemaful and schemaless records.
- Success/error reporters, native Connect DLQ, retries, proxy and TLS/mTLS.

## Operation resolution

Priority:

1. Header named by `snow.sink.operation.header`.
2. Fixed configured operation.
3. Compatibility inference:
   - null value → DELETE;
   - missing `sys_id` → CREATE;
   - key/value present → PATCH or PUT according to update mode.

The identifier may come from a primitive key, a configured key field, a value field, or header. Reject disagreement between identifiers unless an explicit precedence policy is set.

ServiceNow documents POST, PATCH, PUT, and DELETE on the Table API ([Table API](https://www.servicenow.com/docs/r/zurich/api-reference/rest-apis/c_TableAPI.html)). PATCH is the default update because Kafka payloads often contain partial state.

## Mapping

- Preserve `u_` custom field names.
- Optional rename and allow/deny lists.
- Null behavior: omit, clear, or reject per configuration.
- Unknown fields: fail, drop, or send to error reporter.
- Nested values: `reject` by default; optional `flatten` or `stringify`.
- Reference fields accept actual database IDs by default.
- Reserved metadata fields and operation headers are not sent.

## Routing

Modes:

- `fixed`: `snow.sink.table`.
- `topic_map`: `snow.sink.topic.<topic>.table`.
- `header`: read a trusted header and validate against an allowlist.

Never allow unrestricted record-controlled paths because table names become URL path components and can cross authorization boundaries.

## Idempotency and delivery

At-least-once Kafka consumption plus non-transactional ServiceNow writes means ambiguous outcomes are possible. For create:

- `retry`: retry POST and accept possible duplicates.
- `fail_ambiguous`: report and stop/re-route after a timeout with unknown outcome.
- `correlation_lookup`: require a unique correlation field, look up before retry, then patch if found.

PATCH/PUT/DELETE are naturally more retry-safe when addressed by immutable `sys_id`, but response-specific policies still apply.

Kafka offsets are committed only after success or after a record is durably accepted by the configured error path. `put()` batches by partition but makes bounded concurrent HTTP calls; ordering within a Kafka partition is preserved by default.

## Configuration

| Property | Default |
|---|---:|
| `snow.sink.routing.mode` | `fixed` |
| `snow.sink.table` | required in fixed mode |
| `snow.sink.operation.mode` | `key_value` |
| `snow.sink.update.method` | `PATCH` |
| `snow.sink.sys.id.key.field` | `sys_id` |
| `snow.sink.operation.header` | `snow.operation` |
| `snow.sink.max.in.flight` | `8` |
| `snow.sink.null.behavior` | `omit` |
| `snow.sink.unknown.field.behavior` | `fail` |
| `snow.sink.nested.behavior` | `reject` |
| `snow.sink.create.ambiguous.behavior` | `fail_ambiguous` |
| `snow.sink.correlation.field` | empty |
| `snow.sink.reporter.success.topic` | empty/disabled |
| `snow.sink.reporter.error.topic` | empty/disabled |

## Reporting

Success report:

```json
{
  "operation": "PATCH",
  "table": "incident",
  "sys_id": "...",
  "status": 200,
  "request_id": "...",
  "source": {"topic": "...", "partition": 0, "offset": 42}
}
```

Error reports add classification, retry count, sanitized response excerpt, and exception class. Secrets and sensitive request bodies are excluded by default.

## Error policy

- 400/422: record error, normally non-retryable.
- 401: refresh OAuth once.
- 403: permission/configuration error.
- 404 update/delete: `fail|ignore|create` policy; default fail.
- 409: retry only if the ServiceNow response indicates a transient conflict.
- 429 and transient 5xx: header-aware retry.
- Timeout after request transmission: ambiguous-write policy.

DLQ payload encryption is the user's converter responsibility. Documentation must warn that Connect DLQ records can contain plaintext.

## Acceptance criteria

- CRUD contract tests pass against a ServiceNow developer instance.
- Per-partition order is preserved with multiple tasks.
- Tombstone delete and Confluent-compatible create/update inference pass fixtures.
- PATCH sends only intended fields.
- Timeout-after-POST exercises every ambiguous-write mode.
- 429 throttling cannot exceed configured concurrency.
- Reporter records bind to original Kafka coordinates.
- Schemaless JSON, Connect Struct, Avro-converted, JSON Schema-converted, and Protobuf-converted records pass.
- Nested values never silently become ServiceNow map strings under default policy.
