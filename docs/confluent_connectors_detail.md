# Confluent ServiceNow Connectors: Implementation Detail

## Inventory and lifecycle

Confluent documents five deployment/product variants: self-managed Platform Source and Sink, Cloud Source Legacy, Cloud Source V2, and Cloud Sink. Cloud Source Legacy was deprecated April 6, 2026 and reaches EOL April 6, 2027 ([Confluent Cloud catalog](https://docs.confluent.io/cloud/current/connectors/overview.html)).

## Platform Source

The source moves creations and updates from one ServiceNow table to one Kafka topic. It is at-least-once, supports one task, retries network failures exponentially, supports an HTTPS proxy, and can begin at a historical date ([overview](https://docs.confluent.io/kafka-connectors/servicenow/current/source-connector/overview.html)).

### Core properties

| Property | Default | Meaning |
|---|---:|---|
| `servicenow.url` | required | Instance base URL |
| `servicenow.username` | required | Basic-auth username |
| `servicenow.password` | required | Basic-auth password |
| `servicenow.table` | required | Table or accessible database view |
| `servicenow.since` | start of launch day | UTC date `YYYY-MM-DD` |
| `batch.max.rows` | `10000` | Rows buffered/fetched per batch |
| `poll.interval.s` | `20` | Poll interval; valid 1–60 |
| `servicenow.view.variable.prefix` | empty | Prefix for view `sys_updated_on` and `sys_id` fields |
| `kafka.topic` | required | Destination topic |
| `retry.max.times` | `3` | Connection retries |
| `connection.timeout.ms` | `50000` | HTTP connect timeout |
| `read.timeout.ms` | `20000` | GET timeout |
| `write.timeout.ms` | `20000` | PUT/POST timeout |
| `proxy.url` | empty | HTTPS proxy |

TLS properties include keystore/truststore paths and passwords. The full property inventory is published by Confluent ([source configuration](https://docs.confluent.io/kafka-connectors/servicenow/current/source-connector/connector_config.html)).

### Limitations

- Only one table and one task.
- No source deletes.
- `sys_audit`, `sys_audit_relation`, and `sys_history_line` are unsupported.
- All table fields are interpreted as strings in the Platform connector.
- At-least-once delivery permits duplicates after non-graceful shutdowns or task movement.

## Cloud Source Legacy

The legacy source is the managed form of the one-table polling design. It supports Avro, JSON Schema, Protobuf, and schemaless JSON; manages offsets through Cloud APIs; and dynamically infers schemas from API responses. Different response field sets can therefore create multiple schema versions ([legacy source](https://docs.confluent.io/cloud/current/connectors/cc-servicenow-source.html)).

Its offset partition contains the table name. Its offset records a time and may include pagination URL/offset plus a cached schema. It polls by `sys_updated_on`, uses `sysparm_offset` while paging, and cannot observe deletion.

## Cloud Source V2

Source V2 is the main compatibility target. It supports up to five tables and multiple tasks, Basic or OAuth2 client-credentials authentication, custom offsets, per-table queries, projections, display values, domain filtering, and actual Kafka schema formats ([Source V2](https://docs.confluent.io/cloud/current/connectors/cc-servicenow-source-v2.html)).

### Per-table properties

| Property | Default | Constraint/meaning |
|---|---:|---|
| `tables.num` | `1` | 1–5 |
| `table{i}.name` | required | Table or Table API-accessible view |
| `table{i}.topic` | required | Destination topic |
| `table{i}.batch.size` | `5000` | 1–50000 |
| `table{i}.start.timestamp` | `1970-01-01 00:00:00` | UTC, second precision |
| `table{i}.timestamp.field` | `sys_updated_on` | May use `sys_created_on` |
| `table{i}.pagination.query` | empty | Encoded query, `${offset}` template |
| `table{i}.pagination.query.field` | empty | Custom cursor field |
| `table{i}.request.parameters.separator` | `&` | Must not conflict with query |
| `table{i}.display.value` | `false` | Table API display-value behavior |
| `table{i}.allowlisted.fields` | empty | Field projection; must include timestamp and `sys_id` |
| `table{i}.request.interval.ms` | `2000` | Minimum 2000 |
| `table{i}.exclude.reference.link` | `false` | Exclude reference URLs |
| `table{i}.query.category` | empty | ServiceNow query category |
| `table{i}.count.records` | `false` | Run count query |
| `table{i}.query.domain` | `true` | Restrict to accessible domains |
| `table{i}.suppress.pagination.header` | `true` | Suppress Link header |

Source V2 warns that ServiceNow timestamp precision is one second and polling below two seconds may create loss or duplicates. It does not emit deletions or tombstones. A field allowlist must include both the cursor timestamp and `sys_id`. The same documentation contains an inconsistent batch-size default: its feature prose says 20,000 while its property table says 5,000; treat the property table as authoritative only after a live configuration check.

### Authentication and retries

`auth.type` is `BASIC` by default and also accepts `OAUTH2`. OAuth configuration includes token URL, client ID, client secret, scope, token property, client authentication mode, and optional headers. Retry controls include `max.retries`, `retry.backoff.policy`, `retry.backoff.ms`, and `retry.on.status.codes`.

## Platform and Cloud Sink

The sink calls the Table API and dynamically chooses an operation:

| Kafka record | Operation |
|---|---|
| Null key, or structured key without `sysId` | `POST` create |
| Null value | `DELETE` |
| Valid key and value | `PUT` update |

Platform does not use PATCH even though ServiceNow documents both PUT and PATCH. The sink requires a value schema; schemaless JSON is unsupported. It supports multiple tasks, proxy/mTLS, at-least-once delivery, result/error reporting, and a DLQ ([Platform sink](https://docs.confluent.io/kafka-connectors/servicenow/current/sink-connector/overview.html), [sink configuration](https://docs.confluent.io/kafka-connectors/servicenow/current/sink-connector/connector_config.html)).

On success, the reporter key is the created or supplied `sysId`; the value contains `requestMethod`, `statusCode`, and `responseString`. Platform defaults reporter topics to `${connector}-success` and `${connector}-error`.

Cloud Sink accepts Avro, JSON Schema, and Protobuf. It creates success, error, and DLQ topics. ServiceNow custom fields must retain their `u_` names or be renamed with an SMT. Nested objects may be converted by the Table API to map-like strings, so Confluent recommends flattening or using a Scripted REST API ([Cloud sink](https://docs.confluent.io/cloud/current/connectors/cc-servicenow-sink.html)).

## Parity implications

- Implement V2 behavior first; provide mappings for legacy names.
- Permit more than five tables in self-managed OSS, with five only as a compatibility-tested profile.
- Improve the offset to `(timestamp, sys_id)` and expose it explicitly.
- Support schemaful and schemaless input/output.
- Support PATCH as a safer default for partial updates while retaining PUT compatibility.
- Keep source deletes out of baseline parity and offer an optional event bridge.
