# PRD-03: Optional ServiceNow Event Bridge

## Objective

Capture deletes and reduce change latency below polling intervals by installing an optional, separately governed ServiceNow application that emits normalized change events to an external gateway.

## Rationale

The Table API source can only query rows that still exist. Confluent Source V2 likewise emits creations and updates, not deletes ([Confluent Source V2](https://docs.confluent.io/cloud/current/connectors/cc-servicenow-source-v2.html)). Customers needing delete propagation require instance-side instrumentation.

## Architecture

```text
ServiceNow Business Rule
  -> durable outbox table
  -> scheduled/async dispatcher
  -> HTTPS event gateway
  -> Kafka producer or compacted ingress topic
  -> optional Connect normalization task
```

Do not make synchronous external calls inside the database transaction. A Business Rule writes a compact event to an application-scoped outbox; an asynchronous worker delivers and retries it. ServiceNow supports insert/update/delete Business Rules and outbound REST scripting, but warns that async Business Rules can run out of order ([Business Rules](https://www.servicenow.com/docs/r/build-workflows/business-rules-classic/c_BusinessRules.html), [outbound REST](https://www.servicenow.com/docs/r/api-reference/web-services/c_ScriptingOutboundREST.html)).

## Event contract

```json
{
  "event_id": "uuid",
  "table": "incident",
  "sys_id": "...",
  "operation": "INSERT|UPDATE|DELETE",
  "sys_updated_on": "2026-09-29 07:30:42",
  "changed_fields": ["state"],
  "record": {},
  "emitted_at": "2026-09-29T07:30:42.456Z",
  "sequence": 123
}
```

For DELETE, include only the approved before-image projection. Sensitive fields are denylisted by default.

## Reliability and security

- Durable outbox with status, attempts, next-attempt time, and last error.
- Verify the after-rule/outbox transaction boundary in a real ServiceNow instance before promising atomic capture; compensate with reconciliation if the write cannot be atomic.
- HMAC or OAuth-authenticated HTTPS.
- Event IDs are idempotency keys.
- Exponential jitter, dead-letter state, manual replay.
- Table and field allowlists.
- Application-scoped roles for administration and dispatch.
- Maximum retention and cleanup job.
- No credentials in scripts or records; use ServiceNow credential facilities.

## Reconciliation

The polling source remains authoritative for insert/update state. A periodic reconciliation job compares configured tables with a compacted Kafka state topic and identifies drift. Event bridge events carry a distinct source marker so downstream consumers can deduplicate polling/event overlap by `event_id`, `sys_id`, and update timestamp.

## Packaging

Release separately from the Apache-licensed connector repository artifact:

- ServiceNow application source/update set.
- Installation and rollback guide.
- Required roles and ACL list.
- Gateway deployment reference.
- Threat model.

## Acceptance criteria

- Insert, update, and delete each create exactly one outbox event in normal operation.
- External downtime does not block the originating business transaction.
- Dispatcher restart loses no acknowledged event.
- Duplicate dispatch is tolerated by event ID.
- Unauthorized tables/fields cannot be configured.
- Delete events contain enough identity for Kafka tombstones.
- Reconciliation detects a deliberately dropped event.
- Back-to-back updates preserve sequence or surface an explicit out-of-order condition.
