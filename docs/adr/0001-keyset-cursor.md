# ADR 0001: Keyset cursor over (sys_updated_on, sys_id) instead of offset pagination

Status: accepted (2026-09-29)

## Context

`sys_updated_on` has one-second precision and rows are updated while a scan runs, so
`sysparm_offset` paging can skip or repeat rows, and a timestamp-only checkpoint loses rows
that share a second with the checkpoint. More than 10,000 rows can share one second after a
bulk update. ServiceNow documents `sysparm_query`, `ORDERBY` and `sysparm_limit`; it does not
document a snapshot-consistent pagination contract.

## Decision

The source keeps a `(timestamp, sys_id)` tuple cursor and issues two query shapes:

- drain the current second: `<base>^<ts>=T^sys_id>S^ORDERBYsys_id`
- move to later seconds: `<base>^<ts>>T^<ts><=HI^ORDERBY<ts>^ORDERBYsys_id`

`HI` is the server time minus a safety lag, fixed for a whole sweep so rows written during
the sweep are picked up by the next one. Every restart re-reads an overlap window before the
committed cursor and suppresses already-emitted `(timestamp, sys_id, sys_mod_count)` versions
from a bounded in-memory cache. Offsets carry `{version, timestamp, sys_id, phase,
fingerprint}`; the partition carries the instance host, table, query fingerprint and
timestamp field so an incompatible offset is refused rather than silently reused.

## Consequences

- No `sysparm_offset`, so no skipped rows when earlier rows change mid-scan.
- Delivery is at-least-once with duplicates bounded by the overlap window; consumers can
  deduplicate on `(sys_id, sys_updated_on, sys_mod_count)`.
- Encoded-query timestamps follow the session user's timezone, so the integration user must
  be set to UTC; the connector warns at startup if it is not.
- Intermediate states overwritten between polls are not observable, and deletes are not
  captured; both are documented and the latter is left to the optional event bridge.
