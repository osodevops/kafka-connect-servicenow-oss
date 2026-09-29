# Open-Source ServiceNow <-> Kafka Connectors: Research and PRD Package

Clean-room research and product requirements for building an open-source alternative to
Confluent's proprietary ServiceNow Kafka connectors. The package follows the same
architecture and evidence standard as the Salesforce connector programme
(`kafka-connect-salesforce-oss`).

## Read in this order

1. **`00_feasibility_and_cleanroom_report.md`**: start here. Feasibility verdict, IP and
   clean-room strategy, tech-stack recommendation, target architecture, feature-parity
   matrix, risks and phased programme of work.

## PRDs (the programme of work)

2. **`01_prd_snow_core.md`** (PRD-00): `snow-core` shared library. Auth, HTTP, Table API
   client, cursor engine, schema modes, throttle and retry. Everything depends on this.
3. **`02_prd_source_connector.md`** (PRD-01): the Source connector. Historical backfill plus
   continuous keyset polling, multi-table, deterministic restarts, at-least-once.
4. **`03_prd_sink_connector.md`** (PRD-02): the Sink connector. Table API create, patch, put
   and delete with bounded concurrency, reporters and ambiguous-write policies.
5. **`04_prd_event_bridge.md`** (PRD-03): the optional ServiceNow-side event bridge for
   deletes and low latency. Out of scope for the 0.x releases; see
   `../servicenow-event-bridge/README.md`.

## Supporting research

- **`confluent_connectors_detail.md`**: externally observable behaviour of Confluent's
  ServiceNow connectors (Platform source and sink, Cloud source legacy and V2, Cloud sink)
  from their public documentation, used only to specify compatibility and the migration
  mapping.
- **`servicenow_apis_and_oss_architecture.md`**: the Table API, encoded queries, auth,
  ACLs and domains, rate limits and how the OSS connectors are built on them.
- **`testing_strategy.md`**: the test tiers and the fake ServiceNow harness.
- **`adr/`**: architecture decision records kept as evidence of independent design.

## Decisions taken during implementation (deviations from the PRD text)

- Build and language follow the Salesforce template: Maven multi-module, Java 17, Maven
  group `sh.oso`, not Gradle or Kotlin.
- `snow.auth.type=basic|oauth2` with `snow.oauth.grant.type=client_credentials|password`.
  The password grant is included because inbound client credentials is disabled by default
  on ServiceNow instances.
- The source marks records with the header `snow.source.operation=UPSERT`, not
  `snow.operation`, so a replayed source topic can never be read as a sink command.
- Where the research documents disagree, the PRD tables win:
  `snow.source.safety.lag.seconds`, `snow.sink.operation.mode=key_value|header|fixed`, and
  the ambiguous-create values `retry|fail_ambiguous|correlation_lookup`.
- `snow.table.<alias>.query.domain` (default `true`) maps to the documented parameter
  `sysparm_query_no_domain`, which is sent only when the flag is `false`.
- Added keys not named in the PRDs: `snow.table.<alias>.sys.id.field` (views),
  `snow.source.bad.row.behavior`, `snow.sink.not.found.behavior`,
  `snow.sink.unknown.field.behavior=passthrough`, `snow.sink.operation.header.required`,
  `snow.sink.reporter.bootstrap.servers`.
- The typed schema mode ships with an explicit field mapping only; the dictionary snapshot
  is deferred. There is no cap on the number of tables (five is the CI-tested profile).
- Exactly-once (KIP-618) is declared unsupported; delivery is at-least-once with a
  documented duplicate bound.

## Headline recommendation

Build a single JVM monorepo with independently packaged source and sink plugins plus a
shared library, using only ServiceNow's public Table API and no ServiceNow-side install.
The core engineering problem is correct incremental polling under one-second timestamp
precision, which the keyset cursor engine in `snow-core` solves.
