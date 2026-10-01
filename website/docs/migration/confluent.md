---
title: "Migrating from Confluent"
description: "Config translation, cutover runbook and key mapping."
sidebar_position: 10
keywords:
  - confluent servicenow connector
  - confluent alternatives
  - open source alternative to confluent servicenow connector
  - migrate from confluent
  - servicenow kafka connector
---

# Migrating from Confluent

This suite is the **open-source alternative to Confluent's ServiceNow connectors**: two
Apache-2.0 connectors with functional parity to Confluent's five (Platform Source and
Sink, Cloud Source Legacy, Cloud Source V2 and Cloud Sink), built clean-room on
ServiceNow's public Table API. This guide takes you from a running Confluent connector
to the OSS equivalent with **a configuration translation script, a zero-loss cutover
procedure, and machine-readable evidence that the migration worked**.

## Why teams migrate

- **Licence cost.** Confluent's ServiceNow connectors are separately licensed premium
  connectors (self-managed) or metered per task (Cloud). The OSS suite is Apache-2.0: no
  licence, no per-connector fees, no licensing topic.
- **No lock-in.** Runs on any Kafka Connect: Apache Kafka, Amazon MSK Connect, Strimzi, or
  Confluent Platform itself.
- **Correct under one-second timestamps.** A `(timestamp, sys_id)` keyset cursor instead of
  a timestamp checkpoint with `sysparm_offset` paging, so a burst of updates in one second
  is neither skipped nor re-read on every poll.
- **Not capped at five tables**, and PATCH instead of PUT by default in the sink.
- **Cloud Source Legacy is deprecated** (April 2026) with end of life in April 2027;
  moving to OSS is the same amount of work as moving to Source V2.

## What replaces what

| Confluent connector | Replacement | Notes |
|---|---|---|
| ServiceNow Source (Platform) | [Source](../connectors/source.md) | One table per connector becomes one alias; `servicenow.since` (a date) becomes a full timestamp |
| ServiceNow Source (Cloud, legacy) | [Source](../connectors/source.md) | Same keys as Platform; schema inference replaced by an explicit schema mode |
| ServiceNow Source V2 (Cloud) | [Source](../connectors/source.md) | Same per-table shape; `table{i}.*` becomes `snow.table.t{i}.*` |
| ServiceNow Sink (Platform) | [Sink](../connectors/sink.md) | `PUT` and the `sysId` key field are kept for parity by the translator |
| ServiceNow Sink (Cloud) | [Sink](../connectors/sink.md) | Same; Schema Registry converters stay on the worker |

## Migration at a glance

1. [Inventory](#step-1-inventory) the Confluent connectors, their tables and consumers
2. [Translate](#step-2-translate-with-confluent2osspy) each configuration with the script
3. [Review the MANUAL notes](#step-3-review-the-manual-notes) it prints
4. [Rehearse](#step-4-rehearse) against the fake or a sub-production instance
5. [Cut over](#step-5-cut-over) with an overlap so nothing is lost
6. [Verify with evidence](#step-6-verify-with-verify_cutoverpy)

## Step 1: Inventory

For every Confluent ServiceNow connector, export its configuration and note:

- the tables (one per Platform or legacy connector, up to five per Source V2 connector)
  and their topics;
- the timestamp field (`sys_updated_on` or `sys_created_on`) and any custom
  `pagination.query`;
- the consumers of each topic and whether they deduplicate on `sys_id` (they will see a
  short overlap window twice);
- for sinks, the consumer group `connect-<name>`, the reporter topics and the DLQ.

```bash
for c in $(curl -s http://connect:8083/connectors | jq -r '.[]'); do
  curl -s "http://connect:8083/connectors/$c" | jq -r 'select(.config["connector.class"] | test("servicenow"; "i")) | .name' \
    && curl -s "http://connect:8083/connectors/$c" > "inventory/$c.json"
done
```

Also capture each source connector's **last successful poll time**; you will need it for
the cutover. The Confluent connector's status and log give it, or the newest
`sys_updated_on` on the topic does.

## Step 2: Translate with `confluent2oss.py`

The repository ships a translator that converts any Confluent ServiceNow connector
configuration into the OSS equivalent and reports every property it mapped, dropped, or
needs a decision on:

```bash
# straight from the Connect REST API
curl -s http://connect:8083/connectors/servicenow-incidents \
  | tools/confluent-migration/confluent2oss.py - -o servicenow-incidents-oss.json
```

```text
======================================================================
migration report
======================================================================
connector.class: io.confluent.connect.servicenow.ServiceNowSourceConnector
                 -> sh.oso.servicenow.source.ServiceNowSourceConnector
mapped   servicenow.url -> snow.url
mapped   servicenow.username -> snow.auth.username (snow.auth.type=basic)
mapped   servicenow.password -> snow.auth.password
mapped   servicenow.table -> snow.table.t1.name (snow.tables=t1)
mapped   kafka.topic -> snow.table.t1.topic
value    servicenow.since: 2026-01-01 -> snow.table.t1.start.timestamp=2026-01-01 00:00:00
value    poll.interval.s: 20 -> snow.table.t1.poll.interval.ms=20000
mapped   batch.max.rows -> snow.table.t1.batch.size
mapped   retry.max.times -> snow.retry.max.attempts
dropped  confluent.license (no licence required (Apache-2.0))
dropped  confluent.topic.bootstrap.servers (no licensing topic)
MANUAL   table1.pagination.query contains ${offset}: rewrite as a plain encoded query
         in snow.table.t1.query; the connector supplies the cursor itself
```

It exits with status 2 when anything is marked `MANUAL`, so it can gate a pipeline.
`python3 tools/confluent-migration/selftest.py` runs its fixtures; CI runs it on every
build.

## Step 3: Review the MANUAL notes

Typical items and what to do:

| MANUAL note | Action |
|---|---|
| `pagination.query` contains `${offset}` or `ORDERBY` | Keep only the filter part in `snow.table.<alias>.query`; the OSS connector adds its own cursor predicate and ordering and rejects `ORDERBY` in the base query |
| `pagination.query.field` set to any value | The OSS cursor is always `timestamp.field` plus `sys_id`; if the field was a timestamp, set `snow.table.<alias>.timestamp.field` to `sys_updated_on` or `sys_created_on` |
| `output.data.format` or `input.data.format` other than `JSON` (Cloud) | Choose the worker's converters for your Schema Registry; for the source also pick a schema mode: `strings` gives Confluent Platform's all-string shape, `schemaless` gives plain JSON |
| Reporter topics configured without `reporter.bootstrap.servers` (typical of Cloud exports) | Set `snow.sink.reporter.bootstrap.servers` (tasks cannot see the worker's bootstrap servers) or remove `snow.sink.reporter.*.topic` |
| `behavior.on.error` with an unexpected value | Sink values `fail`, `log` and `ignore` map to `behavior.on.api.errors`; source values `FAIL` and `IGNORE` map to `snow.source.bad.row.behavior`; anything else is set by hand. See [Error handling](../reference/error-handling.md) |
| `oauth2.token.property` other than `access_token`, or `oauth2.client.headers` set | The OSS client reads the standard `access_token` property and sends no custom token-request headers; ServiceNow's `/oauth_token.do` needs neither |
| `csfle.enabled=true` | Client-side field level encryption is not supported; decrypt before the connector or with an SMT |
| No `servicenow.url`, no credentials, or no table with a name and topic | Set `snow.url`, `snow.auth.*` (or `snow.oauth.*`), `snow.tables` and `snow.table.<alias>.name` and `.topic` |
| Non-numeric `poll.interval.s`, `tables.num`, `read.timeout.ms` or `write.timeout.ms`, or a malformed `servicenow.since` or `start.timestamp` | Set the OSS key by hand in the right unit (`ms`, `YYYY-MM-DD HH:MM:SS` UTC) |
| Any property the translator does not recognise | Unknown `servicenow.*`, `table*`, `reporter.*` and `oauth2.*` keys are flagged rather than guessed; check the [configuration reference](../reference/configuration/source.md) |

The report also prints `REVIEW` hints, which do not change the exit code: the cutover
`start.timestamp` to set (see Step 5), the OAuth grant to use when `auth.type=OAUTH2`
(`client_credentials` is assumed; `password` if the instance has inbound client credentials
disabled, see [ServiceNow setup](../getting-started/servicenow-setup.md)), the `PUT` and
`sysId` parity settings on a sink, `consumer.override.*` keys that need
`connector.client.config.override.policy=All` on the worker, TLS store paths to check on
the new worker, cursor fields appended to a `fields` projection, and
`servicenow.view.variable.prefix` translated to a view's `timestamp.field` and
`sys.id.field` (confirm the column names).

## Step 4: Rehearse

Rehearse the translated configuration twice:

1. **Against the fake.** Run the [quickstart](../getting-started/quickstart.md) stack and
   register the translated JSON with `snow.url` pointed at the fake and Basic auth. This
   proves the configuration validates and the topic shape is what consumers expect,
   without touching an instance.
2. **Against a sub-production instance.** Point it at a sandbox or a Personal Developer
   Instance with the real integration user. Confirm three things:
   - **Record shape.** The default `schemaless` mode emits the row as the Table API returns
     it, which matches Confluent's schemaless JSON output. Confluent Platform's all-string
     Struct is `snow.source.schema.mode=strings`. Diff a handful of records; if a consumer
     depends on a Confluent-specific header or field, bridge it with an SMT now.
   - **Converters.** The OSS connectors emit standard Connect records; keep whatever
     key and value converters you use today.
   - **Sink operations.** With the translator's parity settings
     (`snow.sink.update.method=PUT`, `snow.sink.sys.id.key.field=sysId`) the sink applies
     the same operation for the same record as Confluent's. Switch to `PATCH` once the
     migration is done unless you rely on `PUT` clearing unlisted fields.

## Step 5: Cut over

Offsets are **not transferable** between connector implementations: Confluent's source
offsets are opaque to any other connector class. The zero-loss strategy is therefore
*replay with overlap*: the OSS connector starts a little before the point the old
connector reached, so the window is delivered twice and nothing is missed.

```bash
# 1. stop the Confluent source connector
curl -s -X DELETE http://connect:8083/connectors/servicenow-incidents

# 2. note the last successful poll time T (UTC, second precision) from its log or status,
#    or from the newest sys_updated_on on the topic
T='2026-09-29 07:30:42'

# 3. start the OSS connector from T minus an overlap (here 60 seconds)
START=$(date -u -d "$T UTC - 60 seconds" '+%Y-%m-%d %H:%M:%S')
jq --arg s "$START" '.config["snow.table.t1.start.timestamp"] = $s' \
  servicenow-incidents-oss.json > servicenow-incidents-cutover.json
curl -s -X POST -H 'Content-Type: application/json' \
  --data @servicenow-incidents-cutover.json http://connect:8083/connectors
```

The overlap only needs to cover the old connector's poll interval plus clock skew between
the two workers; a minute is generous. From then on the OSS connector checkpoints its own
cursor tuple and the window never matters again. Consumers see the rows updated inside
the window twice; they dedupe on `sys_id` plus `sys_updated_on` and `sys_mod_count`, or
are idempotent.

Variants:

- **Source V2 with several tables.** Set a `start.timestamp` per alias from each table's
  last poll; they are independent.
- **A table that was never fully loaded** (or a new topic): set `start.timestamp` to
  `1970-01-01 00:00:00` (the default) and let the backfill run at full page rate.
- **Sinks.** Sink offsets are plain Kafka consumer offsets and **do** carry over: give the
  OSS sink the same connector name so it inherits the `connect-<name>` group, or reset the
  new group to the old group's committed position with `kafka-consumer-groups.sh
  --reset-offsets`. Keep `snow.sink.update.method=PUT` during the switch for identical
  semantics, and set `snow.sink.create.ambiguous.behavior=correlation_lookup` if the topic
  contains creates that could be redelivered.

## Step 6: Verify with `verify_cutover.py`

Trust, but generate a report. The verifier cross-checks **what ServiceNow says changed**
(a keyset scan of the table since `T`) against **what landed in Kafka**, and writes an
evidence file you can attach to the change ticket:

```bash
# 1. dump the topic since the cutover: one TSV line per record,
#    sys_id <TAB> sys_updated_on <TAB> sys_mod_count
kafka-console-consumer.sh --bootstrap-server broker:9092 \
  --topic servicenow.incident --from-beginning --timeout-ms 30000 \
| jq -r '(.payload // .) | [.sys_id, .sys_updated_on, .sys_mod_count] | @tsv' > dump.tsv

# 2. compare against the instance (read-only Table API scan of the window)
tools/confluent-migration/verify_cutover.py \
  --table incident \
  --instance-url https://acme.service-now.com --username "$SNOW_USERNAME" --password "$SNOW_PASSWORD" \
  --since "$T" --until "$T_END" --query "active=true" \
  --topic-dump dump.tsv --evidence migration-evidence-incident.json
```

```json
{
  "report": "confluent-cutover-verification",
  "schemaVersion": 1,
  "parameters": {"table": "incident", "timestampField": "sys_updated_on",
                 "since": "2026-09-29 07:30:42", "until": "2026-09-29 08:05:00"},
  "instance": {"source": "live", "rows": 1841, "pages": 2, "sysIdSetSha256": "6b0f6f2b..."},
  "topic": {"records": 1907, "uniqueSysIds": 1841, "outOfWindow": 0, "sysIdSetSha256": "6b0f6f2b..."},
  "missingCount": 0,
  "staleVersionCount": 0,
  "extraCount": 0,
  "duplicateRecords": 66,
  "verdict": "PASS: every row updated in the window reached Kafka"
}
```

Use `--query` for the connector's base filter and `--until` for the end of the window
(take the dump after it, so both sides see the same rows). Pass `--bearer-token` or
`SNOW_TOKEN` for OAuth, and `--timestamp-field sys_created_on` for a table polled by
creation time. If the person running the check has no instance access, export the window
from ServiceNow (`GET /api/now/table/incident?sysparm_query=...&sysparm_fields=sys_id,sys_updated_on,sys_mod_count`,
saved as JSON) and pass it with `--instance-export window.json` instead of
`--instance-url` and credentials; the evidence then records `"source": "export"`.

The exit code is `0` for PASS, `1` when any row updated in the window never arrived or
arrived only in an older version, `2` for a usage error and `3` for a failed scan or an
unreadable export; wire it into the runbook as a gate. Credentials never appear in the
report or on the console.

**The procedure itself is proven in CI.** Every build runs
`ConfluentCutoverMigrationTest`, which executes this exact cutover (old-connector era,
gap, OSS start with `start.timestamp = T minus overlap`) against the fake ServiceNow and
asserts zero loss, bounded duplicates and monotonic cursor continuity, then publishes
`migration-evidence.json` as a CI artefact on
[every CI run](https://github.com/osodevops/kafka-connect-servicenow-oss/actions).

## Rollback

Keep the old connector's JSON. Rolling back is the same procedure in reverse: delete the
OSS connector, re-create the Confluent connector with its `servicenow.since` or
`table{i}.start.timestamp` set to just before the OSS connector's cursor (read it with
`GET /connectors/<name>/offsets`), and accept the same overlap. Consumers already dedupe,
so a rollback is boring, which is the point.

:::warning Licensing during overlap
Confluent's connectors are proprietary. Running them, including during a side-by-side
window, is only permitted under an active licence or subscription. Plan the overlap while
you are still licensed and confirm the terms with your Confluent agreement.
:::

## Behaviour differences to review

| Area | Confluent | OSS |
|---|---|---|
| Sink update operation | `PUT` (full update) for every keyed record with a value | `PATCH` by default (`snow.sink.update.method`); the translator sets `PUT` for parity. `PUT` semantics for fields left out of the body are instance-dependent, which is why `PATCH` is the default |
| Sink key field | Structured key field `sysId` | `sys_id` by default (`snow.sink.sys.id.key.field`); the translator sets `sysId` |
| Sink schemaless input | Not supported; a value schema is required | Supported (`Map` path) |
| Nested sink values | Passed to the Table API, which stores them as map strings | Rejected by default with the field path; `flatten` or `stringify` on request |
| Table count | One (Platform, legacy) or five (Source V2) per connector | Unlimited; five is the CI-tested profile |
| Tasks | One (Platform, legacy); several (V2) | Up to one per table, assigned by rendezvous hashing |
| Paging | Timestamp checkpoint with `sysparm_offset` pages | `(timestamp, sys_id)` keyset; no `sysparm_offset` |
| Minimum poll interval | 2,000 ms (V2), 1 s (Platform) | No hard minimum; below two seconds gains nothing because of the safety lag |
| Source record headers | None | `snow.table`, `snow.instance`, `snow.source.operation=UPSERT`, `snow.extracted_at`, `snow.schema.mode` |
| Source schema | Platform: all strings. Cloud: inferred from responses, so field sets create schema versions | Explicit: `schemaless`, `strings` (all optional strings, only grows) or `typed` with a declared mapping |
| Reporter record shape | Key: `sysId`; value: `requestMethod`, `statusCode`, `responseString` | Key: `sys_id`; value: `operation`, `table`, `sys_id`, `status`, `request_id`, `source{topic,partition,offset}` (+ `classification`, `retries`, `response_excerpt`, `exception` on errors) |
| Reporter topics | Created by the connector, default `${connector}-success` and `${connector}-error` | Named explicitly in `snow.sink.reporter.*.topic`, created by you |
| Retry configuration | `retry.max.times` or `max.retries`, `retry.backoff.policy`, `retry.backoff.ms`, `retry.on.status.codes` | `snow.retry.max.attempts`, `snow.retry.max.elapsed.ms`, `snow.retry.initial.backoff.ms`, `snow.retry.max.backoff.ms`; the retryable set is fixed and `Retry-After` is honoured |
| Ambiguous creates | Retried | `fail_ambiguous` by default; `retry` or `correlation_lookup` on request |
| Exactly-once | Not supported | Declared `UNSUPPORTED` |
| Deletes | Not captured | Not captured; the optional event bridge is the documented path |

## Appendix: property mapping tables

`t1`, `t2`, ... are the aliases the translator generates; rename them freely, the alias is
not part of the offset.

### Platform Source (`io.confluent.connect.servicenow.ServiceNowSourceConnector`)

| Confluent | OSS |
|---|---|
| `connector.class` | `sh.oso.servicenow.source.ServiceNowSourceConnector` |
| `servicenow.url` | `snow.url` |
| `servicenow.username` | `snow.auth.username` (and `snow.auth.type=basic`) |
| `servicenow.password` | `snow.auth.password` |
| `servicenow.table` | `snow.table.t1.name` (and `snow.tables=t1`) |
| `kafka.topic` | `snow.table.t1.topic` |
| `servicenow.since` (`YYYY-MM-DD`) | `snow.table.t1.start.timestamp` (`YYYY-MM-DD 00:00:00`) |
| `batch.max.rows` | `snow.table.t1.batch.size` |
| `poll.interval.s` | `snow.table.t1.poll.interval.ms` (multiplied by 1000) |
| `servicenow.view.variable.prefix` | `snow.table.t1.timestamp.field=<prefix>sys_updated_on` and `snow.table.t1.sys.id.field=<prefix>sys_id` |
| `retry.max.times` | `snow.retry.max.attempts` |
| `connection.timeout.ms` | `snow.http.connect.timeout.ms` |
| `read.timeout.ms`, `write.timeout.ms` | `snow.http.request.timeout.ms` (the larger of the two) |
| `proxy.url` | `snow.http.proxy.url` |
| `servicenow.ssl.keystore.path` (Cloud: `servicenow.ssl.keystore.location` or `servicenow.ssl.keystorefile`), `servicenow.ssl.keystore.password` | `snow.tls.keystore.path`, `snow.tls.keystore.password` (`snow.tls.keystore.type` keeps its default) |
| `servicenow.ssl.truststore.path` (Cloud: `servicenow.ssl.truststore.location` or `servicenow.ssl.truststorefile`), `servicenow.ssl.truststore.password` | `snow.tls.truststore.path`, `snow.tls.truststore.password` (`snow.tls.truststore.type` keeps its default) |
| `servicenow.ssl.key.password` | DROPPED: the private key is unlocked with `snow.tls.keystore.password`; re-key the keystore if the two differ |
| (none) | `snow.source.schema.mode=strings` for the Platform connector's all-string shape |

### Cloud Source V2 (`ServiceNowSourceV2`)

| Confluent | OSS |
|---|---|
| `connector.class` | `sh.oso.servicenow.source.ServiceNowSourceConnector` |
| `servicenow.url` | `snow.url` |
| `auth.type` (`BASIC`, `OAUTH2`) | `snow.auth.type` (`basic`, `oauth2`) |
| `servicenow.username`, `servicenow.password` | `snow.auth.username`, `snow.auth.password` |
| OAuth token URL | `snow.oauth.token.url` |
| OAuth client id, client secret | `snow.oauth.client.id`, `snow.oauth.client.secret` |
| OAuth scope | `snow.oauth.scope` |
| `table{i}.name` | `snow.table.t{i}.name` (and `t{i}` appended to `snow.tables`) |
| `table{i}.topic` | `snow.table.t{i}.topic` |
| `table{i}.start.timestamp` | `snow.table.t{i}.start.timestamp` |
| `table{i}.timestamp.field` | `snow.table.t{i}.timestamp.field` |
| `table{i}.batch.size` | `snow.table.t{i}.batch.size` |
| `table{i}.request.interval.ms` | `snow.table.t{i}.poll.interval.ms` |
| `table{i}.pagination.query` | `snow.table.t{i}.query` (MANUAL when it contains `${offset}` or `ORDERBY`) |
| `table{i}.allowlisted.fields` | `snow.table.t{i}.fields` |
| `table{i}.display.value` | `snow.table.t{i}.display.value` |
| `table{i}.exclude.reference.link` | `snow.table.t{i}.exclude.reference.link` |
| `table{i}.query.category` | `snow.table.t{i}.query.category` |
| `table{i}.query.domain` | `snow.table.t{i}.query.domain` |
| `max.retries` | `snow.retry.max.attempts` |
| `retry.backoff.ms` | `snow.retry.initial.backoff.ms` |

### Platform Sink and Cloud Sink (`io.confluent.connect.servicenow.ServiceNowSinkConnector`)

| Confluent | OSS |
|---|---|
| `connector.class` | `sh.oso.servicenow.sink.ServiceNowSinkConnector` |
| `topics`, `topics.regex` | unchanged |
| `servicenow.url` | `snow.url` |
| `servicenow.username`, `servicenow.password` | `snow.auth.username`, `snow.auth.password` (and `snow.auth.type=basic`) |
| `servicenow.table` | `snow.sink.table` (and `snow.sink.routing.mode=fixed`) |
| (implicit `PUT`) | `snow.sink.update.method=PUT` |
| (implicit `sysId` key field) | `snow.sink.sys.id.key.field=sysId` |
| (implicit key/value inference) | `snow.sink.operation.mode=key_value` |
| `reporter.bootstrap.servers` | `snow.sink.reporter.bootstrap.servers` |
| `reporter.result.topic.name` | `snow.sink.reporter.success.topic` |
| `reporter.error.topic.name` | `snow.sink.reporter.error.topic` |
| `reporter.producer.*` | `snow.sink.reporter.producer.*` |
| `behavior.on.error` | `behavior.on.api.errors` |
| `retry.max.times` | `snow.retry.max.attempts` |
| `connection.timeout.ms`, `read.timeout.ms`, `write.timeout.ms` | `snow.http.connect.timeout.ms`, `snow.http.request.timeout.ms` |
| `proxy.url` | `snow.http.proxy.url` |
| Keystore and truststore properties | `snow.tls.keystore.*`, `snow.tls.truststore.*` |
| `errors.tolerance`, `errors.deadletterqueue.*` | unchanged |

### DROPPED (no OSS equivalent needed)

| Confluent | Reason |
|---|---|
| `confluent.license`, `confluent.topic.*` | No licence, no licensing topic |
| `servicenow.ssl.enabled`, `servicenow.ssl.protocol` | TLS is used whenever `snow.url` is `https://`, with the JVM default protocol |
| `servicenow.ssl.key.password` | The private key is unlocked with `snow.tls.keystore.password`; re-key the keystore if the two differ |
| `oauth2.client.auth.mode`, `oauth2.token.property=access_token`, `oauth2.client.scope=any` | Client credentials are posted form-encoded, the standard `access_token` property is read, and no scope is sent unless `snow.oauth.scope` is set |
| `tables.num` | Implied by `snow.tables`; `table{i}.*` groups with an index beyond it are dropped |
| `table{i}.request.parameters.separator` | Requests are built by the client, not by string concatenation |
| `table{i}.count.records` | `sysparm_no_count=true` is always sent |
| `table{i}.suppress.pagination.header` | Always suppressed |
| `retry.backoff.policy`, `retry.on.status.codes` | The backoff is always exponential with jitter, `Retry-After` is honoured and the retryable set is fixed |
| `reporter.result.topic.replication.factor`, `reporter.result.topic.partitions`, `reporter.error.topic.replication.factor`, `reporter.error.topic.partitions`, `reporter.admin.*`, `reporter.*.topic.key.format`, `reporter.*.topic.value.format` | The sink does not create topics and always writes JSON reporter records |
| Source-side `reporter.error.topic.name` | The source has no reporter; use `errors.tolerance=all` with `errors.deadletterqueue.topic.name` |
| `kafka.auth.mode`, `kafka.api.key`, `kafka.api.secret`, `kafka.service.account.id`, `auto.restart.on.user.error` | Cloud platform settings; worker settings on self-managed Connect |
| `schema.context.name`, `sr.service.account.id`, `key.subject.name.strategy`, `value.subject.name.strategy`, `auto.register.schemas`, `use.latest.version` | Schema Registry settings; set them on the converter if needed |
| `csfle.enabled=false`, `csfle.onFailure` | Client-side field level encryption is not supported (`csfle.enabled=true` is MANUAL) |

### MANUAL (needs a decision)

| Confluent | What to decide |
|---|---|
| `table{i}.pagination.query` with `${offset}` or `ORDERBY` | Reduce to a plain filter |
| `table{i}.pagination.query.field` with any value | Choose `sys_updated_on` or `sys_created_on` for `timestamp.field` |
| `output.data.format`, `input.data.format` other than `JSON` (Cloud) | Choose the worker converters and the source schema mode |
| `oauth2.token.property` other than `access_token`; `oauth2.client.headers` | Not supported; confirm the standard token response is enough |
| `csfle.enabled=true` | Decrypt before the connector |
| Reporter topics without `reporter.bootstrap.servers` | Set `snow.sink.reporter.bootstrap.servers` or remove the reporter topics |
| Unexpected values for `auth.type`, `behavior.on.error`, `poll.interval.s`, `tables.num`, the timeouts, `servicenow.since` or `start.timestamp` | Set the OSS key by hand |
| No `servicenow.url`, credentials, table name or topic | Set `snow.url`, `snow.auth.*`, `snow.tables`, `snow.table.<alias>.name` and `.topic` |
| Any unrecognised property | Check the configuration reference |

`errors.*`, `transforms*`, `predicates*`, converters, `tasks.max`, `topics` and the
`producer.override.*`, `consumer.override.*` and `topic.creation.*` families are standard
Connect keys and pass through unchanged.
