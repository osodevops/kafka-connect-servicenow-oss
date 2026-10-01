# Confluent to OSS migration tooling

Two dependency-free Python 3.9+ scripts supporting the
[Migrating from Confluent guide](../../website/docs/migration/confluent.md), plus a
self-test that CI runs on every build.

| Script | Purpose |
|---|---|
| `confluent2oss.py` | Translates a Confluent ServiceNow connector config (Platform source/sink, Cloud source legacy, Cloud source V2, Cloud sink) into the `sh.oso` connector config, with a per-property migration report |
| `verify_cutover.py` | After cutover, scans the ServiceNow table (live, or from a saved Table API export) for every row updated in the cutover window and proves each one reached the Kafka topic; writes a SHA-256 backed evidence report |
| `selftest.py` | Runs both scripts against the fixtures in `examples/` and a localhost fake Table API; no network, no dependencies (`python3 tools/confluent-migration/selftest.py`) |

Requirements: Python 3.9 or newer, nothing else. `verify_cutover.py` needs read access
to the table through the REST Table API (the connector's own integration user is ideal),
or a saved export of the window passed with `--instance-export`.

## `confluent2oss.py`: config translator

```bash
# from the Connect REST API of the old cluster
curl -s http://connect:8083/connectors/servicenow-incidents \
  | tools/confluent-migration/confluent2oss.py - -o servicenow-incidents-oss.json

# from a file, fixing the cutover start point at the same time
tools/confluent-migration/confluent2oss.py old.json --start-timestamp "2026-09-29 08:55:00" -o new.json

# standalone worker (.properties) instead of the REST envelope
tools/confluent-migration/confluent2oss.py old.json --format properties -o new.properties

# sink: recommended OSS defaults (PATCH + sys_id) instead of exact Confluent parity (PUT + sysId)
tools/confluent-migration/confluent2oss.py old-sink.json --recommended-sink-defaults
```

Accepts the `{"name","config"}` envelope returned by `GET /connectors/{name}` or a bare
config map, from a file or `-` (stdin). Confluent Cloud exports (`confluent connect
cluster describe --output json`) work as long as you pass the `config` object.

What it does:

- maps the connector class and every documented Confluent property to its `snow.*` key
- converts units and formats (`poll.interval.s` to milliseconds, `servicenow.since`
  `YYYY-MM-DD` to `YYYY-MM-DD 00:00:00`, `auth.type=OAUTH2` to `snow.auth.type=oauth2`)
- expands Cloud V2 `table{i}.*` into `snow.tables=t1,...,tn` plus `snow.table.t{i}.*`
- resolves `${connector}` in reporter and DLQ topic names to the original connector
  name so existing topics keep working
- drops Confluent licence, Cloud platform and Schema Registry housekeeping keys
- flags anything that needs a human decision as `MANUAL`, and prints `REVIEW` hints
- passes standard Connect keys through unchanged (`tasks.max`, `topics`, converters,
  `errors.*`, `transforms*`, `predicates*`, `producer/consumer.override.*`,
  `topic.creation.*`, `config.action.reload`)

Exit codes: `0` fully translated (the JSON can be POSTed as is); `2` manual follow-ups
exist, printed on stderr and listed under a top-level `_migration_notes` next to
`config` (strip it before POSTing: `jq 'del(._migration_notes)'`); `1` usage or
input error. `--quiet` silences the report, the exit code still tells you.

The connector name gets a `-oss` suffix by default (`--name-suffix ''` or `--name X`
to change it) so the two connectors can coexist in one cluster during the cutover.

### Platform source and Cloud source (legacy)

| Confluent | OSS | Notes |
|---|---|---|
| `io.confluent.connect.servicenow.ServiceNowSourceConnector`, `ServiceNowSource` | `sh.oso.servicenow.source.ServiceNowSourceConnector` | |
| `servicenow.url` | `snow.url` | trailing `/` removed |
| `servicenow.username`, `servicenow.user` | `snow.auth.username` + `snow.auth.type=basic` | |
| `servicenow.password` | `snow.auth.password` | |
| `servicenow.table` | `snow.tables=t1`, `snow.table.t1.name` | alias `t1`; rename it if you like |
| `kafka.topic` | `snow.table.t1.topic` | |
| `servicenow.since` (`YYYY-MM-DD`) | `snow.table.t1.start.timestamp=YYYY-MM-DD 00:00:00` | see the cutover runbook: override with `--start-timestamp` |
| `batch.max.rows` | `snow.table.t1.batch.size` | |
| `poll.interval.s` | `snow.table.t1.poll.interval.ms` | seconds x 1000 |
| `servicenow.view.variable.prefix=<p>` | `snow.table.t1.timestamp.field=<p>sys_updated_on`, `snow.table.t1.sys.id.field=<p>sys_id` | only when non-empty |
| `retry.max.times` | `snow.retry.max.attempts` | |
| `connection.timeout.ms` | `snow.http.connect.timeout.ms` | |
| `read.timeout.ms`, `write.timeout.ms` | `snow.http.request.timeout.ms` | the larger of the two |
| `proxy.url` | `snow.http.proxy.url` | |
| `servicenow.ssl.keystore.path`, `.keystore.password` | `snow.tls.keystore.path`, `snow.tls.keystore.password` | Cloud spelling `servicenow.ssl.keystorefile` also accepted |
| `servicenow.ssl.truststore.path`, `.truststore.password` | `snow.tls.truststore.path`, `snow.tls.truststore.password` | Cloud spelling `servicenow.ssl.truststorefile` also accepted |
| `servicenow.ssl.key.password` | DROPPED | `snow-core` unlocks the private key with `snow.tls.keystore.password`; re-key the keystore if the two passwords differ |
| `output.data.format=JSON` (Cloud) | `key.converter=StringConverter`, `value.converter=JsonConverter`, `value.converter.schemas.enable=false` | `AVRO`, `JSON_SR`, `PROTOBUF` are MANUAL (Schema Registry converter) |

### Cloud source V2

| Confluent | OSS | Notes |
|---|---|---|
| `ServiceNowSourceV2` | `sh.oso.servicenow.source.ServiceNowSourceConnector` | |
| `servicenow.url` | `snow.url` | |
| `auth.type=BASIC` + `connection.user`/`connection.password` | `snow.auth.type=basic`, `snow.auth.username`/`snow.auth.password` | |
| `auth.type=OAUTH2` | `snow.auth.type=oauth2`, `snow.oauth.grant.type=client_credentials` | REVIEW: use `snow.oauth.grant.type=password` plus `snow.auth.username`/`password` if inbound client credentials is disabled on the instance |
| `oauth2.token.url`, `oauth2.client.id`, `oauth2.client.secret` | `snow.oauth.token.url`, `snow.oauth.client.id`, `snow.oauth.client.secret` | |
| `oauth2.client.scope` | `snow.oauth.scope` | dropped when it is Confluent's placeholder `any` |
| `tables.num` + `table{i}.name` / `table{i}.topic` | `snow.tables=t1,...,tn`, `snow.table.t{i}.name` / `.topic` | indexes beyond `tables.num` are dropped |
| `table{i}.batch.size` | `snow.table.t{i}.batch.size` | |
| `table{i}.start.timestamp` | `snow.table.t{i}.start.timestamp` | |
| `table{i}.timestamp.field` | `snow.table.t{i}.timestamp.field` | `sys_updated_on` or `sys_created_on` |
| `table{i}.pagination.query` | `snow.table.t{i}.query` | MANUAL when it contains `${offset}` or `ORDERBY`: the OSS keyset cursor adds its own predicates and ordering |
| `table{i}.allowlisted.fields` | `snow.table.t{i}.fields` | the cursor fields (`sys_id` + timestamp field) are appended if missing |
| `table{i}.request.interval.ms` | `snow.table.t{i}.poll.interval.ms` | |
| `table{i}.display.value` | `snow.table.t{i}.display.value` | |
| `table{i}.exclude.reference.link` | `snow.table.t{i}.exclude.reference.link` | |
| `table{i}.query.category` | `snow.table.t{i}.query.category` | |
| `table{i}.query.domain` | `snow.table.t{i}.query.domain` | `false` needs the `query_no_domain_table_api` role |
| `max.retries` | `snow.retry.max.attempts` | |
| `retry.backoff.ms` | `snow.retry.initial.backoff.ms` | |
| `behavior.on.error=FAIL|IGNORE` | `snow.source.bad.row.behavior=fail|skip` | closest OSS policy; REVIEW |

### Sink (Platform and Cloud)

| Confluent | OSS | Notes |
|---|---|---|
| `io.confluent.connect.servicenow.ServiceNowSinkConnector`, `ServiceNowSink` | `sh.oso.servicenow.sink.ServiceNowSinkConnector` | |
| `servicenow.url`, `servicenow.user`/`.password`, timeouts, proxy, TLS | as for the source | |
| `servicenow.table` | `snow.sink.table` + `snow.sink.routing.mode=fixed` | |
| (implicit Confluent behaviour) | `snow.sink.operation.mode=key_value` | null value = delete, no id = create, key + value = update |
| (implicit: PUT update, key field `sysId`) | `snow.sink.update.method=PUT`, `snow.sink.sys.id.key.field=sysId` | exact parity; `--recommended-sink-defaults` emits `PATCH` and `sys_id` instead |
| `reporter.result.topic.name` | `snow.sink.reporter.success.topic` | `${connector}` resolved to the old connector name |
| `reporter.error.topic.name` | `snow.sink.reporter.error.topic` | |
| `reporter.bootstrap.servers` | `snow.sink.reporter.bootstrap.servers` | required whenever a reporter topic is set (MANUAL on Cloud exports, which never had it) |
| `reporter.producer.*` | `snow.sink.reporter.producer.*` | security settings for the reporter producer |
| `behavior.on.error` | `behavior.on.api.errors=fail|log|ignore` | |
| `retry.max.times` | `snow.retry.max.attempts` | |
| `max.poll.interval.ms`, `max.poll.records` (Cloud) | `consumer.override.max.poll.interval.ms`, `consumer.override.max.poll.records` | needs `connector.client.config.override.policy=All` on the worker |
| `errors.tolerance`, `errors.deadletterqueue.*` | unchanged | `${connector}` resolved |

### Dropped (no OSS equivalent, safe to lose)

`confluent.license`, `confluent.topic.*`; `retry.backoff.policy`, `retry.on.status.codes`
(OSS uses full-jitter exponential backoff, honours `Retry-After` and classifies statuses
itself); `servicenow.ssl.enabled`, `servicenow.ssl.protocol` (TLS follows `https://`
and the JVM default); `oauth2.token.property=access_token`, `oauth2.client.auth.mode`;
`table{i}.count.records`, `table{i}.suppress.pagination.header`,
`table{i}.request.parameters.separator`; `reporter.*.topic.replication.factor`,
`reporter.*.topic.partitions`, `reporter.*.format*`, `reporter.admin.*` (OSS does not
create reporter topics and always writes JSON: create the topics beforehand); Cloud
platform keys `kafka.auth.mode`, `kafka.api.key`, `kafka.api.secret`,
`kafka.service.account.id`, `schema.context.name`, `sr.service.account.id`,
`key.subject.name.strategy`, `value.subject.name.strategy`,
`auto.restart.on.user.error`, `csfle.enabled=false`, `csfle.onFailure`,
`auto.register.schemas`, `use.latest.version`; the source-side
`reporter.error.topic.name` (use the Connect DLQ).

### MANUAL (a decision is needed; exit 2)

- `table{i}.pagination.query` containing `${offset}` or `ORDERBY`: rewrite as a plain base filter
- `table{i}.pagination.query.field`: custom cursor fields do not exist; use `timestamp.field`
- `output.data.format` / `input.data.format` other than `JSON`: pick converters for your Schema Registry
- `oauth2.token.property` other than `access_token`, `oauth2.client.headers`, `csfle.enabled=true`
- reporter topics without `reporter.bootstrap.servers`
- any unrecognised property (in particular unknown `servicenow.*`, `table*`, `reporter.*`, `oauth2.*` keys)

## Cutover runbook (summary)

The OSS source starts from a timestamp, not from Confluent's offset, so the cutover is a
bounded re-read with duplicates but no loss. The full procedure with screenshots and the
per-table checklist is in the migration guide; the essentials:

1. Translate the config and fix the manual items. Keep the Confluent connector running.
2. Pause the Confluent connector (`PUT /connectors/{name}/pause`) and note the time of its
   last successful poll, T (its task log, or the newest `sys_updated_on` on the topic).
3. Start the OSS connector with `snow.table.<alias>.start.timestamp = T minus a margin`
   (`--start-timestamp "YYYY-MM-DD HH:MM:SS"`, UTC). Five minutes is a comfortable margin;
   the connector's `overlap.seconds` and `sys_mod_count` dedup make the re-read cheap.
4. Delete the Confluent connector once the OSS one is past T.
5. Verify with `verify_cutover.py --since "T minus margin"` and file the evidence report.

The same procedure runs in CI on every build (`ConfluentCutoverMigrationTest`), which
uploads `migration-evidence.json` as the `migration-evidence` artifact.

## `verify_cutover.py`: post-cutover evidence

```bash
export SNOW_USERNAME=kafka_integration SNOW_PASSWORD='...'   # or --bearer-token / SNOW_TOKEN
tools/confluent-migration/verify_cutover.py --table incident \
  --instance-url https://acme.service-now.com \
  --since "2026-09-29 08:55:00" --until "2026-09-29 09:30:00" \
  --query "active=true" \
  --topic-dump dump.tsv --evidence evidence-incident.json
```

It pages the Table API (`sysparm_query=<base>^sys_updated_on>=SINCE^sys_updated_on<=UNTIL^ORDERBYsys_updated_on^ORDERBYsys_id`,
`sysparm_fields=sys_id,sys_updated_on,sys_mod_count`, `sysparm_limit=1000`,
`sysparm_no_count=true`, offset paging, which is fine for a read-only scan bounded by
`--until`) and compares against the dump:

- **missing**: rows in ServiceNow that never reached the topic (loss)
- **staleVersion**: rows on the topic only in an older version than ServiceNow holds (loss)
- **extra**: dump rows not in the scan (normally records from before `--since`, or rows since deleted)
- **duplicates**: at-least-once redeliveries (expected across a cutover; consumers dedupe on `sys_id`, `sys_updated_on`, `sys_mod_count`)

Exit `0` = PASS (zero loss), `1` = loss found, `2` = usage error, `3` = ServiceNow
request failure (after bounded retries on 429/5xx honouring `Retry-After`) or an
unreadable export. The evidence JSON (`schemaVersion` 1) contains the parameters, counts,
the lists above and the SHA-256 of the sorted `sys_id` set on each side; credentials never
appear in it or on the console. Pass `--timestamp-field sys_created_on` if the table polls
by creation time, and take the dump after `--until` so both sides see the same window. The
integration user must be in the UTC timezone, exactly as the connector requires.

### Offline: `--instance-export`

When the person running the check has no instance access, or the evidence has to be
reproducible from files alone, export the window from ServiceNow once and compare against
the file instead of the live table:

```bash
# anyone with read access saves the window (same base query, fields and bounds)
curl -s -u "$SNOW_USERNAME:$SNOW_PASSWORD" -H 'Accept: application/json' \
  "https://acme.service-now.com/api/now/table/incident?sysparm_query=active=true^sys_updated_on>=2026-09-29%2008:55:00^sys_updated_on<=2026-09-29%2009:30:00&sysparm_fields=sys_id,sys_updated_on,sys_mod_count&sysparm_limit=100000&sysparm_no_count=true" \
  > window.json

# no --instance-url, no credentials
tools/confluent-migration/verify_cutover.py --table incident \
  --since "2026-09-29 08:55:00" --until "2026-09-29 09:30:00" \
  --instance-export window.json --topic-dump dump.tsv --evidence evidence-incident.json
```

The export is the Table API response as saved (`{"result": [...]}`) or a bare JSON array
of rows; display-value exports (`{"value": ..., "display_value": ...}` per field) are
unwrapped. Rows without a 32-character `sys_id` are skipped and counted as
`instance.unparsableRows`. The evidence records `"source": "export"` and the file name
instead of an instance URL, so a reviewer can tell the two modes apart. A single large
`sysparm_limit` is fine for a bounded window; page it yourself and concatenate the
`result` arrays if the instance caps the page size.

### Producing the topic dump

The dump is a TSV with one line per Kafka record: `sys_id<TAB>sys_updated_on[<TAB>sys_mod_count]`.
With the default `JsonConverter` (`schemas.enable=false`) or the schema envelope
(`schemas.enable=true`, hence `.payload // .`):

```bash
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic servicenow.incident \
  --from-beginning --timeout-ms 30000 \
| jq -r '(.payload // .) | [.sys_id, .sys_updated_on, .sys_mod_count] | @tsv' > dump.tsv
```

With `snow.table.<alias>.display.value=all` each field is an object, so select `.value`:
`[.sys_id.value, .sys_updated_on.value, .sys_mod_count.value]`. For Avro or Protobuf topics
use `kafka-avro-console-consumer` / `kafka-protobuf-console-consumer` in front of the same
`jq` filter. Blank lines, `#` comments and lines whose first column is not a 32-character
`sys_id` are skipped and counted as `unparsableLines` in the report.

## Fixtures and self-test

`examples/` holds realistic Confluent configs with placeholder secrets: `platform-source.json`,
`cloud-source-legacy.json`, `cloud-source-v2.json` (has a `${offset}` pagination query, so it
exits 2), `platform-sink.json`, `cloud-sink.json`. `selftest.py` translates all of them and
asserts the mappings, the exit codes, that no Confluent key survives, that `compare()`
classifies loss/stale/extra/duplicates correctly, that `verify_cutover.py` pages and
retries against a fake Table API on localhost, and that `--instance-export` reaches the
same verdicts from a saved response. CI runs it from the repository root.
