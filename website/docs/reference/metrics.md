---
title: "Metrics"
description: "JMX MBeans registered by the source and sink tasks: names, attributes, units and how to scrape them."
sidebar_position: 6
---

# Metrics

Both connectors register read-only JMX MXBeans on the worker's platform MBean server for
the lifetime of each task. They sit in the `sh.oso.servicenow` domain next to the
framework's own `kafka.connect` beans, so whatever already scrapes the worker (JConsole,
`jmx_exporter`, Jolokia, Datadog's JMX integration) picks them up with one extra pattern.

| MBean | Registered by | One per |
|---|---|---|
| `sh.oso.servicenow:type=source-table,connector=<name>,task=<n>,table=<table>` | `ServiceNowSourceTask` | table assigned to the task |
| `sh.oso.servicenow:type=sink-writer,connector=<name>,task=<n>` | `ServiceNowSinkTask` | sink task |

`connector` is the connector name from the Connect REST API (the `name` key the framework
copies into every task configuration). `task` is the task index, so `connector=orders,task=1`
is the task the REST API reports as `orders-1`. Values containing commas, equals signs,
colons or whitespace are quoted the way `ObjectName.quote` does.

The connectors never fail a task over metrics: if the MBean server refuses a registration
the task logs a warning and runs without it. A task restarted inside the same worker JVM
replaces its previous bean rather than failing with an "already registered" error, and
`stop()` unregisters everything the task registered.

:::note How the task index reaches the task
Kafka Connect does not pass the task index to `Task.start()`. Both connectors therefore
set the internal `snow.task.id` key in the configuration they hand each task from
`taskConfigs()`. Like `snow.task.tables`, it is an internal key: it is left out of the
configuration reference and must not be set by hand. A task started without it (for
example directly from a test harness) gets a per-JVM sequence number instead, so bean
names stay unique.
:::

## Source table MBean

`sh.oso.servicenow:type=source-table,connector=<name>,task=<n>,table=<table>`

Counters are cumulative since the task started; gauges reflect the last poll.

| Attribute | Type | Meaning |
|---|---|---|
| `Table` | string | ServiceNow table or view name. |
| `CursorTimestamp` | string | `sys_updated_on` (or the configured timestamp field) of the last emitted row, `yyyy-MM-dd HH:mm:ss` in instance time. Empty before the first row. |
| `CursorEpochSeconds` | long | The same cursor as epoch seconds; `0` before the first row. |
| `LagSeconds` | long | Instance clock minus the cursor after the last poll, in seconds. The instance clock is inferred from response `Date` headers. |
| `Phase` | string | `backfill` while the initial sweep from `start.timestamp` runs, then `stream`. |
| `RecordsEmitted` | long | Records returned to the framework. |
| `PagesFetched` | long | Table API pages (requests) fetched. |
| `DuplicatesSuppressed` | long | Row versions dropped by the overlap dedup cache. The duplicate bound described in [delivery semantics](../concepts/delivery-semantics.md) applies after a restart, when the cache is empty. |
| `RowsSkipped` | long | Rows skipped under `snow.source.bad.row.behavior=skip`. |
| `Retries` | long | Polls that ended in a retryable failure surfaced to the worker as a `RetriableException`, after snow-core's own bounded retries were exhausted. |
| `ThrottledMillis` | long | Milliseconds this table's requests spent waiting after `429` responses (`Retry-After` or backoff). |
| `LastPollDurationMs` | long | Wall-clock duration of the last poll, in milliseconds. |
| `LastSuccessfulRequestEpochMs` | long | Epoch milliseconds of the last successful Table API response for this table; `0` if there has been none. |
| `SchemaVersion` | int | Current value-schema version in `strings` and `typed` schema modes; `0` in `schemaless` mode. |

What to alert on:

- `LagSeconds` climbing while `PagesFetched` is flat means the table is not being polled;
  check the task status and the worker log.
- `LagSeconds` climbing while `PagesFetched` grows means the connector cannot keep up;
  raise `snow.source.batch.size` or spread tables over more tasks.
- `ThrottledMillis` growing steadily means the instance rate-limit rule is engaged; see
  [rate limits](../concepts/rate-limits.md).

## Sink writer MBean

`sh.oso.servicenow:type=sink-writer,connector=<name>,task=<n>`

| Attribute | Type | Meaning |
|---|---|---|
| `Creates` | long | Records written with `POST` (`CREATE`). |
| `Patches` | long | Records written with `PATCH`, including rows a correlation lookup matched. |
| `Puts` | long | Records written with `PUT`. |
| `Deletes` | long | Records written with `DELETE`. |
| `WrittenByOperation` | tabular | The four counters above keyed by operation name, for tools that prefer one attribute. |
| `Written` | long | Successful writes, all operations. Writes resolved by `snow.sink.not.found.behavior=ignore` count under the operation that was attempted. |
| `Failures` | long | Records that failed permanently and went to the error topic, dead letter queue and `behavior.on.api.errors`. |
| `Ambiguous` | long | Failures classified `AMBIGUOUS` (sent, no response, not resolved by the policy). A subset of `Failures`. |
| `RetriesExhausted` | long | Records whose transient failures outlived the retry budget. The batch is re-delivered, so one record can count more than once. |
| `InFlight` | int | Requests on the wire right now. |
| `MaxInFlightObserved` | int | The highest `InFlight` seen; compare with `snow.sink.max.in.flight`. |
| `Retries` | long | Requests re-sent: snow-core's retries on `429`, `5xx`, `408`, `425` and transport failures, plus ambiguous `POST` and `DELETE` re-sends made by the sink's own policies. |
| `ThrottledMillis` | long | Milliseconds this task's requests spent waiting after `429` responses. |
| `ReporterSuccess` | long | Reports published to `snow.sink.reporter.success.topic`. |
| `ReporterError` | long | Reports published to `snow.sink.reporter.error.topic`, including unknown-field reports under `snow.sink.unknown.field.behavior=report`. |
| `LastSuccessfulRequestEpochMs` | long | Epoch milliseconds of the last successful ServiceNow response for this task; `0` if there has been none. |

What to alert on:

- `InFlight` pinned at `snow.sink.max.in.flight` with `ThrottledMillis` growing means the
  instance is the bottleneck; lower the in-flight count or ask for a higher rate-limit rule.
- `Ambiguous` above zero needs a human look: the rows may or may not exist. The error
  topic carries the request id for each one.
- `RetriesExhausted` growing while `Written` is flat means the instance has been
  unavailable or throttled for longer than `snow.retry.max.elapsed.ms`.

## Reading the beans

### JConsole or JMC

Start the worker with a JMX port, then connect and open the `sh.oso.servicenow` tree:

```bash
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.port=9999 \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false"
bin/connect-distributed.sh config/connect-distributed.properties
jconsole localhost:9999
```

### Prometheus `jmx_exporter`

Add two rules to the exporter configuration that already scrapes `kafka.connect`:

```yaml
rules:
  - pattern: 'sh.oso.servicenow<type=source-table, connector=(.+), task=(\d+), table=(.+)><>(\w+)'
    name: servicenow_source_table_$4
    labels:
      connector: "$1"
      task: "$2"
      table: "$3"
    help: "ServiceNow source table metric $4"
    type: GAUGE
  - pattern: 'sh.oso.servicenow<type=sink-writer, connector=(.+), task=(\d+)><>(\w+)'
    name: servicenow_sink_writer_$3
    labels:
      connector: "$1"
      task: "$2"
    help: "ServiceNow sink writer metric $3"
    type: GAUGE
```

The exporter lower-cases attribute names, so the series come out as
`servicenow_source_table_LagSeconds` style names unless you add `lowercaseOutputName: true`.
`WrittenByOperation` is tabular data; scrape the four scalar counters instead. Cumulative
counters are exported as gauges here because the exporter cannot tell them apart; use
`rate()` or `increase()` over them as you would with any counter.

Example PromQL:

```promql
# Tables more than five minutes behind the instance clock
max by (connector, table) (servicenow_source_table_LagSeconds) > 300

# Sink throughput per task
rate(servicenow_sink_writer_Written[5m])

# Share of poll time the source spent throttled
rate(servicenow_source_table_ThrottledMillis[5m]) / 1000
```

## Relationship to the framework's metrics

Kafka Connect's own `kafka.connect:type=source-task-metrics` and `sink-task-metrics` beans
still apply (poll and put batch sizes, offset commit latency, active record counts). The
beans on this page add what only the connector knows: where the cursor is, what the
instance answered and how long it kept the connector waiting.
