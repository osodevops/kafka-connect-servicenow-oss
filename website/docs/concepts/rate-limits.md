---
title: "Rate limits"
description: "Retry-After, backoff with jitter and the per-instance concurrency cap."
sidebar_position: 4
---

# Rate limits

ServiceNow has no single global API quota. Administrators create **inbound REST rate
limit rules** per instance that cap requests per hour for a resource, a user, a role or
everyone, and the counts are shared by every integration using the same user. The
connectors therefore do not hard-code a budget; they react to what the instance tells
them and keep their own request rate predictable so you can size a rule for it.

## What the instance does

When a request exceeds a rule, the instance answers `429 Too Many Requests` with a
`Retry-After` header giving the number of seconds until the window opens again. Some
releases also add `X-RateLimit-*` headers; they are not documented for every release and
the connectors read them when present but never depend on them.

Rules are configured under **All > System Web Services > REST > Rate Limit Rules**; see
[ServiceNow setup](../getting-started/servicenow-setup.md). A rule for the integration
user alone is the safest: it protects the rest of the instance from a misconfigured
backfill without throttling other integrations.

## How the connectors react

Every request goes through `snow-core`'s `RetryPolicy`:

1. A `429` is retryable. If `Retry-After` is present the connector sleeps for exactly that
   long (seconds or an HTTP date); otherwise it backs off exponentially with full jitter
   from `snow.retry.initial.backoff.ms`, doubling up to `snow.retry.max.backoff.ms`.
2. `408`, `425`, `500`, `502`, `503` and `504` are retried with the same backoff.
   `409` is retried only when the response body indicates a transient conflict.
   `400`, `403`, `404` and `422` are never retried.
3. Retries stop after `snow.retry.max.attempts` attempts **or** when
   `snow.retry.max.elapsed.ms` has passed since the first attempt, whichever comes first.
   The connector then throws a `RetriableException`, so the Connect framework itself
   retries the poll or the batch, and the task status stays `RUNNING` instead of `FAILED`.
4. Throttled time is counted per table (source) and per writer (sink) in the JMX metrics,
   so a rule that is too tight shows up as `throttledMillis` before it shows up as
   latency.

Retries never loop indefinitely inside `poll()` or `put()`; the bounds above always
apply.

## The per-instance concurrency cap

`snow.http.max.concurrent.requests` caps the number of requests in flight to one instance
from one task, whichever tables or partitions they belong to. It is a semaphore in
`snow-core`, taken before a request is sent and released when the response is read, so a
429 storm or a slow instance cannot pile up requests. The sink's
`snow.sink.max.in.flight` (default `8`) additionally caps how many partition lanes write
concurrently, and the smaller of the two wins.

With `snow.http.adaptive.throttling=true` the cap adapts: it halves on a `429` and grows
by one after fifty consecutive successes, and when the instance reports remaining
capacity below ten percent of its limit the connector sleeps until the reset time instead
of racing into the `429`.

## Sizing

The source's request rate is easy to state:

| Phase | Requests |
|---|---|
| Backfill | One page per `batch.size` rows, back to back, per table. A million rows at the default `5000` is about 200 requests |
| Stream, idle table | One request per `poll.interval.ms` per table, whether or not anything changed. Ten tables at the default `5000` ms is 7,200 requests per hour |
| Stream, busy table | One request per poll plus one per additional full page |
| Startup | One probe request per table |

Guidance:

- **Poll interval** is the lever for steady-state cost. Latency-sensitive tables can poll
  every few seconds; reference data can poll every few minutes. Set
  `snow.table.<alias>.poll.interval.ms` per table rather than lowering the global
  `snow.source.poll.interval.ms`. Do not go below two seconds: with one-second timestamp
  precision and the default one-second safety lag there is nothing to gain.
- **Batch size** trades memory for requests. `sysparm_limit` up to `10000` is the instance
  default cap; larger pages mean fewer requests during backfill but larger JSON bodies to
  parse and hold. `5000` is a sensible default; the response size cap in `snow-core`
  protects the worker if a table has very wide rows.
- **Tasks** spread tables across workers but do not reduce request count. `tasks.max`
  above the number of tables creates idle tasks; below it, several tables share a task
  and are polled round-robin, which is fine for tables with modest change rates.
- **Sink** throughput is roughly `snow.sink.max.in.flight` divided by the instance's
  write latency per request, because the Table API writes one record per call. With
  eight lanes and 150 ms per write that is about fifty writes per second per task. Add
  tasks (and partitions) before raising `max.in.flight` beyond what the instance is happy
  with.
- **Rule size.** Sum the steady-state rate of every connector using the integration user,
  add the backfill rate you are prepared to run in one hour, and set the hourly rule a
  little above it. A backfill that hits the rule slows down gracefully; it does not fail.

## Reading the signals

- Worker log: `429 ... retrying after` lines mean the rule is engaged; `retries exhausted`
  followed by a `RetriableException` means it stayed engaged longer than
  `snow.retry.max.elapsed.ms`.
- JMX (`sh.oso.servicenow:type=source-table,...` and `type=sink-writer,...`): `retries`,
  `throttledMillis`, `lagSeconds` (source) and `inFlight` (sink).
- Instance side: the rate limit rule's own violation log shows which user and resource
  tripped it.
