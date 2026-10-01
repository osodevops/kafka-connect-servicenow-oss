---
title: "Local testing"
description: "The fake ServiceNow, the Docker e2e and the contract tier."
sidebar_position: 2
---

# Local testing

There is no "LocalStack for ServiceNow", so this project ships its own **fake
ServiceNow**, exported from `snow-core`'s test-jar (`sh.oso.servicenow.testing`). Every
connector behaviour, including same-second buckets, restart at every page, 429 storms and
ambiguous writes, is verified against it in CI with no instance and no secrets. Two
optional tiers run against a real instance when you have one.

## The fake ServiceNow

| Component | Simulates | How |
|---|---|---|
| `MockServiceNowServer` | Basic auth, `POST /oauth_token.do` (`client_credentials`, `password` and `refresh_token` grants with a configurable `expires_in`), and the Table API | WireMock on a dynamic port with a stateful `ResponseDefinitionTransformerV2` |
| `TableStore`, `FakeTable`, `FakeRow` | Tables keyed by `sys_id`; `sys_created_on`, `sys_updated_on` (second precision from a `MutableClock`) and `sys_mod_count` maintained on every write | In-memory; tracks the in-flight high-water mark for the concurrency-bound tests |
| `EncodedQueryEvaluator` | `=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`, `STARTSWITH`, `IN`, `ISEMPTY`, `ISNOTEMPTY`, `^`, `^OR`, `^NQ`, `ORDERBY`, `ORDERBYDESC`; `sysparm_limit`, `sysparm_offset`, `sysparm_fields`, `sysparm_display_value`, `sysparm_exclude_reference_link`, `sysparm_query_no_domain` | Plus an `invalidQueryReturnsNoRows` toggle that mimics `glide.invalid_query.returns_no_rows` |
| `FaultInjector` | `unauthorizedOnce`, `rateLimit(times, retryAfter)`, `serverError`, `timeoutAfterWrite` (the write is applied, then the response is delayed past the client timeout), `malformedJsonOnce`, `truncatedBodyOnce`, `hideField` (ACL simulation), `forbidTable` | Per table or globally |
| `RequestJournal` | What the connector actually sent: bodies per request, request counts, the in-flight high-water mark | Used by `PatchSendsOnlyIntendedFieldsTest` and the writer bound tests |
| `FakeServiceNowMain` | The same fake as a standalone process, seeded with `incident` rows | Runs in the `fake` profile of `examples/docker-compose.yml` and behind the Docker e2e |

Because both connectors take their endpoints from configuration (`snow.url`,
`snow.oauth.token.url`), pointing a connector at the fake is a config change, not a code
path.

## Test tiers

| Tier | What runs | Command |
|---|---|---|
| Unit | Pure logic: cursor comparator and offset codec (jqwik property tests), query builder, retry policy, classifiers, mappers | `mvn test` |
| Fake harness | Connector tasks over real HTTP against `MockServiceNowServer`: auth, pagination, dedup, restart, ordering, fault injection | `mvn test` |
| Docker e2e | The packaged plugin ZIPs inside a real Kafka Connect worker (`plugin.discovery=service_load`) on Kafka 3.x and 4.x | `mvn verify` (skip with `-DskipE2E`) |
| Contract | The Table API operations and per-table options against a real instance | `mvn verify -Pcontract -DskipE2E -DskipUnitTests=true` |
| Soak | A 1,000,000-row backfill in a 256 MB heap | `mvn verify -Psoak -DskipE2E -DskipUnitTests=true -pl connect-servicenow-source -am` |

## Writing a test against the harness

`TaskHarness` (source) and `SinkTaskHarness` (sink) implement the Connect task contexts
with an in-memory offset store, so restart and resume are tested without a worker:

```java
MockServiceNowServer snow = new MockServiceNowServer().start();
snow.tables().create("incident")
    .insert(row("number", "INC0000001", "short_description", "Unable to connect"))
    .insert(row("number", "INC0000002", "short_description", "Printer on fire"));

Map<String, String> props = new HashMap<>();
props.put("snow.url", snow.baseUrl());
props.put("snow.auth.type", "basic");
props.put("snow.auth.username", "admin");
props.put("snow.auth.password", "admin");
props.put("snow.tables", "inc");
props.put("snow.table.inc.name", "incident");
props.put("snow.table.inc.topic", "servicenow.incident");

TaskHarness harness = TaskHarness.start(new ServiceNowSourceTask(), props);
List<SourceRecord> first = harness.pollUntil(records -> records.size() == 2);

snow.clock().advanceSeconds(5);
snow.tables().get("incident").update(first.get(0).key().toString(), "urgency", "1");

harness.restart();                       // keeps the in-memory offsets, new task instance
List<SourceRecord> after = harness.pollUntil(records -> !records.isEmpty());
assertThat(after).extracting(r -> r.key()).containsExactly(first.get(0).key());
```

Fault injection:

```java
snow.faults().unauthorizedOnce();                       // 401 then success: refresh once
snow.faults().rateLimit(3, Duration.ofSeconds(2));      // three 429s with Retry-After: 2
snow.faults().timeoutAfterWrite("incident");            // POST applied, response never arrives
snow.faults().hideField("incident", "caller_id");       // ACL hides a field
snow.faults().forbidTable("sys_user");                  // 403 on one table
```

The exact harness API is in `snow-core/src/test/java/sh/oso/servicenow/testing`; the
tests in each connector module are the best examples.

## The Docker end-to-end tests

`ConnectClusterEndToEndIT` in `e2e-tests` uses Testcontainers to start a Kafka broker
and a Connect worker (`connect-distributed.sh` on an `apache/kafka` image), copies both
assembled plugin directories into `/plugins`, sets `plugin.discovery=service_load`, and
drives the connectors over REST with a real producer and consumer. The fake ServiceNow
runs on the host and the worker reaches it through `host.testcontainers.internal`. It
proves what task-level tests cannot: plugin packaging, class loader isolation, the KIP-898
manifests and REST configuration plumbing.

- Docker must be running; the tests are skipped with `-DskipE2E`.
- The worker image defaults to `apache/kafka:3.7.0`; override it with
  `-De2e.kafka.image=apache/kafka:4.1.0`. CI runs both.
- Tests cover `/connector-plugins` listing both classes, source rows arriving with the
  right key and headers, sink JSON records creating and patching fake rows, tombstones
  deleting them, and two sink tasks preserving per-partition order.

## The contract tier

`*ContractIT` classes (`TableApiContractIT`, `SourceContractIT`, `SinkContractIT`) run
against a real instance and are **skipped unless `SNOW_URL` is set**:

```bash
export SNOW_URL=https://dev123456.service-now.com
export SNOW_USERNAME=admin
export SNOW_PASSWORD='...'
# optional: also exercise the OAuth client credentials grant
export SNOW_OAUTH_CLIENT_ID=...
export SNOW_OAUTH_CLIENT_SECRET=...

mvn verify -Pcontract -DskipE2E -DskipUnitTests=true
```

They prove the six Table API operations, the query, projection, display value, view and
domain options against a live instance, and the sink's create, patch, put and delete. Each
test creates scratch rows in `incident` tagged with a unique marker in
`short_description`, and deletes them in `@AfterAll`; a failed run can leave rows behind,
which the next run's marker query does not pick up, so clean them with
`short_descriptionSTARTSWITHkafka-connect-servicenow-contract` if they bother you.

A **Personal Developer Instance** is enough. Register at
[developer.servicenow.com](https://developer.servicenow.com/), request an instance, note
its URL and the `admin` password, and set the `admin` user's time zone to UTC. PDIs
hibernate after a period of inactivity and are reclaimed after a longer one; wake the
instance in the developer portal before running the tier.

In CI, `contract-tests.yml` runs this tier on `workflow_dispatch` and weekly from the
`servicenow-pdi` GitHub environment, which requires a reviewer to approve each run, so
the secrets never reach pull request builds. Keep real-instance runs out of per-PR CI:
timing makes them flaky and the instance is shared.

## The soak tier

`BackfillSoakIT` seeds the fake with one million `incident` rows spread across many
seconds (including several buckets larger than the batch size), runs the source task in a
JVM capped at `-Xmx256m` until the cursor reaches the high-water mark, and asserts that
every row was emitted exactly once with a monotonic cursor. It takes several minutes and
runs weekly in `soak.yml`; run it locally before touching the poller or the dedup cache.
