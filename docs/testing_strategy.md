# Testing strategy

Everything runs locally with no ServiceNow instance. The test tiers mirror the
Salesforce connector programme: unit and fake-harness tests under Surefire, Docker
end-to-end tests under Failsafe in `e2e-tests`, plus two optional tiers (contract
tests against a real instance, and a weekly soak).

## Test tiers

| Tier | Proves | Runs | Where |
|---|---|---|---|
| Unit | pure logic: cursor comparator, offset codec, query builder, retry policy, classifiers, mappers (jqwik property tests for the cursor and codec) | every build (`mvn verify`) | each module, Surefire `*Test` |
| Fake harness | connector behaviour over real HTTP against `MockServiceNowServer` (auth, pagination, dedup, restart, ordering, fault injection) | every build | each module, Surefire `*Test` |
| Docker e2e | the packaged plugin ZIPs inside a real Kafka Connect worker (`plugin.discovery=service_load`) on Kafka 3.x and 4.x | every build (skip with `-DskipE2E`) | `e2e-tests`, Failsafe `*IT` |
| Contract | the six Table API operations, query/projection/display/view/domain options and sink CRUD against a real instance | manual `workflow_dispatch` plus a weekly schedule, gated by the `servicenow-pdi` environment | `*ContractIT` under `-Pcontract`, env-gated on `SNOW_URL` |
| Soak | 1,000,000-row backfill in a 256 MB heap | weekly | `BackfillSoakIT` under `-Psoak` |

## The fake harness

`snow-core` publishes a `test-jar` with `sh.oso.servicenow.testing`:

1. **`MockServiceNowServer`** (WireMock `org.wiremock:wiremock`, dynamic port):
   - Basic auth and `POST /oauth_token.do` for the `client_credentials`, `password` and
     `refresh_token` grants, with a configurable `expires_in`.
   - A stateful Table API (`/api/now/table/{table}[/{sys_id}]`) implemented as a WireMock
     `ResponseDefinitionTransformerV2` over an in-memory `TableStore`: rows keyed by `sys_id`,
     with `sys_created_on`, `sys_updated_on` (second precision from a `MutableClock`) and
     `sys_mod_count` maintained on every write.
   - `EncodedQueryEvaluator`: the subset of the encoded query language the connectors and
     their users need (`=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`, `STARTSWITH`, `IN`, `ISEMPTY`,
     `ISNOTEMPTY`, `^`, `^OR`, `^NQ`, `ORDERBY`, `ORDERBYDESC`), plus a toggle that mimics
     `glide.invalid_query.returns_no_rows`.
   - `sysparm_limit`, `sysparm_offset`, `sysparm_fields`, `sysparm_display_value`,
     `sysparm_exclude_reference_link`, `sysparm_query_no_domain`, ServiceNow-shaped bodies
     and error JSON.
2. **`FaultInjector`**: `unauthorizedOnce`, `rateLimit(times, retryAfter)`, `serverError`,
   `timeoutAfterWrite` (the write is applied, then the response is delayed past the client
   timeout), `malformedJsonOnce`, `truncatedBodyOnce`, `hideField` (ACL simulation),
   `forbidTable`.
3. **`RequestJournal`**: what the connector actually sent (PATCH bodies per record, the
   in-flight high-water mark used by the concurrency-bound tests, request counts).
4. **`FakeServiceNowMain`**: the same fake as a standalone process for the
   `docker compose --profile fake` quickstart.

Every connector config has the endpoint it needs (`snow.url`, `snow.oauth.token.url`), so
pointing a connector at the fake is a config change, not a code path.

## Kafka side

- `TaskHarness` (source) and `SinkTaskHarness` (sink) implement the Connect task contexts
  with an in-memory offset store, so restart and resume are tested without a worker.
- `ConnectClusterEndToEndIT` starts Kafka and a Connect worker with Testcontainers, copies
  the assembled plugin directories into `/plugins`, registers both connectors over REST and
  drives them with a real producer and consumer. The fake ServiceNow runs on the host and is
  reached through `host.testcontainers.internal`.
- `ConfigDocsGeneratorTest` renders the configuration reference from each `ConfigDef`; CI
  fails if the rendered files differ from what is committed.
- `PluginZipContentsTest` opens each plugin ZIP and asserts the service-loader manifests are
  present and that no `connect-api`, `kafka-clients` or `connect-transforms` jar leaked into
  `lib/`.

## Contract tier (real instance)

`*ContractIT` classes are skipped unless `SNOW_URL` is set. They create scratch rows in
`incident` tagged with a unique marker in `short_description`, and delete them in
`@AfterAll`. The GitHub workflow `contract-tests.yml` runs them from the `servicenow-pdi`
environment, so the secrets are never exposed to pull requests. See
`website/docs/development/local-testing.md` for running them from a laptop.
