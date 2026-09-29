# Support, lifecycle and maintenance

kafka-connect-servicenow is open source (Apache-2.0). Commercial support for the
two connectors is available from OSO as an annual **Enterprise support
subscription** (overview: https://servicenowkafkaconnector.com/enterprise-support).
The figures below are the standard terms; a support schedule
agreed with a subscriber may extend them (longer hours, on-call cover, service
credits) but never reduces them.

## How to get help

| Channel | Who | What to expect |
|---|---|---|
| [GitHub issues](https://github.com/osodevops/kafka-connect-servicenow-oss/issues) | everyone | Best effort. Bugs are triaged; there is no response-time commitment. |
| support@oso.sh / support portal | subscribers | The severity targets below. Include the connector version, the Kafka Connect distribution and version, the connector configuration with secrets redacted, the worker log around the failure and, where relevant, a dead letter queue sample. |
| Security reports | everyone | See [SECURITY.md](SECURITY.md); never open a public issue for a vulnerability. |
| Sales | everyone | sales@oso.sh, or book a call at https://meetings-eu1.hubspot.com/sion-smith |

## Scope (subscription)

Covered: installation of the plugins on Kafka Connect workers (including Amazon
MSK Connect and Strimzi image builds) and upgrades between releases; configuration
of the source and sink connectors; authentication flows (Basic, OAuth 2.0) and
secret handling on the worker; cursors, backfill, offset inspection and offset
resets; dead letter queue, success and error reporters and error handling;
throughput, task sizing and ServiceNow rate limits; diagnosis of failures and
suspected missing or duplicated records; ServiceNow Table API changes and
family release (Xanadu, Yokohama, Zurich and later) impacts on the connectors; converter and schema registry
questions as they relate to the records the connectors produce and consume;
security advisories and patches; guidance on the documented migration from
Confluent's ServiceNow connectors.

Out of scope: operating the Kafka brokers and Kafka Connect clusters themselves
(available as a separate service), ServiceNow instance administration beyond what the
connectors need (roles, ACLs, OAuth application registry entries), schema registry and converter operation, the applications on
either side of the topics, Confluent's own connectors, and custom development
(available as engineering days).

## Severity levels and response targets

Support hours: 08:00 to 18:00 UK time, Monday to Friday, excluding UK public
holidays. Response targets are measured within those hours; a ticket raised
outside them is picked up at the start of the next support period. There is no
staffed 24x7 desk. Out-of-hours P1 response is available as a per-incident
call-out or as an on-call retainer for agreed days, both priced separately in
the support agreement.

| Priority | Definition | Initial response | Workaround / mitigation |
|---|---|---|---|
| P1 | A production connector has stopped or is failing with no workaround; records are suspected lost, duplicated or corrupted in production; or a connector is exhausting the instance's rate limits or degrading it for other integrations | 60 minutes | 4 hours |
| P2 | A production pipeline is degraded or partially failing (one table, a filling dead letter queue, throughput below need); a workaround exists but is not sustainable; a rehearsal or cutover is blocked ahead of a scheduled change | 4 hours | 1 business day |
| P3 | Non-urgent defect, an issue with an acceptable workaround, or a how-to question with production impact | 1 business day | 3 business days |
| P4 | Enhancement request, documentation, roadmap question | 2 business days | 5 business days |

Every P1 receives a documented root cause analysis within 5 business days of
resolution. Escalation runs from the responding engineer to a lead engineer and
then to the CTO. Subscribers receive a monthly report on these targets and a
quarterly service review.

## Supported versions

| Component | Supported window | Notes |
|---|---|---|
| kafka-connect-servicenow | the current and the previous **minor** release (`0.N` and `0.N-1`) | Patch releases replace the previous patch. Versions outside the window keep working but receive no fixes; the end of support for a minor release is announced in the CHANGELOG with the release that succeeds it. |
| Apache Kafka Connect | Kafka 3.x; built and tested against 3.9 | Dead letter queue support needs Connect 2.6 or later. Java 17 and 21 on the worker (both in CI). |
| Connect runtimes | Apache Kafka, Amazon MSK Connect, Strimzi, Confluent Platform | The plugins are standard Kafka Connect plugin ZIPs. |
| ServiceNow | REST Table API (`/api/now/table`), Basic and OAuth 2.0 inbound authentication | Only public, documented APIs are used; no ServiceNow-side application is installed. |
| ServiceNow releases | each family release (two a year) | Subscribers: the supported connector versions are tested against each family release on a developer instance during its early-availability window, and anything a subscriber needs to act on is communicated in writing before the release reaches production instances. |

## Security fixes

- Reports are acknowledged within two business days and given a CVSS-based
  severity assessment within five.
- Critical and high severity issues in a supported version are fixed, or a
  mitigation is published, within ten business days; other severities with the
  next scheduled release.
- Fixes are backported to every supported minor release and published as a
  GitHub Security Advisory; subscribers are notified directly.
- Bundled-dependency vulnerabilities in the plugin ZIPs are monitored with
  Dependabot and addressed in patch releases.

## Maintenance and continuity

- **Release process.** Every release is cut from `main` by the automated process
  in [website/docs/development/releasing.md](website/docs/development/releasing.md):
  CI runs the unit, integration and Docker end-to-end suites on Java 17 and 21
  against the fake ServiceNow shipped in `snow-core`; release-please produces the
  version and CHANGELOG from conventional commits; the release workflow validates
  the version, builds and tests everything, publishes signed artefacts to Maven
  Central (`sh.oso`) and attaches the two plugin ZIPs to the GitHub release. The
  documented Confluent cutover procedure runs on every build and publishes a
  migration evidence artefact.
- **Who can release.** At least two OSO engineers hold release rights (repository
  admin, CI, Maven Central namespace). Either can cut a release or respond to an
  incident; the process does not depend on one person.
- **Source availability.** The connectors are public and Apache-2.0 licensed. The
  repository, CI definitions and release scripts contain everything needed to
  build, test and release them; there is nothing to escrow.
- **No phone-home.** The connectors never contact OSO. There are no licence keys
  and no licensing topic on the Kafka cluster.
