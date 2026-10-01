---
title: "Enterprise Support"
description: "The OSO Enterprise support subscription for the connectors."
sidebar_position: 20
---

# Enterprise Support

The connectors are open source and free to run. If you run them in production and want
someone accountable for them, OSO, the team that builds and maintains the connectors,
offers an annual **Enterprise support subscription**. This page sets out what it covers
so you can decide whether it is right for you. There are no licence keys and nothing
changes in the software: the subscription buys support and maintenance commitments, and
if it ends the connectors keep running.

## Community or enterprise

| | Community | Enterprise support subscription |
|---|---|---|
| Who | Everyone | Subscribers |
| Channel | [GitHub issues](https://github.com/osodevops/kafka-connect-servicenow-oss/issues) and [discussions](https://github.com/osodevops/kafka-connect-servicenow-oss/discussions) | support@oso.sh, the OSO support portal, and a shared Slack Connect or Microsoft Teams channel |
| Response | Best effort, no commitment | P1 within 60 minutes, P2 within 4 hours, P3 one business day, P4 two business days |
| Who answers | Maintainers, as time allows | The engineers who write the code, directly; no first-line triage layer, no outsourced helpdesk |
| Fixes | Next release | Patch releases for your P1 and P2 defects |

## What the subscription includes

| Component | Detail |
|---|---|
| Maintained releases | Security patches, dependency vulnerability updates and critical defect fixes for the supported versions, published as releases with plugin ZIPs. A defect you raise at P1 or P2 is fixed in a patch release rather than left for the next feature release. |
| ServiceNow family release readiness | ServiceNow ships two family releases a year (Xanadu, Yokohama, Zurich and later). The supported connector versions are tested against each one on a developer instance during its early-availability window, and you are told in writing of anything you need to act on before the release reaches your production instances. |
| Kafka Connect compatibility | Verification against each Apache Kafka release in the supported window, on Apache Kafka, Amazon MSK Connect, Strimzi and Confluent Platform. |
| Production support | Support during business hours with the priority definitions, response targets, monthly reporting and escalation below. |
| Onboarding review | Once, at the start of the subscription: a review of each connector configuration, the authentication flow and secret handling, the integration user's roles and time zone, rate limit rules and headroom, your offsets, backfill and dead letter queue runbook, and an upgrade to the current release. |
| Upgrade guidance | Release notes that flag anything requiring action, and help planning and executing upgrades. |
| Roadmap | Your issues are prioritised over unsubscribed issues, and your feature requests are recorded and reviewed at each quarterly service review. |
| Confluent migration guidance | Questions about the [documented migration path](migration/confluent.md), the configuration translator and the cutover verification are within support. Hands-on participation in a cutover is available as engineering days. |

## What is covered

Support covers the two connectors and their integration with your environment:

- Installation of the plugins on Kafka Connect workers, including Amazon MSK Connect
  custom plugins and Strimzi `KafkaConnect` image builds, and upgrades between releases
- Connector configuration for the [source](connectors/source.md) and the
  [sink](connectors/sink.md), including table selection, per-table queries and
  projections, topic routing, operation resolution and reporter topics
- [Authentication](reference/authentication.md): the Basic, OAuth 2.0 client credentials
  and password flows, application registry requirements as they relate to the connectors,
  and secret handling on the Connect worker
- [Offsets and recovery](concepts/offsets-and-recovery.md): the cursor tuple, query
  fingerprints, backfill, offset inspection and offset resets
- [Error handling](reference/error-handling.md): dead letter queue configuration, success
  and error reporters, retry classification, ambiguous-write policies and ServiceNow API
  error interpretation
- Throughput and rate limits: task sizing, batch sizes, poll intervals, the
  [concurrency cap](concepts/rate-limits.md) and ServiceNow rate limit rules
- Diagnosis of connector failures, performance problems and unexpected behaviour,
  including suspected missing or duplicated records
- ServiceNow Table API changes and family release impacts on the connectors
- Converter and [schema registry](reference/schemas.md) questions as they relate to the
  records the connectors produce and consume
- Security advisories and patches for the connectors and their bundled dependencies

## What is not covered

These remain with you or the relevant vendor. Support for the Kafka and Kafka Connect
platform itself is available from OSO as a separate service.

- Operation of the Apache Kafka brokers and the Kafka Connect clusters themselves, and of
  MSK, Strimzi or Confluent Platform as products
- ServiceNow instance administration: users, roles, ACLs, OAuth application registry
  entries, rate limit rules and instance properties, beyond confirming what the connectors
  need
- Schema registry and converter operation beyond the integration with the connectors
- The applications and data flows on either side of the topics
- Confluent's own ServiceNow connectors
- Custom development and feature work, available as engineering days

## Hours and response targets

Business hours are 08:00 to 18:00 UK time, Monday to Friday, excluding UK public
holidays. Response targets are measured within those hours; a ticket raised outside them
is picked up at the start of the next business period. OSO does not operate a staffed
24x7 desk: out-of-hours P1 call-out and on-call cover for agreed days, such as a
production cutover or a ServiceNow upgrade weekend, are available as priced options.

| Priority | Definition | Response | Resolution target |
|---|---|---|---|
| P1 Critical | A production connector has stopped or is failing with no workaround; records are suspected lost, duplicated or corrupted in production; or a connector is exhausting the instance's rate limits or degrading it for other integrations | Within 60 minutes | Workaround or recovery path within 4 hours; patch release as soon as practicable |
| P2 High | A production pipeline is degraded or partially failing (one table of several, a filling dead letter queue, throughput below need); a workaround exists but is not sustainable; a rehearsal or cutover is blocked ahead of a scheduled change | Within 4 hours | Within 1 business day |
| P3 Medium | A non-urgent defect, an issue with an acceptable workaround, or a how-to question with production impact | Within 1 business day | Within 3 business days |
| P4 Low | Enhancement requests, documentation and roadmap questions | Within 2 business days | Within 5 business days or the next scheduled release |

Every P1 receives a documented root cause analysis within five business days of
resolution. Escalation runs from the responding engineer to a lead engineer and then to
the CTO. Subscribers receive a monthly report against these targets and a quarterly
service review.

## Supported versions

| Component | Supported |
|---|---|
| kafka-connect-servicenow | The current release and the previous minor release, with security patches for both. |
| Apache Kafka Connect | Kafka 3.x, built and tested against 3.9, with the end-to-end tests also run on a 4.x worker; Java 17 and 21 on the worker. |
| Connect runtimes | Apache Kafka, Amazon MSK Connect, Strimzi and Confluent Platform. |
| ServiceNow | The REST Table API and Basic and OAuth 2.0 inbound authentication; only public, documented APIs are used and nothing is installed on the instance. |

## Security and maintenance commitments

Vulnerability reports are acknowledged within two business days and assessed within
five; critical and high severity issues in a supported version are fixed, or a
mitigation published, within ten business days, and subscribers are notified directly.
Releases are cut by an automated process from `main`, tested on Java 17 and 21 against
the fake ServiceNow shipped in the repository, signed and published to Maven Central with
the plugin ZIPs, checksums and an SBOM attached to the GitHub release. At least two OSO
engineers hold release rights. The connectors never contact OSO, and OSO has no access to
your clusters, instances or data.

The full policy is published in the repository as
[SUPPORT.md](https://github.com/osodevops/kafka-connect-servicenow-oss/blob/main/SUPPORT.md)
and
[SECURITY.md](https://github.com/osodevops/kafka-connect-servicenow-oss/blob/main/SECURITY.md).

## How to get it

The subscription is a flat annual fee covering both connectors on every Kafka Connect
cluster you run, production and non-production, against every ServiceNow instance, with
no per-connector or per-task count. Email [sales@oso.sh](mailto:sales@oso.sh) or
[book a 30-minute call](https://meetings-eu1.hubspot.com/sion-smith) and tell us which
Kafka distribution you run Connect on, how many instances and environments are involved,
which connectors you use or plan to use, and whether you are migrating from Confluent's
connectors. You will get a written proposal with the service levels above and a price for
your estate.
