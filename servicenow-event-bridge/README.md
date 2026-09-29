# ServiceNow Event Bridge (not part of the connector release)

PRD-03 (`docs/04_prd_event_bridge.md`) describes an optional ServiceNow-side application
(Business Rule, outbox table, async dispatcher, HTTPS gateway) that captures deletes and
cuts latency below the poll interval. It needs code installed on the ServiceNow instance and
is governed, packaged and released separately from the Apache-2.0 connector artefacts.

Nothing in this directory ships in the plugin ZIPs or on Maven Central. The polling source
connector remains authoritative; the bridge, when it exists, is reconciled against it.
