# ADR 0002: Basic, OAuth 2.0 client credentials and OAuth 2.0 password grants

Status: accepted (2026-09-29)

## Context

PRD-00 asks for Basic and OAuth 2.0 client credentials. ServiceNow's inbound client
credentials grant is disabled by default and needs the system property
`glide.oauth.inbound.client.credential.grant_type.enabled` plus an OAuth application user;
many instances only expose the resource-owner password grant. JWT bearer is not a generally
available inbound grant and is not advertised.

## Decision

`snow.auth.type=basic|oauth2`. For `oauth2`, `snow.oauth.grant.type=client_credentials`
(default) or `password`; the password grant reuses `snow.auth.username` and
`snow.auth.password` together with the client id and secret. Tokens are cached and refreshed
single-flight before expiry, using `refresh_token` when the instance issues one. A
`TokenProvider` interface leaves room for JWT or custom flows without touching the
connectors.

## Consequences

- Customers on any supported release can authenticate without instance changes (Basic or
  password grant), and can move to client credentials when the property is enabled.
- All secrets are Connect `PASSWORD` types, redacted in `toString`, logs and exceptions.
- Confluent's `auth.type=BASIC|OAUTH2` maps directly in the migration tooling.
