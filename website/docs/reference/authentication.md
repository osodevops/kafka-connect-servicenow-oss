---
title: "Authentication"
description: "Basic auth, OAuth 2.0 client credentials and password grants."
sidebar_position: 3
---

# Authentication

Both connectors share the same `snow.auth.*` and `snow.oauth.*` configuration,
implemented once in `snow-core` and defined once in `CoreConfigDefs`.

## Flows

| Flow | Configuration | When to use |
|---|---|---|
| Basic | `snow.auth.type=basic` | Simplest; a dedicated integration user over HTTPS. Credentials travel with every request |
| OAuth 2.0 client credentials | `snow.auth.type=oauth2`, `snow.oauth.grant.type=client_credentials` | Recommended for server-to-server integrations; needs the inbound client credentials grant enabled on the instance and an OAuth application user |
| OAuth 2.0 password | `snow.auth.type=oauth2`, `snow.oauth.grant.type=password` | Works on every instance without enabling anything; combines the OAuth client with the integration user's credentials |

Instance-side setup for each flow is in
[ServiceNow setup](../getting-started/servicenow-setup.md). JWT bearer is not a generally
available inbound grant on ServiceNow and is not offered; the `TokenProvider` interface in
`snow-core` leaves room for it or for an instance-specific flow without touching the
connectors.

### Basic

```json
{
  "snow.url": "https://acme.service-now.com",
  "snow.auth.type": "basic",
  "snow.auth.username": "${file:/secrets/snow.properties:username}",
  "snow.auth.password": "${file:/secrets/snow.properties:password}"
}
```

The connector sends `Authorization: Basic ...` on every request. No token endpoint is
involved and a `401` is a hard failure after one retry.

### OAuth 2.0 client credentials

```json
{
  "snow.url": "https://acme.service-now.com",
  "snow.auth.type": "oauth2",
  "snow.oauth.grant.type": "client_credentials",
  "snow.oauth.client.id": "${file:/secrets/snow.properties:client.id}",
  "snow.oauth.client.secret": "${file:/secrets/snow.properties:client.secret}",
  "snow.oauth.scope": "useraccount"
}
```

The connector posts `grant_type=client_credentials` with the client id and secret,
form-encoded, to `{snow.url}/oauth_token.do`. The token runs as the OAuth application
user configured on the application registry record, so that user's roles and UTC time
zone apply. `snow.oauth.scope` is optional; `snow.oauth.token.url` overrides the derived
token endpoint when the instance sits behind a gateway.

### OAuth 2.0 password

```json
{
  "snow.url": "https://acme.service-now.com",
  "snow.auth.type": "oauth2",
  "snow.oauth.grant.type": "password",
  "snow.oauth.client.id": "${file:/secrets/snow.properties:client.id}",
  "snow.oauth.client.secret": "${file:/secrets/snow.properties:client.secret}",
  "snow.auth.username": "${file:/secrets/snow.properties:username}",
  "snow.auth.password": "${file:/secrets/snow.properties:password}"
}
```

The connector posts `grant_type=password` with the client id, client secret, username
and password. The token runs as the named user. Confluent's `auth.type=OAUTH2` maps to
this configuration or the client credentials one depending on how the instance is set up;
see [Migrating from Confluent](../migration/confluent.md).

## Token lifecycle

`OAuthTokenProvider` implements the `TokenProvider` contract (`authorization()`,
`invalidate(seen)`, `close()`):

- The access token is fetched lazily on the first request and cached per task.
- It is refreshed proactively at `expires_in` minus 30 seconds, using the `refresh_token`
  grant when the instance issued a refresh token, otherwise by repeating the original
  grant.
- Refresh is **single-flight**: fifty concurrent callers that find the token expired
  trigger exactly one token request and share its result.
- On any `401`, the calling request invalidates the token it used (invalidate-if-same, so
  a caller holding a stale token cannot discard a newer one) and retries **once** with a
  fresh token. A second `401` is a `ConnectException` naming the flow, never the secret.
- A failed token request is classified like any other HTTP failure: `429` and `5xx` from
  `/oauth_token.do` are retried with backoff; `400` and `401` (bad client or bad
  credentials) fail the task with a diagnostic.
- Tokens are dropped on `close()`. Nothing is written to disk.

`BasicTokenProvider` returns a constant header and `invalidate` is a no-op, so the `401`
retry-once still happens but cannot succeed with different credentials.

## Secret handling

- `snow.auth.password`, `snow.oauth.client.secret`, the proxy password and the TLS
  passwords are Connect `PASSWORD`-type configs, masked in the REST API, in
  `toString()` and in logs.
- Tokens, `Authorization` headers and token endpoint responses are never logged at any
  level; `Redaction` also scrubs them from exception messages and from the sink
  reporter's `response_excerpt`. The `NoCredentialLeakTest` in `snow-core` captures the
  log at `TRACE` across the auth, `401` and `5xx` paths to keep it that way.
- Use a Connect **config provider** to keep secrets out of connector JSON and out of the
  config topic:

```properties
# worker
config.providers=file,env
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
```

```json
"snow.oauth.client.secret": "${file:/secrets/snow.properties:client.secret}",
"snow.auth.password": "${env:SNOW_PASSWORD}"
```

Any provider works, including the vault and secrets-manager providers shipped by MSK
Connect, Strimzi and Confluent Platform, because resolution happens in the worker before
the connector sees the value.

## Proxy

```properties
snow.http.proxy.url=http://proxy.internal:3128
snow.http.proxy.username=${file:/secrets/snow.properties:proxy.user}
snow.http.proxy.password=${file:/secrets/snow.properties:proxy.password}
```

The proxy applies to every request to the instance, including the token endpoint. The
username and password are optional and are supplied through a Java `Authenticator`, so
Basic proxy authentication works with the JDK's default `jdk.http.auth.tunneling.disabledSchemes`
setting cleared on the worker if the proxy requires it for HTTPS tunnels.

## TLS and mutual TLS

The instance's certificate is validated against the JVM truststore by default. To trust a
private CA, or to present a client certificate:

```properties
snow.tls.truststore.path=/secrets/snow-truststore.p12
snow.tls.truststore.password=${file:/secrets/snow.properties:truststore.password}
snow.tls.truststore.type=PKCS12

snow.tls.keystore.path=/secrets/snow-client.p12
snow.tls.keystore.password=${file:/secrets/snow.properties:keystore.password}
snow.tls.keystore.type=PKCS12
```

`type` accepts `PKCS12` (default) or `JKS`. The keystore is used for mutual TLS when the
instance, or a gateway in front of it, requests a client certificate; `snow-core` builds
one `SSLContext` per task and it is exercised in the test suite against a WireMock
server configured with `needClientAuth`. TLS settings and proxy settings combine.

## Timeouts

```properties
snow.http.connect.timeout.ms=10000
snow.http.request.timeout.ms=60000
```

`request.timeout.ms` bounds the whole request, including reading the body. A timeout
that fires after a `POST` was transmitted is an **ambiguous** failure for the sink; see
[Error handling](error-handling.md).
