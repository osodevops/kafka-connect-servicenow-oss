---
title: "Releasing"
description: "release-please, Maven Central and the GitHub release."
sidebar_position: 3
---

# Releasing

Releases are fully automated and nobody tags by hand:

1. **Conventional commits** (`feat:`, `fix:`, `feat!:`, ...) on `main` drive
   [release-please](https://github.com/googleapis/release-please), which maintains a
   release PR with the version bump in every module POM and `CHANGELOG.md`.
2. **Merging the release PR** creates the `vX.Y.Z` tag and the GitHub Release.
3. The **release workflow** then:
   - validates that the version is not a `-SNAPSHOT`, that the root POM version equals
     the tag, and that **every reactor module** resolves to that version (release-please
     bumps each module POM; a module left behind fails here, not on Central),
   - runs `mvn clean verify` (unit, fake-harness and Docker e2e tiers),
   - deploys the published modules to **Maven Central** (Sonatype Central Portal, `sh.oso`
     namespace), signed, with sources and javadoc,
   - writes a SHA-256 file per plugin ZIP and copies the CycloneDX SBOM from the release
     profile's `target/bom.json`,
   - attaches the two plugin ZIPs, their `.sha256` files and the SBOM to the GitHub Release.

## Required repository secrets

| Secret | Purpose |
|---|---|
| `CENTRAL_USERNAME` / `CENTRAL_TOKEN` | Sonatype Central Portal user token (organisation-level `OSSRH_USERNAME` / `OSSRH_TOKEN` are accepted as a fallback) |
| `MAVEN_GPG_PRIVATE_KEY` | ASCII-armoured GPG private key for artefact signing |
| `MAVEN_GPG_PASSPHRASE` | Its passphrase |

The `servicenow-pdi` GitHub **environment** holds `SNOW_URL`, `SNOW_USERNAME`,
`SNOW_PASSWORD` and optionally `SNOW_OAUTH_CLIENT_ID` and `SNOW_OAUTH_CLIENT_SECRET` for
the contract tier, with a required reviewer so that a run has to be approved. Those are
not used by the release workflow.

Run the **Verify Release Secrets** workflow (`workflow_dispatch`) before the first release
and whenever a secret is rotated. It checks that the four secrets are present, that the
Central Portal accepts the token for the `sh.oso` namespace, and that the GPG key
imports, is not expired and can sign.

## The first release

The repository starts at `0.0.1-SNAPSHOT` and release-please would propose `0.0.2`. To
start the public series at `0.1.0`:

1. Run Verify Release Secrets and, once, the Contract Tests workflow against the PDI; keep
   the run as evidence.
2. Land a commit on `main` with a `Release-As` footer:

   ```text
   chore: prepare first public release

   Release-As: 0.1.0
   ```

   release-please rewrites its open release PR to `chore(main): release 0.1.0`.
3. Merge that PR. The tag `v0.1.0` and the GitHub Release are created, and the release
   workflow runs.
4. After it succeeds, merge the follow-up `chore(main): release 0.1.1-SNAPSHOT` PR,
   confirm the docs deploy and the sidebar version badge, and smoke-test the
   [quickstart](../getting-started/quickstart.md) with the released ZIPs.

Later releases need only step 3: merge the release PR that release-please keeps open.

## What is published

To Maven Central under `sh.oso`:

| Artefact | Contents |
|---|---|
| `kafka-connect-servicenow-parent` | The parent POM |
| `snow-core` | jar, sources, javadoc, and the `tests` classifier jar with the fake ServiceNow |
| `connect-servicenow-source` | jar, sources, javadoc |
| `connect-servicenow-sink` | jar, sources, javadoc |

`e2e-tests` is never published. Attached to the GitHub Release:

```text
connect-servicenow-source-<version>-kafka-connect-plugin.zip
connect-servicenow-source-<version>-kafka-connect-plugin.zip.sha256
connect-servicenow-sink-<version>-kafka-connect-plugin.zip
connect-servicenow-sink-<version>-kafka-connect-plugin.zip.sha256
kafka-connect-servicenow-<version>-bom.json
```

## Manual deploy

For a one-off local deploy with the same profile and settings:

```bash
export CENTRAL_USERNAME=... CENTRAL_TOKEN=... MAVEN_GPG_PASSPHRASE=...
git checkout v0.1.0
mvn clean deploy -s release/m2-settings.xml -Prelease -DskipTests
```

`release/m2-settings.xml` maps the environment variables onto the `central` server id
and the GPG passphrase. The `release` profile adds the sources, javadoc, GPG and
CycloneDX plugins and the Central publishing plugin with `autoPublish` and
`waitUntil=published`, so the command returns only when the deployment is visible.

## Recovery

- **The `validate` job failed.** Nothing was published. Fix the cause on `main` (usually a
  module POM version), let release-please open a new release PR, and delete the failed
  tag and GitHub Release if they were created.
- **The `publish` job failed before or during the Central upload.** Re-run the job from
  the Actions tab, or trigger the Release workflow manually (`workflow_dispatch`) with the
  version and tag. The Central deployment name is `${artifactId}-${version}`, so a repeated
  upload of the same version is idempotent: a deployment that was validated but not
  published is replaced, and one that is already published is reported as such rather than
  duplicated.
- **Central succeeded but attaching assets failed.** Re-run only the `publish` job; the
  deploy step will report the version as already published and the checksum, SBOM and
  attach steps will run again. Assets on the GitHub Release are overwritten by name.
- **A bad release reached Central.** Central artefacts cannot be deleted. Ship a patch
  release with the fix and note the withdrawn version in `CHANGELOG.md`.

## Maven coordinates

```xml
<dependency>
  <groupId>sh.oso</groupId>
  <artifactId>snow-core</artifactId>
  <version><!-- latest release --></version>
</dependency>
```

The connector artefacts (`connect-servicenow-source`, `connect-servicenow-sink`) are
published the same way; most users want the plugin ZIPs from GitHub Releases instead.
The fake ServiceNow for your own tests is the `snow-core` artefact with
`<classifier>tests</classifier>` and `<type>test-jar</type>`.
