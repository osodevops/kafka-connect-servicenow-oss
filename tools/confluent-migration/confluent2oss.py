#!/usr/bin/env python3
"""Translate a Confluent ServiceNow connector config into the OSS equivalent.

Reads a Kafka Connect connector JSON (either the {"name","config"} envelope returned by
the Connect REST API or a bare config map), detects which Confluent ServiceNow connector
it is, and emits the equivalent configuration for the open-source sh.oso connectors
together with a migration report listing every property that was translated, set,
dropped or needs manual attention.

Supported Confluent connectors (connector.class):
  io.confluent.connect.servicenow.ServiceNowSourceConnector   Platform source
  ServiceNowSource                                            Cloud source (legacy)
  ServiceNowSourceV2                                          Cloud source V2
  io.confluent.connect.servicenow.ServiceNowSinkConnector     Platform sink
  ServiceNowSink                                              Cloud sink

Examples:
  ./confluent2oss.py old-connector.json                      # translated JSON to stdout
  ./confluent2oss.py old-connector.json -o new.json          # write file, report on stderr
  curl -s http://connect:8083/connectors/NAME | ./confluent2oss.py -
  ./confluent2oss.py old.json --start-timestamp "2026-09-29 08:55:00"   # cutover start
  ./confluent2oss.py old.json --format properties -o new.properties     # standalone worker
  ./confluent2oss.py old-sink.json --recommended-sink-defaults          # PATCH + sys_id

Exit codes:
  0  fully translated; the output can be POSTed to the Connect REST API as is
  2  one or more properties need a manual decision; they are printed on stderr and
     listed under "_migration_notes" next to "config" (remove that key before POSTing,
     e.g. jq 'del(._migration_notes)')
  1  usage or input error

No dependencies beyond Python 3.9+.
"""

import argparse
import json
import re
import sys

SOURCE_CLASS = "sh.oso.servicenow.source.ServiceNowSourceConnector"
SINK_CLASS = "sh.oso.servicenow.sink.ServiceNowSinkConnector"

# Confluent connector class -> (target class, kind)
CLASS_MAP = {
    "io.confluent.connect.servicenow.ServiceNowSourceConnector": (SOURCE_CLASS, "platform-source"),
    "ServiceNowSource": (SOURCE_CLASS, "cloud-source-legacy"),
    "ServiceNowSourceV2": (SOURCE_CLASS, "cloud-source-v2"),
    "io.confluent.connect.servicenow.ServiceNowSinkConnector": (SINK_CLASS, "platform-sink"),
    "ServiceNowSink": (SINK_CLASS, "cloud-sink"),
}
SOURCE_KINDS = {"platform-source", "cloud-source-legacy", "cloud-source-v2"}
SINK_KINDS = {"platform-sink", "cloud-sink"}

# Connection, auth, TLS and retry keys shared by every Confluent variant (Confluent -> OSS).
# Several spellings exist across Platform, Cloud legacy and Cloud V2; all are accepted.
CONNECTION_MAP = {
    "servicenow.url": "snow.url",
    "servicenow.username": "snow.auth.username",
    "servicenow.user": "snow.auth.username",
    "connection.user": "snow.auth.username",
    "servicenow.password": "snow.auth.password",
    "connection.password": "snow.auth.password",
    "oauth2.token.url": "snow.oauth.token.url",
    "oauth2.client.id": "snow.oauth.client.id",
    "oauth2.client.secret": "snow.oauth.client.secret",
    "connection.timeout.ms": "snow.http.connect.timeout.ms",
    "proxy.url": "snow.http.proxy.url",
    "retry.max.times": "snow.retry.max.attempts",
    "max.retries": "snow.retry.max.attempts",
    "retry.backoff.ms": "snow.retry.initial.backoff.ms",
    "servicenow.ssl.keystore.path": "snow.tls.keystore.path",
    "servicenow.ssl.keystore.location": "snow.tls.keystore.path",
    "servicenow.ssl.keystorefile": "snow.tls.keystore.path",
    "servicenow.ssl.keystore.password": "snow.tls.keystore.password",
    "servicenow.ssl.truststore.path": "snow.tls.truststore.path",
    "servicenow.ssl.truststore.location": "snow.tls.truststore.path",
    "servicenow.ssl.truststorefile": "snow.tls.truststore.path",
    "servicenow.ssl.truststore.password": "snow.tls.truststore.password",
}

# Platform / Cloud legacy single-table source keys -> per-alias suffix (alias t1).
SINGLE_TABLE_MAP = {
    "servicenow.table": "name",
    "kafka.topic": "topic",
    "batch.max.rows": "batch.size",
}

# Cloud Source V2 table{i}.<suffix> -> snow.table.t{i}.<suffix>
TABLE_SUFFIX_MAP = {
    "name": "name",
    "topic": "topic",
    "batch.size": "batch.size",
    "start.timestamp": "start.timestamp",
    "timestamp.field": "timestamp.field",
    "allowlisted.fields": "fields",
    "request.interval.ms": "poll.interval.ms",
    "display.value": "display.value",
    "exclude.reference.link": "exclude.reference.link",
    "query.category": "query.category",
    "query.domain": "query.domain",
}
TABLE_SUFFIX_DROPPED = {
    "servicenow.ssl.key.password": "snow-core unlocks the private key with snow.tls.keystore.password; re-key the keystore if the two passwords differ",
    "count.records": "OSS never runs a count query (sysparm_no_count=true)",
    "suppress.pagination.header": "OSS always suppresses the Link header",
    "request.parameters.separator": "OSS builds its own query string",
}

# Properties with no OSS equivalent that are safe to drop.
DROPPED = {
    "confluent.license": "no licence required (Apache-2.0)",
    "retry.backoff.policy": "OSS uses full-jitter exponential backoff and honours Retry-After",
    "retry.on.status.codes": "OSS classifies retryable statuses itself (429/408/425/5xx)",
    "servicenow.ssl.enabled": "TLS is used whenever snow.url is https://",
    "servicenow.ssl.protocol": "OSS uses the JVM default TLS protocol (TLSv1.3 on JDK 17+)",
    "oauth2.client.auth.mode": "OSS posts client credentials form-encoded to the token endpoint",
    "kafka.auth.mode": "Confluent Cloud platform setting",
    "kafka.api.key": "Confluent Cloud platform setting",
    "kafka.api.secret": "Confluent Cloud platform setting",
    "kafka.service.account.id": "Confluent Cloud platform setting",
    "schema.context.name": "Confluent Cloud Schema Registry setting; configure the converter instead",
    "sr.service.account.id": "Confluent Cloud Schema Registry setting",
    "auto.restart.on.user.error": "Confluent Cloud platform setting",
    "csfle.onFailure": "CSFLE is not supported by the OSS suite",
    "auto.register.schemas": "set value.converter.auto.register.schemas on the converter if needed",
    "use.latest.version": "set value.converter.use.latest.version on the converter if needed",
    "reporter.result.topic.replication.factor": "OSS does not create reporter topics; create them beforehand",
    "reporter.result.topic.partitions": "OSS does not create reporter topics; create them beforehand",
    "reporter.error.topic.replication.factor": "OSS does not create reporter topics; create them beforehand",
    "reporter.error.topic.partitions": "OSS does not create reporter topics; create them beforehand",
}
DROPPED_PREFIXES = {
    "confluent.topic": "Confluent licensing topic - not needed",
    "reporter.admin.": "OSS does not create reporter topics, so no admin client is configured",
    "reporter.result.topic.key.format": "OSS reporter records are always JSON",
    "reporter.result.topic.value.format": "OSS reporter records are always JSON",
    "reporter.error.topic.key.format": "OSS reporter records are always JSON",
    "reporter.error.topic.value.format": "OSS reporter records are always JSON",
}

# Standard Kafka Connect framework keys copied verbatim.
PASSTHROUGH_PREFIXES = (
    "tasks.max", "topics", "topics.regex", "key.converter", "value.converter", "header.converter",
    "errors.", "transforms", "predicates", "config.action.reload", "producer.override.",
    "consumer.override.", "topic.creation.", "exactly.once.support", "transaction.boundary",
    "offsets.storage.topic",
)

# Output ordering: keys are grouped by these prefixes, in this order, then everything else.
OUTPUT_ORDER = (
    "connector.class", "name", "tasks.max", "topics", "snow.url", "snow.auth.type", "snow.auth.",
    "snow.oauth.grant.type", "snow.oauth.",
    "snow.tables", "snow.table.", "snow.source.", "snow.sink.", "behavior.", "snow.http.",
    "snow.tls.", "snow.retry.",
)

TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TABLE_KEY_RE = re.compile(r"^table(\d+)\.(.+)$")


class Report:
    """Collects migration notes; MANUAL notes decide the exit code."""

    def __init__(self):
        self.notes = []

    def add(self, level, text):
        self.notes.append((level, text))

    def mapped(self, old, new):
        self.add("mapped", f"{old} -> {new}")

    def set(self, key, value, why=""):
        self.add("set", f"{key}={value}" + (f" ({why})" if why else ""))

    def dropped(self, key, why):
        self.add("dropped", f"{key} ({why})")

    def skipped(self, key, why):
        self.add("skipped", f"{key} ({why})")

    def kept(self, key):
        self.add("kept", f"{key} (framework property)")

    def review(self, text):
        self.add("REVIEW", text)

    def manual(self, text):
        self.add("MANUAL", text)

    @property
    def manual_notes(self):
        return [t for level, t in self.notes if level == "MANUAL"]

    @property
    def review_notes(self):
        return [t for level, t in self.notes if level == "REVIEW"]

    def lines(self):
        return [f"{level:<8} {text}" for level, text in self.notes]


class Ctx:
    def __init__(self, config, kind, target_class, original_name, new_name, options):
        self.config = config
        self.kind = kind
        self.out = {"connector.class": target_class}
        self.report = Report()
        self.original_name = original_name
        self.new_name = new_name
        self.options = options
        self.tables = {}          # alias -> {suffix: value}
        self.table_count = None   # from tables.num
        self.request_timeouts = []
        self.reporter_topics_seen = False

    @property
    def is_source(self):
        return self.kind in SOURCE_KINDS

    @property
    def is_sink(self):
        return self.kind in SINK_KINDS

    def table(self, alias):
        return self.tables.setdefault(alias, {})


def detect(config):
    src_class = str(config.get("connector.class", ""))
    target = CLASS_MAP.get(src_class)
    if target is None:
        simple = src_class.split(".")[-1]
        target = CLASS_MAP.get(simple)
    if target is None:
        if "tables.num" in config or "table1.name" in config:
            target = (SOURCE_CLASS, "cloud-source-v2")
        elif simple.endswith("SinkConnector"):
            target = (SINK_CLASS, "platform-sink")
        elif simple.endswith("SourceConnector"):
            target = (SOURCE_CLASS, "platform-source")
    if target is None:
        raise SystemExit(f"error: unrecognised Confluent ServiceNow connector class: {src_class!r}")
    return src_class, target[0], target[1]


def as_str(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def normalise_timestamp(value):
    """Confluent accepts YYYY-MM-DD (Platform) or 'YYYY-MM-DD HH:MM:SS' (V2); OSS wants the latter."""
    value = value.strip()
    if DATE_RE.match(value):
        return value + " 00:00:00", True
    if TIMESTAMP_RE.match(value):
        return value, True
    iso = value.replace("T", " ").rstrip("Z")
    if TIMESTAMP_RE.match(iso):
        return iso, True
    return value, False


# --- handlers -------------------------------------------------------------------------

def handle_connection(key, value, ctx):
    out, rep = ctx.out, ctx.report
    if key == "auth.type":
        mode = value.strip().upper()
        if mode == "BASIC":
            out["snow.auth.type"] = "basic"
            rep.add("value", "auth.type: BASIC -> snow.auth.type=basic")
        elif mode == "OAUTH2":
            out["snow.auth.type"] = "oauth2"
            out["snow.oauth.grant.type"] = "client_credentials"
            rep.add("value", "auth.type: OAUTH2 -> snow.auth.type=oauth2, snow.oauth.grant.type=client_credentials")
        else:
            rep.manual(f"auth.type={value!r} is not BASIC or OAUTH2; set snow.auth.type by hand")
        return True
    if key in ("read.timeout.ms", "write.timeout.ms"):
        try:
            ctx.request_timeouts.append((key, int(value)))
        except ValueError:
            rep.manual(f"{key}={value!r} is not an integer; set snow.http.request.timeout.ms by hand")
        return True
    if key == "oauth2.client.scope":
        if value.strip().lower() in ("", "any"):
            rep.dropped(key, "Confluent's placeholder default; OSS sends no scope unless snow.oauth.scope is set")
        else:
            out["snow.oauth.scope"] = value
            rep.mapped(key, "snow.oauth.scope")
        return True
    if key == "oauth2.token.property":
        if value.strip() in ("", "access_token"):
            rep.dropped(key, "OSS reads the standard access_token property")
        else:
            rep.manual(f"oauth2.token.property={value!r}: OSS reads access_token from the token "
                       "response; a custom property is not supported")
        return True
    if key == "oauth2.client.headers":
        if value.strip():
            rep.manual(f"oauth2.client.headers={value!r}: custom token-request headers are not supported; "
                       "ServiceNow's /oauth_token.do needs none")
        else:
            rep.skipped(key, "empty")
        return True
    if key in CONNECTION_MAP:
        if value.strip() == "" and not key.endswith("password"):
            rep.skipped(key, "empty")
            return True
        if key.endswith("password") and value == "":
            rep.skipped(key, "empty")
            return True
        new = CONNECTION_MAP[key]
        if new == "snow.url":
            value = value.rstrip("/")
        out[new] = value
        rep.mapped(key, new)
        if new.startswith("snow.tls.") and key.endswith(("file", "location")):
            rep.review(f"{key} mapped to {new}; check the path is readable on the OSS worker")
        return True
    return False


def handle_source(key, value, ctx):
    out, rep = ctx.out, ctx.report
    if key in SINGLE_TABLE_MAP:
        ctx.table("t1")[SINGLE_TABLE_MAP[key]] = value
        rep.mapped(key, f"snow.table.t1.{SINGLE_TABLE_MAP[key]}")
        return True
    if key == "servicenow.since":
        if value.strip() == "":
            rep.skipped(key, "empty; Confluent defaults to launch day, OSS defaults to 1970-01-01 (full backfill)")
            return True
        ts, ok = normalise_timestamp(value)
        if ok:
            ctx.table("t1")["start.timestamp"] = ts
            rep.mapped(key, f"snow.table.t1.start.timestamp={ts}")
        else:
            rep.manual(f"servicenow.since={value!r} is not YYYY-MM-DD; set snow.table.t1.start.timestamp by hand")
        return True
    if key == "poll.interval.s":
        try:
            ms = int(float(value) * 1000)
        except ValueError:
            rep.manual(f"poll.interval.s={value!r} is not numeric; set snow.table.t1.poll.interval.ms by hand")
            return True
        ctx.table("t1")["poll.interval.ms"] = str(ms)
        rep.mapped(key, f"snow.table.t1.poll.interval.ms={ms} (seconds x 1000)")
        return True
    if key == "servicenow.view.variable.prefix":
        prefix = value.strip()
        if prefix:
            ctx.table("t1")["timestamp.field"] = f"{prefix}sys_updated_on"
            ctx.table("t1")["sys.id.field"] = f"{prefix}sys_id"
            rep.mapped(key, f"snow.table.t1.timestamp.field={prefix}sys_updated_on, "
                            f"snow.table.t1.sys.id.field={prefix}sys_id")
        else:
            rep.skipped(key, "empty")
        return True
    if key == "tables.num":
        try:
            ctx.table_count = int(value)
            rep.add("value", f"tables.num={value} -> snow.tables will list t1..t{value}")
        except ValueError:
            rep.manual(f"tables.num={value!r} is not an integer")
        return True
    m = TABLE_KEY_RE.match(key)
    if m:
        index, suffix = int(m.group(1)), m.group(2)
        alias = f"t{index}"
        if suffix in TABLE_SUFFIX_DROPPED:
            rep.dropped(key, TABLE_SUFFIX_DROPPED[suffix])
        elif suffix == "pagination.query":
            if value.strip() == "":
                rep.skipped(key, "empty")
            elif "${offset}" in value.lower() or "orderby" in value.lower():
                rep.manual(f"{key}={value!r}: contains ${{offset}} and/or ORDERBY. The OSS keyset cursor adds "
                           f"its own timestamp/sys_id predicates and ordering; rewrite it as a plain base "
                           f"filter in snow.table.{alias}.query (or leave it unset)")
            else:
                ctx.table(alias)["query"] = value
                rep.mapped(key, f"snow.table.{alias}.query")
        elif suffix == "pagination.query.field":
            if value.strip() == "":
                rep.skipped(key, "empty")
            else:
                rep.manual(f"{key}={value!r}: custom cursor fields have no OSS equivalent; the cursor is always "
                           f"(snow.table.{alias}.timestamp.field, sys_id). Use timestamp.field=sys_created_on "
                           f"if you polled by creation time")
        elif suffix in TABLE_SUFFIX_MAP:
            if value.strip() == "" and suffix not in ("name", "topic"):
                rep.skipped(key, "empty")
            else:
                new_suffix = TABLE_SUFFIX_MAP[suffix]
                if suffix == "start.timestamp":
                    value, ok = normalise_timestamp(value)
                    if not ok:
                        rep.manual(f"{key}={value!r} is not 'YYYY-MM-DD HH:MM:SS'")
                        return True
                ctx.table(alias)[new_suffix] = value
                rep.mapped(key, f"snow.table.{alias}.{new_suffix}")
        else:
            rep.manual(f"{key}: unknown per-table property; check snow.table.{alias}.* in the OSS reference")
        return True
    if key == "behavior.on.error":
        mode = value.strip().upper()
        target = {"FAIL": "fail", "IGNORE": "skip"}.get(mode)
        if target:
            out["snow.source.bad.row.behavior"] = target
            rep.mapped(key, f"snow.source.bad.row.behavior={target}")
            rep.review("behavior.on.error covered record-conversion errors on Confluent; "
                       "snow.source.bad.row.behavior is the closest OSS policy (fail|skip)")
        else:
            rep.manual(f"behavior.on.error={value!r}: expected FAIL or IGNORE")
        return True
    if key == "reporter.error.topic.name":
        rep.dropped(key, "the OSS source has no reporter; use errors.tolerance=all + errors.deadletterqueue.topic.name")
        return True
    if key == "output.data.format":
        return handle_data_format(key, value, ctx)
    return False


def handle_sink(key, value, ctx):
    out, rep = ctx.out, ctx.report
    if key == "servicenow.table":
        out["snow.sink.table"] = value
        out["snow.sink.routing.mode"] = "fixed"
        rep.mapped(key, "snow.sink.table (+ snow.sink.routing.mode=fixed)")
        return True
    if key == "reporter.result.topic.name":
        out["snow.sink.reporter.success.topic"] = value
        rep.mapped(key, "snow.sink.reporter.success.topic")
        ctx.reporter_topics_seen = True
        return True
    if key == "reporter.error.topic.name":
        out["snow.sink.reporter.error.topic"] = value
        rep.mapped(key, "snow.sink.reporter.error.topic")
        ctx.reporter_topics_seen = True
        return True
    if key == "reporter.bootstrap.servers":
        out["snow.sink.reporter.bootstrap.servers"] = value
        rep.mapped(key, "snow.sink.reporter.bootstrap.servers")
        return True
    if key.startswith("reporter.producer."):
        new = "snow.sink.reporter.producer." + key[len("reporter.producer."):]
        out[new] = value
        rep.mapped(key, new)
        return True
    if key == "behavior.on.error":
        mode = value.strip().lower()
        if mode in ("fail", "log", "ignore"):
            out["behavior.on.api.errors"] = mode
            rep.mapped(key, f"behavior.on.api.errors={mode}")
        else:
            rep.manual(f"behavior.on.error={value!r}: expected fail, log or ignore")
        return True
    if key in ("max.poll.interval.ms", "max.poll.records"):
        new = "consumer.override." + key
        out[new] = value
        rep.mapped(key, new)
        rep.review(f"{new} needs connector.client.config.override.policy=All on the worker")
        return True
    if key == "input.data.format":
        return handle_data_format(key, value, ctx)
    return False


def handle_data_format(key, value, ctx):
    fmt = value.strip().upper()
    out, rep = ctx.out, ctx.report
    if fmt == "JSON":
        out.setdefault("key.converter", "org.apache.kafka.connect.storage.StringConverter")
        out.setdefault("value.converter", "org.apache.kafka.connect.json.JsonConverter")
        out.setdefault("value.converter.schemas.enable", "false")
        rep.mapped(key, "key.converter=StringConverter, value.converter=JsonConverter (schemas.enable=false)")
    elif fmt in ("AVRO", "JSON_SR", "PROTOBUF"):
        rep.manual(f"{key}={fmt}: configure key.converter/value.converter for your Schema Registry "
                   f"(e.g. io.confluent.connect.avro.AvroConverter + value.converter.schema.registry.url); "
                   f"Confluent's converters are not bundled with the OSS plugin")
    else:
        rep.manual(f"{key}={value!r}: unknown data format; set key.converter/value.converter by hand")
    return True


def handle_dropped(key, value, ctx):
    rep = ctx.report
    if key == "csfle.enabled":
        if value.strip().lower() == "true":
            rep.manual("csfle.enabled=true: client-side field level encryption is not supported by the OSS suite")
        else:
            rep.dropped(key, "CSFLE disabled")
        return True
    if key in DROPPED:
        rep.dropped(key, DROPPED[key])
        return True
    for prefix, why in DROPPED_PREFIXES.items():
        if key.startswith(prefix):
            rep.dropped(key, why)
            return True
    return False


# --- finalisation ---------------------------------------------------------------------

def finalize(ctx):
    out, rep, opts = ctx.out, ctx.report, ctx.options

    if ctx.request_timeouts:
        chosen = max(ctx.request_timeouts, key=lambda kv: kv[1])
        out["snow.http.request.timeout.ms"] = str(chosen[1])
        rep.mapped(" + ".join(k for k, _ in ctx.request_timeouts),
                   f"snow.http.request.timeout.ms={chosen[1]} (largest of the Confluent read/write timeouts)")

    if "snow.auth.type" not in out:
        if "snow.oauth.client.id" in out or "snow.oauth.client.secret" in out:
            out["snow.auth.type"] = "oauth2"
            out["snow.oauth.grant.type"] = "client_credentials"
            rep.set("snow.auth.type", "oauth2", "oauth2.* keys present")
            rep.set("snow.oauth.grant.type", "client_credentials")
        elif "snow.auth.username" in out or "snow.auth.password" in out:
            out["snow.auth.type"] = "basic"
            rep.set("snow.auth.type", "basic", "Confluent Basic auth")
        else:
            rep.manual("no credentials found: set snow.auth.type and snow.auth.username/password "
                       "or snow.oauth.client.id/secret")
    if out.get("snow.auth.type") == "oauth2" and "snow.auth.username" in out:
        rep.review("both OAuth client credentials and a username/password were present; OSS uses the "
                   "client_credentials grant and ignores the username unless snow.oauth.grant.type=password")
    if "snow.url" not in out:
        rep.manual("servicenow.url missing: snow.url is required")

    if ctx.is_source:
        finalize_source(ctx)
    if ctx.is_sink:
        finalize_sink(ctx)

    for key, value in list(out.items()):
        if isinstance(value, str) and "${connector}" in value:
            resolved = value.replace("${connector}", ctx.original_name)
            out[key] = resolved
            rep.add("value", f"{key}: ${{connector}} -> {ctx.original_name} (keeps the existing topic name)")
        if isinstance(value, str) and "io.confluent." in value and not key.startswith("snow."):
            rep.review(f"{key}={value} references a Confluent class; make sure it is installed on the OSS "
                       f"worker or replace it")


def finalize_source(ctx):
    out, rep, opts = ctx.out, ctx.report, ctx.options
    if not ctx.tables:
        rep.manual("no table found: set snow.tables and snow.table.<alias>.name/topic")
        return
    aliases = sorted(ctx.tables, key=lambda a: int(a[1:]))
    if ctx.table_count is not None:
        limited = [a for a in aliases if int(a[1:]) <= ctx.table_count]
        for extra in aliases:
            if extra not in limited:
                rep.dropped(f"{extra}.*", f"index beyond tables.num={ctx.table_count}")
        aliases = limited
    kept = []
    for alias in aliases:
        spec = ctx.tables[alias]
        if not spec.get("name"):
            rep.dropped(f"snow.table.{alias}.*", "no table name configured")
            continue
        kept.append(alias)
        if not spec.get("topic"):
            rep.manual(f"snow.table.{alias}.topic is required (no kafka.topic / table{alias[1:]}.topic found)")
        ts_field = spec.get("timestamp.field", "sys_updated_on")
        if spec.get("fields"):
            fields = [f.strip() for f in spec["fields"].split(",") if f.strip()]
            for required in (ts_field, "sys_id"):
                if required not in fields:
                    fields.append(required)
                    rep.review(f"snow.table.{alias}.fields: added required cursor field {required}")
            spec["fields"] = ",".join(fields)
        if opts.get("start_timestamp"):
            spec["start.timestamp"] = opts["start_timestamp"]
            rep.set(f"snow.table.{alias}.start.timestamp", opts["start_timestamp"], "--start-timestamp")
    if not kept:
        rep.manual("no table with a name found: set snow.tables and snow.table.<alias>.name/topic")
        return
    out["snow.tables"] = ",".join(kept)
    rep.set("snow.tables", out["snow.tables"])
    order = ("name", "topic", "start.timestamp", "timestamp.field", "sys.id.field", "query", "fields",
             "display.value", "exclude.reference.link", "query.domain", "query.category", "batch.size",
             "poll.interval.ms")
    for alias in kept:
        spec = ctx.tables[alias]
        for suffix in order:
            if suffix in spec:
                out[f"snow.table.{alias}.{suffix}"] = spec[suffix]
        for suffix, value in spec.items():
            if suffix not in order:
                out[f"snow.table.{alias}.{suffix}"] = value
    if not opts.get("start_timestamp"):
        rep.review("cutover: before starting the OSS connector set snow.table.<alias>.start.timestamp to the "
                   "Confluent connector's last successful poll time minus a margin (or rerun with "
                   "--start-timestamp \"YYYY-MM-DD HH:MM:SS\"); see the README runbook")


def finalize_sink(ctx):
    out, rep, opts = ctx.out, ctx.report, ctx.options
    if "snow.sink.table" not in out:
        rep.manual("servicenow.table missing: set snow.sink.table (or a topic_map/header routing mode)")
    out.setdefault("snow.sink.operation.mode", "key_value")
    rep.set("snow.sink.operation.mode", "key_value", "Confluent infers the operation from key/value presence")
    if opts.get("recommended_sink_defaults"):
        out.setdefault("snow.sink.update.method", "PATCH")
        out.setdefault("snow.sink.sys.id.key.field", "sys_id")
        rep.set("snow.sink.update.method", "PATCH", "--recommended-sink-defaults")
        rep.set("snow.sink.sys.id.key.field", "sys_id", "--recommended-sink-defaults")
        rep.review("PATCH sends only the fields present in the record and sys_id is read from a key field "
                   "named sys_id; producers that send Confluent-style keys ({\"sysId\": ...}) must be updated")
    else:
        out.setdefault("snow.sink.update.method", "PUT")
        out.setdefault("snow.sink.sys.id.key.field", "sysId")
        rep.set("snow.sink.update.method", "PUT", "exact Confluent parity: full-record update")
        rep.set("snow.sink.sys.id.key.field", "sysId", "exact Confluent parity: structured key field")
        rep.review("Confluent parity settings emitted (snow.sink.update.method=PUT, "
                   "snow.sink.sys.id.key.field=sysId). Once producers are under your control prefer "
                   "snow.sink.update.method=PATCH and snow.sink.sys.id.key.field=sys_id "
                   "(rerun with --recommended-sink-defaults)")
    if ctx.reporter_topics_seen and "snow.sink.reporter.bootstrap.servers" not in out:
        rep.manual("reporter topics are configured but reporter.bootstrap.servers was not; set "
                   "snow.sink.reporter.bootstrap.servers (the worker's bootstrap servers are not visible "
                   "to tasks) or remove snow.sink.reporter.*.topic")


def ordered(out):
    result = {}
    for prefix in OUTPUT_ORDER:
        for key in list(out):
            if key not in result and (key == prefix or key.startswith(prefix)):
                result[key] = out[key]
    for key, value in out.items():
        if key not in result:
            result[key] = value
    return result


# --- public API -----------------------------------------------------------------------

def translate(config, name=None, start_timestamp=None, recommended_sink_defaults=False,
              name_suffix="-oss"):
    """Translate one Confluent config map. Returns (oss_config, report)."""
    config = {k: as_str(v) for k, v in dict(config).items()}
    original_name = name or config.get("name") or "servicenow-connector"
    new_name = original_name + name_suffix
    if start_timestamp:
        start_timestamp, ok = normalise_timestamp(start_timestamp)
        if not ok:
            raise SystemExit("error: --start-timestamp must be 'YYYY-MM-DD HH:MM:SS' (UTC)")
    src_class, target_class, kind = detect(config)
    ctx = Ctx(config, kind, target_class, original_name, new_name,
              {"start_timestamp": start_timestamp, "recommended_sink_defaults": recommended_sink_defaults})
    rep = ctx.report
    rep.add("class", f"{src_class} -> {target_class} ({kind})")

    for key, value in config.items():
        if key == "connector.class":
            continue
        if key == "name":
            ctx.out["name"] = new_name
            continue
        if handle_connection(key, value, ctx):
            continue
        if ctx.is_source and handle_source(key, value, ctx):
            continue
        if ctx.is_sink and handle_sink(key, value, ctx):
            continue
        if handle_dropped(key, value, ctx):
            continue
        if key.startswith(PASSTHROUGH_PREFIXES):
            ctx.out[key] = value
            rep.kept(key)
        else:
            rep.manual(f"{key}={value!r}: not recognised; check the OSS configuration reference")

    finalize(ctx)
    return ordered(ctx.out), rep


def envelope(name, oss_config, report):
    doc = {"name": name, "config": oss_config}
    if report.manual_notes:
        doc["_migration_notes"] = report.manual_notes
    return doc


def render_properties(name, oss_config, report):
    lines = [f"# translated by confluent2oss.py for connector {name}"]
    for note in report.manual_notes:
        lines.append(f"# MANUAL: {note}")
    for note in report.review_notes:
        lines.append(f"# REVIEW: {note}")
    lines.append(f"name={name}")
    for key, value in oss_config.items():
        if key == "name":
            continue
        escaped = str(value).replace("\\", "\\\\").replace("\n", "\\n")
        lines.append(f"{key}={escaped}")
    return "\n".join(lines) + "\n"


def report_text(report):
    manual = report.manual_notes
    tail = (f"{len(manual)} propert{'y needs' if len(manual) == 1 else 'ies need'} manual attention."
            if manual else "no manual follow-ups detected.")
    return "\n".join(["", "=" * 72, "migration report", "=" * 72, *report.lines(), "", tail, ""])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", help="Confluent connector JSON file, or - for stdin")
    parser.add_argument("-o", "--output", help="write the translated config here (default: stdout)")
    parser.add_argument("--format", choices=("json", "properties"), default="json",
                        help="json: Connect REST envelope (default); properties: standalone worker file")
    parser.add_argument("--name", help="connector name for the output (default: input name + suffix)")
    parser.add_argument("--name-suffix", default="-oss",
                        help="suffix appended to the input connector name (default: -oss)")
    parser.add_argument("--start-timestamp", metavar="'YYYY-MM-DD HH:MM:SS'",
                        help="override every table's start.timestamp with the cutover point (UTC)")
    parser.add_argument("--recommended-sink-defaults", action="store_true",
                        help="emit snow.sink.update.method=PATCH and snow.sink.sys.id.key.field=sys_id "
                             "instead of the exact Confluent parity values (PUT, sysId)")
    parser.add_argument("--quiet", action="store_true", help="suppress the migration report on stderr")
    args = parser.parse_args(argv)

    try:
        raw = sys.stdin.read() if args.input == "-" else open(args.input, encoding="utf-8").read()
        doc = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: cannot read {args.input}: {exc}", file=sys.stderr)
        return 1
    if not isinstance(doc, dict):
        print("error: input must be a JSON object", file=sys.stderr)
        return 1
    config = doc.get("config", doc)
    input_name = doc.get("name") or config.get("name")

    oss_config, report = translate(config, name=input_name, start_timestamp=args.start_timestamp,
                                   recommended_sink_defaults=args.recommended_sink_defaults,
                                   name_suffix="" if args.name else args.name_suffix)
    new_name = args.name or ((input_name or "servicenow-connector") + args.name_suffix)
    if "name" in oss_config:
        oss_config["name"] = new_name

    if args.format == "properties":
        rendered = render_properties(new_name, oss_config, report)
    else:
        rendered = json.dumps(envelope(new_name, oss_config, report), indent=2) + "\n"

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(rendered)
    else:
        sys.stdout.write(rendered)
    if not args.quiet:
        print(report_text(report), file=sys.stderr)
        if args.output:
            print(f"wrote {args.output}", file=sys.stderr)
    return 2 if report.manual_notes else 0


if __name__ == "__main__":
    sys.exit(main())
