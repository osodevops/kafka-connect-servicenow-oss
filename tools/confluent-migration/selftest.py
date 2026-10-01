#!/usr/bin/env python3
"""Self-test for the Confluent migration tooling. No network, no dependencies.

Run from anywhere:  python3 tools/confluent-migration/selftest.py

Checks:
  * confluent2oss translates every fixture in examples/ to the documented OSS keys,
    maps the connector classes, never lets a confluent.* key survive, and returns
    exit 2 exactly when a MANUAL follow-up exists (the ${offset} fixture);
  * the CLI works from a file, from stdin, in --format properties, with
    --start-timestamp and --recommended-sink-defaults;
  * verify_cutover.compare() reports loss, stale versions, extras, duplicates and
    out-of-window rows correctly on synthetic data, and the CLI pages a fake Table API
    served from localhost (with one 429 + Retry-After) and exits 0 / 1 / 3 as documented;
  * verify_cutover --instance-export compares against a saved Table API response with no
    instance URL or credentials, in both the {"result": [...]} and bare-list shapes.
"""

import http.server
import json
import os
import subprocess
import sys
import tempfile
import threading
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLES = os.path.join(HERE, "examples")
sys.path.insert(0, HERE)

import confluent2oss  # noqa: E402
import verify_cutover  # noqa: E402

FAILURES = []


def check(condition, message):
    if not condition:
        FAILURES.append(message)
        print(f"  FAIL  {message}")
    else:
        print(f"  ok    {message}")


def fixture(name):
    with open(os.path.join(EXAMPLES, name), encoding="utf-8") as f:
        doc = json.load(f)
    return doc["name"], doc["config"]


def run_cli(script, *args, stdin=None):
    proc = subprocess.run([sys.executable, os.path.join(HERE, script), *args],
                          input=stdin, capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def no_confluent_keys(cfg, label):
    leaked = [k for k in cfg if k.startswith("confluent.") or k.startswith("kafka.api") or k == "kafka.auth.mode"]
    check(not leaked, f"{label}: no Confluent licence/Cloud keys survive ({leaked or 'none'})")


# --- confluent2oss ---------------------------------------------------------------------

def test_platform_source():
    print("platform-source.json")
    name, cfg = fixture("platform-source.json")
    out, rep = confluent2oss.translate(cfg, name=name)
    expected = {
        "connector.class": "sh.oso.servicenow.source.ServiceNowSourceConnector",
        "snow.url": "https://acme.service-now.com",
        "snow.auth.type": "basic",
        "snow.auth.username": "kafka_integration",
        "snow.auth.password": "CHANGE_ME",
        "snow.tables": "t1",
        "snow.table.t1.name": "incident",
        "snow.table.t1.topic": "servicenow.incident",
        "snow.table.t1.start.timestamp": "2026-01-01 00:00:00",
        "snow.table.t1.batch.size": "5000",
        "snow.table.t1.poll.interval.ms": "20000",
        "snow.retry.max.attempts": "3",
        "snow.http.connect.timeout.ms": "50000",
        "snow.http.request.timeout.ms": "30000",
        "snow.http.proxy.url": "https://proxy.acme.internal:3128",
        "snow.tls.truststore.path": "/etc/pki/truststore.jks",
        "snow.tls.truststore.password": "CHANGE_ME",
        "tasks.max": "1",
        "value.converter.schemas.enable": "false",
        "topic.creation.default.partitions": "3",
    }
    for key, value in expected.items():
        check(out.get(key) == value, f"{key} = {value!r} (got {out.get(key)!r})")
    check("snow.table.t1.timestamp.field" not in out, "empty view prefix emits no timestamp.field")
    check("snow.table.t1.sys.id.field" not in out, "empty view prefix emits no sys.id.field")
    no_confluent_keys(out, "platform-source")
    check(not rep.manual_notes, f"no MANUAL notes ({rep.manual_notes})")
    check(any("start.timestamp" in n for n in rep.review_notes), "REVIEW note about cutover start.timestamp")

    # view prefix + explicit start timestamp override
    cfg2 = dict(cfg, **{"servicenow.view.variable.prefix": "u_"})
    out2, _ = confluent2oss.translate(cfg2, name=name, start_timestamp="2026-09-29 08:55:00")
    check(out2.get("snow.table.t1.timestamp.field") == "u_sys_updated_on", "view prefix -> timestamp.field=u_sys_updated_on")
    check(out2.get("snow.table.t1.sys.id.field") == "u_sys_id", "view prefix -> sys.id.field=u_sys_id")
    check(out2.get("snow.table.t1.start.timestamp") == "2026-09-29 08:55:00", "--start-timestamp overrides start.timestamp")


def test_cloud_source_legacy():
    print("cloud-source-legacy.json")
    name, cfg = fixture("cloud-source-legacy.json")
    out, rep = confluent2oss.translate(cfg, name=name)
    check(out["connector.class"] == confluent2oss.SOURCE_CLASS, "ServiceNowSource -> OSS source class")
    check(out.get("snow.url") == "https://acme.service-now.com", "trailing slash stripped from snow.url")
    check(out.get("snow.auth.username") == "kafka_integration", "servicenow.user -> snow.auth.username")
    check(out.get("snow.table.t1.poll.interval.ms") == "30000", "poll.interval.s=30 -> 30000 ms")
    check(out.get("snow.table.t1.batch.size") == "10000", "batch.max.rows -> batch.size")
    check(out.get("value.converter") == "org.apache.kafka.connect.json.JsonConverter", "output.data.format=JSON -> JsonConverter")
    check(out.get("value.converter.schemas.enable") == "false", "output.data.format=JSON -> schemas.enable=false")
    check(out.get("name") == name + "-oss", "config.name carries the -oss suffix")
    no_confluent_keys(out, "cloud-source-legacy")
    check(not rep.manual_notes, f"no MANUAL notes ({rep.manual_notes})")


def test_cloud_source_v2():
    print("cloud-source-v2.json")
    name, cfg = fixture("cloud-source-v2.json")
    out, rep = confluent2oss.translate(cfg, name=name)
    expected = {
        "connector.class": "sh.oso.servicenow.source.ServiceNowSourceConnector",
        "snow.auth.type": "oauth2",
        "snow.oauth.grant.type": "client_credentials",
        "snow.oauth.token.url": "https://acme.service-now.com/oauth_token.do",
        "snow.oauth.client.id": "CHANGE_ME",
        "snow.oauth.client.secret": "CHANGE_ME",
        "snow.tables": "t1,t2",
        "snow.table.t1.name": "incident",
        "snow.table.t1.topic": "servicenow.incident",
        "snow.table.t1.batch.size": "5000",
        "snow.table.t1.start.timestamp": "2026-01-01 00:00:00",
        "snow.table.t1.timestamp.field": "sys_updated_on",
        "snow.table.t1.query": "active=true^priority<=2",
        "snow.table.t1.fields": "sys_id,sys_updated_on,number,short_description,state",
        "snow.table.t1.poll.interval.ms": "2000",
        "snow.table.t1.display.value": "false",
        "snow.table.t1.exclude.reference.link": "true",
        "snow.table.t1.query.domain": "true",
        "snow.table.t2.name": "change_request",
        "snow.table.t2.topic": "servicenow.change_request",
        "snow.table.t2.batch.size": "20000",
        "snow.table.t2.timestamp.field": "sys_created_on",
        "snow.table.t2.poll.interval.ms": "5000",
        "snow.retry.max.attempts": "3",
        "snow.retry.initial.backoff.ms": "3000",
        "snow.source.bad.row.behavior": "fail",
        "tasks.max": "2",
        "errors.tolerance": "none",
    }
    for key, value in expected.items():
        check(out.get(key) == value, f"{key} = {value!r} (got {out.get(key)!r})")
    check("snow.table.t2.query" not in out, "${offset} pagination query is NOT emitted")
    check("snow.oauth.scope" not in out, "oauth2.client.scope=any is dropped")
    check("snow.table.t1.query.category" not in out, "empty query.category is skipped")
    for gone in ("retry.backoff.policy", "retry.on.status.codes", "reporter.error.topic.name",
                 "table1.count.records", "table1.suppress.pagination.header", "table1.request.parameters.separator"):
        check(gone not in out, f"{gone} dropped")
    no_confluent_keys(out, "cloud-source-v2")
    manual = "\n".join(rep.manual_notes)
    check("table2.pagination.query=" in manual and "${offset}" in manual, "MANUAL note for ${offset} pagination query")
    check("table2.pagination.query.field" in manual, "MANUAL note for pagination.query.field")
    check("output.data.format=JSON_SR" in manual, "MANUAL note for Schema Registry format")
    check(len(rep.manual_notes) == 3, f"exactly 3 MANUAL notes ({len(rep.manual_notes)})")

    # fields projection is completed with the cursor fields when a Confluent config omitted them
    cfg2 = dict(cfg, **{"table1.allowlisted.fields": "number,state"})
    out2, _ = confluent2oss.translate(cfg2, name=name)
    check(out2.get("snow.table.t1.fields") == "number,state,sys_updated_on,sys_id",
          "fields projection gains the cursor fields")


def test_platform_sink():
    print("platform-sink.json")
    name, cfg = fixture("platform-sink.json")
    out, rep = confluent2oss.translate(cfg, name=name)
    expected = {
        "connector.class": "sh.oso.servicenow.sink.ServiceNowSinkConnector",
        "topics": "incident-updates",
        "snow.url": "https://acme.service-now.com",
        "snow.auth.type": "basic",
        "snow.auth.username": "kafka_integration",
        "snow.auth.password": "CHANGE_ME",
        "snow.sink.table": "incident",
        "snow.sink.routing.mode": "fixed",
        "snow.sink.operation.mode": "key_value",
        "snow.sink.update.method": "PUT",
        "snow.sink.sys.id.key.field": "sysId",
        "snow.sink.reporter.bootstrap.servers": "kafka:9092",
        "snow.sink.reporter.success.topic": "servicenow-incident-sink-success",
        "snow.sink.reporter.error.topic": "servicenow-incident-sink-error",
        "snow.sink.reporter.producer.security.protocol": "SASL_SSL",
        "snow.sink.reporter.producer.sasl.mechanism": "PLAIN",
        "snow.tls.keystore.path": "/etc/pki/client.p12",
        "snow.tls.keystore.password": "CHANGE_ME",
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": "dlq-servicenow-incident-sink",
        "tasks.max": "2",
    }
    for key, value in expected.items():
        check(out.get(key) == value, f"{key} = {value!r} (got {out.get(key)!r})")
    check("snow.http.proxy.url" not in out, "empty proxy.url is skipped")
    check(not any(k.startswith("reporter.") for k in out), "no raw reporter.* key survives")
    check(not any("reporter.admin" in k for k in out), "reporter.admin.* dropped")
    no_confluent_keys(out, "platform-sink")
    check(not rep.manual_notes, f"no MANUAL notes ({rep.manual_notes})")
    check(any("PATCH" in n for n in rep.review_notes), "REVIEW note recommends PATCH + sys_id")

    out2, _ = confluent2oss.translate(cfg, name=name, recommended_sink_defaults=True)
    check(out2.get("snow.sink.update.method") == "PATCH", "--recommended-sink-defaults -> PATCH")
    check(out2.get("snow.sink.sys.id.key.field") == "sys_id", "--recommended-sink-defaults -> sys_id")


def test_cloud_sink():
    print("cloud-sink.json")
    name, cfg = fixture("cloud-sink.json")
    out, rep = confluent2oss.translate(cfg, name=name)
    expected = {
        "connector.class": "sh.oso.servicenow.sink.ServiceNowSinkConnector",
        "snow.sink.table": "incident",
        "snow.auth.type": "basic",
        "snow.http.connect.timeout.ms": "50000",
        "snow.retry.max.attempts": "3",
        "consumer.override.max.poll.interval.ms": "300000",
        "consumer.override.max.poll.records": "500",
        "errors.deadletterqueue.topic.name": "dlq-ServiceNowSink_0",
        "snow.sink.reporter.success.topic": "success-ServiceNowSink_0",
        "snow.sink.reporter.error.topic": "error-ServiceNowSink_0",
    }
    for key, value in expected.items():
        check(out.get(key) == value, f"{key} = {value!r} (got {out.get(key)!r})")
    no_confluent_keys(out, "cloud-sink")
    manual = "\n".join(rep.manual_notes)
    check("input.data.format=AVRO" in manual, "MANUAL note for AVRO converter")
    check("snow.sink.reporter.bootstrap.servers" in manual, "MANUAL note: reporter bootstrap servers missing")

    cfg2 = dict(cfg, **{"csfle.enabled": "true", "behavior.on.error": "LOG"})
    out2, rep2 = confluent2oss.translate(cfg2, name=name)
    check(out2.get("behavior.on.api.errors") == "log", "behavior.on.error=LOG -> behavior.on.api.errors=log")
    check(any("csfle" in n for n in rep2.manual_notes), "csfle.enabled=true is MANUAL")


def test_unknown_and_fallbacks():
    print("edge cases")
    out, rep = confluent2oss.translate({
        "connector.class": "io.confluent.connect.servicenow.ServiceNowSourceConnector",
        "servicenow.url": "https://x.service-now.com",
        "servicenow.username": "u", "servicenow.password": "p",
        "servicenow.table": "incident", "kafka.topic": "t",
        "servicenow.custom.thing": "1", "reporter.mystery": "2",
    })
    manual = "\n".join(rep.manual_notes)
    check("servicenow.custom.thing" in manual and "reporter.mystery" in manual, "unknown servicenow./reporter. keys are MANUAL")
    out, rep = confluent2oss.translate({"connector.class": "ServiceNowSourceV2", "servicenow.url": "https://x",
                                        "auth.type": "BASIC", "connection.user": "u", "connection.password": "p"})
    check(out.get("snow.auth.username") == "u", "connection.user (Cloud V2) -> snow.auth.username")
    check(any("no table" in n for n in rep.manual_notes), "source without tables is MANUAL")
    out, rep = confluent2oss.translate({"connector.class": "ServiceNowSink", "servicenow.url": "https://x",
                                        "servicenow.user": "u", "servicenow.password": "p", "servicenow.table": "incident",
                                        "oauth2.client.id": "id", "oauth2.client.secret": "s"})
    check(out.get("snow.auth.type") == "oauth2" and out.get("snow.oauth.grant.type") == "client_credentials",
          "oauth2.* keys without auth.type infer oauth2 + client_credentials")
    try:
        confluent2oss.translate({"connector.class": "io.confluent.something.Else"})
        check(False, "unknown connector class raises")
    except SystemExit:
        check(True, "unknown connector class raises")
    out, rep = confluent2oss.translate({
        "connector.class": "io.confluent.connect.servicenow.ServiceNowSourceConnector",
        "servicenow.url": "https://x.service-now.com", "servicenow.username": "u", "servicenow.password": "p",
        "servicenow.table": "incident", "kafka.topic": "t",
        "servicenow.ssl.keystore.path": "/k.jks", "servicenow.ssl.keystore.password": "kp",
        "servicenow.ssl.key.password": "other", "value.subject.name.strategy": "TopicNameStrategy",
        "key.subject.name.strategy": "TopicNameStrategy",
    })
    check(not rep.manual_notes, f"ssl.key.password and subject strategies are not MANUAL ({rep.manual_notes})")
    dropped = "\n".join(t for level, t in rep.notes if level == "dropped")
    check("servicenow.ssl.key.password" in dropped, "servicenow.ssl.key.password is DROPPED at the top level")
    check("value.subject.name.strategy" in dropped and "key.subject.name.strategy" in dropped,
          "Schema Registry subject strategies are DROPPED")
    check(not any(k.endswith("subject.name.strategy") for k in out), "subject strategies not emitted")
    out, rep = confluent2oss.translate({"connector.class": "ServiceNowSourceV2", "servicenow.url": "https://x",
                                        "auth.type": "OAUTH2", "oauth2.client.id": "id", "oauth2.client.secret": "s",
                                        "table1.name": "incident", "table1.topic": "t"})
    check(any("snow.oauth.grant.type=password" in n for n in rep.review_notes), "OAUTH2 gets a REVIEW note on the grant choice")
    check(not rep.manual_notes, "OAUTH2 alone is not MANUAL")


def test_cli():
    print("confluent2oss CLI")
    code, stdout, stderr = run_cli("confluent2oss.py", os.path.join(EXAMPLES, "platform-source.json"))
    doc = json.loads(stdout)
    check(code == 0, f"platform-source exits 0 (got {code})")
    check(doc["name"] == "servicenow-incidents-oss" and "_migration_notes" not in doc, "exit-0 envelope has no _migration_notes")
    check("migration report" in stderr, "report printed on stderr")

    code, stdout, stderr = run_cli("confluent2oss.py", os.path.join(EXAMPLES, "cloud-source-v2.json"))
    doc = json.loads(stdout)
    check(code == 2, f"cloud-source-v2 (${{offset}} query) exits 2 (got {code})")
    check(isinstance(doc.get("_migration_notes"), list) and len(doc["_migration_notes"]) == 3,
          "_migration_notes lists the MANUAL items")
    check("_migration_notes" not in doc["config"] and not any(k.startswith("_") for k in doc["config"]),
          "config map stays a valid Connect config")
    check("MANUAL" in stderr, "MANUAL items printed on stderr")

    code, stdout, _ = run_cli("confluent2oss.py", "-", "--quiet", stdin=json.dumps(fixture("platform-sink.json")[1]))
    check(code == 0 and json.loads(stdout)["config"]["snow.sink.table"] == "incident", "bare config map on stdin works")

    with tempfile.TemporaryDirectory() as tmp:
        target = os.path.join(tmp, "out.properties")
        code, stdout, stderr = run_cli("confluent2oss.py", os.path.join(EXAMPLES, "platform-sink.json"),
                                       "--format", "properties", "-o", target, "--name", "snow-sink")
        text = open(target, encoding="utf-8").read()
        check(code == 0 and stdout == "" and f"wrote {target}" in stderr, "-o writes the file and reports on stderr")
        check("name=snow-sink\n" in text and "snow.sink.table=incident\n" in text, "--format properties renders key=value")
        check("connector.class=sh.oso.servicenow.sink.ServiceNowSinkConnector" in text, "properties has connector.class")

    code, _, stderr = run_cli("confluent2oss.py", os.path.join(tmp, "missing.json"))
    check(code == 1 and "cannot read" in stderr, "missing input exits 1")
    code, _, _ = run_cli("confluent2oss.py", "--help")
    check(code == 0, "--help exits 0")


# --- verify_cutover -------------------------------------------------------------------

def sid(n):
    return f"{n:032x}"


def test_compare():
    print("verify_cutover.compare")
    instance = [
        (sid(1), "2026-09-29 09:00:01", "3"),
        (sid(2), "2026-09-29 09:00:02", "1"),
        (sid(3), "2026-09-29 09:00:03", "7"),   # missing from the dump
        (sid(4), "2026-09-29 09:00:04", "2"),   # dump only has an older version
        (sid(5), "2026-09-29 09:00:04", "5"),   # same second, dump has lower sys_mod_count
    ]
    dump = [
        (sid(1), "2026-09-29 09:00:01", "3"),
        (sid(1), "2026-09-29 09:00:01", "3"),   # duplicate delivery
        (sid(2), "2026-09-29 09:00:02", None),
        (sid(4), "2026-09-29 08:59:00", "1"),   # stale
        (sid(5), "2026-09-29 09:00:04", "4"),   # stale by sys_mod_count
        (sid(9), "2026-09-29 09:00:09", "1"),   # extra
        (sid(8), "2026-09-28 10:00:00", "1"),   # before the window: ignored
    ]
    r = verify_cutover.compare(instance, dump, since="2026-09-29 08:55:00", until="2026-09-29 09:30:00")
    check(r["missing"] == [sid(3)], f"missing detected ({r['missing']})")
    check(r["stale_version"] == [sid(4), sid(5)], f"stale versions detected ({r['stale_version']})")
    check(r["extra"] == [sid(9)], f"extra detected ({r['extra']})")
    check(r["duplicates"] == {sid(1): 2} and r["duplicate_records"] == 1, "duplicates counted")
    check(r["out_of_window"] == 1, "out-of-window dump row ignored")
    check(r["instance_rows"] == 5 and r["dump_lines"] == 7 and r["dump_unique"] == 5, "counts")
    check(r["loss"] == 3, f"loss = missing + stale ({r['loss']})")
    check(len(r["instance_sha256"]) == 64 and r["instance_sha256"] != r["dump_sha256"], "digests differ when sets differ")

    clean = verify_cutover.compare(instance, [(s, ts, m) for s, ts, m in instance] + [(sid(1), "2026-09-29 09:00:01", "3")])
    check(clean["loss"] == 0 and clean["instance_sha256"] == clean["dump_sha256"], "identical sets: zero loss, equal digests")
    check(verify_cutover.compare([], [])["loss"] == 0, "empty inputs: zero loss")
    check(verify_cutover.compare([(sid(1), "2026-09-29T09:00:01Z", 1)], [(sid(1), "2026-09-29 09:00:01", None)])["loss"] == 0,
          "ISO-8601 and space-separated timestamps compare equal")
    check(verify_cutover.build_query("active=true", "sys_updated_on", "2026-09-29 08:55:00", "2026-09-29 09:30:00") ==
          "active=true^sys_updated_on>=2026-09-29 08:55:00^sys_updated_on<=2026-09-29 09:30:00^ORDERBYsys_updated_on^ORDERBYsys_id",
          "encoded query shape")

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "dump.tsv")
        with open(path, "w", encoding="utf-8") as f:
            f.write("# comment\n")
            f.write(f"{sid(1)}\t2026-09-29 09:00:01\t3\n")
            f.write(f"{sid(2)}\t2026-09-29 09:00:02\n")
            f.write(f"{sid(3)}\n")
            f.write("null\t\n")
            f.write("not-a-sys-id\tx\ty\n")
            f.write("\n")
        rows, skipped = verify_cutover.load_dump(path)
        check(rows == [(sid(1), "2026-09-29 09:00:01", "3"), (sid(2), "2026-09-29 09:00:02", None), (sid(3), None, None)],
              f"dump parser handles 1-3 columns ({rows})")
        check(skipped == 1, f"dump parser counts unparsable lines ({skipped})")


class FakeTableApi(http.server.BaseHTTPRequestHandler):
    rows = []
    fail_status = None
    throttle_once = True
    seen_queries = []

    def log_message(self, *_):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        params = dict(urllib.parse.parse_qsl(parsed.query))
        FakeTableApi.seen_queries.append((self.headers.get("Authorization", ""), params))
        if FakeTableApi.fail_status:
            self.send_response(FakeTableApi.fail_status)
            self.end_headers()
            self.wfile.write(b'{"error":{"message":"nope"}}')
            return
        if FakeTableApi.throttle_once:
            FakeTableApi.throttle_once = False
            self.send_response(429)
            self.send_header("Retry-After", "0")
            self.end_headers()
            return
        if not parsed.path.endswith("/api/now/table/incident"):
            self.send_response(404)
            self.end_headers()
            return
        limit, offset = int(params["sysparm_limit"]), int(params["sysparm_offset"])
        page = FakeTableApi.rows[offset:offset + limit]
        body = json.dumps({"result": [{"sys_id": s, "sys_updated_on": ts, "sys_mod_count": m} for s, ts, m in page]})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))


def test_verify_cli():
    print("verify_cutover CLI against a localhost fake")
    FakeTableApi.rows = [(sid(i), f"2026-09-29 09:{i // 60:02d}:{i % 60:02d}", "1") for i in range(1, 2501)]
    server = http.server.HTTPServer(("127.0.0.1", 0), FakeTableApi)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}"
    env = dict(os.environ, SNOW_USERNAME="connect", SNOW_PASSWORD="secret")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            dump = os.path.join(tmp, "dump.tsv")
            evidence = os.path.join(tmp, "evidence.json")
            with open(dump, "w", encoding="utf-8") as f:
                for s, ts, m in FakeTableApi.rows:
                    f.write(f"{s}\t{ts}\t{m}\n")
                f.write(f"{sid(1)}\t{FakeTableApi.rows[0][1]}\t1\n")   # one duplicate
            common = ["--table", "incident", "--instance-url", url, "--since", "2026-09-29 08:55:00",
                      "--until", "2026-09-29 09:59:59", "--topic-dump", dump, "--evidence", evidence,
                      "--page-size", "1000", "--quiet"]
            proc = subprocess.run([sys.executable, os.path.join(HERE, "verify_cutover.py"), *common],
                                  capture_output=True, text=True, env=env)
            check(proc.returncode == 0, f"complete dump -> exit 0 (got {proc.returncode}: {proc.stderr.strip()})")
            report = json.load(open(evidence, encoding="utf-8"))
            check(report["instance"]["rows"] == 2500 and report["instance"]["pages"] == 3, "paged 2500 rows in 3 pages")
            check(report["duplicateRecords"] == 1 and report["missingCount"] == 0, "duplicate counted, nothing missing")
            check(report["instance"]["sysIdSetSha256"] == report["topic"]["sysIdSetSha256"], "digests match")
            check("secret" not in json.dumps(report) and "connect" not in json.dumps(report["parameters"]),
                  "credentials absent from the evidence")
            check(report["parameters"]["auth"] == "basic", "auth kind recorded")
            auth, params = FakeTableApi.seen_queries[-1]
            check(auth.startswith("Basic "), "Basic Authorization header sent")
            check(params["sysparm_fields"] == "sys_id,sys_updated_on,sys_mod_count" and params["sysparm_no_count"] == "true"
                  and params["sysparm_query"].endswith("^ORDERBYsys_updated_on^ORDERBYsys_id"), "Table API parameters")
            check(any(p["sysparm_offset"] == "2000" for _, p in FakeTableApi.seen_queries), "sysparm_offset paging advanced")
            check(not FakeTableApi.throttle_once, "429 + Retry-After was retried")

            with open(dump, "w", encoding="utf-8") as f:
                for s, ts, m in FakeTableApi.rows[:-2]:
                    f.write(f"{s}\t{ts}\t{m}\n")
            proc = subprocess.run([sys.executable, os.path.join(HERE, "verify_cutover.py"), *common],
                                  capture_output=True, text=True, env=env)
            report = json.load(open(evidence, encoding="utf-8"))
            check(proc.returncode == 1 and report["missingCount"] == 2, f"two rows missing -> exit 1 (got {proc.returncode})")
            check(report["missing"] == [sid(2499), sid(2500)], "missing sys_ids listed")

            FakeTableApi.fail_status = 403
            proc = subprocess.run([sys.executable, os.path.join(HERE, "verify_cutover.py"), *common],
                                  capture_output=True, text=True, env=env)
            check(proc.returncode == 3 and "HTTP 403" in proc.stderr, f"403 -> exit 3 (got {proc.returncode})")
            FakeTableApi.fail_status = None

            proc = subprocess.run([sys.executable, os.path.join(HERE, "verify_cutover.py"), *common],
                                  capture_output=True, text=True, env={k: v for k, v in os.environ.items() if not k.startswith("SNOW_")})
            check(proc.returncode == 2, f"no credentials -> usage error 2 (got {proc.returncode})")
    finally:
        server.shutdown()


def test_verify_export():
    print("verify_cutover CLI against an offline --instance-export")
    rows = [(sid(i), f"2026-09-29 09:{i // 60:02d}:{i % 60:02d}", "1") for i in range(1, 301)]
    env = {k: v for k, v in os.environ.items() if not k.startswith("SNOW_")}
    with tempfile.TemporaryDirectory() as tmp:
        dump = os.path.join(tmp, "dump.tsv")
        evidence = os.path.join(tmp, "evidence.json")
        export = os.path.join(tmp, "export.json")
        with open(dump, "w", encoding="utf-8") as f:
            for s_, ts, m in rows:
                f.write(f"{s_}\t{ts}\t{m}\n")
        # Table API envelope, with one row in display-value shape and one junk row
        result = [{"sys_id": s_, "sys_updated_on": ts, "sys_mod_count": m} for s_, ts, m in rows[:-1]]
        last = rows[-1]
        result.append({"sys_id": {"value": last[0], "display_value": last[0]},
                       "sys_updated_on": {"value": last[1], "display_value": last[1]},
                       "sys_mod_count": {"value": last[2], "display_value": last[2]}})
        result.append({"sys_id": "not-a-sys-id"})
        with open(export, "w", encoding="utf-8") as f:
            json.dump({"result": result}, f)
        common = ["--table", "incident", "--since", "2026-09-29 08:55:00", "--until", "2026-09-29 09:59:59",
                  "--topic-dump", dump, "--evidence", evidence, "--quiet"]

        proc = subprocess.run([sys.executable, os.path.join(HERE, "verify_cutover.py"), *common,
                               "--instance-export", export], capture_output=True, text=True, env=env)
        check(proc.returncode == 0, f"export + complete dump -> exit 0 without URL or credentials (got {proc.returncode}: {proc.stderr.strip()})")
        report = json.load(open(evidence, encoding="utf-8"))
        check(report["instance"]["source"] == "export" and report["instance"]["rows"] == 300, "evidence records the export source and row count")
        check(report["instance"]["unparsableRows"] == 1 and report["instance"]["pages"] == 0, "junk export row counted, no pages fetched")
        check(report["parameters"]["instanceUrl"] is None and report["parameters"]["instanceExport"] == "export.json",
              "evidence names the export and no instance URL")
        check(report["parameters"]["auth"] == "none", "no auth recorded for an offline run")

        with open(export, "w", encoding="utf-8") as f:
            json.dump(result[:-1], f)                       # bare list shape
        with open(dump, "w", encoding="utf-8") as f:
            for s_, ts, m in rows[:-2]:
                f.write(f"{s_}\t{ts}\t{m}\n")
        proc = subprocess.run([sys.executable, os.path.join(HERE, "verify_cutover.py"), *common,
                               "--instance-export", export], capture_output=True, text=True, env=env)
        report = json.load(open(evidence, encoding="utf-8"))
        check(proc.returncode == 1 and report["missing"] == [sid(299), sid(300)],
              f"bare-list export, two rows missing -> exit 1 (got {proc.returncode})")

        with open(export, "w", encoding="utf-8") as f:
            f.write('{"error": "nope"}')
        proc = subprocess.run([sys.executable, os.path.join(HERE, "verify_cutover.py"), *common,
                               "--instance-export", export], capture_output=True, text=True, env=env)
        check(proc.returncode == 3 and "result" in proc.stderr, f"export without a result array -> exit 3 (got {proc.returncode})")

        proc = subprocess.run([sys.executable, os.path.join(HERE, "verify_cutover.py"), *common],
                              capture_output=True, text=True, env=env)
        check(proc.returncode == 2 and "--instance-export" in proc.stderr, f"neither URL nor export -> usage error 2 (got {proc.returncode})")


def main():
    for test in (test_platform_source, test_cloud_source_legacy, test_cloud_source_v2, test_platform_sink,
                 test_cloud_sink, test_unknown_and_fallbacks, test_cli, test_compare, test_verify_cli,
                 test_verify_export):
        test()
    print()
    if FAILURES:
        print(f"selftest FAILED: {len(FAILURES)} check(s)")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("selftest passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
