#!/usr/bin/env python3
"""Post-cutover verification: prove the OSS source connector missed nothing.

Scans a ServiceNow table through the REST Table API for every row whose timestamp
field falls inside the cutover window, compares that set against a dump of the Kafka
topic, and writes a machine-readable evidence report (counts, SHA-256 digests of both
sys_id sets, the parameters used) that you can attach to a change ticket.

ServiceNow side (read-only, Table API GET):
  --instance-url https://acme.service-now.com --table incident
  --username U --password P        Basic auth  (or SNOW_USERNAME / SNOW_PASSWORD env)
  --bearer-token TOKEN             OAuth bearer token (or SNOW_TOKEN env)
  --since "YYYY-MM-DD HH:MM:SS"    window start, UTC (the OSS start.timestamp you used)
  --until "YYYY-MM-DD HH:MM:SS"    window end, UTC (default: now; take the dump after it)
  --query "active=true"            optional base filter (the connector's snow.table.<alias>.query)

Kafka side:
  --topic-dump FILE   TSV, one line per record: sys_id<TAB>sys_updated_on[<TAB>sys_mod_count]
                      Produce it with kafka-console-consumer + jq (see README.md):
                        kafka-console-consumer.sh --bootstrap-server B --topic servicenow.incident \\
                          --from-beginning --timeout-ms 10000 \\
                        | jq -r '(.payload // .) | [.sys_id, .sys_updated_on, .sys_mod_count] | @tsv' \\
                        > dump.tsv

Example:
  ./verify_cutover.py --table incident --instance-url https://acme.service-now.com \\
      --username "$SNOW_USERNAME" --password "$SNOW_PASSWORD" \\
      --since "2026-09-29 08:55:00" --until "2026-09-29 09:30:00" \\
      --topic-dump dump.tsv --evidence evidence-incident.json

Exit codes: 0 = PASS (no missing rows), 1 = FAIL (loss found), 2 = usage error,
3 = ServiceNow request failure. Credentials are never printed or written to the report.
"""

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
SYS_ID_RE = re.compile(r"^[0-9a-f]{32}$")
TABLE_RE = re.compile(r"^[a-z0-9_]+$")
RETRYABLE = {408, 425, 429, 500, 502, 503, 504}


class RequestFailure(Exception):
    pass


# --- pure comparison (unit-tested by selftest.py) --------------------------------------

def normalise_ts(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    value = value.replace("T", " ")
    if value.endswith("Z"):
        value = value[:-1]
    return value[:19] if len(value) > 19 and value[19] in ".+" else value


def to_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return -1


def compare(instance_rows, dump_rows, since=None, until=None):
    """Compare Table API rows against a topic dump.

    instance_rows: iterable of (sys_id, timestamp, sys_mod_count); one entry per row.
    dump_rows:     iterable of (sys_id, timestamp, sys_mod_count); one entry per Kafka record,
                   timestamp and sys_mod_count may be None.
    since/until:   optional window bounds; dump rows with a timestamp outside the window are
                   counted as out_of_window and ignored, not reported as extra.

    Returns a dict with missing (instance rows absent from the dump), stale_version (present but
    the dump's newest version is older than the instance's), extra (dump rows not in the instance
    scan), duplicates (sys_id -> record count for count > 1), counts and SHA-256 digests.
    """
    instance = {}
    for sys_id, ts, mod in instance_rows:
        if not sys_id:
            continue
        instance[sys_id] = (normalise_ts(ts) or "", to_int(mod))

    dump_lines = 0
    out_of_window = 0
    counts = {}
    newest = {}
    for sys_id, ts, mod in dump_rows:
        dump_lines += 1
        if not sys_id:
            continue
        ts = normalise_ts(ts)
        if ts and ((since and ts < since) or (until and ts > until)):
            out_of_window += 1
            continue
        counts[sys_id] = counts.get(sys_id, 0) + 1
        candidate = (ts or "", to_int(mod))
        if sys_id not in newest or candidate > newest[sys_id]:
            newest[sys_id] = candidate

    missing = sorted(s for s in instance if s not in counts)
    stale = []
    for sys_id, (ts, mod) in instance.items():
        seen = newest.get(sys_id)
        if seen is None or not ts or not seen[0]:
            continue
        if seen[0] < ts or (seen[0] == ts and mod >= 0 and seen[1] >= 0 and seen[1] < mod):
            stale.append(sys_id)
    stale.sort()
    extra = sorted(s for s in counts if s not in instance)
    duplicates = {s: c for s, c in sorted(counts.items()) if c > 1}

    return {
        "instance_rows": len(instance),
        "dump_lines": dump_lines,
        "dump_unique": len(counts),
        "out_of_window": out_of_window,
        "missing": missing,
        "stale_version": stale,
        "extra": extra,
        "duplicates": duplicates,
        "duplicate_records": sum(c - 1 for c in duplicates.values()),
        "instance_sha256": digest(instance),
        "dump_sha256": digest(counts),
        "loss": len(missing) + len(stale),
    }


def digest(sys_ids):
    return hashlib.sha256("\n".join(sorted(sys_ids)).encode("utf-8")).hexdigest()


# --- inputs ---------------------------------------------------------------------------

def load_dump(path):
    """Reads sys_id<TAB>timestamp[<TAB>sys_mod_count] lines. Returns (rows, skipped_line_count)."""
    rows, skipped = [], 0
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line.strip() or line.startswith("#"):
                continue
            parts = line.split("\t")
            sys_id = parts[0].strip().strip('"')
            if sys_id.lower() in ("null", "sys_id"):
                continue
            if not SYS_ID_RE.match(sys_id):
                skipped += 1
                continue
            ts = parts[1].strip().strip('"') if len(parts) > 1 else None
            mod = parts[2].strip().strip('"') if len(parts) > 2 else None
            if ts in ("", "null"):
                ts = None
            if mod in ("", "null"):
                mod = None
            rows.append((sys_id, ts, mod))
    return rows, skipped


def build_query(base_query, ts_field, since, until):
    parts = [base_query] if base_query else []
    parts.append(f"{ts_field}>={since}")
    if until:
        parts.append(f"{ts_field}<={until}")
    parts.append(f"ORDERBY{ts_field}")
    parts.append("ORDERBYsys_id")
    return "^".join(parts)


def fetch_rows(instance_url, table, auth_header, query, ts_field, page_size, timeout, max_attempts,
               log=lambda *_: None):
    base = instance_url.rstrip("/")
    headers = {"Authorization": auth_header, "Accept": "application/json",
               "User-Agent": "kafka-connect-servicenow verify_cutover"}
    rows, offset, pages = [], 0, 0
    while True:
        params = {
            "sysparm_query": query,
            "sysparm_fields": f"sys_id,{ts_field},sys_mod_count",
            "sysparm_limit": str(page_size),
            "sysparm_offset": str(offset),
            "sysparm_no_count": "true",
            "sysparm_suppress_pagination_header": "true",
            "sysparm_display_value": "false",
            "sysparm_exclude_reference_link": "true",
        }
        url = f"{base}/api/now/table/{table}?{urllib.parse.urlencode(params)}"
        body = get_json(url, headers, timeout, max_attempts)
        page = body.get("result") or []
        for r in page:
            rows.append((r.get("sys_id"), r.get(ts_field), r.get("sys_mod_count")))
        pages += 1
        log(f"page {pages}: {len(page)} rows (total {len(rows)})")
        if len(page) < page_size:
            return rows, pages
        offset += len(page)


def get_json(url, headers, timeout, max_attempts):
    attempt = 0
    while True:
        attempt += 1
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE and attempt < max_attempts:
                retry_after = exc.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 30)
                time.sleep(delay)
                continue
            detail = ""
            try:
                detail = exc.read(500).decode("utf-8", "replace")
            except Exception:  # noqa: BLE001 - best effort only
                pass
            raise RequestFailure(f"HTTP {exc.code} from {redact(url)}: {detail}".strip()) from None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt < max_attempts:
                time.sleep(min(2 ** attempt, 30))
                continue
            raise RequestFailure(f"request to {redact(url)} failed: {exc}") from None


def redact(url):
    parsed = urllib.parse.urlsplit(url)
    return f"{parsed.scheme}://{parsed.hostname}{parsed.path}"


def auth_header_from(args):
    token = args.bearer_token or os.environ.get("SNOW_TOKEN")
    username = args.username or os.environ.get("SNOW_USERNAME")
    password = args.password or os.environ.get("SNOW_PASSWORD")
    if token:
        return "Bearer " + token, "bearer"
    if username and password:
        raw = f"{username}:{password}".encode("utf-8")
        return "Basic " + base64.b64encode(raw).decode("ascii"), "basic"
    return None, None


def parse_timestamp(value, flag):
    value = normalise_ts(value)
    if not value or not TIMESTAMP_RE.match(value):
        raise SystemExit(f"error: {flag} must be 'YYYY-MM-DD HH:MM:SS' (UTC), got {value!r}")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table", required=True, help="ServiceNow table or view name")
    parser.add_argument("--instance-url", required=True, help="https://<instance>.service-now.com")
    parser.add_argument("--username", help="Basic auth user (or SNOW_USERNAME)")
    parser.add_argument("--password", help="Basic auth password (or SNOW_PASSWORD, preferred)")
    parser.add_argument("--bearer-token", help="OAuth access token (or SNOW_TOKEN)")
    parser.add_argument("--since", required=True, help="window start 'YYYY-MM-DD HH:MM:SS' (UTC)")
    parser.add_argument("--until", help="window end 'YYYY-MM-DD HH:MM:SS' (UTC); default: now")
    parser.add_argument("--query", default="", help="base encoded query, e.g. the connector's snow.table.<alias>.query")
    parser.add_argument("--timestamp-field", default="sys_updated_on",
                        help="cursor field (default sys_updated_on; use sys_created_on if the connector does)")
    parser.add_argument("--topic-dump", required=True, help="TSV: sys_id<TAB>timestamp[<TAB>sys_mod_count]")
    parser.add_argument("--evidence", default="migration-evidence.json", help="evidence report path")
    parser.add_argument("--page-size", type=int, default=1000, help="sysparm_limit per page (default 1000)")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds")
    parser.add_argument("--max-attempts", type=int, default=5, help="retries per page on 429/5xx")
    parser.add_argument("--max-list", type=int, default=1000,
                        help="cap on sys_ids listed per category in the report (counts are always exact)")
    parser.add_argument("--quiet", action="store_true", help="only print the verdict")
    args = parser.parse_args(argv)

    if not TABLE_RE.match(args.table):
        parser.error("--table must match ^[a-z0-9_]+$")
    if not args.instance_url.lower().startswith(("https://", "http://")):
        parser.error("--instance-url must start with https:// (or http:// for the fake)")
    since = parse_timestamp(args.since, "--since")
    until = parse_timestamp(args.until, "--until") if args.until else \
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if until < since:
        parser.error("--until is before --since")
    auth_header, auth_kind = auth_header_from(args)
    if not auth_header:
        parser.error("provide --username and --password (or SNOW_USERNAME/SNOW_PASSWORD), or --bearer-token")

    log = (lambda *_: None) if args.quiet else (lambda msg: print(msg, file=sys.stderr))
    query = build_query(args.query, args.timestamp_field, since, until)
    log(f"instance : {redact(args.instance_url)} table={args.table} auth={auth_kind}")
    log(f"query    : {query}")

    dump_rows, skipped = load_dump(args.topic_dump)
    log(f"dump     : {len(dump_rows)} records from {args.topic_dump} ({skipped} unparsable line(s) skipped)")

    started = datetime.now(timezone.utc)
    try:
        instance_rows, pages = fetch_rows(args.instance_url, args.table, auth_header, query,
                                          args.timestamp_field, args.page_size, args.timeout,
                                          args.max_attempts, log)
    except RequestFailure as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 3

    result = compare(instance_rows, dump_rows, since=since, until=until)
    verdict = ("PASS: every row updated in the window reached Kafka" if result["loss"] == 0 else
               f"FAIL: {len(result['missing'])} row(s) never reached Kafka and "
               f"{len(result['stale_version'])} row(s) only reached Kafka in an older version")

    evidence = {
        "report": "confluent-cutover-verification",
        "schemaVersion": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "instanceUrl": redact(args.instance_url),
            "table": args.table,
            "timestampField": args.timestamp_field,
            "since": since,
            "until": until,
            "baseQuery": args.query,
            "effectiveQuery": query,
            "pageSize": args.page_size,
            "topicDump": os.path.basename(args.topic_dump),
            "auth": auth_kind,
        },
        "instance": {
            "rows": result["instance_rows"],
            "pages": pages,
            "scanSeconds": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
            "sysIdSetSha256": result["instance_sha256"],
        },
        "topic": {
            "records": result["dump_lines"],
            "uniqueSysIds": result["dump_unique"],
            "outOfWindow": result["out_of_window"],
            "unparsableLines": skipped,
            "sysIdSetSha256": result["dump_sha256"],
        },
        "missingCount": len(result["missing"]),
        "missing": result["missing"][:args.max_list],
        "staleVersionCount": len(result["stale_version"]),
        "staleVersion": result["stale_version"][:args.max_list],
        "extraCount": len(result["extra"]),
        "extra": result["extra"][:args.max_list],
        "duplicateSysIds": len(result["duplicates"]),
        "duplicateRecords": result["duplicate_records"],
        "duplicates": dict(list(result["duplicates"].items())[:args.max_list]),
        "verdict": verdict,
        "notes": [
            "duplicates are expected across an at-least-once cutover; consumers dedupe on (sys_id, sys_updated_on, sys_mod_count)",
            "extra rows usually come from records the dump captured before --since or from rows since deleted in ServiceNow",
            "the integration user must be in the UTC timezone for the encoded-query timestamps to line up",
        ],
    }
    with open(args.evidence, "w", encoding="utf-8") as f:
        json.dump(evidence, f, indent=2)
    print(f"{verdict}  ->  {args.evidence}")
    log(f"instance={result['instance_rows']} topic_unique={result['dump_unique']} "
        f"missing={len(result['missing'])} stale={len(result['stale_version'])} "
        f"extra={len(result['extra'])} duplicate_records={result['duplicate_records']}")
    return 1 if result["loss"] else 0


if __name__ == "__main__":
    sys.exit(main())
