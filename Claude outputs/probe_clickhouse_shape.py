#!/usr/bin/env python3
"""Probe the shape of OpenTelemetry data in ClickHouse.

Produces everything needed to write an otel_api_sync mapping.yaml against
real telemetry, without guessing attribute keys:

  1. every otel table and its schema,
  2. span-name / service-name inventory with counts,
  3. the full attribute-KEY inventory (SpanAttributes, ResourceAttributes,
     LogAttributes) with occurrence counts, distinct-value counts, and a
     shape-classified sample value per key,
  4. a full row dump of ONE candidate execution group (the trace with the
     most spans in the window), so grouping/derive expressions can be
     written against a real, complete group.

Values are REDACTED by default: numbers, booleans and timestamps are kept
verbatim (they are what mapping type-coercion decisions depend on), while
free-text strings are masked to shape + a short prefix. Run with --raw to
keep everything.

Usage:
    pip install clickhouse-connect
    python probe_clickhouse_shape.py --host my-ch-host --database otel \
        [--port 8123] [--username default] [--password ... | env CLICKHOUSE_PASSWORD] \
        [--days 7] [--span-name suite.run] [--group-attr ci.run_id] \
        [--limit 60] [--raw] [--out clickhouse_shape.json]

If you already know the attribute that identifies one suite execution,
pass --group-attr and the dump section will pick the newest complete
group by that key instead of by TraceId.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys

try:
    import clickhouse_connect
except ImportError:
    sys.exit("clickhouse-connect is required:  pip install clickhouse-connect")

NUMERIC_RE = re.compile(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$")
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")


def classify(value):
    """Shape label for a string value: what a mapping would coerce it to."""
    if value is None:
        return "null"
    text = str(value)
    if not text.strip():
        return "empty"
    if NUMERIC_RE.match(text.strip()):
        return "int" if "." not in text and "e" not in text.lower() else "float"
    if text.strip().lower() in ("true", "false", "pass", "fail", "passed", "failed"):
        return "boolish"
    if ISO_RE.match(text.strip()):
        return "datetime"
    if "," in text:
        return f"csv[{text.count(',') + 1}]"
    return f"str[{len(text)}]"


def redact(value, raw: bool):
    """Keep machine-shaped values; mask free text unless --raw."""
    if raw or value is None:
        return value
    text = str(value)
    shape = classify(text)
    if shape in ("int", "float", "boolish", "datetime", "empty", "null"):
        return value
    if shape.startswith("csv"):
        parts = text.split(",")
        return ",".join(redact(p, raw=False) if isinstance(redact(p, False), str) else str(p) for p in parts[:3]) + (",…" if len(parts) > 3 else "")
    prefix = text[:8]
    return f"<{shape}:{prefix}…>" if len(text) > 8 else text


def jsonable(value):
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return value


def query_rows(client, sql, params=None):
    result = client.query(sql, parameters=params or {})
    cols = result.column_names
    return [dict(zip(cols, row)) for row in result.result_rows]


def attr_inventory(client, database, table, column, where, params):
    """Key inventory of one Map(String,String) attribute column."""
    sql = f"""
        SELECT kv.1 AS key,
               count() AS occurrences,
               uniqExact(kv.2) AS distinct_values,
               any(kv.2) AS sample_value,
               min(kv.2) AS min_value,
               max(kv.2) AS max_value
        FROM (
            SELECT arrayJoin(arrayZip(mapKeys({column}), mapValues({column}))) AS kv
            FROM {database}.{table}
            WHERE {where}
        )
        GROUP BY key
        ORDER BY key
    """
    try:
        return query_rows(client, sql, params)
    except Exception as exc:  # column absent / not a Map — report, don't die
        return [{"error": str(exc).splitlines()[0]}]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("CLICKHOUSE_PORT", "8123")))
    ap.add_argument("--username", default=os.environ.get("CLICKHOUSE_USER", "default"))
    ap.add_argument("--password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    ap.add_argument("--database", default=os.environ.get("CLICKHOUSE_DATABASE", "otel"))
    ap.add_argument("--secure", action="store_true", help="TLS (https) connection")
    ap.add_argument("--days", type=int, default=7, help="lookback window for inventories")
    ap.add_argument("--span-name", default=None, help="restrict trace analysis to this SpanName")
    ap.add_argument("--group-attr", default=None,
                    help="SpanAttributes key identifying one suite execution; dump newest group by it")
    ap.add_argument("--limit", type=int, default=60, help="max rows in the group dump")
    ap.add_argument("--raw", action="store_true", help="do NOT redact string values")
    ap.add_argument("--out", default="clickhouse_shape.json")
    args = ap.parse_args()

    client = clickhouse_connect.get_client(
        host=args.host, port=args.port, username=args.username,
        password=args.password, database=args.database, secure=args.secure,
    )

    report = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "database": args.database,
        "window_days": args.days,
        "redacted": not args.raw,
        "tables": {},
    }
    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)
    params = {"since": since.replace(tzinfo=None)}

    # ---- 1. tables + schemas -------------------------------------------
    tables = [r["name"] for r in query_rows(
        client, "SELECT name FROM system.tables WHERE database = %(db)s ORDER BY name",
        {"db": args.database})]
    print(f"tables in {args.database}: {tables}")
    for table in tables:
        schema = query_rows(client, f"DESCRIBE TABLE {args.database}.{table}")
        report["tables"][table] = {
            "schema": [{"name": c["name"], "type": c["type"]} for c in schema],
        }

    # ---- 2 + 3. per-table inventories ----------------------------------
    attr_columns = ("SpanAttributes", "ResourceAttributes", "LogAttributes", "Attributes")
    for table, info in report["tables"].items():
        col_names = {c["name"] for c in info["schema"]}
        if "Timestamp" not in col_names and "TimeUnix" not in col_names:
            continue
        ts_col = "Timestamp" if "Timestamp" in col_names else "TimeUnix"
        where = f"{ts_col} >= %(since)s"
        try:
            info["row_count_in_window"] = query_rows(
                client, f"SELECT count() AS n, min({ts_col}) AS oldest, max({ts_col}) AS newest "
                        f"FROM {args.database}.{table} WHERE {where}", params)[0]
        except Exception as exc:
            info["row_count_in_window"] = {"error": str(exc).splitlines()[0]}
            continue

        if "SpanName" in col_names:
            info["span_names"] = query_rows(
                client, f"SELECT SpanName, ServiceName, count() AS n FROM {args.database}.{table} "
                        f"WHERE {where} GROUP BY SpanName, ServiceName ORDER BY n DESC LIMIT 50", params)

        info["attributes"] = {}
        for column in attr_columns:
            if column not in col_names:
                continue
            inventory = attr_inventory(client, args.database, table, column, where, params)
            for entry in inventory:
                if "sample_value" in entry:
                    entry["value_shape"] = classify(entry["sample_value"])
                    for field in ("sample_value", "min_value", "max_value"):
                        entry[field] = redact(entry[field], args.raw)
            info["attributes"][column] = inventory

    # ---- 4. dump one candidate execution group -------------------------
    trace_table = next((t for t in report["tables"] if "SpanName" in
                        {c["name"] for c in report["tables"][t]["schema"]}), None)
    if trace_table:
        where = "Timestamp >= %(since)s"
        if args.span_name:
            where += " AND SpanName = %(span)s"
            params["span"] = args.span_name

        if args.group_attr:
            params["gattr"] = args.group_attr
            candidates = query_rows(
                client, f"SELECT SpanAttributes[%(gattr)s] AS gkey, count() AS n, max(Timestamp) AS newest "
                        f"FROM {args.database}.{trace_table} WHERE {where} AND SpanAttributes[%(gattr)s] != '' "
                        f"GROUP BY gkey ORDER BY newest DESC LIMIT 1", params)
            group_filter, gdesc = ("SpanAttributes[%(gattr)s] = %(gkey)s",
                                   f"{args.group_attr}={candidates[0]['gkey']}") if candidates else (None, None)
            if candidates:
                params["gkey"] = candidates[0]["gkey"]
        else:
            candidates = query_rows(
                client, f"SELECT TraceId, count() AS n, max(Timestamp) AS newest "
                        f"FROM {args.database}.{trace_table} WHERE {where} "
                        f"GROUP BY TraceId ORDER BY n DESC, newest DESC LIMIT 1", params)
            group_filter, gdesc = ("TraceId = %(gkey)s",
                                   f"TraceId={candidates[0]['TraceId']}") if candidates else (None, None)
            if candidates:
                params["gkey"] = candidates[0]["TraceId"]

        if group_filter:
            rows = query_rows(
                client, f"SELECT * FROM {args.database}.{trace_table} WHERE {where} AND {group_filter} "
                        f"ORDER BY Timestamp LIMIT {int(args.limit)}", params)
            for row in rows:
                for key, value in list(row.items()):
                    if isinstance(value, dict):
                        row[key] = {k: redact(v, args.raw) for k, v in value.items()}
                    else:
                        row[key] = jsonable(redact(value, args.raw) if key not in
                                            ("Timestamp", "TraceId", "SpanId", "SpanName",
                                             "ServiceName", "Duration") else jsonable(value))
            report["sample_group"] = {"table": trace_table, "selected_by": gdesc,
                                      "row_count": len(rows), "rows": rows}
            print(f"sample group: {gdesc} ({len(rows)} rows)")
        else:
            report["sample_group"] = {"note": "no rows matched the window/filters"}

    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, default=str)
    print(f"\nwrote {args.out}")
    print("Review it for anything sensitive (redaction is on by default), then send it back.")


if __name__ == "__main__":
    main()
