#!/usr/bin/env python
"""Does `mint-role` actually enforce, against a real engine?

Unit tests prove the DDL is BUILT correctly. They cannot prove ClickHouse or
PostgreSQL accept it, and they certainly cannot prove the database ENFORCES it —
which is the entire claim mint-role makes over the S3 path. `_mint_s3` learned
this the expensive way: its verification passed on credentials that happened to
be in the operator's shell.

So this mints for real and then tries to break out:

    python scripts/verify_adr0044_sql_minting.py clickhouse --host 127.0.0.1 --port 18123
    python scripts/verify_adr0044_sql_minting.py postgres   --host 127.0.0.1 --port 15432

Every refusal is paired with a POSITIVE CONTROL showing the admin CAN do the
same thing. Without that, "denied" is indistinguishable from a weak admin, a
missing table, or a typo — the same observation for opposite reasons.

It creates a scratch table, exercises the credential, and drops it.

────────────────────────────────────────────────────────────────────────────
PRE-REGISTERED PREDICTIONS

  P1. Both engines accept the DDL as written. Confidence: medium — VALID UNTIL,
      column GRANTs and row policies are all long-standing, but the exact
      spelling is untested against these versions.
  P2. The minted credential can SELECT its own table. High.
  P3. It CANNOT write. High — and the ClickHouse half is the one worth being
      wrong about, because readonly is a profile property and granting only
      SELECT does not imply it.
  P4. It CANNOT read a second table. High.
  P5. With a row filter, it sees a STRICT SUBSET of what admin sees — the
      claim that separates mint-role from mint-sts. Medium-high: this is where
      "policy created" and "policy enforced" can diverge, particularly on
      PostgreSQL where a table without RLS accepts a policy that does nothing.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dag_tools.domain_broker import sql_minting as sm  # noqa: E402

TABLE = "adr0044_probe"


def _p(label, ok, detail=""):
    mark = {True: "PASS", False: "FAIL", None: "INFO"}[ok]
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))


# ── ClickHouse ─────────────────────────────────────────────────────────────

def verify_clickhouse(args) -> int:
    import clickhouse_connect

    admin = clickhouse_connect.get_client(
        host=args.host, port=args.port,
        username=os.getenv("CH_USER", "default"),
        password=os.getenv("CH_PASSWORD", ""),
    )
    db = args.database
    admin.command(f"CREATE DATABASE IF NOT EXISTS {db}")
    admin.command(f"DROP TABLE IF EXISTS {db}.{TABLE}")
    admin.command(f"CREATE TABLE {db}.{TABLE} (id Int32, region String) ENGINE = MergeTree ORDER BY id")
    admin.command(f"INSERT INTO {db}.{TABLE} VALUES (1,'EMEA'),(2,'APAC'),(3,'EMEA')")
    admin.command(f"DROP TABLE IF EXISTS {db}.{TABLE}_other")
    admin.command(f"CREATE TABLE {db}.{TABLE}_other (id Int32) ENGINE = MergeTree ORDER BY id")
    admin.command(f"INSERT INTO {db}.{TABLE}_other VALUES (9)")

    scope = {"host": args.host, "port": str(args.port), "schema": db, "table": TABLE}
    print("\n1. minting (with a row filter, which is the interesting case)")
    creds = sm.mint_clickhouse(scope, "urn:probe", {}, {"row_filters": "region = 'EMEA'"})
    _p("engine accepted the DDL", True, f"user={creds['username']} expires={creds['expires_at']}")

    user = clickhouse_connect.get_client(
        host=args.host, port=args.port,
        username=creds["username"], password=creds["password"], database=db,
    )

    print("\n2. what the minted credential can do")
    admin_rows = admin.query(f"SELECT count() FROM {db}.{TABLE}").result_rows[0][0]
    try:
        seen = user.query(f"SELECT count() FROM {db}.{TABLE}").result_rows[0][0]
        _p("can read its own table", True, f"{seen} of {admin_rows} rows")
        _p("ROW POLICY IS ENFORCED", seen < admin_rows,
           f"admin sees {admin_rows}, minted sees {seen}"
           + ("" if seen < admin_rows else " — policy created but NOT enforced"))
    except Exception as exc:
        _p("can read its own table", False, str(exc)[:120])

    try:
        user.command(f"INSERT INTO {db}.{TABLE} VALUES (4,'EMEA')")
        _p("write is refused", False, "WROTE — readonly profile not pinned")
    except Exception as exc:
        _p("write is refused", True, type(exc).__name__)

    try:
        user.query(f"SELECT count() FROM {db}.{TABLE}_other")
        _p("second table is refused", False, "reached a table it was not granted")
    except Exception as exc:
        _p("second table is refused", True, type(exc).__name__)

    print("\n3. positive control (admin can do what the minted credential cannot)")
    admin.command(f"INSERT INTO {db}.{TABLE} VALUES (5,'APAC')")
    _p("admin CAN write", True)
    _p("admin CAN read the second table", True,
       str(admin.query(f"SELECT count() FROM {db}.{TABLE}_other").result_rows[0][0]))

    admin.command(f"DROP USER IF EXISTS {creds['username']}")
    admin.command(f"DROP TABLE IF EXISTS {db}.{TABLE}")
    admin.command(f"DROP TABLE IF EXISTS {db}.{TABLE}_other")
    print("\ncleaned up.")
    return 0


# ── PostgreSQL ─────────────────────────────────────────────────────────────

def verify_postgres(args) -> int:
    import psycopg2

    conn = psycopg2.connect(host=args.host, port=args.port,
                            user=os.getenv("PG_USER", "postgres"),
                            password=os.getenv("PG_PASSWORD", ""),
                            dbname=args.database)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}, {TABLE}_other")
        cur.execute(f"CREATE TABLE {TABLE} (id int, region text)")
        cur.execute(f"INSERT INTO {TABLE} VALUES (1,'EMEA'),(2,'APAC'),(3,'EMEA')")
        cur.execute(f"CREATE TABLE {TABLE}_other (id int)")
        cur.execute(f"INSERT INTO {TABLE}_other VALUES (9)")
        # RLS must be ON, or a policy is inert — mint_postgres refuses without it.
        cur.execute(f"ALTER TABLE {TABLE} ENABLE ROW LEVEL SECURITY")

    scope = {"host": args.host, "port": str(args.port), "schema": "public", "table": TABLE}
    print("\n1. minting (with a row filter)")
    creds = sm.mint_postgres(scope, "urn:probe", {"database": args.database},
                             {"row_filters": "region = 'EMEA'"})
    _p("engine accepted the DDL", True, f"role={creds['username']} expires={creds['expires_at']}")

    uconn = psycopg2.connect(host=args.host, port=args.port,
                             user=creds["username"], password=creds["password"],
                             dbname=args.database)
    uconn.autocommit = True

    print("\n2. what the minted credential can do")
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {TABLE}")
        admin_rows = cur.fetchone()[0]
    try:
        with uconn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {TABLE}")
            seen = cur.fetchone()[0]
        _p("can read its own table", True, f"{seen} of {admin_rows} rows")
        _p("ROW POLICY IS ENFORCED", seen < admin_rows,
           f"admin sees {admin_rows}, minted sees {seen}"
           + ("" if seen < admin_rows else " — policy created but NOT enforced"))
    except Exception as exc:
        _p("can read its own table", False, str(exc)[:120])

    try:
        with uconn.cursor() as cur:
            cur.execute(f"INSERT INTO {TABLE} VALUES (4,'EMEA')")
        _p("write is refused", False, "WROTE — only SELECT should be granted")
    except Exception as exc:
        _p("write is refused", True, type(exc).__name__)

    try:
        with uconn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {TABLE}_other")
        _p("second table is refused", False, "reached a table it was not granted")
    except Exception as exc:
        _p("second table is refused", True, type(exc).__name__)

    print("\n3. positive control")
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {TABLE} VALUES (5,'APAC')")
        cur.execute(f"SELECT count(*) FROM {TABLE}_other")
        _p("admin CAN write and read the second table", True, str(cur.fetchone()[0]))

    uconn.close()
    with conn.cursor() as cur:
        cur.execute(f"DROP OWNED BY {creds['username']}")
        cur.execute(f"DROP ROLE IF EXISTS {creds['username']}")
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}, {TABLE}_other")
    conn.close()
    print("\ncleaned up.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("engine", choices=["clickhouse", "postgres"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--database", default=None)
    args = ap.parse_args()
    if args.database is None:
        args.database = "iagent" if args.engine == "clickhouse" else "postgres"
    return verify_clickhouse(args) if args.engine == "clickhouse" else verify_postgres(args)


if __name__ == "__main__":
    raise SystemExit(main())
