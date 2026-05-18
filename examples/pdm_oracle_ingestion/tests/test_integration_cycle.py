"""End-to-end PDM Oracle ingestion cycle test.

Requires the docker-compose stack to be running:

    cd examples/pdm_oracle_ingestion
    docker compose up -d oracle postgres restate

The test runs everything else in-process from the host:
  * real dlt with the sql_database source pulling from Oracle into Postgres
  * real oracle_ack.mark_as_processed handler against real Oracle
  * verifies row counts in Postgres after each cycle
  * verifies processed_flag / sync_date / PDM_STATS state in Oracle

Skips with a clear message if Oracle on localhost:1525 isn't reachable.
"""
import asyncio
import os
import socket
import sys
import time
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import pytest

ORACLE_HOST = os.environ.get("PDM_ORACLE_HOST", "localhost")
ORACLE_PORT = int(os.environ.get("PDM_ORACLE_PORT", "1525"))
ORACLE_SERVICE = os.environ.get("PDM_ORACLE_SERVICE", "FREEPDB1")
ORACLE_USER = os.environ.get("PDM_ORACLE_USER", "pdm")
ORACLE_PASSWORD = os.environ.get("PDM_ORACLE_PASSWORD", "pdm")
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://admin:password@localhost:5433/pdm_local"
)
RESTATE_INGRESS_URL = os.environ.get("RESTATE_INGRESS_URL", "http://localhost:8087")
RESTATE_ADMIN_URL = os.environ.get("RESTATE_ADMIN_URL", "http://localhost:9077")


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(timeout)
        try:
            s.connect((host, port))
            return True
        except OSError:
            return False


def _wait_for_oracle(deadline_s: int = 180):
    """Block until Oracle answers a real SELECT, not just a TCP connect."""
    import oracledb
    deadline = time.time() + deadline_s
    last_err = None
    while time.time() < deadline:
        try:
            with oracledb.connect(
                user=ORACLE_USER,
                password=ORACLE_PASSWORD,
                dsn=f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM dual")
                    cur.fetchone()
            return
        except Exception as e:
            last_err = e
            time.sleep(3)
    raise RuntimeError(f"Oracle not reachable after {deadline_s}s: {last_err}")


@pytest.fixture(scope="module")
def oracle_alive():
    if not _port_open(ORACLE_HOST, ORACLE_PORT):
        pytest.skip(
            f"Oracle not listening on {ORACLE_HOST}:{ORACLE_PORT} — "
            "start with `docker compose up -d oracle postgres restate`"
        )
    pytest.importorskip("oracledb")
    _wait_for_oracle()


@pytest.fixture(scope="module")
def postgres_alive():
    if not _port_open("localhost", 5433):
        pytest.skip("Postgres not listening on localhost:5433")
    pytest.importorskip("psycopg2")


@pytest.fixture(scope="module")
def deps():
    pytest.importorskip("dlt")
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("restate")


@pytest.fixture
def oracle_conn(oracle_alive):
    import oracledb
    conn = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}",
    )
    yield conn
    conn.close()


@pytest.fixture
def reset_state(oracle_conn, postgres_alive):
    """Truncate Oracle staging/stats and re-seed; drop Postgres pdm_raw schema."""
    import psycopg2

    with oracle_conn.cursor() as cur:
        cur.execute("DELETE FROM pdm_stats")
        cur.execute("DELETE FROM pdm_staging")
        for i in range(1, 6):
            cur.execute(
                "INSERT INTO pdm_staging (part_id, part_number, description, unit_of_measure) "
                "VALUES (:1, :2, :3, :4)",
                [i, f"P-{i:04d}", f"seed part {i}", "EA"],
            )
    oracle_conn.commit()

    pg = psycopg2.connect(POSTGRES_DSN)
    pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS pdm_raw CASCADE")
    pg.close()


def _dlt_extract_unprocessed():
    """One dlt run: copy unprocessed PDM rows from Oracle into Postgres."""
    import dlt
    from dlt.sources.sql_database import sql_database
    from sqlalchemy import create_engine, select

    oracle_url = (
        f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}"
        f"@{ORACLE_HOST}:{ORACLE_PORT}/?service_name={ORACLE_SERVICE}"
    )
    engine = create_engine(oracle_url)

    def adapt(query, table):
        return select(table).where(table.c.processed_flag == "N")

    src = sql_database(
        credentials=engine,
        schema=ORACLE_USER.lower(),
        table_names=["pdm_staging"],
        query_adapter_callback=adapt,
    )

    pipeline = dlt.pipeline(
        pipeline_name="pdm_test",
        destination=dlt.destinations.postgres(POSTGRES_DSN),
        dataset_name="pdm_raw",
        progress=None,
    )
    info = pipeline.run(src, write_disposition="merge", primary_key="part_id")
    return info


def _count_postgres_rows() -> int:
    import psycopg2
    pg = psycopg2.connect(POSTGRES_DSN)
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema='pdm_raw' AND table_name='pdm_staging'"
            )
            if cur.fetchone()[0] == 0:
                return 0
            cur.execute("SELECT COUNT(*) FROM pdm_raw.pdm_staging")
            return cur.fetchone()[0]
    finally:
        pg.close()


def _oracle_run_ack(record_ids):
    """Invoke the real oracle_ack handler against real Oracle."""
    # Ensure the handler sees Oracle creds via the env vars it reads.
    os.environ["ORACLE_DSN"] = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    os.environ["ORACLE_USER"] = ORACLE_USER
    os.environ["ORACLE_PASSWORD"] = ORACLE_PASSWORD

    # Add the repo root to sys.path so we can import dag_tools without install.
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from dag_tools.restate_handlers import oracle_ack

    class FakeCtx:
        async def run(self, name, fn):
            return fn()

    asyncio.run(oracle_ack.mark_as_processed(
        FakeCtx(),
        {
            "table_name": "pdm_staging",
            "pk_column": "part_id",
            "record_ids": record_ids,
            "stats_table": "pdm_stats",
        },
    ))


def _restate_run_ack(record_ids):
    """Drive the ack through real Restate ingress (synchronous call).

    POSTing to /<service>/<handler> (no /send suffix) makes Restate block
    until the handler finishes, so we don't need to poll afterwards.
    """
    import httpx
    resp = httpx.post(
        f"{RESTATE_INGRESS_URL}/GenericOracleAckService/mark_as_processed",
        json={
            "table_name": "pdm_staging",
            "pk_column": "part_id",
            "record_ids": record_ids,
            "stats_table": "pdm_stats",
        },
        timeout=60.0,
    )
    resp.raise_for_status()


def _oracle_unprocessed_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pdm_staging WHERE processed_flag = 'N'")
        return cur.fetchone()[0]


def _oracle_stats_rows(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT rows_acked, source_table FROM pdm_stats ORDER BY cycle_id")
        return cur.fetchall()


@pytest.mark.integration
def test_full_pdm_cycle_oracle_dlt_postgres_restate(
    deps, oracle_conn, postgres_alive, reset_state
):
    """Validates exactly the round-trip the user asked about:

    1. Oracle starts with 5 unprocessed rows
    2. dlt pulls them incrementally into Postgres → Postgres has 5 rows
    3. oracle_ack flips processed_flag in Oracle and writes a stats row
    4. Second dlt pull finds nothing new → Postgres still at 5 rows
    5. Insert a 6th row into Oracle
    6. Third dlt pull picks up the new row → Postgres now at 6 rows
    7. oracle_ack on the new row → another stats row, Oracle drained again
    """
    # --- pre-conditions ------------------------------------------------------
    assert _oracle_unprocessed_count(oracle_conn) == 5
    assert _count_postgres_rows() == 0
    assert _oracle_stats_rows(oracle_conn) == []

    # --- cycle 1 -------------------------------------------------------------
    info1 = _dlt_extract_unprocessed()
    print(f"\ncycle 1 dlt: {info1}")
    assert _count_postgres_rows() == 5, "cycle 1 should land 5 rows in Postgres"

    # ack back to Oracle
    _oracle_run_ack([1, 2, 3, 4, 5])
    assert _oracle_unprocessed_count(oracle_conn) == 0, "all rows now acked"
    stats = _oracle_stats_rows(oracle_conn)
    assert stats == [(5, "pdm_staging")], "one stats row for the 5-row ack"

    # --- cycle 2: nothing new ------------------------------------------------
    info2 = _dlt_extract_unprocessed()
    print(f"\ncycle 2 dlt: {info2}")
    assert _count_postgres_rows() == 5, "cycle 2 should add nothing"
    assert _oracle_stats_rows(oracle_conn) == [(5, "pdm_staging")], "no new stats row"

    # --- new Oracle row, cycle 3 --------------------------------------------
    with oracle_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pdm_staging (part_id, part_number, description, unit_of_measure) "
            "VALUES (99, 'P-0099', 'new part inserted mid-cycle', 'EA')"
        )
    oracle_conn.commit()
    assert _oracle_unprocessed_count(oracle_conn) == 1

    info3 = _dlt_extract_unprocessed()
    print(f"\ncycle 3 dlt: {info3}")
    assert _count_postgres_rows() == 6, "cycle 3 should pick up the new row"

    _oracle_run_ack([99])
    assert _oracle_unprocessed_count(oracle_conn) == 0
    final_stats = _oracle_stats_rows(oracle_conn)
    assert final_stats == [(5, "pdm_staging"), (1, "pdm_staging")], (
        "two stats rows total: one per ack batch"
    )


@pytest.fixture(scope="module")
def restate_worker_registered():
    """Skip unless Restate ingress is up and the worker is registered."""
    import httpx
    if not _port_open("localhost", 8087):
        pytest.skip(
            "Restate ingress not on localhost:8087 — "
            "run `docker compose up -d restate-handlers` and register the deployment"
        )
    try:
        resp = httpx.get(f"{RESTATE_ADMIN_URL}/deployments", timeout=5.0)
        resp.raise_for_status()
        deps = resp.json().get("deployments", [])
        has_worker = any(
            "restate-handlers" in d.get("uri", "") or "9080" in d.get("uri", "")
            for d in deps
        )
        if not has_worker:
            pytest.skip(
                "No worker deployment registered with Restate — register with: "
                "curl -X POST http://localhost:9077/deployments "
                "-d '{\"uri\":\"http://restate-handlers:9080\"}'"
            )
    except Exception as e:
        pytest.skip(f"Restate admin not responsive: {e}")


@pytest.mark.integration
def test_full_pdm_cycle_via_restate_ingress(
    deps, oracle_conn, postgres_alive, reset_state, restate_worker_registered
):
    """Same cycle as the direct-handler test, but the ack goes through
    Restate's real ingress -> durable journal -> worker -> Oracle.

    This proves the cycle works end-to-end with Restate in the loop,
    not just the handler invoked directly.
    """
    assert _oracle_unprocessed_count(oracle_conn) == 5
    assert _count_postgres_rows() == 0
    assert _oracle_stats_rows(oracle_conn) == []

    # --- cycle 1 -------------------------------------------------------------
    _dlt_extract_unprocessed()
    assert _count_postgres_rows() == 5

    _restate_run_ack([1, 2, 3, 4, 5])
    assert _oracle_unprocessed_count(oracle_conn) == 0
    assert _oracle_stats_rows(oracle_conn) == [(5, "pdm_staging")]

    # --- cycle 2 -------------------------------------------------------------
    _dlt_extract_unprocessed()
    assert _count_postgres_rows() == 5

    # --- cycle 3 -------------------------------------------------------------
    with oracle_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pdm_staging (part_id, part_number, description, unit_of_measure) "
            "VALUES (99, 'P-0099', 'new part via restate cycle', 'EA')"
        )
    oracle_conn.commit()

    _dlt_extract_unprocessed()
    assert _count_postgres_rows() == 6

    _restate_run_ack([99])
    assert _oracle_unprocessed_count(oracle_conn) == 0
    assert _oracle_stats_rows(oracle_conn) == [
        (5, "pdm_staging"),
        (1, "pdm_staging"),
    ]
