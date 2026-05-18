"""Validates the stateful PDM-from-Oracle ingestion cycle.

The example at examples/pdm_oracle_ingestion/ reads unprocessed PDM rows from
an Oracle staging table via dlt, then acknowledges them back to Oracle through
the GenericOracleAckService Restate handler so the next cycle skips them.

These tests exercise the read -> ack -> cycle loop in-process by:
  * Using an in-memory sqlite database as a stand-in for the Oracle staging
    table (same SQL semantics for the UPDATE the handler issues).
  * Patching oracledb.connect to wrap that sqlite connection.
  * Driving the real mark_as_processed handler via a minimal restate.Context
    double.

If restate-sdk or oracledb are not installed in this environment, the tests
skip rather than fail.
"""
import asyncio
import sqlite3
from unittest.mock import patch

import pytest

pytest.importorskip("restate")
pytest.importorskip("oracledb")

from dag_tools.restate_handlers import oracle_ack


class FakeContext:
    """Minimal restate.Context double: runs side-effects inline."""
    async def run(self, name, fn):
        return fn()


class _CtxMgr:
    def __init__(self, target):
        self._target = target

    def __enter__(self):
        return self._target

    def __exit__(self, *_):
        return False


class FakeOracleCursor:
    def __init__(self, sqlite_cursor, recorder):
        self._cursor = sqlite_cursor
        self._recorder = recorder

    def execute(self, sql, binds=None):
        self._recorder.append((sql, dict(binds or {})))
        if binds:
            self._cursor.execute(sql, binds)
        else:
            self._cursor.execute(sql)


class FakeOracleConnection:
    """Wraps a sqlite connection with the oracledb context-manager surface."""
    def __init__(self, sqlite_conn, recorder):
        self._conn = sqlite_conn
        self._recorder = recorder

    def cursor(self):
        return _CtxMgr(FakeOracleCursor(self._conn.cursor(), self._recorder))

    def commit(self):
        self._conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def _set_oracle_env(monkeypatch):
    monkeypatch.setenv("ORACLE_DSN", "fake-dsn")
    monkeypatch.setenv("ORACLE_USER", "fake-user")
    monkeypatch.setenv("ORACLE_PASSWORD", "fake-pw")


def test_mark_as_processed_rejects_missing_payload_fields(monkeypatch):
    _set_oracle_env(monkeypatch)
    with pytest.raises(ValueError, match="Missing required payload"):
        asyncio.run(oracle_ack.mark_as_processed(
            FakeContext(),
            {"table_name": "PDM_STAGING"},
        ))


def test_mark_as_processed_rejects_missing_oracle_credentials(monkeypatch):
    for var in ("ORACLE_DSN", "ORACLE_USER", "ORACLE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ValueError, match="Missing Oracle connection credentials"):
        asyncio.run(oracle_ack.mark_as_processed(
            FakeContext(),
            {"table_name": "PDM_STAGING", "pk_column": "PART_ID", "record_ids": [1]},
        ))


def test_mark_as_processed_chunks_into_groups_of_1000(monkeypatch):
    """Oracle's IN clause maxes out at 1000 binds — the handler must chunk."""
    _set_oracle_env(monkeypatch)

    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE PDM_STAGING (PART_ID INTEGER, processed_flag TEXT, sync_date TEXT)"
    )
    recorder = []
    fake_conn = FakeOracleConnection(db, recorder)

    record_ids = list(range(1, 2501))  # 2500 → 1000 + 1000 + 500

    with patch.object(oracle_ack.oracledb, "connect", return_value=fake_conn):
        asyncio.run(oracle_ack.mark_as_processed(
            FakeContext(),
            {
                "table_name": "PDM_STAGING",
                "pk_column": "PART_ID",
                "record_ids": record_ids,
            },
        ))

    assert len(recorder) == 3, "expected one UPDATE per chunk, no stats insert"
    assert [len(binds) for _, binds in recorder] == [1000, 1000, 500]
    for sql, _ in recorder:
        assert "UPDATE PDM_STAGING" in sql
        assert "processed_flag = 'Y'" in sql
        assert "sync_date = CURRENT_TIMESTAMP" in sql
        assert "WHERE PART_ID IN" in sql


def test_mark_as_processed_appends_stats_row_when_stats_table_given(monkeypatch):
    """With stats_table in the payload, one extra INSERT lands per ack call."""
    _set_oracle_env(monkeypatch)

    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE PDM_STAGING (PART_ID INTEGER, processed_flag TEXT, sync_date TEXT)"
    )
    db.execute(
        "CREATE TABLE PDM_STATS (rows_acked INTEGER, source_table TEXT, acked_at TEXT DEFAULT CURRENT_TIMESTAMP)"
    )
    recorder = []
    fake_conn = FakeOracleConnection(db, recorder)

    with patch.object(oracle_ack.oracledb, "connect", return_value=fake_conn):
        asyncio.run(oracle_ack.mark_as_processed(
            FakeContext(),
            {
                "table_name": "PDM_STAGING",
                "pk_column": "PART_ID",
                "record_ids": [1, 2, 3],
                "stats_table": "PDM_STATS",
            },
        ))

    assert len(recorder) == 2, "expected one UPDATE + one stats INSERT"
    update_sql, _ = recorder[0]
    insert_sql, insert_binds = recorder[1]
    assert "UPDATE PDM_STAGING" in update_sql
    assert "INSERT INTO PDM_STATS" in insert_sql
    assert insert_binds == {"rows_acked": 3, "source_table": "PDM_STAGING"}

    row = db.execute(
        "SELECT rows_acked, source_table FROM PDM_STATS"
    ).fetchone()
    assert row == (3, "PDM_STAGING")


def test_read_ack_cycle_two_iterations_drains_unprocessed_rows(monkeypatch):
    """The core stateful-ingestion semantic.

    Cycle 1: dlt-style SELECT WHERE processed_flag='N' returns all seeded rows.
    Ack:     Restate handler flips processed_flag to 'Y' and stamps sync_date.
    Cycle 2: the same SELECT returns nothing — proving the ack is durable
             and the next dlt run will correctly skip already-acked rows.
    """
    _set_oracle_env(monkeypatch)

    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE PDM_STAGING (
            PART_ID INTEGER PRIMARY KEY,
            PART_NUMBER TEXT,
            processed_flag TEXT DEFAULT 'N',
            sync_date TEXT
        )
    """)
    db.execute(
        "CREATE TABLE PDM_STATS (rows_acked INTEGER, source_table TEXT, acked_at TEXT DEFAULT CURRENT_TIMESTAMP)"
    )
    db.executemany(
        "INSERT INTO PDM_STAGING (PART_ID, PART_NUMBER) VALUES (?, ?)",
        [(i, f"P-{i:04d}") for i in range(1, 6)],
    )
    db.commit()

    def read_unprocessed():
        return [row[0] for row in db.execute(
            "SELECT PART_ID FROM PDM_STAGING WHERE processed_flag = 'N' ORDER BY PART_ID"
        ).fetchall()]

    def restate_ack(record_ids):
        fake_conn = FakeOracleConnection(db, recorder=[])
        with patch.object(oracle_ack.oracledb, "connect", return_value=fake_conn):
            asyncio.run(oracle_ack.mark_as_processed(
                FakeContext(),
                {
                    "table_name": "PDM_STAGING",
                    "pk_column": "PART_ID",
                    "record_ids": record_ids,
                    "stats_table": "PDM_STATS",
                },
            ))

    cycle1 = read_unprocessed()
    assert cycle1 == [1, 2, 3, 4, 5], "cycle 1 should see all seeded rows"

    restate_ack(cycle1)

    cycle2 = read_unprocessed()
    assert cycle2 == [], "cycle 2 should see no rows — they were all acked"

    synced = db.execute(
        "SELECT COUNT(*) FROM PDM_STAGING WHERE sync_date IS NOT NULL"
    ).fetchone()[0]
    assert synced == 5, "every acked row should have a sync_date stamped"

    stats = db.execute(
        "SELECT rows_acked, source_table FROM PDM_STATS"
    ).fetchall()
    assert stats == [(5, "PDM_STAGING")], "ack must write one stats row"


def test_read_ack_cycle_third_iteration_picks_up_newly_inserted_rows(monkeypatch):
    """After draining, a fresh insert should be visible on the next cycle."""
    _set_oracle_env(monkeypatch)

    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE PDM_STAGING (
            PART_ID INTEGER PRIMARY KEY,
            processed_flag TEXT DEFAULT 'N',
            sync_date TEXT
        )
    """)
    db.executemany(
        "INSERT INTO PDM_STAGING (PART_ID) VALUES (?)",
        [(i,) for i in (10, 11, 12)],
    )
    db.commit()

    def read_unprocessed():
        return [row[0] for row in db.execute(
            "SELECT PART_ID FROM PDM_STAGING WHERE processed_flag = 'N' ORDER BY PART_ID"
        ).fetchall()]

    def restate_ack(record_ids):
        fake_conn = FakeOracleConnection(db, recorder=[])
        with patch.object(oracle_ack.oracledb, "connect", return_value=fake_conn):
            asyncio.run(oracle_ack.mark_as_processed(
                FakeContext(),
                {
                    "table_name": "PDM_STAGING",
                    "pk_column": "PART_ID",
                    "record_ids": record_ids,
                },
            ))

    restate_ack(read_unprocessed())
    assert read_unprocessed() == []

    db.execute("INSERT INTO PDM_STAGING (PART_ID) VALUES (?)", (99,))
    db.commit()

    assert read_unprocessed() == [99], "newly inserted row must be visible on next cycle"

    restate_ack([99])
    assert read_unprocessed() == []
