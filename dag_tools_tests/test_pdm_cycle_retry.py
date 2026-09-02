"""A failed cycle must retry, and must not retry forever.

The reported symptom:

    Skipping 1 run for sensor pdm_extraction_cycle_sensor already completed
    with run keys: ["2026-08-31 15:27:33"]

The sensor passed ``run_key=<completion timestamp>``, and Dagster dedupes
on run_key PERMANENTLY -- once a run existed for that key it would never
fire for it again. So a FAILED cycle stalled until the source published a
new completion marker, and the operator's only recourse was deleting the
failed run.

The run_key was redundant on top of that. "newest completion is newer than
newest consumption" is already the idempotency guard, and it is the
correct one: after a failed run it still reads "not consumed", which is
true. What the run_key added was a permanent block on ever acting on that
truth again.

Attempts are now counted per completion marker in the sensor cursor, so a
transient failure retries and a deterministic one gives up instead of
re-firing against the source every interval forever.

These drive the REAL generated sensor against a real database, with a
cursor that round-trips the way it does in production.
"""
import json

import pytest

pytest.importorskip("dagster_dlt")

import sqlalchemy as sa
from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

import dag_tools_tests.test_pdm_component_build as build


CONTROL_DDL = (
    "CREATE TABLE PDM_CONTROL "
    "(LOAD_STATUS TEXT, LOAD_TYPE TEXT, LOAD_TS TIMESTAMP)"
)


@pytest.fixture
def source(tmp_path):
    """A sqlite stand-in for the source Oracle, reachable by SQLAlchemy URL."""
    url = f"sqlite:///{tmp_path / 'src.sqlite'}"
    engine = sa.create_engine(url)
    with engine.begin() as conn:
        conn.execute(sa.text(CONTROL_DDL))
    return url, engine


def _row(engine, status, ts, load_type=None):
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO PDM_CONTROL (LOAD_STATUS, LOAD_TYPE, LOAD_TS) "
                "VALUES (:s, :t, :ts)"
            ),
            {"s": status, "t": load_type, "ts": ts},
        )


def _sensor(url, max_attempts=3):
    defs = build._component(
        pipeline={"cycle_sensor": {"enabled": True, "max_attempts": max_attempts}},
        source_config={
            "type": "sql_database", "drivername": "oracle+oracledb",
            "credentials": url, "database": "FREEPDB1", "schema": "PDM",
        },
    ).build_defs(None)
    return next(s for s in defs.sensors if s.name == "pdm_cycle_sensor")


def _tick(sensor, instance, cursor=None):
    """One sensor evaluation, returning (result, new_cursor)."""
    ctx = build_sensor_context(instance=instance, cursor=cursor)
    result = sensor(ctx)
    return result, ctx.cursor


def _is_run(result):
    if isinstance(result, RunRequest):
        return True
    if isinstance(result, list):
        return any(isinstance(r, RunRequest) for r in result)
    return False


COMPLETED_AT = "2026-08-31 15:27:33"


def test_a_completion_marker_triggers_a_run(source):
    url, engine = source
    _row(engine, "COMPLETED", COMPLETED_AT, "FULL")
    result, _ = _tick(_sensor(url), DagsterInstance.ephemeral())
    assert _is_run(result), result


def test_the_same_marker_retries_after_a_failure(source):
    """The reported bug. The first tick launches; the run fails, so no
    consumption row is written; the next tick must launch again."""
    url, engine = source
    _row(engine, "COMPLETED", COMPLETED_AT, "FULL")
    sensor = _sensor(url)
    instance = DagsterInstance.ephemeral()

    first, cursor = _tick(sensor, instance)
    assert _is_run(first)

    # The run failed: nothing consumed it.
    second, cursor = _tick(sensor, instance, cursor)
    assert _is_run(second), "a failed cycle did not retry"


def test_no_run_key_is_set(source):
    """A run_key is what made the stall permanent, so its absence is the
    fix and worth asserting directly.

    This is the load-bearing assertion of the file. The dedupe itself
    happens in the DAEMON, not in the sensor, so
    ``build_sensor_context`` cannot reproduce it -- the retry test above
    passes with or without the run_key. Verified by restoring the run_key
    and confirming this test, and only this test, fails.
    """
    url, engine = source
    _row(engine, "COMPLETED", COMPLETED_AT, "FULL")
    result, _ = _tick(_sensor(url), DagsterInstance.ephemeral())
    request = result if isinstance(result, RunRequest) else result[0]
    assert request.run_key is None, request.run_key


def test_retries_are_bounded(source):
    """A deterministically-failing load must stop, or it re-fires against
    the source every interval forever."""
    url, engine = source
    _row(engine, "COMPLETED", COMPLETED_AT, "FULL")
    sensor = _sensor(url, max_attempts=3)
    instance = DagsterInstance.ephemeral()

    cursor = None
    for attempt in range(1, 4):
        result, cursor = _tick(sensor, instance, cursor)
        assert _is_run(result), f"attempt {attempt} did not launch"

    result, cursor = _tick(sensor, instance, cursor)
    assert isinstance(result, SkipReason), result
    assert "not retrying" in str(result.skip_message)


def test_the_attempt_number_rides_on_the_run(source):
    url, engine = source
    _row(engine, "COMPLETED", COMPLETED_AT, "FULL")
    sensor = _sensor(url)
    instance = DagsterInstance.ephemeral()

    first, cursor = _tick(sensor, instance)
    second, cursor = _tick(sensor, instance, cursor)

    req = second if isinstance(second, RunRequest) else second[0]
    assert req.tags["pdm/attempt"] == "2", req.tags


def test_a_new_marker_resets_the_count(source):
    """Otherwise one bad load would poison every later cycle."""
    url, engine = source
    _row(engine, "COMPLETED", COMPLETED_AT, "FULL")
    sensor = _sensor(url, max_attempts=2)
    instance = DagsterInstance.ephemeral()

    cursor = None
    for _ in range(2):
        _, cursor = _tick(sensor, instance, cursor)
    exhausted, cursor = _tick(sensor, instance, cursor)
    assert isinstance(exhausted, SkipReason)

    _row(engine, "COMPLETED", "2026-08-31 18:00:00", "DELTA")
    result, cursor = _tick(sensor, instance, cursor)
    assert _is_run(result), "a fresh completion marker did not reset the count"
    req = result if isinstance(result, RunRequest) else result[0]
    assert req.tags["pdm/attempt"] == "1"


def test_a_consumed_cycle_stops_firing(source):
    """The real idempotency guard, unchanged: our own completion row is
    what settles the cycle."""
    url, engine = source
    _row(engine, "COMPLETED", COMPLETED_AT, "FULL")
    sensor = _sensor(url)
    instance = DagsterInstance.ephemeral()

    first, cursor = _tick(sensor, instance)
    assert _is_run(first)

    _row(engine, "CONSUMED", "2026-08-31 15:40:00")
    result, cursor = _tick(sensor, instance, cursor)
    assert isinstance(result, SkipReason), result
    assert "already consumed" in str(result.skip_message)


def test_an_unreadable_cursor_is_treated_as_a_fresh_marker(source):
    """Cursor formats change across releases. A stale one must not wedge
    the sensor shut."""
    url, engine = source
    _row(engine, "COMPLETED", COMPLETED_AT, "FULL")
    result, _ = _tick(
        _sensor(url), DagsterInstance.ephemeral(), cursor="not json at all"
    )
    assert _is_run(result), result


def test_max_attempts_below_one_is_refused():
    with pytest.raises(ValueError, match="at least 1"):
        build._component(
            pipeline={"cycle_sensor": {"enabled": True, "max_attempts": 0}}
        ).build_defs(None)
