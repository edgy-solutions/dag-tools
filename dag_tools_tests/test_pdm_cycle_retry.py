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


# ---------------------------------------------------------------------------
# The same stall, in the request sensor
# ---------------------------------------------------------------------------
#
# The overlay sensor had the identical bug in a worse form: it wrote the
# cursor BEFORE the run's outcome was known, so a failed request marked the
# list as handled and was skipped as "unchanged" forever. Worse because
# nothing is ever asked for, so no completion marker arrives and the whole
# cycle waits on a request that never landed.

from dagster import DagsterRunStatus

from dag_tools.components.restate_dlt_sync.component import MEI_DIGEST_TAG


@pytest.fixture
def overlay(tmp_path):
    f = tmp_path / "meis.yaml"
    f.write_text("- M-1\n- M-2\n")
    return f


def _overlay_sensor(overlay):
    defs = build._component(pipeline={"mei_table": {
        "name": "PDM_MEI_REQUEST", "mei_column": "MEI_NUMBER",
        "source_file": str(overlay),
    }}).build_defs(None)
    return next(s for s in defs.sensors if s.name == "pdm_mei_overlay_sensor")


def test_the_request_sensor_fires_for_a_new_list(overlay):
    result, _ = _tick(_overlay_sensor(overlay), DagsterInstance.ephemeral())
    assert _is_run(result), result


def test_the_request_sensor_sets_no_run_key(overlay):
    """A run_key made the skip permanent regardless of outcome."""
    result, _ = _tick(_overlay_sensor(overlay), DagsterInstance.ephemeral())
    req = result if isinstance(result, RunRequest) else result[0]
    assert req.run_key is None, req.run_key


def test_the_request_sensor_tags_the_list_it_launched_for(overlay):
    result, _ = _tick(_overlay_sensor(overlay), DagsterInstance.ephemeral())
    req = result if isinstance(result, RunRequest) else result[0]
    assert MEI_DIGEST_TAG in req.tags, req.tags


def test_a_failed_request_is_retried(overlay):
    """No successful run exists for this list, so it must fire again."""
    sensor = _overlay_sensor(overlay)
    instance = DagsterInstance.ephemeral()

    first, cursor = _tick(sensor, instance)
    assert _is_run(first)

    second, cursor = _tick(sensor, instance, cursor)
    assert _is_run(second), "a failed request was never retried"


def test_a_succeeded_request_is_not_repeated(overlay):
    """Re-asking for an identical list is pure waste, and on a delta load
    can make the source redo work."""
    from dagster._core.test_utils import create_run_for_test

    sensor = _overlay_sensor(overlay)
    instance = DagsterInstance.ephemeral()

    result, cursor = _tick(sensor, instance)
    req = result if isinstance(result, RunRequest) else result[0]

    create_run_for_test(
        instance,
        job_name="pdm_mei_request_job",
        status=DagsterRunStatus.SUCCESS,
        tags=dict(req.tags),
    )

    again, cursor = _tick(sensor, instance, cursor)
    assert isinstance(again, SkipReason), again
    assert "already succeeded" in str(again.skip_message)


def test_a_success_for_a_DIFFERENT_list_does_not_count(overlay):
    """The digest is what makes the check specific. Without it, any past
    success would suppress every future request."""
    from dagster._core.test_utils import create_run_for_test

    sensor = _overlay_sensor(overlay)
    instance = DagsterInstance.ephemeral()

    create_run_for_test(
        instance,
        job_name="pdm_mei_request_job",
        status=DagsterRunStatus.SUCCESS,
        tags={MEI_DIGEST_TAG: "some-other-list"},
    )

    result, _ = _tick(sensor, instance)
    assert _is_run(result), result
