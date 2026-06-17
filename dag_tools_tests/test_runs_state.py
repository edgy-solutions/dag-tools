"""Tests for the resumable run-state machine."""
from datetime import datetime, timezone

import pytest

pytest.importorskip("pydantic")

from dag_tools.qual.runs import (
    QualRunState,
    RepState,
    RepStatus,
    default_local_state_path,
    pending_or_resumable,
    rep_id_for,
    transition,
)


def _rep(rid="a:k", status=RepStatus.PENDING, run_id=None):
    return RepState(
        rep_id=rid, class_hash="a", asset_key=["k"],
        repo="r", git_sha="s", runnability="runnable",
        status=status, run_id=run_id,
    )


def _state(*reps):
    now = datetime(2026, 6, 15, tzinfo=timezone.utc)
    return QualRunState(
        qual_id="q", side="baseline",
        started_at=now, updated_at=now,
        reps={r.rep_id: r for r in reps},
    )


def test_rep_id_for_is_stable_and_joins_path():
    assert rep_id_for("hash", ["foo", "bar"]) == "hash:foo/bar"


def test_default_local_state_path_uses_dagtools_home(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    p = default_local_state_path("q1", "baseline")
    assert p == tmp_path / "quals" / "q1" / "baseline-state.json"


def test_pending_or_resumable_picks_active_states():
    """Resumable set = PENDING + LAUNCHED + FAILED. PASSED + SKIPPED are
    terminal and stable."""
    reps = [
        _rep("p", RepStatus.PENDING),
        _rep("l", RepStatus.LAUNCHED),
        _rep("f", RepStatus.FAILED),
        _rep("k", RepStatus.PASSED),
        _rep("s", RepStatus.SKIPPED),
    ]
    state = _state(*reps)
    ids = {r.rep_id for r in pending_or_resumable(state)}
    assert ids == {"p", "l", "f"}


def test_transition_updates_status_and_run_id():
    rep = _rep(status=RepStatus.PENDING)
    new = transition(rep, status=RepStatus.LAUNCHED, run_id="run-1", bump_attempts=True)
    assert new.status == RepStatus.LAUNCHED
    assert new.run_id == "run-1"
    assert new.attempts == 1
    assert new.last_updated is not None


def test_transition_preserves_existing_run_id_when_not_overridden():
    """If you transition LAUNCHED->FAILED to record an error, don't lose
    the run_id we already captured — it's needed for forensics."""
    rep = _rep(status=RepStatus.LAUNCHED, run_id="run-1")
    new = transition(rep, status=RepStatus.FAILED, error="boom")
    assert new.run_id == "run-1"
    assert new.error == "boom"


def test_qualrunstate_round_trips_through_json():
    state = _state(_rep("a", RepStatus.PENDING), _rep("b", RepStatus.PASSED))
    body = state.model_dump_json()
    fresh = QualRunState.model_validate_json(body)
    assert set(fresh.reps.keys()) == {"a", "b"}
    assert fresh.reps["b"].status == RepStatus.PASSED


def test_qualrunstate_tolerates_unknown_fields():
    """Forward compatibility per the schema rule."""
    state = QualRunState.model_validate({
        "qual_id": "q",
        "side": "baseline",
        "started_at": "2026-06-15T00:00:00+00:00",
        "updated_at": "2026-06-15T00:00:00+00:00",
        "reps": {},
        "field_from_the_future": "ignored",
    })
    assert state.qual_id == "q"
