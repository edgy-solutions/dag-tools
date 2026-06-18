"""Unit tests for RepDiff + class-level roll-up.

The diff is the load-bearing logic for "did the candidate behave like
baseline." Wrong parity decisions = wrong upgrade verdicts. So this
matrix covers every parity dimension explicitly.
"""
from datetime import datetime, timezone
from typing import List, Optional

from dag_tools.qual.classes import Representative, Runnability
from dag_tools.qual.runs.records import (
    AssetCheckResultSummary,
    MaterializationEventSummary,
    RunRecord,
)
from dag_tools.qual.verdict import diff_rep


def _rep(asset_key=("hello",)):
    return Representative(
        repo="alpha", git_sha="sha-a",
        asset_key=list(asset_key),
        runnability=Runnability.RUNNABLE,
        runnability_reason="default",
    )


def _record(
    *,
    side="baseline",
    success=True,
    materializations: Optional[List[MaterializationEventSummary]] = None,
    metadata_keys: Optional[List[str]] = None,
    checks: Optional[List[AssetCheckResultSummary]] = None,
    duration=10.0,
):
    return RunRecord(
        qual_id="q1", side=side, class_hash="h1",
        asset_key=["hello"], repo="alpha", git_sha="sha-a",
        run_id=f"run-{side}", success=success,
        status="SUCCESS" if success else "FAILURE",
        started_at=datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
        duration_seconds=duration,
        materialization_events=materializations or [
            MaterializationEventSummary(asset_key=["hello"], metadata_keys=["row_count"])
        ],
        asset_check_results=checks or [],
        metadata_keys=metadata_keys if metadata_keys is not None else ["row_count"],
        event_count=len(materializations or [1]),
    )


# ---------------------------------------------------------------------------
# All-parity-matches happy path
# ---------------------------------------------------------------------------


def test_diff_passes_when_everything_matches():
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=_record(side="baseline"),
        candidate=_record(side="candidate"),
    )
    assert diff.is_pass
    assert diff.success_parity is True
    assert diff.materialization_count_parity is True
    assert diff.materialization_asset_keys_parity is True
    assert diff.metadata_keys_parity is True
    assert diff.asset_check_parity is True
    assert diff.notes == []


# ---------------------------------------------------------------------------
# Per-parity failure dimensions
# ---------------------------------------------------------------------------


def test_diff_fails_on_success_mismatch():
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=_record(success=True),
        candidate=_record(side="candidate", success=False),
    )
    assert not diff.is_pass
    assert diff.success_parity is False
    assert any("success parity" in n for n in diff.notes)


def test_diff_fails_on_materialization_count_mismatch():
    baseline = _record(materializations=[
        MaterializationEventSummary(asset_key=["hello"]),
        MaterializationEventSummary(asset_key=["world"]),
    ])
    candidate = _record(side="candidate", materializations=[
        MaterializationEventSummary(asset_key=["hello"]),
    ])
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=baseline, candidate=candidate,
    )
    assert not diff.is_pass
    assert diff.materialization_count_parity is False


def test_diff_fails_on_materialization_asset_keys_mismatch():
    """Same count, different keys."""
    baseline = _record(materializations=[
        MaterializationEventSummary(asset_key=["alpha"]),
    ])
    candidate = _record(side="candidate", materializations=[
        MaterializationEventSummary(asset_key=["beta"]),
    ])
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=baseline, candidate=candidate,
    )
    assert diff.materialization_count_parity is True
    assert diff.materialization_asset_keys_parity is False
    assert any("baseline-only" in n or "candidate-only" in n for n in diff.notes)


def test_diff_compares_metadata_KEY_set_not_values():
    """Recipe rule: metadata values may legitimately differ
    (different timestamps, different run IDs in metadata). Only the KEY
    set is the parity check."""
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=_record(metadata_keys=["row_count", "checksum"]),
        candidate=_record(side="candidate", metadata_keys=["row_count", "checksum"]),
    )
    assert diff.metadata_keys_parity is True
    assert diff.is_pass


def test_diff_fails_on_metadata_key_set_difference():
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=_record(metadata_keys=["row_count"]),
        candidate=_record(side="candidate", metadata_keys=["row_count", "extra"]),
    )
    assert diff.metadata_keys_parity is False
    assert any("metadata key set" in n for n in diff.notes)


def test_diff_fails_on_asset_check_parity_break():
    """Same checks, different pass/fail outcomes."""
    baseline_checks = [
        AssetCheckResultSummary(asset_key=["hello"], check_name="non_null", passed=True),
    ]
    candidate_checks = [
        AssetCheckResultSummary(asset_key=["hello"], check_name="non_null", passed=False),
    ]
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=_record(checks=baseline_checks),
        candidate=_record(side="candidate", checks=candidate_checks),
    )
    assert diff.asset_check_parity is False


# ---------------------------------------------------------------------------
# Duration: informational, never gating
# ---------------------------------------------------------------------------


def test_diff_duration_is_informational_not_gating():
    """Even a 100x duration delta does NOT fail the diff."""
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=_record(duration=1.0),
        candidate=_record(side="candidate", duration=100.0),
    )
    assert diff.is_pass
    assert diff.duration_delta_s == 99.0


# ---------------------------------------------------------------------------
# Missing data
# ---------------------------------------------------------------------------


def test_diff_returns_undecidable_when_baseline_missing():
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=None, candidate=_record(side="candidate"),
    )
    assert not diff.is_pass
    assert diff.success_parity is None
    assert any("baseline run record missing" in n for n in diff.notes)
    assert diff.candidate_run_id == "run-candidate"


def test_diff_returns_undecidable_when_candidate_missing():
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=_record(), candidate=None,
    )
    assert not diff.is_pass
    assert any("candidate run record missing" in n for n in diff.notes)
    assert diff.baseline_run_id == "run-baseline"


def test_diff_returns_undecidable_when_both_missing():
    diff = diff_rep(
        rep=_rep(), class_hash="h1",
        baseline=None, candidate=None,
    )
    assert not diff.is_pass
    assert any("no run records" in n for n in diff.notes)


# ---------------------------------------------------------------------------
# build_class_verdicts roll-up
# ---------------------------------------------------------------------------


def test_class_verdict_green_when_all_runnable_reps_pass():
    from dag_tools.qual.classes import ClassKeyComponents, ClassMatrix, EquivalenceClass
    from dag_tools.qual.verdict import build_class_verdicts

    rep = _rep(asset_key=["a"])
    cls = EquivalenceClass(
        class_hash="hX",
        key=ClassKeyComponents(),
        member_count=1, member_repo_count=1,
        members=[], representatives=[rep],
    )
    matrix = ClassMatrix(
        qual_id="q1", generated_at=datetime.now(timezone.utc),
        asset_count=1, class_count=1, classes=[cls],
    )
    rep_id = f"{cls.class_hash}:{'/'.join(rep.asset_key)}"
    diffs = {
        rep_id: diff_rep(
            rep=rep, class_hash=cls.class_hash,
            baseline=_record(), candidate=_record(side="candidate"),
        )
    }
    verdicts = build_class_verdicts(matrix, diff_by_rep_id=diffs)
    assert len(verdicts) == 1
    assert verdicts[0].is_green is True
    assert verdicts[0].failure_summary is None


def test_class_verdict_red_when_any_runnable_rep_fails_parity():
    from dag_tools.qual.classes import ClassKeyComponents, ClassMatrix, EquivalenceClass
    from dag_tools.qual.verdict import build_class_verdicts

    reps = [_rep(asset_key=["a"]), _rep(asset_key=["b"])]
    cls = EquivalenceClass(
        class_hash="hX",
        key=ClassKeyComponents(),
        member_count=2, member_repo_count=1,
        members=[], representatives=reps,
    )
    matrix = ClassMatrix(
        qual_id="q1", generated_at=datetime.now(timezone.utc),
        asset_count=2, class_count=1, classes=[cls],
    )
    diffs = {}
    for rep in reps:
        rep_id = f"{cls.class_hash}:{'/'.join(rep.asset_key)}"
        if rep.asset_key == ["a"]:
            # parity passes
            d = diff_rep(rep=rep, class_hash=cls.class_hash,
                         baseline=_record(), candidate=_record(side="candidate"))
        else:
            # parity fails
            d = diff_rep(rep=rep, class_hash=cls.class_hash,
                         baseline=_record(success=True),
                         candidate=_record(side="candidate", success=False))
        diffs[rep_id] = d

    verdicts = build_class_verdicts(matrix, diff_by_rep_id=diffs)
    assert verdicts[0].is_green is False
    assert "1 of 2" in (verdicts[0].failure_summary or "")


def test_synthetic_class_is_not_gated_on_runnable_parity():
    """SYNTHETIC_REQUIRED classes are reported but not gated here — the
    verdict layer handles them separately via the probe-coverage rule."""
    from dag_tools.qual.classes import (
        ClassKeyComponents, ClassMatrix, EquivalenceClass, Representative,
    )
    from dag_tools.qual.verdict import build_class_verdicts

    rep = Representative(
        repo="alpha", git_sha="s",
        asset_key=["x"],
        runnability=Runnability.SYNTHETIC_REQUIRED,
        runnability_reason="tag",
    )
    cls = EquivalenceClass(
        class_hash="hX",
        key=ClassKeyComponents(),
        member_count=1, member_repo_count=1,
        members=[], representatives=[rep],
    )
    matrix = ClassMatrix(
        qual_id="q1", generated_at=datetime.now(timezone.utc),
        asset_count=1, class_count=1, classes=[cls],
    )

    verdicts = build_class_verdicts(matrix, diff_by_rep_id={})
    # No diffs at all (synthetic never ran), but the class still rolls up
    # as is_green=True because the runnable-parity criterion doesn't apply.
    assert verdicts[0].is_green is True
    assert verdicts[0].runnability == "synthetic_required"
