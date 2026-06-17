"""Unit tests for representative selection + runnability classification."""
from dag_tools.qual.classes import (
    ClassMember,
    Runnability,
    classify_runnability,
    pick_representatives,
)


def _m(repo, key, *, tags=None):
    return ClassMember(
        repo=repo, git_sha=f"sha-{repo}",
        asset_key=list(key) if isinstance(key, (list, tuple)) else [key],
        tags=tags or {},
    )


# ---------------------------------------------------------------------------
# Runnability classification
# ---------------------------------------------------------------------------


def test_classify_default_is_runnable():
    m = _m("repo", "x")
    bucket, reason = classify_runnability(m)
    assert bucket == Runnability.RUNNABLE
    assert "default" in reason


def test_classify_synthetic_required_via_tag():
    m = _m("repo", "x", tags={"synthetic_required": "true"})
    bucket, reason = classify_runnability(m)
    assert bucket == Runnability.SYNTHETIC_REQUIRED
    assert "synthetic_required" in reason


def test_classify_observe_only_via_tag():
    m = _m("repo", "x", tags={"observe_only": "true"})
    bucket, _ = classify_runnability(m)
    assert bucket == Runnability.OBSERVE_ONLY


def test_classify_tag_value_case_insensitive():
    """'TRUE', 'True', 'true' all mean true. False stays false."""
    assert classify_runnability(_m("r", "x", tags={"synthetic_required": "TRUE"}))[0] \
        == Runnability.SYNTHETIC_REQUIRED
    assert classify_runnability(_m("r", "x", tags={"synthetic_required": "True"}))[0] \
        == Runnability.SYNTHETIC_REQUIRED
    assert classify_runnability(_m("r", "x", tags={"synthetic_required": "false"}))[0] \
        == Runnability.RUNNABLE


def test_classify_synthetic_wins_over_observe():
    """Both opt-out tags set: synthetic_required wins (it's the stronger
    "do not run this" signal)."""
    m = _m("r", "x", tags={
        "synthetic_required": "true", "observe_only": "true",
    })
    bucket, _ = classify_runnability(m)
    assert bucket == Runnability.SYNTHETIC_REQUIRED


# ---------------------------------------------------------------------------
# pick_representatives — selection algorithm
# ---------------------------------------------------------------------------


def test_pick_returns_empty_when_no_members():
    assert pick_representatives([], prefer_tag="regression", reps_per_class=2) == []


def test_pick_returns_empty_when_reps_per_class_zero():
    members = [_m("r", "a")]
    assert pick_representatives(members, prefer_tag="regression", reps_per_class=0) == []


def test_pick_spreads_across_repos_first():
    """Three repos, reps_per_class=3 -> one from each repo."""
    members = [
        _m("alpha", "x1"), _m("alpha", "x2"),
        _m("beta", "y1"), _m("beta", "y2"),
        _m("gamma", "z1"),
    ]
    reps = pick_representatives(members, prefer_tag="regression", reps_per_class=3)
    assert {r.repo for r in reps} == {"alpha", "beta", "gamma"}


def test_pick_caps_at_reps_per_class():
    members = [
        _m("alpha", "a"), _m("beta", "b"), _m("gamma", "c"), _m("delta", "d"),
    ]
    reps = pick_representatives(members, prefer_tag="regression", reps_per_class=2)
    assert len(reps) == 2


def test_pick_prefers_tagged_members():
    """When one member is tagged with the prefer_tag, it wins over an
    untagged member from the SAME repo even though the untagged would
    sort earlier alphabetically."""
    members = [
        _m("alpha", "a"),  # untagged, sorts first alphabetically
        _m("alpha", "z", tags={"regression": "true"}),  # tagged, sorts last
    ]
    reps = pick_representatives(members, prefer_tag="regression", reps_per_class=1)
    assert len(reps) == 1
    assert reps[0].asset_key == ["z"]
    assert reps[0].is_preferred is True


def test_pick_fills_second_picks_from_seen_repos():
    """When reps_per_class exceeds the number of distinct repos, pass 2
    fills remaining slots from already-seen repos."""
    members = [_m("alpha", "x1"), _m("alpha", "x2"), _m("alpha", "x3")]
    reps = pick_representatives(members, prefer_tag="regression", reps_per_class=3)
    assert len(reps) == 3
    assert all(r.repo == "alpha" for r in reps)


def test_pick_is_deterministic():
    members = [_m("beta", "b"), _m("alpha", "a"), _m("gamma", "c")]
    reps1 = pick_representatives(members, prefer_tag="regression", reps_per_class=3)
    reps2 = pick_representatives(members, prefer_tag="regression", reps_per_class=3)
    assert [r.asset_key for r in reps1] == [r.asset_key for r in reps2]
    # And the order is alphabetical-by-repo (no tag preference here).
    assert [r.repo for r in reps1] == ["alpha", "beta", "gamma"]


def test_pick_carries_runnability_through():
    members = [
        _m("alpha", "ok"),
        _m("beta", "synth", tags={"synthetic_required": "true"}),
    ]
    reps = pick_representatives(members, prefer_tag="regression", reps_per_class=2)
    by_repo = {r.repo: r for r in reps}
    assert by_repo["alpha"].runnability == Runnability.RUNNABLE
    assert by_repo["beta"].runnability == Runnability.SYNTHETIC_REQUIRED


def test_pick_marks_is_preferred_correctly():
    members = [
        _m("alpha", "regr", tags={"regression": "true"}),
        _m("beta", "plain"),
    ]
    reps = pick_representatives(members, prefer_tag="regression", reps_per_class=2)
    by_repo = {r.repo: r for r in reps}
    assert by_repo["alpha"].is_preferred is True
    assert by_repo["beta"].is_preferred is False
