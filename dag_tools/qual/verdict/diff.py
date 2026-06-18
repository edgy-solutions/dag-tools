"""Per-representative diff between baseline and candidate run records.

The recipe's diff bar (Phase Q6):

  > Per representative: success parity, materialization event count &
  > asset keys, metadata key set (values may differ), asset-check status
  > parity, IO round-trip probe parity. Duration deltas reported but
  > non-gating.

Each diff produces a :class:`RepDiff` with one boolean per parity check
(None when the data isn't available — e.g. baseline didn't run yet) and
an overall ``is_pass`` that is True iff every check passed AND we had
enough data to decide.

Class-level roll-up (:func:`build_class_verdicts`) groups RepDiffs by
``class_hash`` and computes ``is_green`` per class: every RUNNABLE
representative's diff must be is_pass=True.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from ..classes import ClassMatrix, EquivalenceClass, Representative, Runnability
from ..runs.records import RunRecord


# ---------------------------------------------------------------------------
# Per-rep diff
# ---------------------------------------------------------------------------


class RepDiff(BaseModel):
    """Diff between baseline and candidate for ONE representative.

    Most boolean fields are ``Optional[bool]`` because we may not have the
    data: a rep that wasn't launched on one side leaves the field None and
    surfaces in ``notes`` instead of producing a misleading False.
    """
    model_config = ConfigDict(extra="ignore")

    class_hash: str
    asset_key: List[str]
    repo: str
    git_sha: str
    runnability: str

    baseline_run_id: Optional[str] = None
    candidate_run_id: Optional[str] = None
    baseline_status: Optional[str] = None
    candidate_status: Optional[str] = None
    baseline_success: Optional[bool] = None
    candidate_success: Optional[bool] = None

    success_parity: Optional[bool] = None
    materialization_count_parity: Optional[bool] = None
    materialization_asset_keys_parity: Optional[bool] = None
    metadata_keys_parity: Optional[bool] = None
    asset_check_parity: Optional[bool] = None

    baseline_duration_s: Optional[float] = None
    candidate_duration_s: Optional[float] = None
    duration_delta_s: Optional[float] = None
    """Always informational — duration changes are never gating per the recipe."""

    is_pass: bool = False
    """True only when every applicable parity check passed. False when
    any parity check failed OR when we couldn't decide (missing data on
    either side)."""

    notes: List[str] = Field(default_factory=list)
    """Operator-facing free-form notes: which parity check failed, what
    keys differed, what was missing on which side. UI / Markdown render
    these verbatim."""


def diff_rep(
    *,
    rep: Representative,
    class_hash: str,
    baseline: Optional[RunRecord],
    candidate: Optional[RunRecord],
) -> RepDiff:
    """Compute a :class:`RepDiff` for one representative."""
    diff = RepDiff(
        class_hash=class_hash,
        asset_key=list(rep.asset_key),
        repo=rep.repo,
        git_sha=rep.git_sha,
        runnability=rep.runnability.value,
    )

    if baseline is None and candidate is None:
        diff.notes.append("no run records on either side — both sides skipped or pending")
        return diff
    if baseline is None:
        diff.notes.append("baseline run record missing")
        if candidate is not None:
            diff.candidate_run_id = candidate.run_id
            diff.candidate_status = candidate.status
            diff.candidate_success = candidate.success
        return diff
    if candidate is None:
        diff.notes.append("candidate run record missing")
        diff.baseline_run_id = baseline.run_id
        diff.baseline_status = baseline.status
        diff.baseline_success = baseline.success
        return diff

    # Both records present — populate the side metadata.
    diff.baseline_run_id = baseline.run_id
    diff.candidate_run_id = candidate.run_id
    diff.baseline_status = baseline.status
    diff.candidate_status = candidate.status
    diff.baseline_success = baseline.success
    diff.candidate_success = candidate.success
    diff.baseline_duration_s = baseline.duration_seconds
    diff.candidate_duration_s = candidate.duration_seconds
    if baseline.duration_seconds is not None and candidate.duration_seconds is not None:
        diff.duration_delta_s = candidate.duration_seconds - baseline.duration_seconds

    # --- 1. Success parity --------------------------------------------------
    diff.success_parity = baseline.success == candidate.success
    if not diff.success_parity:
        diff.notes.append(
            f"success parity: baseline={baseline.success} candidate={candidate.success}"
        )

    # --- 2. Materialization count parity -----------------------------------
    b_mats = baseline.materialization_events
    c_mats = candidate.materialization_events
    diff.materialization_count_parity = len(b_mats) == len(c_mats)
    if not diff.materialization_count_parity:
        diff.notes.append(
            f"materialization count: baseline={len(b_mats)} candidate={len(c_mats)}"
        )

    # --- 3. Materialization asset keys parity ------------------------------
    b_keys = {tuple(m.asset_key) for m in b_mats}
    c_keys = {tuple(m.asset_key) for m in c_mats}
    diff.materialization_asset_keys_parity = b_keys == c_keys
    if not diff.materialization_asset_keys_parity:
        only_b = b_keys - c_keys
        only_c = c_keys - b_keys
        bits = []
        if only_b:
            bits.append(f"baseline-only: {sorted(only_b)}")
        if only_c:
            bits.append(f"candidate-only: {sorted(only_c)}")
        diff.notes.append("materialization asset keys differ — " + "; ".join(bits))

    # --- 4. Metadata keys parity (key set; values may differ) --------------
    b_meta = set(baseline.metadata_keys)
    c_meta = set(candidate.metadata_keys)
    diff.metadata_keys_parity = b_meta == c_meta
    if not diff.metadata_keys_parity:
        only_b = b_meta - c_meta
        only_c = c_meta - b_meta
        bits = []
        if only_b:
            bits.append(f"baseline-only: {sorted(only_b)}")
        if only_c:
            bits.append(f"candidate-only: {sorted(only_c)}")
        diff.notes.append("metadata key set differs — " + "; ".join(bits))

    # --- 5. Asset-check parity (same (key, name) and same passed) ---------
    b_checks = _check_signature(baseline.asset_check_results)
    c_checks = _check_signature(candidate.asset_check_results)
    diff.asset_check_parity = b_checks == c_checks
    if not diff.asset_check_parity:
        diff.notes.append(
            f"asset-check parity broke: baseline={sorted(b_checks)} candidate={sorted(c_checks)}"
        )

    # --- Overall verdict ---------------------------------------------------
    parity_fields = (
        diff.success_parity,
        diff.materialization_count_parity,
        diff.materialization_asset_keys_parity,
        diff.metadata_keys_parity,
        diff.asset_check_parity,
    )
    diff.is_pass = all(p is True for p in parity_fields)

    return diff


def _check_signature(results) -> set:
    """Reduce an asset_check_results list to a hashable set of
    ``(asset_key_tuple, check_name, passed)`` tuples for parity comparison."""
    out = set()
    for r in results or []:
        try:
            out.add((tuple(r.asset_key), r.check_name, bool(r.passed) if r.passed is not None else None))
        except Exception:
            continue
    return out


# ---------------------------------------------------------------------------
# Per-class verdict roll-up
# ---------------------------------------------------------------------------


class ClassVerdict(BaseModel):
    """Roll-up of the diff for one equivalence class."""
    model_config = ConfigDict(extra="ignore")

    class_hash: str
    runnability: str
    """The class's runnability label, taken from the first representative.
    A class always has uniform runnability across reps in practice (it's
    derived from tags); recording it here makes the verdict roll-up
    legible without re-reading the class matrix."""

    member_count: int
    rep_count: int
    rep_diffs: List[RepDiff]

    is_green: bool
    """True iff EVERY RUNNABLE representative's RepDiff has ``is_pass=True``.
    Non-runnable classes (SYNTHETIC_REQUIRED, OBSERVE_ONLY) are reported
    separately — they're not part of the runnable-green criterion."""

    failure_summary: Optional[str] = None
    """Short operator-facing summary when the class isn't green. None when
    is_green is True."""


def build_class_verdicts(
    matrix: ClassMatrix,
    *,
    diff_by_rep_id: Dict[str, RepDiff],
) -> List[ClassVerdict]:
    """Roll RepDiffs up into per-class verdicts.

    ``diff_by_rep_id`` is keyed by the same ``rep_id`` the runs state uses
    so the caller can populate it without re-deriving keys here.
    """
    verdicts: List[ClassVerdict] = []
    for cls in matrix.classes:
        per_class: List[RepDiff] = []
        runnability_label = (
            cls.representatives[0].runnability.value
            if cls.representatives else Runnability.RUNNABLE.value
        )
        for rep in cls.representatives:
            rep_id = f"{cls.class_hash}:{'/'.join(rep.asset_key)}"
            d = diff_by_rep_id.get(rep_id)
            if d is None:
                d = RepDiff(
                    class_hash=cls.class_hash,
                    asset_key=list(rep.asset_key),
                    repo=rep.repo, git_sha=rep.git_sha,
                    runnability=rep.runnability.value,
                    notes=["no diff produced — rep missing from both sides' state"],
                )
            per_class.append(d)

        # is_green: every RUNNABLE rep must pass. Non-RUNNABLE classes
        # are not gated on runnable parity (they're gated separately at
        # the verdict level — synthetic via probes, observe-only via
        # other comparison paths).
        if runnability_label == Runnability.RUNNABLE.value:
            is_green = bool(per_class) and all(d.is_pass for d in per_class)
            failure_summary = (
                None if is_green
                else _summarize_class_failure(per_class)
            )
        else:
            is_green = True
            failure_summary = None

        verdicts.append(ClassVerdict(
            class_hash=cls.class_hash,
            runnability=runnability_label,
            member_count=cls.member_count,
            rep_count=len(cls.representatives),
            rep_diffs=per_class,
            is_green=is_green,
            failure_summary=failure_summary,
        ))
    return verdicts


def _summarize_class_failure(diffs: List[RepDiff]) -> str:
    failed = [d for d in diffs if not d.is_pass]
    if not failed:
        return "no diff data"
    asset_keys = ", ".join("/".join(d.asset_key) for d in failed[:3])
    if len(failed) > 3:
        asset_keys += f", and {len(failed) - 3} more"
    return f"{len(failed)} of {len(diffs)} rep(s) failed parity ({asset_keys})"
