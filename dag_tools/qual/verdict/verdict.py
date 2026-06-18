"""Q6 verdict orchestrator + `UPGRADE_VERDICT.md` / `verdict.json` writer.

Recipe verdict logic:

  > **GO when:**
  > 1. Candidate preflight: all code locations load on the test deployment
  >    (hard gate).
  > 2. All RUNNABLE classes green; all SYNTHETIC_REQUIRED classes green
  >    via probes.
  > 3. All orchestration diffs empty or signed off with citations.
  > 4. All co_upgrade_risks separately validated or pinned back.

Phase 2 Q6 v1 evaluates 1 + 2(part a, runnable). Q5-dependent and
orchestration-dependent pieces are surfaced as **blocking issues** unless
the operator opts in via accept flags. co_upgrade_risks default to
blocking until the operator passes ``--accept-co-upgrade-risks``.

The verdict is **STRICT by default** — operators must explicitly accept
each known gap. That matches the recipe's intent: the report calls out
what's untested so the operator decides, not the tool.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field

from ..classes import ClassMatrix, Runnability
from ..probes.state import ProbeRepState, ProbeRepStatus, ProbeRunState
from ..qualify import QualificationManifest
from ..registry import InventoryRegistry, layout
from ..runs.records import RunRecord
from ..runs.state import QualRunState, RepStatus
from .diff import ClassVerdict, RepDiff, build_class_verdicts, diff_rep


logger = logging.getLogger(__name__)


SCHEMA_VERSION = 1


class VerdictStatus(str, Enum):
    GO = "go"
    NO_GO = "no_go"


class GapAcceptance(BaseModel):
    """Operator opt-in for known gaps that v1 can't auto-evaluate."""
    model_config = ConfigDict(extra="ignore")

    co_upgrade_risks: bool = False
    """When False (default), any co_upgrade_risk in the manifest blocks GO.
    Operator passes ``--accept-co-upgrade-risks`` after they've separately
    validated each one."""

    synthetic_coverage_missing: bool = False
    """When False (default), the presence of SYNTHETIC_REQUIRED classes
    without probe coverage (Q5 not yet built) blocks GO."""

    orchestration_deferred: bool = False
    """When False (default), the absence of orchestration snapshots
    blocks GO. v1 doesn't run orchestration."""


class Verdict(BaseModel):
    """The persisted ``verdict.json`` payload."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)
    qual_id: str
    generated_at: datetime

    baseline_version: str
    candidate_version: str

    status: VerdictStatus
    blocking_issues: List[str] = Field(default_factory=list)
    """Empty when status == GO; one or more reasons when NO_GO."""

    # Per-criterion evaluations (None means not evaluable in v1).
    preflight_passed: Optional[bool] = None
    runnable_classes_green: bool = False
    runnable_classes_total: int = 0
    runnable_classes_red: List[str] = Field(default_factory=list)
    """Class hashes of failing RUNNABLE classes — operator triage list."""
    synthetic_classes_total: int = 0
    synthetic_classes_with_probe_coverage: int = 0
    """Synthetic classes whose probes PASSED on BOTH sides. Counts toward
    the GO gate the same way runnable classes do — once every synthetic
    class is here, the operator no longer needs
    ``--accept-synthetic-coverage-missing``."""
    synthetic_classes_red: List[str] = Field(default_factory=list)
    """Synthetic classes whose probes RAN and FAILED on baseline or
    candidate. Unlike "no probe deployed yet" (which is opt-out-able),
    a probe that ran-and-failed is a real regression signal and blocks
    GO regardless of acceptance flags."""
    observe_only_classes_total: int = 0
    orchestration_status: str = "deferred"
    """"deferred" until orchestration snapshot support lands."""
    co_upgrade_risks_total: int = 0
    co_upgrade_risks_accepted: bool = False

    # Coverage
    asset_count: int = 0
    class_count: int = 0

    # Detail
    class_verdicts: List[ClassVerdict] = Field(default_factory=list)
    co_upgrade_risks: List[Dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def build_verdict(
    qual_id: str,
    *,
    registry: InventoryRegistry,
    gaps: Optional[GapAcceptance] = None,
    now: Optional[datetime] = None,
) -> Verdict:
    """Read every artifact under qual_id and produce the verdict.

    Hard errors (manifest or class matrix missing) raise; soft data gaps
    (one side's state not yet published) surface as blocking issues.
    """
    now = now or datetime.now(tz=timezone.utc)
    gaps = gaps or GapAcceptance()
    manifest = _read_manifest(registry, qual_id)
    matrix = _read_class_matrix(registry, qual_id)

    baseline_records, baseline_state_missing = _load_side_records(
        registry, qual_id, "baseline", matrix,
    )
    candidate_records, candidate_state_missing = _load_side_records(
        registry, qual_id, "candidate", matrix,
    )

    # --- Per-rep diffs ----------------------------------------------------
    diff_by_rep_id: Dict[str, RepDiff] = {}
    for cls in matrix.classes:
        for rep in cls.representatives:
            rep_id = f"{cls.class_hash}:{'/'.join(rep.asset_key)}"
            diff_by_rep_id[rep_id] = diff_rep(
                rep=rep, class_hash=cls.class_hash,
                baseline=baseline_records.get(rep_id),
                candidate=candidate_records.get(rep_id),
            )

    class_verdicts = build_class_verdicts(matrix, diff_by_rep_id=diff_by_rep_id)

    # --- Roll-ups ---------------------------------------------------------
    runnable_classes = [v for v in class_verdicts if v.runnability == Runnability.RUNNABLE.value]
    synthetic_classes = [v for v in class_verdicts if v.runnability == Runnability.SYNTHETIC_REQUIRED.value]
    observe_classes = [v for v in class_verdicts if v.runnability == Runnability.OBSERVE_ONLY.value]

    runnable_red = [v.class_hash for v in runnable_classes if not v.is_green]
    runnable_green = not runnable_red and bool(runnable_classes)
    if not runnable_classes:
        # No runnable classes at all — typically because every class is
        # synthetic. Treat as vacuously green so the gating decision falls
        # to the synthetic-coverage criterion.
        runnable_green = True

    # --- Synthetic coverage -----------------------------------------------
    baseline_probes_state = _load_probe_run_state(registry, qual_id, "baseline")
    candidate_probes_state = _load_probe_run_state(registry, qual_id, "candidate")
    baseline_probes = {ch: rs.status for ch, rs in (baseline_probes_state or {}).items()}
    candidate_probes = {ch: rs.status for ch, rs in (candidate_probes_state or {}).items()}

    # Build per-class probe diffs (when both records exist) — same shape
    # as the runnable-rep diff so reporting renders uniformly.
    probe_diff_by_class_hash: Dict[str, RepDiff] = {}
    for cls_v in synthetic_classes:
        ch = cls_v.class_hash
        b_state = (baseline_probes_state or {}).get(ch)
        c_state = (candidate_probes_state or {}).get(ch)
        b_record = _load_probe_record(registry, qual_id, "baseline", b_state)
        c_record = _load_probe_record(registry, qual_id, "candidate", c_state)
        if b_record is None and c_record is None:
            continue  # No records → fall back to state-based coverage.
        pseudo_rep = _probe_pseudo_rep(ch, b_state or c_state)
        probe_diff_by_class_hash[ch] = diff_rep(
            rep=pseudo_rep, class_hash=ch,
            baseline=b_record, candidate=c_record,
        )

    synthetic_green: List[str] = []
    synthetic_red: List[str] = []
    for cls_v in synthetic_classes:
        ch = cls_v.class_hash
        b = baseline_probes.get(ch)
        c = candidate_probes.get(ch)
        probe_diff = probe_diff_by_class_hash.get(ch)

        if b == ProbeRepStatus.FAILED or c == ProbeRepStatus.FAILED:
            synthetic_red.append(ch)
        elif b == ProbeRepStatus.PASSED and c == ProbeRepStatus.PASSED:
            # Both passed at the terminal level. If we ALSO have run
            # records and the diff failed, this is a divergence: probe
            # succeeded both times but produced different outputs — a
            # real regression signal, NOT covered.
            if probe_diff is not None and not probe_diff.is_pass:
                synthetic_red.append(ch)
            else:
                synthetic_green.append(ch)
        # else: probe still PENDING / LAUNCHED / not yet deployed →
        # counted under "missing coverage" via the difference below.
    synthetic_uncovered = (
        len(synthetic_classes) - len(synthetic_green) - len(synthetic_red)
    )

    # Rebuild class_verdicts now that we have probe diffs to attach.
    class_verdicts = build_class_verdicts(
        matrix,
        diff_by_rep_id=diff_by_rep_id,
        probe_diff_by_class_hash=probe_diff_by_class_hash,
    )
    # Recompute synthetic_classes against the updated verdict list so
    # both downstream consumers see the same objects.
    synthetic_classes = [
        v for v in class_verdicts
        if v.runnability == Runnability.SYNTHETIC_REQUIRED.value
    ]
    runnable_classes = [
        v for v in class_verdicts
        if v.runnability == Runnability.RUNNABLE.value
    ]
    observe_classes = [
        v for v in class_verdicts
        if v.runnability == Runnability.OBSERVE_ONLY.value
    ]

    candidate_preflight_body = registry.read_side_preflight(qual_id, "candidate")
    preflight_passed: Optional[bool] = None
    if candidate_preflight_body:
        try:
            doc = candidate_preflight_body.decode("utf-8")
            import json
            preflight_passed = bool(json.loads(doc).get("passed"))
        except Exception as e:
            logger.warning("build_verdict: bad preflight json: %s", e)

    # --- Blocking issues --------------------------------------------------
    blocking: List[str] = []
    if preflight_passed is False:
        blocking.append("candidate preflight failed — see preflight.json")
    elif preflight_passed is None:
        blocking.append("candidate preflight has not been run")
    if not runnable_green:
        blocking.append(
            f"{len(runnable_red)} runnable class(es) failed parity: "
            + ", ".join(runnable_red[:5])
            + ("..." if len(runnable_red) > 5 else "")
        )
    if synthetic_red:
        # Probe ran AND failed, OR ran on both sides but diverged — real
        # regression signal, NOT opt-out-able. The verdict's
        # ``class_verdicts[*].probe_diff`` carries the diff notes so the
        # operator can see WHAT diverged in UPGRADE_VERDICT.md.
        blocking.append(
            f"{len(synthetic_red)} synthetic-required class(es) had failing probes "
            "(probe FAILED or run records diverged): "
            + ", ".join(synthetic_red[:5])
            + ("..." if len(synthetic_red) > 5 else "")
        )
    if synthetic_uncovered > 0 and not gaps.synthetic_coverage_missing:
        blocking.append(
            f"{synthetic_uncovered} synthetic-required class(es) have no probe coverage "
            "(generate via `dagtools qual synthetic` + run via "
            "`dagtools qual probes run --side baseline|candidate`); "
            "pass --accept-synthetic-coverage-missing to ignore"
        )
    if not gaps.orchestration_deferred:
        blocking.append(
            "orchestration snapshots are not produced yet; "
            "pass --accept-orchestration-deferred to ignore"
        )
    if manifest.co_upgrade_risks and not gaps.co_upgrade_risks:
        blocking.append(
            f"{len(manifest.co_upgrade_risks)} co_upgrade_risk(s) require operator validation; "
            "pass --accept-co-upgrade-risks once each is separately validated or pinned back"
        )
    if baseline_state_missing:
        blocking.append("baseline state not found — run `dagtools qual run --side baseline`")
    if candidate_state_missing:
        blocking.append("candidate state not found — run `dagtools qual run --side candidate`")

    status = VerdictStatus.GO if not blocking else VerdictStatus.NO_GO

    return Verdict(
        qual_id=qual_id,
        generated_at=now,
        baseline_version=manifest.baseline.dagster,
        candidate_version=manifest.candidate.dagster,
        status=status,
        blocking_issues=blocking,
        preflight_passed=preflight_passed,
        runnable_classes_green=runnable_green,
        runnable_classes_total=len(runnable_classes),
        runnable_classes_red=runnable_red,
        synthetic_classes_total=len(synthetic_classes),
        synthetic_classes_with_probe_coverage=len(synthetic_green),
        synthetic_classes_red=synthetic_red,
        observe_only_classes_total=len(observe_classes),
        orchestration_status="deferred",
        co_upgrade_risks_total=len(manifest.co_upgrade_risks),
        co_upgrade_risks_accepted=gaps.co_upgrade_risks,
        asset_count=matrix.asset_count,
        class_count=matrix.class_count,
        class_verdicts=class_verdicts,
        co_upgrade_risks=[r.model_dump(by_alias=True) for r in manifest.co_upgrade_risks],
    )


def publish_verdict(
    verdict: Verdict,
    *,
    registry: InventoryRegistry,
    allow_overwrite: bool = False,
) -> None:
    """Write ``verdict.json`` + ``UPGRADE_VERDICT.md`` immutably-by-default."""
    registry.put_qualification_verdict(
        qual_id=verdict.qual_id,
        json_body=verdict.model_dump_json(indent=2).encode("utf-8"),
        markdown_body=render_markdown(verdict).encode("utf-8"),
        allow_overwrite=allow_overwrite,
    )


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def render_markdown(verdict: Verdict) -> str:
    """Render the operator-facing UPGRADE_VERDICT.md."""
    lines: List[str] = []
    headline = (
        "✅ GO" if verdict.status == VerdictStatus.GO
        else "🛑 NO-GO"
    )
    lines.append(f"# {headline} — `{verdict.qual_id}`")
    lines.append("")
    lines.append(
        f"**Baseline**: `{verdict.baseline_version}`  →  "
        f"**Candidate**: `{verdict.candidate_version}`"
    )
    lines.append("")
    lines.append(f"Generated {verdict.generated_at.isoformat()}.")
    lines.append("")

    if verdict.blocking_issues:
        lines.append("## Blocking issues")
        lines.append("")
        for issue in verdict.blocking_issues:
            lines.append(f"- {issue}")
        lines.append("")

    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- Total assets: **{verdict.asset_count}**")
    lines.append(f"- Equivalence classes: **{verdict.class_count}**")
    lines.append(f"- Runnable classes: **{verdict.runnable_classes_total}** "
                 f"(green: {verdict.runnable_classes_total - len(verdict.runnable_classes_red)}, "
                 f"red: {len(verdict.runnable_classes_red)})")
    lines.append(f"- Synthetic-required classes: **{verdict.synthetic_classes_total}** "
                 f"(probe coverage: {verdict.synthetic_classes_with_probe_coverage})")
    lines.append(f"- Observe-only classes: **{verdict.observe_only_classes_total}**")
    lines.append("")
    lines.append("## Criteria")
    lines.append("")
    lines.append(_criterion_line("Candidate preflight",
                                 verdict.preflight_passed,
                                 unknown_note="not yet run"))
    lines.append(_criterion_line("All RUNNABLE classes green",
                                 verdict.runnable_classes_green))
    if verdict.synthetic_classes_total == 0:
        synth_mark, synth_note = "✅", "no synthetic-required classes"
    else:
        covered = verdict.synthetic_classes_with_probe_coverage
        red = len(verdict.synthetic_classes_red)
        if red > 0:
            synth_mark = "🛑"
            synth_note = (
                f"{red} probe(s) FAILED; "
                f"{covered}/{verdict.synthetic_classes_total} green"
            )
        elif covered == verdict.synthetic_classes_total:
            synth_mark = "✅"
            synth_note = f"all {covered}/{verdict.synthetic_classes_total} probes passed"
        else:
            synth_mark = "⏳"
            synth_note = (
                f"{covered}/{verdict.synthetic_classes_total} via probes "
                "(deploy + run remaining probes)"
            )
    lines.append(
        f"- {synth_mark} **SYNTHETIC_REQUIRED classes covered**: {synth_note}"
    )
    lines.append(f"- ⏳ **Orchestration snapshots**: {verdict.orchestration_status}")
    risk_line = (
        "none in manifest" if verdict.co_upgrade_risks_total == 0
        else (
            f"{verdict.co_upgrade_risks_total} risk(s); "
            f"operator acceptance: {'yes' if verdict.co_upgrade_risks_accepted else 'no'}"
        )
    )
    lines.append(f"- {'✅' if verdict.co_upgrade_risks_total == 0 or verdict.co_upgrade_risks_accepted else '🛑'}"
                 f" **co_upgrade_risks**: {risk_line}")
    lines.append("")

    if verdict.co_upgrade_risks:
        lines.append("## Co-upgrade risks")
        lines.append("")
        lines.append("| Library | From | To | Severity |")
        lines.append("|---|---|---|---|")
        for r in verdict.co_upgrade_risks:
            lines.append(
                f"| `{r.get('lib','?')}` | `{r.get('from','?')}` | "
                f"`{r.get('to','?')}` | {r.get('severity','?')} |"
            )
        lines.append("")

    if verdict.runnable_classes_red:
        lines.append("## Failing RUNNABLE classes")
        lines.append("")
        red_set = set(verdict.runnable_classes_red)
        for cls in verdict.class_verdicts:
            if cls.class_hash not in red_set:
                continue
            lines.append(f"### `{cls.class_hash}`")
            lines.append("")
            lines.append(cls.failure_summary or "—")
            lines.append("")
            for diff in cls.rep_diffs:
                if diff.is_pass:
                    continue
                ak = "/".join(diff.asset_key)
                lines.append(f"- `{diff.repo}` `{ak}`")
                for note in diff.notes:
                    lines.append(f"  - {note}")
            lines.append("")

    if verdict.synthetic_classes_red:
        lines.append("## Failing SYNTHETIC_REQUIRED probes")
        lines.append("")
        red_set = set(verdict.synthetic_classes_red)
        for cls in verdict.class_verdicts:
            if cls.class_hash not in red_set:
                continue
            lines.append(f"### `{cls.class_hash}`")
            lines.append("")
            pd = cls.probe_diff
            if pd is None:
                lines.append("- probe terminal status FAILED on one or both sides")
            else:
                lines.append(
                    f"- baseline run `{pd.baseline_run_id or '-'}` "
                    f"vs candidate run `{pd.candidate_run_id or '-'}`"
                )
                for note in pd.notes:
                    lines.append(f"  - {note}")
            lines.append("")

    lines.append("## All classes")
    lines.append("")
    lines.append("| Class | Runnability | Members | Reps | Verdict |")
    lines.append("|---|---|---:|---:|---|")
    for cls in verdict.class_verdicts:
        verdict_cell = "✅ green" if cls.is_green else "🛑 red"
        if cls.runnability != Runnability.RUNNABLE.value:
            verdict_cell = f"(skipped — {cls.runnability})"
        lines.append(
            f"| `{cls.class_hash}` | {cls.runnability} | "
            f"{cls.member_count} | {cls.rep_count} | {verdict_cell} |"
        )
    lines.append("")
    return "\n".join(lines)


def _criterion_line(name: str, value: Optional[bool], *, unknown_note: str = "unknown") -> str:
    if value is True:
        return f"- ✅ **{name}**: pass"
    if value is False:
        return f"- 🛑 **{name}**: fail"
    return f"- ❓ **{name}**: {unknown_note}"


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------


def _read_manifest(registry: InventoryRegistry, qual_id: str) -> QualificationManifest:
    body = registry.read_qualification_manifest(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no qualification manifest at qual_id={qual_id!r}; "
            f"run `dagtools qual init --id {qual_id} ...` first"
        )
    return QualificationManifest.model_validate(yaml.safe_load(body))


def _read_class_matrix(registry: InventoryRegistry, qual_id: str) -> ClassMatrix:
    body = registry.read_qualification_classes_json(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no equivalence-class matrix for qual_id={qual_id!r}; "
            f"run `dagtools qual classes --id {qual_id}` first"
        )
    return ClassMatrix.model_validate_json(body)


def _load_probe_run_state(
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
) -> Dict[str, "ProbeRepState"]:
    """Return ``class_hash -> ProbeRepState`` for the side, or empty if
    probes haven't been run on that side yet. Empty dict (not None) so
    the caller can treat "no probes deployed" and "probes still
    pending" uniformly as "no coverage" — distinct from "probe failed"
    which is a real signal."""
    body = registry.read_probes_state(qual_id, side)
    if not body:
        return {}
    try:
        state = ProbeRunState.model_validate_json(body)
    except Exception as e:
        logger.warning("_load_probe_run_state: invalid state for %s/%s: %s", qual_id, side, e)
        return {}
    return dict(state.probes)


def _load_probe_record(
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    probe_state,
) -> Optional[RunRecord]:
    """Read the probe's :class:`RunRecord` from the registry, if any.

    Returns None when the probe state is missing or has no ``run_id``
    yet (PENDING / LAUNCHED-but-never-persisted). Missing records are
    treated as graceful degradation — the verdict falls back to the
    state-based PASSED/FAILED check.
    """
    if probe_state is None or probe_state.run_id is None:
        return None
    body = registry.read_probe_run_record(
        qual_id, side, probe_state.class_hash, probe_state.run_id,
    )
    if body is None:
        return None
    try:
        return RunRecord.model_validate_json(body)
    except Exception as e:
        logger.warning(
            "_load_probe_record: invalid record %s/%s/%s/%s: %s",
            qual_id, side, probe_state.class_hash, probe_state.run_id, e,
        )
        return None


def _probe_pseudo_rep(class_hash: str, probe_state):
    """Synthesize a Representative for the probe diff.

    The runner uses the same trick when building probe RunRecords — we
    feed Q2's ``build_run_record`` a pseudo-rep so the records have
    consistent shape. Here we do the symmetric thing for diffing.
    """
    from ..classes import Representative, Runnability
    module_name = probe_state.module_name if probe_state else f"probe_{class_hash[:8]}"
    return Representative(
        repo="dag-tools-probes",
        git_sha=class_hash[:12],
        asset_key=[f"{module_name}_downstream"],
        runnability=Runnability.SYNTHETIC_REQUIRED,
        runnability_reason="probe",
    )


def _load_side_records(
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    matrix: ClassMatrix,
) -> tuple[Dict[str, RunRecord], bool]:
    """Load run records for every PASSED rep in this side's state.

    Returns (rep_id -> RunRecord, state_missing). When state_missing is
    True, the side hasn't been run yet (or the registry write was lost).
    """
    state_body = registry.read_side_state(qual_id, side)
    if not state_body:
        return {}, True
    try:
        state = QualRunState.model_validate_json(state_body)
    except Exception as e:
        logger.warning("_load_side_records: invalid state for %s/%s: %s", qual_id, side, e)
        return {}, True

    out: Dict[str, RunRecord] = {}
    for rep_id, rep_state in state.reps.items():
        # Q6 only cares about reps that produced a record (PASSED or FAILED
        # with a run_id). PENDING/SKIPPED/no-run-id reps just leave the
        # corresponding diff entry empty.
        if rep_state.run_id is None:
            continue
        body = registry.read_run_record(
            qual_id, side, rep_state.class_hash, rep_state.run_id,
        )
        if body is None:
            continue
        try:
            out[rep_id] = RunRecord.model_validate_json(body)
        except Exception as e:
            logger.warning(
                "_load_side_records: invalid run record %s/%s/%s: %s",
                qual_id, side, rep_state.run_id, e,
            )
    return out, False
