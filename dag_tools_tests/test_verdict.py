"""End-to-end tests for build_verdict — moto registry with a fully-staged
qualification (manifest + classes + baseline state + candidate state +
run records + preflight).

Verifies the recipe's GO logic + the strict-by-default gap acceptance:
v1 cannot say GO unless the operator opts into the deferred orchestration
and (when applicable) synthetic coverage and co_upgrade_risks.
"""
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("yaml")

import boto3
from moto import mock_aws

from dag_tools.inventory import SCHEMA_VERSION as INV_VER
from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.preflight import publish_preflight_report
from dag_tools.qual.preflight.preflight import (
    PreflightReport, CheckResult,
)
from dag_tools.qual.qualify import (
    Deployment, VersionTarget, create_qualification,
)
from dag_tools.qual.registry import (
    BuildMeta, ImmutableKeyExists, InventoryRegistry,
    S3Storage, StorageSettings, layout,
)
from dag_tools.qual.runs.records import (
    AssetCheckResultSummary, MaterializationEventSummary, RunRecord,
)
from dag_tools.qual.runs.state import (
    QualRunState, RepState, RepStatus, rep_id_for,
)
from dag_tools.qual.verdict import (
    GapAcceptance, Verdict, VerdictStatus,
    build_verdict, publish_verdict, render_markdown,
)


BUCKET = "dag-tools-verdict-test"


# ---------------------------------------------------------------------------
# Heavy fixture: stage a complete qualification end-to-end
# ---------------------------------------------------------------------------


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _publish_inventory(registry, repo, sha, asset_records):
    """Drop one inventory publish into the registry."""
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    artifacts = {
        layout.ASSETS_FILE: json.dumps({
            "schema_version": 1,
            "inventory_schema_version": INV_VER,
            "records": asset_records,
        }).encode("utf-8"),
        layout.AUTOMATION_FILE: b'{"schema_version":1,"sensors":[],"schedules":[],"asset_checks":[]}',
        layout.IO_MANAGERS_FILE: b'{"schema_version":1,"entries":[]}',
        layout.DBT_PROJECTS_FILE: b'{"schema_version":1,"projects":[]}',
        layout.LOAD_VALIDATION_FILE: b'{"schema_version":1,"timestamp":"2026-06-15T00:00:00+00:00","loads":true,"locations":[],"failures":[],"warnings":[]}',
    }
    registry.publish_build(
        repo=repo, git_sha=sha, artifacts=artifacts,
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def _asset(asset_key, tags: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    return {
        "schema_version": INV_VER,
        "asset_key": list(asset_key),
        "compute_kind": "python",
        "io_manager_key": "io_manager",
        "io_manager_class": "dagster.InMemoryIOManager",
        "io_manager_family": "in_memory",
        "partitions_def_class": None,
        "partition_mapping_classes": [],
        "resource_keys": ["io_manager"],
        "resource_classes": {"io_manager": "dagster.InMemoryIOManager"},
        "integration_libs": [],
        "has_asset_checks": False,
        "automation_condition_type": None,
        "tags": tags or {},
    }


def _seed_full_qualification(
    registry,
    *,
    qual_id="q-test",
    co_upgrade_risks: Optional[List[Dict[str, str]]] = None,
    asset_tags: Optional[Dict[str, str]] = None,
):
    """Manifest + class matrix. Returns (manifest, matrix)."""
    _publish_inventory(registry, "alpha", "sha-alpha", [_asset(["hello"], tags=asset_tags)])

    risks = co_upgrade_risks or []
    create_qualification(
        qual_id=qual_id, registry=registry,
        baseline=VersionTarget(
            dagster="1.10.6",
            pins={r["lib"]: r["from"] for r in risks},
        ),
        candidate=VersionTarget(
            dagster="1.12.1",
            pins={r["lib"]: r["to"] for r in risks},
        ),
        deployment=Deployment(graphql_url="http://test/graphql"),
    )
    matrix = build_class_matrix(qual_id, registry=registry)
    publish_class_matrix(matrix, registry=registry)
    return matrix


def _publish_state_and_records(
    registry, qual_id, side, matrix,
    *,
    rep_status=RepStatus.PASSED,
    record_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
):
    """Synthesize a state file with one rep per class + matching run records."""
    overrides = record_overrides or {}
    reps_dict: Dict[str, Dict[str, Any]] = {}
    for cls in matrix.classes:
        for rep in cls.representatives:
            rid = rep_id_for(cls.class_hash, rep.asset_key)
            run_id = f"run-{side}-{cls.class_hash[:6]}"
            reps_dict[rid] = {
                "rep_id": rid,
                "class_hash": cls.class_hash,
                "asset_key": list(rep.asset_key),
                "repo": rep.repo,
                "git_sha": rep.git_sha,
                "runnability": rep.runnability.value,
                "status": rep_status.value,
                "run_id": run_id,
                "attempts": 1,
            }
            # Write the matching run record.
            override = overrides.get(rid, {})
            rec = RunRecord(
                qual_id=qual_id, side=side,
                class_hash=cls.class_hash, asset_key=list(rep.asset_key),
                repo=rep.repo, git_sha=rep.git_sha,
                run_id=run_id,
                success=override.get("success", True),
                status=override.get("status", "SUCCESS" if override.get("success", True) else "FAILURE"),
                materialization_events=override.get("materialization_events", [
                    MaterializationEventSummary(asset_key=list(rep.asset_key), metadata_keys=["row_count"]),
                ]),
                metadata_keys=override.get("metadata_keys", ["row_count"]),
                asset_check_results=override.get("asset_check_results", []),
                duration_seconds=10.0,
                event_count=1,
            )
            registry.put_run_record(
                qual_id=qual_id, side=side,
                class_hash=cls.class_hash, run_id=run_id,
                body=rec.model_dump_json().encode("utf-8"),
            )
    state = {
        "schema_version": 1,
        "qual_id": qual_id, "side": side,
        "started_at": "2026-06-15T00:00:00+00:00",
        "updated_at": "2026-06-15T00:00:00+00:00",
        "reps": reps_dict,
    }
    registry.put_side_state(qual_id, side, json.dumps(state).encode())


def _publish_preflight(registry, qual_id, side, passed=True):
    """Stand in a candidate-side preflight report."""
    report = PreflightReport(
        qual_id=qual_id, side=side,
        generated_at=datetime.now(timezone.utc),
        deployment_version="1.12.1",
        expected_version="1.12.1",
        checks=[
            CheckResult(name="dagster_version", passed=True, detail="ok"),
            CheckResult(name="code_locations_loaded", passed=passed,
                        detail="all loaded" if passed else "broken loc errored"),
        ],
        passed=passed,
    )
    publish_preflight_report(report, registry=registry)


# ---------------------------------------------------------------------------
# Default-strict behavior: every gap blocks GO unless accepted
# ---------------------------------------------------------------------------


def test_verdict_no_go_by_default_when_orchestration_not_accepted(setup):
    """With all artifacts green but orchestration deferred, default config
    must NO_GO until the operator passes the accept flag."""
    registry = setup
    matrix = _seed_full_qualification(registry)
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    _publish_state_and_records(registry, "q-test", "candidate", matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    verdict = build_verdict("q-test", registry=registry)
    assert verdict.status == VerdictStatus.NO_GO
    assert any("orchestration" in i for i in verdict.blocking_issues)


def test_verdict_go_when_all_gates_pass_and_known_gaps_accepted(setup):
    """The clean happy path: every parity check passes, preflight passes,
    operator explicitly accepts orchestration deferral (no co_upgrade_risks
    in this qualification, no synthetic classes)."""
    registry = setup
    matrix = _seed_full_qualification(registry)
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    _publish_state_and_records(registry, "q-test", "candidate", matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    assert verdict.status == VerdictStatus.GO
    assert verdict.blocking_issues == []
    assert verdict.runnable_classes_green is True
    assert verdict.preflight_passed is True


# ---------------------------------------------------------------------------
# Hard gates
# ---------------------------------------------------------------------------


def test_verdict_no_go_when_candidate_preflight_failed(setup):
    """Recipe rule: candidate preflight is a hard gate. Even with full
    accept flags, a failed preflight blocks GO."""
    registry = setup
    matrix = _seed_full_qualification(registry)
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    _publish_state_and_records(registry, "q-test", "candidate", matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=False)

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True,
                           co_upgrade_risks=True,
                           synthetic_coverage_missing=True),
    )
    assert verdict.status == VerdictStatus.NO_GO
    assert verdict.preflight_passed is False
    assert any("preflight failed" in i for i in verdict.blocking_issues)


def test_verdict_no_go_when_runnable_class_parity_breaks(setup):
    """One rep's candidate run records a different success state ->
    class is red -> verdict is NO_GO."""
    registry = setup
    matrix = _seed_full_qualification(registry)
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    # Force the candidate's record to be a failure.
    rid = next(iter([
        rep_id_for(c.class_hash, r.asset_key)
        for c in matrix.classes for r in c.representatives
    ]))
    _publish_state_and_records(
        registry, "q-test", "candidate", matrix,
        record_overrides={rid: {"success": False, "status": "FAILURE"}},
    )
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    assert verdict.status == VerdictStatus.NO_GO
    assert verdict.runnable_classes_green is False
    assert verdict.runnable_classes_red


def test_verdict_no_go_when_baseline_state_missing(setup):
    """No baseline run yet -> can't decide -> NO_GO with actionable issue."""
    registry = setup
    matrix = _seed_full_qualification(registry)
    # Only publish candidate state, not baseline.
    _publish_state_and_records(registry, "q-test", "candidate", matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    assert verdict.status == VerdictStatus.NO_GO
    assert any("baseline state" in i for i in verdict.blocking_issues)


def test_verdict_no_go_when_candidate_state_missing(setup):
    registry = setup
    matrix = _seed_full_qualification(registry)
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    # No candidate state at all (and no preflight either — also blocking).

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    assert verdict.status == VerdictStatus.NO_GO
    assert any("candidate state" in i for i in verdict.blocking_issues)
    assert any("preflight" in i for i in verdict.blocking_issues)


# ---------------------------------------------------------------------------
# co_upgrade_risks and synthetic-coverage gate
# ---------------------------------------------------------------------------


def test_verdict_no_go_when_co_upgrade_risks_unaccepted(setup):
    registry = setup
    matrix = _seed_full_qualification(
        registry,
        co_upgrade_risks=[{"lib": "dbt-core", "from": "1.8.0", "to": "1.9.0", "severity": "warning"}],
    )
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    _publish_state_and_records(registry, "q-test", "candidate", matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    assert verdict.status == VerdictStatus.NO_GO
    assert verdict.co_upgrade_risks_total == 1
    assert any("co_upgrade_risk" in i for i in verdict.blocking_issues)


def test_verdict_go_when_co_upgrade_risks_accepted(setup):
    registry = setup
    matrix = _seed_full_qualification(
        registry,
        co_upgrade_risks=[{"lib": "dbt-core", "from": "1.8.0", "to": "1.9.0", "severity": "warning"}],
    )
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    _publish_state_and_records(registry, "q-test", "candidate", matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True, co_upgrade_risks=True),
    )
    assert verdict.status == VerdictStatus.GO


def test_verdict_no_go_when_synthetic_classes_without_probe_coverage(setup):
    """An asset tagged synthetic_required exists -> 1 synthetic class ->
    NO_GO until --accept-synthetic-coverage-missing."""
    registry = setup
    matrix = _seed_full_qualification(
        registry,
        asset_tags={"synthetic_required": "true"},
    )
    _publish_state_and_records(registry, "q-test", "baseline", matrix, rep_status=RepStatus.SKIPPED)
    _publish_state_and_records(registry, "q-test", "candidate", matrix, rep_status=RepStatus.SKIPPED)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    assert verdict.status == VerdictStatus.NO_GO
    assert verdict.synthetic_classes_total == 1
    assert verdict.synthetic_classes_with_probe_coverage == 0
    assert any("synthetic" in i for i in verdict.blocking_issues)


# ---------------------------------------------------------------------------
# Q5c probe coverage feeds Q6 verdict
# ---------------------------------------------------------------------------


def _publish_probe_state(registry, qual_id, side, class_hashes, status: str):
    """Synthesize a ProbeRunState with one probe per class_hash at the
    given status — lets verdict tests assert Q6's coverage rollup
    without exercising the runner."""
    probes_dict = {
        ch: {
            "class_hash": ch,
            "module_name": f"probe_{ch[:8]}",
            "status": status,
            "run_id": f"probe-run-{side}-{ch[:6]}" if status in ("passed", "failed") else None,
            "attempts": 1 if status in ("passed", "failed") else 0,
        }
        for ch in class_hashes
    }
    state = {
        "schema_version": 1,
        "qual_id": qual_id, "side": side,
        "started_at": "2026-06-15T00:00:00+00:00",
        "updated_at": "2026-06-15T00:00:00+00:00",
        "probes": probes_dict,
    }
    registry.put_probes_state(qual_id, side, json.dumps(state).encode())


def test_verdict_go_when_probes_pass_on_both_sides(setup):
    """Q5c integration: a synthetic class with PASSED probes on baseline
    AND candidate counts as covered, removes the missing-coverage blocker,
    and contributes to GO without --accept-synthetic-coverage-missing."""
    registry = setup
    matrix = _seed_full_qualification(
        registry, asset_tags={"synthetic_required": "true"},
    )
    _publish_state_and_records(registry, "q-test", "baseline", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_state_and_records(registry, "q-test", "candidate", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    class_hashes = [c.class_hash for c in matrix.classes]
    _publish_probe_state(registry, "q-test", "baseline", class_hashes, "passed")
    _publish_probe_state(registry, "q-test", "candidate", class_hashes, "passed")

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),  # NO synthetic accept
    )
    assert verdict.status == VerdictStatus.GO, verdict.blocking_issues
    assert verdict.synthetic_classes_with_probe_coverage == 1
    assert verdict.synthetic_classes_red == []


def test_verdict_no_go_when_probe_failed_even_with_synthetic_accept(setup):
    """A probe that RAN and FAILED is a real regression signal — it must
    block GO regardless of --accept-synthetic-coverage-missing (which
    only excuses *missing* coverage, not actively-failing probes)."""
    registry = setup
    matrix = _seed_full_qualification(
        registry, asset_tags={"synthetic_required": "true"},
    )
    _publish_state_and_records(registry, "q-test", "baseline", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_state_and_records(registry, "q-test", "candidate", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    class_hashes = [c.class_hash for c in matrix.classes]
    _publish_probe_state(registry, "q-test", "baseline", class_hashes, "passed")
    _publish_probe_state(registry, "q-test", "candidate", class_hashes, "failed")

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(
            orchestration_deferred=True,
            synthetic_coverage_missing=True,  # even with this opt-in
        ),
    )
    assert verdict.status == VerdictStatus.NO_GO
    assert verdict.synthetic_classes_red == class_hashes
    assert any("failing probes" in i for i in verdict.blocking_issues)


def test_verdict_partial_probe_coverage_still_blocks(setup):
    """Probes deployed only on one side → still uncovered → blocked
    unless --accept-synthetic-coverage-missing."""
    registry = setup
    matrix = _seed_full_qualification(
        registry, asset_tags={"synthetic_required": "true"},
    )
    _publish_state_and_records(registry, "q-test", "baseline", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_state_and_records(registry, "q-test", "candidate", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    class_hashes = [c.class_hash for c in matrix.classes]
    # Only baseline has probe runs; candidate side hasn't run probes yet.
    _publish_probe_state(registry, "q-test", "baseline", class_hashes, "passed")

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    assert verdict.status == VerdictStatus.NO_GO
    assert verdict.synthetic_classes_with_probe_coverage == 0
    assert any("no probe coverage" in i for i in verdict.blocking_issues)


# ---------------------------------------------------------------------------
# Q5e probe RunRecord diff in Q6 — divergent-but-passing probes
# ---------------------------------------------------------------------------


def _publish_probe_record(registry, qual_id, side, class_hash, *,
                          materialization_keys, metadata_keys,
                          asset_check_results=None):
    """Persist a probe RunRecord under the class_hash + run_id we
    synthesized in _publish_probe_state. Matching the runner's persisted
    shape lets Q6 read + diff."""
    run_id = f"probe-run-{side}-{class_hash[:6]}"
    record = RunRecord(
        qual_id=qual_id, side=side, class_hash=class_hash,
        asset_key=[f"probe_{class_hash[:8]}_downstream"],
        repo="dag-tools-probes", git_sha=class_hash[:12],
        run_id=run_id, success=True, status="SUCCESS",
        materialization_events=[
            MaterializationEventSummary(asset_key=list(k), metadata_keys=metadata_keys)
            for k in materialization_keys
        ],
        metadata_keys=metadata_keys,
        asset_check_results=asset_check_results or [],
        duration_seconds=2.0, event_count=len(materialization_keys),
    )
    registry.put_probe_run_record(
        qual_id=qual_id, side=side, class_hash=class_hash, run_id=run_id,
        body=record.model_dump_json().encode("utf-8"),
    )


def test_verdict_diverged_probes_blocks_go_even_with_passing_status(setup):
    """Both probe sides terminate as PASSED at the run level but produce
    DIFFERENT materialization metadata keys — this is exactly the kind
    of regression Q6 exists to surface. The class must end up in
    synthetic_classes_red and block GO regardless of acceptance flags."""
    registry = setup
    matrix = _seed_full_qualification(
        registry, asset_tags={"synthetic_required": "true"},
    )
    _publish_state_and_records(registry, "q-test", "baseline", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_state_and_records(registry, "q-test", "candidate", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    class_hashes = [c.class_hash for c in matrix.classes]
    _publish_probe_state(registry, "q-test", "baseline", class_hashes, "passed")
    _publish_probe_state(registry, "q-test", "candidate", class_hashes, "passed")

    for ch in class_hashes:
        # Baseline record: one metadata key "row_count".
        _publish_probe_record(
            registry, "q-test", "baseline", ch,
            materialization_keys=[[f"probe_{ch[:8]}_downstream"]],
            metadata_keys=["row_count"],
        )
        # Candidate record: DIFFERENT metadata key set.
        _publish_probe_record(
            registry, "q-test", "candidate", ch,
            materialization_keys=[[f"probe_{ch[:8]}_downstream"]],
            metadata_keys=["row_count", "schema_version"],
        )

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(
            orchestration_deferred=True,
            synthetic_coverage_missing=True,
        ),
    )
    assert verdict.status == VerdictStatus.NO_GO
    assert verdict.synthetic_classes_red == class_hashes
    # And the per-class probe_diff carries the divergence detail.
    for cls in verdict.class_verdicts:
        if cls.class_hash in class_hashes:
            assert cls.probe_diff is not None
            assert cls.probe_diff.metadata_keys_parity is False
            assert any("metadata key set differs" in n for n in cls.probe_diff.notes)


def test_verdict_matching_probe_records_count_as_covered(setup):
    """When the probe RunRecords match across sides (same materialization
    + metadata + check parity) AND both probes PASSED, the class is
    counted as covered — same outcome as the records-less back-compat
    path, but now backed by the actual run record diff."""
    registry = setup
    matrix = _seed_full_qualification(
        registry, asset_tags={"synthetic_required": "true"},
    )
    _publish_state_and_records(registry, "q-test", "baseline", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_state_and_records(registry, "q-test", "candidate", matrix,
                               rep_status=RepStatus.SKIPPED)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    class_hashes = [c.class_hash for c in matrix.classes]
    _publish_probe_state(registry, "q-test", "baseline", class_hashes, "passed")
    _publish_probe_state(registry, "q-test", "candidate", class_hashes, "passed")
    for ch in class_hashes:
        for side in ("baseline", "candidate"):
            _publish_probe_record(
                registry, "q-test", side, ch,
                materialization_keys=[[f"probe_{ch[:8]}_downstream"]],
                metadata_keys=["row_count"],
            )

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    assert verdict.status == VerdictStatus.GO, verdict.blocking_issues
    # ClassVerdict carries the probe_diff for operator review.
    for cls in verdict.class_verdicts:
        if cls.runnability == "synthetic_required":
            assert cls.probe_diff is not None
            assert cls.probe_diff.is_pass is True


# ---------------------------------------------------------------------------
# Publish + render
# ---------------------------------------------------------------------------


def test_publish_verdict_writes_both_json_and_md(setup):
    registry = setup
    matrix = _seed_full_qualification(registry)
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    _publish_state_and_records(registry, "q-test", "candidate", matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    publish_verdict(verdict, registry=registry)

    j = registry.read_qualification_verdict_json("q-test")
    md = registry.read_qualification_verdict_md("q-test")
    assert j is not None and md is not None
    parsed = json.loads(j)
    assert parsed["qual_id"] == "q-test"
    # The headline always appears in the MD title.
    assert b"# " in md and b"`q-test`" in md


def test_publish_verdict_immutable_by_default(setup):
    registry = setup
    matrix = _seed_full_qualification(registry)
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    _publish_state_and_records(registry, "q-test", "candidate", matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    publish_verdict(verdict, registry=registry)
    with pytest.raises(ImmutableKeyExists):
        publish_verdict(verdict, registry=registry)


def test_render_markdown_uses_go_or_nogo_headline(setup):
    registry = setup
    matrix = _seed_full_qualification(registry)
    _publish_state_and_records(registry, "q-test", "baseline", matrix)
    _publish_state_and_records(registry, "q-test", "candidate", matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=True)

    go_verdict = build_verdict(
        "q-test", registry=registry,
        gaps=GapAcceptance(orchestration_deferred=True),
    )
    no_go_verdict = build_verdict("q-test", registry=registry)  # default strict

    assert "GO" in render_markdown(go_verdict)
    assert "NO-GO" in render_markdown(no_go_verdict)


# ---------------------------------------------------------------------------
# Hard errors
# ---------------------------------------------------------------------------


def test_build_verdict_raises_when_manifest_missing(setup):
    registry = setup
    with pytest.raises(FileNotFoundError, match="run `dagtools qual init"):
        build_verdict("never-existed", registry=registry)


def test_build_verdict_raises_when_classes_missing(setup):
    registry = setup
    create_qualification(
        qual_id="naked", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://test/graphql"),
    )
    with pytest.raises(FileNotFoundError, match="run `dagtools qual classes"):
        build_verdict("naked", registry=registry)
