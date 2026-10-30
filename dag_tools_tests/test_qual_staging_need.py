"""Phase B1 - staging-need reporting in `qual classes`.

No module-level importorskip: every test here must genuinely execute. The
matrix tests use moto directly (it is a declared test dependency); if it is
missing the module fails loudly rather than skipping.

The load-bearing rule under test: ``upstream_asset_keys is None`` means
UNKNOWN and must never classify as NONE_NEEDED.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import boto3
import pytest
from dagster import AssetKey, Definitions, asset
from moto import mock_aws

from dag_tools.inventory import SCHEMA_VERSION as INVENTORY_SCHEMA_VERSION
from dag_tools.inventory import AssetRecord
from dag_tools.inventory.extractors import extract_records
from dag_tools.qual.classes import (
    ClassMember,
    Runnability,
    StagingNeed,
    build_class_matrix,
    build_executability_index,
    classify_staging_need,
    pick_representatives,
    render_markdown,
)
from dag_tools.qual.qualify import VersionTarget, create_qualification
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)


# ---------------------------------------------------------------------------
# Extractor / schema
# ---------------------------------------------------------------------------


def _by_key(records):
    return {tuple(r.asset_key): r for r in records}


def test_asset_with_no_deps_records_empty_list_not_none():
    @asset
    def lonely():
        return 1

    rec = _by_key(extract_records(Definitions(assets=[lonely])))[("lonely",)]
    assert rec.upstream_asset_keys == []
    assert rec.upstream_asset_keys is not None


def test_declared_upstreams_are_recorded_sorted():
    @asset
    def up():
        return 1

    @asset(deps=[AssetKey(["raw", "src", "tbl"]), up])
    def down():
        return 2

    rec = _by_key(extract_records(Definitions(assets=[up, down])))[("down",)]
    assert rec.upstream_asset_keys == [["raw", "src", "tbl"], ["up"]]


def test_external_dep_stub_is_inventoried_as_non_executable():
    @asset(deps=[AssetKey(["raw", "src", "tbl"])])
    def down():
        return 2

    recs = _by_key(extract_records(Definitions(assets=[down])))
    assert recs[("raw", "src", "tbl")].is_executable is False
    assert recs[("down",)].is_executable is True


def test_schema_version_is_three():
    assert INVENTORY_SCHEMA_VERSION == 3
    assert AssetRecord(asset_key=["x"]).schema_version == 3
    # A pre-v3 record round-trips as unknown, not as "no upstreams".
    assert AssetRecord(asset_key=["x"]).upstream_asset_keys is None


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


def _member(ups, key=("m",)):
    return ClassMember(
        repo="r", git_sha="s", asset_key=list(key), upstream_asset_keys=ups
    )


def _rec(key, executable):
    return AssetRecord(asset_key=list(key), is_executable=executable)


def test_pre_v3_inventory_classifies_unknown_not_none_needed():
    need, reason = classify_staging_need(_member(None), {})
    assert need is StagingNeed.UNKNOWN
    assert need is not StagingNeed.NONE_NEEDED
    assert "re-survey" in reason
    # Still unknown even when an index is fully populated.
    idx = build_executability_index([_rec(("u",), False)])
    assert classify_staging_need(_member(None), idx)[0] is StagingNeed.UNKNOWN


def test_no_upstreams_means_none_needed():
    need, reason = classify_staging_need(_member([]), {})
    assert need is StagingNeed.NONE_NEEDED
    assert "no declared upstreams" in reason


def test_external_upstream_means_none_needed():
    idx = build_executability_index([_rec(("ext",), False)])
    need, reason = classify_staging_need(_member([["ext"]]), idx)
    assert need is StagingNeed.NONE_NEEDED
    assert "1 upstream(s) are external" in reason


def test_executable_upstream_means_staging_required():
    idx = build_executability_index([_rec(("real",), True)])
    need, reason = classify_staging_need(_member([["real"]]), idx)
    assert need is StagingNeed.REQUIRED
    assert "real" in reason


def test_upstream_executable_in_any_repo_means_staging_required():
    # Repo A holds the stub, repo B defines the real asset.
    idx = build_executability_index(
        [_rec(("shared",), False), _rec(("shared",), True)]
    )
    assert idx[("shared",)] is True
    assert classify_staging_need(_member([["shared"]]), idx)[0] is StagingNeed.REQUIRED
    # Unknown beats False when nothing is executable.
    idx2 = build_executability_index(
        [_rec(("k",), False), _rec(("k",), None)]
    )
    assert idx2[("k",)] is None


def test_upstream_with_unknown_executability_is_unknown():
    idx = build_executability_index([_rec(("u",), None)])
    need, reason = classify_staging_need(_member([["u"]]), idx)
    assert need is StagingNeed.UNKNOWN
    assert "u" in reason


def test_unresolvable_upstream_key_is_unknown():
    need, reason = classify_staging_need(_member([["ghost"]]), {})
    assert need is StagingNeed.UNKNOWN
    assert "ghost" in reason


def test_required_wins_over_unknown_when_both_apply():
    idx = build_executability_index([_rec(("real",), True)])
    need, _ = classify_staging_need(_member([["ghost"], ["real"]]), idx)
    assert need is StagingNeed.REQUIRED


# ---------------------------------------------------------------------------
# Matrix / surface
# ---------------------------------------------------------------------------

BUCKET = "dag-tools-staging-need-test"


@pytest.fixture
def registry(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _record(key, compute, ups, executable=True, tags=None):
    """Each asset gets its own compute_kind so it forms its own class and is
    therefore its own representative."""
    d = {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "asset_key": key,
        "compute_kind": compute,
        "is_executable": executable,
        "tags": tags or {},
    }
    if ups is not None:
        d["upstream_asset_keys"] = ups
    return d


def _publish(registry, repo, sha, records):
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    artifacts = {
        layout.ASSETS_FILE: json.dumps({
            "schema_version": 1,
            "inventory_schema_version": INVENTORY_SCHEMA_VERSION,
            "records": records,
        }).encode(),
        layout.DBT_PROJECTS_FILE: b'{"schema_version":1,"projects":[]}',
        layout.AUTOMATION_FILE: b'{"schema_version":1,"sensors":[],"schedules":[],"asset_checks":[]}',
        layout.IO_MANAGERS_FILE: b'{"schema_version":1,"entries":[]}',
        layout.LOAD_VALIDATION_FILE: b'{"schema_version":1,"timestamp":"2026-06-15T12:00:00+00:00","loads":true,"locations":[],"failures":[],"warnings":[]}',
    }
    registry.publish_build(
        repo=repo, git_sha=sha, artifacts=artifacts,
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def _init(registry):
    create_qualification(
        qual_id="q-test",
        registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
    )


def _fleet_records():
    return [
        _record(["a"], "ka", []),                              # runnable, none
        _record(["b"], "kb", [["a"]]),                         # required
        _record(["c"], "kc", [["ext"]]),                       # none (external)
        _record(["d"], "kd", None),                            # unknown
        _record(["ext"], "kext", [], executable=False),        # observe_only
        _record(["s"], "ks", [], tags={"synthetic_required": "true"}),
    ]


def _build(registry):
    _publish(registry, "alpha", "shaA", _fleet_records())
    _init(registry)
    return build_class_matrix("q-test", registry=registry)


def _reps(matrix):
    return {tuple(r.asset_key): r for c in matrix.classes for r in c.representatives}


def test_launchable_now_counts_only_runnable_and_unstaged(registry):
    matrix = _build(registry)
    reps = _reps(matrix)
    assert reps[("a",)].staging_need is StagingNeed.NONE_NEEDED
    assert reps[("b",)].staging_need is StagingNeed.REQUIRED
    assert reps[("c",)].staging_need is StagingNeed.NONE_NEEDED
    assert reps[("d",)].staging_need is StagingNeed.UNKNOWN
    # none_needed but not runnable: excluded.
    assert reps[("ext",)].staging_need is StagingNeed.NONE_NEEDED
    assert reps[("ext",)].runnability is Runnability.OBSERVE_ONLY
    assert reps[("s",)].runnability is Runnability.SYNTHETIC_REQUIRED
    assert matrix.launchable_now_count == 2  # a and c


def test_staging_need_does_not_change_runnability(registry):
    matrix = _build(registry)
    reps = _reps(matrix)
    assert reps[("b",)].runnability is Runnability.RUNNABLE  # REQUIRED, still runnable
    assert reps[("d",)].runnability is Runnability.RUNNABLE  # UNKNOWN, still runnable
    assert matrix.coverage_by_runnability == {
        "runnable": 4, "synthetic_required": 1, "observe_only": 1,
    }


def test_coverage_by_staging_need_sums_to_representative_count(registry):
    matrix = _build(registry)
    n_reps = sum(len(c.representatives) for c in matrix.classes)
    assert sum(matrix.coverage_by_staging_need.values()) == n_reps
    assert set(matrix.coverage_by_staging_need) == {s.value for s in StagingNeed}
    assert matrix.schema_version == 2


def test_markdown_reports_launchable_now(registry):
    md = render_markdown(_build(registry))
    assert "- Representatives by staging need:" in md
    assert "  - **none_needed**: 4" in md
    assert "  - **required**: 1" in md
    assert "  - **unknown**: 1" in md
    assert "- Launchable now (runnable, no staged upstream needed): **2**" in md
    assert "staging **required**" in md


def test_pick_representatives_without_index_yields_unknown():
    reps = pick_representatives(
        [_member([])], prefer_tag="regression", reps_per_class=1
    )
    assert reps[0].staging_need is StagingNeed.UNKNOWN
    assert reps[0].staging_need_reason == "no executability index supplied"
