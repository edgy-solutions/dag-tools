"""End-to-end tests for build_class_matrix + publish_class_matrix.

Stitches the whole Q1 pipeline together with a moto-backed registry:
  - publish a couple of fake inventories (assets + dbt projects),
  - init a qualification,
  - build the class matrix,
  - publish + read back, verify shape and immutability.

Covers the load-bearing recipe invariants:
  - Q1 reads the manifest's inventory_pins, not the registry's latest.
  - Custom dbt translators force own classes.
  - The class JSON is immutable per qual_id by default.
"""
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("yaml")

import boto3
import json
from moto import mock_aws

from dag_tools.inventory import SCHEMA_VERSION as INVENTORY_SCHEMA_VERSION
from dag_tools.qual.classes import (
    Runnability,
    build_class_matrix,
    publish_class_matrix,
    render_markdown,
)
from dag_tools.qual.qualify import (
    VersionTarget,
    create_qualification,
)
from dag_tools.qual.registry import (
    BuildMeta,
    ImmutableKeyExists,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)


BUCKET = "dag-tools-classes-test"


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def registry(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _asset_record(**over):
    base = {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "asset_key": ["a"],
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
        "tags": {},
    }
    base.update(over)
    return base


def _publish_inventory(
    registry, repo, sha, records, dbt_projects=None, hours_ago=1
):
    """Drop a full survey publish into the registry — assets.json + the
    dbt_projects.json companion (so build_class_matrix's custom-translator
    join has something to find)."""
    when = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    artifacts = {
        layout.ASSETS_FILE: json.dumps({
            "schema_version": 1,
            "inventory_schema_version": INVENTORY_SCHEMA_VERSION,
            "records": records,
        }).encode("utf-8"),
        layout.DBT_PROJECTS_FILE: json.dumps({
            "schema_version": 1,
            "projects": dbt_projects or [],
        }).encode("utf-8"),
        layout.AUTOMATION_FILE: b'{"schema_version":1,"sensors":[],"schedules":[],"asset_checks":[]}',
        layout.IO_MANAGERS_FILE: b'{"schema_version":1,"entries":[]}',
        layout.LOAD_VALIDATION_FILE: b'{"schema_version":1,"timestamp":"2026-06-15T12:00:00+00:00","loads":true,"locations":[],"failures":[],"warnings":[]}',
    }
    registry.publish_build(
        repo=repo, git_sha=sha, artifacts=artifacts,
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def _init_qual(registry, qual_id="q-test"):
    create_qualification(
        qual_id=qual_id,
        registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
    )


# ---------------------------------------------------------------------------
# Happy path: two repos, three classes
# ---------------------------------------------------------------------------


def test_build_class_matrix_groups_identical_assets_across_repos(registry):
    """Two assets with identical structural shape from different repos
    should land in the SAME class — that's the fleet-merge intent."""
    rec = _asset_record(asset_key=["x"])
    _publish_inventory(registry, "alpha", "shaA", [rec])
    _publish_inventory(registry, "beta", "shaB", [dict(rec, asset_key=["y"])])
    _init_qual(registry)

    matrix = build_class_matrix("q-test", registry=registry)
    assert matrix.class_count == 1
    cls = matrix.classes[0]
    assert cls.member_count == 2
    assert cls.member_repo_count == 2
    repos = {r.repo for r in cls.representatives}
    assert repos == {"alpha", "beta"}, "reps must span both repos"


def test_build_class_matrix_splits_by_io_manager_family(registry):
    """Two assets that differ only in io_manager_class should land in
    different classes (FQN-based key)."""
    in_mem = _asset_record(asset_key=["x"])
    custom = _asset_record(
        asset_key=["y"], io_manager_class="myco.io.CustomIOManager",
    )
    _publish_inventory(registry, "alpha", "sha", [in_mem, custom])
    _init_qual(registry)

    matrix = build_class_matrix("q-test", registry=registry)
    assert matrix.class_count == 2


def test_build_class_matrix_segregates_custom_dbt_translator(registry):
    """Two dbt assets, same compute_kind + io_manager — but their repos'
    dbt_projects show different translator FQNs. They MUST land in
    different classes per recipe item 3."""
    dbt_rec = _asset_record(
        asset_key=["dbt_table"], compute_kind="dbt",
        integration_libs=["dagster_dbt"],
    )
    _publish_inventory(
        registry, "alpha", "shaA", [dbt_rec],
        dbt_projects=[{
            "schema_version": 1,
            "project_dir": "/dbt",
            "translator_class": "myco.AlphaTranslator",
            "is_custom_translator": True,
        }],
    )
    _publish_inventory(
        registry, "beta", "shaB", [dict(dbt_rec, asset_key=["dbt_other"])],
        dbt_projects=[{
            "schema_version": 1,
            "project_dir": "/dbt",
            "translator_class": "myco.BetaTranslator",
            "is_custom_translator": True,
        }],
    )
    _init_qual(registry)

    matrix = build_class_matrix("q-test", registry=registry)
    assert matrix.class_count == 2, "different translators must split classes"


def test_build_class_matrix_does_not_segregate_when_translator_is_stock(registry):
    """The translator key component is only added when ``is_custom_translator``
    is True. Stock translators don't split classes."""
    dbt_rec = _asset_record(
        asset_key=["t"], compute_kind="dbt", integration_libs=["dagster_dbt"],
    )
    _publish_inventory(
        registry, "alpha", "shaA", [dbt_rec],
        dbt_projects=[{
            "translator_class": "dagster_dbt.DagsterDbtTranslator",
            "is_custom_translator": False,
        }],
    )
    _publish_inventory(
        registry, "beta", "shaB", [dict(dbt_rec, asset_key=["u"])],
        dbt_projects=[{
            "translator_class": "dagster_dbt.DagsterDbtTranslator",
            "is_custom_translator": False,
        }],
    )
    _init_qual(registry)

    matrix = build_class_matrix("q-test", registry=registry)
    assert matrix.class_count == 1


# ---------------------------------------------------------------------------
# Runnability roll-up
# ---------------------------------------------------------------------------


def test_coverage_by_runnability_counts_representatives(registry):
    runnable = _asset_record(asset_key=["a"])
    synth = _asset_record(
        asset_key=["b"], io_manager_class="myco.SnowflakeOnly",
        tags={"synthetic_required": "true"},
    )
    obs = _asset_record(
        asset_key=["c"], io_manager_class="myco.External",
        tags={"observe_only": "true"},
    )
    _publish_inventory(registry, "alpha", "sha", [runnable, synth, obs])
    _init_qual(registry)

    matrix = build_class_matrix("q-test", registry=registry)
    coverage = matrix.coverage_by_runnability
    assert coverage[Runnability.RUNNABLE.value] >= 1
    assert coverage[Runnability.SYNTHETIC_REQUIRED.value] >= 1
    assert coverage[Runnability.OBSERVE_ONLY.value] >= 1


# ---------------------------------------------------------------------------
# Manifest pinning invariant
# ---------------------------------------------------------------------------


def test_q1_reads_pinned_sha_not_latest(registry, monkeypatch, tmp_path):
    """The load-bearing recipe invariant: Q1 uses the manifest's
    inventory_pins[], NOT the registry's current latest.json. A new survey
    publish between init and qual classes must NOT shift the qualification."""
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    rec_pinned = _asset_record(asset_key=["original"])
    _publish_inventory(registry, "alpha", "sha-original", [rec_pinned])
    _init_qual(registry, qual_id="pin-test")

    # New survey publishes a different sha AFTER qual init.
    rec_new = _asset_record(
        asset_key=["new_one"], io_manager_class="myco.Different",
    )
    _publish_inventory(registry, "alpha", "sha-newer", [rec_new])

    matrix = build_class_matrix("pin-test", registry=registry)

    # We should see only the pinned (original) asset, not the post-init one.
    all_asset_keys = [
        m.asset_key for cls in matrix.classes for m in cls.members
    ]
    assert ["original"] in all_asset_keys
    assert ["new_one"] not in all_asset_keys


# ---------------------------------------------------------------------------
# Publish + immutability
# ---------------------------------------------------------------------------


def test_publish_class_matrix_writes_both_json_and_md(registry):
    rec = _asset_record(asset_key=["x"])
    _publish_inventory(registry, "alpha", "sha", [rec])
    _init_qual(registry)

    matrix = build_class_matrix("q-test", registry=registry)
    publish_class_matrix(matrix, registry=registry)

    json_body = registry.read_qualification_classes_json("q-test")
    md_body = registry.read_qualification_classes_md("q-test")
    assert json_body is not None and md_body is not None
    parsed = json.loads(json_body)
    assert parsed["qual_id"] == "q-test"
    assert "Equivalence-class matrix" in md_body.decode()


def test_publish_class_matrix_is_immutable_by_default(registry):
    rec = _asset_record(asset_key=["x"])
    _publish_inventory(registry, "alpha", "sha", [rec])
    _init_qual(registry)
    matrix = build_class_matrix("q-test", registry=registry)
    publish_class_matrix(matrix, registry=registry)
    with pytest.raises(ImmutableKeyExists):
        publish_class_matrix(matrix, registry=registry)


def test_publish_class_matrix_allow_overwrite_bypasses(registry):
    rec = _asset_record(asset_key=["x"])
    _publish_inventory(registry, "alpha", "sha", [rec])
    _init_qual(registry)
    matrix = build_class_matrix("q-test", registry=registry)
    publish_class_matrix(matrix, registry=registry)
    publish_class_matrix(matrix, registry=registry, allow_overwrite=True)


def test_build_raises_when_manifest_missing(registry):
    """Operator clarity: if you run qual classes before qual init, fail
    fast with a message that tells you what to do."""
    with pytest.raises(FileNotFoundError, match="run `dagtools qual init"):
        build_class_matrix("never-existed", registry=registry)


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def test_render_markdown_includes_class_table_and_coverage(registry):
    rec = _asset_record(asset_key=["a"])
    _publish_inventory(registry, "alpha", "sha", [rec])
    _init_qual(registry)
    matrix = build_class_matrix("q-test", registry=registry)
    md = render_markdown(matrix)

    assert "# Equivalence-class matrix — `q-test`" in md
    assert "## Coverage" in md
    assert "## Classes" in md
    assert "| Class hash |" in md  # header row of the table
