"""CLI tests for ``dagtools qual report``."""
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("typer")

import boto3
from moto import mock_aws
from typer.testing import CliRunner

from dag_tools.inventory import SCHEMA_VERSION as INV_VER
from dag_tools.qual.cli import app
from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.preflight import publish_preflight_report
from dag_tools.qual.preflight.preflight import CheckResult, PreflightReport
from dag_tools.qual.qualify import (
    Deployment, VersionTarget, create_qualification,
)
from dag_tools.qual.registry import (
    BuildMeta, InventoryRegistry, S3Storage, StorageSettings, layout,
)
from dag_tools.qual.runs.records import (
    MaterializationEventSummary, RunRecord,
)
from dag_tools.qual.runs.state import RepStatus, rep_id_for


BUCKET = "dag-tools-qual-report-cli-test"


@pytest.fixture
def home_override(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    return tmp_path


def _stage_complete_qual(registry, *, candidate_preflight_passed=True):
    """Mini-fixture: one asset, manifest, classes, baseline+candidate states
    with matching run records, and a candidate preflight."""
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    rec = {
        "schema_version": INV_VER,
        "asset_key": ["hello"],
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
    registry.publish_build(
        repo="alpha", git_sha="sha-alpha",
        artifacts={
            layout.ASSETS_FILE: json.dumps({
                "schema_version": 1, "inventory_schema_version": INV_VER,
                "records": [rec],
            }).encode("utf-8"),
            layout.AUTOMATION_FILE: b'{"schema_version":1,"sensors":[],"schedules":[],"asset_checks":[]}',
            layout.IO_MANAGERS_FILE: b'{"schema_version":1,"entries":[]}',
            layout.DBT_PROJECTS_FILE: b'{"schema_version":1,"projects":[]}',
            layout.LOAD_VALIDATION_FILE: b'{"schema_version":1,"timestamp":"2026-06-15T00:00:00+00:00","loads":true,"locations":[],"failures":[],"warnings":[]}',
        },
        meta=BuildMeta(repo="alpha", git_sha="sha-alpha", timestamp=when),
    )
    create_qualification(
        qual_id="q1", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://test/graphql"),
    )
    matrix = build_class_matrix("q1", registry=registry)
    publish_class_matrix(matrix, registry=registry)

    for side in ("baseline", "candidate"):
        reps = {}
        for cls in matrix.classes:
            for r in cls.representatives:
                rid = rep_id_for(cls.class_hash, r.asset_key)
                run_id = f"run-{side}-{cls.class_hash[:6]}"
                reps[rid] = {
                    "rep_id": rid,
                    "class_hash": cls.class_hash,
                    "asset_key": list(r.asset_key),
                    "repo": r.repo, "git_sha": r.git_sha,
                    "runnability": r.runnability.value,
                    "status": "passed", "run_id": run_id,
                    "attempts": 1,
                }
                record = RunRecord(
                    qual_id="q1", side=side,
                    class_hash=cls.class_hash, asset_key=list(r.asset_key),
                    repo=r.repo, git_sha=r.git_sha,
                    run_id=run_id, success=True, status="SUCCESS",
                    materialization_events=[
                        MaterializationEventSummary(asset_key=list(r.asset_key), metadata_keys=["row_count"]),
                    ],
                    metadata_keys=["row_count"], asset_check_results=[],
                    duration_seconds=10.0, event_count=1,
                )
                registry.put_run_record(
                    qual_id="q1", side=side,
                    class_hash=cls.class_hash, run_id=run_id,
                    body=record.model_dump_json().encode(),
                )
        state = {
            "schema_version": 1,
            "qual_id": "q1", "side": side,
            "started_at": "2026-06-15T00:00:00+00:00",
            "updated_at": "2026-06-15T00:00:00+00:00",
            "reps": reps,
        }
        registry.put_side_state("q1", side, json.dumps(state).encode())

    # Candidate-side preflight
    publish_preflight_report(
        PreflightReport(
            qual_id="q1", side="candidate",
            generated_at=datetime.now(timezone.utc),
            deployment_version="1.12.1", expected_version="1.12.1",
            checks=[
                CheckResult(name="dagster_version", passed=True, detail="ok"),
                CheckResult(name="code_locations_loaded", passed=candidate_preflight_passed,
                            detail="all loaded" if candidate_preflight_passed else "broken"),
            ],
            passed=candidate_preflight_passed,
        ),
        registry=registry,
    )


def test_dagtools_qual_report_returns_no_go_by_default(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _stage_complete_qual(reg)

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "report", "--id", "q1"],
        )
    # Default-strict: orchestration not accepted -> NO_GO -> exit 2.
    assert result.exit_code == 2, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "no_go"
    assert any("orchestration" in i for i in payload["blocking_issues"])


def test_dagtools_qual_report_goes_when_operator_accepts_gaps(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _stage_complete_qual(reg)

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "report", "--id", "q1",
             "--accept-orchestration-deferred"],
        )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "go"
    # The verdict.json + UPGRADE_VERDICT.md both landed.
    with mock_aws():
        # re-create the registry view for the next assertion in same mock context
        pass


def test_dagtools_qual_report_writes_both_artifacts(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _stage_complete_qual(reg)

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "report", "--id", "q1",
             "--accept-orchestration-deferred"],
        )
        assert result.exit_code == 0, result.output
        assert reg.read_qualification_verdict_json("q1") is not None
        assert reg.read_qualification_verdict_md("q1") is not None


def test_dagtools_qual_report_table_format(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _stage_complete_qual(reg)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "report", "--id", "q1",
             "--accept-orchestration-deferred",
             "--format", "table"],
        )
    assert result.exit_code == 0, result.output
    assert "qual report:" in result.output
    assert "=> GO" in result.output


def test_dagtools_qual_report_missing_manifest_exits_clean(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "report", "--id", "never-existed"],
        )
    assert result.exit_code == 2
    assert "no qualification manifest" in result.output
