"""End-to-end CLI tests for ``dagtools qual synthetic``."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("typer")
pytest.importorskip("yaml")

import boto3
from moto import mock_aws
from typer.testing import CliRunner

from dag_tools.inventory import SCHEMA_VERSION as INV_VER
from dag_tools.qual.cli import app
from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.qualify import VersionTarget, create_qualification
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)


BUCKET = "dag-tools-qual-synthetic-cli-test"


def _publish_asset(registry, repo, sha, *,
                   asset_key, io_manager_class, io_manager_family,
                   tags=None):
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    rec = {
        "schema_version": INV_VER,
        "asset_key": list(asset_key),
        "compute_kind": "python",
        "io_manager_key": "io_manager",
        "io_manager_class": io_manager_class,
        "io_manager_family": io_manager_family,
        "partitions_def_class": None,
        "partition_mapping_classes": [],
        "resource_keys": ["io_manager"],
        "resource_classes": {"io_manager": io_manager_class},
        "integration_libs": [],
        "has_asset_checks": False,
        "automation_condition_type": None,
        "tags": tags or {},
    }
    artifacts = {
        layout.ASSETS_FILE: json.dumps({
            "schema_version": 1,
            "inventory_schema_version": INV_VER,
            "records": [rec],
        }).encode("utf-8"),
        layout.AUTOMATION_FILE: b'{"schema_version":1,"sensors":[],"schedules":[],"asset_checks":[]}',
        layout.IO_MANAGERS_FILE: b'{"schema_version":1,"entries":[]}',
        layout.DBT_PROJECTS_FILE: b'{"schema_version":1,"projects":[]}',
        layout.LOAD_VALIDATION_FILE: b'{"schema_version":1,"timestamp":"2026-06-15T12:00:00+00:00","loads":true,"locations":[],"failures":[],"warnings":[]}',
    }
    registry.publish_build(
        repo=repo, git_sha=sha, artifacts=artifacts,
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def _setup(reg, qual_id="q-test"):
    _publish_asset(
        reg, "alpha", "shaA",
        asset_key=["x"],
        io_manager_class="dagster.InMemoryIOManager",
        io_manager_family="in_memory",
    )
    _publish_asset(
        reg, "beta", "shaB",
        asset_key=["y"],
        io_manager_class="dagster_snowflake.SnowflakeIOManager",
        io_manager_family="snowflake",
        tags={"synthetic_required": "true"},
    )
    create_qualification(
        qual_id=qual_id, registry=reg,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
    )
    matrix = build_class_matrix(qual_id, registry=reg)
    publish_class_matrix(matrix, registry=reg)


@pytest.fixture
def home_override(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    return tmp_path


def test_qual_synthetic_happy_path_writes_both_registry_and_local(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup(reg)

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "synthetic", "--id", "q-test"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["qual_id"] == "q-test"
        assert payload["published_to_registry"] is True
        assert payload["local_path"]
        assert Path(payload["local_path"]).exists()
        assert (Path(payload["local_path"]) / "probe_manifest.json").exists()

        # Registry side has it too.
        assert reg.read_probe_manifest("q-test") is not None


def test_qual_synthetic_skip_publish(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup(reg)

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "synthetic", "--id", "q-test", "--skip-publish"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["published_to_registry"] is False
        # Registry was NOT written.
        assert reg.read_probe_manifest("q-test") is None
        # Local was.
        assert Path(payload["local_path"]).exists()


def test_qual_synthetic_skip_local(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup(reg)

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "synthetic", "--id", "q-test", "--skip-local"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["local_path"] is None
        assert payload["published_to_registry"] is True
        assert reg.read_probe_manifest("q-test") is not None


def test_qual_synthetic_table_format(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup(reg)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "synthetic", "--id", "q-test", "--format", "table"],
        )
    assert result.exit_code == 0, result.output
    assert "qual synthetic" in result.output
    assert "q-test" in result.output


def test_qual_synthetic_missing_manifest_exits_clean(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "synthetic", "--id", "never-existed"],
        )
    assert result.exit_code == 2
    assert "no qualification manifest" in result.output


def test_qual_synthetic_refuses_second_publish_without_allow_overwrite(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup(reg)

        first = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "synthetic", "--id", "q-test"],
        )
        assert first.exit_code == 0, first.output

        second = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "synthetic", "--id", "q-test"],
        )
        assert second.exit_code == 2
        assert "probe bundle publish failed" in second.output

        third = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "synthetic", "--id", "q-test", "--allow-overwrite"],
        )
        assert third.exit_code == 0, third.output
