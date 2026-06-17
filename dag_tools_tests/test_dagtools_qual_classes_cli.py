"""End-to-end CLI tests for ``dagtools qual classes``."""
import json
from datetime import datetime, timedelta, timezone

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
from dag_tools.qual.qualify import VersionTarget, create_qualification
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)


BUCKET = "dag-tools-qual-classes-cli-test"


def _publish_one_asset(registry, repo, sha, asset_key):
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    rec = {
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
        "tags": {},
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


@pytest.fixture
def home_override(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    return tmp_path


def test_dagtools_qual_classes_happy_path(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _publish_one_asset(reg, "alpha", "shaA", ["x"])
        _publish_one_asset(reg, "beta", "shaB", ["y"])
        create_qualification(
            qual_id="q-test", registry=reg,
            baseline=VersionTarget(dagster="1.10.6"),
            candidate=VersionTarget(dagster="1.12.1"),
        )

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "classes", "--id", "q-test"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["qual_id"] == "q-test"
        # Two repos, identical asset structure -> single class.
        assert payload["class_count"] == 1
        cls = payload["classes"][0]
        assert cls["member_count"] == 2
        assert cls["member_repo_count"] == 2

        # Both JSON and MD landed in the registry.
        assert reg.read_qualification_classes_json("q-test") is not None
        assert reg.read_qualification_classes_md("q-test") is not None


def test_dagtools_qual_classes_table_format(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _publish_one_asset(reg, "alpha", "shaA", ["x"])
        create_qualification(
            qual_id="q-test", registry=reg,
            baseline=VersionTarget(dagster="1.10.6"),
            candidate=VersionTarget(dagster="1.12.1"),
        )
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "classes", "--id", "q-test", "--format", "table"],
        )
    assert result.exit_code == 0, result.output
    assert "q-test" in result.output
    assert "HASH" in result.output


def test_dagtools_qual_classes_missing_manifest_exits_clean(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "classes", "--id", "never-existed"],
        )
    assert result.exit_code == 2
    assert "no qualification manifest" in result.output


def test_dagtools_qual_classes_refuses_second_publish(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _publish_one_asset(reg, "alpha", "shaA", ["x"])
        create_qualification(
            qual_id="q-test", registry=reg,
            baseline=VersionTarget(dagster="1.10.6"),
            candidate=VersionTarget(dagster="1.12.1"),
        )

        first = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "classes", "--id", "q-test"],
        )
        assert first.exit_code == 0, first.output

        second = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "classes", "--id", "q-test"],
        )
        assert second.exit_code == 2
        assert "qual classes failed" in second.output

        # --allow-overwrite succeeds.
        third = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "classes", "--id", "q-test", "--allow-overwrite"],
        )
        assert third.exit_code == 0, third.output
