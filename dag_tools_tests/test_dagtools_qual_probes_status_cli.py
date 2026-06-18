"""CLI smoke tests for ``dagtools qual probes status``."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("typer")

import boto3
from moto import mock_aws
from typer.testing import CliRunner

from dag_tools.inventory import SCHEMA_VERSION as INV_VER
from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.cli import app
from dag_tools.qual.graphql import CodeLocationStatus, DagsterGraphQLClient
from dag_tools.qual.probes.runner import PROBES_LOCATION_NAME
from dag_tools.qual.qualify import (
    Deployment, VersionTarget, create_qualification,
)
from dag_tools.qual.registry import (
    BuildMeta, InventoryRegistry, S3Storage, StorageSettings, layout,
)
from dag_tools.qual.synthetic import generate_bundle, publish_bundle


BUCKET = "dag-tools-probes-status-cli-test"


def _publish_asset(reg, repo, sha, *, asset_key, io_manager_class, tags=None):
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    rec = {
        "schema_version": INV_VER,
        "asset_key": list(asset_key),
        "compute_kind": "python",
        "io_manager_key": "io_manager",
        "io_manager_class": io_manager_class,
        "io_manager_family": "snowflake",
        "partitions_def_class": None,
        "partition_mapping_classes": [],
        "resource_keys": ["io_manager"],
        "resource_classes": {"io_manager": io_manager_class},
        "integration_libs": [],
        "has_asset_checks": False,
        "automation_condition_type": None,
        "tags": tags or {},
    }
    reg.publish_build(
        repo=repo, git_sha=sha, artifacts={
            layout.ASSETS_FILE: json.dumps({
                "schema_version": 1,
                "inventory_schema_version": INV_VER,
                "records": [rec],
            }).encode("utf-8"),
            layout.AUTOMATION_FILE: b'{"schema_version":1,"sensors":[],"schedules":[],"asset_checks":[]}',
            layout.IO_MANAGERS_FILE: b'{"schema_version":1,"entries":[]}',
            layout.DBT_PROJECTS_FILE: b'{"schema_version":1,"projects":[]}',
            layout.LOAD_VALIDATION_FILE: b'{"schema_version":1,"timestamp":"2026-06-15T12:00:00+00:00","loads":true,"locations":[],"failures":[],"warnings":[]}',
        },
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def _seed(reg):
    _publish_asset(
        reg, "alpha", "shaA", asset_key=["x"],
        io_manager_class="dagster_snowflake.SnowflakeIOManager",
        tags={"synthetic_required": "true"},
    )
    create_qualification(
        qual_id="q-test", registry=reg,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://dagster-test/graphql"),
    )
    matrix = build_class_matrix("q-test", registry=reg)
    publish_class_matrix(matrix, registry=reg)
    bundle = generate_bundle("q-test", registry=reg)
    publish_bundle(bundle, registry=reg)
    return bundle


def _patched_client(loaded_keys, state="LOADED"):
    client = MagicMock(spec=DagsterGraphQLClient)
    client.get_code_locations.return_value = [
        CodeLocationStatus(name=PROBES_LOCATION_NAME, load_status=state),
    ]
    client.get_location_asset_keys.return_value = loaded_keys
    client.close.return_value = None
    return patch(
        "dag_tools.qual.probes.status._default_client_factory",
        lambda m: client,
    )


@pytest.fixture
def home_override(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    return tmp_path


def test_cli_status_fully_loaded(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        bundle = _seed(reg)
        probe = bundle.manifest.probes[0]

        with _patched_client([
            [f"{probe.module_name}_upstream"],
            [f"{probe.module_name}_downstream"],
        ]):
            result = runner.invoke(
                app,
                ["--registry", f"s3://{BUCKET}",
                 "qual", "probes", "status", "--id", "q-test"],
            )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["fully_loaded_class_count"] == 1
    assert payload["missing_class_hashes"] == []


def test_cli_status_exit_nonzero_on_gap(home_override):
    """`--exit-nonzero-on-gap` lets operator shell scripts gate on a
    clean deploy."""
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed(reg)

        with _patched_client([]):  # nothing loaded
            result = runner.invoke(
                app,
                ["--registry", f"s3://{BUCKET}",
                 "qual", "probes", "status", "--id", "q-test",
                 "--exit-nonzero-on-gap"],
            )
    assert result.exit_code == 2


def test_cli_status_missing_probe_manifest(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "probes", "status", "--id", "never-existed"],
        )
    assert result.exit_code == 2
    assert "no qualification manifest" in result.output


def test_cli_status_table_format(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed(reg)

        with _patched_client([]):
            result = runner.invoke(
                app,
                ["--registry", f"s3://{BUCKET}",
                 "qual", "probes", "status", "--id", "q-test",
                 "--format", "table"],
            )
    assert result.exit_code == 0, result.output
    assert "qual probes status" in result.output
    assert "MISSING" in result.output
