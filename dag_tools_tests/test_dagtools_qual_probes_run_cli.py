"""End-to-end CLI tests for ``dagtools qual probes run``."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("typer")
pytest.importorskip("yaml")

import boto3
from moto import mock_aws
from typer.testing import CliRunner

from dag_tools.inventory import SCHEMA_VERSION as INV_VER
from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.cli import app
from dag_tools.qual.graphql import (
    DagsterGraphQLClient,
    EventLogEntry,
    RunStatusInfo,
)
from dag_tools.qual.qualify import (
    Deployment,
    VersionTarget,
    create_qualification,
)
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)
from dag_tools.qual.synthetic import generate_bundle, publish_bundle


BUCKET = "dag-tools-qual-probes-cli-test"


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
    reg.publish_build(
        repo=repo, git_sha=sha, artifacts=artifacts,
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def _seed(reg, qual_id="q-test"):
    _publish_asset(
        reg, "alpha", "shaA",
        asset_key=["x"],
        io_manager_class="dagster_snowflake.SnowflakeIOManager",
        tags={"synthetic_required": "true"},
    )
    create_qualification(
        qual_id=qual_id, registry=reg,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://dagster-test/graphql"),
    )
    matrix = build_class_matrix(qual_id, registry=reg)
    publish_class_matrix(matrix, registry=reg)
    publish_bundle(generate_bundle(qual_id, registry=reg), registry=reg)


def _patched_factory(launch_run_id="probe-run-1", success=True):
    """Build a context manager that patches the runner's default factory
    to return a canned-behavior mock — same approach used in the Q2 CLI
    tests."""
    client = MagicMock(spec=DagsterGraphQLClient)
    client.launch_asset_run.return_value = launch_run_id
    client.poll_to_completion.return_value = RunStatusInfo(
        run_id=launch_run_id,
        status="SUCCESS" if success else "FAILURE",
        start_time=0.0, end_time=2.0,
    )
    client.get_event_log.return_value = []
    client.get_run_status.side_effect = lambda rid: client.poll_to_completion.return_value
    client.close.return_value = None
    return patch(
        "dag_tools.qual.probes.runner._default_client_factory",
        lambda manifest: client,
    ), client


@pytest.fixture
def home_override(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    return tmp_path


def test_cli_probes_run_happy_path(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed(reg)

        ctx, client = _patched_factory()
        with ctx:
            result = runner.invoke(
                app,
                ["--registry", f"s3://{BUCKET}",
                 "qual", "probes", "run", "--id", "q-test", "--side", "baseline"],
            )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["qual_id"] == "q-test"
        assert payload["passed"] == 1
        assert payload["probe_total"] == 1


def test_cli_probes_run_invalid_side_exits_clean(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "probes", "run", "--id", "q-test", "--side", "production"],
        )
    assert result.exit_code == 2
    assert "must be 'baseline' or 'candidate'" in result.output


def test_cli_probes_run_missing_probe_manifest(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        # Seed qual but NOT probes.
        _publish_asset(
            reg, "alpha", "shaA",
            asset_key=["x"],
            io_manager_class="dagster.InMemoryIOManager",
        )
        create_qualification(
            qual_id="q-test", registry=reg,
            baseline=VersionTarget(dagster="1.10.6"),
            candidate=VersionTarget(dagster="1.12.1"),
            deployment=Deployment(graphql_url="http://dagster-test/graphql"),
        )
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "probes", "run", "--id", "q-test", "--side", "baseline"],
        )
    assert result.exit_code == 2
    assert "no probe manifest" in result.output


def test_cli_probes_run_failure_exits_nonzero(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed(reg)

        ctx, _ = _patched_factory(success=False)
        with ctx:
            result = runner.invoke(
                app,
                ["--registry", f"s3://{BUCKET}",
                 "qual", "probes", "run", "--id", "q-test", "--side", "baseline"],
            )
        # FAILED count triggers nonzero exit so CI / shell can gate.
        assert result.exit_code == 2
        # JSON summary still emitted before exit; smoke-check it's present.
        assert '"failed": 1' in result.output


def test_cli_probes_run_table_format(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed(reg)

        ctx, _ = _patched_factory()
        with ctx:
            result = runner.invoke(
                app,
                ["--registry", f"s3://{BUCKET}",
                 "qual", "probes", "run", "--id", "q-test", "--side", "baseline",
                 "--format", "table"],
            )
        assert result.exit_code == 0, result.output
        assert "qual probes run" in result.output
        assert "passed=1" in result.output
