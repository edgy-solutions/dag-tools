"""CLI tests for ``dagtools qual run``.

We patch the runner's default factory so the CLI talks to a mocked
GraphQL client. Verifies argument routing, exit codes, and that the
classes/manifest sanity checks fire cleanly.
"""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

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
from dag_tools.qual.graphql import (
    DagsterGraphQLClient,
    EventLogEntry,
    RunStatusInfo,
)
from dag_tools.qual.qualify import Deployment, VersionTarget, create_qualification
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)


BUCKET = "dag-tools-qual-run-cli-test"


@pytest.fixture
def home_override(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    return tmp_path


def _seed_qual(registry):
    """Stand up a registry with one asset + manifest + class matrix."""
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
    artifacts = {
        layout.ASSETS_FILE: json.dumps({
            "schema_version": 1,
            "inventory_schema_version": INV_VER,
            "records": [rec],
        }).encode("utf-8"),
        layout.AUTOMATION_FILE: b'{"schema_version":1,"sensors":[],"schedules":[],"asset_checks":[]}',
        layout.IO_MANAGERS_FILE: b'{"schema_version":1,"entries":[]}',
        layout.DBT_PROJECTS_FILE: b'{"schema_version":1,"projects":[]}',
        layout.LOAD_VALIDATION_FILE: b'{"schema_version":1,"timestamp":"2026-06-15T00:00:00+00:00","loads":true,"locations":[],"failures":[],"warnings":[]}',
    }
    registry.publish_build(
        repo="alpha", git_sha="sha-alpha", artifacts=artifacts,
        meta=BuildMeta(repo="alpha", git_sha="sha-alpha", timestamp=when),
    )
    create_qualification(
        qual_id="q-test", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://test/graphql"),
    )
    matrix = build_class_matrix("q-test", registry=registry)
    publish_class_matrix(matrix, registry=registry)


def _fake_client():
    client = MagicMock(spec=DagsterGraphQLClient)
    client.launch_asset_run.return_value = "run-1"
    client.poll_to_completion.return_value = RunStatusInfo(
        run_id="run-1", status="SUCCESS", start_time=0.0, end_time=10.0,
    )
    client.get_event_log.return_value = [
        EventLogEntry(
            event_type="MaterializationEvent",
            asset_key=["hello"], step_key="step1",
            metadata_keys=["row_count"],
        )
    ]
    client.close.return_value = None
    return client


def test_dagtools_qual_run_success(home_override, monkeypatch):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed_qual(reg)

        client = _fake_client()
        import dag_tools.qual.runs.runner as runner_module
        monkeypatch.setattr(
            runner_module, "_default_client_factory", lambda manifest: client
        )

        result = runner.invoke(
            app,
            [
                "--registry", f"s3://{BUCKET}",
                "qual", "run", "--id", "q-test", "--side", "baseline",
                "--poll-interval", "0",
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["passed"] == 1
        assert payload["failed"] == 0


def test_dagtools_qual_run_bad_side_exits_2(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "run", "--id", "q", "--side", "rogue"],
        )
    assert result.exit_code == 2
    assert "baseline" in result.output and "candidate" in result.output


def test_dagtools_qual_run_missing_classes_exits_clean(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        create_qualification(
            qual_id="naked", registry=reg,
            baseline=VersionTarget(dagster="1.10.6"),
            candidate=VersionTarget(dagster="1.12.1"),
            deployment=Deployment(graphql_url="http://x/graphql"),
        )
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "run", "--id", "naked", "--side", "baseline"],
        )
    assert result.exit_code == 2
    assert "dagtools qual classes" in result.output


def test_dagtools_qual_run_table_format(home_override, monkeypatch):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed_qual(reg)
        client = _fake_client()
        import dag_tools.qual.runs.runner as runner_module
        monkeypatch.setattr(
            runner_module, "_default_client_factory", lambda manifest: client
        )

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "run", "--id", "q-test", "--side", "baseline",
             "--poll-interval", "0", "--format", "table"],
        )
    assert result.exit_code == 0, result.output
    assert "qual run:" in result.output
    assert "passed=1" in result.output
