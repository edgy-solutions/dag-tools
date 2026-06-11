"""End-to-end tests for the dagtools CLI via Typer's test runner.

Covers `dagtools registry status` against a moto-backed bucket; verifies
JSON-by-default output, table output, the staleness exit code, and the
two ways to inject registry config (--registry flag and DAGTOOLS_REGISTRY
env var).
"""
import json
import os
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("typer")

import boto3
from moto import mock_aws
from typer.testing import CliRunner

from dag_tools.qual.cli import app
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
)


BUCKET = "dag-tools-cli-test"


@pytest.fixture
def registry():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        # Drop a single fresh repo for the happy path.
        meta = BuildMeta(
            repo="patriot",
            git_sha="abc123def456",
            timestamp=datetime.now(tz=timezone.utc) - timedelta(hours=1),
            dagster_version="1.13.1",
            dagtools_version="0.1.0",
        )
        reg.publish_build("patriot", "abc123def456", {}, meta)
        yield reg


def test_registry_status_json_default(registry):
    runner = CliRunner()
    with mock_aws():
        # moto fixture above is the same in-process mock; re-entering is a no-op.
        boto3.client("s3", region_name="us-east-1")  # ensure session exists
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}", "registry", "status"],
        )
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["repo_count"] == 1
    assert parsed["fresh_count"] == 1
    assert parsed["repos"][0]["repo"] == "patriot"
    assert parsed["repos"][0]["state"] == "fresh"


def test_registry_status_table_output(registry):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1")
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}", "registry", "status", "--format", "table"],
        )
    assert result.exit_code == 0, result.output
    # Table output uses words, not JSON.
    assert "registry status" in result.output
    assert "patriot" in result.output
    assert "fresh" in result.output


def test_registry_status_picks_up_env_var(registry):
    runner = CliRunner()
    env = {
        "DAGTOOLS_REGISTRY": f"s3://{BUCKET}",
        # Inherit AWS creds from the moto mock env.
    }
    with mock_aws():
        boto3.client("s3", region_name="us-east-1")
        result = runner.invoke(app, ["registry", "status"], env=env)
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["repos"][0]["repo"] == "patriot"


def test_registry_status_exit_nonzero_on_stale_flag():
    """When the flag is set and the fleet has stale/missing entries, exit 2."""
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        old_meta = BuildMeta(
            repo="patriot",
            git_sha="old",
            timestamp=datetime.now(tz=timezone.utc) - timedelta(days=5),
        )
        reg.publish_build("patriot", "old", {}, old_meta)

        result = runner.invoke(
            app,
            [
                "--registry", f"s3://{BUCKET}",
                "registry", "status",
                "--max-age-hours", "24",
                "--exit-nonzero-on-stale",
            ],
        )
    assert result.exit_code == 2, result.output


def test_invalid_registry_uri_rejected_cleanly():
    """Bad URI should fail fast with a clear error and a non-zero exit."""
    runner = CliRunner()
    result = runner.invoke(
        app, ["--registry", "s3://bucket/with/path", "registry", "status"]
    )
    assert result.exit_code != 0
    assert "must not include a path" in result.output
