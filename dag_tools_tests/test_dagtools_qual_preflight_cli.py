"""CLI tests for ``dagtools qual preflight``."""
import json
from unittest.mock import MagicMock

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("typer")

import boto3
from moto import mock_aws
from typer.testing import CliRunner

from dag_tools.qual.cli import app
from dag_tools.qual.graphql import (
    CodeLocationStatus,
    DagsterGraphQLClient,
    RunStatusInfo,
)
from dag_tools.qual.qualify import Deployment, VersionTarget, create_qualification
from dag_tools.qual.registry import (
    InventoryRegistry,
    S3Storage,
    StorageSettings,
)


BUCKET = "dag-tools-qual-preflight-cli-test"


@pytest.fixture
def home_override(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    return tmp_path


def _seed(registry, baseline="1.10.6", candidate="1.12.1"):
    create_qualification(
        qual_id="q1", registry=registry,
        baseline=VersionTarget(dagster=baseline),
        candidate=VersionTarget(dagster=candidate),
        deployment=Deployment(graphql_url="http://x/graphql"),
    )


def _client(version="1.12.1", locations=None):
    c = MagicMock(spec=DagsterGraphQLClient)
    c.get_dagster_version.return_value = version
    c.get_code_locations.return_value = locations or [
        CodeLocationStatus(name="patriot", load_status="LOADED"),
    ]
    c.get_run_status.return_value = RunStatusInfo(run_id="r", status="SUCCESS")
    c.close.return_value = None
    return c


def test_dagtools_qual_preflight_success(home_override, monkeypatch):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed(reg)

        client = _client(version="1.12.1")
        import dag_tools.qual.preflight.preflight as p
        monkeypatch.setattr(p, "_default_client_factory", lambda m: client)

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "preflight", "--id", "q1", "--side", "candidate"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["passed"] is True
        assert payload["expected_version"] == "1.12.1"
        # report landed in the registry
        assert reg.read_side_preflight("q1", "candidate") is not None


def test_dagtools_qual_preflight_fails_with_exit_2_on_failed_check(
    home_override, monkeypatch
):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed(reg, candidate="1.12.1")

        client = _client(version="1.11.0")  # mismatch
        import dag_tools.qual.preflight.preflight as p
        monkeypatch.setattr(p, "_default_client_factory", lambda m: client)

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "preflight", "--id", "q1", "--side", "candidate"],
        )
        # Failed checks => non-zero exit so CI can gate.
        assert result.exit_code == 2
        payload = json.loads(result.output)
        assert payload["passed"] is False


def test_dagtools_qual_preflight_bad_side_exits_2(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "preflight", "--id", "q1", "--side", "wrong"],
        )
    assert result.exit_code == 2
    assert "baseline" in result.output and "candidate" in result.output


def test_dagtools_qual_preflight_missing_manifest_exits_clean(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "preflight", "--id", "none", "--side", "baseline"],
        )
    assert result.exit_code == 2
    assert "no qualification manifest" in result.output


def test_dagtools_qual_preflight_table_format(home_override, monkeypatch):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _seed(reg)
        client = _client(version="1.10.6")
        import dag_tools.qual.preflight.preflight as p
        monkeypatch.setattr(p, "_default_client_factory", lambda m: client)

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "preflight", "--id", "q1", "--side", "baseline",
             "--format", "table"],
        )
    assert result.exit_code == 0, result.output
    assert "qual preflight:" in result.output
    assert "PASS" in result.output
