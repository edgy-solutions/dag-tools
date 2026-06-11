"""End-to-end CLI tests for ``dagtools survey``."""
import json
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("dagster")
pytest.importorskip("moto")
pytest.importorskip("typer")

import boto3
from moto import mock_aws
from typer.testing import CliRunner

from dag_tools.qual.cli import app
from dag_tools.qual.registry import layout


BUCKET = "dag-tools-survey-cli-test"


@pytest.fixture
def good_module(tmp_path: Path) -> Path:
    py = tmp_path / "g.py"
    py.write_text(textwrap.dedent("""
        from dagster import Definitions, InMemoryIOManager, asset

        @asset
        def hello():
            return 1

        defs = Definitions(
            assets=[hello], resources={'io_manager': InMemoryIOManager()},
        )
    """))
    return py


@pytest.fixture
def broken_module(tmp_path: Path) -> Path:
    py = tmp_path / "broken.py"
    py.write_text("raise RuntimeError('CLI fixture load failure')\n")
    return py


def test_dagtools_survey_publishes_then_reports_via_status(good_module):
    """Full loop: survey publishes -> registry status sees the result."""
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)

        survey_result = runner.invoke(
            app,
            [
                "--registry", f"s3://{BUCKET}",
                "survey",
                "--locations", str(good_module),
                "--repo", "patriot",
                "--sha", "abc123",
                "--build", "build-42",
            ],
        )
        assert survey_result.exit_code == 0, survey_result.output
        payload = json.loads(survey_result.output)
        assert payload["published"] is True
        assert payload["pointer_sha"] == "abc123"
        assert layout.ASSETS_FILE in payload["artifacts_written"]

        # Now ask the registry status command — should see one fresh repo.
        status_result = runner.invoke(
            app, ["--registry", f"s3://{BUCKET}", "registry", "status"]
        )
        assert status_result.exit_code == 0
        status_payload = json.loads(status_result.output)
        assert status_payload["fresh_count"] == 1
        assert status_payload["repos"][0]["repo"] == "patriot"
        assert status_payload["repos"][0]["pointer"]["git_sha"] == "abc123"


def test_dagtools_survey_load_failure_exits_nonzero_and_publishes_nothing(
    broken_module
):
    """The load-gate contract — surface as a non-zero exit + empty registry."""
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)

        result = runner.invoke(
            app,
            [
                "--registry", f"s3://{BUCKET}",
                "survey",
                "--locations", str(broken_module),
                "--repo", "patriot",
                "--sha", "abc123",
            ],
        )
        assert result.exit_code == 2, result.output
        # JSON output is still emitted even on failure (recipe rule: machine-readable).
        payload = json.loads(result.output)
        assert payload["published"] is False
        assert payload["load_validation"]["loads"] is False
        assert payload["load_validation"]["failures"], (
            "expected at least one failure detail in load_validation"
        )

        # The registry was untouched: registry status reports an empty fleet.
        status_result = runner.invoke(
            app, ["--registry", f"s3://{BUCKET}", "registry", "status"]
        )
        status_payload = json.loads(status_result.output)
        assert status_payload["repo_count"] == 0


def test_dagtools_survey_table_format(good_module):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            [
                "--registry", f"s3://{BUCKET}",
                "survey",
                "--locations", str(good_module),
                "--repo", "patriot",
                "--sha", "abc123",
                "--format", "table",
            ],
        )
    assert result.exit_code == 0, result.output
    assert "survey @" in result.output
    assert "PUBLISHED" in result.output
    assert "patriot" not in result.output  # the table doesn't echo the repo string
    assert "abc123" in result.output


def test_dagtools_survey_skip_publish(good_module):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            [
                "--registry", f"s3://{BUCKET}",
                "survey",
                "--locations", str(good_module),
                "--repo", "patriot",
                "--sha", "abc123",
                "--skip-publish",
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["published"] is False
        assert payload["load_validation"]["loads"] is True

        # Skip means the registry is untouched.
        status_result = runner.invoke(
            app, ["--registry", f"s3://{BUCKET}", "registry", "status"]
        )
        status_payload = json.loads(status_result.output)
        assert status_payload["repo_count"] == 0
