"""End-to-end CLI tests for ``dagtools qual init``."""
import json
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("typer")
pytest.importorskip("yaml")

import boto3
import yaml
from moto import mock_aws
from typer.testing import CliRunner

from dag_tools.qual.cli import app
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
)


BUCKET = "dag-tools-qual-cli-test"


@pytest.fixture
def home_override(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    return tmp_path


def _publish(registry, repo, sha, hours_ago=1):
    when = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    registry.publish_build(
        repo=repo, git_sha=sha, artifacts={},
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def test_dagtools_qual_init_basic(home_override, tmp_path):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _publish(reg, "patriot", "sha-patriot")
        _publish(reg, "domain-a", "sha-a")

        result = runner.invoke(
            app,
            [
                "--registry", f"s3://{BUCKET}",
                "qual", "init",
                "--id", "2026-06-15-test",
                "--baseline", "1.10.6",
                "--candidate", "1.12.1",
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["qual_id"] == "2026-06-15-test"
        assert payload["baseline"]["dagster"] == "1.10.6"
        assert payload["candidate"]["dagster"] == "1.12.1"
        assert {p["repo"] for p in payload["inventory_pins"]} == {"patriot", "domain-a"}

        # registry copy exists
        body = reg.read_qualification_manifest("2026-06-15-test")
        assert body is not None
        # local copy exists at ${DAGTOOLS_HOME}/quals/<id>/manifest.yaml
        local = home_override / "quals" / "2026-06-15-test" / "manifest.yaml"
        assert local.exists()


def test_dagtools_qual_init_with_pins_files(home_override, tmp_path):
    """Pins files: YAML mapping of lib -> version. The CLI reads both and
    diffs into co_upgrade_risks."""
    runner = CliRunner()

    base_pins = tmp_path / "base.yaml"
    base_pins.write_text(textwrap.dedent("""
        dbt-core: 1.8.5
        dagster-dbt: 0.27.0
    """))
    cand_pins = tmp_path / "cand.yaml"
    cand_pins.write_text(textwrap.dedent("""
        dbt-core: 1.9.0
        dagster-dbt: 0.29.0
    """))

    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _publish(reg, "patriot", "sha-patriot")

        result = runner.invoke(
            app,
            [
                "--registry", f"s3://{BUCKET}",
                "qual", "init",
                "--id", "2026-06-15-test",
                "--baseline", "1.10.6",
                "--candidate", "1.12.1",
                "--baseline-pins", str(base_pins),
                "--candidate-pins", str(cand_pins),
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)

        # dagster-dbt filtered out as family; dbt-core flagged.
        risks = payload["co_upgrade_risks"]
        assert len(risks) == 1
        assert risks[0]["lib"] == "dbt-core"
        assert risks[0]["from"] == "1.8.5"
        assert risks[0]["to"] == "1.9.0"


def test_dagtools_qual_init_deployment_and_selection_flags(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _publish(reg, "patriot", "sha-patriot")

        result = runner.invoke(
            app,
            [
                "--registry", f"s3://{BUCKET}",
                "qual", "init",
                "--id", "deploy-test",
                "--baseline", "1.10.6",
                "--candidate", "1.12.1",
                "--graphql-url", "https://dagster-test.internal/graphql",
                "--graphql-auth-env", "DAGSTER_TEST_TOKEN",
                "--staging-overrides", "s3://dag-tools/config/staging.yaml",
                "--prefer-tag", "smoke",
                "--reps-per-class", "3",
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["deployment"]["graphql_url"] == "https://dagster-test.internal/graphql"
        assert payload["deployment"]["auth"] == "env:DAGSTER_TEST_TOKEN"
        assert payload["staging_overrides"] == "s3://dag-tools/config/staging.yaml"
        assert payload["selection"]["reps_per_class"] == 3
        assert payload["selection"]["prefer_tag"] == "smoke"


def test_dagtools_qual_init_refuses_existing_id(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _publish(reg, "patriot", "sha-patriot")

        first = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "init", "--id", "dup",
             "--baseline", "1.10.6", "--candidate", "1.12.1"],
        )
        assert first.exit_code == 0, first.output

        second = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "init", "--id", "dup",
             "--baseline", "1.10.6", "--candidate", "1.12.1"],
        )
        assert second.exit_code == 2
        assert "qual init failed" in second.output

        # Same call with --allow-overwrite succeeds
        overwrite = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "init", "--id", "dup",
             "--baseline", "1.10.6", "--candidate", "1.13.0",
             "--allow-overwrite"],
        )
        assert overwrite.exit_code == 0, overwrite.output


def test_dagtools_qual_init_table_format(home_override):
    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _publish(reg, "patriot", "sha-patriot")

        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "init", "--id", "table-test",
             "--baseline", "1.10.6", "--candidate", "1.12.1",
             "--format", "table"],
        )
    assert result.exit_code == 0, result.output
    assert "table-test" in result.output
    assert "inventory pinned" in result.output
    assert "co_upgrade_risks" in result.output


def test_dagtools_qual_init_bad_pins_file_exits_clean(home_override, tmp_path):
    """A pins file that isn't a mapping at top level should fail fast with
    a clear error, not crash."""
    bad = tmp_path / "bad.yaml"
    bad.write_text("- not\n- a\n- mapping\n")

    runner = CliRunner()
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        result = runner.invoke(
            app,
            ["--registry", f"s3://{BUCKET}",
             "qual", "init", "--id", "bad-pins",
             "--baseline", "1.10.6", "--candidate", "1.12.1",
             "--baseline-pins", str(bad)],
        )
    assert result.exit_code == 2
    assert "must contain a mapping" in result.output
