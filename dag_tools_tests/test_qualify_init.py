"""End-to-end tests for create_qualification — moto-backed registry + local file.

Covers:
  * The pinning step reads every repo's latest.json and freezes it.
  * Manifest is written to BOTH the registry and a local path.
  * Re-running with the same qual_id raises (immutability).
  * --allow-overwrite bypasses the immutability.
  * Repos without a latest.json are skipped (with WARNING) — partial fleet
    state doesn't block the operator.
  * co_upgrade_risks are populated from the pin diff.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("yaml")

import boto3
import yaml
from moto import mock_aws

from dag_tools.qual.qualify import (
    Deployment,
    VersionTarget,
    create_qualification,
    default_local_manifest_path,
)
from dag_tools.qual.registry import (
    BuildMeta,
    ImmutableKeyExists,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)


BUCKET = "dag-tools-qualify-test"


@pytest.fixture
def registry():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


@pytest.fixture
def home_override(tmp_path: Path, monkeypatch):
    """Redirect DAGTOOLS_HOME so default_local_manifest_path doesn't pollute
    the real user home during tests."""
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    return tmp_path


def _publish(registry, repo, sha, hours_ago=1):
    """Helper to drop a freshly-published repo into the registry."""
    when = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    registry.publish_build(
        repo=repo, git_sha=sha, artifacts={},
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_create_qualification_pins_inventories(registry, home_override):
    _publish(registry, "patriot", "sha-patriot")
    _publish(registry, "domain-a", "sha-a")
    _publish(registry, "domain-b", "sha-b")

    manifest = create_qualification(
        qual_id="2026-06-15-test",
        registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
    )

    pinned_repos = sorted(p.repo for p in manifest.inventory_pins)
    assert pinned_repos == ["domain-a", "domain-b", "patriot"]
    by_repo = {p.repo: p for p in manifest.inventory_pins}
    assert by_repo["patriot"].git_sha == "sha-patriot"
    assert by_repo["domain-a"].pinned_timestamp is not None


def test_create_qualification_writes_to_registry_and_local(
    registry, home_override
):
    _publish(registry, "patriot", "sha-patriot")

    manifest = create_qualification(
        qual_id="2026-06-15-test",
        registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
    )

    # Registry copy is parseable YAML and round-trips.
    body = registry.read_qualification_manifest("2026-06-15-test")
    assert body is not None
    parsed = yaml.safe_load(body)
    assert parsed["qual_id"] == "2026-06-15-test"
    assert parsed["baseline"]["dagster"] == "1.10.6"

    # Local copy at the conventional path.
    local = default_local_manifest_path("2026-06-15-test")
    assert local.exists(), f"expected local manifest at {local}"
    assert local.read_text().startswith("schema_version") or "qual_id" in local.read_text()


def test_create_qualification_records_co_upgrade_risks(registry, home_override):
    _publish(registry, "patriot", "sha-patriot")

    manifest = create_qualification(
        qual_id="2026-06-15-test",
        registry=registry,
        baseline=VersionTarget(
            dagster="1.10.6",
            pins={"dbt-core": "1.8.5", "dagster-dbt": "0.27.0"},
        ),
        candidate=VersionTarget(
            dagster="1.12.1",
            pins={"dbt-core": "1.9.0", "dagster-dbt": "0.29.0"},
        ),
    )

    # dagster-dbt filtered out, dbt-core flagged as warning (minor bump).
    assert len(manifest.co_upgrade_risks) == 1
    risk = manifest.co_upgrade_risks[0]
    assert risk.lib == "dbt-core"
    assert risk.from_version == "1.8.5"
    assert risk.to_version == "1.9.0"
    assert risk.severity == "warning"


def test_create_qualification_carries_deployment_and_selection(
    registry, home_override
):
    _publish(registry, "patriot", "sha-patriot")

    from dag_tools.qual.qualify import Selection

    manifest = create_qualification(
        qual_id="2026-06-15-test",
        registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(
            graphql_url="https://dagster-test.internal/graphql",
            auth="env:DAGSTER_TEST_TOKEN",
        ),
        staging_overrides="s3://dag-tools/config/staging.yaml",
        selection=Selection(prefer_tag="regression", reps_per_class=3),
    )
    assert manifest.deployment.graphql_url == "https://dagster-test.internal/graphql"
    assert manifest.deployment.auth == "env:DAGSTER_TEST_TOKEN"
    assert manifest.staging_overrides == "s3://dag-tools/config/staging.yaml"
    assert manifest.selection.reps_per_class == 3


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_re_init_same_qual_id_raises(registry, home_override):
    _publish(registry, "patriot", "sha-patriot")
    create_qualification(
        qual_id="dup", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
    )
    with pytest.raises(ImmutableKeyExists):
        create_qualification(
            qual_id="dup", registry=registry,
            baseline=VersionTarget(dagster="1.10.6"),
            candidate=VersionTarget(dagster="1.12.1"),
        )


def test_allow_overwrite_bypasses_immutability(registry, home_override):
    _publish(registry, "patriot", "sha-patriot")
    create_qualification(
        qual_id="dup", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
    )
    # second time succeeds with --allow-overwrite
    manifest = create_qualification(
        qual_id="dup", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.13.0"),  # different candidate
        allow_overwrite=True,
    )
    assert manifest.candidate.dagster == "1.13.0"


# ---------------------------------------------------------------------------
# Partial-fleet handling
# ---------------------------------------------------------------------------


def test_repo_with_no_latest_pointer_is_skipped(registry, home_override, caplog):
    """A repo prefix that exists but has no latest.json: skip with WARNING
    (the operator already sees it in `dagtools registry status`)."""
    # publish one healthy repo
    _publish(registry, "healthy", "sha1")
    # create an orphan: per-build artifact but no latest pointer
    registry.storage.put_immutable(
        layout.inventory_artifact_key("orphan", "x", layout.ASSETS_FILE),
        b"{}",
    )

    import logging
    with caplog.at_level(logging.WARNING, logger="dag_tools.qual.qualify.init"):
        manifest = create_qualification(
            qual_id="partial", registry=registry,
            baseline=VersionTarget(dagster="1.10.6"),
            candidate=VersionTarget(dagster="1.12.1"),
        )

    pinned = [p.repo for p in manifest.inventory_pins]
    assert pinned == ["healthy"]
    assert any("orphan" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Round-trip: YAML written matches the manifest semantically
# ---------------------------------------------------------------------------


def test_registry_yaml_uses_recipe_aliases(registry, home_override):
    """The YAML on disk uses ``from`` / ``to`` (not ``from_version`` /
    ``to_version``) per the recipe sample so operators can read/edit it
    fluently."""
    _publish(registry, "patriot", "sha-patriot")
    create_qualification(
        qual_id="aliases", registry=registry,
        baseline=VersionTarget(dagster="1.10.6", pins={"dbt-core": "1.8.5"}),
        candidate=VersionTarget(dagster="1.12.1", pins={"dbt-core": "1.9.0"}),
    )
    body = registry.read_qualification_manifest("aliases")
    parsed = yaml.safe_load(body)
    risk = parsed["co_upgrade_risks"][0]
    assert "from" in risk and "to" in risk
    assert "from_version" not in risk
