"""Tests for compute_staleness against a moto-backed registry."""
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")

import boto3
from moto import mock_aws

from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StalenessState,
    StorageSettings,
    compute_staleness,
    layout,
)


BUCKET = "dag-tools-status-test"


@pytest.fixture
def registry():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _publish(registry, repo, sha, when):
    meta = BuildMeta(
        repo=repo,
        git_sha=sha,
        build_id=f"build-{sha[:6]}",
        timestamp=when,
        dagster_version="1.13.1",
        dagtools_version="0.1.0",
    )
    registry.publish_build(repo, sha, {}, meta)


def test_fresh_when_pointer_younger_than_threshold(registry):
    now = datetime(2026, 6, 11, 12, 0, 0, tzinfo=timezone.utc)
    _publish(registry, "patriot", "abc123", now - timedelta(hours=1))
    report = compute_staleness(registry, max_age=timedelta(hours=24), now=now)
    assert report.repo_count == 1
    assert report.fresh_count == 1
    assert report.stale_count == 0
    s = report.repos[0]
    assert s.state == StalenessState.FRESH
    assert s.pointer is not None and s.pointer.git_sha == "abc123"
    assert 3500 < (s.age_seconds or 0) < 3700  # ~3600


def test_stale_when_pointer_older_than_threshold(registry):
    now = datetime(2026, 6, 11, 12, 0, 0, tzinfo=timezone.utc)
    _publish(registry, "patriot", "old", now - timedelta(hours=48))
    report = compute_staleness(registry, max_age=timedelta(hours=24), now=now)
    assert report.stale_count == 1
    assert report.repos[0].state == StalenessState.STALE


def test_missing_when_repo_has_artifacts_but_no_pointer(registry):
    """Simulate a survey that wrote a build but crashed before the pointer.
    The repo appears in list_repos() (via S3 list under inventory/), but
    latest.json is absent."""
    # Write only a per-build artifact, NOT the pointer.
    storage = registry.storage
    storage.put_immutable(
        layout.inventory_artifact_key("orphan", "sha", layout.ASSETS_FILE),
        b'{"assets":[]}',
    )
    report = compute_staleness(registry)
    repo_names = [r.repo for r in report.repos]
    assert "orphan" in repo_names
    s = next(r for r in report.repos if r.repo == "orphan")
    assert s.state == StalenessState.MISSING
    assert s.pointer is None


def test_mixed_fleet_classification(registry):
    """One fresh, one stale, one missing — verify the aggregate counts."""
    now = datetime(2026, 6, 11, 12, 0, 0, tzinfo=timezone.utc)
    _publish(registry, "fresh-repo", "f1", now - timedelta(hours=2))
    _publish(registry, "stale-repo", "s1", now - timedelta(days=3))
    # Missing: artifact present, pointer absent.
    registry.storage.put_immutable(
        layout.inventory_artifact_key("orphan-repo", "x", layout.ASSETS_FILE),
        b"{}",
    )

    report = compute_staleness(registry, max_age=timedelta(hours=24), now=now)
    assert report.repo_count == 3
    assert report.fresh_count == 1
    assert report.stale_count == 1
    assert report.missing_count == 1
    assert report.unreadable_count == 0


def test_unreadable_when_pointer_is_garbage(registry):
    """A latest.json that doesn't parse as our schema flags as UNREADABLE,
    not a crash."""
    # Write a non-JSON body at the pointer key.
    registry.storage.put_mutable(
        layout.latest_pointer_key("corrupt-repo"), b"not json at all",
    )
    report = compute_staleness(registry)
    s = next(r for r in report.repos if r.repo == "corrupt-repo")
    assert s.state == StalenessState.UNREADABLE
    assert s.error is not None
