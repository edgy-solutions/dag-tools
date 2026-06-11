"""Moto-backed tests for S3Storage + InventoryRegistry.

Covers:
  * Immutable-put refuses to overwrite (HEAD-then-PUT semantics).
  * publish_build writes meta + artifacts immutably, then the pointer.
  * The pointer write is LAST — if any per-build artifact fails, the
    pointer keeps the previous good build's identity visible to readers.
  * read_latest_pointer returns None for a repo that's never published.
"""
from datetime import datetime, timezone

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")

import boto3
from moto import mock_aws

from dag_tools.qual.registry import (
    BuildMeta,
    ImmutableKeyExists,
    InventoryRegistry,
    LatestPointer,
    S3Storage,
    StorageSettings,
    layout,
)


BUCKET = "dag-tools-test"


@pytest.fixture
def s3_bucket():
    """Stand up a mock S3 bucket via moto.mock_aws and yield the bucket name."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield BUCKET


@pytest.fixture
def storage(s3_bucket):
    return S3Storage(StorageSettings(bucket=s3_bucket))


@pytest.fixture
def registry(storage):
    return InventoryRegistry(storage)


# --- S3Storage --------------------------------------------------------------


def test_storage_put_immutable_then_get(storage):
    storage.put_immutable("inventory/foo/sha/meta.json", b'{"hello":"world"}')
    body = storage.get("inventory/foo/sha/meta.json")
    assert body == b'{"hello":"world"}'


def test_storage_put_immutable_refuses_overwrite(storage):
    key = "inventory/foo/sha/meta.json"
    storage.put_immutable(key, b'{"v":1}')
    with pytest.raises(ImmutableKeyExists, match="refusing to overwrite"):
        storage.put_immutable(key, b'{"v":2}')


def test_storage_put_mutable_overwrites_freely(storage):
    key = "inventory/foo/latest.json"
    storage.put_mutable(key, b'{"v":1}')
    storage.put_mutable(key, b'{"v":2}')
    assert storage.get(key) == b'{"v":2}'


def test_storage_get_optional_returns_none_for_missing(storage):
    assert storage.get_optional("does/not/exist") is None


def test_storage_list_subdirs_after_publish(storage):
    """list_subdirs treats '/' as delimiter and returns the immediate
    'folder' names — the building block of list_repos()."""
    storage.put_immutable("inventory/repo-a/sha1/meta.json", b"{}")
    storage.put_immutable("inventory/repo-b/sha1/meta.json", b"{}")
    storage.put_immutable("inventory/repo-a/sha2/meta.json", b"{}")
    subs = storage.list_subdirs("inventory/")
    assert sorted(subs) == ["repo-a", "repo-b"]


# --- InventoryRegistry ------------------------------------------------------


def _meta(repo: str = "patriot", sha: str = "abc123") -> BuildMeta:
    return BuildMeta(
        repo=repo,
        git_sha=sha,
        build_id="build-42",
        timestamp=datetime(2026, 6, 11, 12, 0, 0, tzinfo=timezone.utc),
        dagster_version="1.13.1",
        dagtools_version="0.1.0",
        inventory_schema_version=1,
    )


def test_publish_build_writes_meta_artifacts_then_pointer(registry):
    meta = _meta()
    artifacts = {
        layout.ASSETS_FILE: b'{"assets":[]}',
        layout.AUTOMATION_FILE: b'{"sensors":[]}',
    }
    pointer = registry.publish_build(
        "patriot", "abc123", artifacts, meta
    )
    # Per-build artifacts written immutably:
    assert registry.read_build_json("patriot", "abc123", layout.ASSETS_FILE) == {"assets": []}
    assert registry.read_build_json("patriot", "abc123", layout.META_FILE) is not None
    # Pointer points back at the published SHA:
    assert pointer.git_sha == "abc123"
    fresh = registry.read_latest_pointer("patriot")
    assert fresh is not None and fresh.git_sha == "abc123"
    assert fresh.build_id == "build-42"


def test_publish_build_is_immutable_for_same_sha(registry):
    meta = _meta()
    registry.publish_build("patriot", "abc123", {layout.ASSETS_FILE: b"{}"}, meta)
    with pytest.raises(ImmutableKeyExists):
        registry.publish_build("patriot", "abc123", {layout.ASSETS_FILE: b"{}"}, meta)


def test_publish_build_allow_overwrite_bypasses_immutability(registry):
    meta = _meta()
    registry.publish_build(
        "patriot", "abc123", {layout.ASSETS_FILE: b'{"v":1}'}, meta
    )
    registry.publish_build(
        "patriot", "abc123", {layout.ASSETS_FILE: b'{"v":2}'},
        meta, allow_overwrite=True,
    )
    assert registry.read_build_json("patriot", "abc123", layout.ASSETS_FILE) == {"v": 2}


def test_pointer_not_updated_when_artifact_write_fails(registry):
    """If a mid-publish write blows up, the pointer must keep its old value.

    The recipe invariant: ``latest.json`` is written LAST, so readers never
    observe a build whose artifacts aren't all there.
    """
    meta_a = _meta(sha="sha-a")
    registry.publish_build(
        "patriot", "sha-a",
        {layout.ASSETS_FILE: b'{"v":"a"}'}, meta_a,
    )

    # Now try to publish sha-b but force the *second* artifact write to fail.
    # The first write (meta.json) succeeds; the second raises before the
    # pointer is touched. The pointer should still resolve to sha-a.
    meta_b = _meta(sha="sha-b")

    original_put = registry.storage.put_immutable
    calls = {"n": 0}

    def flaky_put(key, body, content_type="application/json"):
        calls["n"] += 1
        if calls["n"] == 2:  # second immutable write blows up
            raise RuntimeError("simulated S3 outage mid-publish")
        original_put(key, body, content_type)

    registry.storage.put_immutable = flaky_put  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="simulated S3 outage"):
        registry.publish_build(
            "patriot", "sha-b",
            {layout.ASSETS_FILE: b'{"v":"b"}'}, meta_b,
        )

    # Pointer still points at sha-a — the failed publish is invisible.
    pointer = registry.read_latest_pointer("patriot")
    assert pointer is not None and pointer.git_sha == "sha-a"


def test_read_latest_pointer_returns_none_for_unknown_repo(registry):
    assert registry.read_latest_pointer("never-published") is None


def test_list_repos_returns_only_published_repos(registry):
    meta = _meta(repo="repo-a", sha="x")
    registry.publish_build("repo-a", "x", {}, meta)
    registry.publish_build("repo-b", "y", {}, _meta(repo="repo-b", sha="y"))
    assert registry.list_repos() == ["repo-a", "repo-b"]


def test_latest_pointer_is_round_trippable(registry):
    """JSON-serialize -> S3 -> JSON-deserialize preserves field semantics."""
    meta = _meta()
    pointer = registry.publish_build("patriot", "abc123", {}, meta)
    fresh = registry.read_latest_pointer("patriot")
    assert isinstance(fresh, LatestPointer)
    assert fresh.git_sha == pointer.git_sha
    assert fresh.timestamp == pointer.timestamp
    assert fresh.dagster_version == "1.13.1"
