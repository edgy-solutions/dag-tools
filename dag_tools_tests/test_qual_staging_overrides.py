"""The manifest's ``staging_overrides`` POINTER must become run config.

Regression suite for a bug that made `--staging-overrides` unusable: both
launchers wrote the pointer string itself into run_config under a
fabricated ``__staging_overrides__`` key. Dagster accepts only
``ops``/``resources``/``execution``/``loggers`` at the top of a run
config, so every such launch died at submission.

Note what the old tests got wrong, because it is the whole lesson here:
they asserted on the dict the launcher built, which is exactly what the
bug produced. These tests assert the launched config would be ACCEPTED by
Dagster — i.e. its top-level keys are legal — which is the property that
actually matters.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from dag_tools.qual.classes import Representative
from dag_tools.qual.graphql import DagsterGraphQLClient
from dag_tools.qual.runs import (
    RUN_CONFIG_TOP_LEVEL_KEYS,
    StagingOverridesError,
    StagingOverridesNotResolved,
    launch_representative,
    merge_run_config,
    resolve_staging_overrides,
)

STAGING_DOC = {
    "resources": {
        "io_manager": {"config": {"s3_bucket": "staging-bucket"}},
        "warehouse": {"config": {"schema": "qual_staging"}},
    }
}


def _rep(asset_key: List[str]) -> Representative:
    return Representative(
        repo="acme/alcs-tools",
        git_sha="a" * 40,
        asset_key=asset_key,
        runnability="runnable",
        runnability_reason="no tags",
    )


def _manifest(staging_overrides: Optional[str] = None) -> Any:
    return SimpleNamespace(staging_overrides=staging_overrides)


def _client() -> MagicMock:
    client = MagicMock(spec=DagsterGraphQLClient)
    client.launch_asset_run.return_value = "run-1"
    return client


def _launched_config(client: MagicMock) -> Dict[str, Any]:
    return client.launch_asset_run.call_args.kwargs["run_config"]


# ---------------------------------------------------------------------------
# The bug
# ---------------------------------------------------------------------------


def test_the_pointer_itself_never_reaches_run_config():
    """The original defect, stated directly."""
    client = _client()
    launch_representative(
        client, _rep(["orders"]),
        qual_id="q1", side="candidate",
        manifest=_manifest("s3://cfg/staging.yaml"),
        staging_overrides=STAGING_DOC,
    )
    config = _launched_config(client)
    assert "__staging_overrides__" not in config
    assert "s3://cfg/staging.yaml" not in str(config)


def test_launched_run_config_has_only_keys_dagster_accepts():
    """The property the old tests should have asserted."""
    client = _client()
    launch_representative(
        client, _rep(["orders"]),
        qual_id="q1", side="candidate",
        manifest=_manifest("s3://cfg/staging.yaml"),
        staging_overrides=STAGING_DOC,
    )
    illegal = set(_launched_config(client)) - RUN_CONFIG_TOP_LEVEL_KEYS
    assert not illegal, f"Dagster would reject top-level keys {sorted(illegal)}"


def test_resolved_overrides_actually_reach_the_launch():
    client = _client()
    launch_representative(
        client, _rep(["orders"]),
        qual_id="q1", side="candidate",
        manifest=_manifest("s3://cfg/staging.yaml"),
        staging_overrides=STAGING_DOC,
    )
    config = _launched_config(client)
    assert config["resources"]["io_manager"]["config"]["s3_bucket"] == "staging-bucket"


def test_an_unresolved_pointer_raises_instead_of_launching():
    """Launching against un-overridden resources would bank a run record
    that looks like a qualification but describes the wrong deployment."""
    client = _client()
    with pytest.raises(StagingOverridesNotResolved):
        launch_representative(
            client, _rep(["orders"]),
            qual_id="q1", side="candidate",
            manifest=_manifest("s3://cfg/staging.yaml"),
        )
    client.launch_asset_run.assert_not_called()


def test_no_pointer_means_no_overrides_and_no_error():
    client = _client()
    launch_representative(
        client, _rep(["orders"]),
        qual_id="q1", side="candidate",
        manifest=_manifest(None),
    )
    assert _launched_config(client) == {}


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def test_resolve_reads_an_s3_pointer_through_the_supplied_fetcher():
    seen = {}

    def fetch(uri: str) -> bytes:
        seen["uri"] = uri
        return b"resources:\n  io_manager:\n    config:\n      s3_bucket: b1\n"

    doc = resolve_staging_overrides("s3://cfg/staging.yaml", fetch=fetch)
    assert seen["uri"] == "s3://cfg/staging.yaml"
    assert doc["resources"]["io_manager"]["config"]["s3_bucket"] == "b1"


def test_resolve_reads_a_local_path_without_any_fetcher(tmp_path):
    """So an operator can iterate on the document before uploading it."""
    p = tmp_path / "staging.yaml"
    p.write_text("resources:\n  warehouse:\n    config:\n      schema: s\n")
    doc = resolve_staging_overrides(str(p))
    assert doc["resources"]["warehouse"]["config"]["schema"] == "s"


def test_resolve_accepts_json_because_json_is_yaml(tmp_path):
    p = tmp_path / "staging.json"
    p.write_text('{"ops": {"my_op": {"config": {"n": 3}}}}')
    assert resolve_staging_overrides(str(p))["ops"]["my_op"]["config"]["n"] == 3


def test_resolve_returns_empty_for_no_pointer():
    assert resolve_staging_overrides(None) == {}
    assert resolve_staging_overrides("") == {}


def test_resolve_rejects_a_document_dagster_would_reject(tmp_path):
    """An operator pointing at the wrong file gets a message naming the
    bad keys, not a config error from inside Dagster's submission path."""
    p = tmp_path / "wrong.yaml"
    p.write_text("baseline: 1.10.19\ncandidate: 1.13.16\n")
    with pytest.raises(StagingOverridesError) as e:
        resolve_staging_overrides(str(p))
    assert "baseline" in str(e.value) and "candidate" in str(e.value)


def test_resolve_rejects_a_non_mapping(tmp_path):
    p = tmp_path / "list.yaml"
    p.write_text("- one\n- two\n")
    with pytest.raises(StagingOverridesError):
        resolve_staging_overrides(str(p))


def test_resolve_treats_an_empty_document_as_no_overrides(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("# everything commented out\n")
    assert resolve_staging_overrides(str(p)) == {}


def test_resolve_errors_on_an_s3_pointer_with_no_fetcher():
    with pytest.raises(StagingOverridesError) as e:
        resolve_staging_overrides("s3://cfg/staging.yaml")
    assert "fetcher" in str(e.value)


def test_resolve_wraps_a_fetch_failure_as_a_typed_error():
    def fetch(uri: str) -> bytes:
        raise RuntimeError("NoSuchKey")

    with pytest.raises(StagingOverridesError) as e:
        resolve_staging_overrides("s3://cfg/missing.yaml", fetch=fetch)
    assert "NoSuchKey" in str(e.value)


# ---------------------------------------------------------------------------
# Merge semantics
# ---------------------------------------------------------------------------


def test_merge_is_recursive_so_a_per_rep_block_does_not_drop_siblings():
    """A shallow merge would delete `warehouse` entirely just because the
    per-rep config happened to mention `io_manager`."""
    merged = merge_run_config(
        overrides=STAGING_DOC,
        explicit={"resources": {"io_manager": {"config": {"prefix": "p"}}}},
    )
    assert "warehouse" in merged["resources"], "sibling resource was dropped"
    assert merged["resources"]["io_manager"]["config"]["s3_bucket"] == "staging-bucket"
    assert merged["resources"]["io_manager"]["config"]["prefix"] == "p"


def test_explicit_config_wins_at_the_leaf():
    merged = merge_run_config(
        overrides={"resources": {"io_manager": {"config": {"s3_bucket": "staging"}}}},
        explicit={"resources": {"io_manager": {"config": {"s3_bucket": "per-rep"}}}},
    )
    assert merged["resources"]["io_manager"]["config"]["s3_bucket"] == "per-rep"


def test_merge_does_not_mutate_its_inputs():
    overrides = {"resources": {"a": {"config": {"x": 1}}}}
    merge_run_config(overrides=overrides, explicit={"resources": {"a": {"config": {"x": 2}}}})
    assert overrides["resources"]["a"]["config"]["x"] == 1


def test_per_rep_config_still_works_with_overrides_present():
    client = _client()
    launch_representative(
        client, _rep(["orders"]),
        qual_id="q1", side="candidate",
        manifest=_manifest("s3://cfg/staging.yaml"),
        staging_overrides=STAGING_DOC,
        run_config={"ops": {"orders": {"config": {"limit": 10}}}},
    )
    config = _launched_config(client)
    assert config["ops"]["orders"]["config"]["limit"] == 10
    assert config["resources"]["warehouse"]["config"]["schema"] == "qual_staging"


# ---------------------------------------------------------------------------
# The probe launcher carried the identical bug
# ---------------------------------------------------------------------------


def _probe():
    from dag_tools.qual.synthetic import ProbeModule

    return ProbeModule(
        class_hash="abc123def456",
        module_name="probe_abc123de",
        source_key="qualifications/q1/probes/abc123def456.py",
    )


def test_probe_launch_does_not_put_the_pointer_in_run_config():
    from dag_tools.qual.probes.runner import _launch_probe

    client = _client()
    _launch_probe(
        client, _probe(), qual_id="q1", side="candidate",
        manifest=_manifest("s3://cfg/staging.yaml"),
        staging_overrides=STAGING_DOC,
    )
    config = _launched_config(client)
    assert "__staging_overrides__" not in config
    assert not set(config) - RUN_CONFIG_TOP_LEVEL_KEYS
    assert config["resources"]["io_manager"]["config"]["s3_bucket"] == "staging-bucket"


def test_probe_launch_refuses_an_unresolved_pointer():
    from dag_tools.qual.probes.runner import _launch_probe

    client = _client()
    with pytest.raises(StagingOverridesNotResolved):
        _launch_probe(
            client, _probe(), qual_id="q1", side="candidate",
            manifest=_manifest("s3://cfg/staging.yaml"),
        )
    client.launch_asset_run.assert_not_called()


# ---------------------------------------------------------------------------
# S3Storage.get_uri — the pointer may live outside the registry bucket
# ---------------------------------------------------------------------------


def test_get_uri_reads_the_bucket_named_in_the_uri_not_the_registry_bucket():
    from dag_tools.qual.registry.client import S3Storage, StorageSettings

    s3 = MagicMock()
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: b"resources: {}\n")}
    storage = S3Storage(StorageSettings(bucket="registry-bucket"), s3_client=s3)

    storage.get_uri("s3://other-bucket/path/to/staging.yaml")

    kwargs = s3.get_object.call_args.kwargs
    assert kwargs["Bucket"] == "other-bucket"
    assert kwargs["Key"] == "path/to/staging.yaml"


def test_get_uri_rejects_a_non_s3_uri():
    from dag_tools.qual.registry.client import S3Storage, StorageSettings

    storage = S3Storage(StorageSettings(bucket="b"), s3_client=MagicMock())
    with pytest.raises(ValueError):
        storage.get_uri("https://example.com/staging.yaml")


def test_get_uri_rejects_a_bucket_with_no_key():
    from dag_tools.qual.registry.client import S3Storage, StorageSettings

    storage = S3Storage(StorageSettings(bucket="b"), s3_client=MagicMock())
    with pytest.raises(ValueError):
        storage.get_uri("s3://just-a-bucket")
