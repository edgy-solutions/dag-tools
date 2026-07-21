"""Tests for the :class:`DltAssetGroupConfig` fields that user-deployment
YAML relies on for per-pipeline k8s resource specification.

The wiring path is:
  YAML `pipelines.<name>.op_tags` → `DltAssetGroupConfig.op_tags`
  → `instantiate_assets` → `dlt_assets_with_io_managers(op_tags=...)`
  → Dagster `@multi_asset(op_tags=...)` → k8s executor reads
    `dagster-k8s/config` at run submit time.

Only the pydantic hop is covered here; the decorator's `op_tags`
passthrough is pre-existing Dagster behavior.
"""
import pytest

from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig
from dag_tools.utils.k8s import resolve_k8s_resource_tags


def test_config_defaults_op_tags_and_pool():
    """Default construction leaves both fields empty — pre-existing user
    YAML that never mentioned op_tags / pool must keep working unchanged."""
    cfg = DltAssetGroupConfig()
    assert cfg.op_tags == {}
    assert cfg.pool is None


def test_config_accepts_dagster_k8s_config_shape():
    """The primary use case — resource requests / limits via
    `dagster-k8s/config`, in the exact shape the k8s launcher reads."""
    op_tags = {
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "8Gi"},
                    "limits":   {"cpu": "4000m", "memory": "16Gi"},
                },
            },
        },
    }
    cfg = DltAssetGroupConfig(op_tags=op_tags, pool="heavy-ingest")
    assert cfg.op_tags == op_tags
    assert cfg.pool == "heavy-ingest"


def test_config_accepts_output_of_resolve_k8s_resource_tags(monkeypatch):
    """The utility documented in the field description must produce a
    dict the field accepts verbatim — otherwise the docstring lies."""
    monkeypatch.setenv("INGEST_CPU_REQUEST", "1000m")
    monkeypatch.setenv("INGEST_MEM_REQUEST", "4Gi")
    tags = resolve_k8s_resource_tags("INGEST")
    cfg = DltAssetGroupConfig(op_tags=tags)
    assert cfg.op_tags["dagster-k8s/config"]["container_config"]["resources"]["requests"]["cpu"] == "1000m"
    assert cfg.op_tags["dagster-k8s/config"]["container_config"]["resources"]["requests"]["memory"] == "4Gi"


def test_effective_op_tags_resolves_env_prefix(monkeypatch):
    """The doc-tools / at-work convention: name an env prefix, the
    deployment sets <PREFIX>_CPU_REQUEST etc., the component resolves
    them into op_tags at defs-load time."""
    monkeypatch.setenv("PDM_INGEST_CPU_REQUEST", "2000m")
    monkeypatch.setenv("PDM_INGEST_MEM_REQUEST", "8Gi")
    monkeypatch.setenv("PDM_INGEST_CPU_LIMIT", "4000m")
    monkeypatch.setenv("PDM_INGEST_MEM_LIMIT", "16Gi")

    cfg = DltAssetGroupConfig(k8s_resource_env_prefix="PDM_INGEST")
    res = cfg.effective_op_tags()["dagster-k8s/config"]["container_config"]["resources"]
    assert res["requests"] == {"cpu": "2000m", "memory": "8Gi"}
    assert res["limits"] == {"cpu": "4000m", "memory": "16Gi"}


def test_effective_op_tags_env_prefix_uses_defaults_when_unset(monkeypatch):
    monkeypatch.delenv("EMPTY_PREFIX_CPU_REQUEST", raising=False)
    monkeypatch.delenv("EMPTY_PREFIX_MEM_REQUEST", raising=False)
    cfg = DltAssetGroupConfig(
        k8s_resource_env_prefix="EMPTY_PREFIX",
        k8s_default_cpu="750m", k8s_default_mem="3Gi",
    )
    res = cfg.effective_op_tags()["dagster-k8s/config"]["container_config"]["resources"]
    # Limits default to requests (resolve_k8s_resource_tags contract).
    assert res["requests"] == {"cpu": "750m", "memory": "3Gi"}
    assert res["limits"] == {"cpu": "750m", "memory": "3Gi"}


def test_effective_op_tags_no_prefix_returns_explicit_op_tags():
    """No prefix -> effective is just the explicit op_tags (back-compat)."""
    ot = {"dagster-k8s/config": {"container_config": {"resources": {"requests": {"cpu": "1"}}}}}
    cfg = DltAssetGroupConfig(op_tags=ot)
    assert cfg.effective_op_tags() == ot


def test_effective_op_tags_explicit_deep_merges_over_env_prefix(monkeypatch):
    """Explicit op_tags win on leaf conflicts AND coexist with env-driven
    resources: keep the env-resolved memory, override cpu, add a node
    selector — all in one merged dagster-k8s/config."""
    monkeypatch.setenv("MIX_CPU_REQUEST", "2000m")
    monkeypatch.setenv("MIX_MEM_REQUEST", "8Gi")

    cfg = DltAssetGroupConfig(
        k8s_resource_env_prefix="MIX",
        op_tags={
            "dagster-k8s/config": {
                "container_config": {
                    "resources": {"requests": {"cpu": "9000m"}},  # override cpu only
                },
                "pod_spec_config": {"node_selector": {"disktype": "ssd"}},  # add sibling
            }
        },
    )
    cfg_tags = cfg.effective_op_tags()["dagster-k8s/config"]
    req = cfg_tags["container_config"]["resources"]["requests"]
    assert req["cpu"] == "9000m"          # explicit wins
    assert req["memory"] == "8Gi"         # env-resolved memory preserved
    assert cfg_tags["pod_spec_config"]["node_selector"] == {"disktype": "ssd"}  # sibling kept


def test_config_yaml_forwarded_shape_via_component_style_kwargs():
    """`DltPipelineComponent.build_defs` splats YAML `pipeline_attrs`
    into `DltAssetGroupConfig(**pipeline_attrs)`. Verify the field names
    match what YAML would surface (snake_case, top-level)."""
    pipeline_attrs = {
        "name": "heavy_ingest",
        "op_tags": {"dagster-k8s/config": {"container_config": {"resources": {"requests": {"cpu": "2"}}}}},
        "pool": "heavy-ingest",
    }
    cfg = DltAssetGroupConfig(**pipeline_attrs)
    assert cfg.name == "heavy_ingest"
    assert cfg.pool == "heavy-ingest"
    assert cfg.op_tags["dagster-k8s/config"]["container_config"]["resources"]["requests"]["cpu"] == "2"
