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
