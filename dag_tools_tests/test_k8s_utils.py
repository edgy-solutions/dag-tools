import os
import pytest
from dag_tools.utils.k8s import resolve_k8s_resource_tags

def test_resolve_k8s_resource_tags_returns_nested_dict():
    """
    Verify that the utility returns a nested dictionary, which is required
    for dagster-k8s config when passed via op_tags or Job tags.
    """
    prefix = "UNIT_TEST"
    for k in [f"{prefix}_CPU_REQUEST", f"{prefix}_MEM_REQUEST", f"{prefix}_CPU_LIMIT", f"{prefix}_MEM_LIMIT"]:
        if k in os.environ:
            del os.environ[k]

    tags = resolve_k8s_resource_tags(prefix=prefix, default_cpu="100m", default_mem="256Mi")
    
    # 1. Check top-level structure
    assert "dagster-k8s/config" in tags
    config = tags["dagster-k8s/config"]
    
    # 2. Verify it is a DICTIONARY (native K8s config format)
    assert isinstance(config, dict)
    
    # 3. Verify structure
    expected_resources = {
        "requests": {"cpu": "100m", "memory": "256Mi"},
        "limits": {"cpu": "100m", "memory": "256Mi"},
    }
    assert config["container_config"]["resources"] == expected_resources

def test_resolve_k8s_resource_tags_compatibility_with_dagster():
    """
    Verify that Dagster's @asset decorator accepts the nested dictionary
    when passed via 'op_tags'.
    """
    from dagster import asset
    
    k8s_tags = resolve_k8s_resource_tags(prefix="DAGSTER_TEST")
    
    # This should SUCCEED with a dictionary because op_tags bypasses 
    # the strict string-only validation used for UI tags.
    @asset(op_tags={**k8s_tags})
    def test_asset_op_tags():
        return 1
        
    import json
    # Dagster automatically JSON-serializes complex tag values internally, 
    # so we parse it back to a dict for the comparison.
    dagster_val = test_asset_op_tags.op.tags["dagster-k8s/config"]
    assert json.loads(dagster_val) == k8s_tags["dagster-k8s/config"]

def test_resolve_k8s_resource_tags_env_overrides():
    """Verify environment variable overrides work correctly."""
    prefix = "PROD_EXTRACT"
    os.environ[f"{prefix}_CPU_REQUEST"] = " 500m "
    os.environ[f"{prefix}_MEM_REQUEST"] = "1Gi"
    
    tags = resolve_k8s_resource_tags(prefix=prefix)
    config = tags["dagster-k8s/config"]
    
    resources = config["container_config"]["resources"]
    assert resources["requests"]["cpu"] == "500m"
    assert resources["requests"]["memory"] == "1Gi"
    assert resources["limits"]["cpu"] == "500m" # Defaulted to request
