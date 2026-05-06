import os
import json
import pytest
from dag_tools.utils.k8s import resolve_k8s_resource_tags

def test_resolve_k8s_resource_tags_returns_json_string():
    """
    Verify that the utility returns a dictionary where values are JSON strings,
    as required by Dagster's strict tag validation.
    """
    # Clear env for clean test
    prefix = "UNIT_TEST"
    for k in [f"{prefix}_CPU_REQUEST", f"{prefix}_MEM_REQUEST", f"{prefix}_CPU_LIMIT", f"{prefix}_MEM_LIMIT"]:
        if k in os.environ:
            del os.environ[k]

    tags = resolve_k8s_resource_tags(prefix=prefix, default_cpu="100m", default_mem="256Mi")
    
    # 1. Check top-level structure
    assert "dagster-k8s/config" in tags
    tag_value = tags["dagster-k8s/config"]
    
    # 2. Verify it is a string (Dagster requirement)
    assert isinstance(tag_value, str)
    
    # 3. Verify it is valid JSON and contains the correct K8s spec
    config = json.loads(tag_value)
    expected_resources = {
        "requests": {"cpu": "100m", "memory": "256Mi"},
        "limits": {"cpu": "100m", "memory": "256Mi"},
    }
    assert config["container_config"]["resources"] == expected_resources

def test_resolve_k8s_resource_tags_env_overrides():
    """Verify that environment variables correctly override defaults and are cleaned."""
    prefix = "PROD_EXTRACT"
    os.environ[f"{prefix}_CPU_REQUEST"] = " 500m "  # Test whitespace stripping
    os.environ[f"{prefix}_MEM_REQUEST"] = "1Gi"
    os.environ[f"{prefix}_CPU_LIMIT"] = "1000m"
    # MEM_LIMIT is missing, should default to request
    
    tags = resolve_k8s_resource_tags(prefix=prefix)
    config = json.loads(tags["dagster-k8s/config"])
    
    resources = config["container_config"]["resources"]
    assert resources["requests"]["cpu"] == "500m"
    assert resources["requests"]["memory"] == "1Gi"
    assert resources["limits"]["cpu"] == "1000m"
    assert resources["limits"]["memory"] == "1Gi" # Defaulted to request
