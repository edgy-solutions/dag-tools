import os
from typing import Dict, Any

def resolve_k8s_resource_tags(prefix: str, default_cpu: str = "500m", default_mem: str = "1Gi") -> Dict[str, Any]:
    """
    Utility to resolve Kubernetes resource requests and limits from environment variables.
    
    Expected environment variables (where <PREFIX> is the provided prefix):
    - <PREFIX>_CPU_REQUEST
    - <PREFIX>_MEM_REQUEST
    - <PREFIX>_CPU_LIMIT
    - <PREFIX>_MEM_LIMIT
    
    If limits are not provided, they default to the request values to maintain a 1:1 ratio,
    ensuring a predictable resource capping and assisting in K8s node scheduling.
    
    Args:
        prefix: The environment variable prefix (e.g., 'INGEST_JOB').
        default_cpu: Fallback CPU request (e.g., '500m').
        default_mem: Fallback memory request (e.g., '1Gi').
        
    Returns:
        A dictionary formatted for Dagster's 'dagster-k8s/config' tag. 
        This should be passed to the 'op_tags' parameter of an @asset or @op decorator,
        or the 'tags' parameter of a Job/Schedule.
    """
    def _fetch_clean_env(key: str, fallback: str) -> str:
        val = os.environ.get(key)
        return str(val).strip() if val else fallback

    req_cpu = _fetch_clean_env(f"{prefix}_CPU_REQUEST", default_cpu)
    req_mem = _fetch_clean_env(f"{prefix}_MEM_REQUEST", default_mem)
    
    # Logic: If limit isn't provided, use the request value to maintain a consistent ratio
    lim_cpu = _fetch_clean_env(f"{prefix}_CPU_LIMIT", req_cpu)
    lim_mem = _fetch_clean_env(f"{prefix}_MEM_LIMIT", req_mem)
        
    return {
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": req_cpu, "memory": req_mem},
                    "limits": {"cpu": lim_cpu, "memory": lim_mem},
                }
            }
        }
    }


def deep_merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge ``override`` into ``base``; ``override`` wins on
    leaf conflicts. Nested dicts merge; everything else is replaced.

    Used to layer explicit op-tags on top of env-prefix-resolved k8s
    resources without clobbering sibling keys (e.g. keep a node selector
    while overriding one resource value).
    """
    result = dict(base)
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def resolve_op_tags_with_env_prefix(
    env_prefix: Any,
    explicit_tags: Any,
    default_cpu: str = "500m",
    default_mem: str = "1Gi",
) -> Dict[str, Any]:
    """The env-prefix convention for component op-tags.

    When ``env_prefix`` is set, resolve ``<PREFIX>_CPU_REQUEST`` /
    ``_MEM_REQUEST`` / ``_CPU_LIMIT`` / ``_MEM_LIMIT`` into the
    ``dagster-k8s/config`` op-tag via :func:`resolve_k8s_resource_tags`,
    then deep-merge ``explicit_tags`` ON TOP (explicit wins on leaf
    conflicts, so callers can override a single value or add node
    selectors / tolerations alongside the env-driven resources).

    When ``env_prefix`` is falsy, returns a copy of ``explicit_tags``
    unchanged — so components that don't opt into the convention behave
    exactly as before.
    """
    explicit_tags = dict(explicit_tags or {})
    if not env_prefix:
        return explicit_tags
    resolved = resolve_k8s_resource_tags(
        prefix=env_prefix, default_cpu=default_cpu, default_mem=default_mem,
    )
    return deep_merge_dicts(resolved, explicit_tags)
