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
