from typing import Any, Dict, List, Mapping
from dagster import AssetKey

class AssetNormalizationRegistry:
    """Centralized registry for stripping connector prefixes and injecting lineage paths.
    
    This abstracts source-specific normalization (e.g., Airbyte, DLT) out of the DBT component 
    so that downstream consumers do not get polluted with ad-hoc logic.
    """
    
    # Generic mappings for standard ingestion tools
    PREFIX_MAPPING: Dict[str, List[str]] = {
        "_airbyte_raw_": ["airbyte", "raw"],
        "airbyte_": ["airbyte"],
        "dlt_": ["dlt"]
    }

    @classmethod
    def register_prefix(cls, prefix: str, lineage: List[str]) -> None:
        """Allows external systems (like Airbyte or DLT components) to dynamically inject normalizations."""
        cls.PREFIX_MAPPING[prefix] = lineage

    @classmethod
    def apply(cls, node_info: Mapping[str, Any]) -> AssetKey:
        """Translates a DBT node's physical model name into a standardized Dagster AssetKey."""
        path: List[str] = ["dbt"]
        
        identifier = node_info.get("identifier", node_info.get("name", ""))
        
        # We only apply complex physical stripping to `source` models
        if node_info.get("resource_type") == "source":
            for prefix, lineage in cls.PREFIX_MAPPING.items():
                if identifier.startswith(prefix):
                    identifier = identifier.replace(prefix, "", 1)
                    path = lineage
                    break
            else:
                source_name = node_info.get("source_name", "")
                if source_name.startswith("airbyte"):
                    path = ["airbyte"]
                elif source_name.startswith("dlt"):
                    path = ["dlt"]
                else:
                    path = []
                    
        # Append database hierarchy
        if node_info.get("database"):
            path.append(node_info["database"])
            
        if node_info.get("schema"):
            path.extend([node_info["schema"], identifier])
        else:
            path.append(identifier)
            
        return AssetKey(path)
