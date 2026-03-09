from typing import Any, Dict, List
from dagster import AssetKey, AssetSpec


def create_source_assets(source_asset_keys: List[Dict[str, Any]], io_manager_key: str) -> List[AssetSpec]:
    """Generates a collection of Dagster AssetSpecs mapping to external 'Source' data repositories."""
    sources_assets = []
    
    for src in source_asset_keys:
        key_value = src.get('key')
        if not key_value:
            continue
            
        # Handle string based asset paths vs array paths
        parsed_key = AssetKey(key_value) if isinstance(key_value, list) else AssetKey.from_user_string(key_value)
            
        sources_assets.append(
            AssetSpec(
                key=parsed_key,
                metadata={"dagster/io_manager_key": io_manager_key},
                kinds=['s3'],
                group_name='sources'
            )
        )
        
    return sources_assets
