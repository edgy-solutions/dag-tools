from typing import Any, Dict, Optional, Sequence
import dagster._check as check

MAX_KEYS = 1000

def get_s3_keys(bucket: str, s3_session: Any, log: Any, prefix: str = "", since_key: Optional[str] = None) -> Dict[str, Any]:
    """Retrieves chronological S3 keys using LastModified for cursor pagination.
    Explicitly handles pagination using ContinuationToken to ensure all objects are seen.
    """
    check.str_param(bucket, "bucket")
    check.str_param(prefix, "prefix")
    check.opt_str_param(since_key, "since_key")

    contents = []
    continuation_token = None

    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": MAX_KEYS,
        }
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
            
        response = s3_session.list_objects_v2(**kwargs)
        contents.extend(response.get("Contents", []))
        
        if not response.get("IsTruncated"):
            break
            
        continuation_token = response.get("NextContinuationToken")

    # Deduplicate and sort by chronological time to ensure cursor stability
    sorted_keys = {
        str(obj["LastModified"]): obj 
        for obj in sorted(contents, key=lambda x: x.get("LastModified"))
        if "LastModified" in obj
    }

    if not since_key or since_key not in sorted_keys:
        return sorted_keys

    # Find the since_key and return only elements after it
    keys_list = list(sorted_keys.keys())
    for idx, key in enumerate(keys_list):
        if key == since_key:
            return {
                k: sorted_keys[k] 
                for k in keys_list[idx + 1 :]
            }
    return {}

from dagster import MultiPartitionsDefinition, DynamicPartitionsDefinition

def key_2_partition_key(key: str) -> str:
    """Default key extractor returning the raw S3 string path as the partition."""
    return key

def get_dynamic_partitions_requests(tables_partitions_def: Any, keys: Dict[str, Any], key_extract_func=key_2_partition_key):
    """Generates Dagster partition injection requests dynamically based on detected keys."""
    if isinstance(tables_partitions_def, MultiPartitionsDefinition):
        dimensions = {}
        items = [key_extract_func(key) for key in list(keys.values())]
        for item in items:
            for key, value in item.keys_by_dimension.items():
                if isinstance(tables_partitions_def.get_partitions_def_for_dimension(key), DynamicPartitionsDefinition):
                    dimensions.setdefault(key, set()).add(value)
        return [
            tables_partitions_def.get_partitions_def_for_dimension(dimension).build_add_request(list(keys_set)) 
            for dimension, keys_set in dimensions.items()
        ]
    return [tables_partitions_def.build_add_request(list(keys.values()))]
