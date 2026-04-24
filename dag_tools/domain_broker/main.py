import os
import json
import logging
import httpx
import boto3
from typing import Dict, Any, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Optional Dagster imports
try:
    from dagster import Definitions, AssetKey
    from dag_tools.components.datahub_lineage.component import asset_keys_to_dataset_urn_converter
except ImportError:
    Definitions = None
    AssetKey = None
    asset_keys_to_dataset_urn_converter = None

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CENTRAL_GATEWAY_URL = os.getenv("CENTRAL_GATEWAY_URL", "http://central-gateway.default.svc.cluster.local")
BROKER_URL = os.getenv("BROKER_URL", "http://domain-broker.team-a.svc.cluster.local")
DAGSTER_DEFS_MODULE = os.getenv("DAGSTER_DEFS_MODULE", "")

# In-memory registry of assets
LOCAL_ASSETS: Dict[str, Any] = {}

class ResolveRequest(BaseModel):
    urn: str

def extract_io_manager_info(defs: 'Definitions', asset_key: 'AssetKey') -> Dict[str, Any]:
    """
    Extracts IO Manager type and configuration from Dagster Definitions.
    This is a simplified representation of how one might inspect Dagster's IO managers.
    """
    # Find the asset spec
    spec = next((s for s in defs.get_all_asset_specs() if s.key == asset_key), None)
    if not spec:
        return {"io_manager_type": "s3_parquet"} # fallback
        
    io_manager_key = spec.io_manager_key or "io_manager"
    
    # Try to get the IO manager resource from definitions
    resource_def = defs.resources.get(io_manager_key)
    
    info = {
        "io_manager_key": io_manager_key,
        "metadata": spec.metadata or {}
    }
    
    # Determine type based on resource class name or config
    if resource_def:
        class_name = resource_def.__class__.__name__.lower()
        if "postgres" in class_name or "clickhouse" in class_name:
            info["io_manager_type"] = "postgres" if "postgres" in class_name else "clickhouse"
            info["db_host"] = "db.local"
            info["schema"] = "public"
            info["table"] = asset_key.path[-1]
        elif "s3" in class_name or "parquet" in class_name or "iceberg" in class_name or "delta" in class_name:
            if "iceberg" in class_name:
                info["io_manager_type"] = "s3_iceberg"
            elif "delta" in class_name:
                info["io_manager_type"] = "s3_delta"
            else:
                info["io_manager_type"] = "s3_parquet"
                
            info["bucket"] = "my-data-lake"
            info["prefix"] = "/".join(asset_key.path)
    else:
        # Fallback based on metadata or defaults
        info["io_manager_type"] = spec.metadata.get("io_manager_type", "s3_parquet")
        info["bucket"] = spec.metadata.get("bucket", "default-bucket")
        info["prefix"] = spec.metadata.get("prefix", "/".join(asset_key.path))
        
    return info

def load_dagster_definitions():
    """Load local Dagster definitions and populate LOCAL_ASSETS."""
    if not DAGSTER_DEFS_MODULE or not Definitions:
        logger.warning("No DAGSTER_DEFS_MODULE specified or Dagster not installed. Using mock assets.")
        LOCAL_ASSETS["my_postgres_table"] = {
            "io_manager_type": "postgres",
            "db_host": "postgres.db.local",
            "db_port": 5432,
            "schema": "public",
            "table": "my_table"
        }
        LOCAL_ASSETS["my_s3_parquet"] = {
            "io_manager_type": "s3_parquet",
            "bucket": "my-data-lake",
            "prefix": "data/my_s3_parquet"
        }
        return

    import importlib
    try:
        module_name, attr_name = DAGSTER_DEFS_MODULE.split(":")
        module = importlib.import_module(module_name)
        defs = getattr(module, attr_name)
        
        if isinstance(defs, Definitions):
            for spec in defs.get_all_asset_specs():
                key_str = spec.key.to_user_string()
                
                # Extract the URN. Assumes you tag assets with "datahub/urn" 
                # or fallback to generating a deterministic one.
                urn = spec.tags.get("datahub/urn") if spec.tags else None
                if not urn:
                    if asset_keys_to_dataset_urn_converter:
                        dataset_urn = asset_keys_to_dataset_urn_converter(spec.key.path)
                        urn = dataset_urn.urn() if dataset_urn else None
                    if not urn:
                        # Fallback deterministic URN generation
                        urn = f"urn:li:dataset:(urn:li:dataPlatform:dagster,{key_str.replace('/', '.')},PROD)"
                
                # Index the dictionary by URN!
                LOCAL_ASSETS[urn] = extract_io_manager_info(defs, spec.key)
            logger.info(f"Loaded {len(LOCAL_ASSETS)} assets mapped by URN.")
    except Exception as e:
        logger.error(f"Failed to load Dagster definitions: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Load definitions
    load_dagster_definitions()
    asset_keys = list(LOCAL_ASSETS.keys())
    
    # Register with Central Gateway
    try:
        async with httpx.AsyncClient() as client:
            payload = {
                "broker_url": BROKER_URL,
                "asset_urns": list(LOCAL_ASSETS.keys())
            }
            response = await client.post(
                f"{CENTRAL_GATEWAY_URL}/api/v1/internal/register",
                json=payload,
                timeout=10.0
            )
            response.raise_for_status()
            logger.info(f"Successfully registered {len(LOCAL_ASSETS.keys())} assets with Central Gateway.")
    except Exception as e:
        logger.error(f"Failed to register with Central Gateway: {e}")
        
    yield
    # Shutdown logic
    logger.info("Domain Broker shutting down.")

app = FastAPI(lifespan=lifespan, title="Domain Broker")

@app.post("/api/v1/internal/resolve")
async def resolve_asset(request: ResolveRequest):
    """
    Called ONLY by the Central Gateway to resolve a DataHub URN into a physical routing ticket.
    """
    urn = request.urn
    
    if urn not in LOCAL_ASSETS:
        raise HTTPException(status_code=404, detail="URN not found in this domain's Dagster deployment.")
        
    asset_info = LOCAL_ASSETS[urn]
    io_type = asset_info.get("io_manager_type", "s3_parquet")
    
    if io_type in ["postgres", "clickhouse"]:
        host = asset_info.get("db_host", "localhost")
        port = asset_info.get("db_port", 5432)
        schema = asset_info.get("schema", "public")
        table = asset_info.get("table", urn.split(",")[-2] if "urn:li:dataset" in urn else urn)
        
        return {
            "source_type": io_type,
            "physical_uri": f"{io_type}://{host}:{port}/{schema}/{table}",
            "credentials": {
                "token": "read-only-db-token-123"
            }
        }
        
    elif io_type in ["s3_parquet", "s3_iceberg", "s3_delta"]:
        bucket = asset_info.get("bucket", "default-bucket")
        fallback_prefix = f"warehouse/{urn.split(',')[-2].replace('.', '/')}" if "urn:li:dataset" in urn else f"warehouse/{urn}"
        prefix = asset_info.get("prefix", fallback_prefix)
        role_arn = os.getenv("AWS_ASSUME_ROLE_ARN", "arn:aws:iam::123456789012:role/DataAccessRole")
        
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/{prefix}/*"]
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}"],
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": [f"{prefix}/*", f"{prefix}"]
                        }
                    }
                }
            ]
        }
        
        try:
            sts_client = boto3.client('sts')
            response = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=f"session-{urn.replace(':', '_').replace(',', '_')[:40]}",
                Policy=json.dumps(policy),
                DurationSeconds=3600
            )
            credentials = response['Credentials']
            
            return {
                "source_type": io_type,
                "physical_uri": f"s3://{bucket}/{prefix}",
                "credentials": {
                    "aws_access_key_id": credentials['AccessKeyId'],
                    "aws_secret_access_key": credentials['SecretAccessKey'],
                    "aws_session_token": credentials['SessionToken']
                }
            }
        except Exception as e:
            logger.error(f"Failed to mint STS token: {e}")
            # Fallback for environments without AWS configured
            return {
                "source_type": io_type,
                "physical_uri": f"s3://{bucket}/{prefix}",
                "credentials": {
                    "aws_access_key_id": "mock_access_key",
                    "aws_secret_access_key": "mock_secret_key",
                    "aws_session_token": "mock_session_token"
                }
            }
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported IO manager type: {io_type}")
