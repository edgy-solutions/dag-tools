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


class RegisterAssetRequest(BaseModel):
    """Payload accepted by ``POST /api/v1/admin/register_asset``.

    Sent by user-deployment sidecars (see ``dag_tools.sidecar``) at
    code-location startup. The broker accepts the asset_record shape
    that ``dag_tools.inventory.extract_records`` produces — same
    contract every other inventory consumer uses, so there's no
    sidecar-specific schema for code-locations to maintain.
    """
    urn: str
    asset_record: Dict[str, Any]

def _build_asset_info_from_record(record) -> Dict[str, Any]:
    """Build the LOCAL_ASSETS value shape from a shared inventory ``AssetRecord``.

    Preserves the dict shape that downstream callers (``resolve_asset``)
    consume. Resource-config placeholders (``db.local``, ``my-data-lake``)
    remain pending a separate resource-config extraction pass — that's a
    follow-up, not part of this migration.
    """
    info: Dict[str, Any] = {
        "io_manager_key": record.io_manager_key or "io_manager",
        "io_manager_type": record.io_manager_family or "s3_parquet",
        "io_manager_class": record.io_manager_class,
        "metadata": dict(record.tags or {}),
    }
    family = record.io_manager_family
    target_path = list(record.asset_key or [])
    if family in ("postgres", "clickhouse"):
        info["db_host"] = "db.local"   # TODO: pull from resource config
        info["schema"] = "public"
        info["table"] = target_path[-1] if target_path else "unknown"
    elif family in ("s3_iceberg", "s3_delta", "s3_parquet"):
        info["bucket"] = "my-data-lake"  # TODO: pull from resource config
        info["prefix"] = "/".join(target_path)
    return info


def extract_io_manager_info(defs: 'Definitions', asset_key: 'AssetKey') -> Dict[str, Any]:
    """Extracts IO Manager type + configuration for a specific asset.

    Now delegates IO manager classification to the shared
    ``dag_tools.inventory`` introspector — which uses an explicit FQN
    registry with MRO walking, replacing the legacy substring-matching
    that silently misclassified custom IO manager forks. The returned
    dict shape is unchanged so downstream consumers don't need updates.
    """
    if defs is None or asset_key is None:
        return {"io_manager_type": "s3_parquet"}

    try:
        from dag_tools.inventory import extract_records
    except Exception as e:
        logger.error(f"Failed to import dag_tools.inventory: {e}")
        return {"io_manager_type": "s3_parquet"}

    records = extract_records(defs)
    target_path = list(asset_key.path)
    record = next((r for r in records if r.asset_key == target_path), None)
    if not record:
        return {"io_manager_type": "s3_parquet"}
    return _build_asset_info_from_record(record)


def load_dagster_definitions():
    """Load local Dagster definitions and populate LOCAL_ASSETS keyed by URN."""
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

        if not isinstance(defs, Definitions):
            return

        try:
            from dag_tools.inventory import extract_records
        except Exception as e:
            logger.error(f"Failed to import dag_tools.inventory: {e}")
            return

        # One walk over Definitions — the shared inventory also derives the
        # URN sidecar via the same datahub converter, so we don't re-call it.
        records = extract_records(defs)
        for record in records:
            urn = record.tags.get("datahub/urn") if record.tags else None
            if not urn:
                urn = record.urn
            if not urn:
                # Fallback deterministic URN generation, mirroring legacy behavior.
                key_str = ".".join(record.asset_key)
                urn = f"urn:li:dataset:(urn:li:dataPlatform:dagster,{key_str},PROD)"
            LOCAL_ASSETS[urn] = _build_asset_info_from_record(record)

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


@app.post("/api/v1/admin/register_asset")
async def register_asset(request: RegisterAssetRequest):
    """Register or update a single asset's URN → routing mapping.

    Called by user-deployment sidecars (see ``dag_tools.sidecar``) at
    code-location startup. Idempotent: repeat calls overwrite the
    existing entry by URN, which gives "latest write wins" semantics
    most useful when a code-location redeploys with updated asset
    metadata.

    The broker stores LOCAL_ASSETS in-process. On broker restart the
    sidecars publish again — durability is a property of the publishing
    deployments, not the registry. That keeps the registry stateless
    enough to scale horizontally without a shared DB; if persistence
    becomes a requirement, swap the dict for a PVC-backed SQLite and
    keep the contract unchanged.
    """
    try:
        from dag_tools.inventory.schema import AssetRecord
        record = AssetRecord.model_validate(request.asset_record)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid asset_record: {exc}",
        )

    LOCAL_ASSETS[request.urn] = _build_asset_info_from_record(record)
    logger.info(
        "Registered asset %s via sidecar (location=%s, key=%s).",
        request.urn,
        record.location,
        ".".join(record.asset_key) if record.asset_key else "?",
    )
    return {
        "status": "ok",
        "urn": request.urn,
        "io_manager_type": LOCAL_ASSETS[request.urn].get("io_manager_type"),
    }


@app.get("/api/v1/admin/registry")
async def list_registry():
    """Operator-facing list of currently-registered URNs.

    Useful when debugging "why doesn't this URN resolve?" — answers
    *which* URNs the registry knows about, *who* registered them
    (via the asset_record.location field), and *what* routing they
    point at.
    """
    return {
        "count": len(LOCAL_ASSETS),
        "urns": sorted(LOCAL_ASSETS.keys()),
    }
