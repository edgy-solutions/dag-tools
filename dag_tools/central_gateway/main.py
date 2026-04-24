import os
import json
import logging
import httpx
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import redis.asyncio as redis
import jwt  # For basic decoding of the Keycloak JWT

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TOPAZ_URL = os.getenv("TOPAZ_URL", "https://localhost:8383")
TOPAZ_AUTHORIZER_API_KEY = os.getenv("TOPAZ_AUTHORIZER_API_KEY", "")

# We'll initialize redis client in lifespan
redis_client: Optional[redis.Redis] = None

class RegisterPayload(BaseModel):
    broker_url: str
    asset_urns: List[str]

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("Connected to Redis.")
    yield
    if redis_client:
        await redis_client.close()
    logger.info("Central Gateway shutting down.")

app = FastAPI(lifespan=lifespan, title="Central Gateway")
security = HTTPBearer()

@app.post("/api/v1/internal/register")
async def register_broker(payload: RegisterPayload):
    """
    Receives heartbeats from Domain Brokers.
    Stores mesh_route:{asset_key} -> broker_url in Redis with a 5-minute TTL.
    """
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis client not initialized")
        
    pipeline = redis_client.pipeline()
    ttl_seconds = 300  # 5 minutes
    
    for urn in payload.asset_urns:
        redis_key = f"mesh_route:{urn}"
        pipeline.setex(redis_key, ttl_seconds, payload.broker_url)
        
    await pipeline.execute()
    logger.info(f"Registered {len(payload.asset_urns)} assets for broker {payload.broker_url}")
    return {"status": "success", "registered_assets": len(payload.asset_urns)}

async def check_topaz_authz(token: str, urn: str) -> tuple[bool, Optional[List[str]], Optional[str]]:
    """
    Calls the Topaz REST API to check authorization.
    Subject = Keycloak user_id (extracted from JWT)
    Resource = {urn}
    Permission = can_read
    """
    try:
        # Decode JWT to get user_id (sub).
        unverified_claims = jwt.decode(token, options={"verify_signature": False})
        user_id = unverified_claims.get("sub")
        if not user_id:
            logger.error("JWT does not contain a 'sub' claim for user_id")
            return False
            
        # Topaz API call (using standard Aserto/Topaz REST API format for Is)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        if TOPAZ_AUTHORIZER_API_KEY:
            headers["Aserto-Tenant-Id"] = TOPAZ_AUTHORIZER_API_KEY
            
        # Standard Topaz check API format (v2/authz/is)
        authz_payload = {
            "identity_context": {
                "identity": user_id,
                "type": "IDENTITY_TYPE_SUB"
            },
            "policy_context": {
                "path": "data_mesh.GET.api.v1.assets.__asset_key.authorize",
                "decisions": ["allowed"]
            },
            "resource_context": {
                "asset": urn,
                "permission": "can_read"
            }
        }
        
        # Fallback/alternative Topaz payload using relations (if Topaz is configured for directory checks)
        check_payload = {
            "subject": {
                "type": "user",
                "key": user_id
            },
            "object": {
                "type": "dataset",
                "key": urn
            },
            "relation": {
                "name": "can_read"
            }
        }
        
        async with httpx.AsyncClient() as client:
            # We assume a check API endpoint is available on Topaz
            # Depending on exact Topaz setup, this could be /api/v2/authz/is or /api/v2/directory/check
            response = await client.post(
                f"{TOPAZ_URL}/api/v2/authz/is",
                json=authz_payload,
                headers=headers,
                timeout=5.0
            )
            
            if response.status_code == 200:
                result = response.json()
                decisions = result.get("decisions", [])
                if isinstance(decisions, list) and len(decisions) > 0:
                    decision_obj = decisions[0]
                    if isinstance(decision_obj, dict):
                        is_authorized = decision_obj.get("is", False)
                if not is_authorized:
                    is_authorized = result.get("decision", False) or result.get("is", False)
                
                # Extract data-masking rules
                allowed_columns = result.get("allowed_columns") or result.get("fields")
                row_filters = result.get("row_filters") or result.get("row_filter") or result.get("filters")
                
                return is_authorized, allowed_columns, row_filters
            else:
                logger.error(f"Topaz AuthZ failed with status {response.status_code}: {response.text}")
                # For development/testing if Topaz is unreachable
                if os.getenv("ALLOW_MOCK_AUTH", "false").lower() == "true":
                    logger.warning("Using mock auth due to Topaz failure")
                    return True, None, None
                return False, None, None
                
    except Exception as e:
        logger.error(f"Error during Topaz AuthZ: {e}")
        if os.getenv("ALLOW_MOCK_AUTH", "false").lower() == "true":
            return True, None, None
        return False, None, None

@app.post("/api/v1/assets/{urn:path}/authorize")
async def authorize_asset(urn: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verifies user JWT against Topaz, looks up asset in Redis, proxies to Domain Broker.
    """
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis client not initialized")
        
    token = credentials.credentials
    
    # 1. Topaz AuthZ
    is_authorized, allowed_columns, row_filters = await check_topaz_authz(token, urn)
    if not is_authorized:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Not authorized to access this asset"
        )
        
    # 2. Routing: O(1) lookup in Redis
    redis_key = f"mesh_route:{urn}"
    broker_url = await redis_client.get(redis_key)
    
    if not broker_url:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active domain broker found for asset {urn}"
        )
        
    # 3. Proxy to Domain Broker
    try:
        async with httpx.AsyncClient() as client:
            resolve_url = f"{broker_url.rstrip('/')}/api/v1/internal/resolve"
            payload = {"urn": urn}
            response = await client.post(resolve_url, json=payload, timeout=10.0)
            
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="Asset not found on broker")
            elif response.status_code != 200:
                logger.error(f"Broker returned status {response.status_code}: {response.text}")
                raise HTTPException(status_code=502, detail="Bad gateway: Domain broker error")
                
            # Return the BrokerTicketResponse directly to the client
            ticket = response.json()
            if allowed_columns is not None:
                ticket["allowed_columns"] = allowed_columns
            if row_filters is not None:
                ticket["row_filters"] = row_filters
            return ticket
            
    except httpx.RequestError as e:
        logger.error(f"Error communicating with domain broker: {e}")
        raise HTTPException(status_code=502, detail="Bad gateway: Could not reach domain broker")
