import os
import json
import logging
import httpx
from urllib.parse import unquote
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import redis.asyncio as redis
import jwt  # For basic decoding of the Keycloak JWT

# The subject-source gauge. MEASURES ONLY — nothing it returns may change a request's outcome.
# See dag_tools/central_gateway/subject_gauge.py and
# invincible-agent docs/plans/dag-tools-gateway-unverified-subject.md
#
# TWO LAYOUTS, and the deployed one is NOT the one the tests use. The image builds with
# `context: ./dag_tools/central_gateway` and `COPY . .`, so main.py and subject_gauge.py land
# FLAT in /app and hypercorn imports `main:app` as a TOP-LEVEL module — __package__ is empty
# and a relative import is a startup CrashLoop, not a test failure. The tests import
# `dag_tools.central_gateway.subject_gauge` (a PEP-420 namespace package), where the relative
# form is the correct one. Both are real; support both. The build workflow's note that
# central_gateway "has no cross-package imports" is what made the flat context safe — this is
# the first INTRA-directory import, and it is the case that note did not cover.
try:
    from . import subject_gauge  # package layout — the test suite
except ImportError:  # pragma: no cover - exercised only by the flattened container layout
    import subject_gauge  # type: ignore[no-redef]  # flat /app layout — the deployed image

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
# Read TOPAZ_AUTHORIZER_URL (the convention used across the mesh — auth.py,
# datahub_wrapper) FIRST; fall back to the legacy TOPAZ_URL name; default to
# the in-cluster authorizer over HTTP. The prior default `https://localhost:8383`
# was doubly wrong (localhost = this pod, not the topaz service; https vs the
# HTTP authorizer) AND the code read TOPAZ_URL while helm set
# TOPAZ_AUTHORIZER_URL — a name mismatch that made the gate UNREACHABLE, so it
# fail-closed DENIED every read. Deny-all looked safe and hid the misconfig
# (broken-closed-hides-brokenness). Caught by the composed-path DA-read seal.
TOPAZ_URL = os.getenv("TOPAZ_AUTHORIZER_URL") or os.getenv("TOPAZ_URL") or "http://topaz-svc:8383"
TOPAZ_AUTHORIZER_API_KEY = os.getenv("TOPAZ_AUTHORIZER_API_KEY", "")

# Comma-separated list of Keycloak 'sub' values to refuse access for.
# Lighter-weight than a Topaz directory relation, useful for sandbox
# tests of the deny path before a full Rego/directory setup lands.
DENIED_USER_SUBS = {
    s.strip()
    for s in os.getenv("DENIED_USER_SUBS", "").split(",")
    if s.strip()
}

# We'll initialize redis client in lifespan
redis_client: Optional[redis.Redis] = None

class RegisterPayload(BaseModel):
    broker_url: str
    asset_urns: List[str]

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    # Announce the subject-source gauge FIRST, before anything can fail. The announcement
    # establishes its own log visibility before making the claim — announcing a posture whose
    # evidence channel is dark is the defect the pairing exists to prevent.
    subject_gauge.announce()
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("Connected to Redis.")
    yield
    if redis_client:
        await redis_client.close()
    logger.info("Central Gateway shutting down.")

app = FastAPI(lifespan=lifespan, title="Central Gateway")
security = HTTPBearer()

@app.get("/health")
async def health_check():
    """Liveness probe: returns 200 as long as the process is running."""
    return {"status": "ok"}

@app.get("/ready")
async def readiness_check():
    """Readiness probe: checks if critical dependencies (Redis) are reachable."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis client not initialized")
    try:
        await redis_client.ping()
        return {"status": "ready"}
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Redis unreachable")

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

async def check_topaz_authz(token: str, urn: str, originator_email: Optional[str] = None) -> tuple[bool, Optional[List[str]], Optional[str]]:
    """
    Calls the Topaz REST API to check authorization.
    Subject = Keycloak user_id (extracted from JWT)
    Resource = {urn}
    Permission = can_read
    """
    try:
        # Decode JWT. The SUBJECT of the DA-read authz decision is the
        # caller's ENTITLEMENT KEY (email), NOT the sub — the seeded
        # dataset `owner`/`reader` relations are email-keyed (DataHub owners
        # are emails), and everything else in this system keys on email
        # (auth.USER_ENTITLEMENT_CLAIM). Sending the sub here is why the gate
        # was dormant-unverified: it matched no owner and denied everyone,
        # which looks safe and hid the misalignment. Sub is kept for logs.
        unverified_claims = jwt.decode(token, options={"verify_signature": False})
        user_id = unverified_claims.get("sub")
        # The authz SUBJECT is the ORIGINATING USER, threaded via
        # X-Originator-Email (the caller's authz_id / entitlement key —
        # email in sandbox, employee-id at work-deploy). The `token` here
        # is a SERVICE-ACCOUNT M2M JWT (DA's transport identity, no user
        # email), so reading email off the token denied everyone
        # (broken-closed: allow-path never functioned). Prefer the
        # originator email; fall back to the token email only for legacy
        # user-JWT callers. Consistent with the content gates, which key
        # on authz_id — so this gate flips with them at work-deploy.
        # THE CLAIM IS CONFIGURABLE, AND THE GATE MUST READ THE SAME ONE THE
        # GAUGE DOES. This was hardcoded `"email"` while subject_gauge read
        # USER_ENTITLEMENT_CLAIM — dormant only because both happened to say
        # "email". Point the env var at `preferred_username` (which work-deploy
        # does) and the two diverge: the gauge resolves a subject and reports a
        # healthy request while the gate looks for a claim the token does not
        # carry and fail-closed denies every read. A gauge reading green
        # through a total outage is worse than no gauge.
        #
        # Sharing the helper makes the agreement STRUCTURAL rather than a
        # comment asking two call sites to stay in step. subject_gauge's own
        # docstring already promised to mirror this precedence; now it cannot
        # drift from it.
        claim = subject_gauge.entitlement_claim()
        subject_key = (originator_email or "").strip() or unverified_claims.get(claim)
        if not subject_key:
            logger.error(
                "DA-read subject unresolvable — no X-Originator-Email and no "
                "token %r claim; fail-CLOSED deny (token_sub=%s)", claim, user_id,
            )
            return False, None, None

        # Topaz API call (using standard Aserto/Topaz REST API format for Is)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        if TOPAZ_AUTHORIZER_API_KEY:
            headers["Aserto-Tenant-Id"] = TOPAZ_AUTHORIZER_API_KEY

        # Standard Topaz check API format (v2/authz/is).
        # data_broker.rego reads the subject from resource_context.user_id
        # (input.user.id is EMPTY on this Topaz — no identity->user resolution
        # objects seeded), and the object from resource_context.asset_key.
        # identity_context is still required by the authorizer's request
        # validation even though the decision reads resource_context.
        authz_payload = {
            "identity_context": {
                "identity": subject_key,
                "type": "IDENTITY_TYPE_MANUAL"
            },
            "policy_context": {
                "path": "data_mesh.GET.api.v1.assets.__asset_key.authorize",
                "decisions": ["allowed"]
            },
            "resource_context": {
                "asset_key": urn,
                "user_id": subject_key,
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
                # ADR-0026 posture: authz is a GATE, not a trailing step.
                # Every non-200 from topaz is a hard DENY with a loud
                # log — never a silent allow. The prior
                # `ALLOW_MOCK_AUTH=true → return (True, None, None)`
                # branch converted "authz service broken" into
                # "allow everything" (fail-open), which meant sandbox
                # had been mock-allowing every data request since the
                # topaz service was first misconfigured. That branch
                # is removed with topaz-config wiring landing in the
                # same PR per `[[coupled-interim-mechanisms-retire-together]]`.
                logger.error(
                    "TOPAZ AUTHZ DENIED: non-200 response from topaz. "
                    "url=%s status=%s user=%s urn=%s body=%r",
                    f"{TOPAZ_URL}/api/v2/authz/is",
                    response.status_code,
                    user_id,
                    urn,
                    response.text[:500],
                )
                return False, None, None

    except Exception as e:
        # Same posture: exception (topaz unreachable, timeout, DNS,
        # TLS, etc.) is a hard DENY with a loud log naming the cause.
        # `ALLOW_MOCK_AUTH=true` used to fall through here to
        # (True, None, None) — that's the fail-open the ADR-0026
        # amendment explicitly killed. Any operator debugging a
        # sudden 403 storm should search the log for
        # "TOPAZ AUTHZ DENIED" — the message names the failure.
        logger.error(
            "TOPAZ AUTHZ DENIED: exception talking to topaz. "
            "url=%s user=%s urn=%s error=%r",
            f"{TOPAZ_URL}/api/v2/authz/is",
            locals().get("user_id"),
            urn,
            e,
        )
        return False, None, None

@app.post("/api/v1/assets/{urn:path}/authorize")
async def authorize_asset(urn: str, request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verifies user JWT against Topaz, looks up asset in Redis, proxies to Domain Broker.
    """
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis client not initialized")

    # FastAPI's urn:path matcher does not decode percent-encoded characters.
    # httpx URL-encodes RFC-3986 sub-delims like `(` and `)` in path components,
    # so a URN of `urn:li:dataset:(urn:li:dataPlatform:postgres,foo,PROD)` arrives
    # as `urn:li:dataset:%28...%29`. The broker registers Redis keys with the raw
    # form, so the lookup misses. Decode here so the registered form matches.
    urn = unquote(urn)

    token = credentials.credentials

    # The originating user's authz_id (email in sandbox), threaded by the
    # data client as X-Originator-Email. `token` is a service-account M2M
    # JWT (DA's transport identity); the authz SUBJECT is this end user.
    originator_email = (request.headers.get("X-Originator-Email") or "").strip() or None

    # ---- SUBJECT-SOURCE GAUGE — observe only, refuse nothing -------------------------------
    # Sited HERE, on the same two inputs the gate below actually uses, so it reports the subject
    # the gate WOULD choose rather than one of its own devising.
    #
    # The blanket except is deliberate and is the rule for any instrument on a live path:
    # MEASURING MUST NEVER BE ABLE TO BREAK THE THING BEING MEASURED. A defect in the gauge
    # degrades to a warning and the request proceeds exactly as it does today.
    #
    # Nothing below may branch on the reading. The moment a request outcome depends on it, it
    # stops being a gauge and becomes an unreviewed enforcement path.
    try:
        subject_gauge.observe(
            urn=urn,
            token=token,
            header_subject=originator_email,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("subject-source gauge failed (%s) — request unaffected", type(exc).__name__)

    # 0. Explicit deny list (sandbox, predates the full Topaz/Rego setup).
    # Checked before Topaz so a known-denied user cannot slip through if
    # Topaz is mock-allowing for the happy path. Originator-Sub header
    # wins over the JWT sub so a service-account M2M token still gets
    # filtered by the END USER's identity.
    if DENIED_USER_SUBS:
        originator_sub = (request.headers.get("X-Originator-Sub") or "").strip()
        try:
            claims = jwt.decode(token, options={"verify_signature": False})
            token_sub = claims.get("sub") or ""
        except Exception:
            token_sub = ""
        effective_sub = originator_sub or token_sub
        if effective_sub and effective_sub in DENIED_USER_SUBS:
            logger.warning(
                "AUTHZ_DENIED effective_sub=%s (originator=%s, token=%s) urn=%s reason=explicit_deny_list",
                effective_sub, originator_sub or "-", token_sub or "-", urn,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied: user is on explicit deny list",
            )

    # 1. Topaz AuthZ
    is_authorized, allowed_columns, row_filters = await check_topaz_authz(token, urn, originator_email)
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
