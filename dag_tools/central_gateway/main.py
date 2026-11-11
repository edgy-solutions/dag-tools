import asyncio
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

# The PRINCIPAL-RECORD gauge: whether the record of a read names who it was authorized for.
# Reads the subject gauge's output; refuses nothing. Same two-layout rule as above.
try:
    from . import principal_gauge  # package layout — the test suite
except ImportError:  # pragma: no cover - exercised only by the flattened container layout
    import principal_gauge  # type: ignore[no-redef]  # flat /app layout — the deployed image

# The internal doors' service-token guard and the caller side that satisfies it. Same
# two-layout rule: this module is flat in the gateway image.
try:
    from . import internal_door  # package layout: the test suite
except ImportError:  # pragma: no cover - exercised only by the flattened container layout
    import internal_door  # type: ignore[no-redef]  # flat /app layout: the deployed image

# The VERIFIED-IDENTITY decision (observe by default; `enforce` is opt-in). Same two-layout rule
# as above: a bare relative import here would crash-loop the flat image.
try:
    from . import identity  # package layout — the test suite
except ImportError:  # pragma: no cover - exercised only by the flattened container layout
    import identity  # type: ignore[no-redef]  # flat /app layout — the deployed image

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
    principal_gauge.announce()
    identity.announce()
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("Connected to Redis.")
    yield
    if redis_client:
        await redis_client.close()
    logger.info("Central Gateway shutting down.")

app = FastAPI(lifespan=lifespan, title="Central Gateway")
security = HTTPBearer()


async def _principal(token: str, on_behalf_of, legacy, urn: str) -> "identity.IdentityDecision":
    """Resolve who this request reads as, ONCE, and refuse here if that is nobody.

    The gate and the record both read what this returns, so there is one resolution and nothing
    for a second one to disagree with. Called before any Topaz call and long before the broker:
    a refusal raises here, so no ticket and no role is minted for a request that names nobody.

    Both postures go through it. Under `enforce` the answer is the verified token's; under
    `observe` a header names the principal, else a VERIFIED claim does, else this refuses. An
    exception fails CLOSED in both: a gateway that cannot work out who is asking does not get to
    authorize a read for them.
    """
    try:
        decision = await asyncio.to_thread(
            identity.resolve_principal,
            token, on_behalf_of, legacy, identity.ROUTE_AUTHORIZE, urn,
        )
    except Exception as exc:  # noqa: BLE001 — fail CLOSED
        logger.error("identity resolution failed (%s) — refusing", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail={"error": "verification_error", "message": "identity verification failed"},
        )
    if decision.refused:
        raise HTTPException(
            status_code=decision.status,
            detail={"error": decision.refused, "message": identity.message_for(decision.refused)},
        )
    return decision

@app.get("/health")
async def health_check():
    """Liveness probe: returns 200 as long as the process is running.

    It also carries the principal-record gauge's counts, the way `telemetry_misses` rides on the
    agent's own health body: a number a roll can be read against, rather than a log line someone
    has to go and grep. `observed` is what separates a quiet gateway from a dark gauge — every
    other count being 0 means nothing until `observed` is not.
    """
    body = {"status": "ok"}
    # An instrument may never break the thing it measures, /health least of all: a readiness
    # probe that 500s because a counter misbehaved takes the pod out.
    try:
        body["principal_record"] = principal_gauge.counts()
    except Exception as exc:  # noqa: BLE001
        logger.warning("principal-record counts unavailable (%s)", type(exc).__name__)
        body["principal_record"] = None
    return body

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

@app.post(
    "/api/v1/internal/register",
    dependencies=[Depends(internal_door.door(internal_door.ROUTE_REGISTER,
                                             internal_door.GATEWAY_REGISTER_CALLERS))],
)
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

async def check_topaz_authz(token: str, urn: str, principal: "identity.IdentityDecision") -> tuple[bool, Optional[List[str]], Optional[str]]:
    """
    Calls the Topaz REST API to check authorization.
    Subject = the resolved principal (`identity.resolve_principal`)
    Resource = {urn}
    Permission = can_read

    The subject is exactly `principal.subject` and NOTHING is read from the token here: no
    `jwt.decode`, no fallback to a claim. This gate used to resolve the subject itself, falling
    back to an unverified token claim when no header was present, while the record kept only the
    header -- two resolutions, and a read authorized for somebody and recorded against nobody
    wherever they differed. There is now one, and it is this argument.

    `principal.subject` is never empty: a request that named nobody was refused before this is
    called. The check below stays anyway, because a caller reaching this with nobody named is a
    wiring mistake that must deny rather than send `None` to an authorizer.
    """
    try:
        # Decode JWT. The SUBJECT of the DA-read authz decision is the
        # caller's ENTITLEMENT KEY (email), NOT the sub — the seeded
        # dataset `owner`/`reader` relations are email-keyed (DataHub owners
        # are emails), and everything else in this system keys on email
        # (auth.USER_ENTITLEMENT_CLAIM). Sending the sub here is why the gate
        # was dormant-unverified: it matched no owner and denied everyone,
        # which looks safe and hid the misalignment. Sub is kept for logs.
        user_id = principal.token_sub
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
        subject_key = principal.subject
        if not subject_key:
            logger.error(
                "DA-read subject unresolvable — the resolved principal names nobody; "
                "fail-CLOSED deny (token_sub=%s)", user_id or "-",
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
    #
    # `on_behalf_of` is the same thing under its ruled wire name (the word the ingest door
    # uses); it is honoured exactly where X-Originator-Email is. If both are present the new
    # name wins; `enforce` additionally refuses the two when they differ.
    legacy_header = (request.headers.get("X-Originator-Email") or "").strip() or None
    on_behalf_of = (request.headers.get("on_behalf_of") or "").strip() or None
    originator_email = on_behalf_of or legacy_header
    enforcing = identity.posture() == identity.POSTURE_ENFORCE

    # ---- THE ONE RESOLUTION of who this request reads as -----------------------------------
    # The gate (1.) and the record (3.) both read this and nothing else, so there is nothing
    # for a second resolution to disagree with. It also emits the posture's evidence line, and
    # it REFUSES a request that names nobody -- here, before Topaz and before the broker.
    decision = await _principal(token, on_behalf_of, legacy_header, urn)

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
    #
    # The reading is KEPT rather than discarded, for the principal-record gauge below. It is
    # kept as None on failure, so a dark subject gauge reads as unclassifiable there rather
    # than as clean — the two states an instrument exists to separate.
    source_reading = None
    try:
        source_reading = subject_gauge.observe(
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
        if enforcing:
            # Under enforce the verified token's sub is ALWAYS checked. X-Originator-Sub is an
            # unsigned header, so it is added to the check ONLY when a listed delegate is
            # vouching for a user (via is set): there it can only ADD a refusal, never remove
            # one. On a user's own token it is ignored, so naming a denied sub cannot deny or
            # free anyone.
            token_sub = decision.token_sub or ""
            originator_sub = (request.headers.get("X-Originator-Sub") or "").strip() if decision.via else ""
        else:
            originator_sub = (request.headers.get("X-Originator-Sub") or "").strip()
            try:
                claims = jwt.decode(token, options={"verify_signature": False})
                token_sub = claims.get("sub") or ""
            except Exception:
                token_sub = ""
        effective_sub = originator_sub or token_sub
        if enforcing:
            effective_sub = next((x for x in (token_sub, originator_sub) if x and x in DENIED_USER_SUBS), "")
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
    is_authorized, allowed_columns, row_filters = await check_topaz_authz(token, urn, decision)
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
            # THE AUTHORIZATION DECISION TRAVELS WITH THE RESOLVE, and this is
            # what makes server-side enforcement possible at all.
            #
            # These used to be grafted onto the ticket AFTER the broker
            # answered, so the broker minted a credential without ever knowing
            # what the caller was entitled to. Row and column narrowing could
            # then only ever be applied by a cooperating client — advisory
            # against anyone holding the credential.
            #
            # Sent, a `mint-role` backend encodes them into the role's GRANTs
            # and ROW POLICY and the DATABASE enforces them. An older broker
            # ignores the extra fields (pydantic), so this is safe to deploy
            # ahead of one.
            # ONE resolution, read everywhere: the subject the GATE decided on is the
            # subject the broker authorizes for and the subject the issue log names. Deriving
            # any of the three separately is how an audit trail quietly starts describing
            # someone other than the decision did.
            subject = decision.subject

            # ---- PRINCIPAL-RECORD GAUGE — observe only, refuse nothing ---------------------
            # The divergence this was built to measure is CLOSED: the gate and the record
            # read one decision, so `record=divergent` is now a structural impossibility
            # rather than a daily occurrence. The gauge stays, and its counts go on /health,
            # because that is the only evidence that it is still true -- a check removed once
            # it passes measures nothing afterwards. Sited here, after the gate allowed, so
            # every reading is about a ticket that was actually issued.
            #
            # Same rule as the gauge above: nothing below may branch on this, and a defect in
            # it degrades to a warning while the request proceeds exactly as it does today.
            try:
                principal_gauge.observe(
                    urn=urn,
                    recorded=subject,
                    source_reading=source_reading,
                    enforcing=enforcing,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("principal-record gauge failed (%s) — request unaffected", type(exc).__name__)

            payload = {
                "urn": urn,
                "allowed_columns": allowed_columns,
                "row_filters": row_filters,
                "subject": subject,
            }
            # The broker's resolve door wants this service's own token (no-op `{}` until the
            # MESH_SERVICE_* settings are present).
            response = await client.post(
                resolve_url, json=payload, headers=await internal_door.service_headers(),
                timeout=10.0)
            
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="Asset not found on broker")
            elif response.status_code != 200:
                logger.error(f"Broker returned status {response.status_code}: {response.text}")
                raise HTTPException(status_code=502, detail="Bad gateway: Domain broker error")
                
            # Return the BrokerTicketResponse directly to the client
            ticket = response.json()

            # TICKET_ISSUED — the line that ties a principal to the frame they
            # are about to hold. Until this existed, the decision was logged
            # here and the minting was logged on the broker, with nothing
            # joining the two to a consumer's actual read: "which read was
            # this?" had no answer, and that answer is where an audit and a
            # complaint both start.
            #
            # The ticket's identity is the BROKER's to mint, so a broker that
            # has not been upgraded stamps none and this logs "-" rather than
            # failing a read over a missing audit field. NO CREDENTIAL IS NAMED
            # HERE, and none may be added: this line goes wherever logs go.
            logger.info(
                "TICKET_ISSUED ticket_id=%s subject=%s urn=%s as_of=%s as_of_source=%s",
                ticket.get("ticket_id") or "-",
                subject or "-",
                urn,
                ticket.get("as_of") or "-",
                ticket.get("as_of_source") or "-",
            )
            if allowed_columns is not None:
                ticket["allowed_columns"] = allowed_columns
            if row_filters is not None:
                ticket["row_filters"] = row_filters
            return ticket
            
    except httpx.RequestError as e:
        logger.error(f"Error communicating with domain broker: {e}")
        raise HTTPException(status_code=502, detail="Bad gateway: Could not reach domain broker")
