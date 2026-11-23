import asyncio
import os
import json
import logging
import httpx
from dataclasses import dataclass
from urllib.parse import unquote
from typing import List, Dict, Any, Optional, Tuple
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

# The REFUSAL-REASON gauge: how many requests each refusal reason accounts for, by name, in two
# separate populations. Counts; refuses nothing. Same two-layout rule as above.
try:
    from . import refusal_gauge  # package layout — the test suite
except ImportError:  # pragma: no cover - exercised only by the flattened container layout
    import refusal_gauge  # type: ignore[no-redef]  # flat /app layout — the deployed image

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

# ---------------------------------------------------------------------------
# The listing's cost bounds
# ---------------------------------------------------------------------------
# A listing asks the authorizer ONCE PER CANDIDATE asset, so its cost grows with the mesh while
# a read's does not. Unbounded, one listing is a request storm at the authorizer -- and the
# requests it would starve are the live reads next to it, which is the gate being taken down by
# a convenience built on top of it. Three bounds, each on a different axis: how many candidates
# one listing may decide on, how many of those decisions may be in flight at once, and how long
# the whole listing may take.
#
# These are bounds on THIS gateway's own behaviour, not readings of anything below it, which is
# why they carry defaults at all. A malformed value WARNS and falls back rather than failing the
# process: a typo in a cost bound must not crash-loop a pod whose reads are otherwise fine.
def _positive_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value <= 0:
        logger.warning(
            "%s=%r is not a positive integer -- using the default of %d", name, raw, default
        )
        return default
    return value


def _positive_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        value = 0.0
    if not (value > 0):
        logger.warning(
            "%s=%r is not a positive number -- using the default of %s", name, raw, default
        )
        return default
    return value


ASSET_LIST_MAX_CANDIDATES = _positive_int("ASSET_LIST_MAX_CANDIDATES", 500)
ASSET_LIST_MAX_CONCURRENCY = _positive_int("ASSET_LIST_MAX_CONCURRENCY", 8)
ASSET_LIST_DEADLINE_SECONDS = _positive_float("ASSET_LIST_DEADLINE_SECONDS", 10.0)

#: Why a listing is not the whole answer. Reason TOKENS, not sentences: a caller branches on
#: them. They say something about this gateway's own health and nothing about what exists -- no
#: count of what was withheld travels with them, because the number of assets a caller may not
#: read is itself information about what the mesh holds.
LIST_TRUNCATED = "candidate_limit_reached"
LIST_UNDECIDED = "authorizer_undecided"
LIST_DEADLINE = "deadline_reached"

#: The routing table's key prefix. The listing's candidate set is exactly the keys the brokers'
#: heartbeats write, which is the same table a read is routed through -- so the listing answers
#: "what may you read right now", not "what exists". An asset whose broker has stopped
#: heartbeating is not listed and not readable either, and the two agreeing is the point.
MESH_ROUTE_PREFIX = "mesh_route:"

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
    refusal_gauge.announce()
    identity.announce()
    # The two internal doors, one line each: the door this pod serves and the door it
    # calls. `callers=0` is a door that under `enforce` admits nobody, which is the one
    # state that otherwise looks exactly like a door nobody has called yet.
    internal_door.announce(internal_door.ROUTE_REGISTER,
                           internal_door.GATEWAY_REGISTER_CALLERS)
    internal_door.announce_caller(internal_door.ROUTE_RESOLVE)
    # Ask this process whether it can verify a signed token, ONCE, here. The answer cannot
    # change while the process runs -- the library is baked into the image and the algorithm
    # list arrives as an environment variable, which is populated once, at startup -- so a
    # readiness probe reads the cached verdict rather than minting a key every few seconds.
    # It never raises; a check that broke leaves the gateway ready and says so loudly.
    identity.run_signature_self_check()
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("Connected to Redis.")
    yield
    if redis_client:
        await redis_client.close()
    logger.info("Central Gateway shutting down.")

app = FastAPI(lifespan=lifespan, title="Central Gateway")
security = HTTPBearer()


async def _principal(token: str, on_behalf_of, legacy, route: str, urn: str) -> "identity.IdentityDecision":
    """Resolve who this request reads as, ONCE, and refuse here if that is nobody.

    `route` is passed in rather than defaulted because it is what the audit line names and what
    the delegate table is keyed by. A default would make the next door added here record its
    decisions as the read door's -- a log describing a decision that was not made -- and it
    would do it silently, since the request would still be resolved and still be refused or
    served correctly. There are two doors and each names itself.

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
            token, on_behalf_of, legacy, route, urn,
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

    It also carries the gauges' counts, the way `telemetry_misses` rides on the agent's own
    health body: a number a roll can be read against, rather than a log line someone has to go
    and grep. `observed` is what separates a quiet gateway from a dark gauge — every other count
    being 0 means nothing until `observed` is not.

    `refusal_reasons` carries two populations and not one number: `would_refuse` is what
    `enforce` would have refused with the request served anyway, `did_refuse` is what this
    gateway actually refused. Summing them would report refusals for requests that were served.
    """
    body = {"status": "ok"}
    # An instrument may never break the thing it measures, /health least of all: a readiness
    # probe that 500s because a counter misbehaved takes the pod out.
    try:
        body["principal_record"] = principal_gauge.counts()
    except Exception as exc:  # noqa: BLE001
        logger.warning("principal-record counts unavailable (%s)", type(exc).__name__)
        body["principal_record"] = None
    # Its OWN block, for the same reason the two counts are two numbers: one gauge failing must
    # not take the other's reading off the body, or an operator reads a gap in the wrong place.
    try:
        body["refusal_reasons"] = refusal_gauge.counts()
    except Exception as exc:  # noqa: BLE001
        logger.warning("refusal-reason counts unavailable (%s)", type(exc).__name__)
        body["refusal_reasons"] = None
    # Its own block again, and on the LIVENESS body rather than only the readiness refusal: a
    # pod held out of service answers no probe an operator reads, so the one place the verdict
    # is legible while nothing is being served is here. Liveness is not readiness, so this
    # still answers 200 when the verdict is negative -- restarting the pod fixes nothing, since
    # the next process gets the same image and the same environment.
    try:
        signing = identity.signature_self_check()
        body["signature_support"] = None if signing is None else signing.report()
    except Exception as exc:  # noqa: BLE001
        logger.warning("signature self-check unavailable (%s)", type(exc).__name__)
        body["signature_support"] = None
    return body

@app.get("/ready")
async def readiness_check():
    """Readiness probe: can this process verify a signed token, and is Redis reachable?

    The signature verdict is checked FIRST, and that order is the point. A gateway whose token
    library cannot verify the algorithm it is configured for refuses every token it is handed,
    and Redis is usually fine at the same moment -- so a gateway that reported "Redis
    unreachable" or nothing at all would send the operator to the wrong suspect. It is also the
    one of the two faults that will never fix itself: Redis comes back, an image does not.

    A verdict of None does not gate. Readiness is refused on a NEGATIVE answer, never on the
    absence of one; that the question is always asked is sealed in the lifespan.
    """
    signing = identity.signature_self_check()
    if signing is not None and not signing.ok:
        raise HTTPException(status_code=503, detail=signing.report())
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

@dataclass(frozen=True)
class AuthzOutcome:
    """What the authorizer said about ONE asset, with its SILENCE kept apart from its REFUSAL.

    `check_topaz_authz` answers `False` for both "the authorizer refused" and "the authorizer
    never answered". For a single read that collapse is correct and deliberate: both deny, the
    caller gets the same 403, and ADR-0026's posture is that every non-answer is a hard deny.

    A listing cannot collapse them. Both produce the same empty list, and an empty list says
    "you may read nothing" -- so an authorizer that is down would tell every caller they have no
    access, with nothing in the answer to show the question was never put. That is a wrong answer
    wearing a right one's shape, which is the whole reason this type exists.

    `allowed` is the decision and means nothing when `undecided` is True. `cause` is a token for
    the LOG, never for a response body: it names what a dependency of this gateway did, and the
    caller is told only that the listing is incomplete.
    """

    allowed: bool
    allowed_columns: Optional[List[str]]
    row_filters: Optional[str]
    undecided: bool = False
    cause: Optional[str] = None


async def topaz_decision(token: str, urn: str, principal: "identity.IdentityDecision") -> AuthzOutcome:
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
            return AuthzOutcome(False, None, None, undecided=True, cause="subject_unresolvable")

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

                return AuthzOutcome(bool(is_authorized), allowed_columns, row_filters)
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
                return AuthzOutcome(False, None, None, undecided=True, cause="authorizer_status")

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
        return AuthzOutcome(False, None, None, undecided=True, cause="authorizer_error")


async def check_topaz_authz(
    token: str, urn: str, principal: "identity.IdentityDecision"
) -> Tuple[bool, Optional[List[str]], Optional[str]]:
    """The read path's view of one decision: a refusal and a silence are both a deny.

    Kept on its exact previous signature and return tuple so the read path is unchanged by the
    listing (AGENTS.md section 6 -- other repositories call into this module). The listing reads
    `topaz_decision` directly because it is the one caller for which the difference matters.
    """
    outcome = await topaz_decision(token, urn, principal)
    return outcome.allowed, outcome.allowed_columns, outcome.row_filters


def _denied_sub(
    request: Request, token: str, decision: "identity.IdentityDecision", enforcing: bool
) -> Tuple[str, str, str]:
    """The explicit deny list, read ONCE for whichever door is asking.

    Extracted so the listing cannot read it differently from the read. A deny list applied on
    one door and not the other is the failure worth preventing structurally: a listed user would
    be told they may read an asset and then refused when they tried, and the listing -- the
    thing built to answer "what may I read" -- would be the component that lied.

    Returns `(effective_sub, originator_sub, token_sub)`, where `effective_sub` is non-empty
    only when it is actually on the list. The caller logs and refuses, because the two doors say
    different things in their log lines: one names the urn it was asked about, the other the
    route. Nothing here raises, so the reading cannot decide a request on its own.

    Posture handling is unchanged from the read path it came from. Under enforce the verified
    token's sub is ALWAYS checked, and `X-Originator-Sub` -- an unsigned header -- joins the
    check only when a listed delegate is vouching for a user (`via` is set): there it can only
    ADD a refusal, never remove one. On a user's own token it is ignored, so naming a denied sub
    in it cannot deny or free anybody.
    """
    if not DENIED_USER_SUBS:
        return "", "", ""
    if enforcing:
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
    if effective_sub not in DENIED_USER_SUBS:
        effective_sub = ""
    return effective_sub, originator_sub, token_sub


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
    decision = await _principal(token, on_behalf_of, legacy_header, identity.ROUTE_AUTHORIZE, urn)

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
    effective_sub, originator_sub, token_sub = _denied_sub(request, token, decision, enforcing)
    if effective_sub:
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
            elif response.status_code == 403:
                # A REFUSAL IS FORWARDED AS A REFUSAL, never as a bad gateway.
                # The broker refuses a read it cannot protect (its unminted
                # posture); reported as 502 that arrives at the consumer as
                # `MeshUnavailable` -- "the gateway could not serve the
                # request" -- which tells the reader to retry and the operator
                # to go and look at the broker's health. Both are the wrong
                # place, and the consumer already has the right word for this
                # one: 403 classifies as `NotEntitled`.
                #
                # Only the broker's reason travels, and only from a dict. Its
                # body for this status carries a reason token and nothing else
                # by construction; forwarding whatever a body happened to hold
                # is how a detail written for an operator reaches a reader.
                reason = None
                try:
                    body = response.json()
                    if isinstance(body, dict):
                        reason = body.get("error")
                except Exception:  # noqa: BLE001
                    reason = None
                logger.warning(
                    "broker refused the read (403) reason=%s urn=%s",
                    reason or "-", urn,
                )
                raise HTTPException(
                    status_code=403,
                    detail={"error": reason} if isinstance(reason, str) else "refused by domain broker",
                )
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


@app.get("/api/v1/assets")
async def list_readable_assets(
    request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """The assets this caller may read, asked of the SAME authorizer the read door asks.

    WHY IT IS A LISTING AND NOT A CATALOGUE. A caller that wants to know what it may read has
    only ever had one way to find out: try to read something and see whether it is refused.
    That is a fine answer for one asset and a bad one for a client building a picker, which
    ends up either hard-coding urns or probing the read door asset by asset -- the second being
    a 403 storm that looks exactly like an attack. So this door answers the question directly.

    WHAT IT IS NOT. It mints no ticket, calls no broker and moves no data: the read door is the
    only way to anything behind it, and this one only answers "would that door open". It
    carries no column lists and no row filters either, although the authorizer answers with
    them: those are the shape of a read, and a listing handing them out would publish the
    MASKING RULES for every asset in the mesh to anyone who can authenticate.

    THE CANDIDATE SET IS THE ROUTING TABLE, which makes the answer "what may you read right
    now" rather than "what exists". An asset whose broker has stopped heartbeating has no route
    key, so it is not listed -- and it is not readable either, because the read door 404s on
    exactly the same absence. The two agreeing is the property worth having; a listing drawn
    from the catalogue instead would name assets the read door cannot serve.

    EVERY DECISION IS THE AUTHORIZER'S, ONE PER ASSET. There is no bulk-decision shortcut and
    no cached answer: a listing that reasoned about permissions itself would be a second gate,
    and the two would disagree the first time a policy changed under one of them.

    AN INCOMPLETE ANSWER SAYS SO. `complete` is False with a reason token in
    `incomplete_reasons` whenever some asset went undecided, the candidate cap was reached or
    the deadline was. This is the one thing the route exists to get right: an empty list from a
    down authorizer is indistinguishable from "you may read nothing", and a caller cannot tell
    a true answer from an outage unless it is told. The reasons name this gateway's own health
    and carry NO COUNTS -- with the authorizer down the number of undecided assets is the size
    of the routing table, and that is a fact about the mesh, not about the caller.
    """
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis client not initialized")

    token = credentials.credentials
    legacy_header = (request.headers.get("X-Originator-Email") or "").strip() or None
    on_behalf_of = (request.headers.get("on_behalf_of") or "").strip() or None
    enforcing = identity.posture() == identity.POSTURE_ENFORCE

    # ---- THE ONE RESOLUTION, under THIS route's id -----------------------------------------
    # ROUTE_LIST, not ROUTE_AUTHORIZE: the audit line names the route, and the delegate table is
    # keyed by it. A listing is no asset's read, so the urn these lines carry is the same "-"
    # they already print for an absent subject or via.
    decision = await _principal(token, on_behalf_of, legacy_header, identity.ROUTE_LIST, "-")

    # ---- the explicit deny list, read by the SAME function the read door reads -------------
    # A listed user is refused outright rather than handed an empty list. The empty list would
    # be a true sentence read as a different one -- "you may read nothing" reads as an answer
    # about entitlements, when the fact is that this door is shut to them -- and the read door
    # refuses them too, so saying the same thing here keeps the two doors in agreement.
    effective_sub, originator_sub, token_sub = _denied_sub(request, token, decision, enforcing)
    if effective_sub:
        logger.warning(
            "AUTHZ_DENIED effective_sub=%s (originator=%s, token=%s) route=%s "
            "reason=explicit_deny_list",
            effective_sub, originator_sub or "-", token_sub or "-", identity.ROUTE_LIST,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied: user is on explicit deny list",
        )

    reasons: List[str] = []

    # ---- the candidate set: SCAN, never KEYS -----------------------------------------------
    # KEYS is O(n) and blocks the whole Redis instance for its duration, which would make one
    # listing a stall for every read and every heartbeat sharing that instance. One candidate
    # past the cap is collected and dropped, so the truncation flag is exact rather than merely
    # safe in the right direction.
    candidates: List[str] = []
    try:
        async for key in redis_client.scan_iter(match=MESH_ROUTE_PREFIX + "*", count=200):
            if isinstance(key, (bytes, bytearray)):
                key = key.decode("utf-8", "replace")
            if key.startswith(MESH_ROUTE_PREFIX):
                candidates.append(key[len(MESH_ROUTE_PREFIX):])
            if len(candidates) > ASSET_LIST_MAX_CANDIDATES:
                candidates.pop()
                reasons.append(LIST_TRUNCATED)
                break
    except Exception as exc:  # noqa: BLE001
        # A routing table that cannot be read is not an empty routing table. Refusing is the
        # only honest answer available: an empty list here would tell the caller they may read
        # nothing, which is a statement about their entitlements and not about this outage.
        logger.error("asset listing could not read the routing table (%r)", exc)
        raise HTTPException(status_code=503, detail="Routing table unavailable")

    # ---- one authorizer decision per candidate, bounded three ways -------------------------
    permitted: List[str] = []
    undecided = 0
    semaphore = asyncio.Semaphore(ASSET_LIST_MAX_CONCURRENCY)

    async def _decide(candidate: str) -> AuthzOutcome:
        async with semaphore:
            return await topaz_decision(token, candidate, decision)

    if candidates:
        tasks = {asyncio.create_task(_decide(c)): c for c in candidates}
        done, pending = await asyncio.wait(list(tasks), timeout=ASSET_LIST_DEADLINE_SECONDS)
        for task in pending:
            task.cancel()
        if pending:
            # A cancelled decision is neither permitted nor undecided in the authorizer's
            # sense: it was never asked. It makes the answer incomplete under its own reason.
            reasons.append(LIST_DEADLINE)
            await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            try:
                outcome = task.result()
            except Exception as exc:  # noqa: BLE001
                # `topaz_decision` does not raise, so this is a defect of ours rather than a
                # refusal -- and a defect counts as undecided, never as a denial, because a
                # listing that swallowed its own bugs would report them to the caller as
                # having no access.
                logger.warning("asset listing decision failed (%s)", type(exc).__name__)
                undecided += 1
                continue
            if outcome.undecided:
                undecided += 1
            elif outcome.allowed:
                permitted.append(tasks[task])

    if undecided:
        reasons.append(LIST_UNDECIDED)

    # The counts go in the LOG, where an operator reads them, and not in the response. With the
    # authorizer down, `undecided` equals the size of the routing table: publishing it would
    # tell every authenticated caller how much the mesh holds.
    logger.info(
        "ASSET_LIST subject=%s via=%s candidates=%d permitted=%d undecided=%d complete=%s",
        decision.subject or "-", decision.via or "-",
        len(candidates), len(permitted), undecided, not reasons,
    )

    return {
        "assets": sorted(permitted),
        "complete": not reasons,
        # Sorted and de-duplicated, so the field is a stable set of tokens rather than a trace
        # of the order in which things went wrong.
        "incomplete_reasons": sorted(set(reasons)),
    }
