"""The two INTERNAL DOORS of the mesh, guarded by a service token.

* `POST /api/v1/internal/resolve` on the domain broker mints credentials.
* `POST /api/v1/internal/register` on the central gateway decides where a URN's route points.

Neither may be reached by anyone but a named mesh service. A door verifies the bearer token with
`identity._verify` (the same code the read path uses, not a copy), requires a `svc:` claim, and
requires that service to be in the door's own comma-separated list.

## Where this module lives, and why

It sits in `central_gateway/` because that directory is the ONLY one both images can see: the
gateway image is built from this directory alone and lands FLAT in `/app` (`main:app`), and the
broker image carries the whole `dag_tools` package. So it uses the same two-layout import as
`identity.py`: a bare relative import is a startup CrashLoop in the flat image.

## Posture

`INTERNAL_DOOR_POSTURE`: `observe` (default) or `enforce`, parsed exactly as the identity
posture is. Separate from `GATEWAY_IDENTITY_POSTURE`, so the doors and the read path flip
independently. Under `observe` a refusal is logged (`would_refuse`) and the request is served.

## Caller side

`service_headers()` obtains the calling service's own token by a client-credentials grant and
returns the `Authorization` header. It never raises: with nothing configured, or on a failed
fetch, it returns `{}` (observe still serves; enforce refuses at the door).

Every setting is read per call. Nothing here logs a token, a claim set or a credential.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Awaitable, Callable, Dict, Optional

import httpx
from fastapi import HTTPException, Request

# Two layouts, as in main.py: the deployed gateway image is flat `/app`.
try:
    from . import identity  # package layout: the test suite, and the broker image
except ImportError:  # pragma: no cover - exercised only by the flattened container layout
    import identity  # type: ignore[no-redef]  # flat /app layout: the gateway image

logger = logging.getLogger(__name__)
_PKG_LOGGER = logging.getLogger("dag_tools.central_gateway")

POSTURE_ENV = "INTERNAL_DOOR_POSTURE"
POSTURE_OBSERVE = identity.POSTURE_OBSERVE
POSTURE_ENFORCE = identity.POSTURE_ENFORCE

#: Callers' list settings, one per door.
BROKER_RESOLVE_CALLERS = "BROKER_RESOLVE_CALLERS"
GATEWAY_REGISTER_CALLERS = "GATEWAY_REGISTER_CALLERS"

#: Route ids, as they appear in the log line and the refusal body.
ROUTE_RESOLVE = "broker.resolve"
ROUTE_REGISTER = "gateway.register"

# Reason tokens (the verifier's own reasons pass through unchanged).
NO_TOKEN = "no_token"
NOT_A_SERVICE = "not_a_service"
SERVICE_NOT_LISTED = "service_not_listed"

# Caller-side settings.
TOKEN_URL_ENV = "MESH_SERVICE_TOKEN_URL"
CLIENT_ID_ENV = "MESH_SERVICE_CLIENT_ID"
CLIENT_SECRET_ENV = "MESH_SERVICE_CLIENT_SECRET"
_REFRESH_MARGIN_SECONDS = 30.0
_FETCH_TIMEOUT_SECONDS = 10.0


def posture() -> str:
    return identity.parse_posture(POSTURE_ENV)


def _listed(callers_env: str) -> set:
    return {s.strip() for s in (os.getenv(callers_env) or "").split(",") if s.strip()}


def _bearer(request: Request) -> Optional[str]:
    h = request.headers.get("authorization") or ""
    scheme, _, rest = h.partition(" ")
    if scheme.lower() != "bearer" or not rest.strip():
        return None
    return rest.strip()


def _refusal(route: str, reason: str) -> HTTPException:
    status = identity._STATUS.get(reason)
    if status is None:
        status = 401 if reason == NO_TOKEN else 403
    return HTTPException(status_code=status, detail={"error": reason, "route": route})


def door(route: str, callers_env: str) -> Callable[[Request], Awaitable[None]]:
    """A FastAPI dependency guarding one internal route. Add it with `dependencies=[Depends(...)]`."""

    async def _guard(request: Request) -> None:
        mode = posture()
        reason: Optional[str] = None
        service = "-"

        token = _bearer(request)
        if token is None:
            reason = NO_TOKEN
        else:
            claims, why = identity._verify(token)
            if why:
                reason = why
            else:
                claim = identity._clean(claims.get(identity.entitlement_claim()))
                if not claim.startswith(identity.SERVICE_PREFIX):
                    reason = NOT_A_SERVICE  # a user: never named in the log
                else:
                    service = claim
                    if claim not in _listed(callers_env):
                        reason = SERVICE_NOT_LISTED

        if reason is None:
            if mode == POSTURE_ENFORCE:
                _PKG_LOGGER.info("internal_door served route=%s service=%s", route, service)
            return
        if mode == POSTURE_ENFORCE:
            _PKG_LOGGER.warning(
                "internal_door refused route=%s reason=%s service=%s", route, reason, service)
            raise _refusal(route, reason)
        _PKG_LOGGER.warning(
            "internal_door would_refuse route=%s reason=%s service=%s", route, reason, service)

    return _guard


# ---------------------------------------------------------------------------
# Caller side
# ---------------------------------------------------------------------------
_cache: Dict[str, object] = {"token": None, "fetch_at": 0.0}
_unset_warned = False


async def service_headers() -> Dict[str, str]:
    """`{"Authorization": "Bearer ..."}` for a call to an internal door, or `{}`. Never raises."""
    global _unset_warned
    url = (os.getenv(TOKEN_URL_ENV) or "").strip()
    cid = (os.getenv(CLIENT_ID_ENV) or "").strip()
    secret = os.getenv(CLIENT_SECRET_ENV) or ""
    if not (url and cid and secret):
        if not _unset_warned:
            _unset_warned = True
            logger.error(
                "internal_door: %s, %s and %s are not all set — calls to the internal doors "
                "carry no service token (refused under enforce)",
                TOKEN_URL_ENV, CLIENT_ID_ENV, CLIENT_SECRET_ENV,
            )
        return {}

    token = _cache["token"]
    if token and time.monotonic() < float(_cache["fetch_at"]):
        return {"Authorization": f"Bearer {token}"}

    try:
        async with httpx.AsyncClient(timeout=_FETCH_TIMEOUT_SECONDS) as client:
            resp = await client.post(
                url,
                data={"grant_type": "client_credentials", "client_id": cid, "client_secret": secret},
            )
            resp.raise_for_status()
            body = resp.json()
        token = body["access_token"]
        if not isinstance(token, str) or not token:
            raise ValueError("no access_token")
        expires_in = float(body.get("expires_in", 60))
    except Exception as exc:  # noqa: BLE001 — a caller never crashes on this
        _cache["token"] = None
        logger.error("internal_door: service token fetch failed (%s); retrying on the next call",
                     type(exc).__name__)
        return {}
    _cache["token"] = token
    _cache["fetch_at"] = time.monotonic() + max(0.0, expires_in - _REFRESH_MARGIN_SECONDS)
    return {"Authorization": f"Bearer {token}"}
