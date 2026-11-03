"""VERIFIED IDENTITY for the DA data gateway: who a request reads as, decided from a PROVEN token.

This is the enforcing sibling of `subject_gauge`. The gauge measures how identity arrives and
never refuses; this module decides, and under the `enforce` posture the gateway obeys it.

## The ruling it implements

1. A user token's identity comes from its VERIFIED claim. A header that differs is refused.
2. A verified service token (claim starts `svc:`) may name a user only if the policy rail lists
   that service as a delegate for the route (the `is_delegate` seam). The audit names both.
3. The header carries authz_id, not email, and is called `on_behalf_of` on the wire.
   A blank `on_behalf_of` from a listed delegate means the service acts as itself.

## Separate from the gauge on purpose

The gauge's `verify_bearer` must stay never-raising and its JWKS failure latches for ever.
That is right for an instrument and wrong for a gate: a latched failure would turn one JWKS
blip into a permanent outage (or, worse, a permanent silent downgrade). Here a JWKS failure is
a 503 for THAT request and the next request tries again.

## Reasons are stable log tokens, not prose

`token_absent`, `token_invalid`, `verification_unconfigured`, `jwks_unavailable`, `no_subject`,
`on_behalf_of_differs`, `not_a_delegate`, `legacy_header`.

Every setting is read per call from the environment, never at import.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Two layouts, as in main.py: the deployed image is flat `/app` and a bare relative import
# is a startup CrashLoop; the tests import the package form.
try:
    from . import subject_gauge  # package layout — the test suite
except ImportError:  # pragma: no cover - exercised only by the flattened container layout
    import subject_gauge  # type: ignore[no-redef]  # flat /app layout — the deployed image

logger = logging.getLogger(__name__)
_PKG_LOGGER = logging.getLogger("dag_tools.central_gateway")

#: The route id the delegate table is keyed on for the asset authorize endpoint.
ROUTE_AUTHORIZE = "assets.authorize"

POSTURE_OBSERVE = "observe"
POSTURE_ENFORCE = "enforce"

SERVICE_PREFIX = "svc:"
JWKS_TIMEOUT_SECONDS = 3

# Reason tokens.
TOKEN_ABSENT = "token_absent"
TOKEN_INVALID = "token_invalid"
UNCONFIGURED = "verification_unconfigured"
JWKS_UNAVAILABLE = "jwks_unavailable"
NO_SUBJECT = "no_subject"
DIFFERS = "on_behalf_of_differs"
NOT_A_DELEGATE = "not_a_delegate"
LEGACY_HEADER = "legacy_header"

_STATUS = {
    TOKEN_ABSENT: 401,
    TOKEN_INVALID: 401,
    UNCONFIGURED: 503,
    JWKS_UNAVAILABLE: 503,
    NO_SUBJECT: 403,
    DIFFERS: 403,
    NOT_A_DELEGATE: 403,
    LEGACY_HEADER: 403,
}

_MESSAGES = {
    TOKEN_ABSENT: "no bearer token was presented",
    TOKEN_INVALID: "the bearer token failed verification",
    UNCONFIGURED: "the gateway has no key source or no issuer configured to verify tokens",
    JWKS_UNAVAILABLE: "the signing keys could not be fetched; retry",
    NO_SUBJECT: "the verified token carries no subject claim",
    DIFFERS: "on_behalf_of differs from the identity the token proves",
    NOT_A_DELEGATE: "this service is not a listed delegate for this route",
    LEGACY_HEADER: "a delegate must name the user in on_behalf_of, not the legacy header",
}


def message_for(reason: str) -> str:
    return _MESSAGES.get(reason, reason)


@dataclass(frozen=True)
class IdentityDecision:
    subject: Optional[str]
    via: Optional[str]
    refused: Optional[str]
    status: Optional[int]
    token_sub: Optional[str]


def _refuse(reason: str, token_sub: Optional[str] = None) -> IdentityDecision:
    return IdentityDecision(None, None, reason, _STATUS[reason], token_sub)


# ---------------------------------------------------------------------------
# Settings — read per call
# ---------------------------------------------------------------------------
def parse_posture(env_name: str) -> str:
    """`observe` (default, also when blank) or `enforce`, read from `env_name`.

    Anything else is `enforce`, loudly. Shared by every posture switch in this package so an
    unknown value is handled the same way everywhere.
    """
    raw = (os.getenv(env_name) or "").strip().lower()
    if raw in ("", POSTURE_OBSERVE):
        return POSTURE_OBSERVE
    if raw != POSTURE_ENFORCE:
        logger.error(
            "%s has an unrecognised value (%d chars) — treating it as "
            "'enforce'", env_name, len(raw),
        )
    return POSTURE_ENFORCE


def posture() -> str:
    """`observe` (default, also when blank) or `enforce`. Anything else is `enforce`, loudly."""
    return parse_posture("GATEWAY_IDENTITY_POSTURE")


def entitlement_claim() -> str:
    return os.getenv("USER_ENTITLEMENT_CLAIM", "email")


def _issuer() -> str:
    return (os.getenv("GATEWAY_JWT_ISSUER") or "").strip()


def _static_key() -> Optional[str]:
    return os.getenv("GATEWAY_JWT_PUBLIC_KEY") or os.getenv("KEYCLOAK_PUBLIC_KEY")


def _algorithms() -> List[str]:
    return [a.strip() for a in os.getenv("GATEWAY_JWT_ALGORITHMS", "RS256").split(",") if a.strip()]


_rail_warned = False


def is_delegate(service: str, route: str) -> bool:
    """THE SEAM: does the policy rail list `service` as a delegate for `route`?

    Called per request, with the route, by `decide`. Any exception or unknown answer from a
    seam is False (fail closed: refused as `not_a_delegate`). This default is a placeholder: the
    rail reader is not wired yet, so it answers False for everyone and says so once, at ERROR.
    Tests and the eventual reader replace this attribute (or pass `is_delegate=` to `decide`).
    """
    global _rail_warned
    if not _rail_warned:
        _rail_warned = True
        logger.error(
            "identity: the policy-rail delegate reader is not wired — no service is a delegate; "
            "every service token naming a user is refused as not_a_delegate"
        )
    return False


def _delegate_answer(seam, service: str, route: str) -> bool:
    try:
        return seam(service, route) is True
    except Exception as exc:  # noqa: BLE001
        logger.error("identity: the delegate seam failed (%s) — treated as not a delegate",
                     type(exc).__name__)
        return False


# ---------------------------------------------------------------------------
# Verification — never latches
# ---------------------------------------------------------------------------
_jwks_client = None
_jwks_client_url: Optional[str] = None


def _make_jwks_client(url: str):
    from jwt import PyJWKClient
    return PyJWKClient(url, cache_keys=True, timeout=JWKS_TIMEOUT_SECONDS)


class _JwksUnavailable(Exception):
    pass


def _jwks_signing_key(url: str, token: str):
    """Resolve a signing key. Connection failure raises `_JwksUnavailable`; it is NOT remembered."""
    global _jwks_client, _jwks_client_url
    import jwt
    try:
        if _jwks_client is None or _jwks_client_url != url:
            _jwks_client = _make_jwks_client(url)
            _jwks_client_url = url
        return _jwks_client.get_signing_key_from_jwt(token).key
    except jwt.PyJWKClientConnectionError as exc:
        raise _JwksUnavailable(type(exc).__name__) from exc
    except jwt.PyJWTError:
        # kid not in the set, or an undecodable header: the TOKEN is bad, not the key service.
        raise
    except Exception as exc:  # noqa: BLE001 — a transport fault from a patched or odd client
        raise _JwksUnavailable(type(exc).__name__) from exc


def _verify(token: Optional[str]):
    """Returns `(claims, None)` on success or `(None, reason)`. Never raises."""
    if not token or not token.strip():
        return None, TOKEN_ABSENT
    issuer = _issuer()
    url = (os.getenv("GATEWAY_JWKS_URL") or "").strip()
    static = _static_key()
    if not issuer or not (url or static):
        return None, UNCONFIGURED
    try:
        import jwt
    except ImportError:  # pragma: no cover
        return None, UNCONFIGURED
    try:
        key = _jwks_signing_key(url, token) if url else static
    except _JwksUnavailable as exc:
        logger.warning("identity: JWKS unavailable (%s) — this request is refused, the next retries", exc)
        return None, JWKS_UNAVAILABLE
    except Exception:  # noqa: BLE001
        return None, TOKEN_INVALID
    try:
        claims = jwt.decode(
            token, key,
            algorithms=_algorithms(),
            issuer=issuer,
            options={"verify_aud": False, "require": ["exp"]},
        )
    except Exception:  # noqa: BLE001
        return None, TOKEN_INVALID
    return claims, None


# ---------------------------------------------------------------------------
# The decision
# ---------------------------------------------------------------------------
def _clean(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def decide(
    token: Optional[str],
    on_behalf_of: Optional[str],
    legacy_subject: Optional[str],
    route: str,
    is_delegate_fn=None,
) -> IdentityDecision:
    """Who this request reads as. Pure apart from reading env and (when configured) JWKS."""
    claims, reason = _verify(token)
    if reason:
        return _refuse(reason)

    token_sub = _clean(claims.get("sub")) or None
    claim = _clean(claims.get(entitlement_claim()))
    if not claim:
        return _refuse(NO_SUBJECT, token_sub)

    obo = _clean(on_behalf_of)
    legacy = _clean(legacy_subject)

    if not claim.startswith(SERVICE_PREFIX):
        # A user proves who they are. No header may say otherwise.
        if (obo and obo != claim) or (legacy and legacy != claim):
            return _refuse(DIFFERS, token_sub)
        return IdentityDecision(claim, None, None, None, token_sub)

    seam = is_delegate_fn if is_delegate_fn is not None else is_delegate  # module attr, at call time
    if not _delegate_answer(seam, claim, route):
        return _refuse(NOT_A_DELEGATE, token_sub)
    if obo and legacy and obo != legacy:
        return _refuse(DIFFERS, token_sub)
    if obo:
        return IdentityDecision(obo, claim, None, None, token_sub)
    if legacy:
        return _refuse(LEGACY_HEADER, token_sub)
    return IdentityDecision(claim, None, None, None, token_sub)


# ---------------------------------------------------------------------------
# Evidence
# ---------------------------------------------------------------------------
def _outcome(d: IdentityDecision) -> str:
    return "refuse" if d.refused else "allow"


def audit(route: str, d: IdentityDecision, urn: str) -> None:
    """One `IDENTITY_AUDIT` line per enforced decision. Never carries a token."""
    subject_gauge.ensure_gauge_visible()  # the audit line is the evidence; a dark channel is none
    msg = "IDENTITY_AUDIT route=%s subject=%s via=%s outcome=%s reason=%s urn=%s"
    args = (route, d.subject or "-", d.via or "-", _outcome(d), d.refused or "-", urn)
    _PKG_LOGGER.log(logging.WARNING if d.refused else logging.INFO, msg, *args)


def would(d: IdentityDecision, urn: str) -> None:
    """The `observe` line: what `enforce` would have done. Nothing branches on it."""
    subject_gauge.ensure_gauge_visible()
    _PKG_LOGGER.info(
        "IDENTITY_WOULD outcome=%s reason=%s subject=%s via=%s urn=%s",
        _outcome(d), d.refused or "-", d.subject or "-", d.via or "-", urn,
    )


def announce() -> str:
    """Startup line: posture and whether an issuer is set. No secret values."""
    line = "identity: posture=%s issuer=%s" % (posture(), "set" if _issuer() else "unset")
    _PKG_LOGGER.info(line)
    return line
