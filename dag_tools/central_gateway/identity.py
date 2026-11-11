"""VERIFIED IDENTITY for the DA data gateway: who a request reads as, decided from a PROVEN token.

This is the enforcing sibling of `subject_gauge`. The gauge measures how identity arrives and
never refuses; this module decides, and under the `enforce` posture the gateway obeys it.

## The ruling it implements

1. A user token's identity comes from its VERIFIED claim. A header that differs is refused.
2. A verified service token (claim starts `svc:`) may name a user only if the policy rail lists
   that service as a delegate for the route (the `is_delegate` seam). The audit names both.
3. The header carries authz_id, not email, and is called `on_behalf_of` on the wire.
   A blank `on_behalf_of` from a listed delegate means the service acts as itself.

## What VERIFIED means here

Signature, then the ISSUER against an accepted LIST, then the AUDIENCE against an accepted list.
Both lists are read from the deployment's own values, neither has a default, and an EMPTY list
REFUSES EVERYTHING: a gateway that has not been told which issuers it trusts does not get to
decide that it trusts the one in front of it. A built-in default would be worse than a wrong
one, because it would make the empty state unreachable and so make the rule unobservable.

The issuer is a LIST because one identity provider is reachable under more than one name. A
token minted against the browser-facing URL and presented by a pod inside the cluster carries
the issuer it was minted with, so the in-cluster name is a second accepted ENTRY rather than a
rewrite of the first. No issuer and no audience is written down here; the deployment lists them.

The issuer is checked in this module rather than handed to PyJWT. `jwt.decode(issuer=[...])` is
a newer PyJWT's behaviour -- an older one compares the list to the claim as a string and refuses
every token, a total outage that reads as a key problem. The AUDIENCE is handed to PyJWT, which
has resolved the aud-may-be-a-list-on-both-sides intersection for far longer than this code has
existed, and getting that intersection subtly wrong is how an audience check passes vacuously.

## Separate from the gauge on purpose

The gauge's `verify_bearer` must stay never-raising and its JWKS failure latches for ever.
That is right for an instrument and wrong for a gate: a latched failure would turn one JWKS
blip into a permanent outage (or, worse, a permanent silent downgrade). Here a JWKS failure is
a 503 for THAT request and the next request tries again.

## Reasons are stable log tokens, not prose

`token_absent`, `token_invalid`, `verification_unconfigured`, `issuer_not_accepted`,
`jwks_unavailable`, `no_subject`, `on_behalf_of_differs`, `not_a_delegate`, `legacy_header`.

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
ISSUER_NOT_ACCEPTED = "issuer_not_accepted"
JWKS_UNAVAILABLE = "jwks_unavailable"
NO_SUBJECT = "no_subject"
DIFFERS = "on_behalf_of_differs"
NOT_A_DELEGATE = "not_a_delegate"
LEGACY_HEADER = "legacy_header"

_STATUS = {
    TOKEN_ABSENT: 401,
    TOKEN_INVALID: 401,
    UNCONFIGURED: 503,
    ISSUER_NOT_ACCEPTED: 401,
    JWKS_UNAVAILABLE: 503,
    NO_SUBJECT: 403,
    DIFFERS: 403,
    NOT_A_DELEGATE: 403,
    LEGACY_HEADER: 403,
}

_MESSAGES = {
    TOKEN_ABSENT: "no bearer token was presented",
    TOKEN_INVALID: "the bearer token failed verification",
    UNCONFIGURED: (
        "the gateway has no key source, no accepted issuer or no accepted audience configured "
        "to verify tokens"
    ),
    ISSUER_NOT_ACCEPTED: "the token's issuer is not one this gateway accepts",
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


#: Where each accepted list is read from. The PLURAL name first, the SINGULAR one second: the
#: singular names predate the list and are kept as one ENTRY of it, so a deployment that set
#: them keeps working and there is never a second resolution to disagree with the first.
ISSUERS_ENV = ("GATEWAY_JWT_ISSUERS", "GATEWAY_JWT_ISSUER")
AUDIENCES_ENV = ("GATEWAY_JWT_AUDIENCES", "GATEWAY_JWT_AUDIENCE")

#: The key source, named here so one list answers "what is unset?" for every caller.
KEY_ENV = ("GATEWAY_JWKS_URL", "GATEWAY_JWT_PUBLIC_KEY", "KEYCLOAK_PUBLIC_KEY")


def _accepted(env_names) -> List[str]:
    """The values named by `env_names`, comma-separated, in order, de-duplicated.

    No defaults, ever. An empty result is the refuse-everything state and must be reachable.
    """
    out: List[str] = []
    for name in env_names:
        for part in (os.getenv(name) or "").split(","):
            value = part.strip()
            if value and value not in out:
                out.append(value)
    return out


def accepted_issuers() -> List[str]:
    """The issuers this gateway accepts. EMPTY REFUSES EVERYTHING; there is no default."""
    return _accepted(ISSUERS_ENV)


def accepted_audiences() -> List[str]:
    """The audiences this gateway accepts. EMPTY REFUSES EVERYTHING; there is no default."""
    return _accepted(AUDIENCES_ENV)


def enforce_blockers() -> List[str]:
    """What is unset before `enforce` could admit anything. Empty means it is configurable now.

    Env var NAMES only, never values. This is the question an operator actually asks -- can this
    deployment turn enforce on? -- answered by the pod itself, so the answer cannot be a sentence
    in a document that went stale when someone edited a values file.
    """
    missing: List[str] = []
    if not accepted_issuers():
        missing.append(ISSUERS_ENV[0])
    if not accepted_audiences():
        missing.append(AUDIENCES_ENV[0])
    if not (_jwks_url() or _static_key()):
        missing.append(" or ".join(KEY_ENV))
    return missing


def _jwks_url() -> str:
    return (os.getenv("GATEWAY_JWKS_URL") or "").strip()


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


_unconfigured_said: Optional[tuple] = None


def _say_unconfigured() -> None:
    """Name what is unset, once per distinct state.

    The refusal's own reason token cannot say WHICH of the three is missing, and a 503 whose
    cause is not in the log is a morning spent on the wrong suspect. Latched on the state rather
    than on having spoken once, so a deployment that is fixed and then broken again says so.
    """
    global _unconfigured_said
    missing = tuple(enforce_blockers())
    if missing == _unconfigured_said:
        return
    _unconfigured_said = missing
    logger.error(
        "identity: verification is unconfigured, so every token is refused — unset: %s",
        ", ".join(missing) or "nothing (a key source answered but no token verifies)",
    )


def _verify(token: Optional[str]):
    """Returns `(claims, None)` on success or `(None, reason)`. Never raises.

    Signature, then audience (PyJWT), then issuer against the accepted list (here). An empty
    accepted list is `UNCONFIGURED` and refuses: checked BEFORE the decode so the refusal names
    a cause an operator can act on, rather than arriving as an indistinguishable invalid-token.
    """
    if not token or not token.strip():
        return None, TOKEN_ABSENT
    issuers = accepted_issuers()
    audiences = accepted_audiences()
    url = _jwks_url()
    static = _static_key()
    if not issuers or not audiences or not (url or static):
        _say_unconfigured()
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
            audience=audiences,
            options={"verify_aud": True, "require": ["exp", "iss", "aud"]},
        )
    except Exception:  # noqa: BLE001
        return None, TOKEN_INVALID
    # The issuer is checked HERE, against the list, for the PyJWT-version reason in the module
    # docstring. `iss` is required above, so this is a proven claim by the time it is read.
    iss = _clean(claims.get("iss"))
    if iss not in issuers:
        logger.warning(
            "identity: a signature-valid token names an issuer this gateway does not accept "
            "(%s) — list it in %s to accept it", iss or "-", ISSUERS_ENV[0],
        )
        return None, ISSUER_NOT_ACCEPTED
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
    """Startup line: posture, how many issuers and audiences are accepted, and whether `enforce`
    could admit anything at all. COUNTS and env var NAMES, never values.

    The count is the point. `issuers=0` is a gateway that refuses every token, and that is the
    one state which otherwise looks exactly like a quiet one.
    """
    blockers = enforce_blockers()
    line = "identity: posture=%s issuers=%d audiences=%d enforce_ready=%s%s" % (
        posture(),
        len(accepted_issuers()),
        len(accepted_audiences()),
        "no" if blockers else "yes",
        (" unset=" + ",".join(blockers)) if blockers else "",
    )
    _PKG_LOGGER.info(line)
    return line
