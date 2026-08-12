"""The SUBJECT-SOURCE GAUGE — how identity ARRIVES at the DA data gateway.

**This module measures. It never refuses.** Nothing here can change the outcome of a request:
no 401, no 403, no header stripped, no authz subject altered. Wiring it in is behaviourally
inert by construction, which is the only reason it is safe to ship onto a live data path.

## What it exists to produce

One number nobody currently has:

    How many live requests ASSERT a subject (via the `X-Originator-Email` header)
    versus PROVE one (via a signature-verified token claim)?

That number decides whether removing the header override is a **config change** or a
**coordinated migration**. There is no way to know which without measuring first, so the gauge is
the deliverable and the eventual verification fix is the easy part.

## Why this is NOT the transport-auth gauge repeated

The mesh's transport migration gauged **whether callers minted** — a yes/no per caller answering
*is a credential present*. This gauges **how identity arrives**, which is strictly richer: a
request can carry a perfectly valid, signature-verified token and *still* name its authorization
subject in a header that overrides the token's own claim. The earlier work has no analogue for
that, so nothing here can be inferred from its shape or its numbers.

## The four buckets, and why the fourth is the real finding

`token-claim`     — subject proved by a verified claim; no header present. The healthy shape.
`header-only`     — the token carries no subject; the header supplied it. Today's DA path: the
                    bearer is an M2M service token with no user email, so the end user's identity
                    can ONLY arrive by header. Expected, and the reason the header exists.
`header-override` — BOTH present and the header won. **This is the bucket that decides the
                    migration**, and it splits again:
                      * ``agreeing``  — header equals the token's claim. Removing the override
                                        changes nothing for this caller: a config change.
                      * ``divergent`` — header names someone the token does not. Removing the
                                        override CHANGES WHO THIS REQUEST READS AS. Every
                                        divergent caller is a migration step, and a divergent
                                        request from an unverified token is the impersonation
                                        shape the packet describes.
`none`            — no subject from either source; the gateway fail-closed denies downstream.

**Counting `header-override` without the agree/diverge split would answer the wrong question.**
A thousand agreeing overrides is a one-line config change; ten divergent ones is a negotiation
with ten callers.

## Verify-if-present

Signatures are validated **when a token carries one and a key is configured**, and the outcome is
recorded. A token that cannot be verified is reported UNVERIFIED with the reason NAMED — never
silently trusted and never refused. `verified=False` is legal to LOG and illegal to authorize on;
nothing in this module authorizes on anything.

## USAGE RULE — two axes, independently valid, and one can be dark while the other is honest

**Read `verification_line()` from the startup log BEFORE interpreting any reading.**

This gauge reports on two axes that must not be collapsed into one "healthy" number:

1. **subject-source** — token-claim / header-only / header-override(+agree|diverge). Always
   readable. Requires no key, no verification, no configuration.
2. **token-verified** — whether the bearer's signature was actually proven.

If startup announced ``verification: NONE CONFIGURED``, then **axis 2 carries no information for
that run**: every request will read ``token_verified=False token_reason=no-verification-key``, and
that says something about the deployment, not about the callers. Axis 1 is still fully honest and
is the axis the migration decision rests on.

**Collapsing them would be the error this design exists to avoid.** "Unverified everywhere because
no key is set" and "unverified everywhere because every caller is forging tokens" are the same
observation for opposite reasons; a single health number cannot distinguish them, and the startup
line is what does. They are reported separately for exactly that reason.

Same posture vocabulary as the fleet's `iagent_mesh.transport_auth`, deliberately, so the two
gauges read the same way. dag-tools does not depend on the SDK, so the pattern is mirrored rather
than imported.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)
_PKG_LOGGER = logging.getLogger("dag_tools.central_gateway")

POSTURE_OBSERVE = "OBSERVE"

#: Subject-source buckets. Values are stable log tokens — grep targets, not prose.
SRC_TOKEN = "token-claim"
SRC_HEADER_ONLY = "header-only"
SRC_HEADER_OVERRIDE = "header-override"
SRC_NONE = "none"

AGREE = "agreeing"
DIVERGE = "divergent"


class CallerIdentity:
    """What a bearer token claims, and whether that claim was PROVEN."""

    __slots__ = ("authz_id", "verified", "reason", "raw")

    def __init__(self, authz_id: Optional[str], verified: bool, reason: str, raw: Any = None):
        self.authz_id = authz_id
        self.verified = verified
        self.reason = reason
        self.raw = raw

    def __repr__(self) -> str:  # pragma: no cover — logging aid
        return f"<CallerIdentity authz_id={self.authz_id!r} verified={self.verified} reason={self.reason!r}>"


def entitlement_claim() -> str:
    """The claim naming the authorization subject. Email in sandbox, employee-id at work.

    Same env var as the rest of the mesh (`USER_ENTITLEMENT_CLAIM`), because a gauge that keyed
    on a different claim than the gate would measure a subject nobody authorizes on.
    """
    return os.getenv("USER_ENTITLEMENT_CLAIM", "email")


def _verification_key() -> Optional[str]:
    return os.getenv("GATEWAY_JWT_PUBLIC_KEY") or os.getenv("KEYCLOAK_PUBLIC_KEY")


_jwks_client = None
_jwks_failed = False


def _jwks_key(token: str):
    """Resolve a signing key from JWKS, if `GATEWAY_JWKS_URL` is configured.

    Cached by PyJWKClient across requests. A JWKS failure is swallowed and latched: the gauge
    degrades to honest-unverified rather than adding a network dependency that can wedge a live
    read path. **Measuring must never be able to break the thing being measured.**
    """
    global _jwks_client, _jwks_failed
    url = os.getenv("GATEWAY_JWKS_URL")
    if not url or _jwks_failed:
        return None
    try:
        if _jwks_client is None:
            from jwt import PyJWKClient
            _jwks_client = PyJWKClient(url, cache_keys=True)
        return _jwks_client.get_signing_key_from_jwt(token).key
    except Exception as exc:  # noqa: BLE001
        _jwks_failed = True
        logger.warning(
            "subject-gauge: JWKS unusable (%s: %s) — gauge degrades to honest-unverified; "
            "verification state will read 'no-verification-key' until this is fixed",
            type(exc).__name__, str(exc)[:120],
        )
        return None


def verify_bearer(token: Optional[str]) -> CallerIdentity:
    """Validate a bearer and extract its subject. NEVER raises, NEVER refuses.

    A decode without signature checking is the presence-check defect wearing a JWT's clothes, so
    when no key is configured the result is reported UNVERIFIED **with the reason named** rather
    than being quietly treated as good.
    """
    if not token:
        return CallerIdentity(None, False, "absent")

    try:
        import jwt  # PyJWT
    except ImportError:  # pragma: no cover — environment without PyJWT
        return CallerIdentity(None, False, "pyjwt-missing")

    key = _jwks_key(token) or _verification_key()
    claim = entitlement_claim()

    if not key:
        # Honest-unverified: we can read who it CLAIMS to be, and we say exactly that.
        try:
            claims = jwt.decode(token, options={"verify_signature": False})
        except Exception as exc:  # noqa: BLE001
            return CallerIdentity(None, False, f"undecodable: {type(exc).__name__}")
        return CallerIdentity(claims.get(claim), False, "no-verification-key", raw=claims)

    try:
        claims = jwt.decode(
            token, key,
            algorithms=os.getenv("GATEWAY_JWT_ALGORITHMS", "RS256").split(","),
            options={"verify_aud": False},
        )
    except Exception as exc:  # noqa: BLE001
        return CallerIdentity(None, False, f"invalid: {type(exc).__name__}")

    return CallerIdentity(claims.get(claim), True, "verified", raw=claims)


def classify(token_subject: Optional[str], header_subject: Optional[str]) -> Dict[str, Optional[str]]:
    """Which source supplied the authorization subject, and did the sources agree?

    Mirrors `central_gateway.check_topaz_authz`'s own precedence — header first, token claim as
    fallback — so the gauge reports the subject the gate WOULD use, not a subject of its own
    devising. If that precedence ever changes, this must change with it or the gauge silently
    starts measuring a different system.
    """
    tok = (token_subject or "").strip() or None
    hdr = (header_subject or "").strip() or None

    if hdr and tok:
        return {
            "source": SRC_HEADER_OVERRIDE,
            "agreement": AGREE if hdr == tok else DIVERGE,
            "effective_subject": hdr,
        }
    if hdr:
        return {"source": SRC_HEADER_ONLY, "agreement": None, "effective_subject": hdr}
    if tok:
        return {"source": SRC_TOKEN, "agreement": None, "effective_subject": tok}
    return {"source": SRC_NONE, "agreement": None, "effective_subject": None}


def _emits_info(lg: logging.Logger) -> bool:
    if not lg.isEnabledFor(logging.INFO):
        return False
    cur: Optional[logging.Logger] = lg
    while cur:
        if cur.handlers:
            return True
        if not cur.propagate:
            return False
        cur = cur.parent
    return False


def ensure_gauge_visible() -> bool:
    """Make this package's INFO records visible IFF they would otherwise vanish.

    **The lesson this function is paid for**, from the mesh's transport gauge: twelve services
    announced `OBSERVE` at startup and then observed nothing, because nothing configured logging
    and the records fell through to `logging.lastResort` (WARNING) and were discarded. A
    migration precondition of "the divergent count reads zero" is satisfied perfectly and falsely
    by a silent gauge. **Zero-because-silent and zero-because-clean are the two states this
    instrument exists to separate.**

    Additive and deferential: does nothing if the records already emit (a second handler would
    double-emit into every properly configured deployment), lowers only this package's level when
    handlers exist upstream, and attaches a handler only when the chain has none — to
    `dag_tools.central_gateway`, never to root. Set `GATEWAY_GAUGE_LOG_AUTOCONFIG=0` to disable,
    which exists so the gauge's own witness can be broken on purpose and shown to go dark.
    """
    if os.getenv("GATEWAY_GAUGE_LOG_AUTOCONFIG", "1").lower() in ("0", "false", "no"):
        return False
    if _emits_info(logger):
        return False

    changed = False
    if _PKG_LOGGER.getEffectiveLevel() > logging.INFO:
        _PKG_LOGGER.setLevel(logging.INFO)
        changed = True
    if _emits_info(logger):
        return changed

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    _PKG_LOGGER.addHandler(handler)
    return True


def posture_line() -> str:
    """`subject-source gauge: OBSERVE (default) [central-gateway]` — posture AND its source.

    The source matters: `OBSERVE (default)` and `OBSERVE (explicit config)` are different claims
    about whether anyone decided.
    """
    src = "explicit config" if os.getenv("GATEWAY_SUBJECT_GAUGE") is not None else "default"
    return f"subject-source gauge: {POSTURE_OBSERVE} ({src}) [central-gateway]"


def verification_line() -> str:
    """Whether signature verification can actually happen — announced, never assumed.

    A gauge reporting `verified=False` on every request because no key is configured looks
    identical to one reporting it because every caller is forging tokens. Announcing which state
    the process is in separates them at startup instead of leaving it to be inferred later.
    """
    if os.getenv("GATEWAY_JWKS_URL"):
        return "subject-gauge verification: JWKS (GATEWAY_JWKS_URL)"
    if _verification_key():
        return "subject-gauge verification: static public key"
    return ("subject-gauge verification: NONE CONFIGURED — every token will read "
            "unverified:no-verification-key; set GATEWAY_JWKS_URL or GATEWAY_JWT_PUBLIC_KEY")


def announce() -> str:
    """Announce at startup, after making sure the gauge's own records can be seen.

    Announcing a posture whose evidence channel is dark is the exact defect this pairing exists
    to prevent — so visibility is established BEFORE the claim is made.
    """
    ensure_gauge_visible()
    line = posture_line()
    logger.info(line)
    logger.info(verification_line())

    # REQUIRE IS NOT IMPLEMENTED, AND SAYS SO. An operator who sets a require-shaped flag and
    # gets silence would reasonably believe the gateway is enforcing. It is not, and a false
    # belief in enforcement is worse than the absent enforcement itself.
    for flag in ("REQUIRE_GATEWAY_AUTH", "REQUIRE_TRANSPORT_AUTH"):
        if os.getenv(flag):
            logger.warning(
                "%s is set but IGNORED by central-gateway: this build ships the OBSERVE gauge "
                "only — no verification is enforced and no header override is removed. "
                "See docs/plans/dag-tools-gateway-unverified-subject.md", flag,
            )
    return line


def observe(*, urn: str, token: Optional[str], header_subject: Optional[str],
            header_sub: Optional[str] = None) -> Dict[str, Any]:
    """Measure ONE request and emit ONE line. Returns the reading, for tests.

    Called for its logging; the return value exists so the classification can be asserted without
    scraping log output. **Nothing in the caller may branch on this** — the moment a request
    outcome depends on the gauge, it stops being a gauge.
    """
    ident = verify_bearer(token)
    reading = classify(ident.authz_id, header_subject)

    logger.info(
        "subject-source: source=%s agreement=%s token_verified=%s token_reason=%s "
        "header_present=%s token_subject_present=%s urn=%s",
        reading["source"],
        reading["agreement"] or "-",
        ident.verified,
        ident.reason,
        bool((header_subject or "").strip()),
        bool(ident.authz_id),
        urn,
    )

    # The one line an operator should be able to grep for without knowing the schema. Only the
    # divergent case earns a WARNING: it is the bucket where removing the override CHANGES WHO
    # THE REQUEST READS AS, and an unverified divergent request is the impersonation shape.
    if reading["agreement"] == DIVERGE:
        logger.warning(
            "SUBJECT-SOURCE DIVERGENT: header names %r, token claims %r, token_verified=%s — "
            "removing the X-Originator-Email override would change this request's subject. "
            "urn=%s", reading["effective_subject"], ident.authz_id, ident.verified, urn,
        )

    return {"identity": ident, **reading}
