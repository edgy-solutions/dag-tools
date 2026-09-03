"""Who a mesh read is authorized as, and what it means when that fails.

THE DEFECT THIS EXISTS TO CLOSE. An agent handler receives a verified
caller, and nothing stops the author from dropping it one line later::

    @app.execute()
    def detect(data: In, caller: CallerIdentity) -> Out:
        client = CortexDataClient()          # reads as the SERVICE
        client = CortexDataClient(caller=caller)  # reads as the user

Both compile. Both return rows. Only one is right, and the wrong one has
no symptom -- every user of the agent reads with the service's
entitlements, and nothing errors.

So identity is resolved in a fixed order, and the order is the ruling:

    caller=  ->  request context  ->  CORTEX_USER_TOKEN  ->  service

Reversed, a value set on a pod would outrank the request's caller: a
config change becomes a silent cross-tenant read with no code to review.
The caller outranking the environment is what keeps that variable
harmless.

INSIDE A REQUEST, FAILING TO RESOLVE RAISES. Being in a handler is
precisely when reading as the service is wrong, so the fall-through stops
there rather than continuing down the rungs.
"""
from __future__ import annotations

import os
from typing import Any, Optional, Protocol, runtime_checkable


USER_TOKEN_ENV = "CORTEX_USER_TOKEN"

#: Minimum SDK version exporting ``current_caller``.
MESH_SDK_FLOOR = "0.4.0"


@runtime_checkable
class Caller(Protocol):
    """What this package needs of a caller -- nothing more.

    A LOCAL PROTOCOL RATHER THAN THE SDK'S TYPE, deliberately. Naming
    ``CallerIdentity`` in a public signature would put a type from an
    OPTIONAL peer into this package's contract, leaving two bad options:
    import it (the hard edge the coupling ruling refused) or annotate it
    under ``TYPE_CHECKING`` (a signature that lies about its own contract
    whenever the peer is absent).

    Declaring the shape instead keeps the boundary honest. dag-tools says
    what it needs, the SDK's ``CallerIdentity`` satisfies it structurally
    with no adapter, and a caller outside the mesh can satisfy the same
    shape without the SDK existing at all.

    ``authz_id`` only. It is the mint-contract subject and the one field
    an authorization decision may key on. The SDK's ``raw`` is for logging
    and must not be read here; its ``sub`` does not exist, so the
    deny-list subject stays an explicit constructor argument.
    """

    @property
    def authz_id(self) -> Optional[str]:
        """The subject to authorize as, or None when unresolved."""
        ...


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


class CortexDataError(Exception):
    """Base for every mesh-read failure this client reports."""


class CallerUnresolved(CortexDataError):
    """No identity to read as.

    A defect in the CALL, not a statement about the data. Raised rather
    than falling back, because the fallback is the confused deputy: rows
    come back, nothing errors, and every user reads with the service's
    entitlements.
    """


class NotEntitled(CortexDataError):
    """The caller resolved and the gate refused.

    An AUTHORIZATION outcome. A composing verb may report this honestly
    as "this caller may not see that" -- it says the data exists and this
    caller cannot have it.
    """


class MeshUnavailable(CortexDataError):
    """The read could not be attempted.

    NOT a statement about entitlement, and NOT a statement that the data
    is absent. Retryable.

    A gateway 404 ("no active domain broker") lands HERE, and that
    classification is a ruling rather than a detail. Routes carry a TTL
    and are re-pushed on a heartbeat, so that 404 is a LIVENESS signal
    about the owning deployment -- not "no such asset". A composer that
    reports "no such data" when the truth is "the owner is down" is
    exactly the dishonest report this taxonomy exists to prevent.
    """


def classify_response(status_code: int, detail: str = "") -> Optional[CortexDataError]:
    """Map a gateway response to an outcome, or None when it succeeded.

    ``raise_for_status()`` used to be the interpreter here, which made a
    TRANSPORT exception stand in for a SEMANTIC outcome: refused, absent
    and unreachable all arrived as one ``HTTPStatusError``, and a verb one
    level up could not tell them apart to report honestly.
    """
    if 200 <= status_code < 300:
        return None
    if status_code in (401, 403):
        return NotEntitled(
            f"the gate refused this caller (HTTP {status_code})"
            + (f": {detail}" if detail else "")
        )
    if status_code == 404:
        # Deliberately NOT "absent" -- see MeshUnavailable.
        return MeshUnavailable(
            f"no active broker holds a route for this asset (HTTP 404)"
            + (f": {detail}" if detail else "")
            + ". Routes expire on a TTL and are re-pushed by heartbeat, so "
            "this reports the owning deployment's liveness, not whether the "
            "asset exists."
        )
    return MeshUnavailable(
        f"the gateway could not serve the request (HTTP {status_code})"
        + (f": {detail}" if detail else "")
    )


# ---------------------------------------------------------------------------
# The rungs
# ---------------------------------------------------------------------------


def _current_caller() -> "tuple[bool, Optional[Caller]]":
    """The SDK's request-scoped caller.

    Returns ``(sdk_present, caller)``. The caller is None both when the
    SDK is absent and when there is no request in scope; ``sdk_present``
    is what lets the constructor say which, because a client running
    outside the mesh should know it has no request context rather than
    silently skipping a rung.
    """
    try:
        from iagent_mesh import current_caller  # noqa: PLC0415
    except Exception:
        return False, None
    try:
        return True, current_caller()
    except Exception:
        return True, None


def resolve_authz_id(
    *,
    caller: Optional[Caller] = None,
    explicit_authz_id: Optional[str] = None,
    allow_service_identity: bool = False,
) -> "tuple[Optional[str], str]":
    """Decide who a read is authorized as. Returns ``(authz_id, rung)``.

    ``authz_id`` is None only when the service identity is the answer --
    the one case where this client legitimately reads as itself.

    Raises :class:`CallerUnresolved` when nothing resolves, and
    specifically when a REQUEST is in scope whose caller did not resolve.
    That second case is the whole point: the SDK distinguishes "no request"
    (None) from "a request by nobody" (a caller with authz_id=None), and
    collapsing them is what lets an agent-pod read fall through to a
    notebook-shaped environment fallback.
    """
    if explicit_authz_id:
        return explicit_authz_id, "explicit"

    if caller is not None:
        authz_id = getattr(caller, "authz_id", None)
        if not authz_id:
            raise CallerUnresolved(
                "a caller was passed but its authz_id is unresolved; refusing "
                "to authorize a read. Reading as the service here would give "
                "every user of this handler the service's entitlements."
            )
        return authz_id, "caller"

    sdk_present, request_caller = _current_caller()
    if request_caller is not None:
        # In a request. This rung is terminal either way -- there is no
        # falling through to an environment variable or to the service.
        authz_id = getattr(request_caller, "authz_id", None)
        if not authz_id:
            reason = getattr(request_caller, "reason", "unresolved")
            raise CallerUnresolved(
                f"inside a mesh request whose caller did not resolve "
                f"({reason}); refusing to authorize a read. This is the "
                f"ordinary case under the OBSERVE posture, not an exotic "
                f"one, and falling back to the service identity here is the "
                f"confused deputy: rows return, nothing errors, and every "
                f"user reads with the service's entitlements."
            )
        return authz_id, "request"

    user_token = os.environ.get(USER_TOKEN_ENV)
    if user_token:
        return user_token, "env"

    if allow_service_identity:
        return None, "service"

    raise CallerUnresolved(
        "no caller identity resolved and the service identity was not opted "
        "into, so there is nobody to authorize this read as. Pass "
        "caller=<the handler's CallerIdentity>, or set "
        f"{USER_TOKEN_ENV}, or construct with service_identity=True to read "
        "as this process deliberately."
        + (
            ""
            if sdk_present
            else (
                f" NOTE: the mesh SDK is not installed, so the request-context "
                f"rung was SKIPPED entirely. Inside an agent handler this "
                f"client cannot see the verified caller -- install iagent_mesh "
                f">= {MESH_SDK_FLOOR}."
            )
        )
    )


def sdk_available() -> bool:
    """Whether the request-context rung can be consulted at all."""
    return _current_caller()[0]
