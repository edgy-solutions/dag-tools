"""CortexDataClient consumes the caller the SDK hands a handler.

THE DEFECT. An agent handler receives a verified caller and nothing stops
the author dropping it one line later::

    client = CortexDataClient()               # reads as the SERVICE
    client = CortexDataClient(caller=caller)  # reads as the user

Both compile, both return rows, and the wrong one has no symptom.

THE ORDER IS THE RULING::

    caller= -> request context -> CORTEX_USER_TOKEN -> service (opt-in)

Reversed, a variable set on a pod would outrank the request's caller: a
config change becomes a silent cross-tenant read with no code to review.

Inside a request, failing to resolve raises. The SDK distinguishes "no
request" (current_caller() is None) from "a request by nobody"
(CallerIdentity with authz_id=None); collapsing those two is what lets an
agent-pod read fall through to a notebook-shaped environment fallback, so
the tests below pin them apart.
"""
import pytest

from dag_tools.cortex_data.identity import (
    USER_TOKEN_ENV,
    CallerUnresolved,
    MeshUnavailable,
    NotEntitled,
    classify_response,
    resolve_authz_id,
)


class _Caller:
    """Satisfies the local protocol structurally, as the SDK's type does."""

    def __init__(self, authz_id, reason="ok"):
        self.authz_id = authz_id
        self.reason = reason


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(USER_TOKEN_ENV, raising=False)


@pytest.fixture
def in_request(monkeypatch):
    """Put a caller in request scope, as the SDK's dependency does."""
    def _set(caller):
        import dag_tools.cortex_data.identity as ident

        monkeypatch.setattr(ident, "_current_caller", lambda: (True, caller))
    return _set


@pytest.fixture
def no_request(monkeypatch):
    """SDK present, no request in scope."""
    import dag_tools.cortex_data.identity as ident

    monkeypatch.setattr(ident, "_current_caller", lambda: (True, None))


@pytest.fixture
def no_sdk(monkeypatch):
    import dag_tools.cortex_data.identity as ident

    monkeypatch.setattr(ident, "_current_caller", lambda: (False, None))


# ---------------------------------------------------------------------------
# Rung 1 -- caller= wins over everything
# ---------------------------------------------------------------------------


def test_an_explicit_caller_is_used(no_request):
    assert resolve_authz_id(caller=_Caller("user@example.com")) == (
        "user@example.com", "caller",
    )


def test_an_explicit_caller_beats_a_request_caller(in_request):
    in_request(_Caller("request-user@example.com"))
    who, rung = resolve_authz_id(caller=_Caller("explicit@example.com"))
    assert (who, rung) == ("explicit@example.com", "caller")


def test_a_passed_caller_that_is_unresolved_raises(no_request):
    """Handing over an unresolved caller is not the same as handing over
    nothing -- it must not degrade to a service read."""
    with pytest.raises(CallerUnresolved, match="authz_id is unresolved"):
        resolve_authz_id(caller=_Caller(None))


# ---------------------------------------------------------------------------
# Rung 2 -- the request context, and the distinction that makes rung 4 work
# ---------------------------------------------------------------------------


def test_a_bare_construction_inside_a_request_uses_the_request_caller(in_request):
    """The change that makes a bare CortexDataClient() CORRECT inside a
    handler rather than a silent service read."""
    in_request(_Caller("handler-user@example.com"))
    assert resolve_authz_id() == ("handler-user@example.com", "request")


def test_inside_a_request_an_unresolved_caller_raises(in_request):
    """The SDK's own warning, pinned: a request whose caller did not
    resolve must NEVER fall through. This is the ordinary case under the
    OBSERVE posture, not an exotic one."""
    in_request(_Caller(None, reason="absent"))
    with pytest.raises(CallerUnresolved, match="did not resolve"):
        resolve_authz_id(allow_service_identity=True)


def test_the_request_rung_is_terminal_even_with_the_env_var_set(
    in_request, monkeypatch,
):
    """No falling through to a lower rung from inside a request -- not to
    the environment, not to the service."""
    monkeypatch.setenv(USER_TOKEN_ENV, "env-user@example.com")
    in_request(_Caller(None, reason="absent"))
    with pytest.raises(CallerUnresolved):
        resolve_authz_id(allow_service_identity=True)


def test_no_request_is_not_the_same_as_a_request_by_nobody(
    no_request, monkeypatch,
):
    """The other half of the same distinction: OUTSIDE a request the lower
    rungs are legitimate. Collapsing the two states is what would let an
    agent-pod read fall through to a notebook-shaped fallback."""
    monkeypatch.setenv(USER_TOKEN_ENV, "notebook-user@example.com")
    assert resolve_authz_id() == ("notebook-user@example.com", "env")


# ---------------------------------------------------------------------------
# Rung 3 -- THE BITE-CHECK
# ---------------------------------------------------------------------------


def test_the_request_caller_outranks_the_environment(in_request, monkeypatch):
    """THE BITE-CHECK, and the reason acceptance 3 needed replacing.

    "CORTEX_USER_TOKEN on an agent pod changes nothing" used to hold only
    because NOTHING READ that variable -- a green certifying an absence,
    not a precedence. It would have flipped red the moment this rung
    landed, reading as a regression in the very work that implements the
    property it was supposed to guard.

    This sets the variable AND provides a caller, then asserts the caller
    won. Verified to fail with the rungs reversed.
    """
    monkeypatch.setenv(USER_TOKEN_ENV, "pod-wide-token@example.com")
    in_request(_Caller("actual-user@example.com"))

    who, rung = resolve_authz_id()

    assert who == "actual-user@example.com", (
        "the pod-wide environment variable outranked the request's caller: "
        "a config change is now a silent cross-tenant read"
    )
    assert rung == "request"


def test_an_explicit_caller_also_outranks_the_environment(
    no_request, monkeypatch,
):
    monkeypatch.setenv(USER_TOKEN_ENV, "pod-wide-token@example.com")
    who, _ = resolve_authz_id(caller=_Caller("explicit@example.com"))
    assert who == "explicit@example.com"


# ---------------------------------------------------------------------------
# Rung 4 -- opt-in and loud
# ---------------------------------------------------------------------------


def test_the_service_identity_requires_opting_in(no_request):
    with pytest.raises(CallerUnresolved, match="service_identity=True"):
        resolve_authz_id()


def test_opting_in_reads_as_the_service(no_request):
    assert resolve_authz_id(allow_service_identity=True) == (None, "service")


def test_the_refusal_names_every_way_out(no_request):
    """An operator hitting this needs to know which of three fixes applies."""
    with pytest.raises(CallerUnresolved) as exc:
        resolve_authz_id()
    message = str(exc.value)
    assert "caller=" in message
    assert USER_TOKEN_ENV in message
    assert "service_identity=True" in message


# ---------------------------------------------------------------------------
# The soft import, and its loudness
# ---------------------------------------------------------------------------


def test_without_the_sdk_the_refusal_says_the_rung_was_skipped(no_sdk):
    """The condition on the coupling ruling. A client running outside the
    mesh must KNOW it has no request context, rather than silently
    skipping the rung and reading as something else."""
    with pytest.raises(CallerUnresolved) as exc:
        resolve_authz_id()
    message = str(exc.value)
    assert "SKIPPED" in message
    assert "iagent_mesh" in message


def test_with_the_sdk_present_the_message_does_not_claim_it_is_missing(no_request):
    with pytest.raises(CallerUnresolved) as exc:
        resolve_authz_id()
    assert "SKIPPED" not in str(exc.value)


def test_the_sdk_is_optional_not_required(no_sdk):
    """Absent SDK must not break the lower rungs."""
    assert resolve_authz_id(caller=_Caller("u@example.com"))[0] == "u@example.com"
    assert resolve_authz_id(allow_service_identity=True) == (None, "service")


# ---------------------------------------------------------------------------
# The four-state outcome -- raise_for_status is no longer the interpreter
# ---------------------------------------------------------------------------


def test_success_classifies_as_no_error():
    assert classify_response(200) is None


@pytest.mark.parametrize("status", [401, 403])
def test_a_refusal_is_unentitled(status):
    assert isinstance(classify_response(status), NotEntitled)


def test_a_404_is_unavailable_not_absent():
    """THE RULING. The gateway answers 404 when no broker holds a live
    route. Routes carry a TTL and are re-pushed on a heartbeat, so that is
    a LIVENESS signal about the owning deployment -- not "no such asset".
    A composer reporting "no such data" when the truth is "the owner is
    down" is the dishonest report this taxonomy exists to prevent."""
    outcome = classify_response(404)
    assert isinstance(outcome, MeshUnavailable)
    assert not isinstance(outcome, NotEntitled)
    assert "liveness" in str(outcome)


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_server_errors_are_unavailable(status):
    assert isinstance(classify_response(status), MeshUnavailable)


def test_the_three_outcomes_are_separately_catchable():
    """A composing verb one level up cannot report honestly over a client
    that collapses them, which is what raise_for_status did."""
    assert not isinstance(classify_response(403), MeshUnavailable)
    assert not isinstance(classify_response(502), NotEntitled)
    assert not issubclass(CallerUnresolved, (NotEntitled, MeshUnavailable))


# ---------------------------------------------------------------------------
# Through the real constructor
# ---------------------------------------------------------------------------


from dag_tools.cortex_data.client import CortexDataClient


@pytest.fixture
def broker(monkeypatch):
    monkeypatch.setenv("CORTEX_BROKER_URL", "http://gw")
    monkeypatch.setenv("MESH_DEV_TOKEN", "transport-token")


def test_a_caller_becomes_the_subject_the_gate_keys_on(broker, no_request):
    client = CortexDataClient(caller=_Caller("user@example.com"))
    assert client.originator_email == "user@example.com"
    assert client.identity_rung == "caller"


def test_a_bare_client_inside_a_request_reads_as_the_caller(broker, in_request):
    """The one-line defect, closed: bare construction is now correct."""
    in_request(_Caller("handler-user@example.com"))
    client = CortexDataClient()
    assert client.originator_email == "handler-user@example.com"
    assert client.identity_rung == "request"


def test_a_bare_client_inside_an_unresolved_request_refuses(broker, in_request):
    """Rather than returning rows read with the service's entitlements."""
    in_request(_Caller(None, reason="absent"))
    with pytest.raises(CallerUnresolved):
        CortexDataClient()


def test_outside_a_request_a_provisioned_process_reads_as_itself(
    broker, no_request,
):
    """Provisioning the process with a transport credential IS the opt-in.

    Demanding a further code flag would break every notebook, CLI and
    Dagster asset while adding no safety: the dangerous case -- bare
    construction inside a handler -- never reaches this rung, because the
    request rung above is terminal. The guard is that terminality, not a
    flag here.
    """
    client = CortexDataClient()
    assert client.identity_rung == "service"
    assert client.originator_email is None


def test_with_no_credential_at_all_the_credential_error_speaks_first(
    monkeypatch, no_request,
):
    """Ordering: a process with nothing provisioned has a transport
    problem, not an identity one, and saying "no caller" would send the
    operator hunting in the wrong place."""
    monkeypatch.setenv("CORTEX_BROKER_URL", "http://gw")
    monkeypatch.delenv("MESH_DEV_TOKEN", raising=False)
    monkeypatch.delenv("CORTEX_CLIENT_ID", raising=False)
    monkeypatch.delenv("CORTEX_CLIENT_SECRET", raising=False)
    with pytest.raises(ValueError, match="jwt_token|M2M"):
        CortexDataClient()


def test_naming_the_service_credentials_is_the_opt_in(broker, no_request):
    """Passing client_id/client_secret explicitly IS asking to read as the
    service, so it needs no second flag. This is what keeps the producing
    IO managers -- which pass them outright -- working unchanged."""
    client = CortexDataClient(client_id="id", client_secret="secret")
    assert client.identity_rung == "service"
    assert client.originator_email is None


def test_the_explicit_flag_also_opts_in(broker, no_request):
    client = CortexDataClient(service_identity=True)
    assert client.identity_rung == "service"


def test_an_explicit_originator_email_still_wins(broker, in_request):
    """Back-compat: callers who named the subject outright have already
    decided, and 61cbfa9's threading must keep working."""
    in_request(_Caller("request-user@example.com"))
    client = CortexDataClient(originator_email="explicit@example.com")
    assert client.originator_email == "explicit@example.com"
    assert client.identity_rung == "explicit"
