"""A consumer gets a receipt for its read, and the gateway logs who got it.

The broker's half of this is in ``test_ticket_identity.py``; these are the two
ends that make the identity usable.

THE CLIENT returns the frame AND a receipt, so an answer can be cited: which
read produced it (``ticket_id``, the same id the gateway logged) and over what
version (``as_of``). ``get_dataframe`` keeps returning the frame alone, because
notebooks, IO managers and agents all call it and a return type that grew a
tuple would break every one of them.

THE RECEIPT IS AN ALLOWLIST, not "the ticket minus credentials". It is meant to
be kept, printed beside an answer and pasted into a ticket, so a denylist would
start publishing the day a ticket gains a secret-shaped field. ``physical_uri``
is left off for exactly that reason: a presigned URI is a credential wearing a
path.

THE GATEWAY logs the ticket id against the PRINCIPAL IT SENT THE BROKER, from
one expression read twice. Deriving the log's subject separately is how an
audit trail quietly starts describing someone other than the decision did.
"""
from __future__ import annotations

import logging
import os
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import polars as pl
import pytest

jwt = pytest.importorskip("jwt")

from dag_tools.cortex_data import client as client_mod
from dag_tools.cortex_data.client import CortexDataClient, ticket_receipt

URN = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/x,PROD)"
BROKER_ENV = {"CORTEX_BROKER_URL": "http://gw", "MESH_DEV_TOKEN": "t"}

FULL_TICKET = {
    "ticket_id": "11111111-2222-3333-4444-555555555555",
    "issued_at": "2026-11-08T12:30:00+00:00",
    "as_of": "41",
    "as_of_source": "read_at_authorize",
    "source_type": "s3_parquet",
    "physical_uri": "s3://bucket/x?X-Amz-Signature=deadbeef",
    "credentials": {
        "aws_access_key_id": "MINTED_KEY",
        "aws_secret_access_key": "MINTED_SECRET",
        "aws_session_token": "MINTED_TOKEN",
    },
    "allowed_columns": ["a", "b"],
    "row_filters": "a > 0",
}


# ── the receipt itself ─────────────────────────────────────────────────────

def test_the_receipt_carries_no_credential():
    """THE acceptance criterion. Everything else can pass while this fails."""
    receipt = ticket_receipt(URN, FULL_TICKET)

    blob = repr(receipt)
    for secret in ("MINTED_KEY", "MINTED_SECRET", "MINTED_TOKEN"):
        assert secret not in blob
    assert "credentials" not in receipt


def test_a_presigned_uri_is_not_on_the_receipt():
    """``physical_uri`` can BE a credential -- a signature in a query string."""
    receipt = ticket_receipt(URN, FULL_TICKET)

    assert "physical_uri" not in receipt
    assert "deadbeef" not in repr(receipt)


def test_the_receipt_is_an_allowlist_so_a_new_ticket_field_is_not_published():
    """The regression that matters for every ticket field added after today.

    A denylist would carry this through; the allowlist means publishing a new
    field is a deliberate edit to TICKET_RECEIPT_FIELDS.
    """
    receipt = ticket_receipt(URN, {**FULL_TICKET, "delegation_secret": "oops"})

    assert "delegation_secret" not in receipt
    assert "oops" not in repr(receipt)


def test_the_receipt_says_which_read_and_over_what_version():
    receipt = ticket_receipt(URN, FULL_TICKET)

    assert receipt["urn"] == URN
    assert receipt["ticket_id"] == FULL_TICKET["ticket_id"]
    assert receipt["issued_at"] == FULL_TICKET["issued_at"]
    assert receipt["as_of"] == "41"
    assert receipt["as_of_source"] == "read_at_authorize"


def test_the_receipt_names_the_narrowing_that_was_applied():
    """A frame with two columns is not a mistake if the ticket said two."""
    receipt = ticket_receipt(URN, FULL_TICKET)

    assert receipt["allowed_columns"] == ["a", "b"]
    assert receipt["row_filters"] == "a > 0"


def test_an_older_broker_yields_a_receipt_of_the_SAME_SHAPE():
    """ONE shape whatever the client is talking to.

    A broker that has not been upgraded stamps no identity. If the keys simply
    vanished, a caller reading `receipt.get("ticket_id")` could not tell an
    un-upgraded broker from a key it had misspelled.
    """
    receipt = ticket_receipt(URN, {"source_type": "s3_parquet"})

    assert receipt["ticket_id"] is None
    assert receipt["issued_at"] is None
    assert receipt["as_of"] is None
    assert receipt["as_of_source"] is None


def test_an_absent_as_of_reason_is_carried_when_the_broker_gives_one():
    receipt = ticket_receipt(URN, {"as_of_unavailable": "no_reader_for_source_type"})

    assert receipt["as_of_unavailable"] == "no_reader_for_source_type"


# ── the client's two entry points ──────────────────────────────────────────

def _install(ticket):
    class _Resp:
        status_code = 200

        def json(self):
            return ticket

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def post(self, url, headers=None, **kw):
            return _Resp()

    return patch.object(httpx, "Client", _Client)


@pytest.fixture
def frames(monkeypatch):
    """Keep real object storage out of it; the read itself is not under test."""
    monkeypatch.setattr(
        client_mod.pl,
        "scan_parquet",
        lambda *a, **k: pl.LazyFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]}),
    )


def _read(ticket, frames):
    with patch.dict(os.environ, BROKER_ENV, clear=True):
        client = CortexDataClient()
        with _install(ticket):
            return client.get_dataframe_with_ticket(URN)


def test_get_dataframe_still_returns_only_a_frame(frames):
    """BACKWARDS COMPATIBILITY (AGENTS.md §6): every existing caller survives.

    `pub-tools`, every IO manager and every notebook call this one.
    """
    with patch.dict(os.environ, BROKER_ENV, clear=True):
        client = CortexDataClient()
        with _install(FULL_TICKET):
            result = client.get_dataframe(URN)

    assert isinstance(result, pl.LazyFrame)


def test_the_frame_comes_back_alongside_its_receipt(frames):
    frame, receipt = _read(FULL_TICKET, frames)

    assert isinstance(frame, pl.LazyFrame)
    assert receipt["ticket_id"] == FULL_TICKET["ticket_id"]


def test_the_frame_is_still_narrowed_when_the_receipt_is_asked_for(frames):
    """The receipt is additional; it must not become an unguarded read path."""
    frame, receipt = _read(FULL_TICKET, frames)

    assert frame.collect_schema().names() == ["a", "b"]
    assert receipt["allowed_columns"] == ["a", "b"]


def test_the_returned_receipt_carries_no_credential(frames):
    _, receipt = _read(FULL_TICKET, frames)

    assert "MINTED_KEY" not in repr(receipt)
    assert "credentials" not in receipt


# ── the gateway's log line ─────────────────────────────────────────────────

@pytest.fixture
def gateway_world(monkeypatch):
    """The real authorize route, with Redis, Topaz and the broker faked.

    ``observe`` posture (the default), so the token is not verified and the
    subject comes from the header -- the arm every sandbox request takes.
    """
    pytest.importorskip("redis", reason="gateway tests need the [broker] extra")
    from fastapi.testclient import TestClient

    from dag_tools.central_gateway import main as gateway

    world = SimpleNamespace(broker=[], ticket={})

    class _Redis:
        async def get(self, key):
            return "http://broker.invalid"

    class _Resp:
        def __init__(self, code, body):
            self.status_code, self._b, self.text = code, body, ""

        def json(self):
            return self._b

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None, timeout=None):
            if "/authz/is" in url:
                return _Resp(200, {"decisions": [{"is": True}]})
            world.broker.append(json)
            return _Resp(200, world.ticket)

    monkeypatch.setattr(gateway, "redis_client", _Redis())
    monkeypatch.setattr(gateway.httpx, "AsyncClient", lambda *a, **k: _Client())
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", set())
    monkeypatch.delenv("GATEWAY_IDENTITY_POSTURE", raising=False)

    world.http = TestClient(gateway.app)
    return world


def _token():
    """A decodable token. ``observe`` never verifies it, but the Topaz step
    reads the subject out of it, so an opaque string fails earlier than the
    line under test."""
    return jwt.encode(
        {"sub": "kc-1", "email": "svc@example.com"}, "k" * 32, algorithm="HS256"
    )


def _authorize(world, email="alice@example.com"):
    return world.http.post(
        f"/api/v1/assets/{URN}/authorize",
        headers={"Authorization": f"Bearer {_token()}", "on_behalf_of": email},
    )


def _issued(caplog):
    return [r.getMessage() for r in caplog.records if "TICKET_ISSUED" in r.getMessage()]


def test_the_gateway_logs_the_ticket_id_with_the_principal(gateway_world, caplog):
    gateway_world.ticket = dict(FULL_TICKET)

    with caplog.at_level(logging.INFO, logger="dag_tools.central_gateway"):
        assert _authorize(gateway_world).status_code == 200

    lines = _issued(caplog)
    assert len(lines) == 1
    assert FULL_TICKET["ticket_id"] in lines[0]
    assert "alice@example.com" in lines[0]


def test_the_logged_subject_is_THE_SUBJECT_THE_BROKER_WAS_GIVEN(gateway_world, caplog):
    """One expression, read twice.

    An audit line naming a different principal from the decision it records is
    worse than no line: it is evidence of something that did not happen.
    """
    gateway_world.ticket = dict(FULL_TICKET)

    with caplog.at_level(logging.INFO, logger="dag_tools.central_gateway"):
        _authorize(gateway_world, email="bob@example.com")

    sent = gateway_world.broker[0]["subject"]
    assert f"subject={sent}" in _issued(caplog)[0]


def test_the_log_names_the_version_that_was_read(gateway_world, caplog):
    gateway_world.ticket = dict(FULL_TICKET)

    with caplog.at_level(logging.INFO, logger="dag_tools.central_gateway"):
        _authorize(gateway_world)

    assert "as_of=41" in _issued(caplog)[0]
    assert "as_of_source=read_at_authorize" in _issued(caplog)[0]


def test_an_unstamped_ticket_logs_a_dash_AND_STILL_SERVES_THE_READ(gateway_world, caplog):
    """A broker that has not been upgraded must not lose its consumers.

    The identity is the broker's to mint. Failing a read over a missing audit
    field would make this change a rollout order problem instead of an additive
    one.
    """
    gateway_world.ticket = {"source_type": "s3_parquet", "physical_uri": "s3://b/x"}

    with caplog.at_level(logging.INFO, logger="dag_tools.central_gateway"):
        response = _authorize(gateway_world)

    assert response.status_code == 200
    assert "ticket_id=-" in _issued(caplog)[0]


def test_the_log_line_names_no_credential(gateway_world, caplog):
    """This line goes wherever logs go, which is further than a ticket does."""
    gateway_world.ticket = dict(FULL_TICKET)

    with caplog.at_level(logging.INFO, logger="dag_tools.central_gateway"):
        _authorize(gateway_world)

    line = _issued(caplog)[0]
    for secret in ("MINTED_KEY", "MINTED_SECRET", "MINTED_TOKEN", "deadbeef"):
        assert secret not in line


def test_the_log_follows_the_VERIFIED_subject_where_it_differs_from_the_header():
    """The arm where the two candidate principals actually disagree.

    Under ``enforce``, a verified token carrying no ``on_behalf_of`` header
    reads as its own claim: the broker is handed that subject while the header
    subject is nothing at all. An audit line built from the header would name
    "-" for a read somebody really made -- so this is the test that makes "one
    expression, read twice" a fact rather than a comment.
    """
    pytest.importorskip("redis", reason="gateway tests need the [broker] extra")
    pytest.importorskip("cryptography", reason="RS256 needs cryptography")
    import time

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from fastapi.testclient import TestClient

    from dag_tools.central_gateway import identity
    from dag_tools.central_gateway import main as gateway

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    pub = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()
    issuer = "https://idp.example/realms/mesh"
    audience = "data-gateway"
    token = jwt.encode(
        {"sub": "kc-1", "email": "alice@example.com", "iss": issuer, "aud": audience,
         "exp": int(time.time()) + 300},
        priv,
        algorithm="RS256",
    )

    sent = []

    class _Redis:
        async def get(self, key):
            return "http://broker.invalid"

    class _Resp:
        def __init__(self, body):
            self.status_code, self._b, self.text = 200, body, ""

        def json(self):
            return self._b

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None, timeout=None):
            if "/authz/is" in url:
                return _Resp({"decisions": [{"is": True}]})
            sent.append(json)
            return _Resp(dict(FULL_TICKET))

    with patch.object(gateway, "redis_client", _Redis()), \
            patch.object(gateway.httpx, "AsyncClient", lambda *a, **k: _Client()), \
            patch.object(gateway, "DENIED_USER_SUBS", set()), \
            patch.object(identity, "_jwks_client", None), \
            patch.dict(os.environ, {
                "GATEWAY_IDENTITY_POSTURE": "enforce",
                "GATEWAY_JWT_ISSUER": issuer,
                "GATEWAY_JWT_AUDIENCE": audience,
                "GATEWAY_JWT_PUBLIC_KEY": pub,
            }):
        with patch("logging.Logger.info") as logged:
            response = TestClient(gateway.app).post(
                f"/api/v1/assets/{URN}/authorize",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200, response.text
    assert sent[0]["subject"] == "alice@example.com"
    lines = [
        call.args[0] % call.args[1:]
        for call in logged.call_args_list
        if call.args and "TICKET_ISSUED" in str(call.args[0])
    ]
    assert len(lines) == 1
    assert "subject=alice@example.com" in lines[0], (
        "the audit line names a different principal from the one the broker "
        "authorized for; it is evidence of something that did not happen"
    )
