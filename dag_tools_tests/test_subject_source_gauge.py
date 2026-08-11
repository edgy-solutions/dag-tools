"""The subject-source gauge measures, never refuses — and its own records are PROVEN visible.

WHY THESE PINS EXIST. This instrument sits on a LIVE data path at work. Two properties are
load-bearing and neither is self-evident from reading the code:

1. **It cannot change a request's outcome.** No refusal, no altered subject, no raise. A gauge
   that can break the thing it measures is worse than no gauge.
2. **Its records actually emit.** The mesh's transport gauge shipped with twelve services
   announcing OBSERVE and observing nothing, because nothing configured logging and the records
   fell through to `logging.lastResort` and were discarded. A migration precondition of "the
   divergent count reads zero" is satisfied perfectly and falsely by a silent gauge.
   ZERO-BECAUSE-SILENT AND ZERO-BECAUSE-CLEAN ARE THE TWO STATES THIS INSTRUMENT SEPARATES,
   so "the gauge is visible" is pinned rather than assumed.

The agree/diverge split on `header-override` is pinned hardest, because it is the number the
whole item exists to produce: a thousand AGREEING overrides is a config change, ten DIVERGENT
ones is a negotiation with ten callers.
"""
from __future__ import annotations

import logging

import pytest

from dag_tools.central_gateway import subject_gauge as sg


# ---------------------------------------------------------------------------
# classify — the four buckets, and the split that decides the migration
# ---------------------------------------------------------------------------
def test_token_only_is_the_healthy_shape():
    r = sg.classify("alice@example.com", None)
    assert r["source"] == sg.SRC_TOKEN
    assert r["agreement"] is None
    assert r["effective_subject"] == "alice@example.com"


def test_header_only_is_todays_DA_path():
    """The bearer is an M2M service token with no user email, so the end user's identity can
    only arrive by header. Expected, and the reason the header exists at all."""
    r = sg.classify(None, "alice@example.com")
    assert r["source"] == sg.SRC_HEADER_ONLY
    assert r["agreement"] is None


def test_override_AGREEING_means_removing_the_override_is_a_config_change():
    r = sg.classify("alice@example.com", "alice@example.com")
    assert r["source"] == sg.SRC_HEADER_OVERRIDE
    assert r["agreement"] == sg.AGREE


def test_override_DIVERGENT_is_the_number_the_item_exists_to_produce():
    """THE PIN THAT MATTERS. Removing the header override changes WHO THIS REQUEST READS AS.
    Counting header-override without this split answers the wrong question."""
    r = sg.classify("service-account-da", "alice@example.com")
    assert r["source"] == sg.SRC_HEADER_OVERRIDE
    assert r["agreement"] == sg.DIVERGE
    assert r["effective_subject"] == "alice@example.com", (
        "the gauge must report the subject the GATE would use — the header wins today"
    )


def test_none_when_neither_source_supplies_a_subject():
    assert sg.classify(None, None)["source"] == sg.SRC_NONE
    assert sg.classify("  ", "")["source"] == sg.SRC_NONE, "whitespace is not a subject"


# ---------------------------------------------------------------------------
# verify_bearer — verify-if-present, and honest about what it could not prove
# ---------------------------------------------------------------------------
def test_absent_token_is_absent_not_invalid():
    i = sg.verify_bearer(None)
    assert i.verified is False and i.reason == "absent" and i.authz_id is None


def test_no_key_configured_reads_the_claim_but_reports_UNVERIFIED(monkeypatch):
    """Honest-unverified. A decode without signature checking is the presence-check defect
    wearing a JWT's clothes, so the subject is readable and the state says so."""
    jwt = pytest.importorskip("jwt")
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("KEYCLOAK_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("GATEWAY_JWKS_URL", raising=False)

    token = jwt.encode({"email": "alice@example.com"}, "shhh", algorithm="HS256")
    i = sg.verify_bearer(token)
    assert i.authz_id == "alice@example.com"
    assert i.verified is False
    assert i.reason == "no-verification-key", "the REASON must be named, never left blank"


def test_a_verified_token_reports_verified(monkeypatch):
    jwt = pytest.importorskip("jwt")
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", "topsecretkey")
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "HS256")
    monkeypatch.delenv("GATEWAY_JWKS_URL", raising=False)

    token = jwt.encode({"email": "alice@example.com"}, "topsecretkey", algorithm="HS256")
    i = sg.verify_bearer(token)
    assert i.verified is True and i.authz_id == "alice@example.com"


def test_a_tampered_token_is_invalid_and_NAMED(monkeypatch):
    jwt = pytest.importorskip("jwt")
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", "topsecretkey")
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "HS256")
    monkeypatch.delenv("GATEWAY_JWKS_URL", raising=False)

    token = jwt.encode({"email": "mallory@example.com"}, "WRONGKEY", algorithm="HS256")
    i = sg.verify_bearer(token)
    assert i.verified is False and i.reason.startswith("invalid:")


def test_garbage_never_raises():
    """MEASURING MUST NEVER BREAK THE MEASURED. Every path returns a CallerIdentity."""
    for junk in ("", "not-a-jwt", "a.b.c", "..", "Bearer x"):
        i = sg.verify_bearer(junk)
        assert isinstance(i, sg.CallerIdentity)
        assert i.verified is False


def test_the_entitlement_claim_follows_the_mesh_env(monkeypatch):
    """A gauge keyed on a different claim than the gate would measure a subject nobody
    authorizes on."""
    jwt = pytest.importorskip("jwt")
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "employee_id")
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("KEYCLOAK_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("GATEWAY_JWKS_URL", raising=False)

    token = jwt.encode({"employee_id": "E1234", "email": "alice@example.com"}, "k",
                       algorithm="HS256")
    assert sg.verify_bearer(token).authz_id == "E1234"


# ---------------------------------------------------------------------------
# observe — one line per request, and it NEVER refuses
# ---------------------------------------------------------------------------
def test_observe_emits_the_reading_and_returns_it(caplog, monkeypatch):
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("GATEWAY_JWKS_URL", raising=False)
    with caplog.at_level(logging.INFO, logger="dag_tools.central_gateway.subject_gauge"):
        r = sg.observe(urn="urn:li:dataset:(x,y,PROD)", token=None,
                       header_subject="alice@example.com")
    assert r["source"] == sg.SRC_HEADER_ONLY
    assert any("subject-source:" in m for m in caplog.messages)


def test_divergent_requests_earn_a_WARNING_an_operator_can_grep(caplog, monkeypatch):
    """Only the divergent bucket warns — it is the one where removing the override changes who
    the request reads as."""
    jwt = pytest.importorskip("jwt")
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("KEYCLOAK_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("GATEWAY_JWKS_URL", raising=False)
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "email")

    token = jwt.encode({"email": "service-account-da"}, "k", algorithm="HS256")
    with caplog.at_level(logging.WARNING, logger="dag_tools.central_gateway.subject_gauge"):
        r = sg.observe(urn="urn:x", token=token, header_subject="alice@example.com")

    assert r["agreement"] == sg.DIVERGE
    assert any("SUBJECT-SOURCE DIVERGENT" in m for m in caplog.messages)


def test_agreeing_override_does_NOT_warn(caplog, monkeypatch):
    jwt = pytest.importorskip("jwt")
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("KEYCLOAK_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("GATEWAY_JWKS_URL", raising=False)
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "email")

    token = jwt.encode({"email": "alice@example.com"}, "k", algorithm="HS256")
    with caplog.at_level(logging.WARNING, logger="dag_tools.central_gateway.subject_gauge"):
        sg.observe(urn="urn:x", token=token, header_subject="alice@example.com")
    assert not any("DIVERGENT" in m for m in caplog.messages)


# ---------------------------------------------------------------------------
# The gauge's own visibility — the lesson the transport gauge paid for
# ---------------------------------------------------------------------------
def test_the_gauge_is_VISIBLE_not_merely_emitted(monkeypatch):
    """THE ZERO-BECAUSE-SILENT PIN. With no logging configured at all, an INFO record from this
    package must still reach a handler. Twelve mesh services announced OBSERVE and observed
    nothing precisely because nothing asserted this."""
    monkeypatch.delenv("GATEWAY_GAUGE_LOG_AUTOCONFIG", raising=False)
    pkg = logging.getLogger("dag_tools.central_gateway")
    saved_handlers, saved_level = list(pkg.handlers), pkg.level
    try:
        pkg.handlers.clear()
        pkg.setLevel(logging.WARNING)  # INFO would be dropped
        sg.ensure_gauge_visible()
        assert sg._emits_info(sg.logger), (
            "the gauge announced a posture into a channel nothing can read"
        )
    finally:
        pkg.handlers[:] = saved_handlers
        pkg.setLevel(saved_level)


def test_visibility_autoconfig_can_be_broken_ON_PURPOSE(monkeypatch):
    """A leg of a litany that has never gone red is not yet a check — so the escape hatch that
    makes the gauge go dark is itself exercised."""
    monkeypatch.setenv("GATEWAY_GAUGE_LOG_AUTOCONFIG", "0")
    assert sg.ensure_gauge_visible() is False


def test_visibility_is_deferential_when_logging_is_already_configured(monkeypatch):
    """Additive-and-deferential: an app that configured logging owns it, and a second handler
    here would DOUBLE-EMIT into every properly configured deployment."""
    monkeypatch.delenv("GATEWAY_GAUGE_LOG_AUTOCONFIG", raising=False)
    pkg = logging.getLogger("dag_tools.central_gateway")
    saved_handlers, saved_level = list(pkg.handlers), pkg.level
    try:
        pkg.handlers.clear()
        pkg.addHandler(logging.NullHandler())
        pkg.setLevel(logging.INFO)
        before = len(pkg.handlers)
        assert sg.ensure_gauge_visible() is False
        assert len(pkg.handlers) == before, "a second handler would double-emit"
    finally:
        pkg.handlers[:] = saved_handlers
        pkg.setLevel(saved_level)


# ---------------------------------------------------------------------------
# Posture announcement — the claim AND its source, and honesty about REQUIRE
# ---------------------------------------------------------------------------
def test_posture_line_names_its_source(monkeypatch):
    """`OBSERVE (default)` and `OBSERVE (explicit config)` are different claims about whether
    anyone decided."""
    monkeypatch.delenv("GATEWAY_SUBJECT_GAUGE", raising=False)
    assert "OBSERVE (default)" in sg.posture_line()
    monkeypatch.setenv("GATEWAY_SUBJECT_GAUGE", "OBSERVE")
    assert "OBSERVE (explicit config)" in sg.posture_line()


def test_missing_verification_key_ANNOUNCES_ITSELF(monkeypatch):
    """A gauge reading unverified on every request because no key is configured looks identical
    to one reading unverified because every caller forges tokens. Startup separates them."""
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("KEYCLOAK_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("GATEWAY_JWKS_URL", raising=False)
    assert "NONE CONFIGURED" in sg.verification_line()


def test_a_REQUIRE_flag_is_loudly_IGNORED_not_silently(caplog, monkeypatch):
    """An operator who sets a require-shaped flag and gets silence would reasonably believe the
    gateway is enforcing. It is not, and a false belief in enforcement is worse than its
    absence."""
    monkeypatch.setenv("REQUIRE_GATEWAY_AUTH", "true")
    with caplog.at_level(logging.WARNING, logger="dag_tools.central_gateway.subject_gauge"):
        sg.announce()
    assert any("IGNORED" in m for m in caplog.messages)


# ---------------------------------------------------------------------------
# The wiring itself — observe-only, pinned at the source
# ---------------------------------------------------------------------------
def _gateway_src() -> str:
    """Read main.py as TEXT, deliberately — not via import.

    Importing the gateway drags in redis/fastapi and makes this pin fail for reasons that have
    nothing to do with what it asserts. A source-level pin is also the stronger property here:
    it cannot be satisfied by a monkeypatched attribute at runtime.
    """
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    return (root / "dag_tools" / "central_gateway" / "main.py").read_text(encoding="utf-8")


def test_the_gateway_does_not_BRANCH_on_the_gauge():
    """THE REGRESSION PIN THAT MATTERS MOST. The moment a request outcome depends on the
    reading, this stops being a gauge and becomes an unreviewed enforcement path. So the call
    site must discard the return value."""
    import re

    src = _gateway_src()
    m = re.search(r"async def authorize_asset\(.*?(?=\n@app\.|\Z)", src, re.S)
    assert m, "authorize_asset not found — this pin is measuring nothing"
    body = m.group(0)

    assert "subject_gauge.observe(" in body, "the gauge must be wired into the request path"
    # A bare statement — no assignment, no branch, nothing reads it back.
    assert "= subject_gauge.observe" not in body, (
        "the gateway assigned the gauge reading — the next edit branches on it"
    )
    assert not re.search(r"if\s+subject_gauge", body), (
        "the gateway BRANCHED on the gauge — that is an unreviewed enforcement path"
    )
    assert "except Exception" in body, (
        "the gauge call must be guarded: measuring may never break the measured"
    )


def test_the_gauge_is_announced_at_STARTUP():
    """Announced in the lifespan, before anything can fail — otherwise a reader cannot tell an
    OBSERVE deployment from one where the gauge was never wired."""
    import re
    src = _gateway_src()
    m = re.search(r"async def lifespan\(.*?(?=\napp = )", src, re.S)
    assert m and "subject_gauge.announce()" in m.group(0), (
        "the gauge posture is not announced at startup"
    )


def test_the_gateway_still_REFUSES_NOTHING_on_the_gauges_account():
    """No refusal may be attributable to the gauge. Any `raise HTTPException` inside the
    gateway must belong to the pre-existing authz/routing logic, never to subject_gauge."""
    import re
    src = _gateway_src()
    for m in re.finditer(r"subject_gauge[^\n]*\n(?:[^\n]*\n){0,12}", src):
        chunk = m.group(0)
        assert "raise HTTPException" not in chunk, (
            "a refusal appeared next to the gauge — this instrument observes only"
        )
