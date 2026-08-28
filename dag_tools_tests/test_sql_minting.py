"""ADR-0044 `mint-role` — PostgreSQL and ClickHouse credentials the SERVER enforces.

EACH CLAIM IS PINNED SEPARATELY, on purpose. A minted role can be expiring and
still reach every table; scoped to one table and still writable; read-only and
still see every row. The ADR named the specific way to get it half-right and
look finished — ClickHouse's read-only is a SETTINGS PROFILE property, NOT
implied by granting only SELECT — so grants and profile are asserted apart.

The DDL is captured rather than executed here; `scripts/verify_adr0044_sql_minting.py`
runs the same code against real engines. Unit tests prove the statements are
built correctly; only a live run proves the engines accept and enforce them, and
the second does not follow from the first — which is the lesson `_mint_s3` paid
for when its verification passed on an operator's shell credentials.
"""
from __future__ import annotations

import pytest

from dag_tools.domain_broker import sql_minting as sm


# ── doubles that record DDL ────────────────────────────────────────────────

class _FakeCHClient:
    def __init__(self):
        self.commands = []

    def command(self, sql):
        self.commands.append(sql)


class _FakeCursor:
    def __init__(self, owner):
        self.owner = owner

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.owner.statements.append(sql)

    def fetchone(self):
        return (self.owner.rls_enabled,)


class _FakePG:
    def __init__(self, rls_enabled=True):
        self.statements = []
        self.rls_enabled = rls_enabled
        self.autocommit = False

    def cursor(self):
        return _FakeCursor(self)

    def close(self):
        pass


@pytest.fixture(autouse=True)
def _identity(monkeypatch):
    monkeypatch.setenv("CH_USER", "admin")
    monkeypatch.setenv("CH_PASSWORD", "pw")
    monkeypatch.setenv("PG_USER", "admin")
    monkeypatch.setenv("PG_PASSWORD", "pw")
    monkeypatch.setenv("BROKER_CREDENTIAL_TTL_SEC", "900")
    yield


CH_SCOPE = {"host": "ch.local", "port": "8123", "schema": "iagent", "table": "events"}
PG_SCOPE = {"host": "pg.local", "port": "5432", "schema": "public", "table": "orders"}


def _ch(monkeypatch, authz=None):
    fake = _FakeCHClient()
    import clickhouse_connect
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kw: fake)
    creds = sm.mint_clickhouse(CH_SCOPE, "urn:x", {}, authz)
    return fake, creds


def _pg(monkeypatch, authz=None, rls_enabled=True):
    fake = _FakePG(rls_enabled)
    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda **kw: fake)
    creds = sm.mint_postgres(PG_SCOPE, "urn:x", {"database": "iagent"}, authz)
    return fake, creds


# ── the scope parser ───────────────────────────────────────────────────────

def test_scope_parses_the_advertised_uri_layout():
    assert sm.sql_scope_from_uri("clickhouse://ch:8123/iagent/events") == {
        "host": "ch", "port": "8123", "schema": "iagent", "table": "events"}
    assert sm.sql_scope_from_uri("postgres://pg/public/orders")["port"] == ""
    assert sm.sql_scope_from_uri("not a uri") is None


# ── ClickHouse: four claims, four assertions ───────────────────────────────

def test_ch_credential_expires(monkeypatch):
    fake, creds = _ch(monkeypatch)
    create = next(c for c in fake.commands if c.startswith("CREATE USER"))
    assert "VALID UNTIL" in create
    assert creds["expires_at"]


def test_ch_is_readonly_and_that_is_a_SEPARATE_claim(monkeypatch):
    """Granting only SELECT does NOT make a ClickHouse user read-only.

    read-only is a settings-profile property. A role with correct grants and an
    inherited permissive profile satisfies half the requirement and looks whole
    — the trap ADR-0044 named before we hit it.
    """
    fake, _ = _ch(monkeypatch)
    create = next(c for c in fake.commands if c.startswith("CREATE USER"))
    assert "readonly = 1" in create, (
        "the profile was not pinned; grants alone leave the user writable"
    )
    # Same statement as the CREATE: a separate ALTER leaves a window in which
    # the user exists and is not yet read-only.
    assert "CREATE USER" in create and "readonly" in create


def test_ch_is_scoped_to_one_table(monkeypatch):
    fake, _ = _ch(monkeypatch)
    grant = next(c for c in fake.commands if c.startswith("GRANT"))
    assert "ON iagent.events TO" in grant
    assert "*.*" not in grant


def test_ch_columns_narrow_when_topaz_narrows(monkeypatch):
    fake, _ = _ch(monkeypatch, {"allowed_columns": ["id", "region"]})
    grant = next(c for c in fake.commands if c.startswith("GRANT"))
    assert "SELECT(id, region)" in grant


def test_ch_row_policy_is_created_server_side(monkeypatch):
    """THE POINT of mint-role: narrowing the client cannot skip."""
    fake, _ = _ch(monkeypatch, {"row_filters": "region = 'EMEA'"})
    policy = next((c for c in fake.commands if c.startswith("CREATE ROW POLICY")), None)
    assert policy and "USING region = 'EMEA'" in policy and "ON iagent.events" in policy


def test_ch_partial_failure_leaves_no_usable_user(monkeypatch):
    """A user created without its row policy is BROADER than intended."""
    calls = []

    class _Boom(_FakeCHClient):
        def command(self, sql):
            calls.append(sql)
            if sql.startswith("CREATE ROW POLICY"):
                raise RuntimeError("policy rejected")

    fake = _Boom()
    import clickhouse_connect
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kw: fake)

    with pytest.raises(RuntimeError):
        sm.mint_clickhouse(CH_SCOPE, "urn:x", {}, {"row_filters": "x = 1"})

    assert any(c.startswith("DROP USER IF EXISTS") for c in calls), (
        "a half-configured user survived the failure"
    )


# ── PostgreSQL ─────────────────────────────────────────────────────────────

def test_pg_credential_expires_and_is_table_scoped(monkeypatch):
    fake, creds = _pg(monkeypatch)
    create = next(s for s in fake.statements if s.startswith("CREATE ROLE"))
    assert "VALID UNTIL" in create and "LOGIN" in create
    assert any("GRANT SELECT ON public.orders TO" in s for s in fake.statements)


def test_pg_revokes_ambient_grants_first(monkeypatch):
    """Without the REVOKE, a PUBLIC grant makes the narrowing decorative."""
    fake, _ = _pg(monkeypatch)
    revoke_at = next(i for i, s in enumerate(fake.statements) if s.startswith("REVOKE ALL"))
    grant_at = next(i for i, s in enumerate(fake.statements) if s.startswith("GRANT SELECT"))
    assert revoke_at < grant_at


def test_pg_columns_narrow_when_topaz_narrows(monkeypatch):
    fake, _ = _pg(monkeypatch, {"allowed_columns": ["id", "total"]})
    assert any("GRANT SELECT (id, total) ON public.orders" in s for s in fake.statements)


def test_pg_row_policy_binds_to_the_minted_role(monkeypatch):
    fake, creds = _pg(monkeypatch, {"row_filters": "region = 'EMEA'"})
    policy = next(s for s in fake.statements if s.startswith("CREATE POLICY"))
    assert "FOR SELECT TO" in policy and creds["username"] in policy
    assert "USING (region = 'EMEA')" in policy


def test_pg_REFUSES_when_rls_is_off_and_a_filter_was_required(monkeypatch):
    """The sharpest one. A policy on a table without RLS is INERT.

    The role would see every row while appearing constrained — a silently-wider
    grant, which is the exact defect mint-role exists to remove. So it raises
    instead of issuing a credential.
    """
    with pytest.raises(sm.MintingError) as exc:
        _pg(monkeypatch, {"row_filters": "region = 'EMEA'"}, rls_enabled=False)
    assert "RLS is not enabled" in str(exc.value)
    assert "ENABLE ROW LEVEL SECURITY" in str(exc.value), "the error must say the fix"


# ── DDL safety ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad", ["events; DROP TABLE x", "ev ents", "", "1abc", "a-b"])
def test_unsafe_identifiers_are_refused_not_sanitised(monkeypatch, bad):
    """Sanitising would grant a DIFFERENT column than the one authorized."""
    import clickhouse_connect
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kw: _FakeCHClient())
    with pytest.raises(sm.MintingError):
        sm.mint_clickhouse({**CH_SCOPE, "table": bad}, "urn:x", {})


def test_no_minting_identity_names_the_variables(monkeypatch):
    for v in ("BROKER_CH_ADMIN_USER", "CH_USER"):
        monkeypatch.delenv(v, raising=False)
    with pytest.raises(sm.MintingError) as exc:
        sm.mint_clickhouse(CH_SCOPE, "urn:x", {})
    assert "BROKER_CH_ADMIN_USER" in str(exc.value)


def test_passwords_are_unique_per_mint(monkeypatch):
    _, a = _ch(monkeypatch)
    _, b = _ch(monkeypatch)
    assert a["password"] != b["password"] and a["username"] != b["username"]
    assert len(a["password"]) >= 32


# ── the privilege the deployment identity usually lacks ────────────────────
#
# Verified on the sandbox: the broker's configured PG_USER (`iagent`) has
# rolcreaterole = false, so every Postgres mint fails there. The code is right
# and the identity cannot do the job — the same shape as the broker's missing
# STS identity in 0.3.0, and the same cost if the message does not say so.

def test_pg_permission_denied_names_the_grant(monkeypatch):
    class _DeniedCursor(_FakeCursor):
        def execute(self, sql, params=None):
            self.owner.statements.append(sql)
            if sql.startswith("CREATE ROLE"):
                raise RuntimeError("permission denied to create role")

    class _DeniedPG(_FakePG):
        def cursor(self):
            return _DeniedCursor(self)

    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda **kw: _DeniedPG())

    with pytest.raises(sm.MintingError) as exc:
        sm.mint_postgres(PG_SCOPE, "urn:x", {"database": "iagent"})

    msg = str(exc.value)
    assert "CREATEROLE" in msg, "the error must name the grant that fixes it"
    assert "BROKER_PG_ADMIN_USER" in msg, "and the alternative to granting it"


def test_ch_access_management_denial_names_the_grant(monkeypatch):
    class _Denied(_FakeCHClient):
        def command(self, sql):
            self.commands.append(sql)
            if sql.startswith("CREATE USER"):
                raise RuntimeError("Not enough privileges. ACCESS_DENIED")

    import clickhouse_connect
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kw: _Denied())

    with pytest.raises(sm.MintingError) as exc:
        sm.mint_clickhouse(CH_SCOPE, "urn:x", {})
    assert "ACCESS MANAGEMENT" in str(exc.value)
