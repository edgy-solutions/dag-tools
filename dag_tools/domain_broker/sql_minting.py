"""`mint-role` — per-request database credentials the SERVER enforces.

ADR-0044's capability matrix puts PostgreSQL and ClickHouse in the `mint-role`
column, and this is that column. Where the object-store minter hands out a
scoped STS credential, these hand out a **short-lived database role**, and the
difference is worth stating because it is an upgrade rather than a parity fix:

    S3         narrowing is by PREFIX. Row/column filters stay client-side.
    mint-role  narrowing is by GRANT and ROW POLICY, enforced by the database.
               A caller who declines to apply the client-side filters still
               sees only their entitled rows and columns.

That is what ADR-0025 always implied and an object store cannot deliver.

WHY THERE IS NO REAPER, AND WHY THAT IS NOT AN OVERSIGHT. Both engines support
`VALID UNTIL`, so **the database enforces expiry**. A leaked credential stops
working whether or not anything cleaned it up, and a broker that dies
mid-request leaves a role that is already harmless. `sweep_expired()` exists to
stop the catalog accumulating dead entries — it is hygiene, not the security
boundary. Designing it the other way round would put correctness in a
background task that has to keep running.

THE TRAP THIS MODULE IS WRITTEN AROUND (ADR-0044 named it before we hit it):
for ClickHouse, read-only is a SETTINGS PROFILE property, separate from table
grants. A role with perfect grants and an inherited permissive profile
satisfies half the requirement and looks whole. Both claims are pinned here,
and the test suite asserts each independently.
"""
from __future__ import annotations

import logging
import os
import re
import secrets
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

#: Identifiers we are willing to interpolate into DDL. Table and column names
#: arrive from a producer's config and from Topaz — neither is end-user input,
#: but DDL cannot be parameterised, so nothing reaches a statement without
#: passing this. A rejected identifier fails the mint; it is never sanitised
#: into something that "looks close", because a silently-altered column name
#: grants the wrong column.
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class MintingError(RuntimeError):
    """Minting failed, with the cause named. Never returns a partial credential."""


def _ident(value: str, what: str) -> str:
    if not value or not _IDENT.match(value):
        raise MintingError(
            f"refusing to build DDL with an unsafe {what}: {value!r}. "
            f"Expected a plain SQL identifier."
        )
    return value


def _password() -> str:
    # No punctuation: this string is interpolated into DDL and also travels
    # back in a JSON ticket and into a DSN. Alphanumeric removes every quoting
    # question at the cost of a few bits, which 48 characters more than repays.
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(48))


def _role_name(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(10)}"


def _expiry() -> datetime:
    ttl = int(os.getenv("BROKER_CREDENTIAL_TTL_SEC", "900"))
    return datetime.now(timezone.utc) + timedelta(seconds=ttl)


def _admin(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


def sql_scope_from_uri(physical_uri: str) -> Optional[Dict[str, str]]:
    """`postgres://host:port/schema/table` -> its parts.

    Mirrors the layout ``SQLIOManager.mesh_uri`` advertises and the cortex data
    client parses. Three implementations of one format is two too many; this is
    the broker's, and it must move with the other two.
    """
    if "://" not in physical_uri:
        return None
    _, _, remainder = physical_uri.partition("://")
    host_port, _, rest = remainder.partition("/")
    if not host_port or not rest:
        return None
    parts = rest.split("/")
    if len(parts) < 2:
        return None
    host, _, port = host_port.partition(":")
    return {
        "host": host,
        "port": port or "",
        "schema": parts[0],
        "table": parts[1],
    }


# ── ClickHouse ─────────────────────────────────────────────────────────────

def mint_clickhouse(
    scope: Dict[str, str],
    urn: str,
    coordinates: Dict[str, Any],
    authz: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """A short-lived ClickHouse user, granted exactly this asset.

    Four claims, each pinned independently because each can be satisfied while
    another is not:

    1. **expiring** — ``VALID UNTIL``, enforced by the server.
    2. **this table only** — ``GRANT SELECT ON <db>.<table>``.
    3. **these columns only** — ``GRANT SELECT(cols)`` when Topaz narrowed them.
    4. **read-only** — ``SETTINGS readonly = 1``, which is a PROFILE property
       and NOT implied by granting only SELECT. Pinned explicitly; the ADR
       flagged this as the way to get it half-right and look finished.

    Plus a row policy when Topaz supplied a filter, which is the whole point:
    it moves narrowing server-side, where a hostile client cannot skip it.
    """
    import clickhouse_connect

    authz = authz or {}
    database = _ident(scope.get("schema") or coordinates.get("database") or "default", "database")
    table = _ident(scope["table"], "table")
    host = scope["host"]
    port = int(scope.get("port") or 8123)

    admin_user = _admin("BROKER_CH_ADMIN_USER", "CH_USER")
    admin_password = _admin("BROKER_CH_ADMIN_PASSWORD", "CH_PASSWORD")
    if not admin_user:
        raise MintingError(
            "no ClickHouse minting identity: set BROKER_CH_ADMIN_USER/"
            "BROKER_CH_ADMIN_PASSWORD (or CH_USER/CH_PASSWORD) on the broker."
        )

    user = _role_name("mesh")
    password = _password()
    valid_until = _expiry().strftime("%Y-%m-%d %H:%M:%S")

    client = clickhouse_connect.get_client(
        host=host, port=port, username=admin_user, password=admin_password or "",
    )
    try:
        # readonly=1 in the SAME statement that creates the user: a separate
        # ALTER would leave a window in which the user exists and can write.
        client.command(
            f"CREATE USER {user} IDENTIFIED WITH sha256_password BY '{password}' "
            f"VALID UNTIL '{valid_until}' SETTINGS readonly = 1"
        )

        columns: List[str] = list(authz.get("allowed_columns") or [])
        if columns:
            cols = ", ".join(_ident(c, "column") for c in columns)
            client.command(f"GRANT SELECT({cols}) ON {database}.{table} TO {user}")
        else:
            client.command(f"GRANT SELECT ON {database}.{table} TO {user}")

        row_filter = authz.get("row_filters")
        if row_filter:
            # The expression comes from the authorization decision, not from a
            # caller. It cannot be parameterised — a policy IS an expression —
            # so it is inlined, and that is why its provenance matters.
            policy = _role_name("meshpol")
            client.command(
                f"CREATE ROW POLICY {policy} ON {database}.{table} "
                f"USING {row_filter} TO {user}"
            )
            logger.info("minted CH row policy for %s: %s", urn, row_filter)
    except Exception:
        # Never leave a usable half-configured user behind: one that exists
        # with no row policy is broader than intended, which is the failure
        # this whole module exists to prevent.
        try:
            client.command(f"DROP USER IF EXISTS {user}")
        except Exception:  # noqa: BLE001
            logger.error("could not drop partially created CH user %s", user)
        raise

    return {
        "username": user,
        "password": password,
        "database": database,
        # Echoed so a consumer can report what it was given without guessing.
        "expires_at": valid_until,
    }


# ── PostgreSQL ─────────────────────────────────────────────────────────────

def mint_postgres(
    scope: Dict[str, str],
    urn: str,
    coordinates: Dict[str, Any],
    authz: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """A short-lived PostgreSQL LOGIN role, granted exactly this asset.

    NOT GATED ON PG18. The ergonomic path would be PG18's native OAuth, but
    libpq's OAUTHBEARER has no Python API for injecting a pre-existing JWT
    (device flow or ``PQsetAuthDataHook`` in C only) and ADBC does not expose
    the hook — recorded in ``cortex_data/client.py`` after it was tried. So a
    short-lived role plus RLS is not the fallback; on current tooling it is
    THE path, and it delivers the same server-side enforcement on 17.

    Claims pinned independently:

    1. **expiring** — ``VALID UNTIL``, enforced by the server.
    2. **this table only** — ``GRANT SELECT`` on one relation, after
       ``REVOKE``ing the ambient PUBLIC grant that would otherwise make the
       narrowing decorative.
    3. **these columns only** — column-level ``GRANT SELECT (cols)``.
    4. **rows** — a ``POLICY ... FOR SELECT TO <role>`` under RLS.

    RLS IS NOT ASSUMED. If the table does not have it enabled, a policy is
    inert and the role would see every row while looking constrained. That case
    RAISES rather than issuing a credential, because a silently-wider grant is
    the exact defect being fixed.
    """
    import psycopg2

    authz = authz or {}
    schema = _ident(scope.get("schema") or "public", "schema")
    table = _ident(scope["table"], "table")
    database = coordinates.get("database") or scope.get("database") or "postgres"
    host = scope["host"]
    port = int(scope.get("port") or 5432)

    admin_user = _admin("BROKER_PG_ADMIN_USER", "PG_USER")
    admin_password = _admin("BROKER_PG_ADMIN_PASSWORD", "PG_PASSWORD")
    if not admin_user:
        raise MintingError(
            "no PostgreSQL minting identity: set BROKER_PG_ADMIN_USER/"
            "BROKER_PG_ADMIN_PASSWORD (or PG_USER/PG_PASSWORD) on the broker."
        )

    role = _role_name("mesh")
    password = _password()
    valid_until = _expiry().strftime("%Y-%m-%d %H:%M:%S+00")

    conn = psycopg2.connect(
        host=host, port=port, user=admin_user,
        password=admin_password or "", dbname=database,
    )
    conn.autocommit = True
    row_filter = authz.get("row_filters")
    try:
        with conn.cursor() as cur:
            if row_filter:
                # Refuse rather than issue a role whose policy cannot bind.
                cur.execute(
                    "SELECT relrowsecurity FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = %s AND c.relname = %s",
                    (schema, table),
                )
                row = cur.fetchone()
                if not row:
                    raise MintingError(f"{schema}.{table} does not exist on {host}")
                if not row[0]:
                    raise MintingError(
                        f"row filtering was required for {urn} but RLS is not enabled on "
                        f"{schema}.{table}. A policy would be inert and the role would see "
                        f"EVERY row while appearing constrained. Enable it: "
                        f"ALTER TABLE {schema}.{table} ENABLE ROW LEVEL SECURITY;"
                    )

            cur.execute(
                f"CREATE ROLE {role} LOGIN PASSWORD '{password}' "
                f"VALID UNTIL '{valid_until}'"
            )
            # PUBLIC grants would make everything below decorative.
            cur.execute(f"REVOKE ALL ON {schema}.{table} FROM {role}")
            cur.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")

            columns = list(authz.get("allowed_columns") or [])
            if columns:
                cols = ", ".join(_ident(c, "column") for c in columns)
                cur.execute(f"GRANT SELECT ({cols}) ON {schema}.{table} TO {role}")
            else:
                cur.execute(f"GRANT SELECT ON {schema}.{table} TO {role}")

            if row_filter:
                policy = _role_name("meshpol")
                cur.execute(
                    f"CREATE POLICY {policy} ON {schema}.{table} "
                    f"FOR SELECT TO {role} USING ({row_filter})"
                )
                logger.info("minted PG row policy for %s: %s", urn, row_filter)
    except Exception:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP ROLE IF EXISTS {role}")
        except Exception:  # noqa: BLE001
            logger.error("could not drop partially created PG role %s", role)
        raise
    finally:
        conn.close()

    return {
        "username": role,
        "password": password,
        "database": database,
        "expires_at": valid_until,
    }
