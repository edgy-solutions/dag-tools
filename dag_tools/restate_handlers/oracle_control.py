"""Durable Oracle writes for the PDM request/response handshake.

The extraction side of this flow already had a handler: ``oracle_ack``
flips ``processed_flag`` once rows have landed. This module adds the two
writes that make the flow a *conversation* rather than a one-way drain:

  * :func:`write_mei_request` — publish the list of top-level Major End
    Items we want, into the MEI table PDM reads. This is what starts the
    transaction; PDM explodes those MEIs and fills the staging tables.

  * :func:`signal_load_complete` — append a row to the control table
    saying we have finished consuming. PDM writes its own STARTED /
    COMPLETED rows there; ours is a third status so both sides can see
    where the cycle got to.

Every table, column, and status string is supplied by the caller. Nothing
about the PDM schema is baked in here — the same handlers serve any
site's naming, which is the whole reason they take this much payload.

Both handlers run their SQL inside ``ctx.run`` so Restate's journal
replays them exactly once if the worker dies mid-write. That matters more
here than for the ack: a duplicated MEI request could make PDM redo a
full load, and a duplicated completion row could make our own sensor
believe a cycle finished that did not.
"""
import os
from typing import Any, Dict, List, Optional

import oracledb
import restate

service = restate.Service(name="GenericOracleControlService")

# Oracle rejects an IN list / bulk bind beyond 1000 entries, and the ack
# handler learned the same lesson. Kept identical so both handlers fail
# (or don't) at the same scale.
CHUNK_SIZE = 1000


def _connect():
    """Open an Oracle connection from environment credentials.

    Deliberately re-read per call rather than cached at import: the worker
    hosts several services and a rotated secret should take effect on the
    next invocation, not the next pod restart.
    """
    dsn = os.environ.get("ORACLE_DSN")
    user = os.environ.get("ORACLE_USER")
    password = os.environ.get("ORACLE_PASSWORD")
    if not all([dsn, user, password]):
        raise ValueError(
            "Missing Oracle connection credentials in environment variables "
            "(ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD)."
        )
    return oracledb.connect(user=user, password=password, dsn=dsn)


def _require(payload: Dict[str, Any], *names: str) -> List[Any]:
    missing = [n for n in names if payload.get(n) in (None, "", [])]
    if missing:
        raise ValueError(
            f"Missing required payload fields: {', '.join(missing)}"
        )
    return [payload[n] for n in names]


@service.handler()
async def write_mei_request(ctx: restate.Context, payload: dict):
    """Publish the MEI list PDM should explode, into the MEI table.

    Payload:
      table_name    (required) — the MEI table, e.g. ``PDM_MEI_REQUEST``
      mei_column    (required) — column the MEIs are itemized in
      mei_values    (required) — list of MEI identifiers
      replace       (optional, default True) — clear the table first, so
                    the table always states the CURRENT request rather
                    than the union of every request ever made
      extra_columns (optional) — constant columns applied to every row
                    (a request id, a requested-at stamp, a load type)
    """
    table_name, mei_column, mei_values = _require(
        payload, "table_name", "mei_column", "mei_values"
    )
    replace = payload.get("replace", True)
    extra_columns: Dict[str, Any] = payload.get("extra_columns") or {}

    # De-duplicate while preserving order: PDM keys off these, and a
    # repeated MEI is at best wasted explosion work.
    seen = set()
    values = [
        v for v in mei_values
        if not (v in seen or seen.add(v))
    ]

    def _write():
        with _connect() as connection:
            with connection.cursor() as cursor:
                if replace:
                    # DELETE, not TRUNCATE: TRUNCATE is DDL in Oracle and
                    # commits implicitly, which would break the atomicity
                    # of clear-then-insert and leave PDM able to observe
                    # an empty request table mid-write.
                    cursor.execute(f"DELETE FROM {table_name}")

                cols = [mei_column, *extra_columns.keys()]
                placeholders = ", ".join(f":{c}" for c in cols)
                sql = (
                    f"INSERT INTO {table_name} ({', '.join(cols)}) "
                    f"VALUES ({placeholders})"
                )
                for i in range(0, len(values), CHUNK_SIZE):
                    rows = [
                        {mei_column: v, **extra_columns}
                        for v in values[i:i + CHUNK_SIZE]
                    ]
                    cursor.executemany(sql, rows)

            connection.commit()
        return len(values)

    return {"meis_written": await ctx.run("write_mei_request", _write)}


@service.handler()
async def signal_load_complete(ctx: restate.Context, payload: dict):
    """Append our completion row to the control table.

    PDM owns the STARTED / COMPLETED rows describing ITS side of the
    load. This adds a row with our own status value, so the control table
    reads as the full history of a cycle from both directions.

    Payload:
      table_name       (required) — the control table
      status_column    (required) — column holding the status string
      status_value     (required) — the value meaning "consumer done"
      timestamp_column (optional) — set to CURRENT_TIMESTAMP when given.
                       Strongly recommended: the cycle sensor orders
                       control rows by it to decide whether a COMPLETED
                       load has already been consumed.
      load_type_column (optional) / load_type (optional) — echo back the
                       FULL / DELTA the load was, so a reader does not
                       have to join our row to PDM's to know.
      extra_columns    (optional) — any further constant columns
    """
    table_name, status_column, status_value = _require(
        payload, "table_name", "status_column", "status_value"
    )
    timestamp_column = payload.get("timestamp_column")
    load_type_column = payload.get("load_type_column")
    load_type = payload.get("load_type")
    extra_columns: Dict[str, Any] = payload.get("extra_columns") or {}

    binds: Dict[str, Any] = {status_column: status_value}
    if load_type_column and load_type:
        binds[load_type_column] = load_type
    binds.update(extra_columns)

    def _write():
        with _connect() as connection:
            with connection.cursor() as cursor:
                cols = list(binds.keys())
                placeholders = [f":{c}" for c in cols]
                if timestamp_column:
                    # A DB-side timestamp, not a Python one: the control
                    # table is a shared clock between two systems and
                    # Oracle's is the only one both sides agree on.
                    cols.append(timestamp_column)
                    placeholders.append("CURRENT_TIMESTAMP")
                cursor.execute(
                    f"INSERT INTO {table_name} ({', '.join(cols)}) "
                    f"VALUES ({', '.join(placeholders)})",
                    binds,
                )
            connection.commit()

    await ctx.run("signal_load_complete", _write)
    return {"status_written": status_value}
