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
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Tuple

import oracledb
import restate

from . import db_retry

service = restate.Service(name="GenericOracleControlService")

# Oracle rejects an IN list / bulk bind beyond 1000 entries, and the ack
# handler learned the same lesson. Kept identical so both handlers fail
# (or don't) at the same scale.
CHUNK_SIZE = 1000


def _connect():
    """Open an Oracle connection from environment credentials.

    Read per call rather than cached at import, because the worker hosts
    several services and the value should not be frozen in one of them.

    What that does NOT buy, and an earlier version of this docstring
    claimed it did: a rotated secret taking effect on the next
    invocation. ``os.environ`` is populated from the container's
    environment when the process starts, so a secret delivered as an
    environment variable is a snapshot and only a restart replaces it --
    which is how a worker ends up still presenting the old password while
    everything else in the deployment has the new one, and then retrying
    it until the account locks. A secret that really does update in place
    has to arrive as a *mounted file* and be read from that file here.

    So a credential this process cannot see now is one it will not see
    later either, and :class:`db_retry.MissingCredentials` says so to
    ``ctx.run``: it is a ``ValueError`` subclass, so nothing that caught
    the old error stops catching this one.
    """
    dsn = os.environ.get("ORACLE_DSN")
    user = os.environ.get("ORACLE_USER")
    password = os.environ.get("ORACLE_PASSWORD")
    if not all([dsn, user, password]):
        raise db_retry.MissingCredentials(
            "Missing Oracle connection credentials in environment variables "
            "(ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD)."
        )
    return oracledb.connect(user=user, password=password, dsn=dsn)


def _refuse(problems: List[str]) -> None:
    """Raise the one refusal, carrying every problem found.

    Every problem is collected rather than raised on the first one,
    because whoever is fixing the producer wants the list.
    """
    if problems:
        raise db_retry.MalformedPayload(
            "The payload cannot be acted on: " + "; ".join(problems) + "."
        )


def _require(payload: Any, *names: str) -> List[Any]:
    """Refuse a payload missing a field the handler needs.

    Raises ``db_retry.MalformedPayload`` -- a ``ValueError`` subclass, so
    nothing that caught the old error stops catching this one (AGENTS
    section 6) -- and it is raised **inside** the step. Raised outside
    one, as this helper used to be called, it left every bound behind:
    the *invocation* failed, and Restate retries an invocation that
    failed on a non-terminal error with no ``max_attempts`` anywhere, so
    a payload with a field missing was retried with backoff for the life
    of the worker and the journal never said what was wrong.

    It names the fields it refuses and **no value** from any of them.
    This message travels into the ``TerminalError``, from there into the
    journal and back to the caller, and a payload for these handlers is
    nothing but table and column names.
    """
    if not isinstance(payload, Mapping):
        raise db_retry.MalformedPayload(
            "The payload cannot be acted on: it is not a mapping of fields."
        )
    missing = [n for n in names if payload.get(n) in (None, "", [])]
    _refuse([f"{n} is required and was not given" for n in missing])
    return [payload[n] for n in names]


def _names_a_database_object(payload: Mapping, name: str) -> Optional[str]:
    """A problem if ``name`` is present and is not text, else ``None``.

    These fields are interpolated into the statement as identifiers, so
    a non-string produces SQL that fails at Oracle for a reason with
    nothing to do with the data -- and under a bounded retry it fails
    three times first.
    """
    value = payload.get(name)
    if value is None or isinstance(value, str):
        return None
    return f"{name} must be text; it names a database object"


def _validated_request(
    payload: Any,
) -> Tuple[str, Any, Optional[str], Any, Dict[str, Any], Dict[str, Any]]:
    """Read and refuse :func:`write_mei_request`'s payload. INSIDE the step.

    The dangerous case is ``mei_values`` arriving as a string. A string
    is truthy, sliceable and iterable, so the presence check above
    accepts one and :func:`build_request_rows` then binds it one
    *character* at a time -- an INSERT that runs, commits, and fills the
    request table with one row per character. PDM reads that table. It
    is the one failure here that reads as a success, so it is named
    apart from the empty case, which merely refuses.
    """
    table_name, mei_values = _require(payload, "table_name", "mei_values")
    key_column = payload.get("key_column") or payload.get("mei_column")
    replace = payload.get("replace", True)
    constants: Dict[str, Any] = (
        payload.get("constants") or payload.get("extra_columns") or {}
    )
    defaults: Dict[str, Any] = payload.get("defaults") or {}

    problems: List[str] = []
    if isinstance(mei_values, (str, bytes)):
        problems.append(
            "mei_values must be a list of identifiers; a string is not one, "
            "and inserting it would write one row per character"
        )
    elif isinstance(mei_values, Mapping) or not isinstance(mei_values, Sequence):
        problems.append("mei_values must be a list of identifiers")
    for name, block in (("constants", constants), ("defaults", defaults)):
        if not isinstance(block, Mapping):
            problems.append(f"{name} must be a mapping of columns")
    for name in ("table_name", "key_column", "mei_column"):
        problems.extend(filter(None, [_names_a_database_object(payload, name)]))
    _refuse(problems)

    return table_name, mei_values, key_column, replace, constants, defaults


def _validated_signal(
    payload: Any,
) -> Tuple[str, Optional[str], Dict[str, Any], Any]:
    """Read and refuse :func:`signal_load_complete`'s payload. INSIDE the step.

    Returns the table, the optional timestamp column, the binds and the
    status written, so the statement -- and the handler's own answer --
    are built from what was refused rather than from a second reading of
    the payload outside any step.
    """
    table_name, status_column, status_value = _require(
        payload, "table_name", "status_column", "status_value"
    )
    timestamp_column = payload.get("timestamp_column")
    load_type_column = payload.get("load_type_column")
    load_type = payload.get("load_type")
    extra_columns: Dict[str, Any] = payload.get("extra_columns") or {}

    problems: List[str] = []
    if not isinstance(extra_columns, Mapping):
        problems.append("extra_columns must be a mapping of columns")
    for name in (
        "table_name",
        "status_column",
        "timestamp_column",
        "load_type_column",
    ):
        problems.extend(filter(None, [_names_a_database_object(payload, name)]))
    _refuse(problems)

    binds: Dict[str, Any] = {status_column: status_value}
    if load_type_column and load_type:
        binds[load_type_column] = load_type
    binds.update(extra_columns)
    return table_name, timestamp_column, binds, status_value


def build_request_rows(
    values: List[Any],
    *,
    key_column: Optional[str] = None,
    constants: Optional[Dict[str, Any]] = None,
    defaults: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Turn the request list into uniform rows ready for executemany.

    A request row often needs more than one column: an identifier that
    varies per row, values that are the same on every row, and values that
    are usually one thing but occasionally overridden. Three sources cover
    all of it without the handler knowing what any column means:

      * ``values`` -- either bare scalars (paired with ``key_column``) or
        mappings, which are used as the row;
      * ``defaults`` -- applied per row, and OVERRIDABLE by the row;
      * ``constants`` -- applied per row and NOT overridable, so a value
        that must be uniform cannot be varied by editing the input list.

    Every row is normalised to the same column set, because
    ``executemany`` binds one statement across the whole batch: a row
    missing a column another row has would bind the wrong parameters.
    Missing values become None rather than being dropped.

    De-duplicated on the WHOLE ROW, never on the identifier alone. An
    identical row really is wasted work, so collapsing it is right; two
    rows that share an identifier and differ anywhere else are two
    different requests, and dropping the second one is not a saving but a
    silent wrong answer -- the request table would carry one of the two
    and the load downstream would read as a correct extraction of it.

    That it was silent is what made it worth fixing rather than
    documenting: there was no error, no warning and no count, so the
    first place the loss could be noticed was in the output.

    Uniform input is unaffected. Bare values under one set of defaults
    and constants produce identical rows, which collapse exactly as they
    did before.
    """
    constants = constants or {}
    defaults = defaults or {}

    rows: List[Dict[str, Any]] = []
    for value in values:
        if isinstance(value, dict):
            row = {**defaults, **value, **constants}
        else:
            if not key_column:
                raise db_retry.MalformedPayload(
                    "a scalar entry needs key_column to say which column it "
                    "belongs in, and none was given. Supply key_column, or "
                    "make the entries mappings."
                )
            row = {**defaults, key_column: value, **constants}

        if key_column:
            key = row.get(key_column)
            if key is None:
                raise db_retry.MalformedPayload(
                    "an entry of the request list has no value for key_column."
                )
        rows.append(row)

    if not rows:
        raise db_retry.MalformedPayload(
            "the request list resolved to zero rows"
        )

    # One column set across the batch, ordered deterministically so the
    # generated INSERT is stable run to run.
    all_columns = sorted({c for row in rows for c in row})
    normalised = [{c: row.get(c) for c in all_columns} for row in rows]

    # De-duplicate here rather than above, on the normalised row: the
    # comparison has to be over the column set the batch is actually
    # bound with, or a row that merely omits a column its neighbour
    # carries would compare unequal to the row it becomes.
    deduplicated: List[Dict[str, Any]] = []
    seen = set()
    for row in normalised:
        try:
            fingerprint = tuple(row[c] for c in all_columns)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
        except TypeError:
            # An unhashable value cannot be compared this way. Keeping
            # the row is the only safe direction: a duplicate that
            # survives is wasted work, and a distinct row that does not
            # is the silent loss this function stopped doing.
            pass
        deduplicated.append(row)
    return deduplicated


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
    def _write():
        # Read, refused and turned into rows INSIDE the step, so the
        # refusal is bounded by the step that carries it. See
        # _validated_request and db_retry.MalformedPayload.
        (
            table_name,
            mei_values,
            key_column,
            replace,
            constants,
            defaults,
        ) = _validated_request(payload)
        rows = build_request_rows(
            mei_values,
            key_column=key_column,
            constants=constants,
            defaults=defaults,
        )

        with _connect() as connection:
            with connection.cursor() as cursor:
                if replace:
                    # DELETE, not TRUNCATE: TRUNCATE is DDL in Oracle and
                    # commits implicitly, which would break the atomicity
                    # of clear-then-insert and leave the source able to
                    # observe an empty request table mid-write.
                    cursor.execute(f"DELETE FROM {table_name}")

                cols = list(rows[0].keys())
                placeholders = ", ".join(f":{c}" for c in cols)
                sql = (
                    f"INSERT INTO {table_name} ({', '.join(cols)}) "
                    f"VALUES ({placeholders})"
                )
                for i in range(0, len(rows), CHUNK_SIZE):
                    cursor.executemany(sql, rows[i:i + CHUNK_SIZE])

            connection.commit()
        return len(rows)

    return {
        "rows_written": await ctx.run(
            "write_mei_request",
            db_retry.terminal_on_damage(_write),
            **db_retry.run_options(),
        )
    }


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
    def _write():
        # Read and refused INSIDE the step, as above.
        table_name, timestamp_column, binds, status_value = _validated_signal(
            payload
        )

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

        # Returned so the handler answers with what the row actually
        # carried, rather than re-reading the payload outside the step.
        return status_value

    return {
        "status_written": await ctx.run(
            "signal_load_complete",
            db_retry.terminal_on_damage(_write),
            **db_retry.run_options(),
        )
    }
