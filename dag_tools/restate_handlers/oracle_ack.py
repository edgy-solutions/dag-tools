import os
from collections.abc import Mapping, Sequence

import oracledb
import restate

from . import db_retry

service = restate.Service(name="GenericOracleAckService")


def _validated(payload):
    """Read the payload, or refuse it in a way no retry will repeat.

    Raises :class:`db_retry.MalformedPayload`, which the call site's
    guard turns into a ``restate.TerminalError`` on the first failure.
    This runs *inside* the step for exactly that reason: refusing a
    payload before the step leaves the guard behind, and an invocation
    that fails on a non-terminal error is retried with no bound at all.

    Every problem is collected rather than raised on the first one,
    because whoever is fixing the producer wants the list.

    The message names the fields it refused and carries **no value**
    from any of them: a payload names a table and a column, and this
    message travels into the journal and back to the caller.
    """
    if not isinstance(payload, Mapping):
        raise db_retry.MalformedPayload(
            "The payload is not a mapping, so none of the required "
            "fields (table_name, pk_column, record_ids) can be read."
        )

    problems = []

    def _text(name, *, required):
        value = payload.get(name)
        if value is None and not required:
            return None
        if not isinstance(value, str) or not value.strip():
            problems.append(f"{name} must be a non-empty string")
            return None
        return value

    table_name = _text("table_name", required=True)
    pk_column = _text("pk_column", required=True)
    stats_table = _text("stats_table", required=False)

    record_ids = payload.get("record_ids")
    # A string is truthy, sliceable and iterable, so the old presence
    # check accepted one and the chunker then bound it one CHARACTER at
    # a time -- an UPDATE that runs, commits and flips the wrong rows.
    # That is the one failure here that looks like a success, so it is
    # named separately from the empty case.
    if isinstance(record_ids, (str, bytes, bytearray)):
        problems.append(
            "record_ids must be a list of identifiers; a string is not one"
        )
    elif not isinstance(record_ids, Sequence):
        problems.append("record_ids must be a list of identifiers")
    elif not record_ids:
        problems.append("record_ids must not be empty")
        record_ids = None

    stats_columns = payload.get("stats_columns")
    if stats_columns and not isinstance(stats_columns, Mapping):
        problems.append("stats_columns must be a mapping of column to value")

    if problems:
        raise db_retry.MalformedPayload(
            "The payload cannot be acted on: " + "; ".join(problems) + "."
        )

    return table_name, pk_column, list(record_ids), stats_table, dict(stats_columns or {})


@service.handler()
async def mark_as_processed(ctx: restate.Context, payload: dict):
    """Batch update Oracle processed flags, optionally append a stats row.

    Payload:
      table_name   (required) — table whose rows get processed_flag='Y'
      pk_column    (required) — PK column name to filter on
      record_ids   (required) — non-empty list of PK values to flip
      stats_table  (optional) — if set, insert one summary row here
      stats_columns (optional) — dict of extra cols to add to the stats insert
    """

    def _execute_oracle_update():
        (
            table_name,
            pk_column,
            record_ids,
            stats_table,
            stats_columns,
        ) = _validated(payload)

        dsn = os.environ.get("ORACLE_DSN")
        user = os.environ.get("ORACLE_USER")
        password = os.environ.get("ORACLE_PASSWORD")

        if not all([dsn, user, password]):
            # A ValueError subclass, so nothing that caught the old error
            # stops catching this one. It is terminal rather than retried
            # because the container's environment is a snapshot taken when
            # the process started: a variable absent now is absent for the
            # life of the pod.
            raise db_retry.MissingCredentials(
                "Missing Oracle connection credentials in environment variables."
            )

        chunk_size = 1000
        chunks = [record_ids[i:i + chunk_size] for i in range(0, len(record_ids), chunk_size)]

        with oracledb.connect(user=user, password=password, dsn=dsn) as connection:
            with connection.cursor() as cursor:
                base_sql = f"UPDATE {table_name} SET processed_flag = 'Y', sync_date = CURRENT_TIMESTAMP WHERE {pk_column} IN "

                for chunk in chunks:
                    bind_names = [f":id{i}" for i in range(len(chunk))]
                    bind_str = f"({','.join(bind_names)})"
                    sql = base_sql + bind_str

                    bind_vars = {f"id{i}": val for i, val in enumerate(chunk)}
                    cursor.execute(sql, bind_vars)

                if stats_table:
                    stats_binds = {
                        "rows_acked": len(record_ids),
                        "source_table": table_name,
                    }
                    stats_binds.update(stats_columns)
                    cols = ", ".join(stats_binds.keys())
                    placeholders = ", ".join(f":{k}" for k in stats_binds.keys())
                    cursor.execute(
                        f"INSERT INTO {stats_table} ({cols}) VALUES ({placeholders})",
                        stats_binds,
                    )

            connection.commit()

    # Bounded, and terminal on the first failure for the errors where
    # retrying is what does the damage: an unbounded retry of a wrong
    # password spends the account's failed-login budget and locks it out.
    #
    # The payload is read INSIDE this step for the other half of the
    # same reason. A refusal before the step is outside every bound:
    # the invocation fails, and Restate retries an invocation that
    # failed on a non-terminal error for the life of the worker.
    # See dag_tools.restate_handlers.db_retry.
    await ctx.run(
        "update_oracle",
        db_retry.terminal_on_damage(_execute_oracle_update),
        **db_retry.run_options(),
    )
