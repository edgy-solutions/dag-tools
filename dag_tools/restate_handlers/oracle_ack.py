import os
import oracledb
import restate

from . import db_retry

service = restate.Service(name="GenericOracleAckService")

@service.handler()
async def mark_as_processed(ctx: restate.Context, payload: dict):
    """Batch update Oracle processed flags, optionally append a stats row.

    Payload:
      table_name   (required) — table whose rows get processed_flag='Y'
      pk_column    (required) — PK column name to filter on
      record_ids   (required) — list of PK values to flip
      stats_table  (optional) — if set, insert one summary row here
      stats_columns (optional) — dict of extra cols to add to the stats insert
    """

    table_name = payload.get("table_name")
    pk_column = payload.get("pk_column")
    record_ids = payload.get("record_ids", [])
    stats_table = payload.get("stats_table")
    stats_columns = payload.get("stats_columns") or {}

    if not all([table_name, pk_column, record_ids]):
        raise ValueError("Missing required payload fields: table_name, pk_column, or record_ids")

    def _execute_oracle_update():
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
    # See dag_tools.restate_handlers.db_retry.
    await ctx.run(
        "update_oracle",
        db_retry.terminal_on_damage(_execute_oracle_update),
        **db_retry.run_options(),
    )
