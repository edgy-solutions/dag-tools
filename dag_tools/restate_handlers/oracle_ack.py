import os
import oracledb
import restate

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
            raise ValueError("Missing Oracle connection credentials in environment variables.")

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

    await ctx.run("update_oracle", _execute_oracle_update)
