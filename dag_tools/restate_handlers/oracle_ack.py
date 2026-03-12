import os
import oracledb
import restate

service = restate.Service(name="GenericOracleAckService")

@service.handler()
async def mark_as_processed(ctx: restate.Context, payload: dict):
    """Generic Restate service for batch updating Oracle processed flags safely."""
    
    table_name = payload.get("table_name")
    pk_column = payload.get("pk_column")
    record_ids = payload.get("record_ids", [])

    if not all([table_name, pk_column, record_ids]):
        raise ValueError("Missing required payload fields: table_name, pk_column, or record_ids")

    def _execute_oracle_update():
        dsn = os.environ.get("ORACLE_DSN")
        user = os.environ.get("ORACLE_USER")
        password = os.environ.get("ORACLE_PASSWORD")

        if not all([dsn, user, password]):
            raise ValueError("Missing Oracle connection credentials in environment variables.")

        # Oracle Constraint: Chunk the record_ids into lists of 1,000 due to IN clause limits
        chunk_size = 1000
        chunks = [record_ids[i:i + chunk_size] for i in range(0, len(record_ids), chunk_size)]

        with oracledb.connect(user=user, password=password, dsn=dsn) as connection:
            with connection.cursor() as cursor:
                # Format string dynamically off passed context (secure within IDP platform boundary)
                base_sql = f"UPDATE {table_name} SET processed_flag = 'Y', sync_date = CURRENT_TIMESTAMP WHERE {pk_column} IN "
                
                for chunk in chunks:
                    bind_names = [f":id{i}" for i in range(len(chunk))]
                    bind_str = f"({','.join(bind_names)})"
                    sql = base_sql + bind_str
                    
                    bind_vars = {f"id{i}": val for i, val in enumerate(chunk)}
                    cursor.execute(sql, bind_vars)
            connection.commit()

    # Execute the update durably within a pure Restate side-effect context run block
    await ctx.run("update_oracle", _execute_oracle_update)
