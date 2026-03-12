import os
import httpx
import oracledb
import restate

service = restate.Service(name="GenericApiSyncService")

@service.handler()
async def process_record(ctx: restate.Context, payload: dict):
    """Generic Restate service for sync data outward to REST APIs securely per row."""
    
    api_path = payload.get("api_path")
    source_table = payload.get("source_table")
    pk_column = payload.get("pk_column")
    pk_value = payload.get("pk_value")
    row_data = payload.get("row_data", {})

    if not all([api_path, source_table, pk_column, pk_value]):
        raise ValueError("Missing required payload fields: api_path, source_table, pk_column, or pk_value")

    # Step 1: Execute the API Call natively (durably wrapped against transient failure)
    def _execute_api_post():
        base_url = os.environ.get("API_BASE_URL", "").rstrip("/")
        api_key = os.environ.get("API_KEY")
        
        if not base_url:
            raise ValueError("Missing API_BASE_URL in environment variables.")

        headers = {"Content-Type": "application/json"}
        if api_key:
             headers["Authorization"] = f"Bearer {api_key}"
             
        # Optional: You could inject a pure dict payload mapper right here against `row_data` if needed
        # row_data = my_mapper(row_data)

        # Using sync requests since this runs exclusively in an isolated sync context node execution block
        import requests
        response = requests.post(f"{base_url}{api_path}", json=row_data, headers=headers)
        response.raise_for_status()
        
        # Typically systems return an upstream correlation ID; safely parse it generic to standard schemas
        res_json = response.json()
        return str(res_json.get("id") or res_json.get("external_id") or "ACK_RECEIVED")

    api_id = await ctx.run("sap_api_post", _execute_api_post)

    # Step 2: Write back natively to SQL Server establishing the two-phase commit ack durability
    def _execute_sqlserver_ack():
        import pyodbc
        
        dsn = os.environ.get("SQLSERVER_DSN")
        if not dsn:
            raise ValueError("Missing SQLSERVER_DSN connection credentials in environment variables.")

        with pyodbc.connect(dsn) as connection:
            with connection.cursor() as cursor:
                sql = f"UPDATE {source_table} SET sync_status = 'COMPLETED', external_id = ? WHERE {pk_column} = ?"
                cursor.execute(sql, api_id, pk_value)
            connection.commit()

    await ctx.run("update_sqlserver", _execute_sqlserver_ack)
    
    # Step 3: Embed durability correlation back natively to Restate KV
    ctx.set(f"last_sync_{pk_value}", api_id)
