import httpx
import sqlalchemy as sa
from dagster import asset, Config, AssetIn
from .transformation import transform_to_outbox

class RestateConfig(Config):
    postgres_dsn: str
    restate_ingress_url: str = "http://restate:8080/SapInductionService/execute_induction/send"

@asset(
    group_name="sap_induction",
    ins={"sap_outbox": AssetIn("sap_outbox")}
)
async def trigger_restate_induction(config: RestateConfig, sap_outbox):
    """
    Queries the SAP_OUTBOX table for records requiring processing and triggers the Restate service.
    """
    engine = sa.create_engine(config.postgres_dsn)
    
    # Query for records that are NEW, or in a retryable state (PENDING, ERROR)
    # Note: In production, you'd likely filter by retry_count as well.
    query = "SELECT * FROM staging.sap_outbox WHERE status IN ('NEW', 'PENDING', 'ERROR')"
    
    records_to_process = []
    with engine.connect() as conn:
        result = conn.execute(sa.text(query))
        records_to_process = [dict(row._mapping) for row in result]

    if not records_to_process:
        return {"status": "no_records_pending"}

    async with httpx.AsyncClient() as client:
        for record in records_to_process:
            # Prepare the payload for SapInductionService
            # We wrap the record data so the service knows which source and PK to update back.
            payload = {
                "source_table": "sap_outbox",
                "pk_column": "record_id",
                "pk_value": record["record_id"],
                "row_data": record
            }
            
            print(f"Triggering Restate for record {record['record_id']}...")
            resp = await client.post(
                config.restate_ingress_url,
                json=payload
            )
            resp.raise_for_status()

    return {"status": "triggered", "count": len(records_to_process)}
