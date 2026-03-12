import logging
import os
from typing import Any, Dict, List, Optional
import restate
import httpx
from .config import SapInductionSettings
from .sap_client import SapODataClient
from .db_client import SapDbClient

logger = logging.getLogger(__name__)

# GLOBAL: Database client cache to prevent connection leaks
_db_client: Optional[SapDbClient] = None

def get_db_client() -> SapDbClient:
    """Returns a singleton Database Client for the worker."""
    global _db_client
    if _db_client is None:
        # Use POSTGRES_DSN for the local outbox as requested
        dsn = os.environ.get("POSTGRES_DSN") or os.environ.get("SQLSERVER_DSN") or os.environ.get("SOURCE__MSSQL__CREDENTIALS")
        if not dsn:
            raise ValueError("Missing database DSN (POSTGRES_DSN, SQLSERVER_DSN or SOURCE__MSSQL__CREDENTIALS)")
        _db_client = SapDbClient(dsn)
    return _db_client

# Create the service definition
service = restate.Service(name="SapInductionService")

@service.handler()
async def execute_induction(ctx: restate.Context, record: Dict[str, Any]) -> Dict[str, Any]:
    """Workflow to process a staged record and perform SAP induction via a State Machine."""
    
    # Load configuration
    settings = SapInductionSettings()
    
    # Initialize SAP Client
    sap_client = SapODataClient(
        base_url=settings.odata.base_url,
        username=settings.odata.auth.username,
        password=settings.odata.auth.password
    )
    
    db_client = get_db_client()

    # Extract metadata from the dispatch payload
    # Note: record is the payload from RestateApiSyncComponent
    source_table = record.get("source_table")
    pk_col = record.get("pk_column")
    pk_val = record.get("pk_value")
    row_data = record.get("row_data", {})
    
    # Extract operational data from row_data
    staged_pn = row_data.get(settings.fields.part_number)
    staged_serials = row_data.get(settings.fields.serial_number, [])
    current_retries = int(row_data.get(settings.fields.retry_count, 0))
    
    # Ensure serials is a list
    if not isinstance(staged_serials, list):
        staged_serials = [staged_serials] if staged_serials else []

    # NEW: Double Write Helper (Webhook Notification + Local DB Update)
    async def _notify_and_update(status: str, error_msg: Optional[str] = None, sap_doc: Optional[str] = None, retry_count: Optional[int] = None):
        
        # Step A: Webhook Notification to Source Application
        async def _push_webhook():
            payload = {
                "id": pk_val,
                "status": status,
                "error_message": error_msg,
                "sap_document": sap_doc
            }
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            if settings.callback_api.auth_header:
                # Assuming the config contains the full header or we default to Bearer
                if ":" in settings.callback_api.auth_header:
                    k, v = settings.callback_api.auth_header.split(":", 1)
                    headers[k.strip()] = v.strip()
                else:
                    headers["Authorization"] = f"Bearer {settings.callback_api.auth_header}"
            
            async with httpx.AsyncClient() as client:
                resp = await client.post(settings.callback_api.base_url, json=payload, headers=headers)
                resp.raise_for_status()
                logger.info(f"Successfully notified source via webhook for record {pk_val} with status {status}")

        await ctx.run(f"webhook-push-{status.lower()}", _push_webhook)

        # Step B: Local Database Update (Outbox)
        def _exec_local_update():
            db_client.update_record_status(
                table=source_table,
                pk_column=pk_col,
                pk_value=pk_val,
                status=status,
                columns_map=settings.source_system.columns.model_dump(),
                error_message=error_msg,
                sap_doc=sap_doc,
                retry_count=retry_count
            )
        
        await ctx.run(f"local-db-update-{status.lower()}", _exec_local_update)

    # Step 1: Resolve Part Number to SAP Material Number
    async def resolve_material():
        filters = f"{settings.fields.part_number} eq '{staged_pn}'"
        results = await sap_client.get_entities(settings.entity_sets.material_search, filters)
        if not results:
            raise ValueError(f"Material search help returned no results for PN: {staged_pn}")
        return results[0].get(settings.fields.material)

    try:
        sap_material = await ctx.run("resolve-material", resolve_material)
    except Exception as e:
        await _notify_and_update("ERROR", error_msg=str(e), retry_count=current_retries + 1)
        raise

    # Step 2: Find Quotation Header + Item for this material
    async def find_quotation():
        filters = f"{settings.fields.material} eq '{sap_material}'"
        results = await sap_client.get_entities(settings.entity_sets.quotation_item, filters)
        if not results:
            return None
        return {
            "quotation": results[0].get(settings.fields.quotation_key),
            "item": results[0].get(settings.fields.item_key)
        }

    quotation_data = await ctx.run("lookup-quotation", find_quotation)
    
    # The "PENDING" State: If no quotation is found, do not fail.
    if not quotation_data:
        logger.info(f"No quotation found for material {sap_material}. Moving to PENDING.")
        await _notify_and_update("PENDING")
        return {"status": "PENDING", "reason": "Quotation not found"}

    # Step 3 & 4: Execute Follow-On Action (Fan-Out for serial numbers)
    serials_to_process = staged_serials if staged_serials else [""]
    aggregated_messages = []
    has_errors = False
    error_summary = ""

    for idx, serial in enumerate(serials_to_process):
        async def call_induction():
            params = {
                settings.function_import.parameters.quotation: quotation_data["quotation"],
                settings.function_import.parameters.quotation_item: quotation_data["item"],
                settings.function_import.parameters.material_number: sap_material,
                settings.function_import.parameters.serial_number: str(serial)
            }
            logger.info(f"Calling SAP induction for serial: {serial}")
            return await sap_client.call_function_import(settings.function_import.name, params)

        run_name = f"induction-call-{serial}" if serial else f"induction-call-index-{idx}"
        messages = await ctx.run(run_name, call_induction)
        
        # Check for errors in SAP messages (Type 'E')
        if isinstance(messages, list):
            for m in messages:
                if m.get("Type") == "E":
                    has_errors = True
                    error_summary += f"{m.get('Message')}; "
            aggregated_messages.extend(messages)
        else:
            if messages.get("Type") == "E":
                has_errors = True
                error_summary += f"{messages.get('Message')}; "
            aggregated_messages.append(messages)

    # Step 5: Final State Evaluation
    if has_errors:
        new_retry_count = current_retries + 1
        if new_retry_count > settings.schedule.max_error_retries:
            logger.critical(f"Record {pk_val} reached max retries ({new_retry_count}). Marking FAILED_PERMANENTLY.")
            await _notify_and_update("FAILED_PERMANENTLY", error_msg=f"Max retries exceeded: {error_summary}", retry_count=new_retry_count)
        else:
            await _notify_and_update("ERROR", error_msg=error_summary, retry_count=new_retry_count)
        return {"status": "ERROR", "messages": aggregated_messages}
    
    # If successful, find the first document number returned (if any)
    sap_doc = next((m.get("Id") for m in aggregated_messages if m.get("Id")), None)
    
    await _notify_and_update("SUCCESS", sap_doc=sap_doc)
    
    return {
        "status": "SUCCESS",
        "processed_pn": staged_pn,
        "sap_material": sap_material,
        "quotation": quotation_data["quotation"],
        "quotation_item": quotation_data["item"],
        "sap_document": sap_doc,
        "messages": aggregated_messages
    }
