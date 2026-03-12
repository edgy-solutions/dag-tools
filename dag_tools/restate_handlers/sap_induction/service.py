import logging
from typing import Any, Dict, List
import restate
from .config import SapInductionSettings
from .sap_client import SapODataClient

logger = logging.getLogger(__name__)

# Create the service definition
service = restate.Service(name="SapInductionService")

@service.handler()
async def execute_induction(ctx: restate.Context, record: Dict[str, Any]) -> Dict[str, Any]:
    """Workflow to process a staged record and perform SAP induction."""
    
    # Load configuration
    settings = SapInductionSettings()
    client = SapODataClient(
        base_url=settings.odata.base_url,
        username=settings.odata.auth.username,
        password=settings.odata.auth.password
    )

    # Extract source data from the incoming record
    # (Mapping internal staged names to logical needs)
    staged_pn = record.get(settings.fields.part_number)
    staged_serials = record.get(settings.fields.serial_number, [])
    
    # Ensure serials is a list
    if not isinstance(staged_serials, list):
        staged_serials = [staged_serials] if staged_serials else []

    # Step 1: Resolve Part Number to SAP Material Number
    async def resolve_material():
        filters = f"{settings.fields.part_number} eq '{staged_pn}'"
        results = await client.get_entities(settings.entity_sets.material_search, filters)
        if not results:
            raise ValueError(f"Material search help returned no results for PN: {staged_pn}")
        return results[0].get(settings.fields.material)

    sap_material = await ctx.run("resolve-material", resolve_material)
    
    # Step 2: Find Quotation Header + Item for this material
    async def find_quotation():
        filters = f"{settings.fields.material} eq '{sap_material}'"
        results = await client.get_entities(settings.entity_sets.quotation_item, filters)
        if not results:
            raise ValueError(f"No quotation item found for material: {sap_material}")
        # Take the first one (SAP logic usually dictates one valid quotation)
        return {
            "quotation": results[0].get(settings.fields.quotation_key),
            "item": results[0].get(settings.fields.item_key)
        }

    quotation_data = await ctx.run("lookup-quotation", find_quotation)
    
    # Step 3 & 4: Execute Follow-On Action (Fan-Out for serial numbers)
    # If no serial numbers, execute once with empty string.
    serials_to_process = staged_serials if staged_serials else [""]
    aggregated_messages = []

    for idx, serial in enumerate(serials_to_process):
        
        async def call_induction():
            params = {
                settings.function_import.parameters.quotation: quotation_data["quotation"],
                settings.function_import.parameters.quotation_item: quotation_data["item"],
                settings.function_import.parameters.material_number: sap_material,
                settings.function_import.parameters.serial_number: str(serial)
            }
            logger.info(f"Calling SAP induction for serial: {serial}")
            return await client.call_function_import(settings.function_import.name, params)

        # Durable run per serial
        run_name = f"induction-call-{serial}" if serial else f"induction-call-index-{idx}"
        messages = await ctx.run(run_name, call_induction)
        
        if isinstance(messages, list):
            aggregated_messages.extend(messages)
        else:
            aggregated_messages.append(messages)

    # Step 5: Return aggregated result
    return {
        "status": "completed",
        "processed_pn": staged_pn,
        "sap_material": sap_material,
        "quotation": quotation_data["quotation"],
        "quotation_item": quotation_data["item"],
        "messages": aggregated_messages
    }
