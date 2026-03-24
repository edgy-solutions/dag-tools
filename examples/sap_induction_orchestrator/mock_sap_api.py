from fastapi import FastAPI, Request, Query
import logging

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mock-sap-api")

@app.get("/MaterialSearchSet")
async def material_search(request: Request):
    logger.info(f"MaterialSearchSet called with params: {request.query_params}")
    # Mocking a match for PN PO-1001 and PO-1002
    return {
        "d": {
            "results": [
                {"Material": "MAT-999001", "PartNumber": "PO-1001"},
                {"Material": "MAT-999002", "PartNumber": "PO-1002"}
            ]
        }
    }

@app.get("/QuotationItemSet")
async def quotation_item(request: Request):
    logger.info(f"QuotationItemSet called with params: {request.query_params}")
    return {
        "d": {
            "results": [
                {"Quotation": "QTN-5001", "Item": "10", "Material": "MAT-999001"},
                {"Quotation": "QTN-5002", "Item": "20", "Material": "MAT-999002"}
            ]
        }
    }

@app.post("/Z_SAP_INDUCTION_FUNC")
async def sap_induction(
    request: Request,
    Quotation: str = Query(...),
    QuotationItem: str = Query(...),
    MaterialNumber: str = Query(...),
    SerialNumber: str = Query(...)
):
    logger.info(f"Z_SAP_INDUCTION_FUNC called: QTN={Quotation}, Item={QuotationItem}, MAT={MaterialNumber}, SN={SerialNumber}")
    
    # Return SAP Success Structure
    return {
        "d": [
            {
                "Type": "S",
                "Id": f"DOC-{SerialNumber}",
                "Message": f"Successfully induced serial {SerialNumber}"
            }
        ]
    }

@app.post("/callback")
async def webhook_callback(request: Request):
    payload = await request.json()
    logger.info(f"--- [CALLBACK] Received State Update ---")
    logger.info(payload)
    logger.info(f"----------------------------------------")
    return {"status": "accepted"}

# Catch-all for other OData metadata/discovery calls
@app.get("/{path:path}")
async def discovery(path: str):
    logger.info(f"Discovery call to: /{path}")
    return {"d": {"EntitySets": ["MaterialSearchSet", "QuotationItemSet"]}}
