from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/{api_path:path}")
async def catch_all(api_path: str, request: Request):
    payload = await request.json()
    print(f"\n--- [SAP MOCK API] Received Payload at /{api_path} ---")
    print(payload)
    print("------------------------------------------------------\n")
    
    # Return a mock correlation ID to simulate typical API upsert behavior
    return {"id": f"sap_ext_{payload.get('PO_NUMBER', 'unknown')}", "status": "success"}
