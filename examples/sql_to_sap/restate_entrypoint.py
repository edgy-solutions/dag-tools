import uvicorn
from dag_tools.restate_handlers.api_sync import app as restate_handlers

if __name__ == "__main__":
    import restate
    # Serve the initialized GenericApiSyncService 
    # to the Restate Engine over port 9080
    uvicorn.run(restate.serve([restate_handlers]), host="0.0.0.0", port=9080)
