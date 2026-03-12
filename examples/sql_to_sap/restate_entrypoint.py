import uvicorn
import restate
from dag_tools.restate_handlers.api_sync import service as api_service
from dag_tools.restate_handlers.oracle_ack import service as oracle_service

# Define the ASGI app with all services bound
app = restate.app(services=[api_service, oracle_service])

if __name__ == "__main__":
    # Serve the initialized services
    # to the Restate Engine over port 9080
    uvicorn.run(app, host="0.0.0.0", port=9080)
