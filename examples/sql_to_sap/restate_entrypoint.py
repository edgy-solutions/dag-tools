import asyncio
import hypercorn.asyncio
from hypercorn.config import Config
import restate

from dag_tools.restate_handlers.api_sync import service as api_service
from dag_tools.restate_handlers.oracle_ack import service as oracle_service

# Define the ASGI app with all services bound
app = restate.app(services=[api_service, oracle_service])

async def main():
    config = Config()
    config.bind = ["0.0.0.0:9080"]
    # Hypercorn provides HTTP/2 support required by Restate
    await hypercorn.asyncio.serve(app, config)

if __name__ == "__main__":
    asyncio.run(main())
