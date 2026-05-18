import asyncio

import hypercorn.asyncio
import restate
from hypercorn.config import Config

from dag_tools.restate_handlers.oracle_ack import service as oracle_ack_service

app = restate.app(services=[oracle_ack_service])


async def main():
    config = Config()
    config.bind = ["0.0.0.0:9080"]
    await hypercorn.asyncio.serve(app, config)


if __name__ == "__main__":
    asyncio.run(main())
