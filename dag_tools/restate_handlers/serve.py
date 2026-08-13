"""Env-driven Restate worker entrypoint.

Run with:  python -m dag_tools.restate_handlers.serve

This is the single, generic entrypoint baked into the published
`restate-worker` image. Consumers never write their own entrypoint or
Dockerfile — they pick services and wire Restate with environment variables.

Environment variables
---------------------
RESTATE_SERVICES        Comma-separated service keys to host. Defaults to all
                        registered services. Valid keys: see SERVICE_REGISTRY.
RESTATE_BIND            host:port hypercorn binds to. Default: 0.0.0.0:9080
RESTATE_ADMIN_URL       If set, the worker self-registers with this Restate
                        admin endpoint on startup (e.g. http://restate:9070).
                        If unset, registration is skipped — register manually.
RESTATE_ADVERTISED_URI  The URL Restate uses to reach this worker
                        (e.g. http://restate-worker:9080). Required whenever
                        RESTATE_ADMIN_URL is set.
"""
import asyncio
import importlib
import logging
import os
import time

import httpx
import hypercorn.asyncio
import restate
from hypercorn.config import Config

logger = logging.getLogger("restate.serve")

# Logical key -> importable module that exposes a module-level `service`.
SERVICE_REGISTRY = {
    "oracle_ack": "dag_tools.restate_handlers.oracle_ack",
    "api_sync": "dag_tools.restate_handlers.api_sync",
    "api_call_plan": "dag_tools.restate_handlers.api_call_plan",
    "sap_induction": "dag_tools.restate_handlers.sap_induction",
}

_REGISTRATION_TIMEOUT_S = 120


def _selected_service_keys() -> list[str]:
    raw = os.environ.get("RESTATE_SERVICES", "").strip()
    if not raw:
        return list(SERVICE_REGISTRY)
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    unknown = [k for k in keys if k not in SERVICE_REGISTRY]
    if unknown:
        raise ValueError(
            f"Unknown RESTATE_SERVICES entries: {unknown}. "
            f"Valid keys: {sorted(SERVICE_REGISTRY)}"
        )
    return keys


def build_app():
    """Assemble the Restate ASGI app from the env-selected services.

    Services are imported lazily — selecting only `oracle_ack` never imports
    the SAP modules, so an unselected service's dependencies are never loaded.
    """
    keys = _selected_service_keys()
    services = []
    for key in keys:
        module = importlib.import_module(SERVICE_REGISTRY[key])
        services.append(module.service)
    logger.info("Hosting Restate services: %s", ", ".join(keys))
    return restate.app(services=services)


async def _self_register(admin_url: str, advertised_uri: str) -> None:
    """Best-effort registration of this worker as a Restate deployment.

    Retries until Restate admin is reachable. Uses force=true so each worker
    (re)start refreshes the deployment to the current code; Restate keeps
    prior revisions alive for in-flight invocations.

    Never raises: a worker that fails to self-register still serves HTTP and
    can be registered manually. With multiple replicas behind one Service URI,
    each pod re-registers the same URI — harmless but slightly wasteful; use a
    post-deploy Job instead if that churn matters.
    """
    endpoint = f"{admin_url.rstrip('/')}/deployments"
    payload = {"uri": advertised_uri, "force": True}
    deadline = time.monotonic() + _REGISTRATION_TIMEOUT_S

    async with httpx.AsyncClient() as client:
        while True:
            try:
                resp = await client.post(endpoint, json=payload, timeout=10.0)
                if resp.status_code < 300:
                    logger.info("Registered with Restate at %s", admin_url)
                    return
                logger.warning(
                    "Restate registration returned %s: %s",
                    resp.status_code, resp.text,
                )
            except Exception as e:
                logger.info("Restate admin not ready (%s), retrying...", e)

            if time.monotonic() > deadline:
                logger.error(
                    "Gave up self-registering after %ss. Register manually: "
                    "POST %s %s",
                    _REGISTRATION_TIMEOUT_S, endpoint, payload,
                )
                return
            await asyncio.sleep(3)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    config = Config()
    config.bind = [os.environ.get("RESTATE_BIND", "0.0.0.0:9080")]

    server = asyncio.create_task(hypercorn.asyncio.serve(build_app(), config))

    admin_url = os.environ.get("RESTATE_ADMIN_URL")
    if admin_url:
        advertised_uri = os.environ.get("RESTATE_ADVERTISED_URI")
        if not advertised_uri:
            raise ValueError(
                "RESTATE_ADMIN_URL is set but RESTATE_ADVERTISED_URI is not — "
                "the worker must know the URL Restate should call it on."
            )
        # Let hypercorn bind before Restate calls back for service discovery.
        await asyncio.sleep(2)
        asyncio.create_task(_self_register(admin_url, advertised_uri))
    else:
        logger.info(
            "RESTATE_ADMIN_URL not set — skipping self-registration; "
            "register this worker manually with POST /deployments."
        )

    await server


if __name__ == "__main__":
    asyncio.run(main())
