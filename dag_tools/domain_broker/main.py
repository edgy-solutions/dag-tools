import os
import json
import logging
import httpx
import boto3
from typing import Dict, Any, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Optional Dagster imports — the broker can boot without dagster
# installed (e.g. for a /health-only readiness path), but won't be
# able to load a real Definitions module.
#
# Note: we deliberately do NOT import
# ``dag_tools.components.datahub_lineage`` here even though it
# contains a URN converter; the broker derives URNs straight off
# ``AssetRecord.urn`` / ``AssetRecord.tags["datahub/urn"]``, and
# pulling the lineage component in transitively requires acryl-
# datahub + datahub-dagster-plugin — heavy deps no user-deployment
# image should be forced to carry just to participate in the mesh.
try:
    from dagster import Definitions, AssetKey
except ImportError:
    Definitions = None
    AssetKey = None

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CENTRAL_GATEWAY_URL = os.getenv("CENTRAL_GATEWAY_URL", "http://central-gateway.default.svc.cluster.local")
BROKER_URL = os.getenv("BROKER_URL", "http://domain-broker.team-a.svc.cluster.local")
DAGSTER_DEFS_MODULE = os.getenv("DAGSTER_DEFS_MODULE", "")

# In-memory registry of assets
LOCAL_ASSETS: Dict[str, Any] = {}

class ResolveRequest(BaseModel):
    urn: str

def _build_asset_info_from_record(record, io_manager=None) -> Dict[str, Any]:
    """Build the LOCAL_ASSETS value shape from a shared inventory ``AssetRecord``.

    When ``io_manager`` is provided AND it implements the mesh-publishing
    protocol (a ``physical_coordinates`` method that returns a routing
    ticket), the ticket is stored verbatim under ``_routing_ticket``.
    The ``/resolve`` endpoint short-circuits to that ticket when present,
    bypassing the fallback bucket/host placeholders entirely. This is
    the path every modern dag-tools IO manager uses (sql.py, delta.py,
    cortex_io_manager.py) to advertise real physical coordinates.

    For IO managers that don't implement the protocol — third-party
    ones, custom forks, or assets without an IO manager binding — we
    fall back to the placeholder bucket/host shape. Those entries
    won't actually resolve to working data; ``/resolve`` will hand
    back a ticket that the cortex data client can dispatch on, but
    the underlying physical_uri is decorative until the IO manager
    is upgraded to the protocol.
    """
    info: Dict[str, Any] = {
        "io_manager_key": record.io_manager_key or "io_manager",
        "io_manager_type": record.io_manager_family or "s3_parquet",
        "io_manager_class": record.io_manager_class,
        "metadata": dict(record.tags or {}),
    }

    # Mesh-publishing protocol: if the IO manager declares its own
    # routing ticket, take it verbatim. Failures here are non-fatal —
    # the broker degrades to the placeholder fallback rather than
    # refusing to load the asset.
    if io_manager is not None and hasattr(io_manager, "physical_coordinates"):
        try:
            ticket = io_manager.physical_coordinates(list(record.asset_key or []))
            if ticket:
                info["_routing_ticket"] = ticket
                return info
        except Exception as exc:
            logger.warning(
                "physical_coordinates() failed for %s: %s",
                record.asset_key,
                exc,
            )

    family = record.io_manager_family
    target_path = list(record.asset_key or [])
    if family in ("postgres", "clickhouse"):
        info["db_host"] = "db.local"
        info["schema"] = "public"
        info["table"] = target_path[-1] if target_path else "unknown"
    elif family in ("s3_iceberg", "s3_delta", "s3_parquet"):
        info["bucket"] = "my-data-lake"
        info["prefix"] = "/".join(target_path)
    return info


def extract_io_manager_info(defs: 'Definitions', asset_key: 'AssetKey') -> Dict[str, Any]:
    """Extracts IO Manager type + configuration for a specific asset.

    Now delegates IO manager classification to the shared
    ``dag_tools.inventory`` introspector — which uses an explicit FQN
    registry with MRO walking, replacing the legacy substring-matching
    that silently misclassified custom IO manager forks. The returned
    dict shape is unchanged so downstream consumers don't need updates.
    """
    if defs is None or asset_key is None:
        return {"io_manager_type": "s3_parquet"}

    try:
        from dag_tools.inventory import extract_records
    except Exception as e:
        logger.error(f"Failed to import dag_tools.inventory: {e}")
        return {"io_manager_type": "s3_parquet"}

    records = extract_records(defs)
    target_path = list(asset_key.path)
    record = next((r for r in records if r.asset_key == target_path), None)
    if not record:
        return {"io_manager_type": "s3_parquet"}
    return _build_asset_info_from_record(record)


def _materializable_asset_keys(defs):
    """Return the set of asset-key tuples this deployment can materialize.

    Used to keep external/source stubs (read handles for assets another
    deployment owns) out of the broker's advertisement sweep.

    Returns ``None`` when the split can't be determined — an older or
    newer Dagster without ``resolve_asset_graph().materializable_asset_keys``.
    Callers treat ``None`` as "don't filter", preserving prior behavior
    rather than silently advertising nothing: an over-advertising broker
    is a routing bug, but a broker that advertises *nothing* takes the
    whole domain offline. Fail toward the status quo, and log it.
    """
    try:
        graph = defs.resolve_asset_graph()
        return {tuple(k.path) for k in graph.materializable_asset_keys}
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Could not determine materializable assets (%s); advertising all "
            "assets. External/source stubs may be advertised.", exc,
        )
        return None


def load_dagster_definitions():
    """Load local Dagster definitions and populate LOCAL_ASSETS keyed by URN."""
    if not DAGSTER_DEFS_MODULE or not Definitions:
        logger.warning("No DAGSTER_DEFS_MODULE specified or Dagster not installed. Using mock assets.")
        LOCAL_ASSETS["my_postgres_table"] = {
            "io_manager_type": "postgres",
            "db_host": "postgres.db.local",
            "db_port": 5432,
            "schema": "public",
            "table": "my_table"
        }
        LOCAL_ASSETS["my_s3_parquet"] = {
            "io_manager_type": "s3_parquet",
            "bucket": "my-data-lake",
            "prefix": "data/my_s3_parquet"
        }
        return

    import importlib
    try:
        module_name, attr_name = DAGSTER_DEFS_MODULE.split(":")
        module = importlib.import_module(module_name)
        defs = getattr(module, attr_name)

        if not isinstance(defs, Definitions):
            return

        try:
            from dag_tools.inventory import extract_records
        except Exception as e:
            logger.error(f"Failed to import dag_tools.inventory: {e}")
            return

        # One walk over Definitions — the shared inventory also derives the
        # URN sidecar via the same datahub converter, so we don't re-call it.
        records = extract_records(defs)

        # Advertise ONLY what this deployment can actually materialize.
        #
        # A Definitions routinely contains external/source stubs: read
        # handles for assets another deployment owns (declared so a local
        # asset can consume them through the mesh). Those look identical
        # to locally-produced assets in the inventory — same shape, same
        # io_manager_key — so without this filter the broker would
        # advertise a physical location for data it does not own and has
        # never written. The gateway stores routes last-writer-wins on a
        # short TTL, so two brokers claiming one asset key make the route
        # flap between the real owner and the phantom.
        #
        # ``materializable_asset_keys`` is Dagster's own executable/external
        # split, so this stays correct regardless of which IO manager is
        # bound — a structural guard rather than a per-IO-manager
        # convention.
        materializable = _materializable_asset_keys(defs)
        if materializable is not None:
            before = len(records)
            records = [
                r for r in records
                if tuple(r.asset_key or []) in materializable
            ]
            skipped = before - len(records)
            if skipped:
                logger.info(
                    "Skipping %d external/source asset(s) — not owned by this "
                    "deployment, so not advertised to the gateway.", skipped,
                )

        # Build a {io_manager_key: io_manager_instance} lookup so each
        # record can resolve its bound IO manager and (if the IO manager
        # implements the mesh-publishing protocol) request a real
        # routing ticket. ``defs.resources`` exposes the configured
        # resource instances — typically these are already pydantic
        # ConfigurableIOManager objects with their fields resolved.
        resources = getattr(defs, "resources", {}) or {}

        for record in records:
            urn = record.tags.get("datahub/urn") if record.tags else None
            if not urn:
                urn = record.urn
            if not urn:
                # Fallback deterministic URN generation.
                key_str = ".".join(record.asset_key)
                urn = f"urn:li:dataset:(urn:li:dataPlatform:dagster,{key_str},PROD)"
            io_manager = resources.get(record.io_manager_key) if record.io_manager_key else None
            LOCAL_ASSETS[urn] = _build_asset_info_from_record(record, io_manager=io_manager)

        logger.info(f"Loaded {len(LOCAL_ASSETS)} assets mapped by URN.")
    except Exception as e:
        logger.error(f"Failed to load Dagster definitions: {e}")

async def _register_once(client: httpx.AsyncClient) -> None:
    """Push this broker's URN list to the Central Gateway.

    The gateway holds (broker_url → URN list) in Redis with a TTL.
    The broker re-pushes on a loop so a single hiccup doesn't blow
    the routing table away — see ``_re_register_loop``.
    """
    payload = {
        "broker_url": BROKER_URL,
        "asset_urns": list(LOCAL_ASSETS.keys()),
    }
    resp = await client.post(
        f"{CENTRAL_GATEWAY_URL}/api/v1/internal/register",
        json=payload,
        timeout=10.0,
    )
    resp.raise_for_status()


async def _re_register_loop() -> None:
    """Re-register with the gateway every
    ``BROKER_REGISTER_INTERVAL_SEC`` seconds.

    Why: the gateway stores ``mesh_route:*`` keys in Redis with a TTL
    (5 minutes in the sandbox). One missed push past TTL silently
    drops every URN this broker serves until the next push. Re-pushing
    every 2 minutes (default) gives 2-3 push attempts per TTL window
    so a transient gateway hiccup never wipes the routing table.

    Non-fatal: per-iteration failures log at ERROR and the loop
    continues; the broker stays serving ``/resolve`` even if the
    gateway is temporarily unreachable. The next push attempt
    recovers automatically when the gateway comes back.
    """
    import asyncio
    interval = float(os.getenv("BROKER_REGISTER_INTERVAL_SEC", "120"))
    while True:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await _register_once(client)
                logger.info(
                    "re-registered %d assets with Central Gateway",
                    len(LOCAL_ASSETS),
                )
        except Exception as exc:
            logger.error("re-register failed: %s", exc)
        await asyncio.sleep(interval)


async def _startup_load_and_register() -> None:
    """Background task: load Dagster definitions, then drive the
    re-register loop.

    Runs outside the synchronous lifespan so a slow
    ``load_dagster_definitions`` (large Dagster Definitions, cold
    pip wheel cache, dlt/datahub imports) can't blow past hypercorn's
    lifespan startup timeout. The broker becomes Ready as soon as
    the FastAPI app is up; ``/health`` reports ``assets: 0`` until
    the load finishes, then jumps to the real count.

    Until the load finishes, ``/resolve`` returns 404 for any URN
    (LOCAL_ASSETS is still empty). That's the right shape: consumers
    can't be served before the broker actually knows what it serves.
    Consumers should be tolerant of a 404 → succeed retry pattern.
    """
    import asyncio
    try:
        # load_dagster_definitions is synchronous-blocking — keep it
        # off the event loop by running it in a worker thread so the
        # FastAPI app stays responsive to /health probes during the
        # load.
        await asyncio.to_thread(load_dagster_definitions)
        logger.info(
            "Loaded %d assets from Dagster definitions", len(LOCAL_ASSETS),
        )
    except Exception as exc:
        logger.error("load_dagster_definitions failed: %s", exc)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await _register_once(client)
            logger.info(
                "initial register: %d assets pushed to gateway",
                len(LOCAL_ASSETS),
            )
    except Exception as exc:
        logger.error("initial register failed (will retry in loop): %s", exc)

    await _re_register_loop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    # Kick off the load + register loop as a fire-and-forget
    # background task so lifespan startup completes immediately.
    # Hypercorn's lifespan startup timeout (60s, not CLI-configurable)
    # is too tight for the heavy Definitions imports a real
    # user-deployment carries — Dagster + dlt + datahub easily eat
    # 90-180s on cold start. Yielding before the load happens means
    # /health responds right away and k8s probes pass.
    task = asyncio.create_task(_startup_load_and_register())
    try:
        yield
    finally:
        task.cancel()
        logger.info("Domain Broker shutting down.")

app = FastAPI(lifespan=lifespan, title="Domain Broker")


@app.get("/health")
async def health():
    """Liveness / readiness probe.

    Returns 200 with the count of registered assets once
    ``load_dagster_definitions`` has populated ``LOCAL_ASSETS``.
    Kubernetes probes hit this endpoint to know when to start
    routing traffic — without it they fall back to a 404 and the
    pod never becomes Ready.
    """
    return {"status": "ok", "assets": len(LOCAL_ASSETS)}


@app.post("/api/v1/internal/resolve")
async def resolve_asset(request: ResolveRequest):
    """
    Called ONLY by the Central Gateway to resolve a DataHub URN into a physical routing ticket.
    """
    urn = request.urn

    if urn not in LOCAL_ASSETS:
        raise HTTPException(status_code=404, detail="URN not found in this domain's Dagster deployment.")

    asset_info = LOCAL_ASSETS[urn]

    # Mesh-publishing protocol short-circuit: when the IO manager
    # supplied its own routing ticket at load time, return it verbatim.
    # No STS minting, no placeholder fallback — the IO manager already
    # knows the real physical coordinates and credentials.
    ticket = asset_info.get("_routing_ticket")
    if ticket:
        return ticket

    io_type = asset_info.get("io_manager_type", "s3_parquet")

    if io_type in ["postgres", "clickhouse"]:
        host = asset_info.get("db_host", "localhost")
        port = asset_info.get("db_port", 5432)
        schema = asset_info.get("schema", "public")
        table = asset_info.get("table", urn.split(",")[-2] if "urn:li:dataset" in urn else urn)
        
        return {
            "source_type": io_type,
            "physical_uri": f"{io_type}://{host}:{port}/{schema}/{table}",
            "credentials": {
                "token": "read-only-db-token-123"
            }
        }
        
    elif io_type in ["s3_parquet", "s3_iceberg", "s3_delta"]:
        bucket = asset_info.get("bucket", "default-bucket")
        fallback_prefix = f"warehouse/{urn.split(',')[-2].replace('.', '/')}" if "urn:li:dataset" in urn else f"warehouse/{urn}"
        prefix = asset_info.get("prefix", fallback_prefix)
        role_arn = os.getenv("AWS_ASSUME_ROLE_ARN", "arn:aws:iam::123456789012:role/DataAccessRole")
        
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/{prefix}/*"]
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}"],
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": [f"{prefix}/*", f"{prefix}"]
                        }
                    }
                }
            ]
        }
        
        try:
            sts_client = boto3.client('sts')
            response = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=f"session-{urn.replace(':', '_').replace(',', '_')[:40]}",
                Policy=json.dumps(policy),
                DurationSeconds=3600
            )
            credentials = response['Credentials']
            
            return {
                "source_type": io_type,
                "physical_uri": f"s3://{bucket}/{prefix}",
                "credentials": {
                    "aws_access_key_id": credentials['AccessKeyId'],
                    "aws_secret_access_key": credentials['SecretAccessKey'],
                    "aws_session_token": credentials['SessionToken']
                }
            }
        except Exception as e:
            logger.error(f"Failed to mint STS token: {e}")
            # Fallback for environments without AWS configured
            return {
                "source_type": io_type,
                "physical_uri": f"s3://{bucket}/{prefix}",
                "credentials": {
                    "aws_access_key_id": "mock_access_key",
                    "aws_secret_access_key": "mock_secret_key",
                    "aws_session_token": "mock_session_token"
                }
            }
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported IO manager type: {io_type}")
