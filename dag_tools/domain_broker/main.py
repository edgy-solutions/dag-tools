import os
import json
import logging
import httpx
import boto3
from typing import Dict, Any, List, Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
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

DEFINITIONS_ERROR: Optional[str] = None
"""Why the Definitions import failed, or None if it did not.

An empty ``LOCAL_ASSETS`` is ambiguous on its own: a deployment with no
mesh assets and a deployment whose import blew up look identical, and the
second used to report ``{"status": "ok", "assets": 0}`` and register an
empty URN list. The gateway then answered every lookup with
``404 No active domain broker found`` -- which reads as "that asset does
not exist" rather than "this broker never loaded". Recording the reason
is what lets /health and the registration path tell them apart."""

DEFINITIONS_LOADED: bool = False
"""True once the load has finished, successfully or not. Distinguishes
"still importing" (a real user deployment takes 90-180s cold) from
"finished with nothing"."""

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


def physical_urn_for(record, io_manager=None) -> Optional[str]:
    """The URN the CATALOG uses for this asset, derived the way the catalog derives it.

    THE BUG THIS EXISTS TO CLOSE. The broker previously took its routing key from
    ``record.urn``, whose derivation forces ``platform="dagster"``
    (``inventory/extractors.py``). That argument does not merely mislabel the platform:
    the converter selects the NAME LAYOUT from it, so ``dagster`` — absent from
    ``FILESYSTEM_PLATFORMS`` — takes the ``".".join(asset_key)`` branch. A key of
    ``minio-svc/publog-lake/publog/p_cage`` therefore became

        registered   ...(dagster, minio-svc.publog-lake.publog.p_cage, PROD)
        catalogued   ...(s3,      minio-svc.publog-lake/publog/p_cage, PROD)

    Not a spelling difference: the dotted form destroys the boundary between platform
    instance, bucket and key prefix that the key convention exists to encode, and the
    instance segment is load-bearing — one S3 path on two servers is two tables. Nothing
    a resolver produced could ever match a route registered that way, so every read 404'd
    at the gateway with a routing table that looked fully populated.

    THE RULE, quoted from the sensor that owns it (``datahub_lineage/component.py``):
    "An asset that materializes an S3 table and the S3 table are the same real-world
    object, so they get ONE catalog entity -- the physical one, named exactly as a
    DataHub source crawler would discover it. Assets with no physical location (a staging
    step, a source stub) keep a dagster-platform entity, because there is no table to
    point at."

    So this mirrors that resolution rather than inventing a third one. The sensor reads
    the platform the asset DECLARED via ``destination_name``; we read the ``source_type``
    off the routing ticket — deliberately the same string, per the SOURCE_TYPE comment in
    every IO manager ("used for BOTH the mesh routing ticket and the ``destination_name``
    the catalog sensor reads, so the two cannot drift"). Same vocabulary, same
    ``resolve_platform`` table, same ``FILESYSTEM_PLATFORMS`` layout list.

    Returns ``None`` when the asset has no physical location to name — no IO manager, no
    ticket, or a platform nobody declared. The caller then falls through to the existing
    dagster-form derivation, which is the correct identity for exactly that case.

    NOTE ON OVERRIDES: the sensor resolves through its component's ``platform_mappings``;
    the broker has no component config, so it resolves without them. A deployment that
    remaps a platform in YAML would drift again here. Wiring that config through is the
    remaining gap, and the reconciliation guard is what would catch it.
    """
    if io_manager is None or not hasattr(io_manager, "physical_coordinates"):
        return None
    try:
        from dag_tools.components.datahub_lineage.component import (
            asset_keys_to_dataset_urn_converter,
        )
        from dag_tools.components.datahub_lineage.platforms import (
            FILESYSTEM_PLATFORMS,
            UNKNOWN_PLATFORM,
            resolve_platform,
        )
    except Exception:  # datahub plugin absent — same posture as _derive_urn
        return None

    asset_key = list(record.asset_key or [])
    if not asset_key:
        return None
    try:
        ticket = io_manager.physical_coordinates(asset_key)
        platform = resolve_platform((ticket or {}).get("source_type"))
        if platform == UNKNOWN_PLATFORM:
            return None
        urn = asset_keys_to_dataset_urn_converter(
            asset_key,
            platform=platform,
            filesystem_platforms=list(FILESYSTEM_PLATFORMS),
        )
        if urn is None:
            return None
        return urn.urn() if hasattr(urn, "urn") else str(urn)
    except Exception as exc:  # noqa: BLE001 — never block asset load on identity derivation
        logger.warning(
            "physical URN derivation failed for %s: %s — falling back", asset_key, exc,
        )
        return None


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


def _split_defs_module(spec: str) -> tuple:
    """Split ``package.module:attribute``, and say so when it is malformed."""
    if ":" not in spec:
        raise ValueError(
            f"DAGSTER_DEFS_MODULE={spec!r} must be '<module>:<attribute>', "
            f"e.g. 'mfg.definitions:defs'. Without the attribute there is "
            f"nothing to look up."
        )
    module_name, _, attr_name = spec.partition(":")
    return module_name, attr_name


def _import_defs_module(module_name: str):
    """Import the defs module, distinguishing the two ways it can fail.

    "No module named 'mfg.definitions'" is ambiguous: the PACKAGE may have
    failed to import (a broken dependency somewhere inside it), or the
    package may be fine and simply have no submodule by that name. Those
    need completely different fixes, and the bare ModuleNotFoundError does
    not separate them -- especially when importing the package emits
    hundreds of lines of its own output first, which reads as success.
    """
    import importlib

    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        parent = module_name.rpartition(".")[0]
        if not parent or getattr(e, "name", None) != module_name:
            raise
        try:
            importlib.import_module(parent)
        except Exception:
            raise  # the parent is the real problem; let its error surface
        raise ModuleNotFoundError(
            f"No module named {module_name!r}. The parent package {parent!r} "
            f"imported fine, so the package is installed and its "
            f"dependencies resolve -- there is simply no {module_name!r} "
            f"submodule. Check DAGSTER_DEFS_MODULE against the real layout: "
            f"`python -c \"import {parent}, pkgutil; "
            f"print([m.name for m in pkgutil.iter_modules({parent}.__path__)])\"`",
            name=module_name,
        ) from e


def _definitions_attrs(module) -> list:
    """Public attributes on the module that ARE a Definitions.

    Turns "no attribute 'defs'" into "no attribute 'defs'; did you mean
    'definitions'?" without having to guess the convention.
    """
    found = []
    for name in dir(module):
        if name.startswith("_"):
            continue
        try:
            if isinstance(getattr(module, name), Definitions):
                found.append(name)
        except Exception:
            continue
    return sorted(found)


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
        module_name, attr_name = _split_defs_module(DAGSTER_DEFS_MODULE)
        module = _import_defs_module(module_name)

        try:
            defs = getattr(module, attr_name)
        except AttributeError:
            raise AttributeError(
                f"module {module_name!r} has no attribute {attr_name!r}. "
                f"Definitions-valued attributes found: "
                f"{_definitions_attrs(module) or '<none>'}"
            ) from None

        if not isinstance(defs, Definitions):
            # Previously a bare `return`, which produced zero assets and
            # not one line of explanation.
            raise TypeError(
                f"{DAGSTER_DEFS_MODULE} is a {type(defs).__name__}, not a "
                f"dagster Definitions. Definitions-valued attributes on "
                f"{module_name!r}: {_definitions_attrs(module) or '<none>'}"
            )

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
            # Resolve the IO manager FIRST: it is the only party that knows what this
            # asset physically is, and identity now depends on it. It used to be fetched
            # after the URN was already decided, so the one object holding the answer sat
            # in scope, unused, while the key was derived from a hardcoded platform.
            io_manager = resources.get(record.io_manager_key) if record.io_manager_key else None

            # Identity precedence, most authoritative first.
            #   1. An explicit datahub/urn tag — someone stated it; nothing overrides that.
            #   2. The PHYSICAL urn, derived as the catalog derives it (see physical_urn_for).
            #   3. record.urn / the dagster fallback — correct only for assets with no
            #      physical location, which is exactly when 2 declines to answer.
            urn = record.tags.get("datahub/urn") if record.tags else None
            if not urn:
                urn = physical_urn_for(record, io_manager)
            if not urn:
                urn = record.urn
            if not urn:
                # Fallback deterministic URN generation.
                key_str = ".".join(record.asset_key)
                urn = f"urn:li:dataset:(urn:li:dataPlatform:dagster,{key_str},PROD)"
            LOCAL_ASSETS[urn] = _build_asset_info_from_record(record, io_manager=io_manager)

        logger.info(f"Loaded {len(LOCAL_ASSETS)} assets mapped by URN.")
    except Exception as e:
        global DEFINITIONS_ERROR
        DEFINITIONS_ERROR = f"{type(e).__name__}: {e}"
        # exc_info, not just the message. This import pulls the whole user
        # deployment -- Dagster, dbt, dlt, datahub and their transitive
        # dependencies -- and the failure is usually an ImportError several
        # layers down. "No module named X" alone does not say WHICH package
        # asked for X, which is the only fact that identifies the culprit.
        logger.error(
            "Failed to load Dagster definitions: %s", e, exc_info=True,
        )

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
    global DEFINITIONS_ERROR, DEFINITIONS_LOADED
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
        DEFINITIONS_ERROR = f"{type(exc).__name__}: {exc}"
        logger.error("load_dagster_definitions failed: %s", exc, exc_info=True)
    finally:
        DEFINITIONS_LOADED = True

    if DEFINITIONS_ERROR:
        # Registering now would push an EMPTY urn list, which the gateway
        # stores as this broker's authoritative claim: "I own nothing."
        # Every lookup then 404s as "no active domain broker", i.e. the
        # asset appears not to exist. Staying silent lets the previous
        # registration age out on its TTL and keeps a healthy replica
        # authoritative, so a broken rollout degrades instead of erasing
        # the routing table.
        logger.error(
            "NOT registering with the gateway: the Dagster definitions "
            "failed to load (%s). This broker would otherwise advertise an "
            "empty asset list and every lookup for its assets would 404. "
            "Fix the import and restart; the re-register loop will not "
            "recover on its own.",
            DEFINITIONS_ERROR,
        )
        return

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

    Always 200, deliberately: this is the LIVENESS probe, and a failed
    import is not something a restart fixes -- returning non-200 would
    put the pod in a crash loop that hides the actual error. The state
    is reported in the body, and ``/ready`` is the one that fails.

    ``status`` is one of:
      * ``loading``  — the import is still running (90-180s is normal for
        a real user deployment carrying Dagster + dlt + datahub);
      * ``error``    — the import raised; ``definitions_error`` says how,
        and this broker has NOT registered with the gateway;
      * ``ok``       — loaded. ``assets: 0`` here genuinely means this
        deployment advertises nothing, rather than "something broke".
    """
    if not DEFINITIONS_LOADED:
        return {"status": "loading", "assets": len(LOCAL_ASSETS)}
    if DEFINITIONS_ERROR:
        return {
            "status": "error",
            "assets": len(LOCAL_ASSETS),
            "definitions_error": DEFINITIONS_ERROR,
            "registered": False,
        }
    return {"status": "ok", "assets": len(LOCAL_ASSETS)}


@app.get("/ready")
async def ready():
    """Readiness probe — 503 until the definitions are loaded cleanly.

    Separate from ``/health`` because the two answer different questions.
    Liveness asks "should this pod be restarted" (no: a bad import
    restarts into the same bad import). Readiness asks "should this pod
    be receiving traffic", and a broker that could not load its
    definitions should not: it has nothing truthful to resolve.
    """
    if not DEFINITIONS_LOADED:
        return JSONResponse(
            status_code=503,
            content={"status": "loading", "assets": len(LOCAL_ASSETS)},
        )
    if DEFINITIONS_ERROR:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "definitions_error": DEFINITIONS_ERROR},
        )
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
