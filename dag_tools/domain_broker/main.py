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

# ── ADR-0044 — the broker mints; producers advertise coordinates ────────────
#
# A routing ticket may carry only credentials THIS BROKER minted for THIS
# request, scoped to the asset, expiring with the access window, unable to
# write. Producers keep their configured write credentials and go on using
# them to materialize assets — nothing about pipeline authoring changes. What
# changed is that a producer's credential must never reach a consumer.
#
# WHY THE BROKER AND NOT THE IO MANAGER. The broker already holds the
# authorization decision, so it is the right place to hold the power to mint
# against it. An IO manager runs in a pipeline pod; teaching it to mint would
# put assume-role privilege in every user deployment in the fleet — dozens of
# powerful credentials replacing one.
#
# ROLLOUT IS PER BACKEND (ADR-0044's capability matrix), so enforcement is
# keyed on source_type via _MINTERS below. A backend with a minter gets its
# echoed credential DROPPED and a fresh one minted per request. A backend
# without one is still on the old path — counted and reported as UNPROTECTED
# rather than quietly passed over, so "which columns are still red" is a
# number instead of a memory.

ECHOED_CREDENTIALS_DROPPED: Dict[str, int] = {}
"""producer io_manager_class -> count of tickets whose echoed credentials we
discarded. This is the ENUMERATION that decides when the transitional period
ends: the hard break (refusing tickets that carry credentials at all) lands
when this reads zero across a full materialization cycle, against a measured
population rather than a remembered list of IO managers."""

UNPROTECTED_SOURCE_TYPES: Dict[str, int] = {}
"""source_type -> asset count still advertising a producer credential because
no minter exists for that backend yet. Not a warning to be grepped for: it is
surfaced on /health so the remaining exposure is a reported number."""

UNADVERTISED_ASSETS: Dict[str, int] = {}
"""reason -> count of assets this deployment did NOT advertise.

AN ADVERTISED-BUT-UNREADABLE LOCATION IS WORSE THAN AN UNADVERTISED ASSET,
because the gateway routes consumers to it with full confidence. The IO
managers already act on that rule — ``arrow.py`` returns None rather than
guess. The broker used to violate it from the other end: an asset whose IO
manager implements no mesh-publishing protocol still got registered, with a
synthesized ``s3://default-bucket/warehouse/<dotted-key>`` URI and a
``dagster``-platform URN. A consumer resolving it received coordinates for a
bucket that does not exist.

That is not a naming problem to be fixed by matching the dagster URN. A
dagster-platform URN means "this asset has no physical location" — there is
nothing to hand a reader. So it is not advertised, and the REASON is counted
here, because "registered 104 assets" while none of them are readable is the
most expensive kind of green."""

NON_FQDN_HOSTS: Dict[str, int] = {}
"""advertised host -> count. A ticket is consumed somewhere else, so a
namespace-local hostname is a coordinate that means different things
depending on where the reader stands (ADR-0044). Reported, not refused —
refusing would take out cross-namespace reads that work today for consumers
that happen to share the producer's namespace."""


def _is_fqdn(host: str) -> bool:
    """A hostname a consumer in another namespace can resolve.

    Deliberately permissive: dotted names, localhost and bare IPs pass. The
    target is the specific failure we hit — a bare Kubernetes service name
    like ``minio-svc``, which resolves only inside the producer's namespace.
    """
    if not host:
        return False
    bare = host.split(":", 1)[0]
    if bare in ("localhost",) or bare.replace(".", "").isdigit():
        return True
    return "." in bare


def _s3_scope_from_uri(physical_uri: str) -> Optional[Dict[str, str]]:
    """Bucket and key prefix a minted credential must be confined to.

    Derived from the advertised URI rather than requiring producers to declare
    a ``scope`` first. That is what lets the broker protect assets published by
    IO managers that have NOT yet been upgraded: an old ticket still carries a
    usable ``physical_uri``, so the broker can mint correctly against it while
    discarding the credential the producer sent. Protection does not wait on
    the fleet.

    A producer that declares ``scope`` explicitly wins over this derivation —
    it knows things the URI does not (a dataset spanning prefixes, say).
    """
    if not physical_uri or not physical_uri.startswith("s3://"):
        return None
    remainder = physical_uri[len("s3://"):]
    bucket, _, prefix = remainder.partition("/")
    if not bucket:
        return None
    return {"bucket": bucket, "prefix": prefix.strip("/")}


def _sts_client(coordinates: Dict[str, Any]):
    """The broker's OWN minting identity — resolved explicitly, never ambiently.

    ADR-0044 put minting authority in the broker. It did not say where the
    broker's credentials come from, and the first implementation left
    ``boto3.client("sts")`` to the default credential chain — env
    ``AWS_ACCESS_KEY_ID``, ``~/.aws/credentials``, IMDS.

    THAT SILENTLY DOES NOT WORK IN THE DEPLOYMENT THIS SHIPS INTO. A domain-
    broker pod is configured with ``S3_ACCESS_KEY`` / ``S3_SECRET_KEY`` (the
    names the Dagster definitions module consumes), which boto3 has never heard
    of. The chain finds nothing, ``assume_role`` raises ``NoCredentialsError``,
    and every object-store read 503s — correct fail-closed behaviour reporting a
    CONFIGURATION problem in the vocabulary of an outage.

    It passed verification because the operator running the script had
    ``AWS_ACCESS_KEY_ID`` exported in their shell. The script's own docstring
    called that "the same chain the broker pod uses" — an assertion nobody had
    checked against a pod.

    So the identity is now NAMED, in precedence order, and its absence is a
    configuration error that says which variables to set:

      1. ``BROKER_STS_ACCESS_KEY_ID`` / ``BROKER_STS_SECRET_ACCESS_KEY`` — the
         purpose-named pair. Explicit wins, so a deployment can give the broker
         a minting identity distinct from whatever else the pod holds. That
         separation is the point of ADR-0044: the credential that writes assets
         and the one used to mint read tickets need not be the same.
      2. ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` — **dag-tools' own
         convention**, and the reason this rung matters most: it is what the IO
         managers themselves read (``user_deployment/mesh_demo_assets.py``,
         ``resources/duckdb.py``, and what ``delta.py`` writes into
         storage_options). THE BROKER RUNS THE SAME IMAGE AS THE DAGSTER USER
         DEPLOYMENT with a different command, so a deployment that already sets
         these for its IO managers has already configured the broker — no new
         wiring, which is the whole reason the same-image pattern is worth
         keeping.
      3. ``S3_ACCESS_KEY``/``S3_SECRET_KEY``, then
         ``MINIO_ACCESS_KEY``/``MINIO_SECRET_KEY`` — NEITHER is a dag-tools
         name. Both are the invincible-agent chart's, and it uses BOTH: the
         domain-broker deployment sets ``S3_*`` in an explicit env list while
         ``iagent-config`` (which the dagster-user-code deployment consumes via
         envFrom) sets ``MINIO_*``. Three names for one credential across one
         stack, and which one a broker sees depends on how its pod was wired.
         Accepted as COMPATIBILITY rungs so an already-deployed broker keeps
         working after upgrading — and so a broker moved onto the same envFrom
         as the deployment it mirrors keeps working too — rather than 503-ing
         until someone edits a chart.

         **Normalise on rung 2.** These rungs exist to absorb a naming mess
         that already shipped, not to bless it.
      4. the ambient chain — IRSA, instance profiles, mounted config. Last,
         because it is the rung that cannot be verified from inside this
         function, and trusting it unverified is what produced this bug.
    """
    kwargs: Dict[str, Any] = {
        "endpoint_url": coordinates.get("endpoint_url") or None,
        "region_name": coordinates.get("region") or "us-east-1",
    }
    for key_var, secret_var in (
        ("BROKER_STS_ACCESS_KEY_ID", "BROKER_STS_SECRET_ACCESS_KEY"),
        ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"),
        ("S3_ACCESS_KEY", "S3_SECRET_KEY"),
        ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"),
    ):
        key, secret = os.getenv(key_var), os.getenv(secret_var)
        if key and secret:
            kwargs["aws_access_key_id"] = key
            kwargs["aws_secret_access_key"] = secret
            break
    return boto3.client("sts", **kwargs)


def _mint_s3(scope: Dict[str, str], urn: str, coordinates: Dict[str, Any]) -> Dict[str, Any]:
    """`mint-sts` — a read-only, prefix-scoped, expiring credential.

    This is the construction the fallback path has always used; ADR-0044's fix
    is that the protocol path now reaches it instead of short-circuiting past
    it. The policy is built from the TICKET'S OWN SCOPE rather than a broad
    default, and the duration from the access window rather than a fixed hour.

    Raises on failure. There is deliberately no fallback to the producer's
    credential: a minting failure must fail the ticket, not silently reinstate
    the vulnerability under load. Fail closed.
    """
    bucket = scope["bucket"]
    prefix = scope.get("prefix", "")
    resource = f"arn:aws:s3:::{bucket}/{prefix}/*" if prefix else f"arn:aws:s3:::{bucket}/*"
    listable = [f"{prefix}/*", prefix] if prefix else ["*"]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            # GetObject ONLY. No PutObject, no DeleteObject, no multipart.
            # The asset's own bytes, nothing else, nothing written.
            {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [resource]},
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{bucket}"],
                "Condition": {"StringLike": {"s3:prefix": listable}},
            },
        ],
    }

    duration = int(os.getenv("BROKER_CREDENTIAL_TTL_SEC", "900"))
    role_arn = os.getenv("AWS_ASSUME_ROLE_ARN", "arn:aws:iam::123456789012:role/DataAccessRole")

    sts = _sts_client(coordinates)
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=f"session-{urn.replace(':', '_').replace(',', '_')[:40]}",
        Policy=json.dumps(policy),
        DurationSeconds=duration,
    )
    c = response["Credentials"]
    minted: Dict[str, Any] = {
        "aws_access_key_id": c["AccessKeyId"],
        "aws_secret_access_key": c["SecretAccessKey"],
        "aws_session_token": c["SessionToken"],
    }
    if coordinates.get("endpoint_url"):
        minted["aws_endpoint_url"] = coordinates["endpoint_url"]
    if coordinates.get("region"):
        minted["aws_region"] = coordinates["region"]
    return minted


# source_type -> (minter, scope-deriver). A backend ABSENT from this table has
# no minting implementation yet and stays on the producer-credential path,
# counted in UNPROTECTED_SOURCE_TYPES. Adding a row here is what turns a column
# of ADR-0044's capability matrix green — and turning it green is what makes
# per-user notebook access safe for that backend.
_MINTERS = {
    "s3_parquet": (_mint_s3, _s3_scope_from_uri),
    "s3_delta": (_mint_s3, _s3_scope_from_uri),
    "s3_iceberg": (_mint_s3, _s3_scope_from_uri),
}

# Keys inside a legacy ticket's ``credentials`` that are COORDINATES, not
# secrets. They must survive the strip: dropping the whole dict would take the
# endpoint with it and leave a ticket nothing can read.
_COORDINATE_KEYS = {
    "aws_endpoint_url": "endpoint_url",
    "aws_region": "region",
    "catalog_uri": "catalog_uri",
    "catalog_type": "catalog_type",
    "warehouse_uri": "warehouse_uri",
    "table_identifier": "table_identifier",
    "database": "database",
}


def _sanitize_ticket(ticket: Dict[str, Any], producer: Optional[str]) -> Dict[str, Any]:
    """Strip a producer credential out of an advertised ticket at LOAD time.

    Runs once per asset, at startup — which is exactly why it may not mint
    here. ``physical_coordinates()`` is a load-time call and its result is
    cached in ``LOCAL_ASSETS`` for the process lifetime, so a credential
    created at this point would be one credential shared by every caller,
    expiring an hour into a broker that goes on reporting healthy. Minting
    belongs in ``/resolve``; this function's job is to make sure there is
    nothing secret left in the cache for it to fall back on.

    Coordinates are preserved and promoted out of ``credentials`` — an
    endpoint URL is not a secret, and dropping it wholesale would leave a
    ticket no consumer could read.
    """
    clean = {k: v for k, v in ticket.items() if k != "credentials"}
    source_type = clean.get("source_type") or ""
    echoed = ticket.get("credentials") or {}

    # Promote non-secret coordinates the producer nested inside credentials.
    for src, dest in _COORDINATE_KEYS.items():
        if src in echoed and dest not in clean:
            clean[dest] = echoed[src]

    if source_type in _MINTERS:
        clean["mode"] = clean.get("mode") or "mint-sts"
        secrets_present = [k for k in echoed if k not in _COORDINATE_KEYS]
        if secrets_present:
            key = producer or "unknown"
            ECHOED_CREDENTIALS_DROPPED[key] = ECHOED_CREDENTIALS_DROPPED.get(key, 0) + 1
            logger.warning(
                "ADR-0044: dropped echoed credential(s) %s advertised by %s for %s. "
                "The broker mints per request; producers should stop returning "
                "'credentials' from physical_coordinates(). Counted in "
                "/health.echoed_credentials_dropped — the hard break lands when "
                "that reads zero.",
                sorted(secrets_present), key, clean.get("physical_uri"),
            )
    else:
        # No minter for this backend yet. The producer credential is still the
        # only way to read it, so it passes through — but it is REPORTED, not
        # silently tolerated, because it is live exposure.
        clean["mode"] = clean.get("mode") or "producer-credential-unprotected"
        clean["credentials"] = echoed
        if echoed:
            UNPROTECTED_SOURCE_TYPES[source_type] = (
                UNPROTECTED_SOURCE_TYPES.get(source_type, 0) + 1
            )

    # A ticket is consumed somewhere else — the hostname has to mean the same
    # thing there. Reported rather than refused; see NON_FQDN_HOSTS.
    #
    # ONLY hostname-bearing coordinates are checked. The authority component of
    # an ``s3://bucket/key`` URI is a BUCKET, not a host: reading `publog-lake`
    # as a namespace-local hostname would flag every S3 asset in the fleet and
    # bury the real finding under false positives. Database URIs
    # (``postgres://host:port/...``) do carry a host, so they are checked.
    candidates = [clean.get("endpoint_url")]
    physical_uri = str(clean.get("physical_uri") or "")
    if physical_uri and not physical_uri.startswith("s3://"):
        candidates.append(physical_uri)

    for candidate in candidates:
        if not candidate:
            continue
        authority = str(candidate).split("://")[-1].split("/")[0]
        host = authority.split(":", 1)[0]
        if host and not _is_fqdn(authority):
            NON_FQDN_HOSTS[host] = NON_FQDN_HOSTS.get(host, 0) + 1
            logger.warning(
                "ADR-0044: advertised host %r is not an FQDN. It resolves in this "
                "deployment's namespace and nowhere else, so any consumer in "
                "another namespace cannot read %s. Set the producer's endpoint to "
                "the .svc.cluster.local form.",
                host, clean.get("physical_uri"),
            )

    return clean

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
                # ADR-0044: whatever the producer advertised, no producer
                # credential is cached. What lands in LOCAL_ASSETS is
                # coordinates; the credential is minted per request in
                # /resolve. See _sanitize_ticket for why it cannot be minted
                # here.
                info["_routing_ticket"] = _sanitize_ticket(
                    ticket, producer=info.get("io_manager_class"),
                )
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


def _unadvertisable_reason(record, io_manager=None) -> str:
    """WHY this asset has no physical identity — named, not left to inference.

    ``physical_urn_for`` has four ways to decline and three of them used to
    ``return None`` in silence. The operator then saw a URN that said
    ``dagster`` and no explanation, and the natural (wrong) conclusion was that
    the NAME needed fixing. It does not: the name is a symptom of the asset
    having no readable location, and the causes want different fixes —
    installing a dependency is not the same job as binding an IO manager.

    Returned strings are stable keys for ``/health.adr0044.unadvertised``, so
    an operator can act on a count rather than reading the whole log.
    """
    if io_manager is None:
        return (
            f"no IO manager bound (io_manager_key="
            f"{record.io_manager_key!r}) — nothing can describe where it lives"
        )
    if not hasattr(io_manager, "physical_coordinates"):
        mod = type(io_manager).__module__
        return (
            f"{mod}.{type(io_manager).__name__} does not implement the "
            f"mesh-publishing protocol (no physical_coordinates). Either mix in "
            f"dag_tools.io_managers.MeshPublishable and define mesh_uri(), or use "
            f"one of dag-tools' own IO managers. NOTE THE MODULE in the name above "
            f"— a vendored copy of a dag-tools class has the same class name and "
            f"none of the protocol, which is easy to misread as the real thing"
        )
    try:
        from dag_tools.components.datahub_lineage.component import (  # noqa: F401
            asset_keys_to_dataset_urn_converter,
        )
    except Exception as exc:  # noqa: BLE001
        # THE WHOLE-DEPLOYMENT CASE. This one affects every asset uniformly and
        # is a packaging problem, not a modelling one — worth distinguishing
        # loudly, because "all 104 unadvertised" reads like a Dagster problem
        # and is actually a missing extra.
        return (
            f"datahub lineage plugin not importable ({type(exc).__name__}) — "
            f"install acryl-datahub + datahub-dagster-plugin in the broker image; "
            f"this affects EVERY asset in this deployment"
        )
    try:
        ticket = io_manager.physical_coordinates(list(record.asset_key or []))
    except Exception as exc:  # noqa: BLE001
        return f"physical_coordinates() raised {type(exc).__name__}: {exc}"
    if not ticket:
        return (
            f"{type(io_manager).__name__}.physical_coordinates() declined — the "
            f"output is not readable by a consumer (local filesystem, non-parquet "
            f"target, or a non-s3 uri_base)"
        )
    return (
        f"source_type {(ticket or {}).get('source_type')!r} maps to no known "
        f"catalog platform"
    )


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
    """Split ``module[:attribute]`` into ``(module, attribute or None)``.

    The attribute is OPTIONAL so the broker accepts the same thing the
    deployment is already configured with. A user deployment started as::

        dagster api grpc --package-name mfg

    has no ``mfg.definitions`` module at all -- Dagster imports the
    package and discovers the ``Definitions`` as an attribute on it. Making
    the broker demand ``mfg.definitions:defs`` meant hunting for a name
    Dagster never asks anyone to know, and produced
    "No module named 'mfg.definitions'" for a package that is perfectly
    fine.

    So ``DAGSTER_DEFS_MODULE=mfg`` now works, and matches
    ``--package-name mfg``. An explicit ``mfg:defs`` still wins when a
    module exposes more than one.
    """
    spec = spec.strip()
    if not spec:
        raise ValueError("DAGSTER_DEFS_MODULE is empty")
    module_name, _, attr_name = spec.partition(":")
    if not module_name:
        raise ValueError(
            f"DAGSTER_DEFS_MODULE={spec!r} has no module part; expected "
            f"'<module>' or '<module>:<attribute>'."
        )
    return module_name, (attr_name.strip() or None)


def _discover_definitions(module, module_name: str):
    """Find the single Definitions on a module, the way --package-name does.

    Exactly one is the ordinary case and needs no configuration. Zero and
    several both need the operator to act, so both say what was found
    rather than falling back to a guess -- picking one of several at
    random would advertise half a deployment and look like it worked.
    """
    found = _definitions_attrs(module)
    if len(found) == 1:
        logger.info(
            "Discovered Definitions %r on %r (no attribute given)",
            found[0], module_name,
        )
        return getattr(module, found[0])
    if not found:
        raise AttributeError(
            f"no dagster Definitions found on {module_name!r}. If the "
            f"Definitions lives in a submodule, name it explicitly: "
            f"DAGSTER_DEFS_MODULE={module_name}.<submodule>:<attribute>"
        )
    raise AttributeError(
        f"{module_name!r} exposes more than one Definitions ({found}); "
        f"say which: DAGSTER_DEFS_MODULE={module_name}:{found[0]}"
    )


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

        if attr_name is None:
            defs = _discover_definitions(module, module_name)
        else:
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
                # NO PHYSICAL IDENTITY — SO IT IS NOT ADVERTISED.
                #
                # This used to fall through to record.urn, which forces
                # platform="dagster" and the dotted ".".join(asset_key) layout.
                # That URN is not a spelling variant of the s3 one: a
                # dagster-platform URN means the asset HAS NO PHYSICAL LOCATION.
                # Registering it advertised a route whose ticket resolved to
                # s3://default-bucket/warehouse/<dotted-key> — a bucket that does
                # not exist — and, when STS minting failed, handed the consumer
                # MOCK CREDENTIALS that look real.
                #
                # A deployment could therefore report "Registered 104 assets",
                # pass every health check, and serve nothing readable. The
                # symptom surfaced as a URN-naming puzzle rather than "this
                # deployment publishes nothing", which is the expensive part.
                #
                # Refusing costs nothing a consumer could have used, and the
                # reason is counted so an operator sees WHY rather than a silent
                # gap between "assets loaded" and "assets advertised".
                reason = _unadvertisable_reason(record, io_manager)
                UNADVERTISED_ASSETS[reason] = UNADVERTISED_ASSETS.get(reason, 0) + 1
                logger.info(
                    "Not advertising %s — %s. It has no physical location a "
                    "consumer could read; a dagster-platform URN is not a "
                    "readable coordinate.",
                    ".".join(record.asset_key or []), reason,
                )
                continue

            LOCAL_ASSETS[urn] = _build_asset_info_from_record(record, io_manager=io_manager)

        if UNADVERTISED_ASSETS:
            logger.warning(
                "Advertising %d of %d asset(s). NOT advertised: %s. An "
                "advertised-but-unreadable location is worse than an "
                "unadvertised asset, so these are withheld rather than "
                "registered with placeholder coordinates.",
                len(LOCAL_ASSETS), len(records), dict(UNADVERTISED_ASSETS),
            )
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
    return {
        "status": "ok",
        "assets": len(LOCAL_ASSETS),
        # ADR-0044 posture, REPORTED AS NUMBERS rather than left in a log
        # nobody greps. `unprotected_source_types` is live exposure: assets
        # still advertising a producer credential because their backend has no
        # minter yet. `echoed_credentials_dropped` is the retirement counter —
        # the transitional period ends, and the hard break lands, when it reads
        # zero across a full materialization cycle.
        "adr0044": {
            "echoed_credentials_dropped": dict(ECHOED_CREDENTIALS_DROPPED),
            "unprotected_source_types": dict(UNPROTECTED_SOURCE_TYPES),
            "non_fqdn_hosts": dict(NON_FQDN_HOSTS),
        },
    }


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

    # Mesh-publishing protocol: the IO manager advertised COORDINATES at load
    # time (credentials already stripped by _sanitize_ticket). This is where
    # the credential is created — per request, scoped to this asset, expiring
    # with this access window.
    #
    # This used to `return ticket` verbatim, which is the defect ADR-0044
    # exists to close: a newer, faster path short-circuiting past the minting
    # the fallback below has always performed. The minting code was never
    # missing; this return statement was in front of it.
    ticket = asset_info.get("_routing_ticket")
    if ticket:
        source_type = ticket.get("source_type") or ""
        minter_entry = _MINTERS.get(source_type)

        if not minter_entry:
            # No minting implementation for this backend yet — it is still on
            # the producer's credential. Advertised as such rather than
            # dressed up as protected.
            return ticket

        minter, scope_from = minter_entry
        scope = ticket.get("scope") or scope_from(ticket.get("physical_uri", ""))
        if not scope:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"Cannot mint a scoped credential for {urn}: no 'scope' declared "
                    f"and none derivable from physical_uri "
                    f"{ticket.get('physical_uri')!r}."
                ),
            )

        try:
            credentials = minter(scope, urn, ticket)
        except Exception as exc:
            # FAIL CLOSED. Never fall back to a producer credential — that
            # would reinstate the vulnerability at exactly the moment the
            # system is under stress, which is when it is least likely to be
            # noticed.
            # NAME THE CONFIGURATION CASE. A broker with no minting identity
            # raises NoCredentialsError, which fail-closed reports as "could
            # not mint" — an outage sentence for what is actually one missing
            # env var. Diagnosing that from a 503 costs an afternoon.
            hint = ""
            if "credential" in f"{type(exc).__name__}{exc}".lower():
                hint = (
                    " The broker has NO MINTING IDENTITY: set "
                    "BROKER_STS_ACCESS_KEY_ID/BROKER_STS_SECRET_ACCESS_KEY (or "
                    "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or "
                    "S3_ACCESS_KEY/S3_SECRET_KEY) on the broker deployment."
                )
            logger.error(
                "ADR-0044: minting failed for %s (%s: %s) — refusing the ticket "
                "rather than falling back to a producer credential.%s",
                urn, type(exc).__name__, exc, hint,
            )
            raise HTTPException(
                status_code=503,
                detail="Could not mint a scoped credential for this asset." + hint,
            )

        return {**{k: v for k, v in ticket.items() if k != "scope"},
                "credentials": credentials}

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
            # Same explicit identity as the protocol path. This used to be a bare
            # boto3.client('sts'), which on a real broker pod finds no credentials
            # at all — see _sts_client for why the ambient chain is empty there.
            sts_client = _sts_client({"endpoint_url": asset_info.get("endpoint_url"),
                                      "region": asset_info.get("region")})
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
            # NO MOCK CREDENTIALS. This used to return aws_access_key_id
            # "mock_access_key" and friends "for environments without AWS
            # configured" — a ticket that is structurally perfect, passes every
            # shape check, and fails at read time with an opaque S3 error that
            # names nothing. The consumer cannot tell a minting failure from a
            # permissions problem from a missing object.
            #
            # It also contradicted the rule the protocol path already follows
            # (ADR-0044): a minting failure fails the TICKET. Two paths, two
            # postures, and the fail-open one was the one nobody was looking at.
            logger.error(
                "Failed to mint STS token for %s: %s — refusing the ticket. "
                "(Previously this returned mock credentials, which read as "
                "success and failed later somewhere else.)", urn, e,
            )
            raise HTTPException(
                status_code=503,
                detail=f"Could not mint a scoped credential for {urn}.",
            )
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported IO manager type: {io_type}")
