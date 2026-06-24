"""Registry sidecar for Dagster user-deployments.

A Dagster user-deployment uses this library to publish its own asset
metadata to the iagent domain-broker registry. The registry then
serves as the URN → physical-routing source-of-truth for data-fetching
clients (CortexDataClient, central-gateway).

The sidecar pattern keeps the registry *passive* — it holds whatever
user-deployments tell it about — and keeps every user-deployment
self-describing: the place that knows about an asset is the place that
materializes it. No central registry needs to be reconfigured when a
new asset appears; the deployment that owns the asset publishes it on
startup.

## Usage

In your Dagster code-location's ``definitions.py``::

    from dagster import Definitions
    from pathlib import Path
    from dagster.components import build_component_defs
    from dag_tools.sidecar import publish_to_registry_at_startup

    defs = build_component_defs(Path(__file__).parent / "components")

    # Publish this code-location's assets to the iagent registry on
    # module import. No-op if MESH_REGISTRY_URL is unset (local dev /
    # tests / non-iagent contexts).
    publish_to_registry_at_startup(defs, location="pub-tools")

That's it. The sidecar reads the Dagster ``Definitions``, extracts a
record per asset via ``dag_tools.inventory.extract_records``, derives
a stable URN, and POSTs each one to
``${MESH_REGISTRY_URL}/api/v1/admin/register_asset``.

## URN derivation order

1. ``asset.tags["datahub/urn"]`` if explicitly set.
2. ``record.urn`` if the inventory extractor populated it from the
   ``datahub-lineage`` component sidecar tag.
3. Fallback: ``urn:li:dataset:(urn:li:dataPlatform:dagster,<asset_key>,PROD)``
   — deterministic; same convention as the legacy domain-broker
   built-in for assets without explicit URN tags.

## Configuration

- ``MESH_REGISTRY_URL`` (env): the domain-broker base URL, e.g.
  ``http://iagent-domain-broker:8000``. When unset, publishing is
  skipped silently — the sidecar never breaks a user-deployment that
  isn't integrating with iagent.
- ``MESH_REGISTRY_TIMEOUT_SEC`` (env, default 10): per-call HTTP
  timeout. Failures are logged at WARNING and the loop continues —
  one asset's registration failure doesn't block others.

## Idempotence

The registry overwrites by URN on each register call, so re-running
on every code-location boot is safe and intended. That gives the
"latest write wins" semantics most useful when a code-location
re-deploys with updated asset metadata.
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, List, Optional

import httpx

if TYPE_CHECKING:
    from dagster import Definitions  # type: ignore[import-not-found]

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SEC = 10.0


def publish_to_registry(
    defs: "Definitions",
    *,
    broker_url: str,
    location: str,
    timeout_sec: Optional[float] = None,
) -> dict:
    """Publish all of a code-location's assets to the registry.

    Returns a small summary dict ``{"published": N, "skipped": M,
    "errors": K}`` for the caller's logs / health endpoints.

    Failures are caught per-asset; one bad asset doesn't block the
    rest. Errors are logged at WARNING.

    Parameters
    ----------
    defs
        The Dagster ``Definitions`` to publish.
    broker_url
        Base URL of the iagent domain-broker (e.g.
        ``http://iagent-domain-broker:8000``).
    location
        Human-readable code-location name. Stored on each record so
        operators can answer "which deployment registered this URN?"
        without re-deriving from the URN's data-platform segment.
    timeout_sec
        Per-call HTTP timeout. Defaults to ``MESH_REGISTRY_TIMEOUT_SEC``
        env or 10 seconds.
    """
    if timeout_sec is None:
        timeout_sec = float(os.getenv("MESH_REGISTRY_TIMEOUT_SEC", _DEFAULT_TIMEOUT_SEC))

    try:
        from dag_tools.inventory import extract_records
    except Exception as exc:
        logger.warning("dag_tools.inventory not importable; sidecar skipped: %s", exc)
        return {"published": 0, "skipped": 0, "errors": 1}

    records = extract_records(defs, location=location)
    if not records:
        logger.info("No assets in %s — nothing to publish.", location)
        return {"published": 0, "skipped": 0, "errors": 0}

    published = 0
    skipped = 0
    errors = 0
    endpoint = f"{broker_url.rstrip('/')}/api/v1/admin/register_asset"

    with httpx.Client(timeout=timeout_sec) as client:
        for record in records:
            urn = _derive_urn(record)
            if not urn:
                logger.warning(
                    "Skipping asset %s — no URN derivable.", record.asset_key,
                )
                skipped += 1
                continue
            try:
                payload = {
                    "urn": urn,
                    "asset_record": record.model_dump(mode="json"),
                }
                resp = client.post(endpoint, json=payload)
                resp.raise_for_status()
                published += 1
            except Exception as exc:
                logger.warning(
                    "Failed to publish %s (%s) to %s: %s",
                    urn, ".".join(record.asset_key), endpoint, exc,
                )
                errors += 1

    logger.info(
        "Sidecar publish from %s → %s: %d published, %d skipped, %d errors.",
        location, endpoint, published, skipped, errors,
    )
    return {"published": published, "skipped": skipped, "errors": errors}


def publish_to_registry_at_startup(
    defs: "Definitions",
    *,
    location: str,
    env_var: str = "MESH_REGISTRY_URL",
) -> Optional[dict]:
    """One-call convenience for the bottom of a ``definitions.py``.

    Reads the broker URL from ``MESH_REGISTRY_URL`` (overridable via
    ``env_var``). When unset, returns ``None`` and logs at DEBUG —
    local development and tests stay quiet by default.

    Catches and logs all exceptions; the sidecar must NEVER break a
    user-deployment's boot. A registry that's down or unreachable is
    an observability concern, not a code-location-startup blocker.
    """
    broker_url = os.getenv(env_var, "").strip()
    if not broker_url:
        logger.debug(
            "%s unset — sidecar publish skipped for %s.", env_var, location,
        )
        return None

    try:
        return publish_to_registry(
            defs, broker_url=broker_url, location=location,
        )
    except Exception as exc:
        # Defensive: keep this catchall so a sidecar bug never wedges
        # a Dagster code-location import. Errors here are operational,
        # not structural; surface them as logs, not exceptions.
        logger.warning(
            "Sidecar publish_to_registry crashed for %s (non-fatal): %s",
            location, exc,
        )
        return {"published": 0, "skipped": 0, "errors": 1}


def _derive_urn(record) -> Optional[str]:
    """URN derivation per the module docstring.

    1. ``tags["datahub/urn"]`` if explicitly set on the asset.
    2. ``record.urn`` populated by the datahub-lineage extractor.
    3. Fallback deterministic URN from the asset key.
    """
    tags = record.tags or {}
    explicit = tags.get("datahub/urn")
    if explicit:
        return str(explicit)
    if record.urn:
        return str(record.urn)
    if record.asset_key:
        key_str = ".".join(record.asset_key)
        return f"urn:li:dataset:(urn:li:dataPlatform:dagster,{key_str},PROD)"
    return None


__all__ = [
    "publish_to_registry",
    "publish_to_registry_at_startup",
]
