"""Dagster Definitions assembly for the dag-tools user-deployment.

Single demo-mode switch (``DAG_TOOLS_DEMO_MODE``) selects which surface
this deployment exposes:

* ``on`` (``true`` / ``1`` / ``yes``) — registers the synthetic
  ``mesh_demo_customers`` dataset from ``mesh_demo_assets.py``. Used in
  sandbox / dev clusters where the bar-chart demo must remain
  exercised.

* ``off`` (default; anything else, including unset) — registers only
  the source-singleton surface (currently empty and waits for the
  owner to fill it in with code like Snowflake all-assets, etc.).
  Production-safe: with the toggle off and no singletons wired, the
  deployment registers zero assets — the honest production state for
  a mesh that has no globally-available source surfaces yet.

The toggle is **single-switch by design**: either demo OR singletons,
not both. Production overrides set ``DAG_TOOLS_DEMO_MODE=false`` to
keep synthetic data out of work clusters.

Orthogonally, an optional **Grist ingest** surface is merged in when
``DAG_TOOLS_GRIST_CONFIG`` points at a component config YAML (chart-
mounted). Disabled by default — no default Grist/Postgres connection
can be guessed, so with the env var unset the surface contributes
nothing. See ``_build_grist_defs``.

This file uses a flat ``Definitions(...)`` rather than the
``build_component_defs`` discovery API because the latter is
deprecated (breaking_version 0.2.0) and assumes a shallow
``<package>/components`` layout that doesn't match the
``dag_tools.user_deployment.*`` nesting. Direct Definitions is
the right shape going forward.
"""

import logging
import os
import re
from pathlib import Path
from typing import Any

from dagster import Definitions

logger = logging.getLogger(__name__)


_TRUTHY = {"1", "true", "yes", "on", "y", "t"}

# ``DAG_TOOLS_GRIST_CONFIG`` names a YAML file (mounted by the chart)
# holding the Grist ingest component config. Unset -> the Grist surface
# is disabled.
_GRIST_CONFIG_ENV = "DAG_TOOLS_GRIST_CONFIG"

# ``{{ env.NAME }}`` references inside the mounted YAML are resolved
# against the container's environment at load time, so secrets (tokens,
# passwords) stay in k8s Secrets / env and never sit in the ConfigMap.
_ENV_TEMPLATE = re.compile(r"\{\{\s*env\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


def _demo_mode_on() -> bool:
    """Read ``DAG_TOOLS_DEMO_MODE`` and translate to a boolean.

    Conservative: only the explicit truthy values flip the switch on.
    Unset / unknown values default to off — production safety.
    """
    return (os.getenv("DAG_TOOLS_DEMO_MODE") or "").strip().lower() in _TRUTHY


def _resolve_env_templates(obj: Any) -> Any:
    """Recursively replace ``{{ env.NAME }}`` in string leaves.

    A string that is *entirely* one reference resolves to the raw env
    value (so numeric/boolean-looking secrets round-trip as strings);
    references embedded in a larger string are substituted in place.
    Unset variables resolve to empty string with a warning — a missing
    secret should surface as a connection error, not a literal
    ``{{ env.X }}`` reaching the driver.
    """
    if isinstance(obj, dict):
        return {k: _resolve_env_templates(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_templates(v) for v in obj]
    if isinstance(obj, str):
        def _sub(match: "re.Match") -> str:
            name = match.group(1)
            val = os.getenv(name)
            if val is None:
                logger.warning("grist config references unset env var %s", name)
                return ""
            return val
        return _ENV_TEMPLATE.sub(_sub, obj)
    return obj


def _build_singleton_defs() -> Definitions:
    """Future source-singleton surface — currently empty.

    When the owner adds singleton modules (e.g. a Snowflake
    all-assets surface), each module exports ``build_defs() -> Definitions``
    and this function merges them. Pattern intentionally mirrors
    ``mesh_demo_assets.build_demo_defs()`` so the conventions stay
    uniform across this deployment.
    """
    # Placeholder — replace with merged Definitions once singletons land.
    return Definitions()


def _build_grist_defs() -> Definitions:
    """Load the Grist ingest component from the chart-mounted config.

    Disabled by default: with ``DAG_TOOLS_GRIST_CONFIG`` unset, or the
    file missing / empty / ``enabled: false``, this returns empty
    Definitions and imports nothing Grist-related (keeps code-location
    startup fast when the surface is off).

    Config file shape (either the component-YAML form or a flat
    ``attributes`` block, both supported)::

        enabled: true            # optional; default true when the file exists
        attributes:
          name: crm
          grist:    { host: ..., org: ..., token: "{{ env.GRIST_TOKEN }}" }
          postgres: { protocol: postgresql, host: ..., password: "{{ env.PG_PASSWORD }}", ... }
    """
    path = os.getenv(_GRIST_CONFIG_ENV)
    if not path:
        return Definitions()

    cfg_path = Path(path)
    if not cfg_path.is_file():
        logger.warning("%s=%s is not a file; Grist surface disabled", _GRIST_CONFIG_ENV, path)
        return Definitions()

    import yaml  # local import — only when a config is actually present

    try:
        doc = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("failed to parse Grist config %s: %s; surface disabled", path, exc)
        return Definitions()

    if isinstance(doc, dict) and doc.get("enabled") is False:
        logger.info("Grist config %s has enabled: false; surface disabled", path)
        return Definitions()

    # Accept the full component-YAML shape (`attributes:`) or a flat map.
    attributes = doc.get("attributes") if isinstance(doc, dict) and "attributes" in doc else doc
    if not isinstance(attributes, dict) or not attributes:
        logger.warning("Grist config %s has no attributes; surface disabled", path)
        return Definitions()

    attributes = _resolve_env_templates(attributes)
    # Drop non-component control keys if present at the top level.
    attributes = {k: v for k, v in attributes.items() if k != "enabled"}

    # Lazy import: pulls in pandas / connectorx / the SQL IO manager only
    # when the Grist surface is actually configured.
    from dag_tools.components.grist_ingest.component import GristIngestComponent

    try:
        component = GristIngestComponent(**attributes)
        return component.build_defs(None)
    except Exception as exc:  # noqa: BLE001
        logger.error("failed to build Grist ingest defs from %s: %s", path, exc)
        raise


def _build_datahub_defs() -> Definitions:
    """Register the global DataHub catalog sensor when configured.

    Gated on ``DATAHUB_SERVER`` (the GMS base URL, already set by the
    chart's ``userDeployments.*.codeLocation.env``). Unset -> no sensor,
    and nothing DataHub-related is imported.

    This is the deployment's ONLY catalog path. IO managers deliberately
    do not emit to DataHub themselves: an IO manager is bound per-asset
    and (for the mesh read facade) may be bound to assets another
    deployment owns, so catalog registration belongs at the
    materialization-event level, where "this deployment actually
    produced this" is unambiguous.

    ``DATAHUB_SENSOR_STATUS`` (``RUNNING``/``STOPPED``, default
    ``RUNNING``) controls whether the sensor is live on load. The
    upstream ``make_datahub_sensor`` defaults to STOPPED, which reads as
    a broken integration — the sensor exists but silently never fires —
    so we default it on wherever DataHub is deliberately configured.

    Failures are non-fatal. Catalog registration is observability; a bad
    DataHub URL or a missing plugin must not take the code location
    offline, because that would stop every materialization in the
    deployment.
    """
    server = os.getenv("DATAHUB_SERVER")
    if not server:
        return Definitions()

    # The sensor is built from acryl-datahub-dagster-plugin. Check it
    # explicitly: the component swallows its own ImportError, so without
    # this guard a missing plugin surfaces as a confusing NameError.
    try:
        import datahub_dagster_plugin  # noqa: F401
    except ImportError:
        logger.warning(
            "DATAHUB_SERVER=%s is set but acryl-datahub-dagster-plugin is not "
            "installed; skipping DataHub catalog sensor. Install the plugin to "
            "enable catalog registration.", server,
        )
        return Definitions()

    try:
        from dag_tools.components.datahub_lineage.component import (
            DatahubLineageComponent,
        )

        component = DatahubLineageComponent(
            datahub_config={"server": server},
            default_status=os.getenv("DATAHUB_SENSOR_STATUS", "RUNNING"),
        )
        defs = component.build_defs(None)
        logger.info("DataHub catalog sensor registered against %s", server)
        return defs
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Failed to build the DataHub catalog sensor (%s); continuing without "
            "it so materializations still run.", exc,
        )
        return Definitions()


def _build_combined_defs() -> Definitions:
    if _demo_mode_on():
        # Lazy import: only pull mesh_demo_assets (and polars, the arrow
        # IO manager) into the deployment's module graph when demo mode
        # is actually on. Cleaner blast-radius if a production import of
        # this module accidentally happens with the env var unset.
        from dag_tools.user_deployment.mesh_demo_assets import build_demo_defs

        demo = build_demo_defs()
        base = Definitions.merge(demo, _build_singleton_defs())
    else:
        base = _build_singleton_defs()

    # Grist and the DataHub catalog sensor are both orthogonal to the
    # demo/singleton switch — merge them in when configured (no-ops when
    # not). The catalog sensor observes whatever the surfaces above
    # materialize, so it goes last.
    return Definitions.merge(base, _build_grist_defs(), _build_datahub_defs())


defs = _build_combined_defs()
