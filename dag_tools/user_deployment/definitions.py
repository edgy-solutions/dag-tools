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

This file uses a flat ``Definitions(...)`` rather than the
``build_component_defs`` discovery API because the latter is
deprecated (breaking_version 0.2.0) and assumes a shallow
``<package>/components`` layout that doesn't match the
``dag_tools.user_deployment.*`` nesting. Direct Definitions is
the right shape going forward.
"""

import os

from dagster import Definitions


_TRUTHY = {"1", "true", "yes", "on", "y", "t"}


def _demo_mode_on() -> bool:
    """Read ``DAG_TOOLS_DEMO_MODE`` and translate to a boolean.

    Conservative: only the explicit truthy values flip the switch on.
    Unset / unknown values default to off — production safety.
    """
    return (os.getenv("DAG_TOOLS_DEMO_MODE") or "").strip().lower() in _TRUTHY


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


def _build_combined_defs() -> Definitions:
    if _demo_mode_on():
        # Lazy import: only pull mesh_demo_assets (and polars,
        # CortexPolarsIOManager) into the deployment's module graph
        # when demo mode is actually on. Cleaner blast-radius if a
        # production import of this module accidentally happens with
        # the env var unset.
        from dag_tools.user_deployment.mesh_demo_assets import build_demo_defs

        demo = build_demo_defs()
        singletons = _build_singleton_defs()
        # Merge — Definitions.merge accepts an iterable. When singletons
        # is empty the merge is a no-op.
        return Definitions.merge(demo, singletons)
    return _build_singleton_defs()


defs = _build_combined_defs()
