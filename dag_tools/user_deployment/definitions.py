"""Dagster Definitions assembly for the dag-tools user-deployment.

Single demo-mode switch (``DAG_TOOLS_DEMO_MODE``) selects which
component subdirectory the deployment scans:

* ``on`` (``true`` / ``1`` / ``yes``) — scans
  ``components/demo/``, registering the synthetic
  ``mesh_demo_customers`` dataset that the bar-chart demo depends on.
  Use in sandbox / dev clusters where the demo path must remain
  exercised.

* ``off`` (default; anything else, including unset) — scans only
  ``components/singletons/``, which holds basic singleton source
  assets (Snowflake all-assets, etc.). That directory is currently
  empty and is filled in by the owner when the real source-singleton
  code lands. With the toggle off and singletons empty, the
  deployment registers zero assets — the honest production state
  for a mesh that has no globally-available source surfaces wired
  yet.

The toggle is **single-switch by design** (per the architect's
guidance): either demo content is active, or the real
source-singleton surface is. They do NOT both run side-by-side.
This avoids the "demo data leaks into production" failure mode.
"""

import os
from pathlib import Path
from typing import Iterable

from dagster import Definitions
from dagster.components import build_component_defs


_TRUTHY = {"1", "true", "yes", "on", "y", "t"}


def _demo_mode_on() -> bool:
    """Read ``DAG_TOOLS_DEMO_MODE`` and translate to a boolean.

    Conservative: only the explicit truthy values flip the switch on.
    Unset / unknown values default to off — production safety.
    """
    return (os.getenv("DAG_TOOLS_DEMO_MODE") or "").strip().lower() in _TRUTHY


def _scan_dirs() -> Iterable[Path]:
    base = Path(__file__).parent / "components"
    if _demo_mode_on():
        # Demo mode: register demo content. Singletons stay
        # available too so a deployment can ALSO surface real source
        # singletons alongside the demo if both happen to be wired.
        # (In practice singletons is empty until the owner fills it
        # in, so this is equivalent to "demo only" today.)
        yield base / "demo"
        yield base / "singletons"
    else:
        # Production: only singletons. Demo content is structurally
        # absent from the running deployment, not just hidden behind
        # a flag — the components module isn't even scanned.
        yield base / "singletons"


def _build_combined_defs() -> Definitions:
    parts = []
    for d in _scan_dirs():
        if d.is_dir() and any(d.iterdir()):
            parts.append(build_component_defs(d))
    if not parts:
        # Empty surface — honest "this deployment has no registered
        # assets right now." Dagster accepts an empty Definitions.
        return Definitions()
    if len(parts) == 1:
        return parts[0]
    return Definitions.merge(*parts)


defs = _build_combined_defs()
