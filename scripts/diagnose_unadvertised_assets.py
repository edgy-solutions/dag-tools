#!/usr/bin/env python
"""Why is this deployment advertising `dagster` URNs instead of physical ones?

Run INSIDE the broker pod — it needs the same image, the same env, and the same
importable Definitions:

    kubectl cp scripts/diagnose_unadvertised_assets.py <ns>/<broker-pod>:/tmp/d.py
    kubectl exec -n <ns> <broker-pod> -- python /tmp/d.py
    kubectl exec -n <ns> <broker-pod> -- python /tmp/d.py board_mapping   # filter

A `dagster`-platform URN is not a misspelling of the `s3` one. It is what the
broker falls back to when it cannot establish a PHYSICAL identity, and it means
the asset has no location a consumer could read. `physical_urn_for` has four
ways to decline and (before this script) three of them were silent, so the
operator saw a wrong-looking name and no cause.

This walks the same four preconditions, per asset, and prints which one fails.
It changes nothing.
"""
from __future__ import annotations

import os
import sys

DEFS = os.getenv("DAGSTER_DEFS_MODULE", "")
needle = sys.argv[1] if len(sys.argv) > 1 else ""

print(f"DAGSTER_DEFS_MODULE = {DEFS!r}\n")

# ── Precondition 0: does the catalog converter import? ─────────────────────
# Whole-deployment failure mode. If this is broken, EVERY asset falls back to a
# dagster URN regardless of how well its IO managers are configured — a
# packaging problem wearing a data-modelling costume.
try:
    from dag_tools.components.datahub_lineage.component import (  # noqa: F401
        asset_keys_to_dataset_urn_converter,
    )
    from dag_tools.components.datahub_lineage.platforms import (  # noqa: F401
        FILESYSTEM_PLATFORMS, UNKNOWN_PLATFORM, resolve_platform,
    )
    print("[OK]   datahub lineage plugin imports")
except Exception as exc:
    print(f"[FAIL] datahub lineage plugin: {type(exc).__name__}: {exc}")
    print("       -> EVERY asset in this deployment will get a dagster URN.")
    print("       -> Fix: install acryl-datahub + datahub-dagster-plugin in the")
    print("          BROKER image (it needs them even though Dagster does not).")
    raise SystemExit(1)

from dag_tools.domain_broker import main as broker  # noqa: E402

# ── Load exactly what the broker loads ─────────────────────────────────────
module_name, attr_name = broker._split_defs_module(DEFS)
module = broker._import_defs_module(module_name)
defs = (getattr(module, attr_name) if attr_name
        else broker._discover_definitions(module, module_name))

from dag_tools.inventory import extract_records  # noqa: E402

records = extract_records(defs)
resources = getattr(defs, "resources", {}) or {}

print(f"\nresource keys in Definitions: {sorted(resources)}\n")
print(f"{len(records)} record(s) extracted\n")
print("=" * 78)

advertised = 0
reasons: dict[str, int] = {}

for record in records:
    key = ".".join(record.asset_key or [])
    if needle and needle not in key:
        continue

    io_key = record.io_manager_key
    io_manager = resources.get(io_key) if io_key else None

    tag_urn = (record.tags or {}).get("datahub/urn")
    physical = broker.physical_urn_for(record, io_manager)

    if tag_urn or physical:
        advertised += 1
        if needle:
            print(f"\n{key}")
            print(f"  ADVERTISED as: {tag_urn or physical}")
            print(f"  source       : {'datahub/urn tag' if tag_urn else 'physical_urn_for'}")
        continue

    # Declined. Walk the preconditions in the broker's own order.
    if io_manager is None:
        reason = f"no IO manager resolved for io_manager_key={io_key!r}"
        detail = ("The key is absent from Definitions(resources=...). Assets "
                  "written by dlt/dbt directly often have no dag-tools IO "
                  "manager at all — nothing then knows where the bytes are.")
    elif not hasattr(io_manager, "physical_coordinates"):
        reason = f"{type(io_manager).__name__} has no physical_coordinates()"
        detail = ("It does not implement the mesh-publishing protocol. Bind "
                  "ConfigurableArrowIOManager / ConfigurableDeltaIOManager / "
                  "ConfigurableDuckDBIOManager / ConfigurableSQLIOManager.")
    else:
        try:
            ticket = io_manager.physical_coordinates(list(record.asset_key or []))
        except Exception as exc:
            ticket, reason = None, f"physical_coordinates() raised {type(exc).__name__}: {exc}"
            detail = "The IO manager errored deriving coordinates."
        else:
            if not ticket:
                reason = f"{type(io_manager).__name__}.physical_coordinates() returned None"
                detail = ("It DECLINED on purpose — local filesystem, a "
                          "non-parquet target, or a uri_base that is not s3://. "
                          "Check the IO manager's fs/uri_base config.")
            else:
                st = ticket.get("source_type")
                plat = resolve_platform(st)
                reason = f"source_type={st!r} -> platform {plat!r}"
                detail = ("The ticket is fine but its source_type maps to no "
                          "catalog platform, so no physical URN can be built.")

    reasons[reason] = reasons.get(reason, 0) + 1
    if needle:
        print(f"\n{key}")
        print(f"  io_manager_key : {io_key!r}")
        print(f"  io_manager     : {type(io_manager).__name__ if io_manager else None}")
        print(f"  NOT ADVERTISED : {reason}")
        print(f"  -> {detail}")

print("\n" + "=" * 78)
print(f"advertised (physical identity): {advertised}")
print(f"not advertised               : {sum(reasons.values())}")
for r, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
    print(f"   {n:5d}  {r}")

if not needle and reasons:
    print("\nRe-run with an asset-name substring for the per-asset detail, e.g.")
    print("   python /tmp/d.py board_mapping")
