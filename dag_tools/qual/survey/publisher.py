"""Orchestrates the load → introspect → publish pipeline.

Recipe rule (Phase 1, item 1.2):

  > A load failure fails the Jenkins stage and **nothing is published** —
  > the registry never contains an inventory for code that doesn't load.

So this module's ``run_survey`` function:

  1. Loads every requested code location (warnings captured).
  2. If **any** location fails to load: returns a ``SurveyOutcome`` with
     ``published=False`` and the failure detail. Publishes nothing.
  3. Otherwise: introspects every location, merges into the per-artifact
     JSON payloads, and publishes via ``InventoryRegistry.publish_build``
     (which writes the immutable per-SHA artifacts first, then
     ``latest.json`` last).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from dag_tools.inventory import SCHEMA_VERSION as INVENTORY_SCHEMA_VERSION

from ..registry import BuildMeta, InventoryRegistry, layout
from .introspector import (
    introspect_assets,
    introspect_automation,
    introspect_dbt_projects,
    summarize_io_managers,
)
from .loader import LoadResult, load_locations
from .schemas import (
    AssetsManifest,
    AutomationInventory,
    DbtProjectsInventory,
    IoManagersInventory,
    LoadValidation,
    WarningRecord,
)


logger = logging.getLogger(__name__)


@dataclass
class SurveyOutcome:
    """The end-state of one ``dagtools survey`` invocation."""

    published: bool
    """True iff every location loaded AND the publish completed cleanly."""

    load_validation: LoadValidation
    """Always populated, even when the load failed (then ``loads=False``)."""

    pointer_sha: Optional[str] = None
    """The SHA written to ``latest.json``. None when nothing was published."""

    artifacts_written: List[str] = field(default_factory=list)
    """Filenames published under inventory/<repo>/<sha>/. Empty when not published."""


def run_survey(
    locations_spec: str,
    repo: str,
    git_sha: str,
    *,
    registry: InventoryRegistry,
    build_id: Optional[str] = None,
    dagster_version: Optional[str] = None,
    dagtools_version: Optional[str] = None,
    allow_overwrite: bool = False,
    skip_publish: bool = False,
    now: Optional[datetime] = None,
) -> SurveyOutcome:
    """Run the full survey for one repo / SHA. See module docstring for rules."""
    now = now or datetime.now(tz=timezone.utc)

    # --- 1. Load every code location (warnings captured per-location). ----
    results: List[LoadResult] = load_locations(locations_spec)

    all_warnings: List[WarningRecord] = []
    for r in results:
        all_warnings.extend(r.warnings_captured)

    failed = [r for r in results if not r.loaded]
    loaded = [r for r in results if r.loaded]

    # --- 2. Refuse to publish anything if any location failed to load. ----
    if failed:
        logger.error(
            "run_survey: %d/%d location(s) failed to load — refusing to publish",
            len(failed), len(results),
        )
        return SurveyOutcome(
            published=False,
            load_validation=LoadValidation(
                timestamp=now,
                loads=False,
                locations=[r.to_loaded_location() for r in loaded],
                failures=[r.to_failure() for r in failed],
                warnings=all_warnings,
            ),
        )

    # --- 3. Introspect every loaded location. ----------------------------
    location_payloads: List[_LocationPayload] = []
    for r in loaded:
        records, assets_manifest = introspect_assets(r.defs, location=r.name)
        automation = introspect_automation(r.defs, location=r.name)
        dbt_projects = introspect_dbt_projects(r.defs)
        location_payloads.append(_LocationPayload(
            result=r,
            asset_records=records,
            assets=assets_manifest,
            automation=automation,
            dbt_projects=dbt_projects,
        ))

    # --- 4. Merge per-location payloads into fleet-flat artifacts. -------
    merged_records = [rec for p in location_payloads for rec in p.asset_records]
    assets_manifest = AssetsManifest(
        inventory_schema_version=INVENTORY_SCHEMA_VERSION,
        records=[rec.model_dump(mode="json") for rec in merged_records],
    )
    automation = AutomationInventory(
        sensors=[s for p in location_payloads for s in p.automation.sensors],
        schedules=[s for p in location_payloads for s in p.automation.schedules],
        asset_checks=[c for p in location_payloads for c in p.automation.asset_checks],
    )
    dbt_projects = DbtProjectsInventory(
        projects=[d for p in location_payloads for d in p.dbt_projects.projects],
    )
    io_managers = summarize_io_managers(merged_records)

    load_validation = LoadValidation(
        timestamp=now,
        loads=True,
        locations=[
            p.result.to_loaded_location(
                asset_count=len(p.asset_records),
                sensor_count=len(p.automation.sensors),
                schedule_count=len(p.automation.schedules),
                asset_check_count=len(p.automation.asset_checks),
            )
            for p in location_payloads
        ],
        failures=[],
        warnings=all_warnings,
    )

    # --- 5. Optionally publish. ------------------------------------------
    if skip_publish:
        return SurveyOutcome(
            published=False,  # by request
            load_validation=load_validation,
            pointer_sha=git_sha,
            artifacts_written=[],
        )

    artifacts = {
        layout.ASSETS_FILE: _to_bytes(assets_manifest),
        layout.AUTOMATION_FILE: _to_bytes(automation),
        layout.IO_MANAGERS_FILE: _to_bytes(io_managers),
        layout.DBT_PROJECTS_FILE: _to_bytes(dbt_projects),
        layout.LOAD_VALIDATION_FILE: _to_bytes(load_validation),
    }

    meta = BuildMeta(
        repo=repo,
        git_sha=git_sha,
        build_id=build_id,
        timestamp=now,
        dagster_version=dagster_version,
        dagtools_version=dagtools_version,
        inventory_schema_version=INVENTORY_SCHEMA_VERSION,
    )

    registry.publish_build(
        repo=repo, git_sha=git_sha,
        artifacts=artifacts, meta=meta,
        allow_overwrite=allow_overwrite,
    )

    return SurveyOutcome(
        published=True,
        load_validation=load_validation,
        pointer_sha=git_sha,
        artifacts_written=[layout.META_FILE, *artifacts.keys()],
    )


@dataclass
class _LocationPayload:
    """Per-location introspection bundle, before merge."""
    result: LoadResult
    asset_records: list
    assets: AssetsManifest
    automation: AutomationInventory
    dbt_projects: DbtProjectsInventory


def _to_bytes(model) -> bytes:
    """Serialize a pydantic BaseModel to JSON bytes (datetime-safe)."""
    return model.model_dump_json().encode("utf-8")


def _detect_dagster_version() -> Optional[str]:
    """Best-effort dagster version detection for ``meta.json``."""
    try:
        import dagster
        return getattr(dagster, "__version__", None)
    except Exception:
        return None


def _detect_dagtools_version() -> Optional[str]:
    """Best-effort dag_tools version detection for ``meta.json``."""
    try:
        from importlib.metadata import version
        return version("dag_tools")
    except Exception:
        return None
