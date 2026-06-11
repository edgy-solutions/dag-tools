"""Per-Definitions introspection: assets, automation, dbt, io_managers.

The asset records are produced by ``dag_tools.inventory.extract_records``
(the shared contract). Everything else lives here because it's
survey-specific framing on top of Dagster's sensor / schedule / asset-check /
dbt-resource APIs.

Soft-failure: like the inventory extractor, every per-item access is wrapped
so one malformed sensor never breaks the whole introspection.
"""
from __future__ import annotations

import logging
from collections import Counter
from typing import Any, List, Optional, Tuple

from dag_tools.inventory import AssetRecord, extract_records
from dag_tools.inventory import SCHEMA_VERSION as INVENTORY_SCHEMA_VERSION
from dag_tools.inventory.classifier import fqn

from .schemas import (
    AssetCheckRecord,
    AssetsManifest,
    AutomationInventory,
    DbtProjectRecord,
    DbtProjectsInventory,
    IoManagerEntry,
    IoManagersInventory,
    ScheduleRecord,
    SensorRecord,
)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Assets — wrapper around the shared inventory extractor
# ---------------------------------------------------------------------------


def introspect_assets(defs: Any, location: Optional[str] = None) -> Tuple[List[AssetRecord], AssetsManifest]:
    """Return the raw AssetRecord list plus the wrapped AssetsManifest payload."""
    records = extract_records(defs, location=location)
    manifest = AssetsManifest(
        inventory_schema_version=INVENTORY_SCHEMA_VERSION,
        records=[r.model_dump(mode="json") for r in records],
    )
    return records, manifest


# ---------------------------------------------------------------------------
# Automation — sensors, schedules, asset checks
# ---------------------------------------------------------------------------


def introspect_automation(defs: Any, location: Optional[str] = None) -> AutomationInventory:
    """Walk sensors / schedules / asset checks. Soft-fails per item."""
    return AutomationInventory(
        sensors=_sensor_records(defs, location),
        schedules=_schedule_records(defs, location),
        asset_checks=_asset_check_records(defs, location),
    )


def _sensor_records(defs: Any, location: Optional[str]) -> List[SensorRecord]:
    out: List[SensorRecord] = []
    sensors = _safe_iter(defs, "sensors")
    for s in sensors:
        try:
            out.append(SensorRecord(
                name=_safe_attr(s, "name", default=type(s).__name__),
                location=location,
                sensor_type=type(s).__name__,
                minimum_interval_seconds=_safe_int(_safe_attr(s, "minimum_interval_seconds")),
                job_name=_safe_attr(s, "job_name"),
                asset_selection=_asset_selection(s),
                description=_safe_attr(s, "description"),
            ))
        except Exception as e:
            logger.warning("_sensor_records: failed for %s: %s", _safe_attr(s, "name", default="?"), e)
    return out


def _schedule_records(defs: Any, location: Optional[str]) -> List[ScheduleRecord]:
    out: List[ScheduleRecord] = []
    schedules = _safe_iter(defs, "schedules")
    for s in schedules:
        try:
            out.append(ScheduleRecord(
                name=_safe_attr(s, "name", default=type(s).__name__),
                location=location,
                cron_schedule=_safe_attr(s, "cron_schedule"),
                execution_timezone=_safe_attr(s, "execution_timezone"),
                job_name=_safe_attr(s, "job_name"),
                description=_safe_attr(s, "description"),
            ))
        except Exception as e:
            logger.warning("_schedule_records: failed for %s: %s", _safe_attr(s, "name", default="?"), e)
    return out


def _asset_check_records(defs: Any, location: Optional[str]) -> List[AssetCheckRecord]:
    """Asset-check specs live on each ``AssetChecksDefinition`` under
    ``defs.asset_checks``.

    Naming gotcha: ``AssetChecksDefinition.specs`` is the *target asset*
    specs (often empty for stock checks); the actual check specs are
    ``check_specs``. We try ``check_specs`` first, then fall back to
    ``specs`` for older API shapes that may differ.
    """
    out: List[AssetCheckRecord] = []
    asset_checks = _safe_iter(defs, "asset_checks")
    for ac in asset_checks:
        try:
            specs = _safe_iter(ac, "check_specs")
            if not specs:
                specs = _safe_iter(ac, "specs")
            for spec in specs:
                try:
                    asset_key = _safe_attr(spec, "asset_key")
                    key_path = list(asset_key.path) if asset_key is not None else []
                    out.append(AssetCheckRecord(
                        name=_safe_attr(spec, "name", default=type(spec).__name__),
                        location=location,
                        asset_key=key_path,
                        description=_safe_attr(spec, "description"),
                    ))
                except Exception as e:
                    logger.warning("_asset_check_records: failed for spec: %s", e)
        except Exception as e:
            logger.warning("_asset_check_records: outer failure: %s", e)
    return out


# ---------------------------------------------------------------------------
# IO managers — derived summary from the asset records.
# ---------------------------------------------------------------------------


def summarize_io_managers(records: List[AssetRecord]) -> IoManagersInventory:
    """Group asset records by ``(io_manager_class, family)`` with counts +
    a small sample of asset keys. Drives the at-a-glance io_managers.json."""
    bucket: dict[Tuple[Optional[str], Optional[str]], List[List[str]]] = {}
    for r in records:
        key = (r.io_manager_class, r.io_manager_family)
        bucket.setdefault(key, []).append(list(r.asset_key))

    entries: List[IoManagerEntry] = []
    for (cls, family), asset_keys in bucket.items():
        entries.append(IoManagerEntry(
            io_manager_class=cls,
            family=family,
            asset_count=len(asset_keys),
            asset_keys_sample=asset_keys[:5],
        ))
    # Stable order: highest count first, then class name
    entries.sort(key=lambda e: (-e.asset_count, e.io_manager_class or ""))
    return IoManagersInventory(entries=entries)


# ---------------------------------------------------------------------------
# dbt projects — best-effort. Custom translator detection is the high-value
# bit (custom code is exactly what stock-shaped tests miss).
# ---------------------------------------------------------------------------


def introspect_dbt_projects(defs: Any) -> DbtProjectsInventory:
    """Walk defs.resources for DbtCliResource (or subclass), then defs.assets
    for AssetsDefinitions backed by a DagsterDbtTranslator. Soft-fails:
    dagster_dbt is optional, so absence is silent."""
    projects: List[DbtProjectRecord] = []
    try:
        resources = dict(_safe_attr(defs, "resources", default={}) or {})
    except Exception:
        resources = {}

    dbt_cli_cls = _try_import("dagster_dbt", "DbtCliResource")
    dagster_dbt_translator_cls = _try_import("dagster_dbt", "DagsterDbtTranslator")

    if dbt_cli_cls is None:
        # dagster_dbt isn't installed in this env — nothing to introspect.
        return DbtProjectsInventory(projects=[])

    translator_by_project_dir: dict[str, str] = _collect_dbt_translators(defs, dagster_dbt_translator_cls)

    for key, res in resources.items():
        if not isinstance(res, dbt_cli_cls):
            continue
        project_dir = _safe_attr(res, "project_dir")
        manifest_path = None
        if project_dir:
            from pathlib import Path
            candidate = Path(str(project_dir)) / "target" / "manifest.json"
            if candidate.exists():
                manifest_path = str(candidate)

        translator_fqn = translator_by_project_dir.get(str(project_dir or ""))
        is_custom = False
        if translator_fqn and dagster_dbt_translator_cls is not None:
            stock_fqn = fqn(dagster_dbt_translator_cls)
            is_custom = translator_fqn != stock_fqn

        projects.append(DbtProjectRecord(
            resource_key=key,
            project_dir=str(project_dir) if project_dir else None,
            manifest_path=manifest_path,
            dbt_version=None,  # too version-sensitive to read confidently
            translator_class=translator_fqn,
            is_custom_translator=is_custom,
        ))

    return DbtProjectsInventory(projects=projects)


def _collect_dbt_translators(defs: Any, translator_base: Any) -> dict[str, str]:
    """Walk every AssetsDefinition; if it carries a ``dagster_dbt`` translator
    instance, record its FQN keyed on ``project_dir`` (when discoverable).

    Best-effort — dagster_dbt internal attributes drift between versions.
    """
    if translator_base is None:
        return {}
    result: dict[str, str] = {}
    try:
        all_assets = _safe_iter(defs, "assets")
    except Exception:
        return result
    for ad in all_assets:
        try:
            # In dagster_dbt 0.28+, the translator is stored on each
            # AssetsDefinition's compute spec under a private attribute.
            # We try a few likely names and skip if none stick.
            translator = (
                _safe_attr(ad, "translator")
                or _safe_attr(ad, "_translator")
                or _safe_attr(ad, "dagster_dbt_translator")
            )
            if translator is None or not isinstance(translator, translator_base):
                continue
            translator_fqn = fqn(type(translator))
            # Find the project_dir; ditto best-effort.
            project_dir = (
                _safe_attr(translator, "project_dir")
                or _safe_attr(ad, "project_dir")
            )
            result[str(project_dir or "")] = translator_fqn
        except Exception:
            continue
    return result


def _try_import(module_name: str, attr: str) -> Any:
    """Import-or-None for optional dependencies (dagster_dbt)."""
    try:
        mod = __import__(module_name, fromlist=[attr])
        return getattr(mod, attr, None)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Soft-failure helpers (mirror the inventory extractor's style)
# ---------------------------------------------------------------------------


def _safe_attr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _safe_iter(obj: Any, name: str) -> List[Any]:
    """Return ``obj.name`` as a list, gracefully handling None / failure."""
    try:
        val = getattr(obj, name, None)
        if val is None:
            return []
        return list(val)
    except Exception:
        return []


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _asset_selection(sensor: Any) -> List[List[str]]:
    """Best-effort: extract the asset keys a sensor targets, when its
    asset_selection exposes them publicly."""
    sel = _safe_attr(sensor, "asset_selection")
    if sel is None:
        return []
    # AssetSelection in newer Dagster has resolve(); some sensors expose
    # an explicit asset_keys frozenset. Try the common shapes.
    try:
        keys = _safe_attr(sel, "asset_keys")
        if keys:
            return [list(k.path) for k in keys]
    except Exception:
        pass
    return []
