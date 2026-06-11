"""Pydantic schemas for the per-build artifacts the survey publishes.

Every artifact embeds ``schema_version`` (monotonic, additive-only) just like
``dag_tools.inventory.AssetRecord`` — so older readers (the long-lived
qualification operator desktop) can tolerate newer writers (CI surveys from
repos on tomorrow's release).

Rules (mirror ``dag_tools.inventory.schema``):

  * Add only ``Optional`` fields with defaults; never rename or remove.
  * Bump the artifact's ``SCHEMA_VERSION_*`` constant in the same commit.
  * Readers use ``extra="ignore"`` so unknown fields are silently tolerated.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


# --- per-artifact schema versions ------------------------------------------
SCHEMA_VERSION_LOAD_VALIDATION = 1
SCHEMA_VERSION_AUTOMATION = 1
SCHEMA_VERSION_IO_MANAGERS = 1
SCHEMA_VERSION_DBT_PROJECTS = 1
SCHEMA_VERSION_ASSETS = 1


# ---------------------------------------------------------------------------
# Load validation — written even on success (to record warnings + which
# locations loaded). On failure the survey writes NOTHING; the registry
# never contains an inventory for code that doesn't load.
# ---------------------------------------------------------------------------


class LoadedLocation(BaseModel):
    """One code location that successfully loaded."""
    model_config = ConfigDict(extra="ignore")

    name: str
    source: str
    """Either a file path or a module path — whichever was passed in."""

    asset_count: Optional[int] = None
    sensor_count: Optional[int] = None
    schedule_count: Optional[int] = None
    asset_check_count: Optional[int] = None


class LoadFailure(BaseModel):
    """One code location that failed to load."""
    model_config = ConfigDict(extra="ignore")

    name: str
    source: str
    error: str
    traceback: Optional[str] = None


class WarningRecord(BaseModel):
    """A captured ``warnings.warn(...)`` event during load."""
    model_config = ConfigDict(extra="ignore")

    category: str
    """Fully-qualified class name of the warning (e.g. ``DeprecationWarning``)."""
    message: str
    filename: Optional[str] = None
    lineno: Optional[int] = None
    location: Optional[str] = None
    """Which code location emitted the warning (when known)."""


class LoadValidation(BaseModel):
    """The ``load_validation.json`` payload."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_LOAD_VALIDATION)
    timestamp: datetime
    loads: bool
    """True iff every requested location loaded without raising."""

    locations: List[LoadedLocation] = Field(default_factory=list)
    failures: List[LoadFailure] = Field(default_factory=list)
    warnings: List[WarningRecord] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Automation inventory — sensors, schedules, asset checks per location.
# ---------------------------------------------------------------------------


class SensorRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_AUTOMATION)
    name: str
    location: Optional[str] = None
    sensor_type: Optional[str] = None
    """Class name (e.g. ``SensorDefinition``, ``AssetSensorDefinition``)."""
    minimum_interval_seconds: Optional[int] = None
    job_name: Optional[str] = None
    asset_selection: List[List[str]] = Field(default_factory=list)
    """Each entry is an AssetKey path."""
    description: Optional[str] = None


class ScheduleRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_AUTOMATION)
    name: str
    location: Optional[str] = None
    cron_schedule: Optional[str] = None
    execution_timezone: Optional[str] = None
    job_name: Optional[str] = None
    description: Optional[str] = None


class AssetCheckRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_AUTOMATION)
    name: str
    location: Optional[str] = None
    asset_key: List[str]
    description: Optional[str] = None


class AutomationInventory(BaseModel):
    """The ``automation.json`` payload."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_AUTOMATION)
    sensors: List[SensorRecord] = Field(default_factory=list)
    schedules: List[ScheduleRecord] = Field(default_factory=list)
    asset_checks: List[AssetCheckRecord] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# IO managers summary — derived from AssetRecord.io_manager_class / family.
# ---------------------------------------------------------------------------


class IoManagerEntry(BaseModel):
    """One distinct IO manager class observed across the fleet."""
    model_config = ConfigDict(extra="ignore")

    io_manager_class: Optional[str] = None
    """FQN of the IO manager. None means "no resource bound" (very rare)."""
    family: Optional[str] = None
    """Classifier family ('postgres', 's3_iceberg', ...). None = no match."""
    asset_count: int = 0
    """How many assets this IO manager is bound to."""
    asset_keys_sample: List[List[str]] = Field(default_factory=list)
    """Up to ~5 sample asset keys, for human eyeballing."""


class IoManagersInventory(BaseModel):
    """The ``io_managers.json`` payload."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_IO_MANAGERS)
    entries: List[IoManagerEntry] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# dbt projects — manifest path, version, translator class (custom flagged).
# ---------------------------------------------------------------------------


class DbtProjectRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_DBT_PROJECTS)
    resource_key: Optional[str] = None
    """Resource key under which the DbtCliResource was registered."""
    project_dir: Optional[str] = None
    manifest_path: Optional[str] = None
    """Best-effort: <project_dir>/target/manifest.json when present."""
    dbt_version: Optional[str] = None
    translator_class: Optional[str] = None
    """FQN of the DagsterDbtTranslator subclass associated with these dbt
    assets. None when not detectable."""
    is_custom_translator: bool = False
    """True when ``translator_class`` is a subclass of (not exactly) the
    stock DagsterDbtTranslator. High-risk: custom code is exactly what
    stock-shaped tests miss."""


class DbtProjectsInventory(BaseModel):
    """The ``dbt_projects.json`` payload."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_DBT_PROJECTS)
    projects: List[DbtProjectRecord] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Assets manifest — embeds the AssetRecord shape from dag_tools.inventory.
# ---------------------------------------------------------------------------


class AssetsManifest(BaseModel):
    """The ``assets.json`` payload — wraps the inventory.AssetRecord list."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_ASSETS)
    inventory_schema_version: int
    """Version of dag_tools.inventory.AssetRecord at write time."""
    records: List[Dict[str, Any]] = Field(default_factory=list)
    """Each entry is ``AssetRecord.model_dump()``."""
