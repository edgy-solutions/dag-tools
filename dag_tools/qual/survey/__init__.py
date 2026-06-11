"""Per-build inventory survey: load Definitions, introspect, publish.

Public surface:
  * ``run_survey`` — the orchestrator the CLI delegates to.
  * ``SurveyOutcome`` — the result type the CLI emits as JSON.
  * ``load_locations`` / ``LoadResult`` — loader entry points (also useful in tests).
  * ``introspect_assets`` / ``introspect_automation`` / ``introspect_dbt_projects`` /
    ``summarize_io_managers`` — introspection primitives, individually testable.

See ``publisher.py`` for the load-gate refusal semantics: when any code
location fails to load, the registry is left untouched.
"""
from .introspector import (
    introspect_assets,
    introspect_automation,
    introspect_dbt_projects,
    summarize_io_managers,
)
from .loader import LoadResult, load_locations
from .publisher import SurveyOutcome, run_survey
from .schemas import (
    AssetCheckRecord,
    AssetsManifest,
    AutomationInventory,
    DbtProjectRecord,
    DbtProjectsInventory,
    IoManagerEntry,
    IoManagersInventory,
    LoadFailure,
    LoadValidation,
    LoadedLocation,
    ScheduleRecord,
    SensorRecord,
    WarningRecord,
)

__all__ = [
    # publisher
    "run_survey",
    "SurveyOutcome",
    # loader
    "load_locations",
    "LoadResult",
    # introspection primitives
    "introspect_assets",
    "introspect_automation",
    "introspect_dbt_projects",
    "summarize_io_managers",
    # schemas
    "AssetCheckRecord",
    "AssetsManifest",
    "AutomationInventory",
    "DbtProjectRecord",
    "DbtProjectsInventory",
    "IoManagerEntry",
    "IoManagersInventory",
    "LoadFailure",
    "LoadValidation",
    "LoadedLocation",
    "ScheduleRecord",
    "SensorRecord",
    "WarningRecord",
]
