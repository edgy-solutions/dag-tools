"""Generic OpenTelemetry -> API synchronisation mapping engine.

Pure library code: no Dagster, Restate or dlt imports, so the Dagster
component, the Restate handler and the tests all share one
implementation of "what does this YAML mean".

The wire contract between the two halves is the **call plan** produced by
:func:`dag_tools.otel_api_sync.render.build_plan` — an ordered, fully
rendered list of HTTP calls with their fallbacks attached. Rendering is
Dagster-side; execution is Restate-side.
"""
from dag_tools.otel_api_sync.environment import (
    NativeSandboxedEnvironment,
    build_environment,
    render_structure,
    render_value,
)
from dag_tools.otel_api_sync.plan import PLAN_FORMAT_VERSION, set_path
from dag_tools.otel_api_sync.functions import DEFAULT_ATTRIBUTE_COLUMNS, build_functions
from dag_tools.otel_api_sync.render import (
    build_plan,
    compute_plan_hash,
    group_readiness,
    group_rows,
    render_plans,
)
from dag_tools.otel_api_sync.spec import (
    ApiSpec,
    FallbackSpec,
    OtelApiSyncSpec,
    ReadinessSpec,
    StepSpec,
    load_spec,
    load_spec_file,
)

__all__ = [
    "ApiSpec",
    "DEFAULT_ATTRIBUTE_COLUMNS",
    "FallbackSpec",
    "NativeSandboxedEnvironment",
    "OtelApiSyncSpec",
    "PLAN_FORMAT_VERSION",
    "ReadinessSpec",
    "StepSpec",
    "build_environment",
    "build_functions",
    "build_plan",
    "compute_plan_hash",
    "group_readiness",
    "group_rows",
    "load_spec",
    "load_spec_file",
    "render_plans",
    "render_structure",
    "render_value",
    "set_path",
]
