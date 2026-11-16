"""Contract as data: map a source's columns onto a contract's.

A pure engine. No Dagster, no dbt, no IO -- it takes parsed documents and
returns a plan, so the mapping semantics are testable without a warehouse or
a code location, and the Dagster component, a test and a future Restate
handler all share one implementation. The component lives at
`dag_tools.components.contract_mapping`.
"""
from dag_tools.contract_mapping.contract import (
    CONTRACT_TYPES,
    Contract,
    ContractColumn,
    load_contract,
)
from dag_tools.contract_mapping.emit import (
    CheckFailure,
    CoercionError,
    RenderError,
    apply_to_rows,
    check_rows,
    render_dbt_artifacts,
    render_model_sql,
    render_schema_yml,
    render_singular_tests,
)
from dag_tools.contract_mapping.mapping import (
    TRANSFORMS,
    WHERE_OPS,
    ColumnMapping,
    Mapping,
    RowFilter,
    load_mapping,
    validate_where,
)
from dag_tools.contract_mapping.normalize import (
    NormalizerOp,
    Normalizers,
    check_re2,
    load_normalizers,
    load_normalizers_text,
)
from dag_tools.contract_mapping.plan import (
    RULE_IDS,
    MappingPlan,
    MappingPlanError,
    PlannedColumn,
    PlannedFilter,
    PlannedTest,
    build_mapping_plan,
)

__all__ = [
    "CONTRACT_TYPES",
    "RULE_IDS",
    "TRANSFORMS",
    "WHERE_OPS",
    "NormalizerOp",
    "Normalizers",
    "check_re2",
    "load_normalizers",
    "load_normalizers_text",
    "CheckFailure",
    "CoercionError",
    "ColumnMapping",
    "Contract",
    "ContractColumn",
    "Mapping",
    "MappingPlan",
    "MappingPlanError",
    "PlannedColumn",
    "PlannedFilter",
    "PlannedTest",
    "RenderError",
    "RowFilter",
    "apply_to_rows",
    "build_mapping_plan",
    "check_rows",
    "load_contract",
    "load_mapping",
    "render_dbt_artifacts",
    "render_model_sql",
    "render_schema_yml",
    "render_singular_tests",
    "validate_where",
]
