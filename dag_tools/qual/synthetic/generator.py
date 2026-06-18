"""Source-code generation for one synthetic probe module per equivalence class.

Recipe (Phase Q5):

  > For each SYNTHETIC_REQUIRED class, generate a module under a dedicated
  > probe code location ... Import the **real** IO manager and resource
  > classes (fully qualified names from the inventory — this is why the
  > survey records them), instantiated with staging config. Trivial
  > upstream asset producing a small deterministic payload matching the
  > class's broad shape ... plus a downstream that loads and asserts
  > equality/shape. Mirror the class's partitions def and partition
  > mappings.

What this v1 generates:

  * Imports the real IO manager class by FQN.
  * Wraps the import in a try/except so the **probe module always loads**
    — a missing import becomes a clear runtime error at materialization,
    not a code-location load failure. The operator can deploy the
    dag-tools-probes location alongside other location work and triage
    individual probes iteratively.
  * Produces a deterministic dict payload that survives any IO manager's
    handle_output / load_input round-trip (the recipe's "small
    deterministic payload matching the class's broad shape" — for v1
    that's a dict; future versions can infer DataFrame / file shapes
    from the io_manager_class FQN).
  * Defines an upstream + downstream asset pair; the downstream asserts
    payload identity after the IO manager round-trip.

Deferred to follow-up slices:
  * Partition def + partition mapping mirroring (recorded in ``notes``).
  * Real staging-config injection (placeholder instantiation today;
    the generated module's ``_IO_MANAGER = <Class>()`` is a hook the
    operator wires up at deploy time).
  * Resource other than the IO manager (only the IO manager is bound in v1).
"""
from __future__ import annotations

import logging
from textwrap import dedent
from typing import List, Optional, Tuple

from ..classes import EquivalenceClass, Runnability
from .schema import ProbeModule, ProbeStatus


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def generate_probe_module(
    cls: EquivalenceClass,
    *,
    qual_id: str,
) -> Tuple[Optional[ProbeModule], str]:
    """Build a :class:`ProbeModule` + its source text for one class.

    Returns ``(module, source_text)`` on success, or ``(None, reason)``
    when the class doesn't get a probe (no representatives, no synthetic
    runnability, etc.). The caller (bundle) collects the skip reasons
    into the manifest so they're visible to the operator.
    """
    if not cls.representatives:
        return None, "class has no representatives"

    rep0 = cls.representatives[0]
    if rep0.runnability != Runnability.SYNTHETIC_REQUIRED:
        return None, f"class runnability is {rep0.runnability.value}, not synthetic_required"

    notes: List[str] = []
    if cls.key.partitions_def_class:
        notes.append(
            f"partitions def {cls.key.partitions_def_class!r} observed but not "
            "synthesized in v1 — operator: augment the probe if partitioning matters"
        )
    if cls.key.partition_mapping_classes:
        notes.append(
            f"partition mappings {cls.key.partition_mapping_classes} observed "
            "but not synthesized in v1"
        )
    if cls.key.custom_dbt_translator_classes:
        notes.append(
            "custom dagster_dbt translator(s) detected on this class — the "
            "synthetic probe does NOT exercise dbt; operator: add a dbt-aware "
            "probe if translator behavior is part of the qualification surface"
        )

    short = cls.class_hash[:8]
    module_name = f"probe_{short}"
    source_key = f"qualifications/{qual_id}/probes/{cls.class_hash}.py"

    source = generate_probe_source(
        cls=cls, module_name=module_name, qual_id=qual_id, notes=notes,
    )

    probe = ProbeModule(
        class_hash=cls.class_hash,
        module_name=module_name,
        source_key=source_key,
        io_manager_class=cls.key.io_manager_class,
        resource_classes=dict(zip(
            range_resource_keys(cls), cls.key.resource_classes,
        )),
        integration_libs=list(cls.key.integration_libs),
        runnability=rep0.runnability.value,
        notes=notes,
        status=ProbeStatus.GENERATED,
    )
    return probe, source


def range_resource_keys(cls: EquivalenceClass) -> List[str]:
    """Best-effort: pull resource keys from the first representative's
    members. ClassKeyComponents flattens to a list of FQNs without their
    keys, so we use member-level keys when available."""
    # ClassKeyComponents.resource_classes is a flat sorted list of FQNs
    # (the key was discarded during class-key construction). For the
    # ProbeModule we just enumerate as "resource_0", "resource_1", etc. so
    # downstream UIs can still display them. The actual generated module
    # only binds io_manager — other resources are operator's job.
    return [f"resource_{i}" for i in range(len(cls.key.resource_classes))]


# ---------------------------------------------------------------------------
# Source generation
# ---------------------------------------------------------------------------


def generate_probe_source(
    *,
    cls: EquivalenceClass,
    module_name: str,
    qual_id: str,
    notes: List[str],
) -> str:
    """Return the Python source text for one probe module.

    Self-contained — the only Dagster surface used is ``Definitions`` +
    ``@asset``, both stable across the Dagster version range we care
    about. The IO manager is imported by FQN with a fallback to
    InMemoryIOManager so the code location ALWAYS loads.
    """
    short = cls.class_hash[:8]
    rep0 = cls.representatives[0]
    io_manager_fqn = cls.key.io_manager_class
    io_manager_import, io_manager_construct = _io_manager_import_lines(io_manager_fqn)

    notes_block = "\n".join(f"#   - {n}" for n in notes) if notes else "#   (none)"

    return dedent(f'''\
"""Synthetic probe for equivalence class {cls.class_hash}.

Auto-generated by `dagtools qual synthetic` for qual_id={qual_id!r}.

Recipe rule (Phase Q5): synthetic probes validate Dagster control flow
through real custom classes. They do NOT validate prod-only credentials,
paths, or data scale — that's the operational gap operators accept when
they pass `--accept-synthetic-coverage-missing` if Q5 hasn't been run.

Class composition (from the inventory at qualification-init time):
  io_manager_class : {cls.key.io_manager_class!r}
  partitions       : {cls.key.partitions_def_class!r}
  integration_libs : {list(cls.key.integration_libs)}

Generation notes:
{notes_block}

Representative used as the seed shape:
  repo     = {rep0.repo!r}
  asset_key= {list(rep0.asset_key)!r}

Operator integration:
  Wire `_IO_MANAGER = ...` below with the staging-configured constructor
  call for your deployment (the import block is a no-arg fallback so the
  code-location load succeeds even when the real class needs config).
"""
from __future__ import annotations

from typing import Any, Dict

from dagster import Definitions, asset

CLASS_HASH = "{cls.class_hash}"

# --- Real IO manager from the inventory FQN ---------------------------
{io_manager_import}

PROBE_PAYLOAD: Dict[str, Any] = {{
    "schema_version": 1,
    "class_hash": CLASS_HASH,
    "probe": "dag-tools synthetic",
}}


@asset
def {module_name}_upstream() -> Dict[str, Any]:
    """Emit the deterministic probe payload. Raises if the real IO manager
    couldn't be imported, so the failure is visible at materialization
    time rather than masked by the in-memory fallback."""
    if _IO_MANAGER_ERROR is not None:
        raise RuntimeError(
            "probe IO manager could not be imported: " + _IO_MANAGER_ERROR
        )
    return dict(PROBE_PAYLOAD)


@asset(deps=[{module_name}_upstream])
def {module_name}_downstream({module_name}_upstream: Dict[str, Any]) -> bool:
    """Load upstream's payload via the same IO manager and assert it
    survived the round-trip intact."""
    payload = {module_name}_upstream
    assert isinstance(payload, dict), "payload was not a dict after round-trip"
    assert payload.get("class_hash") == CLASS_HASH, (
        "payload corrupted after IO manager round-trip"
    )
    return True


defs = Definitions(
    assets=[{module_name}_upstream, {module_name}_downstream],
    resources={{"io_manager": {io_manager_construct}}},
)
''')


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _io_manager_import_lines(io_manager_fqn: Optional[str]) -> Tuple[str, str]:
    """Build the IO manager import block + the constructor expression.

    Returns ``(import_block, construct_expr)`` where:
      * ``import_block`` is a multi-line string defining ``_IO_MANAGER_CLASS``,
        ``_IO_MANAGER``, and ``_IO_MANAGER_ERROR`` with a try/except fallback.
      * ``construct_expr`` is the expression to reference in the
        ``Definitions(resources={...})`` block — always ``_IO_MANAGER``,
        but factored out in case future generators want something more
        clever.
    """
    if not io_manager_fqn:
        return (
            dedent('''\
            from dagster._core.storage.mem_io_manager import InMemoryIOManager
            _IO_MANAGER = InMemoryIOManager()
            _IO_MANAGER_ERROR = None'''),
            "_IO_MANAGER",
        )

    module_path, _, class_name = io_manager_fqn.rpartition(".")
    if not module_path or not class_name:
        return (
            dedent(f'''\
            # ``{io_manager_fqn}`` did not look like a fully-qualified class
            # path; falling back to InMemoryIOManager and recording the error.
            from dagster._core.storage.mem_io_manager import InMemoryIOManager
            _IO_MANAGER = InMemoryIOManager()
            _IO_MANAGER_ERROR = "malformed io_manager_class FQN: {io_manager_fqn!r}"'''),
            "_IO_MANAGER",
        )

    return (
        dedent(f'''\
        try:
            from {module_path} import {class_name} as _IO_MANAGER_CLASS
            _IO_MANAGER = _IO_MANAGER_CLASS()  # operator: override with staging config
            _IO_MANAGER_ERROR = None
        except Exception as _e:  # noqa: BLE001
            # Fall back to InMemoryIOManager so the code location LOADS;
            # the probe asset will raise at materialization with the
            # captured error so operators can triage iteratively.
            from dagster._core.storage.mem_io_manager import InMemoryIOManager
            _IO_MANAGER = InMemoryIOManager()
            _IO_MANAGER_ERROR = repr(_e)'''),
        "_IO_MANAGER",
    )
