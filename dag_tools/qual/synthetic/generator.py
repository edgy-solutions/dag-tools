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
  * Constructs the IO manager with the manifest's ``probes`` config
    (literals, or ``{env: VAR}`` rendered as ``os.environ[...]``).
  * Produces a deterministic dict payload that survives any IO manager's
    handle_output / load_input round-trip (the recipe's "small
    deterministic payload matching the class's broad shape" — for v1
    that's a dict; future versions can infer DataFrame / file shapes
    from the io_manager_class FQN).
  * Defines an upstream + downstream asset pair; the downstream asserts
    payload identity after the IO manager round-trip.

Deferred to follow-up slices:
  * Partition def + partition mapping mirroring (recorded in ``notes``).
  * Resource other than the IO manager (only the IO manager is bound in v1).
"""
from __future__ import annotations

import keyword
import logging
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple

from ..classes import EquivalenceClass, Runnability
from ..qualify.manifest import ProbeSettings, is_env_ref
from .schema import ProbeModule, ProbeStatus


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def generate_probe_module(
    cls: EquivalenceClass,
    *,
    qual_id: str,
    probe_settings: Optional[ProbeSettings] = None,
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

    io_fqn = cls.key.io_manager_class
    settings = probe_settings or ProbeSettings()
    io_config = settings.config_for(cls.class_hash, io_fqn)
    if io_fqn and not settings.has_config_for(cls.class_hash, io_fqn):
        notes.append(
            f"no constructor config supplied for `{io_fqn}`; the probe "
            "constructs it with no arguments and will fail at materialization "
            "if the class requires config"
        )

    short = cls.class_hash[:8]
    module_name = f"probe_{short}"
    source_key = f"qualifications/{qual_id}/probes/{cls.class_hash}.py"

    source = generate_probe_source(
        cls=cls, module_name=module_name, qual_id=qual_id, notes=notes,
        io_manager_config=io_config,
    )

    probe = ProbeModule(
        class_hash=cls.class_hash,
        module_name=module_name,
        source_key=source_key,
        io_manager_class=cls.key.io_manager_class,
        io_manager_config_keys=sorted(io_config),
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
    io_manager_config: Optional[Dict[str, Any]] = None,
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
    io_manager_import, io_manager_construct = _io_manager_import_lines(
        io_manager_fqn, config=io_manager_config,
    )

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
  `_IO_MANAGER` is constructed with the constructor config from the
  qualification manifest's `probes` block (`dagtools qual init
  --probe-config`); env references appear as os.environ[...] and are
  resolved at code-location load, never baked into this file. With no
  config it is a no-arg construct.
"""
from __future__ import annotations

from typing import Any, Dict

from dagster import Definitions, asset

CLASS_HASH = "{cls.class_hash}"

# --- Real IO manager from the inventory FQN ---------------------------
{io_manager_import}

# Unique resource key per probe so the dag-tools-probes location can
# merge every probe's Definitions without clobbering. The operator can
# point this key at staging-configured IO managers at deploy time.
IO_MANAGER_KEY = "io_manager_{short}"

PROBE_PAYLOAD: Dict[str, Any] = {{
    "schema_version": 1,
    "class_hash": CLASS_HASH,
    "probe": "dag-tools synthetic",
}}


@asset(io_manager_key=IO_MANAGER_KEY)
def {module_name}_upstream() -> Dict[str, Any]:
    """Emit the deterministic probe payload. Raises if the real IO manager
    couldn't be imported, so the failure is visible at materialization
    time rather than masked by the in-memory fallback."""
    if _IO_MANAGER_ERROR is not None:
        raise RuntimeError(
            "probe IO manager could not be imported: " + _IO_MANAGER_ERROR
        )
    return dict(PROBE_PAYLOAD)


@asset(deps=[{module_name}_upstream], io_manager_key=IO_MANAGER_KEY)
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
    resources={{IO_MANAGER_KEY: {io_manager_construct}}},
)
''')


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _io_manager_import_lines(
    io_manager_fqn: Optional[str],
    *,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    """Build the IO manager import block + the constructor expression.

    Returns ``(import_block, construct_expr)`` where:
      * ``import_block`` is a multi-line string defining ``_IO_MANAGER_CLASS``,
        ``_IO_MANAGER``, and ``_IO_MANAGER_ERROR`` with a try/except fallback.
      * ``config`` is the already-resolved constructor kwargs (no lookup here).
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

    construct, uses_os = _render_construct(config or {})
    block = (
        "try:\n"
        f"    from {module_path} import {class_name} as _IO_MANAGER_CLASS\n"
        f"{construct}\n"
        "    _IO_MANAGER_ERROR = None\n"
        "except Exception as _e:  # noqa: BLE001\n"
        "    # Fall back to InMemoryIOManager so the code location LOADS;\n"
        "    # the probe asset will raise at materialization with the\n"
        "    # captured error so operators can triage iteratively.\n"
        "    from dagster._core.storage.mem_io_manager import InMemoryIOManager\n"
        "    _IO_MANAGER = InMemoryIOManager()\n"
        "    _IO_MANAGER_ERROR = repr(_e)"
    )
    if uses_os:
        block = "import os\n" + block
    return block, "_IO_MANAGER"


def _render_construct(config: Dict[str, Any]) -> Tuple[str, bool]:
    """Render the ``_IO_MANAGER = _IO_MANAGER_CLASS(...)`` lines (indented
    for the ``try`` body). Returns ``(text, uses_os)``.

    No config -> exactly the historical no-arg line. Kwargs are sorted by
    name so regenerating an unchanged config is byte-identical. Env
    references render as ``os.environ["VAR"]`` and are NEVER resolved here:
    the source is published to the registry and written to disk.
    """
    if not config:
        return "    _IO_MANAGER = _IO_MANAGER_CLASS()  # operator: override with staging config", False
    uses_os = False
    lines = ["    _IO_MANAGER = _IO_MANAGER_CLASS("]
    for key in sorted(config):
        value = config[key]
        if not key.isidentifier() or keyword.iskeyword(key):
            raise ValueError(f"io manager kwarg name {key!r} is not a valid identifier")
        if is_env_ref(value):
            uses_os = True
            rendered = f"os.environ[{value['env']!r}]"
        else:
            rendered = repr(value)
        lines.append(f"        {key}={rendered},")
    lines.append("    )")
    return "\n".join(lines), uses_os
