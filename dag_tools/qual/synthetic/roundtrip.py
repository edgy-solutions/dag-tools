"""Source generation for the Q2 IO round-trip probe (one module per RUNNABLE class).

Q2 launches each representative alone and records what ``handle_output`` did;
nothing reads the output back. The round-trip probe closes that: it loads the
output the side's Q2 run just wrote, through the class's real IO manager and
Dagster's own input-loading path, asserts the value is not None, and records
its type and columns.

Design points (see ``docs/plans/q2-roundtrip-probe.md``):

  * The probe declares the representative's asset as an external
    ``AssetSpec`` bound to the real IO manager, plus a tiny downstream that
    takes it as an input. Input loading happens before the body runs.
  * The downstream returns ``MaterializeResult``, so no value reaches an IO
    manager and nothing is written to the deployment's storage.
  * The fallback for an unimportable IO manager is NOT ``InMemoryIOManager``:
    it would fail input loading with a misleading missing-key error before the
    body ran. ``_ImportFailedIOManager.load_input`` raises the import error.
  * A partitioned class gets a ``DynamicPartitionsDefinition`` stand-in:
    generation is offline and cannot know the partition key the Q2 run used.
  * The module must not import ``dag_tools.qual.*``: it ships in a separate
    code location with its own deploy cadence.
  * Env references in the IO manager config render as ``os.environ[...]`` and
    are never resolved at generation time.
"""
from __future__ import annotations

from textwrap import dedent
from typing import List, Optional, Tuple

from ..classes import EquivalenceClass, Runnability
from ..qualify.manifest import ProbeSettings
from .generator import _render_construct
from .schema import RoundTripNotApplicable, RoundTripProbe


MODULE_PREFIX = "roundtrip_"
PROBE_ASSET_PREFIX = "dagtools_roundtrip_"
PARTITIONS_DEF_PREFIX = "dagtools_rt_"

_TIME_WINDOW_MARKERS = ("timewindow", "daily", "weekly", "monthly", "hourly", "timepartitions")


def roundtrip_module_name(class_hash: str) -> str:
    return f"{MODULE_PREFIX}{class_hash[:8]}"


def roundtrip_probe_asset_key(class_hash: str) -> List[str]:
    return [f"{PROBE_ASSET_PREFIX}{class_hash[:8]}"]


def first_runnable_rep(cls: EquivalenceClass):
    """The class's first RUNNABLE representative, or None."""
    for rep in cls.representatives:
        if rep.runnability == Runnability.RUNNABLE:
            return rep
    return None


def roundtrip_not_applicable_reason(cls: EquivalenceClass) -> Optional[str]:
    """Why a RUNNABLE class gets no round-trip probe, or None when it does.

    dbt assets yield no value for an IO manager to store, so there is no
    ``load_input`` to test. Detection matches the class key's own
    (``dagster_dbt`` in ``integration_libs`` or a ``dbt*`` compute kind).
    """
    key = cls.key
    if "dagster_dbt" in (key.integration_libs or []) or (
        (key.compute_kind or "").lower().startswith("dbt")
    ):
        return "dbt class: no value for an IO manager to load"
    if not key.io_manager_class:
        return "no IO manager class recorded for this class"
    return None


def generate_roundtrip_module(
    cls: EquivalenceClass,
    *,
    qual_id: str,
    probe_settings: Optional[ProbeSettings] = None,
) -> Tuple[Optional[RoundTripProbe], str]:
    """Build a :class:`RoundTripProbe` and its source for one class.

    Returns ``(probe, source)``, or ``(None, reason)`` when the class gets
    none (not RUNNABLE, or not applicable).
    """
    # Function-scope import: ``runs`` imports this package at module load.
    from ..runs.state import rep_id_for

    rep = first_runnable_rep(cls)
    if rep is None:
        return None, "class has no RUNNABLE representative"
    reason = roundtrip_not_applicable_reason(cls)
    if reason is not None:
        return None, reason

    io_fqn = cls.key.io_manager_class
    settings = probe_settings or ProbeSettings()
    io_config = settings.config_for(cls.class_hash, io_fqn)

    notes: List[str] = []
    if io_fqn and not settings.has_config_for(cls.class_hash, io_fqn):
        notes.append(
            f"no constructor config supplied for `{io_fqn}`; the probe "
            "constructs it with no arguments and will fail at launch if the "
            "class requires config"
        )

    partitions_class = cls.key.partitions_def_class
    short = cls.class_hash[:8]
    partitions_name = f"{PARTITIONS_DEF_PREFIX}{short}" if partitions_class else None
    if partitions_class:
        lowered = partitions_class.lower()
        if any(marker in lowered for marker in _TIME_WINDOW_MARKERS):
            notes.append(
                f"partitions def `{partitions_class}` is time-based; the probe "
                "uses a dynamic stand-in, so an IO manager whose loader reads "
                "`context.asset_partitions_time_window` will fail on both sides"
            )
        if "multi" in lowered:
            notes.append(
                f"partitions def `{partitions_class}` is multi-dimensional; "
                "dimensions are not mirrored, the probe uses a single dynamic "
                "stand-in keyed by the partition key the Q2 run used"
            )

    module_name = roundtrip_module_name(cls.class_hash)
    source = generate_roundtrip_source(
        class_hash=cls.class_hash,
        asset_key=list(rep.asset_key),
        io_manager_fqn=io_fqn,
        io_manager_config=io_config,
        partitions_def_name=partitions_name,
        qual_id=qual_id,
        notes=notes,
    )
    probe = RoundTripProbe(
        class_hash=cls.class_hash,
        rep_id=rep_id_for(cls.class_hash, list(rep.asset_key)),
        asset_key=list(rep.asset_key),
        module_name=module_name,
        probe_asset_key=roundtrip_probe_asset_key(cls.class_hash),
        partitions_def_name=partitions_name,
        io_manager_class=io_fqn,
        io_manager_config_keys=sorted(io_config),
        notes=notes,
    )
    return probe, source


def not_applicable_entry(cls: EquivalenceClass) -> Optional[RoundTripNotApplicable]:
    reason = roundtrip_not_applicable_reason(cls)
    if reason is None:
        return None
    return RoundTripNotApplicable(class_hash=cls.class_hash, reason=reason)


def generate_roundtrip_source(
    *,
    class_hash: str,
    asset_key: List[str],
    io_manager_fqn: Optional[str],
    io_manager_config: Optional[dict],
    partitions_def_name: Optional[str],
    qual_id: str,
    notes: List[str],
) -> str:
    """Return the Python source text for one round-trip probe module."""
    short = class_hash[:8]
    probe_name = f"{PROBE_ASSET_PREFIX}{short}"
    module_path, _, class_name = (io_manager_fqn or "").rpartition(".")
    construct, uses_os = _render_construct(io_manager_config or {})
    if not module_path or not class_name:
        import_block = (
            "_IO_MANAGER_CLASS = None\n"
            f"_IO_MANAGER_ERROR = {('malformed io_manager_class FQN: ' + repr(io_manager_fqn))!r}\n"
            "_IO_MANAGER = _ImportFailedIOManager(_IO_MANAGER_ERROR)"
        )
    else:
        import_block = (
            "try:\n"
            f"    from {module_path} import {class_name} as _IO_MANAGER_CLASS\n"
            f"{construct}\n"
            "    _IO_MANAGER_ERROR = None\n"
            "except Exception as _e:  # noqa: BLE001\n"
            "    # The location must LOAD. The failure surfaces when the probe\n"
            "    # loads its input, as the import error itself.\n"
            "    _IO_MANAGER_ERROR = repr(_e)\n"
            "    _IO_MANAGER = _ImportFailedIOManager(_IO_MANAGER_ERROR)"
        )
    os_import = "import os\n" if uses_os and module_path and class_name else ""
    notes_block = "\n".join(f"#   - {n}" for n in notes) if notes else "#   (none)"

    if partitions_def_name:
        partitions_def_line = (
            f"_PARTITIONS = DynamicPartitionsDefinition(name={partitions_def_name!r})\n"
        )
        partitions_arg = ", partitions_def=_PARTITIONS"
    else:
        partitions_def_line = "_PARTITIONS = None\n"
        partitions_arg = ""

    return dedent(f'''\
"""Q2 round-trip probe for equivalence class {class_hash}.

Auto-generated by `dagtools qual synthetic` for qual_id={qual_id!r}.

Loads the output the representative's Q2 run wrote, through the class's
real IO manager, and records the loaded value's type and columns. It writes
nothing: the probe returns MaterializeResult. It does not check values.

Generation notes:
{notes_block}
"""
from __future__ import annotations

{os_import}from dagster import (
    AssetIn,
    AssetSpec,
    Definitions,
    DynamicPartitionsDefinition,
    IOManager,
    MaterializeResult,
    MetadataValue,
    asset,
)

CLASS_HASH = "{class_hash}"
REP_ASSET_KEY = {list(asset_key)!r}
IO_MANAGER_KEY = "io_manager_rt_{short}"


class _ImportFailedIOManager(IOManager):
    """Stands in when the real IO manager could not be imported or built.

    Input loading runs before the probe body, so a fallback that merely
    lacked the object would fail with a missing-key error and hide the
    import error. This raises the import error itself."""

    def __init__(self, error):
        self._error = error

    def handle_output(self, context, obj):
        raise RuntimeError("probe IO manager unavailable: " + self._error)

    def load_input(self, context):
        raise RuntimeError("probe IO manager could not be imported: " + self._error)


{import_block}

{partitions_def_line}
_UPSTREAM = AssetSpec(REP_ASSET_KEY{partitions_arg}).with_io_manager_key(IO_MANAGER_KEY)


@asset(
    key={[probe_name]!r},
    ins={{"up": AssetIn(key=REP_ASSET_KEY)}},
    description="Loads the representative's output through its IO manager.",
    {("partitions_def=_PARTITIONS," if partitions_def_name else "")}
)
def {probe_name}(up) -> MaterializeResult:
    if _IO_MANAGER_ERROR is not None:
        raise RuntimeError(
            "probe IO manager could not be imported: " + _IO_MANAGER_ERROR
        )
    if up is None:
        raise RuntimeError("IO manager loaded None")
    metadata = {{
        "dagtools/loaded_type": type(up).__module__ + "." + type(up).__qualname__,
    }}
    try:
        columns = list(up.columns)
        if all(isinstance(c, str) for c in columns):
            metadata["dagtools/loaded_columns"] = MetadataValue.json(columns)
    except Exception:  # noqa: BLE001 - the value simply has no columns
        pass
    try:
        metadata["dagtools/loaded_len"] = int(len(up))
    except Exception:  # noqa: BLE001 - the value simply has no length
        pass
    return MaterializeResult(metadata=metadata)


defs = Definitions(
    assets=[_UPSTREAM, {probe_name}],
    resources={{IO_MANAGER_KEY: _IO_MANAGER}},
)
''')
