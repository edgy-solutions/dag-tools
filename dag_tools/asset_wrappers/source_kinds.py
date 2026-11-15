"""Source-kind seam for the dlt asset factory.

``source_config.type`` selects a :class:`SourceKind`, which supplies the four
hooks the factory varies on: ``validate`` (at load), ``prepare_units``,
``build_source`` and ``extra_defs``. Only :data:`LEGACY` ships here; it
delegates to the existing ``_prepare_units`` / ``build_dlt_source``, which
stay where they are (downstream repos import them).

Resolution never fails an existing YAML: ``type`` was never read, so a value
that is missing, a legacy alias, or simply unknown resolves to LEGACY. The one
refusal is an unregistered value that is a near miss of a registered
non-legacy kind, which would otherwise fall into the SQL path and fail later
on a missing credential, naming the wrong problem.
"""
import difflib
import logging
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set

logger = logging.getLogger(__name__)

#: ``type`` values the repo's own YAML and tests already use; they select LEGACY
#: silently and are never candidates for the near-miss check.
LEGACY_ALIASES: FrozenSet[str] = frozenset({"sql_database", "postgres", "duckdb"})

_NEAR_MISS_CUTOFF = 0.8


class SourceKind:
    """Interface for one kind of dlt source. Subclass and register."""

    def validate(self, source_config: Dict[str, Any]) -> None:
        """Called at load, before units are prepared. Raise to fail the load."""

    def prepare_units(
        self,
        sources,
        source_config: Dict[str, Any],
        dest_config: Dict[str, Any],
        config: Any,
        staging: Optional[Any] = None,
        staging_config: Optional[Dict[str, Any]] = None,
        destination_override: Optional[Any] = None,
    ):
        """Same contract as ``_prepare_units``: ``(specs, units)``."""
        raise NotImplementedError

    def build_source(
        self,
        base: Dict[str, Any],
        database: str,
        schema: str,
        query_callback: Optional[Callable],
        config: Any,
        dest_driver: str,
        defer_table_reflect: bool = True,
    ):
        """Same contract as ``build_dlt_source``."""
        raise NotImplementedError

    def extra_defs(
        self,
        group_name: str,
        asset_keys: List[Any],
        source_config: Dict[str, Any],
        group_config: Any,
    ) -> Dict[str, List[Any]]:
        """Extra ``jobs`` / ``schedules`` / ``sensors`` / ``asset_checks``."""
        return {}

    def validate_group(self, group_config: Any) -> None:
        """Called at load with the pipeline's ``DltAssetGroupConfig``. Raise to fail the load."""

    def execute(
        self,
        context: Any,
        dlt: Any,
        *,
        base: Dict[str, Any],
        pipeline: Any,
        config: Any,
        run_kwargs: Dict[str, Any],
    ):
        """The body of the generated asset: yield its events.

        The default is exactly the legacy body, one ``dlt.run``. A kind that must
        choose its source at RUN time (from a run tag, or from what the destination
        already holds) overrides this. The kind is a registry singleton shared by
        every pipeline: keep no per-pipeline state on it; everything arrives here.
        """
        yield from dlt.run(context=context, **run_kwargs)

    def localize(self, source_config: Dict[str, Any], component_dir: Any) -> Dict[str, Any]:
        """Resolve component-relative paths in ``source_config``; the default changes nothing."""
        return source_config

    def schedule_job_name(self, group_name: str) -> Optional[str]:
        """Name of the job a ``schedule:`` should target, or None for ``<name>_job``."""
        return None


class LegacyKind(SourceKind):
    """Today's behaviour: dispatch by ``creds.drivername`` inside the originals."""

    def prepare_units(self, *args, **kwargs):
        from dag_tools.asset_wrappers import dlt_assets_parsing

        return dlt_assets_parsing._prepare_units(*args, **kwargs)

    def build_source(self, *args, **kwargs):
        from dag_tools.asset_wrappers import dlt_assets_parsing

        return dlt_assets_parsing.build_dlt_source(*args, **kwargs)


LEGACY: SourceKind = LegacyKind()

SOURCE_KINDS: Dict[str, SourceKind] = {}

_warned: Set[str] = set()


def register_source_kind(name: str, kind: SourceKind) -> None:
    """Register ``kind`` under ``name`` (an exact ``source_config.type`` value)."""
    SOURCE_KINDS[name] = kind


def resolve_source_kind(source_config: Dict[str, Any]) -> SourceKind:
    """Pick the kind for ``source_config``; see the module docstring for the rules."""
    value = (source_config or {}).get("type")
    if value is None or value in LEGACY_ALIASES:
        return LEGACY
    if value in SOURCE_KINDS:
        return SOURCE_KINDS[value]
    candidates = [n for n in SOURCE_KINDS if n not in LEGACY_ALIASES]
    if isinstance(value, str):
        close = difflib.get_close_matches(value, candidates, n=1, cutoff=_NEAR_MISS_CUTOFF)
        if close:
            raise ValueError(
                f"source_config type {value!r} is not a registered source kind but "
                f"resembles {close[0]!r}; fix the spelling."
            )
    key = str(value)
    if key not in _warned:
        _warned.add(key)
        logger.warning(
            "source_config type %r is not a registered source kind (registered: %s); "
            "falling back to drivername dispatch.",
            value, sorted(SOURCE_KINDS) or "none",
        )
    return LEGACY
