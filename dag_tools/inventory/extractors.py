"""Walks a Dagster ``Definitions`` and produces structural inventory records.

Two consumers:
  * The Domain Broker at startup — uses ``io_manager_family`` for runtime
    routing of the data mesh.
  * The Dagster qualification survey (``dagtools survey``) at CI time — uses
    the full ``AssetRecord`` to publish per-build inventory to MinIO.

Design rules (from the recipe):

  * **Public Dagster accessors only.** Internal ``dagster._core.*`` paths
    drift across versions. If you must reach in, version-gate it and
    soft-fail per field (record ``None`` + log WARNING; never abort).
  * **Per-asset isolation.** Each asset's extraction is wrapped in
    try/except so one malformed asset never aborts the rest.
  * **Type names, not type objects.** Everything that leaves this module
    is JSON-friendly (strings, lists, dicts, bools, None).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from .classifier import classify, fqn
from .schema import AssetRecord


logger = logging.getLogger(__name__)


def extract_records(defs: Any, location: Optional[str] = None) -> List[AssetRecord]:
    """Extract one ``AssetRecord`` per asset in the given Dagster Definitions.

    Soft-fails per asset: if one asset's introspection raises, the rest still
    get extracted. Returns an empty list if ``defs`` is None or has no assets.

    Args:
      defs: A Dagster ``Definitions`` instance.
      location: Optional code-location name; written to each record so the
        downstream registry can group by location.

    Returns:
      A list of validated ``AssetRecord`` instances (one per asset that
      successfully introspected).
    """
    if defs is None:
        return []

    specs = _resolve_specs(defs)
    if not specs:
        return []

    resources = _resources_dict(defs)
    asset_check_keys = _asset_check_keys(defs)

    records: List[AssetRecord] = []
    for spec in specs:
        try:
            record = _extract_one(spec, defs, resources, asset_check_keys, location)
            records.append(record)
        except Exception as e:
            try:
                asset_key_str = ".".join(spec.key.path)
            except Exception:
                asset_key_str = "<unknown>"
            logger.exception(
                "extract_records: failed for %s: %s — skipping this asset",
                asset_key_str, e,
            )

    return records


def _resolve_specs(defs: Any) -> List[Any]:
    """Return all AssetSpecs from a Definitions, tolerating API drift.

    Dagster public APIs for "give me all asset specs" have varied:

      * 1.13+        ``defs.resolve_all_asset_specs()``
      * some pre-1.13 ``defs.get_all_asset_specs()``
      * older fallback: walk ``defs.get_repository_def().assets_defs_by_key``
        and pull ``specs`` from each ``AssetsDefinition``.

    Try each in order. Logs warnings on failure but never raises.
    """
    for attr in ("resolve_all_asset_specs", "get_all_asset_specs"):
        getter = getattr(defs, attr, None)
        if not callable(getter):
            continue
        try:
            return list(getter())
        except Exception as e:
            logger.warning("extract_records: %s() failed: %s", attr, e)

    # Last-resort fallback for pre-1.10 layouts: walk the resolved repo.
    try:
        repo = defs.get_repository_def() if hasattr(defs, "get_repository_def") else None
        if repo is None:
            return []
        specs: List[Any] = []
        assets_defs = getattr(repo, "assets_defs_by_key", None) or {}
        for ad in {id(a): a for a in assets_defs.values()}.values():
            specs_attr = getattr(ad, "specs", None)
            if specs_attr is None:
                continue
            try:
                specs.extend(specs_attr)
            except Exception:
                continue
        return specs
    except Exception as e:
        logger.error("extract_records: no usable asset-spec API: %s", e)
        return []


# ---------------------------------------------------------------------------
# Internal helpers — version-gated, soft-failure.
# ---------------------------------------------------------------------------


def _extract_one(
    spec: Any,
    defs: Any,
    resources: Dict[str, Any],
    asset_check_keys: Set[Tuple[str, ...]],
    location: Optional[str],
) -> AssetRecord:
    asset_key_path = list(spec.key.path)
    asset_key_tuple = tuple(asset_key_path)

    # In Dagster 1.13+, io_manager_key and required_resource_keys live on
    # the AssetsDefinition, not the AssetSpec. Look them up via defs.
    assets_def = _get_assets_def(defs, spec.key)

    io_manager_key = _io_manager_key(assets_def, spec.key)
    io_manager_cls = _io_manager_class(io_manager_key, resources)
    io_manager_class_fqn = fqn(io_manager_cls) if io_manager_cls else None
    io_manager_family = classify(io_manager_cls) if io_manager_cls else None

    resource_keys = _resource_keys(assets_def, io_manager_key)
    resource_classes = {
        k: fqn(type(resources[k])) for k in resource_keys if k in resources
    }
    integration_libs = _integration_libs(resources, resource_keys)

    partitions_def = _safe_attr(spec, "partitions_def")
    partitions_def_class = (
        fqn(type(partitions_def)) if partitions_def is not None else None
    )
    partition_mapping_classes = _partition_mapping_classes(spec)

    automation_condition = _safe_attr(spec, "automation_condition")
    automation_condition_type = (
        fqn(type(automation_condition)) if automation_condition is not None else None
    )

    return AssetRecord(
        asset_key=asset_key_path,
        location=location,
        is_executable=_is_executable(assets_def),
        group=_safe_attr(spec, "group_name"),
        compute_kind=_compute_kind(spec),
        code_version=_safe_attr(spec, "code_version"),
        io_manager_key=io_manager_key,
        io_manager_class=io_manager_class_fqn,
        io_manager_family=io_manager_family,
        partitions_def_class=partitions_def_class,
        partition_mapping_classes=partition_mapping_classes,
        resource_keys=resource_keys,
        resource_classes=resource_classes,
        integration_libs=integration_libs,
        automation_condition_type=automation_condition_type,
        freshness_policy=_serialize_policy(_safe_attr(spec, "freshness_policy")),
        backfill_policy=_serialize_policy(_safe_attr(spec, "backfill_policy")),
        tags=_extract_tags(spec),
        has_asset_checks=asset_key_tuple in asset_check_keys,
        job_names=[],  # job->asset mapping is hard without internals; left blank for now
        urn=_derive_urn(asset_key_path),
    )


def _is_executable(assets_def: Any) -> Optional[bool]:
    """Whether Dagster can actually materialize this asset.

    ``AssetsDefinition.is_executable`` is the public discriminator, and it
    covers both shapes that matter: an explicit ``AssetSpec`` passed to
    ``Definitions``, and the stub Dagster auto-creates when an asset
    declares ``deps=[AssetKey(...)]`` for a key defined nowhere in the
    Definitions. The second is the common one — every dlt/Sling-style
    ingest names its upstream source table that way, and the resulting
    stub is indistinguishable from a real asset by tags, group, or key
    shape. Only this flag separates them.

    Returns None (not False) when the lookup is unavailable, so a reader
    can tell "external" from "we couldn't tell" and doesn't skip real
    assets on a Dagster version whose API moved.
    """
    if assets_def is None:
        return None
    value = _safe_attr(assets_def, "is_executable", default=None)
    return bool(value) if isinstance(value, bool) else None


def _get_assets_def(defs: Any, asset_key: Any) -> Optional[Any]:
    """Look up the AssetsDefinition for a given key, tolerating API absence."""
    getter = getattr(defs, "get_assets_def", None)
    if not callable(getter):
        return None
    try:
        return getter(asset_key)
    except Exception:
        return None


def _safe_attr(obj: Any, name: str, default: Any = None) -> Any:
    """Soft-fail attribute access. Returns ``default`` on any exception."""
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _resources_dict(defs: Any) -> Dict[str, Any]:
    try:
        return dict(defs.resources or {})
    except Exception:
        return {}


def _asset_check_keys(defs: Any) -> Set[Tuple[str, ...]]:
    """Set of asset_key tuples that have at least one check defined."""
    try:
        keys: Set[Tuple[str, ...]] = set()
        getter = getattr(defs, "get_asset_check_specs", None)
        if not callable(getter):
            return keys
        for check_spec in getter():
            try:
                keys.add(tuple(check_spec.asset_key.path))
            except Exception:
                continue
        return keys
    except Exception as e:
        logger.debug("_asset_check_keys: %s", e)
        return set()


def _io_manager_key(assets_def: Any, asset_key: Any) -> Optional[str]:
    """Resolve the IO manager key for an asset.

    In Dagster 1.13+ this is reached via the AssetsDefinition:
    ``assets_def.get_io_manager_key_for_asset_key(asset_key)``. Falls back
    to the Dagster default ``"io_manager"`` when the lookup is unavailable.
    """
    if assets_def is not None:
        getter = getattr(assets_def, "get_io_manager_key_for_asset_key", None)
        if callable(getter):
            try:
                key = getter(asset_key)
                if key:
                    return key
            except Exception:
                pass
    return "io_manager"


def _io_manager_class(key: Optional[str], resources: Dict[str, Any]) -> Optional[type]:
    if not key:
        return None
    res = resources.get(key)
    if res is None:
        return None
    return type(res)


def _resource_keys(assets_def: Any, io_manager_key: Optional[str]) -> List[str]:
    """Required resource keys for an asset.

    AssetsDefinition exposes ``required_resource_keys`` as a frozenset in
    1.13+. We add ``io_manager_key`` for completeness so the resource_classes
    map always includes it.
    """
    keys: Set[str] = set()

    if assets_def is not None:
        required = _safe_attr(assets_def, "required_resource_keys")
        if required:
            try:
                keys.update(required)
            except Exception:
                pass

    if io_manager_key:
        keys.add(io_manager_key)

    return sorted(keys)


def _partition_mapping_classes(spec: Any) -> List[str]:
    """Extract FQNs of every PartitionMapping class referenced in this spec's deps."""
    classes: Set[str] = set()
    deps = _safe_attr(spec, "deps", default=[]) or []
    try:
        for dep in deps:
            mapping = _safe_attr(dep, "partition_mapping")
            if mapping is not None:
                classes.add(fqn(type(mapping)))
    except Exception:
        pass
    return sorted(classes)


def _integration_libs(resources: Dict[str, Any], resource_keys: List[str]) -> List[str]:
    """Top-level ``dagster_*`` packages this asset touches via its resources.

    Derived from the IO manager and resource module paths. Returns deduped,
    sorted list of top-level package names (e.g. ``["dagster_aws", "dagster_dbt"]``).
    """
    libs: Set[str] = set()

    for key in resource_keys:
        res = resources.get(key)
        if res is None:
            continue
        try:
            top = type(res).__module__.split(".")[0]
        except Exception:
            continue
        if top.startswith("dagster_"):  # dagster_dbt, dagster_aws, dagster_dlt, etc.
            libs.add(top)

    return sorted(libs)


def _compute_kind(spec: Any) -> Optional[str]:
    """Extract a single compute kind string from an asset spec.

    Newer Dagster exposes ``kinds`` as a set; older versions used a single
    ``compute_kind`` string. Take the first kind alphabetically for stability.
    """
    kinds = _safe_attr(spec, "kinds")
    if kinds:
        try:
            kinds_sorted = sorted(kinds)
            if kinds_sorted:
                return str(kinds_sorted[0])
        except Exception:
            pass

    return _safe_attr(spec, "compute_kind")


def _extract_tags(spec: Any) -> Dict[str, str]:
    raw = _safe_attr(spec, "tags", default={}) or {}
    try:
        return {str(k): str(v) for k, v in raw.items()}
    except Exception:
        return {}


def _serialize_policy(policy: Any) -> Optional[Dict[str, Any]]:
    """Best-effort dict serialization of a policy (freshness / backfill).

    Tries pydantic ``model_dump()`` first, then NamedTuple ``_asdict()``,
    then a typed-repr fallback. Returns None on full failure.
    """
    if policy is None:
        return None
    try:
        if hasattr(policy, "model_dump"):
            return policy.model_dump()
        if hasattr(policy, "_asdict"):
            return dict(policy._asdict())
        return {"type": fqn(type(policy)), "repr": repr(policy)[:200]}
    except Exception:
        return None


def _derive_urn(asset_key_path: List[str]) -> Optional[str]:
    """Best-effort DataHub URN derivation via the existing converter.

    Returns None if the datahub_dagster_plugin isn't installed (the
    converter module imports it conditionally) or the converter fails
    for this particular asset.

    Passes ``platform='dagster'`` to the converter explicitly — the
    inventory layer is by definition called from a Dagster context, so
    the platform is known. Without this hint, the converter defaults
    to ``'unknown'`` whenever the asset key's first segment isn't in
    its recognized-platforms list (``clickhouse / snowflake / postgres``),
    producing URNs like
    ``urn:li:dataset:(urn:li:dataPlatform:unknown,mesh_demo_customers,PROD)``.
    Downstream consumers (CortexPolarsIOManager.handle_output,
    Engine DA's CortexDataClient lookups, the DataHub emit path) all
    assemble the URN with ``platform='dagster'``, so the ``unknown``
    form coming out of the inventory walker never matches and lookups
    return 404 even when the data exists.
    """
    try:
        from dag_tools.components.datahub_lineage.component import (
            asset_keys_to_dataset_urn_converter,
        )
    except Exception:
        return None
    try:
        urn = asset_keys_to_dataset_urn_converter(
            asset_key_path, platform="dagster"
        )
        if urn is None:
            return None
        # DataHub URN classes expose .urn() for the canonical string form;
        # fall back to str() if a future version drops that method.
        if hasattr(urn, "urn"):
            return urn.urn()
        return str(urn)
    except Exception:
        return None
