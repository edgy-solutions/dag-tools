"""Declarative component: bundles of structured documents in, one Lance table per mapped entity out.

Wires up, from a single YAML block:

  * one ``multi_asset`` -- an asset per table the mapping declares, plus
    ``_rejects`` -- dynamically partitioned by bundle, with asset checks for
    the reject rate, file resolution, unknown structural paths and empty
    output;
  * a discovery sensor that adds a partition per new bundle and requests its
    run, a bounded number per tick, STOPPED unless asked;
  * a maintenance job (compaction and old-version cleanup), with an optional
    schedule, also STOPPED.

The engine, the sources and the writer are ``dag_tools.documents`` and import
no Dagster; this module is the Dagster surface over them. Nothing here knows
any document format: the mapping is the deployment's file.

Writes to one destination are serialised by a Dagster **pool** named from the
destination, whose limit is ``write_concurrency`` (default 1). Two runs of
one bundle writing at once would each delete and each append, so the bundle
would land twice -- see ``docs/DOCUMENT_TABLES.md`` for the measurements.
"""
# NOTE: intentionally NO `from __future__ import annotations` -- Dagster
# introspects the decorated functions' annotations at definition time, and
# string annotations break that resolution (the grist_ingest trap).
import hashlib
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from dagster import (
    AddDynamicPartitionsRequest,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    DynamicPartitionsDefinition,
    Failure,
    MaterializeResult,
    MetadataValue,
    OpExecutionContext,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    TableColumn,
    TableSchema,
    define_asset_job,
    job,
    multi_asset,
    op,
    sensor,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dag_tools.documents.engine import (
    BundleStats,
    compile_mapping,
    empty_tables,
    run_bundle,
)
from dag_tools.documents.lance_writer import LanceWriter
from dag_tools.documents.mapping import REJECTS_TABLE, load_mapping
from dag_tools.documents.sources import BundleRef, BundleSource, open_source

from .schema import DocumentTablesSpec

log = logging.getLogger(__name__)

#: The run tag carrying the bundle's own key; the partition key is its encoding.
BUNDLE_TAG = "dagtools/document_bundle"

#: Substrings Dagster refuses in a partition key (``INVALID_PARTITION_SUBSTRINGS``).
_INVALID_PARTITION_SUBSTRINGS = ("...", "\a", "\b", "\f", "\n", "\r", "\t", "\v", "\0")

_UNKNOWN_PATH_SAMPLE = 50


# ------------------------------------------------------------------ helpers
def partition_key_for(bundle_key: str) -> str:
    """A bundle's partition key: its key with ``/`` spelled ``__``.

    Raises ``ValueError`` for a key Dagster cannot hold as a partition key.
    """
    if not bundle_key:
        raise ValueError("a bundle key is empty")
    bad = [s for s in _INVALID_PARTITION_SUBSTRINGS if s in bundle_key]
    if bad:
        raise ValueError(f"bundle key {bundle_key!r} carries characters a partition key may not hold")
    return bundle_key.replace("/", "__")


def pool_for(destination_uri: str) -> str:
    """The pool that serialises writes to one destination.

    Named by a digest of the URI, never the URI itself: a pool name is shown
    in the UI and an object-store URI may name a bucket a deployment would
    rather not display.
    """
    digest = hashlib.sha256(destination_uri.encode("utf-8")).hexdigest()[:12]
    return f"dagtools_lance_{digest}"


def _resolve_local(uri: str, component_dir: Optional[str]) -> str:
    """A relative local path resolves against the component directory."""
    if "://" in uri:
        if uri.startswith("file://"):
            uri = uri[len("file://"):]
        else:
            return uri
    p = Path(uri)
    if not p.is_absolute() and component_dir:
        p = Path(component_dir) / p
    return str(p)


def _resolve_mapping(mapping: Any, component_dir: Optional[str]) -> Any:
    if isinstance(mapping, dict):
        return mapping
    p = Path(str(mapping))
    if not p.is_absolute() and component_dir:
        p = Path(component_dir) / p
    return p


def _ensure_pool_limit(instance: Any, pool: str, limit: int) -> None:
    """Make the pool's limit ``write_concurrency``. Best effort, never fatal."""
    try:
        storage = instance.event_log_storage
        current = storage.get_concurrency_info(pool).slot_count
        if current != limit:
            storage.set_concurrency_slots(pool, limit)
            log.info("document_tables: pool %s limit set to %d", pool, limit)
    except Exception as e:  # noqa: BLE001 -- best effort; the storage backend decides what it raises  # pragma: no cover
        log.warning("document_tables: could not set pool %s limit: %s", pool, type(e).__name__)


def _column_schema(schema: Any) -> TableSchema:
    cols = []
    for f in schema:
        md = f.metadata or {}
        t = "blob" if md.get(b"lance-encoding:blob") == b"true" else str(f.type)
        cols.append(TableColumn(name=f.name, type=t))
    return TableSchema(columns=cols)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


# ------------------------------------------------------------------ the plan
class _Plan:
    """Everything resolved at load: the mapping compiled, paths resolved, keys named."""

    def __init__(self, spec: DocumentTablesSpec, component_dir: Optional[str]):
        self.spec = spec
        self.mapping_doc = load_mapping(_resolve_mapping(spec.mapping, component_dir))
        self.cm = compile_mapping(self.mapping_doc)
        self.source_uri = _resolve_local(spec.source.uri, component_dir)
        self.destination_uri = _resolve_local(spec.destination.uri, component_dir)
        # one asset per dataset: a table's own, or the `into` several tables share
        self.tables: List[str] = list(self.cm.dataset_names) + [REJECTS_TABLE]
        unknown = [t for t in spec.checks.allow_empty if t not in self.cm.dataset_names]
        if unknown:
            raise ValueError(
                f"checks.allow_empty names tables the mapping does not declare: {unknown}; "
                f"its tables are {self.cm.dataset_names} (a shared `into` is named once, by that name)"
            )
        self.prefix = spec.prefix
        self.pool = pool_for(self.destination_uri)
        self.partition_name = f"{spec.name}_bundles"
        self.partitions_def = DynamicPartitionsDefinition(name=self.partition_name)

    def key(self, table: str) -> AssetKey:
        return AssetKey([*self.prefix, table])

    def source(self) -> BundleSource:
        return open_source(
            self.source_uri, bundle_depth=self.spec.source.bundle_depth,
            endpoint_url=self.spec.source.endpoint_url,
        )

    def writer(self) -> LanceWriter:
        return LanceWriter(self.destination_uri, self.spec.destination.storage_options or None)

    # checks ---------------------------------------------------------------
    def check_specs(self) -> List[AssetCheckSpec]:
        c = self.spec.checks
        out = [
            AssetCheckSpec(
                name="reject_rate", asset=self.key(REJECTS_TABLE),
                description=f"At most {c.max_reject_rate:.2%} of a bundle's documents rejected.",
            )
        ]
        if self.cm.expected_paths is not None:
            out.append(AssetCheckSpec(
                name="unknown_paths", asset=self.key(REJECTS_TABLE),
                description=f"At most {c.unknown_paths} structural path(s) outside expected_paths.",
            ))
        for t in self.cm.dataset_names:
            if self.cm.file_columns(t):
                out.append(AssetCheckSpec(
                    name="file_resolution_rate", asset=self.key(t),
                    description=f"At least {c.min_file_resolution_rate:.2%} of file references resolve.",
                ))
            if t not in c.allow_empty:
                out.append(AssetCheckSpec(
                    name="non_empty", asset=self.key(t),
                    description="The bundle produced at least one row for this table.",
                ))
        return out


# ------------------------------------------------------------------ bundle resolution
def _bundle_for_partition(plan: _Plan, source: BundleSource, partition_key: str, tagged: Optional[str]) -> BundleRef:
    """The bundle a partition names: the run's tag when it agrees, else a unique encoding match."""
    if tagged:
        try:
            agrees = partition_key_for(tagged) == partition_key
        except ValueError:
            agrees = False
        if not agrees:
            raise Failure(
                description=(
                    f"run tag {BUNDLE_TAG} names a bundle whose partition key is not "
                    f"{partition_key!r}; the tag and the partition disagree"
                )
            )
        return source.ref_for(tagged)
    matches = []
    for ref in source.list_bundles(start_after=plan.spec.discovery.start_after):
        try:
            if partition_key_for(ref.key) == partition_key:
                matches.append(ref)
        except ValueError:
            continue
    if len(matches) != 1:
        raise Failure(
            description=(
                f"partition {partition_key!r} matches {len(matches)} bundle(s) in the source; "
                f"exactly one is required (launch it with the {BUNDLE_TAG} tag to name it)"
            )
        )
    return matches[0]


# ------------------------------------------------------------------ results
def _table_metadata(plan: _Plan, table: str, stats: BundleStats, result: Any, writer: LanceWriter,
                    schema: Any, ingested_at: datetime, bundle_bytes: Optional[int] = None,
                    bundle_modified: Optional[str] = None) -> Dict[str, Any]:
    md: Dict[str, Any] = {
        "dagster/row_count": int(stats.rows.get(table, 0)),
        "dagster/uri": MetadataValue.path(writer.uri(table)),
        "dagster/column_schema": _column_schema(schema),
        "destination_name": "lance",
        "bundle": stats.bundle,
        "publish_period": stats.publish_period or "",
        "documents": stats.documents,
        "documents_parsed": stats.documents_parsed,
        "documents_rejected": stats.documents_rejected,
        "rows_deleted": int(result.deleted.get(table, 0)),
        "dataset_version": int(result.versions.get(table, -1)),
        "ingested_at": _iso(ingested_at),
        "duration_seconds": round(stats.duration_seconds, 3),
    }
    if bundle_bytes is not None:
        md["dagtools/bundle_bytes"] = int(bundle_bytes)
    if bundle_modified is not None:
        md["dagtools/bundle_modified"] = bundle_modified
    feeding = plan.cm.tables_of(table) if table != REJECTS_TABLE else []
    if feeding != [table] and feeding:
        md["rows_by_table"] = MetadataValue.json({t: int(stats.rows_by_table.get(t, 0)) for t in feeding})
    kept = {t: stats.members_kept[t] for t in feeding if t in stats.members_kept}
    if kept:
        # `only_unreferenced`: members no document's file column resolved to were kept
        md["members_kept"] = MetadataValue.json(kept) if feeding != [table] else kept[table]
        skipped = {t: stats.members_skipped.get(t, 0) for t in kept}
        md["members_skipped"] = MetadataValue.json(skipped) if feeding != [table] else skipped[table]
    if table in stats.coercion_failures:
        md["coercion_failures"] = MetadataValue.json(dict(stats.coercion_failures[table]))
    if table in stats.unmapped_values:
        md["unmapped_values"] = MetadataValue.json(dict(stats.unmapped_values[table]))
    if table in stats.null_scopes:
        # A lookup whose own scope came out null or empty: it matched nothing.
        md["null_scopes"] = MetadataValue.json(dict(stats.null_scopes[table]))
    if table in stats.file_resolution:
        md["file_resolution"] = MetadataValue.json(
            {col: dict(outcomes) for col, outcomes in stats.file_resolution[table].items()}
        )
    if table == REJECTS_TABLE:
        md["unknown_paths"] = len(stats.unknown_paths)
        md["documents_with_unknown_paths"] = stats.documents_with_unknown_paths
        if stats.unknown_paths:
            md["unknown_paths_sample"] = MetadataValue.json(sorted(stats.unknown_paths)[:_UNKNOWN_PATH_SAMPLE])
        md["members"] = stats.members
        md["attachment_bytes"] = stats.attachment_bytes
        md["publish_period_unmatched"] = bool(stats.publish_period_unmatched)
        # Index rows whose scope came out null or empty were filed under no
        # key at all; they belong to no table, so they are reported here.
        idx = {
            k[len("indexes."):]: int(sum(v.values()))
            for k, v in stats.null_scopes.items() if k.startswith("indexes.")
        }
        if idx:
            md["index_null_scopes"] = MetadataValue.json(idx)
        if stats.unexpanded_zips:
            md["unexpanded_zips"] = len(stats.unexpanded_zips)
    return md


def _check_results(plan: _Plan, stats: BundleStats) -> Iterator[AssetCheckResult]:
    c = plan.spec.checks
    warn = AssetCheckSeverity.WARN
    rk = plan.key(REJECTS_TABLE)

    if stats.documents == 0:
        yield AssetCheckResult(
            asset_key=rk, check_name="reject_rate", passed=False, severity=warn,
            metadata={"documents": 0, "reason": "the bundle held no document the mapping matches"},
        )
    else:
        rate = stats.documents_rejected / stats.documents
        yield AssetCheckResult(
            asset_key=rk, check_name="reject_rate", passed=rate <= c.max_reject_rate, severity=warn,
            metadata={"reject_rate": round(rate, 6), "max_reject_rate": c.max_reject_rate,
                      "documents": stats.documents, "documents_rejected": stats.documents_rejected},
        )

    if plan.cm.expected_paths is not None:
        n = len(stats.unknown_paths)
        yield AssetCheckResult(
            asset_key=rk, check_name="unknown_paths", passed=n <= c.unknown_paths, severity=warn,
            metadata={"unknown_paths": n, "allowed": c.unknown_paths},
        )

    for t in plan.cm.dataset_names:
        k = plan.key(t)
        if plan.cm.file_columns(t):
            rate = stats.file_resolution_rate(t)
            if rate is None:
                yield AssetCheckResult(
                    asset_key=k, check_name="file_resolution_rate", passed=True, severity=warn,
                    metadata={"references": 0},
                )
            else:
                yield AssetCheckResult(
                    asset_key=k, check_name="file_resolution_rate",
                    passed=rate >= c.min_file_resolution_rate, severity=warn,
                    metadata={"file_resolution_rate": round(rate, 6),
                              "min_file_resolution_rate": c.min_file_resolution_rate},
                )
        if t not in c.allow_empty:
            rows = int(stats.rows.get(t, 0))
            yield AssetCheckResult(
                asset_key=k, check_name="non_empty", passed=rows > 0, severity=warn,
                metadata={"rows": rows},
            )


# ------------------------------------------------------------------ factories
def build_tables_asset(plan: _Plan) -> Any:
    spec = plan.spec
    schemas = empty_tables(plan.cm)
    specs = [
        AssetSpec(
            key=plan.key(t),
            group_name=spec.group_name,
            owners=list(spec.owners) or None,
            tags=dict(spec.tags) or None,
            kinds={"lance"},
            description=(
                "Documents the mapping could not read, one row each, with the reason."
                if t == REJECTS_TABLE else f"Rows of `{t}`, one Lance dataset, partitioned by bundle."
                + ("" if plan.cm.tables_of(t) == [t] else
                   " Written by tables " + ", ".join(f"`{x}`" for x in plan.cm.tables_of(t)) + ".")
            ),
        )
        for t in plan.tables
    ]

    @multi_asset(
        name=f"{spec.name}_tables",
        specs=specs,
        check_specs=plan.check_specs(),
        partitions_def=plan.partitions_def,
        can_subset=False,
        pool=plan.pool,
    )
    def _tables(context: AssetExecutionContext):
        _ensure_pool_limit(context.instance, plan.pool, spec.write_concurrency)
        partition_key = context.partition_key
        tagged = context.run.tags.get(BUNDLE_TAG) if context.run else None
        source = plan.source()
        ref = _bundle_for_partition(plan, source, partition_key, tagged)
        ingested_at = datetime.now(timezone.utc)
        writer = plan.writer()
        started = time.perf_counter()
        writer.begin_bundle(ref.key, schemas)
        with source.open_bundle(ref) as bundle:
            stats = run_bundle(
                plan.cm, bundle, writer.sink,
                ingested_at=ingested_at,
                flush_rows=spec.destination.flush_rows,
                flush_attachment_bytes=spec.destination.flush_attachment_bytes,
                redact=spec.redact,
            )
            bundle_bytes, bundle_modified = bundle.size, bundle.modified
        result = writer.finish()
        stats.duration_seconds = time.perf_counter() - started
        context.log.info(
            f"document_tables: bundle {ref.key}: {stats.documents_parsed}/{stats.documents} documents "
            f"parsed, rows {dict(stats.rows)}"
        )
        for t in plan.tables:
            yield MaterializeResult(
                asset_key=plan.key(t),
                metadata=_table_metadata(plan, t, stats, result, writer, schemas[t], ingested_at,
                                         bundle_bytes, bundle_modified),
            )
        yield from _check_results(plan, stats)

    return _tables


def build_discovery_sensor(plan: _Plan, job_def: Any) -> Any:
    spec = plan.spec
    d = spec.discovery

    @sensor(
        name=f"{spec.name}_discovery",
        job=job_def,
        minimum_interval_seconds=d.interval_seconds,
        default_status=DefaultSensorStatus.RUNNING if d.default_status == "RUNNING" else DefaultSensorStatus.STOPPED,
        description="Adds a partition per new bundle and requests its run, a bounded number per tick.",
    )
    def _discover(context: SensorEvaluationContext):
        _ensure_pool_limit(context.instance, plan.pool, spec.write_concurrency)
        existing = set(context.instance.get_dynamic_partitions(plan.partition_name))
        by_partition: Dict[str, BundleRef] = {}
        refused = 0
        source = plan.source()
        for ref in source.list_bundles(start_after=d.start_after):
            try:
                pk = partition_key_for(ref.key)
            except ValueError:
                refused += 1
                context.log.warning(f"document_tables: bundle {ref.key!r} cannot be a partition key; skipped")
                continue
            if pk in by_partition:
                raise Failure(
                    description=(
                        f"bundles {by_partition[pk].key!r} and {ref.key!r} both encode to partition "
                        f"{pk!r}; rename one"
                    )
                )
            by_partition[pk] = ref
        above = source.files_above_depth
        # Order: tree bundles first, by key (their modified time is unknown at
        # discovery); then zip bundles by (modified, key).
        new: List[Tuple[str, BundleRef]] = sorted(
            ((pk, ref) for pk, ref in by_partition.items() if pk not in existing),
            key=lambda item: (
                (0, "", item[1].key) if item[1].kind == "tree" else (1, item[1].modified or "", item[1].key)
            ),
        )
        if not new:
            return SkipReason(
                f"no new bundle ({len(by_partition)} listed, {refused} refused, "
                f"{above} file(s) above the bundle depth)"
            )
        batch = new[: d.max_new_partitions_per_tick]
        context.log.info(
            f"document_tables: {len(new)} new bundle(s); requesting {len(batch)} this tick "
            f"({above} file(s) above the bundle depth are not bundles)"
        )
        return SensorResult(
            run_requests=[
                RunRequest(run_key=f"{pk}@{ref.stamp or 'tree'}", partition_key=pk, tags={BUNDLE_TAG: ref.key})
                for pk, ref in batch
            ],
            dynamic_partitions_requests=[
                AddDynamicPartitionsRequest(
                    partitions_def_name=plan.partition_name, partition_keys=[pk for pk, _ in batch]
                )
            ],
        )

    return _discover


def build_maintenance_job(plan: _Plan) -> Any:
    spec = plan.spec
    m = spec.maintenance

    @op(name=f"{spec.name}_maintain", pool=plan.pool)
    def _maintain(context: OpExecutionContext) -> None:
        _ensure_pool_limit(context.instance, plan.pool, spec.write_concurrency)
        out = plan.writer().maintain(
            plan.tables, older_than=timedelta(days=m.older_than_days), retain_versions=m.retain_versions
        )
        for table, summary in out.items():
            context.log.info(f"document_tables: maintained {table}: {summary}")

    @job(name=f"{spec.name}_maintenance", description="Compact every table and remove old versions.")
    def _maintenance():
        _maintain()

    return _maintenance


class DocumentTablesComponent(Component, Resolvable, Model):
    """Turn bundles of structured documents into one Lance table per mapped entity.

    The mapping (a path or the mapping itself, inline) says what to read and
    which tables to write; nothing in this component knows a document format.
    """

    name: str
    """Names the assets' default key prefix, the partitions, the jobs and the sensor."""

    source: Dict[str, Any]
    """`uri` (a local directory or s3://bucket/prefix), `endpoint_url`, `bundle_depth`."""

    mapping: Any
    """A path to the mapping YAML (relative paths resolve against the component
    directory) or the mapping itself, inline."""

    destination: Dict[str, Any]
    """`uri`, `storage_options`, `flush_rows`, `flush_attachment_bytes`."""

    asset_key_prefix: Optional[List[str]] = None
    checks: Dict[str, Any] = {}
    discovery: Dict[str, Any] = {}
    maintenance: Dict[str, Any] = {}
    write_concurrency: int = 1
    redact: bool = False
    group_name: Optional[str] = None
    owners: List[str] = []
    tags: Dict[str, str] = {}

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        spec = DocumentTablesSpec.from_attributes(
            name=self.name,
            source=self.source,
            mapping=self.mapping,
            destination=self.destination,
            asset_key_prefix=self.asset_key_prefix,
            checks=self.checks,
            discovery=self.discovery,
            maintenance=self.maintenance,
            write_concurrency=self.write_concurrency,
            redact=self.redact,
            group_name=self.group_name,
            owners=self.owners,
            tags=self.tags,
        )
        component_dir = str(context.path) if getattr(context, "path", None) else None
        return build_document_tables_defs(spec, component_dir)


def build_document_tables_defs(spec: DocumentTablesSpec, component_dir: Optional[str] = None) -> Definitions:
    """The definitions for one spec; the component's body, callable from Python."""
    plan = _Plan(spec, component_dir)
    tables = build_tables_asset(plan)
    ingest = define_asset_job(
        name=f"{spec.name}_ingest",
        selection=AssetSelection.assets(*[plan.key(t) for t in plan.tables]),
        partitions_def=plan.partitions_def,
    )
    maintenance = build_maintenance_job(plan)
    schedules = []
    if spec.maintenance.cron:
        schedules.append(ScheduleDefinition(
            name=f"{spec.name}_maintenance_schedule",
            job=maintenance,
            cron_schedule=spec.maintenance.cron,
            default_status=(
                DefaultScheduleStatus.RUNNING if spec.maintenance.default_status == "RUNNING"
                else DefaultScheduleStatus.STOPPED
            ),
        ))
    return Definitions(
        assets=[tables],
        jobs=[ingest, maintenance],
        sensors=[build_discovery_sensor(plan, ingest)],
        schedules=schedules,
    )


__all__ = [
    "BUNDLE_TAG",
    "DocumentTablesComponent",
    "build_document_tables_defs",
    "partition_key_for",
    "pool_for",
]
