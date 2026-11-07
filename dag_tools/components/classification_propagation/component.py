"""Declarative component: a derived dataset's labels, composed from what it read.

Steps 4b/4c of the classification build (ADR-0064). Step 4a labels a source
dataset where an operator declares the labels; this component answers the next
question -- what a column *computed* from those sources publishes as -- for
datasets something else builds. One component instance covers a set of derived
datasets. It materializes none of them, moves no data and reads no warehouse:
it watches for their materializations, composes each output column's label from
the source columns the asset's own lineage says it read, and publishes that.

Shape, per `AGENTS.md` §2: every knob is a YAML field inside `type:` +
`attributes:`, the composition engine stays pure and Dagster-free
(`dag_tools.warehouse_source.propagate`), and this module is the only thing
here that knows about Dagster or a catalog.

**It is a sensor, not an asset.** The derived asset already exists and belongs
to whatever builds it -- dbt, dlt, a Python asset -- and a component that
wrapped it to add labels would have to own its build. The sensor launches no
runs: it publishes, records one `AssetObservation` per dataset against the
derived asset's own key, and reports what it did as its tick's skip reason.

**The labels of the sources are read back out of their own
materializations.** Step 4a publishes the whole per-column label set in its
asset's metadata, so the authority here is the same deployment's own event
log rather than a second copy of the source YAML -- which could disagree with
what was actually published -- or a query against the catalog, which would
make propagation depend on the thing it is feeding.

**Nothing is clobbered in the catalog.** Both aspects go up as PATCHes: the
classification document is added to the dataset's custom properties without
touching the description or the properties a crawler ingested, and each
column's terms are added and the document's *other* terms removed, so a
human's tags and a crawler's own terms survive. Within the document's own
vocabulary this component is the authority -- that is what makes a downgrade
actually take effect rather than piling a second ladder value onto the first.

**A failure is loud and local.** One derived dataset whose sources are not
labelled, or whose lineage names an upstream nobody declared, fails that
dataset: it publishes nothing, records the reason against the asset, and is
retried on the next tick so fixing the YAML needs no re-materialization. The
other datasets are unaffected.

**Nothing here names a database, schema, table, column, dimension or value.**
"""
# NOTE: no `from __future__ import annotations`, for the reason recorded in
# `warehouse_source.component` -- the house rule at the 1.10.19 compat floor
# is that a component module's annotations stay real objects.
import json
import os
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from dagster import (
    AssetKey,
    AssetObservation,
    DefaultSensorStatus,
    Definitions,
    MetadataValue,
    SkipReason,
    sensor,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dag_tools.warehouse_source import (
    CLASSIFICATION_ORIGIN_PROPERTY,
    CLASSIFICATION_PROPERTY,
    ColumnRef,
    DimensionSet,
    DowngradeRule,
    Propagation,
    PropagationError,
    propagate,
)

from ..warehouse_source.component import dimensions_from
from .schema import ClassificationPropagationSpec, DatasetBlock

#: Dagster's own metadata keys for a table's shape and column lineage. Spelled
#: out rather than imported: `TableMetadataSet` is private
#: (`dagster._core.definitions.metadata`), and a private import is a worse
#: dependency than a canary test asserting these are the keys it emits --
#: which is what `test_classification_propagation.py` does.
COLUMN_LINEAGE_METADATA_KEY = "dagster/column_lineage"
COLUMN_SCHEMA_METADATA_KEY = "dagster/column_schema"

#: Where step 4a publishes the label set this component reads back.
PUBLISHED_COLUMNS_METADATA_KEY = "columns"

#: How this label came to be, in the property that question is already asked
#: of. A reader who cannot tell a composed label from a declared one cannot
#: tell what it is worth -- the same reason step 4a records `board` against
#: `catalog_tag`.
PROPAGATED_ORIGIN = "propagated"

#: DataHub's own system actor, required on the audit stamp of a term
#: association. A catalog identity, not a name from any data domain.
SYSTEM_ACTOR = "urn:li:corpuser:datahub"

#: Cursor states. A dataset is reprocessed while its last outcome is a
#: failure, so an operator who fixes the configuration does not have to
#: re-materialize the asset to get the labels published.
PUBLISHED = "published"
FAILED = "failed"


def key_text(asset_key: Any) -> str:
    """An asset key as the YAML spells it: its parts joined by '/'."""
    path = getattr(asset_key, "path", asset_key)
    return "/".join(path)


def _unwrap(value: Any) -> Any:
    """The value inside a `MetadataValue`, or the value itself.

    Metadata arrives wrapped when it is read back out of the event log and
    raw when a caller hands it over directly, and both shapes reach this
    module -- the wrapping is normalization's business, not the producer's.
    """
    return getattr(value, "value", value)


def published_labels(
    metadata: Mapping[str, Any], *, source: str
) -> Dict[str, Mapping[str, Any]]:
    """Step 4a's published label set, as column -> `{dimension: values}`.

    An upstream carrying no such document fails rather than contributing
    nothing: an unlabelled source is not an unrestricted one, and composing
    only the labels that happen to be present publishes a ceiling computed
    from a subset of the inputs, which looks exactly like a correct answer.
    """
    document = _unwrap(metadata.get(PUBLISHED_COLUMNS_METADATA_KEY))
    if not isinstance(document, Mapping) or not document:
        raise PropagationError(
            f"{source!r} publishes no label set, so what its columns are "
            f"classified as is unknown. Its latest materialization or "
            f"observation carries no {PUBLISHED_COLUMNS_METADATA_KEY!r} "
            "metadata -- either it is not a labelled source, or it has not "
            "run since it became one, and in both cases propagating from it "
            "would invent a classification"
        )
    labels: Dict[str, Mapping[str, Any]] = {}
    for column, entry in document.items():
        if not isinstance(entry, Mapping) or "labels" not in entry:
            raise PropagationError(
                f"{source!r} publishes column {column!r} without a label; the "
                "label set it published is not the one this reads"
            )
        labels[str(column)] = entry["labels"] or {}
    return labels


def declared_lineage(
    metadata: Mapping[str, Any], *, sources: Sequence[str], where: str
) -> Dict[str, List[ColumnRef]]:
    """Column lineage, as output column -> the source columns it read.

    A dep naming an asset the operator did not declare as a source is
    refused. The alternative -- dropping it -- would compose that column's
    label from the sources that happen to be declared, so an upstream nobody
    labelled would quietly stop counting.
    """
    lineage = _unwrap(metadata.get(COLUMN_LINEAGE_METADATA_KEY))
    deps = getattr(lineage, "deps_by_column", None)
    if not deps:
        return {}
    known = set(sources)
    resolved: Dict[str, List[ColumnRef]] = {}
    for column, dep_list in deps.items():
        refs: List[ColumnRef] = []
        for dep in dep_list or ():
            dataset = key_text(dep.asset_key)
            if dataset not in known:
                raise PropagationError(
                    f"{where} column {str(column)!r} reads {dataset!r}, which "
                    "is not declared as one of its sources. Add it to "
                    "`sources` if it carries labels; if it does not, it is an "
                    "unlabelled input and this column cannot be classified "
                    "from what it read"
                )
            refs.append(
                ColumnRef(dataset=dataset, column=str(dep.column_name))
            )
        resolved[str(column)] = refs
    return resolved


def output_columns(
    metadata: Mapping[str, Any],
    lineage: Mapping[str, Sequence[ColumnRef]],
    *,
    where: str,
) -> Tuple[str, ...]:
    """Which columns the derived dataset has.

    Its declared schema when it has one, otherwise the columns its lineage
    names. An asset that declares neither is not propagated at all rather
    than propagated over an empty column list, which would publish nothing
    and report success.
    """
    schema = _unwrap(metadata.get(COLUMN_SCHEMA_METADATA_KEY))
    columns = [
        str(column.name) for column in (getattr(schema, "columns", None) or ())
    ]
    if columns:
        return tuple(columns)
    if lineage:
        return tuple(lineage)
    raise PropagationError(
        f"{where} names no columns: its latest materialization carries "
        f"neither {COLUMN_SCHEMA_METADATA_KEY!r} nor "
        f"{COLUMN_LINEAGE_METADATA_KEY!r}, so there is nothing to classify. "
        "Attach a table schema to the asset, which is where the names of its "
        "columns belong"
    )


def rules_for(dataset: DatasetBlock) -> List[DowngradeRule]:
    """The dataset's declared downgrades, as the engine takes them."""
    return [
        DowngradeRule(
            id=rule.id,
            owner=rule.owner,
            reason=rule.reason,
            label=dict(rule.label),
            columns=tuple(rule.columns),
        )
        for rule in dataset.downgrades
    ]


def propagate_dataset(
    dimensions: DimensionSet,
    dataset: DatasetBlock,
    *,
    derived_metadata: Mapping[str, Any],
    source_metadata: Mapping[str, Mapping[str, Any]],
) -> Propagation:
    """Compose every output column's label. The whole decision, as data.

    A source the operator declared and that has never materialized is a
    missing entry in `source_metadata`, and it fails here for the same reason
    an unlabelled column does.
    """
    where = key_text(dataset.key_parts)
    inputs: Dict[str, Dict[str, Mapping[str, Any]]] = {}
    for source in dataset.source_keys:
        if source not in source_metadata:
            raise PropagationError(
                f"{where} declares {source!r} as a source, but it has no "
                "materialization to read a label set from. A source that has "
                "never been observed cannot be propagated from"
            )
        inputs[source] = dict(
            published_labels(source_metadata[source], source=source)
        )
    lineage = declared_lineage(
        derived_metadata, sources=dataset.source_keys, where=where
    )
    columns = output_columns(derived_metadata, lineage, where=where)
    return propagate(
        dimensions,
        output_columns=columns,
        inputs=inputs,
        lineage=lineage,
        downgrades=rules_for(dataset),
        where=where,
    )


def vocabulary_terms(dimensions: DimensionSet) -> Tuple[str, ...]:
    """Every term URN the document can publish, in its own order.

    What makes the term patch exact: the terms of this vocabulary that a
    column should no longer carry are removed by name, and anything outside
    it -- a crawler's terms, a human's tags -- is left alone.
    """
    return tuple(
        value.term(dimension.id)
        for dimension in dimensions.dimensions
        for value in dimension.values
    )


def classification_document(
    propagation: Propagation, dimensions: DimensionSet
) -> Dict[str, Any]:
    """The published label set as one document, the shape step 4a publishes.

    One extra thing per column, which is the whole point of 4b/4c: how the
    label was arrived at -- the source columns it composed from, or the table
    max it fell back to -- and the downgrade that was applied, with both
    labels, if one was.
    """
    columns: Dict[str, Any] = {}
    for decision in propagation.columns:
        row = decision.as_dict()
        row["terms"] = list(dimensions.terms_for(decision.label))
        columns[decision.column] = row
    return {
        "dimensions_source": dimensions.source,
        "dimensions_digest": dimensions.digest,
        "columns": columns,
    }


def propagation_properties(
    propagation: Propagation,
    dimensions: DimensionSet,
    *,
    dataset: DatasetBlock,
    asset_key: str,
) -> Dict[str, str]:
    """The custom properties the dataset is patched with."""
    document = classification_document(propagation, dimensions)
    properties = {
        CLASSIFICATION_ORIGIN_PROPERTY: PROPAGATED_ORIGIN,
        CLASSIFICATION_PROPERTY: json.dumps(
            document, sort_keys=True, separators=(",", ":")
        ),
        "classification_sources": ",".join(dataset.source_keys),
        "column_count": str(len(propagation.columns)),
        "table_max_column_count": str(len(propagation.table_max_columns)),
        "downgraded_column_count": str(len(propagation.downgrades)),
        "dagster_asset_key": asset_key,
    }
    if dimensions.digest:
        properties["classification_digest"] = dimensions.digest
    if dimensions.source:
        properties["classification_source"] = dimensions.source
    return properties


def mcps_for(
    propagation: Propagation,
    dimensions: DimensionSet,
    *,
    dataset: DatasetBlock,
    asset_key: str,
):
    """Everything the catalog is told, as proposals. No emission here.

    Separated so a test can assert on what *would* be sent with no catalog
    running -- the lesson of §4i, where a publisher that swallowed its own
    failures made "it did not raise" look like evidence.
    """
    from datahub.metadata.schema_classes import GlossaryTermAssociationClass
    from datahub.specific.dataset import DatasetPatchBuilder

    builder = DatasetPatchBuilder(dataset.dataset_urn)
    for key, value in propagation_properties(
        propagation, dimensions, dataset=dataset, asset_key=asset_key
    ).items():
        builder.add_custom_property(key, value)
    vocabulary = vocabulary_terms(dimensions)
    for decision in propagation.columns:
        published = set(dimensions.terms_for(decision.label))
        field = builder.for_field(decision.column)
        for term in published:
            field.add_term(GlossaryTermAssociationClass(urn=term))
        for term in vocabulary:
            if term not in published:
                # Removing by name is what makes a downgrade take effect. An
                # add-only patch would leave the stricter term in place, and a
                # column carrying two values of one dimension reads as the
                # stricter of them -- the declared rule would have no effect
                # and nothing would say so.
                field.remove_term(term)
    return list(builder.build())


def _catalog_token(spec: ClassificationPropagationSpec) -> Optional[str]:
    """The PAT, from config or the standard variable. `""` is not a token.

    Resolved the same way step 4a and `DatahubLineageComponent` do it, for
    the reason recorded in `AGENTS.md` §4i: with
    METADATA_SERVICE_AUTH_ENABLED=true a missing token fails in a misleading
    way, and an empty string is sent as a credential and rejected.
    """
    token = spec.catalog.token or os.environ.get("DATAHUB_TOKEN")
    return token or None


def _graph(spec: ClassificationPropagationSpec) -> Any:
    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

    params: Dict[str, Any] = {"server": spec.catalog.server}
    token = _catalog_token(spec)
    if token:
        params["token"] = token
    return DataHubGraph(DatahubClientConfig(**params))


def emit_propagation(
    propagation: Propagation,
    dimensions: DimensionSet,
    spec: ClassificationPropagationSpec,
    dataset: DatasetBlock,
    asset_key: str,
) -> int:
    """Send the proposals. A failure fails this dataset, deliberately.

    The published label set IS the deliverable: a column whose terms do not
    land is a column a broker finds unclassified, and reporting success would
    turn a catalog outage into a silent release.
    """
    graph = _graph(spec)
    mcps = mcps_for(
        propagation, dimensions, dataset=dataset, asset_key=asset_key
    )
    for mcp in mcps:
        graph.emit_mcp(mcp)
    return len(mcps)


def observation_metadata(
    propagation: Propagation,
    dimensions: DimensionSet,
    *,
    dataset: DatasetBlock,
    emitted: Optional[int],
    dry_run: bool,
) -> Dict[str, Any]:
    """What the derived asset records about its own classification."""
    document = classification_document(propagation, dimensions)
    metadata: Dict[str, Any] = {
        "dataset_urn": dataset.dataset_urn,
        "platform": dataset.platform,
        "classification_origin": PROPAGATED_ORIGIN,
        "classification_source": dimensions.source or "",
        "classification_digest": dimensions.digest or "",
        "sources": MetadataValue.json(list(dataset.source_keys)),
        "columns": MetadataValue.json(document["columns"]),
        "column_count": MetadataValue.int(len(propagation.columns)),
        # Named so an operator can see what declaring column lineage would
        # buy: each of these columns carries the whole table's ceiling.
        "table_max_columns": MetadataValue.json(
            list(propagation.table_max_columns)
        ),
        "downgrades": MetadataValue.json(
            [applied.as_dict() for applied in propagation.downgrades]
        ),
        "dry_run": MetadataValue.bool(dry_run),
        "catalog_proposals": MetadataValue.int(emitted or 0),
    }
    unratified = dimensions.values_without_ratification
    if unratified:
        metadata["unratified_values"] = MetadataValue.json(list(unratified))
    return metadata


def failure_metadata(dataset: DatasetBlock, error: Exception) -> Dict[str, Any]:
    """What is recorded when a dataset cannot be classified.

    Recorded against the derived asset rather than only logged, because the
    question "what is this column classified as" is asked of the asset, and
    "nobody could work it out" has to be an answer it can give.
    """
    return {
        "dataset_urn": dataset.dataset_urn,
        "classification_propagated": MetadataValue.bool(False),
        "classification_failure": str(error),
        "sources": MetadataValue.json(list(dataset.source_keys)),
    }


def _cursor_of(raw: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """The sensor's own cursor: asset key -> what was last done with it."""
    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
    except ValueError:
        # A cursor this component cannot read is reset rather than crashed on:
        # the worst a reset does is republish a label set that is already
        # correct, and the alternative is a sensor that never ticks again.
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _latest(instance: Any, key: AssetKey):
    """The newest materialization record of an asset, or None.

    `fetch_materializations` rather than `get_latest_materialization_event`
    because the record carries the storage id, and the storage id is what the
    cursor compares -- a timestamp can repeat and a run id can materialize an
    asset more than once.
    """
    result = instance.fetch_materializations(key, limit=1)
    records = getattr(result, "records", None) or ()
    return records[0] if records else None


def _latest_published(instance: Any, key: AssetKey):
    """The newest event on a SOURCE asset that could carry a label set.

    Both logs, because a step-4a source is an *observable source asset*:
    observing it emits an `AssetObservation` and never a materialization, so
    a materializations-only lookup finds nothing there and every propagation
    from a real 4a source fails with "publishes no label set" -- a wiring
    failure dressed as a labelling one. Storage ids are one sequence, so
    "newest" is well defined across the two logs.

    The DERIVED side deliberately does not do this. This sensor writes its own
    `AssetObservation` against the derived asset; were that counted, it would
    be the newest event, would never match the storage id the cursor banked,
    and the dataset would republish on every tick forever.
    """
    records = []
    for fetch in (instance.fetch_materializations, instance.fetch_observations):
        result = fetch(key, limit=1)
        records.extend(getattr(result, "records", None) or ())
    if not records:
        return None
    return max(records, key=lambda record: record.storage_id)


def _event_metadata(record: Any) -> Dict[str, Any]:
    """The metadata of whichever event a record holds.

    `EventLogRecord` answers `None` for the type it is not, so one reader
    covers a materialization and an observation without asking which it got.
    """
    event = record.asset_materialization or record.asset_observation
    return dict(event.metadata) if event is not None else {}


def build_propagation_sensor(
    spec: ClassificationPropagationSpec, component_dir: Optional[str]
):
    """Factory, not a closure defined inline (`AGENTS.md` §2)."""
    datasets = list(spec.datasets)
    status = DefaultSensorStatus[spec.default_status]
    kwargs: Dict[str, Any] = {"name": spec.sensor_name, "default_status": status}
    if spec.minimum_interval_seconds is not None:
        kwargs["minimum_interval_seconds"] = spec.minimum_interval_seconds

    @sensor(**kwargs)
    def _propagate_labels(context):
        # Read at every tick, not once at load: the mounted document is the
        # authority at the moment of publishing, and a ConfigMap can move
        # between a deploy and a tick.
        dimensions = dimensions_from(
            spec.classification.dimensions_file, component_dir, required=True
        )
        if spec.classification.require_ratified:
            unratified = dimensions.values_without_ratification
            if unratified:
                raise ValueError(
                    "the classification document declares values nobody has "
                    f"ratified ({list(unratified)}), and require_ratified is "
                    "on"
                )
        cursor = _cursor_of(context.cursor)
        published: List[str] = []
        failed: List[str] = []
        waiting: List[str] = []
        unchanged = 0

        for dataset in datasets:
            asset_key = key_text(dataset.key_parts)
            record = _latest(context.instance, AssetKey(dataset.key_parts))
            if record is None:
                waiting.append(asset_key)
                continue
            seen = cursor.get(asset_key) or {}
            if (
                seen.get("storage_id") == record.storage_id
                and seen.get("status") == PUBLISHED
            ):
                unchanged += 1
                continue
            try:
                derived_metadata = _event_metadata(record)
                source_metadata: Dict[str, Mapping[str, Any]] = {}
                for source in dataset.source_keys:
                    upstream = _latest_published(
                        context.instance, AssetKey(source.split("/"))
                    )
                    if upstream is not None:
                        source_metadata[source] = _event_metadata(upstream)
                propagation = propagate_dataset(
                    dimensions,
                    dataset,
                    derived_metadata=derived_metadata,
                    source_metadata=source_metadata,
                )
                emitted = None
                if spec.catalog.server and not spec.dry_run:
                    emitted = emit_propagation(
                        propagation, dimensions, spec, dataset, asset_key
                    )
                context.instance.report_runless_asset_event(
                    AssetObservation(
                        asset_key=AssetKey(dataset.key_parts),
                        metadata=observation_metadata(
                            propagation,
                            dimensions,
                            dataset=dataset,
                            emitted=emitted,
                            dry_run=spec.dry_run,
                        ),
                    )
                )
                cursor[asset_key] = {
                    "storage_id": record.storage_id,
                    "status": PUBLISHED,
                }
                published.append(asset_key)
            except Exception as error:  # one dataset, not the tick
                context.log.error(
                    f"classification propagation failed for {asset_key}: "
                    f"{error}"
                )
                failed.append(asset_key)
                already = (
                    seen.get("storage_id") == record.storage_id
                    and seen.get("status") == FAILED
                )
                if not already:
                    # Once per materialization, not once per tick: the dataset
                    # is retried while it is failing, so an operator who fixes
                    # the configuration needs no re-materialization.
                    context.instance.report_runless_asset_event(
                        AssetObservation(
                            asset_key=AssetKey(dataset.key_parts),
                            metadata=failure_metadata(dataset, error),
                        )
                    )
                cursor[asset_key] = {
                    "storage_id": record.storage_id,
                    "status": FAILED,
                }

        context.update_cursor(json.dumps(cursor, sort_keys=True))
        parts = [f"published {len(published)}"]
        if failed:
            parts.append(f"failed {sorted(failed)}")
        if waiting:
            parts.append(f"not yet materialized {sorted(waiting)}")
        if unchanged:
            parts.append(f"unchanged {unchanged}")
        return SkipReason(", ".join(parts))

    return _propagate_labels


class ClassificationPropagationComponent(Component, Resolvable, Model):
    """Derived datasets whose column labels are composed from what they read.

    Builds one sensor. It materializes nothing, launches no runs and reads no
    warehouse: for each derived dataset it watches, it composes every output
    column's label from the source columns the asset's column lineage says it
    read, publishes those labels against the dataset's own URN, and records
    the decision against the asset.
    """

    datasets: List[Dict[str, Any]]
    """The derived datasets: each `asset_key` (its parts joined by '/'),
    `dataset_urn` (the PHYSICAL platform's, never `dagster`), `sources` (the
    upstream assets whose published labels it composes from), optional
    `description`, and optional `downgrades`, each `{id, owner, reason,
    columns, label}`."""

    classification: Dict[str, Any] = {}
    """Where the dimensions document is read from: `dimensions_file` (normally
    unset -- the mount, or DAGTOOLS_CLASSIFICATION_LEVELS) and
    `require_ratified`."""

    catalog: Dict[str, Any] = {}
    """Where the labels are published: `server`, `token`. No server means
    nothing is emitted and every decision is still recorded."""

    sensor_name: str = "classification_propagation"
    """Name of the sensor this component builds."""

    minimum_interval_seconds: Optional[int] = None
    """Minimum seconds between evaluations."""

    default_status: str = "STOPPED"
    """`RUNNING` or `STOPPED`. Stopped by default, like every other sensor
    this library ships: publishing classifications is not something a
    deployment should start doing because a code location loaded."""

    dry_run: bool = False
    """Compose and record the whole decision, and emit nothing. The recorded
    metadata is what would have been published."""

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        spec = ClassificationPropagationSpec.from_attributes(
            datasets=self.datasets,
            classification=self.classification,
            catalog=self.catalog,
            sensor_name=self.sensor_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=self.default_status,
            dry_run=self.dry_run,
        )
        component_dir = str(context.path) if getattr(context, "path", None) else None

        # Validate the declared downgrade labels against the document NOW when
        # it is there to be read, the same way step 4a validates its source
        # labels: a rule naming a value the document does not declare should
        # be heard about while the operator is still looking at the YAML. When
        # the mount is absent -- a developer's checkout -- the load succeeds
        # and the tick is what fails, with the path named.
        dimensions = dimensions_from(
            spec.classification.dimensions_file, component_dir, required=False
        )
        if dimensions is not None and not dimensions.is_empty:
            for dataset in spec.datasets:
                for rule in dataset.downgrades:
                    dimensions.validate_label(
                        dict(rule.label),
                        where=f"downgrade rule {rule.id!r}",
                    )

        return Definitions(sensors=[build_propagation_sensor(spec, component_dir)])
