"""Declarative component: one labelled source dataset, observed not moved.

Step 4a of the classification build (ADR-0064). One component instance
describes one source dataset -- where it physically lives, which columns carry
which classification labels, who owns it, where the labels came from -- and
builds one **observable source asset** for it. Materializing that asset moves
no data: it counts the rows, names the columns, records an as-of, and publishes
the label set to the catalog against the dataset's own URN.

Shape, per `AGENTS.md` §2: every knob is a YAML field inside `type:` +
`attributes:`, the mapping and labelling engine stays pure and Dagster-free
(`dag_tools.warehouse_source`), and this module is the only thing here that
knows about Dagster, a warehouse or a catalog.

**The asset of record is the physical platform's dataset URN**, per §4l. A
`dagster`-platform URN is refused by the spec, not merely discouraged: the
catalog would describe one object, a crawler would discover a second, and a
broker asked to authorize a read of the physical one would find no labels on
it. When `catalog.emit_lineage_from_asset` is on, a `dagster` dataset node is
created *downstream* of the physical one -- the asset reads the source, so
that is the direction of the edge -- in the same fabric as the source, read
off the operator's own URN.

**The observation reads through the tagged warehouse resource**, so it appears
in query history attributed to this asset like every other read. A schema
observation is still a read; one that arrived untagged would be counted as
unexplained by step 1's audit.

**The dimensions document is read at materialization, not just at load.** The
mounted document is the authority at the moment of publishing, not at the
moment of the deploy -- a ConfigMap can move between them. It is validated at
load too, when the file is there to be read, so an operator editing YAML finds
out then; a developer's laptop with no mount loads fine and fails at run with
the path named.

**Nothing here names a database, schema, table, column, dimension or value.**
"""
# NOTE: no `from __future__ import annotations`. The house reason is that a
# component's `config:` parameter must be a real class object at definition
# time -- but this component has no run config at all, and that is itself
# measured rather than assumed: dagster calls an observe function as
# `observe_fn(context, **resource_kwargs)` (`_core/definitions/source_asset.py`),
# so only resources are injected. A pythonic `config:` parameter on an
# observable source asset raises `TypeError: ... missing 1 required positional
# argument: 'config'` at RUN time, not at definition time -- identical on
# dagster 1.10.19 and 1.13.1. Every knob therefore arrives as a YAML
# attribute, which is where this library wants them anyway.
import hashlib
import os
from typing import Any, Dict, List, Optional

from dagster import (
    DataVersion,
    Definitions,
    MetadataValue,
    ObserveResult,
    SourceAsset,
    observable_source_asset,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dag_tools.warehouse_source import (
    CatalogPayload,
    DimensionSet,
    DimensionsError,
    build_catalog_payload,
    env_of,
    label_source,
    observe,
    plan_observation,
    read_as_of,
    read_column_names,
    read_row_count,
    resolve_dimensions_path,
)

from .schema import WarehouseSourceSpec

#: The platform a Dagster asset appears under when it is drawn as a lineage
#: node. It is never the asset of record -- see the module docstring.
DAGSTER_PLATFORM = "dagster"

#: DataHub's own system actor, required on the audit stamp of a term
#: association. A catalog identity, not a name from any data domain.
SYSTEM_ACTOR = "urn:li:corpuser:datahub"


def _resolve_path(candidate: str, component_dir: Optional[str]) -> str:
    """Resolve a configured path relative to the component's own directory.

    Same helper as `read_audit`: a relative path in a component file means
    "next to this file", which is what makes an example runnable from a
    checkout.
    """
    if os.path.isabs(candidate) or not component_dir:
        return candidate
    return os.path.join(component_dir, candidate)


def load_dimensions(path: str) -> DimensionSet:
    """Read and validate the mounted document. The only IO in the label path.

    The digest is of the bytes as they were read, not of the parsed document:
    the question an auditor asks of a published label is which document it was
    published against, and two documents that parse the same are the same
    answer while two files that differ are two answers.
    """
    import yaml

    with open(path, "rb") as handle:
        raw = handle.read()
    digest = hashlib.sha256(raw).hexdigest()[:12]
    document = yaml.safe_load(raw.decode("utf-8")) if raw.strip() else None
    return DimensionSet.from_document(document, source=path, digest=digest)


def dimensions_for(
    spec: WarehouseSourceSpec,
    component_dir: Optional[str],
    *,
    required: bool,
) -> Optional[DimensionSet]:
    """The document this source's labels are validated against.

    `required=False` at load: a code location must not fail to load because a
    cluster mount is absent from a developer's machine. `required=True` at
    materialization: deny by default means an absent document classifies
    everything as most restrictive, and publishing a label set validated
    against no document would instead claim a classification nobody ratified.
    So the materialization fails with the path named.
    """
    configured = spec.classification.dimensions_file
    explicit = (
        _resolve_path(configured, component_dir) if configured else None
    )
    path = resolve_dimensions_path(explicit)
    if not os.path.exists(path):
        if not required:
            return None
        raise DimensionsError(
            f"the classification dimensions document is not at {path!r}. It is "
            "the deployment's own file -- dag-tools ships none -- and without "
            "it every column is most restrictive in every dimension, so there "
            "is nothing this component could publish that a reader should "
            "trust. Mount it, or point at it with "
            "DAGTOOLS_CLASSIFICATION_LEVELS"
        )
    return load_dimensions(path)


def _catalog_token(spec: WarehouseSourceSpec) -> Optional[str]:
    """The PAT, from config or the standard variable. `""` is not a token.

    Resolved the same way `DatahubLineageComponent` does it, and for the
    reason recorded in `AGENTS.md` §4i: with
    METADATA_SERVICE_AUTH_ENABLED=true a missing token fails in a misleading
    way, and an empty string is sent as a credential and rejected.
    """
    token = spec.catalog.token or os.environ.get("DATAHUB_TOKEN")
    return token or None


def _graph(spec: WarehouseSourceSpec) -> Any:
    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

    params: Dict[str, Any] = {"server": spec.catalog.server}
    token = _catalog_token(spec)
    if token:
        params["token"] = token
    return DataHubGraph(DatahubClientConfig(**params))


def _mcps_for(payload: CatalogPayload, spec: WarehouseSourceSpec, asset_key: str):
    """Everything the catalog is told, as proposals. No emission here.

    Separated so a test can assert on what *would* be sent without a catalog
    running -- the lesson of §4i, where a publisher that swallowed its own
    failures made "execute() did not raise" look like evidence.

    No `schemaMetadata`: this component reads column *names*, not types, and a
    schema aspect claiming a type for every column would be a guess written
    into the catalog as fact. The crawler that owns the platform fills that in.
    The terms ride on `editableSchemaMetadata`, which carries a field path and
    needs no type.
    """
    import time

    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DatasetLineageTypeClass,
        DatasetPropertiesClass,
        EditableSchemaFieldInfoClass,
        EditableSchemaMetadataClass,
        GlossaryTermAssociationClass,
        GlossaryTermsClass,
        StatusClass,
        SubTypesClass,
        UpstreamClass,
        UpstreamLineageClass,
    )

    urn = payload.dataset_urn
    stamp = AuditStampClass(time=int(time.time() * 1000), actor=SYSTEM_ACTOR)
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetPropertiesClass(
                description=payload.description,
                customProperties=dict(payload.custom_properties),
            ),
        ),
        MetadataChangeProposalWrapper(entityUrn=urn, aspect=StatusClass(removed=False)),
        MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=SubTypesClass(typeNames=["Table"])
        ),
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=EditableSchemaMetadataClass(
                created=stamp,
                lastModified=stamp,
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=column.name,
                        glossaryTerms=GlossaryTermsClass(
                            terms=[
                                GlossaryTermAssociationClass(urn=term)
                                for term in column.terms
                            ],
                            auditStamp=stamp,
                        ),
                    )
                    for column in payload.columns
                    if column.terms
                ],
            ),
        ),
    ]
    if spec.catalog.emit_lineage_from_asset:
        observer = (
            f"urn:li:dataset:(urn:li:dataPlatform:{DAGSTER_PLATFORM},"
            f"{asset_key},{env_of(urn)})"
        )
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=observer,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=urn, type=DatasetLineageTypeClass.COPY
                        )
                    ]
                ),
            )
        )
    return mcps


def emit_payload(
    payload: CatalogPayload, spec: WarehouseSourceSpec, asset_key: str
) -> int:
    """Send the proposals. A failure fails the materialization, deliberately.

    The opposite call from `CustomDbtProjectComponent._publish_to_datahub`,
    which swallows everything so a catalog outage cannot fail a dbt build.
    There, the build is the deliverable and the catalog is a bonus. Here the
    published label set IS the deliverable: if it does not land, a broker finds
    no labels on the dataset and denies every read of it, and a component that
    reported success would have turned an outage into a silent embargo.
    """
    graph = _graph(spec)
    mcps = _mcps_for(payload, spec, asset_key)
    for mcp in mcps:
        graph.emit_mcp(mcp)
    return len(mcps)


def _observation_metadata(
    payload: CatalogPayload,
    observation: Any,
    dimensions: DimensionSet,
    *,
    emitted: Optional[int],
    dry_run: bool,
) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {
        "dataset_urn": payload.dataset_urn,
        "platform": payload.platform,
        "column_count": MetadataValue.int(len(payload.columns)),
        "schema_hash": observation.schema_hash,
        "as_of": observation.as_of,
        "observed_at": observation.observed_at,
        "data_version": observation.data_version,
        "classification_source": dimensions.source or "",
        "classification_digest": dimensions.digest or "",
        "columns": MetadataValue.json(payload.as_json()["columns"]),
        "defaulted_columns": MetadataValue.json(list(payload.defaulted_columns)),
        "defaulted_column_count": MetadataValue.int(len(payload.defaulted_columns)),
        "dry_run": MetadataValue.bool(dry_run),
    }
    if observation.row_count is not None:
        metadata["row_count"] = MetadataValue.int(observation.row_count)
    unratified = dimensions.values_without_ratification
    if unratified:
        # Visible rather than fatal unless the operator asked for fatal: a
        # value nobody has signed yet is still a value the document declares.
        metadata["unratified_values"] = MetadataValue.json(list(unratified))
    metadata["catalog_proposals"] = MetadataValue.int(emitted or 0)
    return metadata


def build_warehouse_source_asset(
    spec: WarehouseSourceSpec, component_dir: Optional[str]
) -> SourceAsset:
    """Factory, not a closure defined inline (`AGENTS.md` §2).

    The statements are rendered once, here, so the bytes that will be sent are
    fixed at load and a dry run shows exactly them -- the same render-then-send
    split `otel_api_sync` keeps between Dagster and Restate.
    """
    plan = plan_observation(
        table=spec.source.table,
        row_count_statement=spec.observation.row_count_statement,
        columns_statement=spec.observation.columns_statement,
        as_of_statement=spec.observation.as_of_statement,
    )
    asset_key = "/".join(list(spec.key_prefix) + [spec.asset_name])

    @observable_source_asset(
        name=spec.asset_name,
        key_prefix=list(spec.key_prefix) or None,
        group_name=spec.group_name,
        tags=spec.tags or None,
        required_resource_keys={spec.resource_key},
        metadata={
            # On the definition, so the URN and the access mode are visible in
            # the UI without running anything.
            "dataset_urn": spec.source.dataset_urn,
            "platform": spec.source.platform,
            "table": spec.source.table,
            "access": spec.source.access,
            "owner": spec.source.owner,
            "label_origin": spec.source.label_origin,
        },
        description=(
            "A classified source dataset, observed rather than moved: row "
            "count, schema and as-of, with every column's classification "
            "label published to the catalog."
        ),
    )
    def _warehouse_source(context) -> ObserveResult:
        dimensions = dimensions_for(spec, component_dir, required=True)
        assert dimensions is not None  # required=True raises instead
        if not dimensions.is_usable:
            raise DimensionsError(
                f"the document at {dimensions.source!r} declares a dimension "
                "with no values, so a label cannot be validated against it. "
                "Deny by default means nothing would be served; nothing is "
                "published either, rather than publishing a classification "
                "nobody ratified"
            )
        unratified = dimensions.values_without_ratification
        if spec.classification.require_ratified and unratified:
            raise DimensionsError(
                f"the document at {dimensions.source!r} declares values with no "
                f"ratified_by and require_ratified is on: {list(unratified)}. "
                "Either the board signs them or this deployment stops claiming "
                "its labels are final"
            )
        labelled = label_source(spec.source, dimensions)

        resource = getattr(context.resources, spec.resource_key, None)
        if resource is None:
            raise ValueError(
                f"no resource '{spec.resource_key}' is bound; the observation "
                "reads through the tagged warehouse resource so that it lands "
                "in query history attributed to this asset"
            )
        fetch_columns = getattr(resource, "fetch_columns", None)
        if fetch_columns is None:
            raise ValueError(
                f"resource '{spec.resource_key}' does not expose "
                "fetch_columns(statement); a schema observation needs the "
                "names a statement projects, which is not the same seam as "
                "fetch_rows -- an empty result set has no rows to read them off"
            )
        columns = read_column_names(fetch_columns(plan.columns_statement))

        row_count = None
        if spec.observation.count_rows:
            row_count = read_row_count(resource.fetch_rows(plan.row_count_statement))
        as_of = None
        if plan.as_of_statement:
            as_of = read_as_of(resource.fetch_rows(plan.as_of_statement))

        observation = observe(
            column_names=columns, row_count=row_count, as_of=as_of
        )
        resolved = labelled.resolve(observation.column_names)
        payload = build_catalog_payload(
            spec=spec.source,
            resolved=resolved,
            observation=observation,
            dimensions=dimensions,
            asset_key=asset_key,
            description=spec.catalog.description,
        )

        emitted: Optional[int] = None
        if spec.dry_run:
            context.log.info(
                "dry run: %d proposals withheld for %s",
                len(payload.columns),
                payload.dataset_urn,
            )
        elif not spec.catalog.server:
            context.log.info(
                "no catalog server configured; %s was observed and not published",
                payload.dataset_urn,
            )
        else:
            emitted = emit_payload(payload, spec, asset_key)
            context.log.info(
                "published %d proposals for %s", emitted, payload.dataset_urn
            )
        if resolved.defaulted:
            context.log.warning(
                "%d column(s) carry no declared label and are published as most "
                "restrictive: %s",
                len(resolved.defaulted),
                list(resolved.defaulted),
            )

        return ObserveResult(
            data_version=DataVersion(observation.data_version),
            metadata=_observation_metadata(
                payload,
                observation,
                dimensions,
                emitted=emitted,
                dry_run=spec.dry_run,
            ),
        )

    return _warehouse_source


class WarehouseSourceComponent(Component, Resolvable, Model):
    """One classified source dataset, observed rather than moved.

    Builds one observable source asset. It writes nothing to the warehouse and
    copies nothing out of it: it counts, names the columns, records an as-of,
    and publishes the label set against the dataset's own URN.
    """

    source: Dict[str, Any]
    """The dataset: `dataset_urn` (the PHYSICAL platform's, never `dagster`),
    `table`, `access` (brokered | passthrough), `owner`, `label_origin`
    (board | catalog_tag) and `columns`, each `{name, labels, cell_predicate?}`
    where `labels` is `{<dimension>: <value|values>}` including the
    hierarchical dimension."""

    asset_name: str
    """Name of the observable source asset this component builds."""

    key_prefix: List[str] = []
    """Optional asset key prefix."""

    observation: Dict[str, Any] = {}
    """How it is looked at: `count_rows`, and your own `row_count_statement` /
    `columns_statement` / `as_of_statement` with `{table}` in them."""

    classification: Dict[str, Any] = {}
    """Where the dimensions document is read from: `dimensions_file` (normally
    unset -- the mount, or DAGTOOLS_CLASSIFICATION_LEVELS) and
    `require_ratified`."""

    catalog: Dict[str, Any] = {}
    """Where the dataset is published: `server`, `token`, `description`,
    `emit_lineage_from_asset`. No server means nothing is emitted."""

    resource_key: str = "warehouse"
    """The tagged warehouse resource the observation reads through."""

    group_name: Optional[str] = None
    tags: Dict[str, str] = {}

    dry_run: bool = False
    """Observe and record the whole label set in the asset's metadata, and emit
    nothing. The metadata is what would have been published."""

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        spec = WarehouseSourceSpec.from_attributes(
            asset_name=self.asset_name,
            key_prefix=self.key_prefix,
            group_name=self.group_name,
            tags=self.tags,
            resource_key=self.resource_key,
            dry_run=self.dry_run,
            source=self.source,
            observation=self.observation,
            classification=self.classification,
            catalog=self.catalog,
        )
        component_dir = str(context.path) if getattr(context, "path", None) else None

        # Validate the labels against the document NOW when it is there to be
        # read: an operator editing this YAML should hear about a value the
        # document does not declare while they are still looking at it. When
        # the mount is absent -- a developer's checkout -- the load succeeds and
        # the materialization is what fails, with the path named.
        dimensions = dimensions_for(spec, component_dir, required=False)
        if dimensions is not None and not dimensions.is_empty:
            label_source(spec.source, dimensions)

        return Definitions(
            assets=[build_warehouse_source_asset(spec, component_dir)]
        )
