"""Declarative component: a contract, a source, and the model between them.

ADR-0066 item 1. One asset per component instance. It loads the contract and
the mapping, reconciles them through the pure engine in
`dag_tools.contract_mapping`, renders the dbt model and the contract tests,
and writes them into a dbt project -- or, in a dry run, writes nothing and
puts the rendered bytes on the materialization so a mapping change can be
reviewed before it reaches a project.

Shape, per `AGENTS.md` §2 (Declarative Component First): every knob is a YAML
field, the engine stays pure and Dagster-free, and the component is the only
thing that knows about Dagster. Per §5 the YAML is a Dagster-centric schema
(`schema.py`) rather than a pass-through of dbt's or DataHub's kwargs.

**Validate at load; do IO at materialization.** When the contract is a file
and the mapping carries its own `source.columns`, the whole plan is built in
`build_defs` -- so a mapping that reads a column the source does not have, or
fails to produce a column a governed read selects, fails the CODE LOCATION
LOAD. When the contract is served over HTTP or the schema comes from the
catalog, that check cannot happen at load and the asset says so in its notes
rather than implying it ran.

**Rendering is Dagster-side, writing is the last thing that happens.** Same
discipline as the OTel mapper: everything is rendered into memory first, so a
dry run shows the exact bytes, and a render that fails leaves the dbt project
untouched instead of half-written.

**Nothing here names a column, a table, a type or a level.** The contract and
the mapping are the deployment's files; the warehouse's type names and its
non-finite predicate come from the mapping.
"""
# NOTE: intentionally NO `from __future__ import annotations` -- the mapping
# asset's `config: ContractMappingRunConfig` parameter must be a real class
# object at definition time or Dagster cannot resolve it as a pythonic Config
# type (it raises DagsterInvalidPythonicConfigDefinitionError naming the
# string). Same trap as `read_audit.component` and `grist_ingest.component`.
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Definitions,
    MetadataValue,
    asset,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dag_tools.contract_mapping.contract import Contract, load_contract
from dag_tools.contract_mapping.emit import render_dbt_artifacts
from dag_tools.contract_mapping.mapping import Mapping, load_mapping
from dag_tools.contract_mapping.plan import MappingPlan, build_mapping_plan

from .schema import ContractMappingRunConfig, ContractMappingSpec


def _resolve_path(candidate: str, component_dir: Optional[str]) -> str:
    """Component-relative first, then as given -- same as the other components."""
    if component_dir:
        joined = os.path.join(component_dir, candidate)
        if os.path.exists(joined):
            return joined
    return candidate


def _load_document(path: str, what: str) -> Dict[str, Any]:
    """Read a YAML or JSON document.

    YAML is loaded with `safe_load`, which also accepts JSON -- so one reader
    covers a hand-written contract and a served one saved to disk.
    """
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        raise ValueError(f"cannot read the {what} at {path}: {exc}") from exc

    import yaml

    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"the {what} at {path} does not parse: {exc}") from exc

    if not isinstance(document, dict):
        raise ValueError(
            f"the {what} at {path} is a "
            f"{type(document).__name__}, not a mapping"
        )
    return document


def _fetch_contract_document(spec: ContractMappingSpec) -> Dict[str, Any]:
    """Fetch the served contract.

    `requests`, not `httpx`: this runs inside a Dagster materialization, and
    the repo's private-CA plumbing wires `REQUESTS_CA_BUNDLE` for exactly
    this kind of call (see `AGENTS.md` §4 on why both vars exist and why
    neither may be dropped).
    """
    import requests

    response = requests.get(
        spec.contract.url,
        headers=dict(spec.contract.headers),
        timeout=spec.contract.timeout_seconds,
    )
    response.raise_for_status()
    document = response.json()
    if not isinstance(document, dict):
        raise ValueError(
            f"{spec.contract.url} served a {type(document).__name__}, not a "
            "contract document"
        )
    return document


def _check_expected_id(spec: ContractMappingSpec, contract: Contract) -> None:
    expected = spec.contract.expect_contract_id
    if expected and contract.contract_id != expected:
        raise ValueError(
            f"the contract served is {contract.contract_id!r}, not the "
            f"expected {expected!r}; refusing to map against a contract this "
            "component was not configured for"
        )


def _datahub_source_schema(
    spec: ContractMappingSpec, mapping: Mapping
) -> Tuple[Dict[str, str], List[str]]:
    """Read the source dataset's schema from the catalog.

    Keyed by the mapping's `source.dataset_urn`, which is the PHYSICAL
    platform's URN (ruled 2026-11-05) -- the same object a broker authorizes a
    read of, so the schema this returns describes the thing that will actually
    be selected from.
    """
    notes: List[str] = []
    urn = mapping.source.dataset_urn
    if not urn:
        raise ValueError(
            "`source_schema.datahub` is set but the mapping declares no "
            "`source.dataset_urn`, so there is no dataset to read a schema for"
        )

    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

    server = spec.source_schema.datahub_config.get("server")
    if not server:
        raise ValueError(
            "`source_schema.datahub_config.server` is required to read a "
            "schema from the catalog"
        )
    # A PAT is required whenever the GMS runs with
    # METADATA_SERVICE_AUTH_ENABLED=true. Same resolution order as
    # DatahubLineageComponent and CustomDbtProjectComponent: explicit config,
    # else the standard DATAHUB_TOKEN env var. No token key when there is
    # none -- an empty string is sent as a credential and rejected.
    token = spec.source_schema.datahub_config.get("token") or os.environ.get(
        "DATAHUB_TOKEN"
    )
    config: Dict[str, Any] = {"server": server}
    if token:
        config["token"] = token

    graph = DataHubGraph(DatahubClientConfig(**config))
    aspect = graph.get_aspect(entity_urn=urn, aspect_type=_schema_metadata_class())

    if aspect is None:
        notes.append(
            f"the catalog has no schema for {urn}, so the mapping's own "
            "`source.columns` is all that is available"
        )
        return {}, notes

    schema: Dict[str, str] = {}
    for field in getattr(aspect, "fields", None) or []:
        name = getattr(field, "fieldPath", None)
        if not name:
            continue
        # The catalog's native type is the operator's own spelling; it is
        # carried through as-is because this component compares names, never
        # types, against the mapping.
        schema[name] = getattr(field, "nativeDataType", None) or ""
    return schema, notes


def _schema_metadata_class() -> Any:
    from datahub.metadata.schema_classes import SchemaMetadataClass

    return SchemaMetadataClass


def _reconcile_schema(
    mapping: Mapping, catalog_schema: Dict[str, str]
) -> Tuple[Optional[Dict[str, str]], List[str]]:
    """Decide which schema the plan is built against, and report disagreement.

    The catalog wins when it has one: it is what the source actually is. The
    mapping's declaration is what makes the load-time check possible, so a
    disagreement between them is reported rather than quietly resolved -- an
    operator whose mapping has drifted from the source wants to hear it from
    here, not from a failed model.
    """
    notes: List[str] = []
    declared = dict(mapping.source.columns)

    if not catalog_schema:
        return (declared or None), notes

    if declared:
        missing = sorted(set(declared) - set(catalog_schema))
        extra = sorted(set(catalog_schema) - set(declared))
        if missing:
            notes.append(
                "the mapping declares source column(s) the catalog does not "
                f"carry: {', '.join(missing)}"
            )
        if extra:
            notes.append(
                f"the catalog carries {len(extra)} source column(s) the "
                "mapping does not declare, which is expected for a source "
                "wider than the contract"
            )
    return catalog_schema, notes


def _write_artifacts(
    spec: ContractMappingSpec,
    plan: MappingPlan,
    files: Dict[str, str],
    component_dir: Optional[str],
) -> Tuple[List[str], List[str]]:
    """Write the rendered files into the dbt project. Returns written, pruned.

    Rendering has already finished by the time this runs, so a render that
    fails leaves the project untouched rather than half-written.
    """
    project = _resolve_path(spec.emit.dbt_project_dir, component_dir)
    if not os.path.isdir(project):
        raise ValueError(
            f"`emit.dbt_project_dir` {project} is not a directory; this "
            "component writes into a dbt project, it does not create one"
        )

    written: List[str] = []
    for name, text in sorted(files.items()):
        if name.startswith("tests/"):
            target = os.path.join(
                project, spec.emit.tests_subdir, name[len("tests/") :]
            )
        else:
            target = os.path.join(project, spec.emit.models_subdir, name)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        written.append(os.path.relpath(target, project).replace(os.sep, "/"))

    pruned: List[str] = []
    if spec.emit.prune:
        tests_dir = os.path.join(project, spec.emit.tests_subdir)
        keep = {
            name[len("tests/") :] for name in files if name.startswith("tests/")
        }
        if os.path.isdir(tests_dir):
            for existing in sorted(os.listdir(tests_dir)):
                # Only files this component would have written are candidates:
                # it writes into a directory it does not own, and deleting an
                # operator's hand-written test is worse than a stale one.
                if not existing.startswith(f"{plan.model_name}__"):
                    continue
                if existing in keep:
                    continue
                os.remove(os.path.join(tests_dir, existing))
                pruned.append(f"{spec.emit.tests_subdir}/{existing}")

    return written, pruned


def build_contract_mapping_asset(
    spec: ContractMappingSpec, component_dir: Optional[str]
) -> AssetsDefinition:
    """Build the asset.

    A factory function, not an inline closure: Dagster introspects every
    `@asset` function parameter as an asset input, so the spec has to arrive
    through the enclosing scope (`AGENTS.md` §2).
    """
    mapping_path = _resolve_path(spec.mapping_file, component_dir)
    mapping = load_mapping(_load_document(mapping_path, "mapping"))

    # Validate as much as can be validated now. A file-backed contract plus a
    # mapping-declared schema is the whole plan, and getting it wrong should
    # fail the code location load rather than a run an hour later.
    load_notes: List[str] = []
    if spec.contract.path:
        contract = load_contract(
            _load_document(_resolve_path(spec.contract.path, component_dir), "contract")
        )
        _check_expected_id(spec, contract)
        if mapping.source.columns and not spec.source_schema.datahub:
            # Built for the exception it raises, not for its value: the plan
            # is rebuilt at materialization against whatever the contract
            # says then. This call is the load-time gate.
            build_mapping_plan(contract, mapping, dict(mapping.source.columns))
        else:
            load_notes.append(
                "the plan was not built at load time because the source "
                "schema comes from the catalog, so source columns are "
                "unverified until this asset runs"
            )
    else:
        load_notes.append(
            "the contract is served over HTTP and is fetched at "
            "materialization, so nothing was reconciled at load time -- a "
            "mapping that disagrees with it fails here, not at the code "
            "location load"
        )

    @asset(
        name=spec.asset_name,
        group_name=spec.group_name,
        io_manager_key=spec.io_manager_key,
        owners=spec.owners or None,
        tags=spec.tags or None,
        description=(
            "Reconciles a mapping with its contract and emits the dbt model "
            "and contract tests. Moves no data."
        ),
    )
    def _contract_mapping(
        context: AssetExecutionContext, config: ContractMappingRunConfig
    ) -> Dict[str, Any]:
        notes = list(load_notes)

        if spec.contract.path:
            document = _load_document(
                _resolve_path(spec.contract.path, component_dir), "contract"
            )
        else:
            document = _fetch_contract_document(spec)
        run_contract = load_contract(document)
        _check_expected_id(spec, run_contract)

        catalog_schema: Dict[str, str] = {}
        if spec.source_schema.datahub:
            catalog_schema, catalog_notes = _datahub_source_schema(spec, mapping)
            notes.extend(catalog_notes)

        source_schema, schema_notes = _reconcile_schema(mapping, catalog_schema)
        notes.extend(schema_notes)
        if not spec.source_schema.from_mapping and not catalog_schema:
            raise ValueError(
                "`source_schema.from_mapping` is false and the catalog "
                "returned no schema, so the mapping cannot be checked against "
                "the source at all"
            )

        plan = build_mapping_plan(run_contract, mapping, source_schema)
        notes.extend(plan.notes)

        files = render_dbt_artifacts(plan)

        dry_run = spec.emit.dry_run if config.dry_run is None else config.dry_run
        written: List[str] = []
        pruned: List[str] = []
        if dry_run:
            notes.append(
                f"dry run: {len(files)} file(s) rendered and nothing written"
            )
        else:
            written, pruned = _write_artifacts(spec, plan, files, component_dir)

        payload: Dict[str, Any] = {
            "contract_id": plan.contract_id,
            "contract_version": plan.contract_version,
            "model_name": plan.model_name,
            "plan_hash": plan.plan_hash,
            "dataset_urn": plan.dataset_urn,
            "source_urn": plan.source_urn,
            "columns": plan.output_column_names,
            "unmapped_declared": plan.unmapped_declared,
            "tests": [test.model_dump(mode="json") for test in plan.tests],
            "notes": notes,
            "dry_run": dry_run,
            "files": {name: text for name, text in files.items()} if dry_run else {},
            "written": written,
            "pruned": pruned,
        }
        context.add_output_metadata(_materialization_metadata(plan, payload, config))
        return payload

    return _contract_mapping


def _materialization_metadata(
    plan: MappingPlan, payload: Dict[str, Any], config: ContractMappingRunConfig
) -> Dict[str, Any]:
    """The headline numbers, on the materialization.

    The notes are published, not just counted. Every one of them names
    something that was NOT emitted -- a test not written, a check not run --
    and a count alone would leave an operator believing the artifact is
    complete.
    """
    cap = max(0, config.max_listed)

    def listed(values: Sequence[Any]) -> Any:
        if cap == 0:
            return MetadataValue.int(len(values))
        return MetadataValue.json({"count": len(values), "shown": list(values[:cap])})

    undeclared = [
        test.rule_id for test in plan.tests if not test.declared_in_contract
    ]
    metadata: Dict[str, Any] = {
        "contract_id": MetadataValue.text(plan.contract_id),
        "contract_version": MetadataValue.text(plan.contract_version or "unstated"),
        "model_name": MetadataValue.text(plan.model_name),
        "plan_hash": MetadataValue.text(plan.plan_hash),
        "columns_produced": MetadataValue.int(len(plan.columns)),
        "declared_not_produced": listed(plan.unmapped_declared),
        "tests_emitted": MetadataValue.int(len(plan.tests)),
        "tests_under_undeclared_rules": listed(sorted(set(undeclared))),
        "notes": listed(payload["notes"]),
        "dry_run": MetadataValue.bool(bool(payload["dry_run"])),
        "files_written": listed(payload["written"]),
    }
    if plan.dataset_urn:
        metadata["asset_of_record"] = MetadataValue.text(plan.dataset_urn)
    if plan.source_urn:
        metadata["source_dataset"] = MetadataValue.text(plan.source_urn)
    if payload["pruned"]:
        metadata["files_pruned"] = listed(payload["pruned"])
    if payload["dry_run"]:
        metadata["rendered"] = MetadataValue.json(payload["files"])
    return metadata


class ContractMappingComponent(Component, Resolvable, Model):
    """Map a source's columns onto a contract's, and emit the dbt artifacts.

    Moves no data: it writes a model and its tests into a dbt project (or, in
    a dry run, nothing at all) and reports what it could not check.
    """

    mapping_file: str
    """The mapping document. Relative paths resolve against the component
    directory."""

    contract: Dict[str, Any]
    """Where the contract comes from: exactly one of `path` or `url`."""

    source_schema: Dict[str, Any] = {}
    """Where the source dataset's schema comes from: the mapping's own
    `source.columns` (the default, and the only one that can fail the code
    location load) and/or the catalog."""

    emit: Dict[str, Any] = {}
    """What is written and where: `dbt_project_dir`, or `dry_run: true`."""

    asset_name: str = "contract_mapping"
    """Name of the asset this component builds."""

    group_name: Optional[str] = None
    io_manager_key: Optional[str] = None
    owners: List[str] = []
    tags: Dict[str, str] = {}

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        spec = ContractMappingSpec.from_attributes(
            asset_name=self.asset_name,
            group_name=self.group_name,
            io_manager_key=self.io_manager_key,
            owners=self.owners,
            tags=self.tags,
            mapping_file=self.mapping_file,
            contract=self.contract,
            source_schema=self.source_schema,
            emit=self.emit,
        )
        component_dir = str(context.path) if getattr(context, "path", None) else None
        return Definitions(assets=[build_contract_mapping_asset(spec, component_dir)])
