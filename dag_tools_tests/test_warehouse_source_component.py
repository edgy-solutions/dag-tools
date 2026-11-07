"""`WarehouseSourceComponent` -- the YAML surface, loaded the way a deployment loads it.

The component is the primary surface: a deployment writes `component.yaml`,
not Python. So these tests go through `build_component_defs` over the real
example rather than constructing the class, which is the only way to catch the
failures that matter here -- a field Dagster rejects, a block that is accepted
and then ignored, an observe function Dagster will not call.

**The example is part of the contract.** A component whose one worked example
cannot load has never been run, which is exactly how the dbt example shipped
pointing at two projects that did not exist.

**The compat floor is probed, not assumed.** 1.10.19 -- the version production
runs -- rejects unknown keys in a component file outright and fails the whole
code-location load. This module loads the example on whatever Dagster is
installed and, if the load is refused for that reason, skips while naming the
field. Anything else that goes wrong still fails loudly.

Every relation, column, dimension and value in the fixtures belongs to the
example; the warehouse is sqlite and the catalog is a recorder.
"""

import importlib.util
import shutil
from pathlib import Path

import dagster as dg
import pytest
from dagster.components import build_component_defs

from dag_tools.components.warehouse_source import (
    WarehouseSourceComponent,
    WarehouseSourceSpec,
    build_warehouse_source_asset,
)
from dag_tools.warehouse_source import DimensionsError, SourceSpecError

EXAMPLE_ROOT = Path(__file__).resolve().parents[1] / "examples" / "warehouse_source"
EXAMPLE = EXAMPLE_ROOT / "dagster_home" / "components"
DIMENSIONS = EXAMPLE_ROOT / "classification" / "dimensions.yaml"


def _chain_text(exc: BaseException) -> str:
    parts = []
    seen = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _skip_if_the_component_file_is_refused(exc: Exception) -> None:
    """Skip when this Dagster will not accept this component file.

    Matched on the raised error rather than on a version number: the field the
    loader rejects is the fact, and a version is only a proxy for it.
    """
    text = _chain_text(exc)
    if "extra_forbidden" in text or "Extra inputs are not permitted" in text:
        pytest.skip(f"this Dagster rejects a key in the component file: {text}")


def _load(components_dir: Path) -> dg.Definitions:
    try:
        return build_component_defs(components_dir)
    except Exception as exc:  # pragma: no cover - only on an old Dagster
        _skip_if_the_component_file_is_refused(exc)
        raise


def _seed_module():
    spec = importlib.util.spec_from_file_location(
        "warehouse_source_example_seed", EXAMPLE_ROOT / "seed.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def example(tmp_path, monkeypatch) -> Path:
    """A copy of the shipped example, against a freshly seeded sqlite file."""
    monkeypatch.syspath_prepend(str(tmp_path.parent))
    monkeypatch.setenv(
        "WAREHOUSE_SOURCE_DB", _seed_module().seed(tmp_path / "source.db")
    )
    monkeypatch.setenv("DAGTOOLS_CLASSIFICATION_LEVELS", str(DIMENSIONS))
    target = tmp_path / "components"
    shutil.copytree(EXAMPLE, target, ignore=shutil.ignore_patterns("__pycache__"))
    return target


def _observe(defs: dg.Definitions, *, raise_on_error: bool = True):
    """Run the source observation. Source assets need naming in a selection.

    `AssetSelection.all()` does not include them, which is worth encoding
    here: a job built that way runs zero steps and still reports success, so
    a test written against it would pass while observing nothing.
    """
    assets = list(defs.assets or [])
    whole = dg.Definitions.merge(
        defs,
        dg.Definitions(
            jobs=[
                dg.define_asset_job(
                    "observe_it",
                    selection=dg.AssetSelection.assets(*[a.key for a in assets]),
                )
            ]
        ),
    )
    resolve = getattr(whole, "resolve_job_def", None) or whole.get_job_def
    result = resolve("observe_it").execute_in_process(
        raise_on_error=raise_on_error
    )
    observations = [
        event.asset_observation_data.asset_observation
        for event in result.all_events
        if event.event_type_value == "ASSET_OBSERVATION"
    ]
    return result, observations


# --- the shipped example ----------------------------------------------------


def test_the_shipped_example_loads_and_builds_one_source_asset(example):
    defs = _load(example)

    assert sorted(defs.resources or {}) == ["warehouse"]
    assert [asset.key.to_user_string() for asset in defs.assets or []] == [
        "party_extract"
    ]
    assert isinstance((defs.assets or [])[0], dg.SourceAsset)


def test_observing_the_example_counts_and_names_without_moving_a_row(example):
    """End to end, and the numbers are the point.

    Five rows and five columns of a real sqlite relation, read through the
    tagged resource -- and the as-of is the relation's own watermark, not a
    clock, because the example declares an `as_of_statement`.
    """
    result, observations = _observe(_load(example))

    assert result.success
    assert len(observations) == 1
    metadata = observations[0].metadata
    assert metadata["row_count"].value == 5
    assert metadata["column_count"].value == 5
    assert metadata["as_of"].value != metadata["observed_at"].value
    assert metadata["classification_source"].value == str(DIMENSIONS)
    assert metadata["classification_digest"].value


def test_the_unlabelled_column_is_published_most_restrictive_and_counted(example):
    """Deny by default, visible as a warning and as a number.

    The example leaves one column out of `columns:` on purpose. It comes out
    at the top of the ladder and holding every value of the unordered
    dimension -- an incomplete list under-serves rather than over-serves.
    """
    _, observations = _observe(_load(example))
    metadata = observations[0].metadata

    assert metadata["defaulted_column_count"].value == 1
    defaulted = metadata["defaulted_columns"].value
    assert defaulted == ["loaded_at"]

    columns = metadata["columns"].value
    assert columns["loaded_at"]["defaulted"] is True
    assert columns["loaded_at"]["labels"] == {
        "example_ladder": ["rung_restricted"],
        "example_facet": ["facet_x", "facet_y"],
    }


def test_an_unratified_value_is_reported_rather_than_refused(example):
    """And `require_ratified` is how a deployment opts into refusing it."""
    _, observations = _observe(_load(example))

    assert observations[0].metadata["unratified_values"].value == [
        "example_facet.facet_y"
    ]


def test_require_ratified_refuses_the_publish_and_names_the_values(example):
    yaml_path = example / "source" / "component.yaml"
    yaml_path.write_text(
        yaml_path.read_text(encoding="utf-8").replace(
            "require_ratified: false", "require_ratified: true"
        ),
        encoding="utf-8",
    )

    with pytest.raises(DimensionsError) as caught:
        _observe(_load(example))

    assert "example_facet.facet_y" in str(caught.value)


def test_the_example_emits_nothing_because_it_names_no_catalog(example):
    """The observation still records the whole label set.

    Which is what lets the component be exercised with no catalog in front of
    it -- and what makes `dry_run` an inspection tool rather than a stub.
    """
    _, observations = _observe(_load(example))

    assert observations[0].metadata["catalog_proposals"].value == 0
    assert observations[0].metadata["columns"].value


# --- what Dagster will actually call ---------------------------------------


def test_the_observe_function_takes_no_run_config(example):
    """Measured, not assumed, and it is why every knob is a YAML attribute.

    Dagster calls an observation as `observe_fn(context, **resource_kwargs)`
    (`_core/definitions/source_asset.py`), so only resources are injected. A
    pythonic `config:` parameter here would pass definition time and raise
    `TypeError: missing 1 required positional argument` at RUN time -- on
    1.10.19 and on 1.13.1 alike. A regression would therefore be invisible to
    any test that only loads the definitions.
    """
    import inspect

    source_asset = (_load(example).assets or [])[0]
    parameters = list(inspect.signature(source_asset.observe_fn).parameters)

    assert parameters == ["context"]


def test_a_resource_without_the_columns_seam_is_refused_with_its_reason(example):
    """Rows are not enough, and the error has to say why.

    The schema statement matches no rows, so the column names come off the
    result's own metadata. A resource offering only `fetch_rows` would observe
    a source with no columns, which is indistinguishable from a relation that
    is not there.
    """
    defs = _load(example)
    assets = list(defs.assets or [])

    class _RowsOnly:
        def fetch_rows(self, statement, parameters=None):
            return [{"n": 0}]

    whole = dg.Definitions(
        assets=assets,
        resources={"warehouse": _RowsOnly()},
        jobs=[
            dg.define_asset_job(
                "observe_it",
                selection=dg.AssetSelection.assets(*[a.key for a in assets]),
            )
        ],
    )
    resolve = getattr(whole, "resolve_job_def", None) or whole.get_job_def
    result = resolve("observe_it").execute_in_process(raise_on_error=False)

    assert not result.success
    failures = [
        event for event in result.all_events if event.event_type_value == "STEP_FAILURE"
    ]
    assert "fetch_columns" in str(failures[0].event_specific_data.error)


# --- the mounted document, at load and at run -------------------------------


def _spec(**overrides) -> WarehouseSourceSpec:
    block = {
        "asset_name": "some_asset",
        "key_prefix": [],
        "group_name": None,
        "tags": {},
        "resource_key": "warehouse",
        "dry_run": False,
        "source": {
            "dataset_urn": (
                "urn:li:dataset:(urn:li:dataPlatform:some_platform,a.b.c,SOME_FABRIC)"
            ),
            "table": "some_relation",
            "access": "brokered",
            "owner": "a-team",
            "label_origin": "board",
            "columns": [],
        },
        "observation": {},
        "classification": {},
        "catalog": {},
    }
    block.update(overrides)
    return WarehouseSourceSpec.from_attributes(**block)


def test_an_absent_document_loads_and_then_fails_the_run_naming_the_path(
    tmp_path, monkeypatch
):
    """The split that keeps a developer's checkout usable and a cluster honest.

    A code location must not fail to load because a cluster mount is absent
    from a laptop. But publishing a label set validated against no document
    would claim a classification nobody ratified -- and deny by default means
    there would be nothing to serve anyway. So the load succeeds and the
    materialization fails with the path in the message.
    """
    missing = tmp_path / "not-mounted" / "dimensions.yaml"
    monkeypatch.setenv("DAGTOOLS_CLASSIFICATION_LEVELS", str(missing))

    component = WarehouseSourceComponent(
        asset_name="some_asset",
        source=_spec().source.model_dump(),
    )

    class _Ctx:
        path = None

    defs = component.build_defs(_Ctx())  # loads
    assets = list(defs.assets or [])
    whole = dg.Definitions(
        assets=assets,
        resources={"warehouse": object()},
        jobs=[
            dg.define_asset_job(
                "observe_it",
                selection=dg.AssetSelection.assets(*[a.key for a in assets]),
            )
        ],
    )
    resolve = getattr(whole, "resolve_job_def", None) or whole.get_job_def

    with pytest.raises(DimensionsError) as caught:
        resolve("observe_it").execute_in_process()

    # The message quotes the path with !r, which doubles a Windows separator.
    assert str(missing) in str(caught.value).replace("\\\\", "\\")


def test_a_label_the_document_does_not_declare_fails_the_LOAD(example):
    """While the operator is still looking at the YAML they just wrote."""
    yaml_path = example / "source" / "component.yaml"
    yaml_path.write_text(
        yaml_path.read_text(encoding="utf-8").replace(
            "example_ladder: rung_internal", "example_ladder: rung_invented"
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception) as caught:
        _load(example)
    assert "rung_invented" in _chain_text(caught.value)


def test_a_dagster_platform_urn_fails_the_load(example):
    """The asset of record is the physical platform's dataset."""
    yaml_path = example / "source" / "component.yaml"
    yaml_path.write_text(
        yaml_path.read_text(encoding="utf-8").replace(
            "dataPlatform:sqlite", "dataPlatform:dagster"
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception) as caught:
        _load(example)
    assert "dagster" in _chain_text(caught.value)


def test_an_unrenderable_statement_fails_the_load_not_the_run():
    """Rendering opens nothing, which is why it is safe to prove it early."""
    with pytest.raises(Exception):
        _spec(source=dict(_spec().source.model_dump(), table="a; DROP TABLE b"))


# --- what the catalog is told ----------------------------------------------


def _mcps(spec_overrides=None, observed=("c_one",)):
    pytest.importorskip("datahub", reason="dag-tools[datahub] extra not installed")
    from dag_tools.components.warehouse_source.component import _mcps_for
    from dag_tools.warehouse_source import (
        DimensionSet,
        build_catalog_payload,
        label_source,
        observe,
    )

    dimensions = DimensionSet.from_document(
        {
            "classification_dimensions": [
                {
                    "id": "ladder",
                    "hierarchical": True,
                    "values": [{"id": "rung_low"}, {"id": "rung_high"}],
                }
            ]
        },
        source="/mounted/doc.yaml",
        digest="deadbeef",
    )
    spec = _spec(
        **(spec_overrides or {}),
        source=dict(
            _spec().source.model_dump(),
            columns=[{"name": "c_one", "labels": {"ladder": "rung_low"}}],
        ),
    )
    labelled = label_source(spec.source, dimensions)
    observation = observe(
        column_names=observed, row_count=1, as_of="w-1", observed_at="t-0"
    )
    payload = build_catalog_payload(
        spec=spec.source,
        resolved=labelled.resolve(observation.column_names),
        observation=observation,
        dimensions=dimensions,
        asset_key="some_asset",
    )
    return payload, _mcps_for(payload, spec, "some_asset")


def test_the_physical_dataset_is_the_entity_every_label_is_attached_to():
    """Not the Dagster asset. A broker authorizing a read of the physical
    object has to find the labels on that object."""
    payload, mcps = _mcps()

    labelled = [mcp for mcp in mcps if mcp.entityUrn == payload.dataset_urn]
    assert len(labelled) == 4
    assert "editableSchemaMetadata" in {mcp.aspectName for mcp in labelled}


def test_no_schema_aspect_is_emitted_because_only_names_were_read():
    """A type for every column would be a guess written into the catalog as
    fact. The crawler that owns the platform fills that in; the terms ride on
    `editableSchemaMetadata`, which carries a field path and needs no type."""
    _, mcps = _mcps()

    assert "schemaMetadata" not in {mcp.aspectName for mcp in mcps}


def test_the_lineage_edge_runs_FROM_the_physical_dataset_TO_the_asset():
    """The asset reads the source, so that is the direction.

    And the Dagster node sits in the same fabric as the source, read off the
    operator's own URN -- the fabric is a deployment's word for its
    environment, not one this library supplies.
    """
    payload, mcps = _mcps()

    lineage = [mcp for mcp in mcps if mcp.aspectName == "upstreamLineage"]
    assert len(lineage) == 1
    assert lineage[0].entityUrn == (
        "urn:li:dataset:(urn:li:dataPlatform:dagster,some_asset,SOME_FABRIC)"
    )
    assert [u.dataset for u in lineage[0].aspect.upstreams] == [payload.dataset_urn]


def test_the_lineage_edge_can_be_turned_off_and_nothing_else_changes():
    """For a deployment whose own tooling already draws it."""
    _, with_edge = _mcps()
    _, without = _mcps({"catalog": {"emit_lineage_from_asset": False}})

    assert len(with_edge) - len(without) == 1
    assert "upstreamLineage" not in {mcp.aspectName for mcp in without}


def test_every_term_on_a_column_reaches_the_field_it_labels():
    _, mcps = _mcps()

    schema = next(mcp for mcp in mcps if mcp.aspectName == "editableSchemaMetadata")
    fields = {
        field.fieldPath: [term.urn for term in field.glossaryTerms.terms]
        for field in schema.aspect.editableSchemaFieldInfo
    }
    assert fields == {"c_one": ["urn:li:glossaryTerm:ladder.rung_low"]}


def test_a_catalog_failure_FAILS_the_materialization(monkeypatch):
    """Deliberately the opposite of the dbt publisher, which swallows.

    There the build is the deliverable and the catalog is a bonus. Here the
    published label set IS the deliverable: if it does not land, a broker
    finds no labels on the dataset and denies every read of it, so a component
    that reported success would turn an outage into a silent embargo.
    """
    pytest.importorskip("datahub", reason="dag-tools[datahub] extra not installed")
    from dag_tools.components.warehouse_source import component as module

    class _Broken:
        def emit_mcp(self, mcp):
            raise RuntimeError("gms is down")

    monkeypatch.setattr(module, "_graph", lambda spec: _Broken())
    payload, _ = _mcps({"catalog": {"server": "http://gms.invalid"}})
    spec = _spec(catalog={"server": "http://gms.invalid"})

    with pytest.raises(RuntimeError, match="gms is down"):
        module.emit_payload(payload, spec, "some_asset")


def test_an_empty_token_is_never_sent_as_a_credential(monkeypatch):
    """`""` is sent and rejected, which fails in a misleading place."""
    from dag_tools.components.warehouse_source.component import _catalog_token

    monkeypatch.delenv("DATAHUB_TOKEN", raising=False)
    assert _catalog_token(_spec(catalog={"token": ""})) is None

    monkeypatch.setenv("DATAHUB_TOKEN", "from-env")
    assert _catalog_token(_spec()) == "from-env"
    assert _catalog_token(_spec(catalog={"token": "explicit"})) == "explicit"


# --- dry run ----------------------------------------------------------------


def test_dry_run_records_everything_and_emits_nothing(example):
    """Which is how to see a label set before a catalog ever hears about it."""
    yaml_path = example / "source" / "component.yaml"
    text = yaml_path.read_text(encoding="utf-8").replace(
        "dry_run: false", "dry_run: true"
    )
    yaml_path.write_text(
        text.replace("# server:", "server:"), encoding="utf-8"
    )

    result, observations = _observe(_load(example))

    assert result.success
    metadata = observations[0].metadata
    assert metadata["dry_run"].value is True
    assert metadata["catalog_proposals"].value == 0
    assert metadata["columns"].value


# --- the factory, built directly -------------------------------------------


def test_the_asset_key_in_the_lineage_node_is_the_prefixed_key(monkeypatch):
    """Computed at build time from the prefix and the name.

    Read off the context instead, it would depend on which Dagster is
    installed -- and the catalog would then carry a different node per
    version.
    """
    monkeypatch.setenv("DAGTOOLS_CLASSIFICATION_LEVELS", str(DIMENSIONS))
    asset = build_warehouse_source_asset(
        _spec(asset_name="some_asset", key_prefix=["some", "prefix"]), None
    )

    assert asset.key.to_user_string() == "some/prefix/some_asset"


def test_the_definition_carries_the_urn_and_the_access_mode(monkeypatch):
    """Visible in the UI without running anything."""
    monkeypatch.setenv("DAGTOOLS_CLASSIFICATION_LEVELS", str(DIMENSIONS))
    asset = build_warehouse_source_asset(_spec(), None)

    def _plain(key):
        value = asset.metadata[key]
        return getattr(value, "value", getattr(value, "text", value))

    assert _plain("access") == "brokered"
    assert _plain("dataset_urn").startswith("urn:li:dataset:(")
    assert _plain("owner") == "a-team"


def test_the_component_requires_the_resource_key_it_was_given(monkeypatch):
    """A rename in the YAML has to reach the asset, or the observation would
    read through whatever happened to be bound under the default name."""
    monkeypatch.setenv("DAGTOOLS_CLASSIFICATION_LEVELS", str(DIMENSIONS))
    asset = build_warehouse_source_asset(
        _spec(resource_key="analytics_warehouse"), None
    )

    assert "analytics_warehouse" in asset.required_resource_keys
    assert "warehouse" not in asset.required_resource_keys


def test_the_schema_refuses_a_key_nobody_reads():
    """An ignored key is a knob the operator believes they turned."""
    with pytest.raises(Exception):
        _spec(observation={"count_rows": True, "cuont_rows": False})
    with pytest.raises(SourceSpecError):
        _spec(source=dict(_spec().source.model_dump(), owner="  "))
    with pytest.raises(DimensionsError):
        from dag_tools.warehouse_source import DimensionSet

        DimensionSet.from_document(
            {"not_the_key": []}, source="/mounted/doc.yaml", digest="0"
        )
