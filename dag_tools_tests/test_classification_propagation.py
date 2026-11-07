"""`ClassificationPropagationComponent` -- what a derived column publishes.

ADR-0064 steps 4b and 4c as a deployment meets them: the sensor the YAML
builds, the label set it reads back out of its sources' own materializations,
and the two patches the catalog is told. The composition rules themselves are
next door in `test_warehouse_source_propagation.py`, against the pure engine;
what is pinned here is everything that only exists once Dagster and the
catalog are in the picture.

Three of these are the tests the mistakes of §4i argue for. The metadata keys
this reads come off a PRIVATE Dagster class, so one test asserts they are
still the keys Dagster emits rather than trusting a constant. The propagation
reads what step 4a publishes, so one test builds a real 4a payload and feeds
it in rather than hand-writing the shape both sides are assumed to agree on.
And nothing is asserted by "the tick did not raise": the claims are about what
the catalog received and what the asset recorded.

Every dimension, value, asset and column named here is invented for this
module. Which dimension is the ladder, how each other one composes, and what
terms a label publishes are all read off the document.
"""

import importlib.util
import json
import shutil
from pathlib import Path

import dagster as dg
import pytest
import yaml
from pydantic import ValidationError

from dag_tools.components.classification_propagation import (
    ClassificationPropagationSpec,
    DatasetBlock,
    build_propagation_sensor,
    classification_document,
    declared_lineage,
    emit_propagation,
    mcps_for,
    output_columns,
    propagate_dataset,
    published_labels,
    vocabulary_terms,
)
from dag_tools.components.classification_propagation.component import (
    COLUMN_LINEAGE_METADATA_KEY,
    COLUMN_SCHEMA_METADATA_KEY,
    PUBLISHED_COLUMNS_METADATA_KEY,
)
from dag_tools.warehouse_source import (
    COLUMN_LINEAGE,
    PERMISSIVE_SET,
    RESTRICTIVE_SET,
    TABLE_MAX,
    DimensionSet,
    DimensionsError,
    PropagationError,
)

SOURCE = "raw/one"
SECOND_SOURCE = "raw/two"
DERIVED = "marts/a"
URN = "urn:li:dataset:(urn:li:dataPlatform:some_platform,a.b.c,SOME_FABRIC)"

DOCUMENT = {
    "classification_dimensions": [
        {
            "id": "ladder",
            "hierarchical": True,
            "values": [
                {"id": "rung_low", "ratified_by": "board"},
                {"id": "rung_mid", "ratified_by": "board"},
                {"id": "rung_high", "ratified_by": "board"},
            ],
        },
        {
            "id": "held_all_of",
            "type": RESTRICTIVE_SET,
            "values": [
                {"id": "all_of_a", "ratified_by": "board"},
                {"id": "all_of_b", "ratified_by": "board"},
            ],
        },
        {
            "id": "held_one_of",
            "type": PERMISSIVE_SET,
            "values": [
                {"id": "one_of_a", "ratified_by": "board"},
                {"id": "one_of_b", "ratified_by": "board"},
            ],
        },
    ]
}


def _dimensions() -> DimensionSet:
    return DimensionSet.from_document(
        DOCUMENT, source="/mounted/doc.yaml", digest="deadbeef"
    )


def _document_file(tmp_path) -> str:
    path = tmp_path / "dimensions.yaml"
    path.write_text(yaml.safe_dump(DOCUMENT), encoding="utf-8")
    return str(path)


def _dataset(**overrides) -> DatasetBlock:
    block = {"asset_key": DERIVED, "dataset_urn": URN, "sources": [SOURCE]}
    block.update(overrides)
    return DatasetBlock.model_validate(block)


def _published(**columns):
    """A source's materialization metadata, as step 4a shapes it."""
    return {
        PUBLISHED_COLUMNS_METADATA_KEY: {
            name: {"labels": label} for name, label in columns.items()
        }
    }


def _derived(schema=("out_one",), lineage=None):
    """The derived asset's materialization metadata, as Dagster shapes it."""
    metadata = {}
    if schema is not None:
        metadata[COLUMN_SCHEMA_METADATA_KEY] = dg.TableSchema(
            columns=[dg.TableColumn(name=name) for name in schema]
        )
    if lineage is not None:
        metadata[COLUMN_LINEAGE_METADATA_KEY] = dg.TableColumnLineage(
            deps_by_column={
                column: [
                    dg.TableColumnDep(
                        asset_key=dg.AssetKey(dataset.split("/")),
                        column_name=source_column,
                    )
                    for dataset, source_column in deps
                ]
                for column, deps in lineage.items()
            }
        )
    return metadata


def _spec(datasets=None, **overrides):
    attributes = {
        "datasets": datasets if datasets is not None else [_dataset().model_dump()],
        "classification": None,
        "catalog": None,
        "sensor_name": "propagate_labels",
        "minimum_interval_seconds": None,
        "default_status": "STOPPED",
        "dry_run": False,
    }
    attributes.update(overrides)
    return ClassificationPropagationSpec.from_attributes(**attributes)


class _Recorder:
    """Stands in for the catalog. Keeps what it was sent."""

    def __init__(self):
        self.mcps = []

    def emit_mcp(self, mcp):
        self.mcps.append(mcp)


# --- the metadata this reads is the metadata Dagster writes -----------------


def test_the_metadata_keys_are_the_ones_dagster_emits():
    """A canary over a PRIVATE class, which is why it is spelled as a constant.

    Column lineage and column schema reach the event log under keys minted by
    `TableMetadataSet`, which is not public API. This module cannot import it
    at runtime, so it hard-codes the two strings -- and if a Dagster upgrade
    renames either, every derived asset would silently look like one that
    declared no lineage and fall back to the table max. A wrong answer that
    reports success, so the keys are pinned to their source here.
    """
    from dagster._core.definitions.metadata import TableMetadataSet

    emitted = dict(
        TableMetadataSet(
            column_lineage=dg.TableColumnLineage(deps_by_column={}),
            column_schema=dg.TableSchema(columns=[]),
        )
    )

    assert set(emitted) == {COLUMN_LINEAGE_METADATA_KEY, COLUMN_SCHEMA_METADATA_KEY}


def test_metadata_read_back_out_of_the_event_log_is_unwrapped():
    """Normalization wraps; the producer does not. Both shapes arrive here.

    Handed over directly a value is a `TableColumnLineage`; read back off a
    materialization record it is a `TableColumnLineageMetadataValue` around
    one. Reading only the raw shape would work in every unit test and find no
    lineage at all in a deployment.
    """
    with dg.DagsterInstance.ephemeral() as instance:
        instance.report_runless_asset_event(
            dg.AssetMaterialization(
                asset_key=dg.AssetKey(DERIVED.split("/")),
                metadata=_derived(
                    schema=("out_one",), lineage={"out_one": [(SOURCE, "col_a")]}
                ),
            )
        )
        record = instance.fetch_materializations(
            dg.AssetKey(DERIVED.split("/")), limit=1
        ).records[0]
        metadata = dict(record.asset_materialization.metadata)

    # Wrapped, as the event log hands it back.
    assert not isinstance(metadata[COLUMN_LINEAGE_METADATA_KEY], dg.TableColumnLineage)
    lineage = declared_lineage(metadata, sources=[SOURCE], where=DERIVED)
    assert lineage["out_one"][0].dataset == SOURCE
    assert output_columns(metadata, lineage, where=DERIVED) == ("out_one",)


# --- reading what step 4a published ----------------------------------------


def test_a_real_step_4a_payload_reads_back_as_labels():
    """The two halves agree because one is fed the other's output.

    Step 4a publishes its label set as its own materialization metadata and
    4b composes from it. Hand-writing that shape on both sides would let the
    producer change and every test still pass.
    """
    from dag_tools.warehouse_source import (
        build_catalog_payload,
        label_source,
        load_source_spec,
        observe,
    )

    dimensions = _dimensions()
    spec = load_source_spec(
        {
            "dataset_urn": URN,
            "table": "some_relation",
            "access": "brokered",
            "owner": "a-team",
            "label_origin": "board",
            "columns": [
                {"name": "col_a", "labels": {"ladder": "rung_low"}},
                {
                    "name": "col_b",
                    "labels": {"ladder": "rung_high", "held_all_of": "all_of_a"},
                },
            ],
        }
    )
    observation = observe(
        column_names=("col_a", "col_b", "col_c"), row_count=3, observed_at="t-0"
    )
    payload = build_catalog_payload(
        spec=spec,
        resolved=label_source(spec, dimensions).resolve(observation.column_names),
        observation=observation,
        dimensions=dimensions,
        asset_key=SOURCE,
    )
    metadata = {PUBLISHED_COLUMNS_METADATA_KEY: payload.as_json()["columns"]}

    labels = published_labels(metadata, source=SOURCE)

    assert set(labels) == {"col_a", "col_b", "col_c"}
    assert dimensions.validate_label(labels["col_a"]).values_for("ladder") == (
        "rung_low",
    )
    # col_c was never labelled, so 4a published deny-by-default for it -- and
    # on a document carrying a permissive dimension that label holds an EMPTY
    # set. A reader that refused one would fail every defaulted column.
    defaulted = dimensions.validate_label(labels["col_c"])
    assert defaulted == dimensions.most_restrictive_label()
    assert defaulted.values_for("held_one_of") == ()


def test_a_defaulted_source_column_propagates_rather_than_failing():
    """The same regression, through the whole component path."""
    dimensions = _dimensions()
    propagation = propagate_dataset(
        dimensions,
        _dataset(),
        derived_metadata=_derived(lineage={"out_one": [(SOURCE, "col_c")]}),
        source_metadata={
            SOURCE: _published(col_c=dimensions.most_restrictive_label().as_dict())
        },
    )

    assert propagation.labels["out_one"] == dimensions.most_restrictive_label()


def test_a_source_publishing_no_label_set_fails_rather_than_counting_as_nothing():
    """An unlabelled source is not an unrestricted one.

    Composing only over the sources that happen to carry labels publishes a
    ceiling computed from a subset of the inputs, and that looks exactly like
    a correct answer.
    """
    with pytest.raises(PropagationError, match="publishes no label set"):
        propagate_dataset(
            _dimensions(),
            _dataset(),
            derived_metadata=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
            source_metadata={SOURCE: {}},
        )


def test_a_source_publishing_a_column_without_a_label_fails():
    """A document in the right place with the wrong shape is not a label set."""
    with pytest.raises(PropagationError, match="without a label"):
        published_labels(
            {PUBLISHED_COLUMNS_METADATA_KEY: {"col_a": {"terms": []}}},
            source=SOURCE,
        )


def test_a_declared_source_that_never_materialized_fails():
    """There is nothing to read a label set from, and nothing is not a label."""
    with pytest.raises(PropagationError, match="no materialization"):
        propagate_dataset(
            _dimensions(),
            _dataset(sources=[SOURCE, SECOND_SOURCE]),
            derived_metadata=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
            source_metadata={SOURCE: _published(col_a={"ladder": "rung_low"})},
        )


# --- which columns, and from what ------------------------------------------


def test_lineage_naming_an_undeclared_source_is_refused():
    """Dropping the dep would quietly stop counting an unlabelled upstream."""
    with pytest.raises(PropagationError, match="not declared as one of its sources"):
        declared_lineage(
            _derived(lineage={"out_one": [(SECOND_SOURCE, "col_a")]}),
            sources=[SOURCE],
            where=DERIVED,
        )


def test_the_columns_come_from_the_declared_schema():
    """The schema is the asset's own word for what columns it has."""
    metadata = _derived(
        schema=("out_one", "out_two"), lineage={"out_one": [(SOURCE, "col_a")]}
    )
    lineage = declared_lineage(metadata, sources=[SOURCE], where=DERIVED)

    assert output_columns(metadata, lineage, where=DERIVED) == ("out_one", "out_two")


def test_without_a_schema_the_columns_are_the_ones_the_lineage_names():
    metadata = _derived(schema=None, lineage={"out_one": [(SOURCE, "col_a")]})
    lineage = declared_lineage(metadata, sources=[SOURCE], where=DERIVED)

    assert output_columns(metadata, lineage, where=DERIVED) == ("out_one",)


def test_an_asset_naming_no_columns_is_refused_not_propagated_over_nothing():
    """Propagating over an empty column list publishes nothing and succeeds."""
    with pytest.raises(PropagationError, match="names no columns"):
        output_columns(_derived(schema=None), {}, where=DERIVED)


def test_a_column_with_lineage_composes_from_the_columns_it_read():
    dimensions = _dimensions()
    propagation = propagate_dataset(
        dimensions,
        _dataset(),
        derived_metadata=_derived(
            schema=("out_one",),
            lineage={"out_one": [(SOURCE, "col_a"), (SOURCE, "col_b")]},
        ),
        source_metadata={
            SOURCE: _published(
                col_a={"ladder": "rung_low", "held_all_of": ["all_of_a"]},
                col_b={"ladder": "rung_mid"},
                col_c={"ladder": "rung_high"},
            )
        },
    )
    decision = propagation.columns[0]

    assert decision.basis == COLUMN_LINEAGE
    assert [str(ref) for ref in decision.sources] == [
        "raw/one.col_a",
        "raw/one.col_b",
    ]
    # col_c was never read, so its rung is not in the answer.
    assert decision.label.values_for("ladder") == ("rung_mid",)
    assert decision.label.values_for("held_all_of") == ("all_of_a",)


def test_a_column_the_lineage_says_nothing_about_carries_the_table_max():
    """The fallback is the whole table's ceiling, never nothing.

    A column with no declared lineage could have read anything the asset
    read, so it is labelled as if it read all of it. Naming those columns is
    how an operator sees what declaring lineage would buy.
    """
    dimensions = _dimensions()
    propagation = propagate_dataset(
        dimensions,
        _dataset(),
        derived_metadata=_derived(
            schema=("out_one", "out_two"),
            lineage={"out_one": [(SOURCE, "col_a")]},
        ),
        source_metadata={
            SOURCE: _published(
                col_a={"ladder": "rung_low"},
                col_b={"ladder": "rung_high", "held_all_of": ["all_of_b"]},
            )
        },
    )

    assert propagation.table_max_columns == ("out_two",)
    fallback = propagation.labels["out_two"]
    assert fallback.values_for("ladder") == ("rung_high",)
    assert fallback.values_for("held_all_of") == ("all_of_b",)
    assert propagation.columns[1].basis == TABLE_MAX


def test_a_declared_downgrade_is_applied_and_recorded_with_its_accountability():
    dimensions = _dimensions()
    propagation = propagate_dataset(
        dimensions,
        _dataset(
            downgrades=[
                {
                    "id": "rule_one",
                    "owner": "example-board",
                    "reason": "an aggregate, not a row-level figure",
                    "columns": ["out_one"],
                    "label": {"ladder": "rung_low", "held_all_of": []},
                }
            ]
        ),
        derived_metadata=_derived(lineage={"out_one": [(SOURCE, "col_b")]}),
        source_metadata={
            SOURCE: _published(
                col_b={"ladder": "rung_high", "held_all_of": ["all_of_a"]}
            )
        },
    )
    applied = propagation.downgrades[0].as_dict()

    assert propagation.labels["out_one"].values_for("ladder") == ("rung_low",)
    assert applied["rule"] == "rule_one"
    assert applied["owner"] == "example-board"
    assert applied["reason"]
    assert applied["derived"]["ladder"] == ["rung_high"]
    assert applied["declared"]["held_all_of"] == []


# --- what the catalog is told ----------------------------------------------


def _propagation(dimensions=None, **dataset_overrides):
    dimensions = dimensions or _dimensions()
    dataset = _dataset(**dataset_overrides)
    return dataset, propagate_dataset(
        dimensions,
        dataset,
        derived_metadata=_derived(lineage={"out_one": [(SOURCE, "col_b")]}),
        source_metadata={
            SOURCE: _published(
                col_b={"ladder": "rung_high", "held_all_of": ["all_of_a"]}
            )
        },
    )


def _ops(mcp):
    return json.loads(mcp.aspect.value)


def test_both_aspects_are_patched_so_what_else_describes_the_dataset_survives():
    """A derived dataset is usually already described by something else.

    Step 4a writes a source dataset it owns outright. A derived one is a
    table a crawler has found, with its own properties and very likely terms
    and tags a human put there. Replacing either aspect would delete all of
    that, so both proposals are PATCHes.
    """
    dimensions = _dimensions()
    dataset, propagation = _propagation(dimensions)

    mcps = mcps_for(propagation, dimensions, dataset=dataset, asset_key=DERIVED)

    assert [mcp.aspectName for mcp in mcps] == [
        "datasetProperties",
        "editableSchemaMetadata",
    ]
    assert {mcp.changeType for mcp in mcps} == {"PATCH"}
    assert {mcp.entityUrn for mcp in mcps} == {URN}
    # Custom properties are patched key by key, never as a whole map.
    paths = [op["path"] for op in _ops(mcps[0])]
    assert all(path.startswith("/customProperties/") for path in paths)


def test_the_terms_patch_removes_the_rest_of_the_vocabulary():
    """What makes a downgrade take effect, rather than merely be recorded.

    A column carrying two values of one dimension reads as the stricter of
    them, so an add-only patch would leave yesterday's term in place and the
    declared downgrade would publish nothing -- silently, since the proposal
    itself succeeded.
    """
    dimensions = _dimensions()
    dataset, propagation = _propagation(
        dimensions,
        downgrades=[
            {
                "id": "rule_one",
                "owner": "example-board",
                "reason": "an aggregate, not a row-level figure",
                "columns": ["out_one"],
                "label": {"ladder": "rung_low", "held_all_of": []},
            }
        ],
    )

    mcps = mcps_for(propagation, dimensions, dataset=dataset, asset_key=DERIVED)
    ops = _ops(mcps[1])
    added = {op["path"].rsplit("/", 1)[-1] for op in ops if op["op"] == "add"}
    removed = {op["path"].rsplit("/", 1)[-1] for op in ops if op["op"] == "remove"}

    assert added == set(dimensions.terms_for(propagation.labels["out_one"]))
    # The rung the data derived is removed by name; so is the obligation the
    # rule cleared.
    assert "urn:li:glossaryTerm:ladder.rung_high" in removed
    assert "urn:li:glossaryTerm:held_all_of.all_of_a" in removed
    assert added | removed == set(vocabulary_terms(dimensions))
    assert not added & removed
    # Every op is scoped to the column it is about; nothing else is touched.
    assert all("/out_one/" in op["path"] for op in ops)


def test_nothing_outside_the_documents_vocabulary_is_touched():
    """A crawler's terms and a human's are not this component's business."""
    dimensions = _dimensions()
    dataset, propagation = _propagation(dimensions)

    ops = _ops(mcps_for(propagation, dimensions, dataset=dataset, asset_key=DERIVED)[1])
    touched = {op["path"].rsplit("/", 1)[-1] for op in ops}

    assert touched == set(vocabulary_terms(dimensions))


def test_the_properties_say_the_label_was_propagated_and_what_from():
    dimensions = _dimensions()
    dataset, propagation = _propagation(dimensions)

    ops = {
        op["path"].rsplit("/", 1)[-1]: op["value"]
        for op in _ops(
            mcps_for(propagation, dimensions, dataset=dataset, asset_key=DERIVED)[0]
        )
    }

    assert ops["classification_origin"] == "propagated"
    assert ops["classification_sources"] == SOURCE
    assert ops["dagster_asset_key"] == DERIVED
    assert ops["classification_digest"] == "deadbeef"
    assert json.loads(ops["classification"])["columns"]["out_one"]["basis"] == (
        COLUMN_LINEAGE
    )


def test_the_document_records_how_each_column_was_arrived_at():
    """The one thing 4b publishes that 4a does not: the derivation."""
    dimensions = _dimensions()
    dataset, propagation = _propagation(dimensions)

    row = classification_document(propagation, dimensions)["columns"]["out_one"]

    assert row["basis"] == COLUMN_LINEAGE
    assert row["sources"] == ["raw/one.col_b"]
    assert row["terms"] == list(dimensions.terms_for(propagation.labels["out_one"]))


def test_emit_sends_every_proposal_and_counts_them(monkeypatch):
    """Asserted on what the catalog received, never on the absence of a raise."""
    import dag_tools.components.classification_propagation.component as module

    dimensions = _dimensions()
    dataset, propagation = _propagation(dimensions)
    recorder = _Recorder()
    monkeypatch.setattr(module, "_graph", lambda spec: recorder)

    emitted = emit_propagation(
        propagation,
        dimensions,
        _spec(catalog={"server": "http://catalog.invalid"}),
        dataset,
        DERIVED,
    )

    assert emitted == len(recorder.mcps) == 2


# --- the sensor ------------------------------------------------------------


def _instance_with(instance, *, derived=None, source=None, derived_key=DERIVED):
    if source is not None:
        instance.report_runless_asset_event(
            dg.AssetMaterialization(
                asset_key=dg.AssetKey(SOURCE.split("/")),
                metadata={
                    PUBLISHED_COLUMNS_METADATA_KEY: dg.MetadataValue.json(
                        source[PUBLISHED_COLUMNS_METADATA_KEY]
                    )
                },
            )
        )
    if derived is not None:
        instance.report_runless_asset_event(
            dg.AssetMaterialization(
                asset_key=dg.AssetKey(derived_key.split("/")), metadata=derived
            )
        )


def _tick(spec, instance, cursor=None):
    sensor_def = build_propagation_sensor(spec, component_dir=None)
    context = dg.build_sensor_context(instance=instance, cursor=cursor)
    return sensor_def(context), context


def _observations(instance, key=DERIVED):
    return instance.fetch_observations(dg.AssetKey(key.split("/")), limit=10).records


def _good_source():
    return _published(
        col_a={"ladder": "rung_low"},
        col_b={"ladder": "rung_high", "held_all_of": ["all_of_a"]},
    )


def test_the_sensor_is_stopped_by_default_and_launches_no_runs(tmp_path):
    """Publishing classifications is not something a code location starts.

    Same posture as every other sensor this library ships -- and this one
    materializes nothing either: a tick records an observation and skips.
    """
    spec = _spec(classification={"dimensions_file": _document_file(tmp_path)})
    sensor_def = build_propagation_sensor(spec, component_dir=None)

    assert sensor_def.default_status == dg.DefaultSensorStatus.STOPPED

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
        )
        result, _ = _tick(spec, instance)

    assert isinstance(result, dg.SkipReason)


def test_a_tick_records_the_whole_decision_against_the_derived_asset(tmp_path):
    """The record is the deliverable when no catalog is configured.

    It is also how this component is exercised with nothing in front of it:
    the metadata is exactly what would have been published.
    """
    spec = _spec(classification={"dimensions_file": _document_file(tmp_path)})

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(
                schema=("out_one", "out_two"),
                lineage={"out_one": [(SOURCE, "col_a")]},
            ),
        )
        result, context = _tick(spec, instance)
        records = _observations(instance)

    assert "published 1" in result.skip_message
    assert len(records) == 1
    metadata = records[0].asset_observation.metadata
    assert metadata["dataset_urn"].value == URN
    assert metadata["classification_origin"].value == "propagated"
    assert metadata["column_count"].value == 2
    assert metadata["table_max_columns"].value == ["out_two"]
    assert metadata["catalog_proposals"].value == 0
    columns = metadata["columns"].value
    assert columns["out_one"]["basis"] == COLUMN_LINEAGE
    assert columns["out_two"]["basis"] == TABLE_MAX
    assert json.loads(context.cursor)[DERIVED]["status"] == "published"


def test_an_unchanged_materialization_is_not_republished(tmp_path):
    """The cursor is the storage id, so one materialization publishes once."""
    spec = _spec(classification={"dimensions_file": _document_file(tmp_path)})

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
        )
        _, first = _tick(spec, instance)
        result, _ = _tick(spec, instance, cursor=first.cursor)
        records = _observations(instance)

    assert "unchanged 1" in result.skip_message
    assert len(records) == 1


def test_a_new_materialization_is_republished(tmp_path):
    """A re-materialization can mean new columns or new lineage."""
    spec = _spec(classification={"dimensions_file": _document_file(tmp_path)})

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
        )
        _, first = _tick(spec, instance)
        _instance_with(
            instance, derived=_derived(lineage={"out_one": [(SOURCE, "col_b")]})
        )
        result, _ = _tick(spec, instance, cursor=first.cursor)
        records = _observations(instance)

    assert "published 1" in result.skip_message
    assert len(records) == 2
    newest = records[0].asset_observation.metadata["columns"].value
    assert newest["out_one"]["sources"] == ["raw/one.col_b"]


def test_a_dataset_that_has_never_materialized_is_waited_for(tmp_path):
    """Nothing has been built yet, which is not a failure of anything."""
    spec = _spec(classification={"dimensions_file": _document_file(tmp_path)})

    with dg.DagsterInstance.ephemeral() as instance:
        result, context = _tick(spec, instance)

    assert "not yet materialized" in result.skip_message
    assert json.loads(context.cursor) == {}


def test_one_dataset_failing_does_not_stop_the_others(tmp_path):
    """A sensor that gave up on the first bad dataset would stall the rest."""
    second = "marts/b"
    spec = _spec(
        datasets=[
            _dataset().model_dump(),
            _dataset(asset_key=second).model_dump(),
        ],
        classification={"dimensions_file": _document_file(tmp_path)},
    )

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
        )
        # The second names no columns at all, so it cannot be classified.
        _instance_with(instance, derived={}, derived_key=second)
        result, context = _tick(spec, instance)
        cursor = json.loads(context.cursor)
        failures = len(_observations(instance, second))

    assert "published 1" in result.skip_message
    assert f"failed ['{second}']" in result.skip_message
    assert cursor[DERIVED]["status"] == "published"
    assert cursor[second]["status"] == "failed"
    assert failures == 1


def test_a_failure_is_recorded_against_the_asset_rather_than_only_logged(tmp_path):
    """"What is this column classified as" is asked of the asset.

    "Nobody could work it out" has to be an answer the asset can give, so the
    failure is an observation carrying the reason.
    """
    spec = _spec(classification={"dimensions_file": _document_file(tmp_path)})

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(instance, source=_good_source(), derived={})
        _tick(spec, instance)
        records = _observations(instance)

    metadata = records[0].asset_observation.metadata
    assert metadata["classification_propagated"].value is False
    assert "names no columns" in metadata["classification_failure"].value


def test_a_failing_dataset_is_retried_without_a_re_materialization(tmp_path):
    """Fixing the YAML is enough; nothing has to be rebuilt to publish.

    The cursor remembers the failure so the observation is not repeated every
    tick, but a failed dataset is reprocessed rather than skipped.
    """
    path = _document_file(tmp_path)
    broken = _spec(
        classification={"dimensions_file": path},
        datasets=[_dataset(sources=[SECOND_SOURCE]).model_dump()],
    )
    fixed = _spec(classification={"dimensions_file": path})

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
        )
        _, first = _tick(broken, instance)
        # Second tick, same materialization, same misconfiguration: retried,
        # and the failure is not recorded twice.
        _, second = _tick(broken, instance, cursor=first.cursor)
        failures = len(_observations(instance))
        result, _ = _tick(fixed, instance, cursor=second.cursor)

    assert json.loads(first.cursor)[DERIVED]["status"] == "failed"
    assert failures == 1
    assert "published 1" in result.skip_message


def test_an_unreadable_cursor_is_reset_rather_than_crashed_on(tmp_path):
    """The worst a reset does is republish a label set that is already right.

    The alternative is a sensor that never ticks again, which is a silent
    embargo on every derived dataset it watches.
    """
    spec = _spec(classification={"dimensions_file": _document_file(tmp_path)})

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
        )
        result, _ = _tick(spec, instance, cursor="not json at all")

    assert "published 1" in result.skip_message


def test_the_document_is_read_at_every_tick_not_once_at_load(tmp_path):
    """A ConfigMap can move between a deploy and a tick, and policy owns it."""
    path = _document_file(tmp_path)
    spec = _spec(classification={"dimensions_file": path})

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
        )
        _, first = _tick(spec, instance)
        before = _observations(instance)[0].asset_observation.metadata
        document = yaml.safe_load(open(path, encoding="utf-8"))
        document["classification_dimensions"][0]["values"].append(
            {"id": "rung_sealed", "ratified_by": "board"}
        )
        with open(path, "w", encoding="utf-8") as handle:
            yaml.safe_dump(document, handle)
        _instance_with(
            instance, derived=_derived(lineage={"out_one": [(SOURCE, "col_b")]})
        )
        _tick(spec, instance, cursor=first.cursor)
        after = _observations(instance)[0].asset_observation.metadata

    assert before["classification_digest"].value != after["classification_digest"].value


def test_dry_run_records_the_whole_decision_and_emits_nothing(tmp_path, monkeypatch):
    """Seeing a label set before a catalog ever hears about it."""
    import dag_tools.components.classification_propagation.component as module

    def _refuse(spec):
        raise AssertionError("a dry run must not reach the catalog")

    monkeypatch.setattr(module, "_graph", _refuse)
    spec = _spec(
        classification={"dimensions_file": _document_file(tmp_path)},
        catalog={"server": "http://catalog.invalid"},
        dry_run=True,
    )

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
        )
        result, _ = _tick(spec, instance)
        metadata = _observations(instance)[0].asset_observation.metadata

    assert "published 1" in result.skip_message
    assert metadata["dry_run"].value is True
    assert metadata["catalog_proposals"].value == 0
    assert metadata["columns"].value["out_one"]["basis"] == COLUMN_LINEAGE


def test_with_a_catalog_configured_the_tick_emits_what_it_recorded(
    tmp_path, monkeypatch
):
    import dag_tools.components.classification_propagation.component as module

    recorder = _Recorder()
    monkeypatch.setattr(module, "_graph", lambda spec: recorder)
    spec = _spec(
        classification={"dimensions_file": _document_file(tmp_path)},
        catalog={"server": "http://catalog.invalid"},
    )

    with dg.DagsterInstance.ephemeral() as instance:
        _instance_with(
            instance,
            source=_good_source(),
            derived=_derived(lineage={"out_one": [(SOURCE, "col_a")]}),
        )
        _tick(spec, instance)
        metadata = _observations(instance)[0].asset_observation.metadata

    assert metadata["catalog_proposals"].value == len(recorder.mcps) == 2
    assert {mcp.entityUrn for mcp in recorder.mcps} == {URN}


def test_require_ratified_fails_the_tick_rather_than_publishing(tmp_path):
    """An operator who asked for signed values gets a failure, not a warning."""
    document = {
        "classification_dimensions": [
            dict(
                DOCUMENT["classification_dimensions"][0],
                values=[{"id": "rung_low"}],
            )
        ]
    }
    path = tmp_path / "unsigned.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    spec = _spec(
        classification={
            "dimensions_file": str(path),
            "require_ratified": True,
        }
    )

    with dg.DagsterInstance.ephemeral() as instance:
        with pytest.raises(ValueError, match="ratified"):
            _tick(spec, instance)


# --- the YAML surface ------------------------------------------------------


EXAMPLE_ROOT = Path(__file__).resolve().parents[1] / "examples" / "warehouse_source"

#: The derived asset the shipped example's propagation watches.
EXAMPLE_DERIVED = "marts/party_exposure"


def _chain_text(exc: BaseException) -> str:
    parts, seen, current = [], set(), exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _skip_if_the_component_file_is_refused(exc: Exception) -> None:
    """Skip when THIS Dagster will not accept this component file.

    Matched on the raised error rather than on a version number, the same way
    step 4a's example test does it: the field the loader rejects is the fact,
    and a version is only a proxy for it.
    """
    text = _chain_text(exc)
    if "extra_forbidden" in text or "Extra inputs are not permitted" in text:
        pytest.skip(f"this Dagster rejects a key in the component file: {text}")


def _module_from(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def example(tmp_path, monkeypatch) -> Path:
    """A copy of the SHIPPED example, seeded, under a unique package name.

    The whole tree rather than just the components directory, because the
    derived asset is ordinary Python next to `definitions.py` -- which is the
    component's premise: what builds a derived table is the deployment's own
    business, and all the propagation needs from it is the column lineage
    Dagster already carries.

    The package is renamed per test because `build_component_defs` imports the
    components directory as a module, so two tests loading two copies under
    one name would both get whichever landed in `sys.modules` first.
    """
    root = tmp_path / "example"
    shutil.copytree(
        EXAMPLE_ROOT,
        root,
        ignore=shutil.ignore_patterns("__pycache__", "*.db"),
    )
    home = root / f"home_{tmp_path.name}"
    (root / "dagster_home").rename(home)
    monkeypatch.syspath_prepend(str(root))
    seed = _module_from(root / "seed.py", f"seed_{tmp_path.name}")
    monkeypatch.setenv("WAREHOUSE_SOURCE_DB", seed.seed(root / "source.db"))
    monkeypatch.setenv(
        "DAGTOOLS_CLASSIFICATION_LEVELS",
        str(root / "classification" / "dimensions.yaml"),
    )
    return home


def _load_example(home: Path) -> dg.Definitions:
    try:
        return _module_from(home / "definitions.py", f"defs_{home.name}").defs
    except Exception as exc:  # pragma: no cover - only on an old Dagster
        _skip_if_the_component_file_is_refused(exc)
        raise


def _run_the_example(defs: dg.Definitions, instance):
    """Observe the source and build the derived table, in one run.

    The source has to be NAMED in the selection: `AssetSelection.all()` leaves
    source assets out, and a job built that way runs the derived asset against
    a source nobody observed.
    """
    keys = [asset.key for asset in (defs.assets or ())]
    whole = dg.Definitions.merge(
        defs,
        dg.Definitions(
            jobs=[
                dg.define_asset_job(
                    "run_it", selection=dg.AssetSelection.assets(*keys)
                )
            ]
        ),
    )
    resolve = getattr(whole, "resolve_job_def", None) or whole.get_job_def
    return resolve("run_it").execute_in_process(instance=instance)


def _tick_the_example(defs: dg.Definitions, instance, cursor=None):
    sensor_def = list(defs.sensors or ())[0]
    context = dg.build_sensor_context(instance=instance, cursor=cursor)
    return sensor_def(context), context


def test_the_shipped_example_builds_one_sensor_and_materializes_nothing(example):
    """The example is part of the contract.

    A component whose one worked example cannot load has never been run, which
    is how the dbt example shipped pointing at two projects that did not
    exist. The derived asset in the list is the EXAMPLE's, not this
    component's -- the component contributes the sensor and nothing else.
    """
    defs = _load_example(example)

    sensors = list(defs.sensors or ())
    assert [sensor.name for sensor in sensors] == ["classification_propagation"]
    assert sensors[0].default_status == dg.DefaultSensorStatus.STOPPED
    assert sorted(
        asset.key.to_user_string() for asset in (defs.assets or ())
    ) == [EXAMPLE_DERIVED, "party_extract"]


def test_the_shipped_example_propagates_from_a_source_it_only_OBSERVED(example):
    """End to end against the real example, and the event type is the point.

    Step 4a's source is an *observable source asset*: observing it emits an
    `AssetObservation` and never a materialization. A source-side lookup that
    read only the materialization log would find nothing here and refuse with
    "publishes no label set" -- a wiring failure wearing a labelling
    failure's words, and invisible to every test that reports its own source
    event as a materialization.

    The three columns are the three outcomes: composed from lineage, composed
    and then downgraded, and composed from the table's ceiling because the
    asset declared no lineage for it.
    """
    defs = _load_example(example)

    with dg.DagsterInstance.ephemeral() as instance:
        run = _run_the_example(defs, instance)
        assert run.success
        kinds = {
            event.event_type_value
            for event in run.all_events
            if event.event_type_value
            in ("ASSET_OBSERVATION", "ASSET_MATERIALIZATION")
        }
        assert kinds == {"ASSET_OBSERVATION", "ASSET_MATERIALIZATION"}

        _, context = _tick_the_example(defs, instance)
        records = _observations(instance, EXAMPLE_DERIVED)

    assert json.loads(context.cursor)[EXAMPLE_DERIVED]["status"] == "published"
    assert len(records) == 1
    metadata = records[0].asset_observation.metadata
    assert metadata["classification_origin"].value == "propagated"
    columns = metadata["columns"].value

    assert columns["state"]["basis"] == COLUMN_LINEAGE
    assert columns["state"]["label"]["example_ladder"] == ["rung_open"]

    # Downgraded: the source column is `rung_restricted` and carries an
    # obligation, and the rule both lowers the ladder and CLEARS the set.
    assert columns["exposure_total"]["basis"] == COLUMN_LINEAGE
    assert columns["exposure_total"]["label"] == {
        "example_ladder": ["rung_internal"],
        "example_facet": [],
    }
    downgrade = columns["exposure_total"]["downgrade"]
    assert downgrade["rule"] == "state_total_is_not_a_party"
    assert downgrade["owner"] == "example-board"
    assert downgrade["derived"]["example_ladder"] == ["rung_restricted"]

    # No lineage declared, so the whole table's ceiling -- which includes the
    # source's unlabelled column, published deny-by-default.
    assert columns["party_count"]["basis"] == TABLE_MAX
    assert columns["party_count"]["label"] == {
        "example_ladder": ["rung_restricted"],
        "example_facet": ["facet_x", "facet_y"],
    }
    assert metadata["table_max_columns"].value == ["party_count"]


def test_a_declared_downgrade_is_validated_against_the_document_at_load(example):
    """A rule naming a value nobody declared, heard about while editing YAML.

    The same stance as step 4a's labels: a typo in a governance decision is
    not something to discover at the first tick after a deploy.
    """
    yaml_path = example / "components" / "propagation" / "component.yaml"
    yaml_path.write_text(
        yaml_path.read_text(encoding="utf-8").replace(
            "example_ladder: rung_internal", "example_ladder: rung_invented"
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception) as caught:
        _load_example(example)

    # The claim is about the CHAIN, not the type: Dagster wraps whatever a
    # component raises during the tree build in a `ComponentTreeException`,
    # and what an operator reads is the cause underneath it.
    text = _chain_text(caught.value)
    assert "DimensionsError" in text
    assert "does not declare 'rung_invented'" in text
    assert "example_ladder" in text


def test_the_code_location_loads_with_no_document_mounted(example, monkeypatch):
    """A developer's checkout has no cluster mount; the tick is what fails.

    Deny by default makes an absent document unusable rather than permissive,
    so nothing is published from it -- and failing the load instead would take
    down every other component in the code location, the source asset
    included.
    """
    monkeypatch.setenv(
        "DAGTOOLS_CLASSIFICATION_LEVELS", str(example / "absent.yaml")
    )

    defs = _load_example(example)

    assert list(defs.sensors or ())
    with dg.DagsterInstance.ephemeral() as instance:
        with pytest.raises(DimensionsError):
            _tick_the_example(defs, instance)


@pytest.mark.parametrize(
    "overrides, match",
    [
        (
            {"dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:dagster,a,PROD)"},
            "platform URN",
        ),
        ({"sources": []}, "names no labelled source"),
        ({"sources": [DERIVED]}, "lists itself as a source"),
        ({"sources": [SOURCE, SOURCE]}, "twice"),
        ({"asset_key": "marts//a"}, "no empty part"),
    ],
)
def test_a_dataset_block_that_cannot_propagate_is_refused(overrides, match):
    with pytest.raises(ValidationError, match=match):
        _dataset(**overrides)


@pytest.mark.parametrize(
    "rule, match",
    [
        ({"owner": ""}, "must name its owner"),
        ({"reason": " "}, "must name its reason"),
        ({"id": ""}, "must name its id"),
        ({"columns": []}, "applies to no column"),
        ({"label": {}}, "declares no label"),
    ],
)
def test_a_downgrade_rule_nobody_is_accountable_for_is_refused(rule, match):
    """The questions an auditor asks of a downgrade, answered where it is declared."""
    block = {
        "id": "rule_one",
        "owner": "example-board",
        "reason": "an aggregate",
        "columns": ["out_one"],
        "label": {"ladder": "rung_low"},
    }
    block.update(rule)

    with pytest.raises(ValidationError, match=match):
        _dataset(downgrades=[block])


def test_two_rules_for_one_column_are_refused_rather_than_ordered():
    """Which rule the record credits decides who is accountable."""

    def _rule(rule_id):
        return {
            "id": rule_id,
            "owner": "example-board",
            "reason": "an aggregate",
            "columns": ["out_one"],
            "label": {"ladder": "rung_low"},
        }

    with pytest.raises(ValidationError, match="two downgrade rules for column"):
        _dataset(downgrades=[_rule("rule_one"), _rule("rule_two")])


def test_one_asset_configured_twice_is_refused():
    """Two blocks for one asset publish two label sets; the later would win."""
    with pytest.raises(ValidationError, match="configured twice"):
        _spec(datasets=[_dataset().model_dump(), _dataset().model_dump()])


def test_a_component_that_propagates_nothing_is_refused():
    with pytest.raises(ValidationError, match="at least one derived dataset"):
        _spec(datasets=[])


def test_an_unknown_sensor_status_is_refused_rather_than_defaulted():
    with pytest.raises(ValidationError, match="default_status"):
        _spec(default_status="PAUSED")


def test_a_key_nobody_reads_is_refused_rather_than_ignored():
    """A block that is accepted and then ignored is the worst outcome here."""
    with pytest.raises(ValidationError):
        DatasetBlock.model_validate(
            {
                "asset_key": DERIVED,
                "dataset_urn": URN,
                "sources": [SOURCE],
                "downgrade": [],
            }
        )
