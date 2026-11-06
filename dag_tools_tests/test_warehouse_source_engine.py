"""The labelling and observation engine, with no Dagster and no warehouse.

`dag_tools.warehouse_source` is pure on purpose: the mounted document in, a
validated label out; rows back in, an observation out. That is what makes the
rules in `AGENTS.md` §4m testable without a cluster, a ConfigMap or a
warehouse -- and these are the rules, not the plumbing.

Every dimension, value, relation and column named here is invented for this
module. The engine names none of them: which dimension is the ladder is
discovered from the document's own `hierarchical` flag.
"""

import pytest

from dag_tools.warehouse_source import (
    DEFAULT_DIMENSIONS_PATH,
    DIMENSIONS_ENV_VAR,
    DimensionSet,
    DimensionsError,
    ObservationError,
    SourceSpec,
    SourceSpecError,
    build_catalog_payload,
    env_of,
    label_source,
    load_source_spec,
    observe,
    plan_observation,
    platform_of,
    read_as_of,
    read_column_names,
    read_row_count,
    resolve_dimensions_path,
    schema_hash,
)

URN = "urn:li:dataset:(urn:li:dataPlatform:some_platform,a.b.c,SOME_FABRIC)"


def _document(*, hierarchical_count=1, unordered_values=("facet_x", "facet_y")):
    ladder = {
        "id": "ladder",
        "hierarchical": True,
        "values": [
            {"id": "rung_low", "ratified_by": "board"},
            {"id": "rung_mid", "ratified_by": "board"},
            {"id": "rung_high", "ratified_by": "board"},
        ],
    }
    dimensions = [ladder]
    if hierarchical_count == 2:
        dimensions.append(dict(ladder, id="second_ladder"))
    elif hierarchical_count == 0:
        dimensions = [dict(ladder, hierarchical=False)]
    dimensions.append(
        {
            "id": "facet",
            "values": [
                {"id": value, "ratified_by": "board"} for value in unordered_values
            ],
        }
    )
    return {"classification_dimensions": dimensions}


def _dimensions(**kwargs) -> DimensionSet:
    return DimensionSet.from_document(
        _document(**kwargs), source="/mounted/doc.yaml", digest="deadbeef"
    )


def _spec(columns=None, **overrides):
    block = {
        "dataset_urn": URN,
        "table": "some_relation",
        "access": "brokered",
        "owner": "a-team",
        "label_origin": "board",
        "columns": columns if columns is not None else [],
    }
    block.update(overrides)
    return load_source_spec(block)


# --- the mounted document ---------------------------------------------------


def test_exactly_one_dimension_is_the_ladder():
    """Zero ladders and two ladders are both refused, and for one reason.

    A clearance is a ceiling on the ordered dimension. With none there is
    nothing to order it against; with two, "the reader's clearance" names two
    different ceilings and whichever the code happened to find first would
    decide what gets served.
    """
    with pytest.raises(DimensionsError):
        _dimensions(hierarchical_count=0)
    with pytest.raises(DimensionsError):
        _dimensions(hierarchical_count=2)

    assert _dimensions().hierarchical.id == "ladder"


def test_the_most_restrictive_label_is_the_top_rung_and_EVERY_facet():
    """Deny by default, and the asymmetry between the two kinds of dimension.

    On the ladder, most restrictive is the last row the document declares. On
    an unordered dimension a label's values are CONJUNCTIVE -- a reader must
    hold all of them -- so requiring every value at once is the narrowest a
    column can be. A column nobody labelled comes out exactly there.
    """
    label = _dimensions().most_restrictive_label()

    assert label.as_dict() == {
        "ladder": ["rung_high"],
        "facet": ["facet_x", "facet_y"],
    }


def test_a_list_on_the_ladder_is_refused_rather_than_interpreted():
    """A range, or a maximum? Nobody can tell, so nobody guesses."""
    dimensions = _dimensions()

    with pytest.raises(DimensionsError, match="exactly one"):
        dimensions.validate_label({"ladder": ["rung_low", "rung_mid"]})


def test_a_label_missing_the_ladder_is_refused_not_defaulted():
    """A half-written label is how a column gets labelled by accident.

    A column with no entry at all is most restrictive everywhere, which is
    safe. A column with an entry that forgot the ladder is not defaulted into
    that, because the operator plainly meant to say something.
    """
    with pytest.raises(DimensionsError, match="hierarchical"):
        _dimensions().validate_label({"facet": "facet_x"})


def test_a_value_the_document_does_not_declare_is_refused():
    with pytest.raises(DimensionsError, match="does not declare"):
        _dimensions().validate_label({"ladder": "rung_invented"})


def test_two_operators_writing_one_label_differently_publish_one_label():
    """Canonical order, so a label is comparable and its digest is stable."""
    dimensions = _dimensions()

    one = dimensions.validate_label({"facet": ["facet_y", "facet_x"], "ladder": "rung_mid"})
    two = dimensions.validate_label({"ladder": "rung_mid", "facet": ["facet_x", "facet_y"]})

    assert one == two
    assert list(one.as_dict()) == ["ladder", "facet"]


def test_an_empty_document_is_unusable_rather_than_permissive():
    """The deny-by-default case, at the document level.

    An absent or empty document means every column is most restrictive in
    every dimension -- so there is nothing to serve, and nothing to publish
    either. It must never read as "no restrictions declared".
    """
    empty = DimensionSet.from_document(
        {"classification_dimensions": []}, source="/mounted/doc.yaml", digest="0"
    )

    assert empty.is_empty
    assert not empty.is_usable
    with pytest.raises(DimensionsError):
        empty.most_restrictive_label()
    with pytest.raises(DimensionsError):
        label_source(_spec(), empty)


def test_a_dimension_declaring_no_values_makes_the_document_unusable():
    """Legitimate before the board has ratified anything -- and still unusable.

    There is no most restrictive value on an empty dimension, so a column
    cannot even be labelled deny-by-default against it.
    """
    document = _document()
    document["classification_dimensions"][1]["values"] = []
    dimensions = DimensionSet.from_document(
        document, source="/mounted/doc.yaml", digest="0"
    )

    assert not dimensions.is_usable
    assert dimensions.unratified == ("facet",)


def test_values_with_no_signature_are_reported_not_refused():
    """`ratified_by` is policy's record, and a different question.

    A deployment standing the document up before each value is signed is a
    legitimate state, so the engine names them and the component's
    `require_ratified` is how a deployment opts into refusing.
    """
    document = _document()
    document["classification_dimensions"][1]["values"][1].pop("ratified_by")
    dimensions = DimensionSet.from_document(
        document, source="/mounted/doc.yaml", digest="0"
    )

    assert dimensions.is_usable
    assert dimensions.values_without_ratification == ("facet.facet_y",)


def test_a_policy_declared_term_urn_wins_over_the_derived_one():
    """The derivation composes two ids the document supplies; it invents none.

    And where policy has already minted the glossary term, that is the term:
    publishing a derived one next to it would put two terms in the catalog
    for one value.
    """
    document = _document()
    document["classification_dimensions"][0]["values"][0]["term_urn"] = (
        "urn:li:glossaryTerm:policy-minted"
    )
    dimensions = DimensionSet.from_document(
        document, source="/mounted/doc.yaml", digest="0"
    )

    assert dimensions.terms_for(dimensions.validate_label({"ladder": "rung_low"})) == (
        "urn:li:glossaryTerm:policy-minted",
    )
    assert dimensions.terms_for(dimensions.validate_label({"ladder": "rung_mid"})) == (
        "urn:li:glossaryTerm:ladder.rung_mid",
    )


def test_explicit_configuration_beats_the_ambient_variable():
    """Same stance as `dest_config`, and here the stake is higher.

    What an ambient variable would retarget is which document a label is
    validated against -- so a leftover from another deployment could quietly
    decide what this one publishes.
    """
    environ = {DIMENSIONS_ENV_VAR: "/from/env.yaml"}

    assert resolve_dimensions_path("/from/yaml.yaml", environ) == "/from/yaml.yaml"
    assert resolve_dimensions_path(None, environ) == "/from/env.yaml"
    assert resolve_dimensions_path(None, {}) == DEFAULT_DIMENSIONS_PATH


# --- the URN ----------------------------------------------------------------


def test_the_asset_of_record_may_not_be_a_dagster_urn():
    """Refused, not discouraged: the failure it causes is invisible.

    The catalog would describe one object, a crawler would discover a second,
    and a broker asked to authorize a read of the physical one would find no
    labels on it.
    """
    with pytest.raises(SourceSpecError, match="dagster"):
        _spec(dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dagster,a.b,PROD)")


def test_the_platform_and_the_fabric_are_read_off_the_operators_urn():
    """Neither is named here. The fabric is a deployment's own word."""
    assert platform_of(URN) == "some_platform"
    assert env_of(URN) == "SOME_FABRIC"

    with pytest.raises(SourceSpecError):
        platform_of("some_platform://a.b.c")


def test_one_column_cannot_carry_two_labels():
    """Two labels on one column publish whichever was resolved last."""
    with pytest.raises(SourceSpecError, match="more than once"):
        _spec(
            columns=[
                {"name": "c_one", "labels": {"ladder": "rung_low"}},
                {"name": "C_ONE", "labels": {"ladder": "rung_high"}},
            ]
        )


def test_an_empty_label_is_refused_so_it_is_never_read_as_deny_by_default():
    with pytest.raises(SourceSpecError):
        _spec(columns=[{"name": "c_one", "labels": {}}])


# --- resolving labels against what the source really has --------------------


def test_an_unlabelled_column_is_published_most_restrictive():
    """An incomplete list under-serves rather than over-serves."""
    labelled = label_source(
        _spec(columns=[{"name": "c_one", "labels": {"ladder": "rung_low"}}]),
        _dimensions(),
    )

    resolved = labelled.resolve(("c_one", "c_two"))

    assert resolved.defaulted == ("c_two",)
    assert resolved.as_dict()["c_two"] == {
        "ladder": ["rung_high"],
        "facet": ["facet_x", "facet_y"],
    }


def test_the_declared_spelling_is_only_how_the_column_was_found():
    """A warehouse returns identifiers in its own case.

    The published spelling is the source's; matching is case-insensitive,
    because the operator's YAML cannot be made to guess which case the
    driver will hand back.
    """
    labelled = label_source(
        _spec(columns=[{"name": "C_One", "labels": {"ladder": "rung_low"}}]),
        _dimensions(),
    )

    resolved = labelled.resolve(("c_one",))

    assert resolved.defaulted == ()
    assert list(resolved.as_dict()) == ["c_one"]


def test_a_labelled_column_the_source_no_longer_has_fails_loudly():
    """The labels are stale, and the quiet outcome is the dangerous one.

    Whatever those columns were renamed to is now published as most
    restrictive, so no read of it would be served -- and the published label
    set would describe the wrong table while looking complete.
    """
    labelled = label_source(
        _spec(columns=[{"name": "c_gone", "labels": {"ladder": "rung_low"}}]),
        _dimensions(),
    )

    with pytest.raises(SourceSpecError, match="stale"):
        labelled.resolve(("c_one",))


def test_columns_differing_only_by_case_are_refused():
    """A label cannot be attached to one of them without risking the other."""
    labelled = label_source(_spec(), _dimensions())

    with pytest.raises(SourceSpecError, match="differ only by case"):
        labelled.resolve(("c_one", "C_ONE"))


# --- observing --------------------------------------------------------------


def test_the_default_statements_move_nothing():
    """An aggregate and an empty projection. That is the whole observation."""
    plan = plan_observation(table="some_relation")

    assert "some_relation" in plan.row_count_statement
    assert "count(" in plan.row_count_statement.lower()
    assert "1 = 0" in plan.columns_statement
    assert plan.as_of_statement is None


def test_an_operators_own_statement_is_used_verbatim():
    """The moment this library rewrites an operator's SQL it owns a dialect."""
    plan = plan_observation(
        table="some_relation",
        row_count_statement="SELECT approximate_count FROM {table} SAMPLE 1",
        as_of_statement="SELECT max(some_column) FROM {table}",
    )

    assert plan.row_count_statement == (
        "SELECT approximate_count FROM some_relation SAMPLE 1"
    )
    assert plan.as_of_statement == "SELECT max(some_column) FROM some_relation"


def test_a_relation_carrying_a_statement_separator_is_refused_at_load():
    """Not a sanitizer -- an operator can always write their own statement.

    It is here so that a copy-paste accident fails where the operator is
    looking, instead of running as two statements.
    """
    with pytest.raises(ObservationError, match="';'"):
        plan_observation(table="some_relation; DROP TABLE other")


def test_a_row_count_past_float_precision_is_exact():
    """A row count is exactly the kind of number that goes past 2**53.

    `float()` would round it, and the rounded value would then move the data
    version on a table that had not changed.
    """
    huge = 2**53 + 1

    assert read_row_count([{"n": str(huge)}]) == huge


def test_a_grouped_count_is_refused_rather_than_summed():
    """Summing would invent an answer the operator did not ask for."""
    with pytest.raises(ObservationError, match="2 rows"):
        read_row_count([{"n": 1}, {"n": 2}])

    with pytest.raises(ObservationError, match="2 values"):
        read_row_count([{"n": 1, "m": 2}])


def test_the_count_is_read_positionally_not_by_alias():
    """Insisting on an alias would be this library naming a column."""
    assert read_row_count([{"whatever_the_dialect_prefers": 7}]) == 7


def test_an_absent_as_of_is_refused_rather_than_recorded_as_empty():
    """An absent as-of is not a fresh one."""
    with pytest.raises(ObservationError):
        read_as_of([{"watermark": None}])

    assert read_as_of([{"watermark": "2026-01-01 00:00:00"}]) == "2026-01-01 00:00:00"


def test_a_source_that_projects_nothing_is_refused():
    """Either the relation is not there or the statement does not project it.

    Both are worth failing for: an observation reporting zero columns would
    publish a label set describing a source that does not exist.
    """
    with pytest.raises(ObservationError):
        read_column_names([])


def test_a_reordered_table_is_a_changed_table():
    """Order is part of the schema hash.

    A labelling written against the old order deserves to be looked at again.
    """
    assert schema_hash(["c_one", "c_two"]) != schema_hash(["c_two", "c_one"])


def test_the_data_version_moves_when_only_the_as_of_moves():
    """Which is what lets an operator's own as-of carry a freshness signal.

    Nothing read the data, so the version cannot be a hash of it -- a source
    whose count and shape are unchanged is not thereby unchanged.
    """
    one = observe(
        column_names=["c_one"], row_count=5, as_of="w-1", observed_at="t-0"
    )
    two = observe(
        column_names=["c_one"], row_count=5, as_of="w-2", observed_at="t-0"
    )

    assert one.data_version != two.data_version


def test_without_an_as_of_the_version_is_the_time_of_the_look():
    """Documented rather than hidden: it says when, not what."""
    observation = observe(column_names=["c_one"], row_count=1, observed_at="t-0")

    assert observation.as_of == "t-0"


# --- what the catalog is told -----------------------------------------------


def _payload(columns=None, observed=("c_one", "c_two")):
    dimensions = _dimensions()
    spec = _spec(
        columns=columns
        if columns is not None
        else [{"name": "c_one", "labels": {"ladder": "rung_low"}}]
    )
    labelled = label_source(spec, dimensions)
    observation = observe(
        column_names=observed, row_count=3, as_of="w-1", observed_at="t-0"
    )
    return build_catalog_payload(
        spec=spec,
        resolved=labelled.resolve(observation.column_names),
        observation=observation,
        dimensions=dimensions,
        asset_key="some_asset",
    )


def test_every_observed_column_is_published_including_the_defaulted_ones():
    """A column left out of the catalog reads as a column with no policy.

    Deny by default has to be visible to be trusted: a reader must be able to
    see that the column is restricted, not merely fail to find it.
    """
    payload = _payload()

    assert [column.name for column in payload.columns] == ["c_one", "c_two"]
    assert payload.defaulted_columns == ("c_two",)
    assert payload.column_terms["c_two"] == [
        "urn:li:glossaryTerm:ladder.rung_high",
        "urn:li:glossaryTerm:facet.facet_x",
        "urn:li:glossaryTerm:facet.facet_y",
    ]


def test_the_published_label_set_says_which_document_ratified_it():
    """The question an auditor asks of a label is which document it came from.

    Two files that differ are two answers, so the digest travels with the
    path -- and both travel with the labels rather than beside them.
    """
    import json

    payload = _payload()
    classification = json.loads(payload.custom_properties["classification"])

    assert classification["dimensions_source"] == "/mounted/doc.yaml"
    assert classification["dimensions_digest"] == "deadbeef"
    assert classification["columns"]["c_one"]["labels"] == {"ladder": ["rung_low"]}


def test_the_access_mode_and_the_owner_reach_the_catalog():
    """A reader that cannot tell brokered from passthrough cannot tell whether
    a denial is expected."""
    payload = _payload()

    assert payload.custom_properties["access"] == "brokered"
    assert payload.custom_properties["owner"] == "a-team"
    assert payload.custom_properties["classification_origin"] == "board"


def test_a_cell_predicate_is_published_and_never_interpreted():
    """This library owns no dialect, so it carries the predicate as text."""
    payload = _payload(
        columns=[
            {
                "name": "c_one",
                "labels": {"ladder": "rung_low"},
                "cell_predicate": "c_two IS NOT NULL",
            }
        ]
    )

    assert payload.columns[0].cell_predicate == "c_two IS NOT NULL"


def test_the_engine_imports_no_dagster_no_datahub_no_warehouse():
    """The claim the whole package rests on, asserted rather than assumed.

    It is what makes the labelling semantics testable without infrastructure
    -- the same discipline `otel_api_sync` keeps for its mapping engine.
    """
    import importlib
    import sys

    forbidden = ("dagster", "datahub", "sqlalchemy", "dlt")
    package = "dag_tools.warehouse_source"
    # The re-import has to happen with the package absent, and the originals
    # have to come back afterwards: a module re-imported under the same name is
    # a DIFFERENT module object, so every class it defines is a second class.
    # Anything still holding the first one -- another test module's
    # `pytest.raises(DimensionsError)`, or the component itself -- would stop
    # recognising the exceptions this package raises, and only in a run where
    # this test went first.
    evicted = {
        name: module
        for name, module in sys.modules.items()
        if name == package or name.startswith(package + ".")
    }
    for name in evicted:
        del sys.modules[name]
    before = {name for name in sys.modules if name.split(".")[0] in forbidden}

    try:
        importlib.import_module(package)
        after = {name for name in sys.modules if name.split(".")[0] in forbidden}
    finally:
        for name in [
            name
            for name in sys.modules
            if name == package or name.startswith(package + ".")
        ]:
            del sys.modules[name]
        sys.modules.update(evicted)

    assert after == before
    assert sys.modules[package] is evicted[package]


def test_the_spec_model_refuses_a_key_nobody_reads():
    """An ignored key is a knob the operator believes they turned."""
    with pytest.raises(SourceSpecError):
        load_source_spec(
            {
                "dataset_urn": URN,
                "table": "some_relation",
                "access": "brokered",
                "owner": "a-team",
                "label_origin": "board",
                "colunms": [],
            }
        )

    assert isinstance(_spec(), SourceSpec)
