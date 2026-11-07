"""Propagation: what a derived column's label is, given its sources'.

ADR-0064 steps 4b and 4c, as rules rather than plumbing. The engine is pure,
so every rule here is exercised with a document invented for this module and
no cluster, no catalog and no warehouse. The package's purity itself is
asserted next door, in `test_warehouse_source_engine.py`.

Every dimension, value, dataset and column named here is invented for this
module. The engine names none of them: which dimension is the ladder, and how
each other one composes, is read off the mounted document.
"""

import pytest

from dag_tools.warehouse_source import (
    COLUMN_LINEAGE,
    EXCLUSIONARY,
    HIERARCHICAL,
    PERMISSIVE_SET,
    RESTRICTIVE_SET,
    TABLE_MAX,
    ColumnRef,
    DimensionSet,
    DimensionsError,
    DowngradeRule,
    Label,
    PropagationError,
    compose,
    propagate,
)

LADDER = {
    "id": "ladder",
    "hierarchical": True,
    "values": [
        {"id": "rung_low", "ratified_by": "board"},
        {"id": "rung_mid", "ratified_by": "board"},
        {"id": "rung_high", "ratified_by": "board"},
    ],
}


def _document(*extra, include_defaults=True):
    dimensions = [LADDER]
    if include_defaults:
        dimensions += [
            {
                "id": "held_one_of",
                "type": PERMISSIVE_SET,
                "values": [
                    {"id": "one_of_a", "ratified_by": "board"},
                    {"id": "one_of_b", "ratified_by": "board"},
                    {"id": "one_of_c", "ratified_by": "board"},
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
                "id": "held_none_of",
                "type": EXCLUSIONARY,
                "values": [
                    {"id": "none_of_a", "ratified_by": "board"},
                    {"id": "none_of_b", "ratified_by": "board"},
                ],
            },
        ]
    return {"classification_dimensions": dimensions + list(extra)}


def _dimensions(*extra, **kwargs) -> DimensionSet:
    return DimensionSet.from_document(
        _document(*extra, **kwargs), source="/mounted/doc.yaml", digest="deadbeef"
    )


def _of(dimensions, dimension_id):
    return dimensions.get(dimension_id)


# --- the four declared types ------------------------------------------------


def test_an_untyped_dimension_still_means_what_it_always_meant():
    """A mounted document that predates `type:` must not change meaning.

    Every unordered dimension was conjunctive -- a reader held all of a
    label's values -- so an untyped one is a restrictive set, and its most
    restrictive label is still every value. A document already on a cluster
    is governance's, not ours; a library release may not re-interpret it.
    """
    dimensions = _dimensions(
        {
            "id": "untyped",
            "values": [
                {"id": "u_a", "ratified_by": "board"},
                {"id": "u_b", "ratified_by": "board"},
            ],
        }
    )
    untyped = _of(dimensions, "untyped")
    assert untyped.kind == RESTRICTIVE_SET
    assert untyped.most_restrictive() == ("u_a", "u_b")
    assert untyped.most_restrictive_of([("u_a",), ("u_b",)]) == ("u_a", "u_b")


def test_a_permissive_set_composes_by_INTERSECTION_not_union():
    """The one place the four types do not line up.

    A reader needs *one* of a permissive set, so a wider set admits more
    readers. Composing two sources by union -- the rule every other
    dimension follows -- would publish a derived column as readable by
    people who could read neither source. The join there is the
    intersection.
    """
    permissive = _of(_dimensions(), "held_one_of")
    assert permissive.most_restrictive_of(
        [("one_of_a", "one_of_b"), ("one_of_b", "one_of_c")]
    ) == ("one_of_b",)
    assert permissive.most_restrictive_of([("one_of_a",), ("one_of_c",)]) == ()


def test_the_most_restrictive_permissive_set_is_empty_and_releases_to_nobody():
    """Empty is the floor on a permissive set and the ceiling everywhere else.

    Deny-by-default has to come out of the same `most_restrictive()` call for
    every type, so the inversion lives on the dimension. An empty permissive
    set read as "no restriction" is how a column nobody labelled gets served
    to everybody.
    """
    dimensions = _dimensions()
    assert _of(dimensions, "held_one_of").most_restrictive() == ()
    assert _of(dimensions, "held_one_of").least_restrictive() == (
        "one_of_a",
        "one_of_b",
        "one_of_c",
    )
    deny = dimensions.most_restrictive_label().as_dict()
    assert deny["held_one_of"] == []
    assert deny["held_all_of"] == ["all_of_a", "all_of_b"]
    assert deny["held_none_of"] == ["none_of_a", "none_of_b"]
    assert deny["ladder"] == ["rung_high"]


def test_the_ladder_composes_by_max_and_the_other_sets_by_union():
    dimensions = _dimensions()
    assert _of(dimensions, "ladder").most_restrictive_of(
        [("rung_mid",), ("rung_low",)]
    ) == ("rung_mid",)
    assert _of(dimensions, "held_all_of").most_restrictive_of(
        [("all_of_a",), ("all_of_b",)]
    ) == ("all_of_a", "all_of_b")
    assert _of(dimensions, "held_none_of").most_restrictive_of(
        [("none_of_b",), ()]
    ) == ("none_of_b",)


@pytest.mark.parametrize(
    "dimension_id,sets",
    [
        ("ladder", [("rung_low",), ("rung_high",), ("rung_mid",)]),
        ("held_one_of", [("one_of_a", "one_of_b"), ("one_of_b", "one_of_c")]),
        ("held_all_of", [("all_of_a",), ("all_of_b",), ()]),
        ("held_none_of", [("none_of_a",), (), ("none_of_a", "none_of_b")]),
    ],
)
def test_composition_is_the_join_of_one_order_in_every_type(dimension_id, sets):
    """Composition is not four rules; it is one order read four ways.

    `at_most_as_restrictive_as` asks the same question of every type -- does
    this admit at least every reader that does? -- and the composed label is
    its least upper bound. Pinning that here is what makes the per-type
    branches in `most_restrictive_of` checkable rather than four guesses.
    """
    dimension = _of(_dimensions(), dimension_id)
    joined = dimension.most_restrictive_of(sets)
    for values in sets:
        assert dimension.at_most_as_restrictive_as(values, joined)
    assert dimension.at_most_as_restrictive_as(joined, joined)
    # And it is the LEAST such: nothing wider than the join still bounds
    # every input, which is what stops a type from over-restricting.
    for values in sets:
        if set(values) != set(joined):
            assert not dimension.at_most_as_restrictive_as(joined, values)


def test_composing_nothing_adds_no_restriction_of_its_own():
    """A dimension no source declares is the identity of its own join.

    Which is why `compose` may skip the labels that leave a dimension out
    instead of having to invent a value for them.
    """
    dimensions = _dimensions()
    assert _of(dimensions, "held_all_of").most_restrictive_of([]) == ()
    assert _of(dimensions, "held_none_of").most_restrictive_of([]) == ()
    assert _of(dimensions, "held_one_of").most_restrictive_of([]) == (
        "one_of_a",
        "one_of_b",
        "one_of_c",
    )
    assert _of(dimensions, "ladder").most_restrictive_of([]) == ("rung_low",)


def test_a_type_the_library_does_not_know_is_refused():
    """A type is a composition rule. An unknown one would have to be guessed."""
    with pytest.raises(DimensionsError) as caught:
        _dimensions({"id": "odd", "type": "mystery_set", "values": []})
    assert "mystery_set" in str(caught.value)


def test_a_dimension_cannot_declare_a_type_that_contradicts_the_flag():
    """`hierarchical: true` plus a set type has no reading we may pick.

    Both spellings stay readable -- the ladder was a flag before the other
    three types were named -- but which one won would decide who gets
    served, so a row that says both is refused.
    """
    with pytest.raises(DimensionsError):
        _dimensions(
            {
                "id": "both",
                "hierarchical": True,
                "type": PERMISSIVE_SET,
                "values": [{"id": "b_a", "ratified_by": "board"}],
            },
            include_defaults=False,
        )


def test_declaring_the_hierarchical_type_is_the_same_as_setting_the_flag():
    """So a document may spell the ladder either way and nothing else changes."""
    document = {
        "classification_dimensions": [
            {
                "id": "ladder",
                "type": HIERARCHICAL,
                "values": [
                    {"id": "rung_low", "ratified_by": "board"},
                    {"id": "rung_high", "ratified_by": "board"},
                ],
            }
        ]
    }
    dimensions = DimensionSet.from_document(document)
    assert dimensions.hierarchical.id == "ladder"
    assert dimensions.get("ladder").hierarchical is True
    assert dimensions.get("ladder").most_restrictive() == ("rung_high",)


# --- a derived label is total ----------------------------------------------


def test_a_derived_label_carries_every_dimension_even_an_undeclared_one():
    """Totality is a safety property, not tidiness.

    `values_for` answers `()` both for a dimension a label leaves out and for
    one it carries empty. On a permissive set those mean "no restriction" and
    "nobody" -- opposite answers -- so a derived label never omits a
    dimension, and an empty tuple in one always means nobody.
    """
    dimensions = _dimensions()
    source = dimensions.validate_label({"ladder": "rung_low"})
    derived = compose(dimensions, [source])
    assert [pair[0] for pair in derived.pairs] == list(dimensions.ids)
    assert derived.declares("held_one_of")
    assert derived.values_for("held_one_of") == (
        "one_of_a",
        "one_of_b",
        "one_of_c",
    )


def test_an_absent_dimension_and_an_empty_one_are_told_apart_by_declares():
    dimensions = _dimensions()
    absent = dimensions.validate_label({"ladder": "rung_low"})
    assert not absent.declares("held_one_of")
    assert absent.values_for("held_one_of") == ()
    nobody = Label(pairs=(("held_one_of", ()),))
    assert nobody.declares("held_one_of")
    assert nobody.values_for("held_one_of") == ()


def test_composing_no_labels_at_all_is_refused():
    """The least restrictive label must never be reached by arriving at it."""
    with pytest.raises(PropagationError) as caught:
        compose(_dimensions(), [])
    assert "least restrictive" in str(caught.value)


def test_composition_against_an_unusable_document_is_refused():
    dimensions = DimensionSet.from_document(
        {"classification_dimensions": [dict(LADDER, values=[])]}
    )
    with pytest.raises(DimensionsError):
        compose(dimensions, [Label(pairs=(("ladder", ("rung_low",)),))])


# --- propagation through column lineage ------------------------------------


def _inputs():
    return {
        "source_one": {
            "col_a": {"ladder": "rung_low", "held_one_of": ["one_of_a", "one_of_b"]},
            "col_b": {"ladder": "rung_mid", "held_all_of": "all_of_a"},
        },
        "source_two": {
            "col_c": {"ladder": "rung_high", "held_one_of": "one_of_b"},
        },
    }


def test_a_columns_label_is_the_max_over_the_source_columns_it_read():
    """Step 4b, in one assertion: lineage in, a composed ceiling out."""
    dimensions = _dimensions()
    result = propagate(
        dimensions,
        output_columns=["out_one"],
        inputs=_inputs(),
        lineage={
            "out_one": [
                ColumnRef("source_one", "col_a"),
                ColumnRef("source_one", "col_b"),
            ]
        },
    )
    (decision,) = result.columns
    assert decision.basis == COLUMN_LINEAGE
    assert decision.label.as_dict() == {
        "ladder": ["rung_mid"],
        "held_one_of": ["one_of_a", "one_of_b"],
        "held_all_of": ["all_of_a"],
        "held_none_of": [],
    }
    assert result.table_max_columns == ()


def test_a_column_with_no_declared_lineage_falls_back_to_the_table_max():
    """And the fallback spans every labelled column of every source read.

    A Python asset that declares no column lineage has read the whole table
    as far as anyone can prove. Falling back to nothing, or to the columns
    that happen to share a name, is how a derived column is published wider
    than its sources.
    """
    dimensions = _dimensions()
    result = propagate(
        dimensions,
        output_columns=["out_one", "out_two"],
        inputs=_inputs(),
        lineage={"out_one": [ColumnRef("source_one", "col_a")]},
    )
    narrow, wide = result.columns
    assert narrow.basis == COLUMN_LINEAGE
    assert narrow.label.values_for("ladder") == ("rung_low",)
    assert wide.basis == TABLE_MAX
    assert wide.sources == ()
    assert result.table_max_columns == ("out_two",)
    # The max of every source column: the top rung reached anywhere, the
    # intersection of the permissive sets, the union of the restrictive ones.
    assert wide.label.as_dict() == {
        "ladder": ["rung_high"],
        "held_one_of": ["one_of_b"],
        "held_all_of": ["all_of_a"],
        "held_none_of": [],
    }


def test_an_unlabelled_source_column_fails_rather_than_counting_as_unrestricted():
    """Deny-by-default belongs where the source is declared, not here.

    By the time propagation runs, a source column with no label means
    lineage and labels disagree about what was read -- and composing the
    ones that are present publishes a ceiling computed from a subset of the
    inputs, which looks exactly like a correct answer.
    """
    with pytest.raises(PropagationError) as caught:
        propagate(
            _dimensions(),
            output_columns=["out_one"],
            inputs=_inputs(),
            lineage={
                "out_one": [
                    ColumnRef("source_one", "col_a"),
                    ColumnRef("source_one", "col_unlabelled"),
                ]
            },
        )
    message = str(caught.value)
    assert "col_unlabelled" in message
    assert "not an unrestricted one" in message


def test_lineage_from_a_source_nothing_labelled_fails():
    with pytest.raises(PropagationError) as caught:
        propagate(
            _dimensions(),
            output_columns=["out_one"],
            inputs=_inputs(),
            lineage={"out_one": [ColumnRef("source_three", "col_a")]},
        )
    assert "source_three" in str(caught.value)


def test_propagating_with_no_labelled_input_at_all_is_refused():
    """An asset that reads nothing labelled is not derived."""
    with pytest.raises(PropagationError) as caught:
        propagate(_dimensions(), output_columns=["out_one"], inputs={})
    assert "not derived" in str(caught.value)


def test_a_source_column_in_another_casing_still_resolves():
    """A warehouse upper-cases its identifiers; a loader snake-cases them.

    One mapping of labels has to work against lineage that came back in
    either spelling -- the same tolerance `spec.py` keeps when it matches a
    declared label to an observed column.
    """
    result = propagate(
        _dimensions(),
        output_columns=["out_one"],
        inputs=_inputs(),
        lineage={"out_one": [ColumnRef("SOURCE_ONE", "COL_A")]},
    )
    assert result.columns[0].label.values_for("ladder") == ("rung_low",)


def test_source_columns_differing_only_by_case_are_refused():
    """Which one was meant decides the label, so it is not picked."""
    inputs = {
        "source_one": {
            "col_a": {"ladder": "rung_low"},
            "COL_A": {"ladder": "rung_high"},
        }
    }
    with pytest.raises(PropagationError) as caught:
        propagate(
            _dimensions(),
            output_columns=["out_one"],
            inputs=inputs,
            lineage={"out_one": [ColumnRef("source_one", "Col_A")]},
        )
    assert "but for case" in str(caught.value)


def test_a_source_label_outside_the_vocabulary_fails_where_every_other_does():
    with pytest.raises(DimensionsError):
        propagate(
            _dimensions(),
            output_columns=["out_one"],
            inputs={"source_one": {"col_a": {"ladder": "rung_imaginary"}}},
            lineage={"out_one": [ColumnRef("source_one", "col_a")]},
        )


# --- downgrades -------------------------------------------------------------


def _rule(**overrides):
    block = {
        "id": "rule_one",
        "owner": "some-steward",
        "reason": "a reason the aspect carries",
        "label": {"ladder": "rung_low"},
        "columns": ("out_one",),
    }
    block.update(overrides)
    return DowngradeRule(**block)


def _propagate_with(rule, columns=("out_one",)):
    return propagate(
        _dimensions(),
        output_columns=list(columns),
        inputs=_inputs(),
        lineage={
            column: [ColumnRef("source_one", "col_b")] for column in columns
        },
        downgrades=[rule],
    )


def test_an_applied_downgrade_records_the_rule_owner_reason_and_both_labels():
    """Step 4c's decision aspect, in full.

    Both labels travel on purpose: the derived one is what the data says,
    the declared one is what was published, and an aspect carrying only the
    second cannot be checked against the first.
    """
    result = _propagate_with(_rule())
    (decision,) = result.columns
    assert decision.is_downgraded
    assert decision.label.values_for("ladder") == ("rung_low",)
    assert decision.derived.values_for("ladder") == ("rung_mid",)
    assert decision.basis == COLUMN_LINEAGE
    recorded = decision.downgrade.as_dict()
    assert recorded["rule"] == "rule_one"
    assert recorded["owner"] == "some-steward"
    assert recorded["reason"] == "a reason the aspect carries"
    assert recorded["derived"]["ladder"] == ["rung_mid"]
    assert recorded["declared"]["ladder"] == ["rung_low"]
    assert result.downgrades == (decision.downgrade,)


def test_a_column_with_no_rule_records_no_downgrade():
    """No downgrade is automatic: absent a rule, the derived label stands."""
    result = propagate(
        _dimensions(),
        output_columns=["out_one"],
        inputs=_inputs(),
        lineage={"out_one": [ColumnRef("source_one", "col_b")]},
    )
    (decision,) = result.columns
    assert not decision.is_downgraded
    assert decision.downgrade is None
    assert decision.derived == decision.label
    assert result.downgrades == ()


def test_a_standing_rule_with_no_named_owner_fails():
    """A rule nobody owns cannot be withdrawn, reviewed or appealed to."""
    with pytest.raises(PropagationError) as caught:
        _propagate_with(_rule(owner="  "))
    assert "names no owner" in str(caught.value)


def test_a_declared_downgrade_with_no_reason_fails():
    """The reason is what the published aspect carries."""
    with pytest.raises(PropagationError) as caught:
        _propagate_with(_rule(reason=""))
    assert "no reason" in str(caught.value)


def test_a_rule_that_tightens_a_dimension_is_not_a_downgrade():
    """It would be recorded as a release of data that was never released.

    And a reader denied by it would have no rule to appeal to, because the
    rule claims to widen.
    """
    with pytest.raises(PropagationError) as caught:
        _propagate_with(_rule(label={"ladder": "rung_high"}))
    assert "more* restrictive" in str(caught.value)


def test_a_rule_that_tightens_only_a_set_dimension_is_still_refused():
    """The check is per dimension, so a mixed rule cannot hide a tightening."""
    with pytest.raises(PropagationError) as caught:
        _propagate_with(
            _rule(label={"ladder": "rung_low", "held_all_of": ["all_of_a", "all_of_b"]})
        )
    assert "held_all_of" in str(caught.value)


def test_a_downgrade_that_changes_nothing_is_refused():
    """A recorded decision that changed nothing misreports that one happened."""
    with pytest.raises(PropagationError) as caught:
        _propagate_with(_rule(label={"ladder": "rung_mid", "held_all_of": "all_of_a"}))
    assert "changed nothing" in str(caught.value)


def test_a_rule_aimed_at_a_column_the_asset_does_not_produce_fails():
    """A permission nobody is watching, left behind by a renamed column."""
    with pytest.raises(PropagationError) as caught:
        _propagate_with(_rule(columns=("out_renamed_away",)))
    assert "out_renamed_away" in str(caught.value)


def test_a_rule_naming_no_columns_is_refused():
    with pytest.raises(PropagationError) as caught:
        _propagate_with(_rule(columns=()))
    assert "names no columns" in str(caught.value)


def test_two_rules_for_one_column_are_refused_rather_than_ordered():
    """Which rule the aspect credits decides who is accountable."""
    with pytest.raises(PropagationError) as caught:
        propagate(
            _dimensions(),
            output_columns=["out_one"],
            inputs=_inputs(),
            lineage={"out_one": [ColumnRef("source_one", "col_b")]},
            downgrades=[_rule(), _rule(id="rule_two")],
        )
    assert "rule_two" in str(caught.value)


def test_a_rules_label_is_validated_against_the_document_like_any_other():
    with pytest.raises(DimensionsError):
        _propagate_with(_rule(label={"ladder": "rung_imaginary"}))


def test_a_rule_label_missing_the_ladder_is_refused_not_defaulted():
    """Same rule as every other label: a half-written one is not completed."""
    with pytest.raises(DimensionsError):
        _propagate_with(_rule(label={"held_all_of": "all_of_a"}))


def test_a_rule_says_nothing_about_the_dimensions_it_omits():
    """A downgrade relaxes what it names and leaves the rest where it derived.

    A rule's label is a declared label like any other, so it is partial. Read
    against a total derived label as-is, every dimension it stayed silent
    about would look like a tightening -- and a rule relaxing one dimension
    could never be written without restating all of them, which is how a
    second dimension gets changed by accident.
    """
    result = _propagate_with(_rule(label={"ladder": "rung_low"}))
    (decision,) = result.columns
    assert decision.label.as_dict() == {
        "ladder": ["rung_low"],
        "held_one_of": ["one_of_a", "one_of_b", "one_of_c"],
        "held_all_of": ["all_of_a"],
        "held_none_of": [],
    }
    assert decision.derived.values_for("held_all_of") == ("all_of_a",)


def test_a_downgraded_column_still_publishes_terms_from_the_document():
    """Step 4c: whatever is published, it is published as the document's terms."""
    dimensions = _dimensions()
    result = _propagate_with(_rule())
    terms = dimensions.terms_for(result.columns[0].label)
    assert terms[0] == "urn:li:glossaryTerm:ladder.rung_low"
    assert "urn:li:glossaryTerm:held_all_of.all_of_a" in terms
    assert all(term.startswith("urn:li:glossaryTerm:") for term in terms)


def test_the_propagation_serializes_to_what_the_catalog_publishes():
    result = _propagate_with(_rule(), columns=("out_one", "out_two"))
    rows = result.as_dict()["columns"]
    assert [row["column"] for row in rows] == ["out_one", "out_two"]
    assert rows[0]["sources"] == ["source_one.col_b"]
    assert rows[0]["basis"] == COLUMN_LINEAGE
    assert rows[0]["downgrade"]["rule"] == "rule_one"
    assert "downgrade" not in rows[1]
    assert result.labels["out_two"].values_for("ladder") == ("rung_mid",)


def test_an_empty_set_means_whatever_its_dimension_type_says_it_means():
    """One shape, three meanings, and none of them is "absent".

    A reader needs at least one value of a permissive dimension, so the empty
    set there releases to NOBODY -- it is that dimension's most restrictive
    label and what deny-by-default resolves to. On a restrictive set it is no
    obligation and on an exclusionary one nothing excluded, both of which are
    the least restrictive end. An empty set is a value the engine publishes
    and has to parse back, and on a declared label it is the only way to
    clear a dimension, so it is accepted everywhere except the ladder -- where
    it is zero values on a dimension that takes exactly one.
    """
    dimensions = _dimensions()

    for dimension in ("held_one_of", "held_all_of", "held_none_of"):
        label = dimensions.validate_label({"ladder": "rung_low", dimension: []})
        assert label.values_for(dimension) == ()
        # Not the same as never mentioning it: an overlay distinguishes the
        # two, so the engine has to as well.
        assert label.declares(dimension)

    with pytest.raises(DimensionsError, match="exactly one"):
        dimensions.validate_label({"ladder": []})


def test_the_deny_by_default_label_round_trips_through_validation():
    """What step 4a publishes, step 4b has to be able to read back.

    An unlabelled column publishes `most_restrictive_label()`, and on a
    document carrying a permissive dimension that label holds an empty set.
    Propagation reads a source's published labels back through
    `validate_label`, so a label this engine emits and then refuses would
    fail every defaulted column -- found by running it, not by review.
    """
    dimensions = _dimensions()
    defaulted = dimensions.most_restrictive_label()

    assert defaulted.values_for("held_one_of") == ()
    assert dimensions.validate_label(defaulted.as_dict()) == defaulted


def test_a_rule_can_clear_a_set_dimension_and_not_only_lower_the_ladder():
    """The empty set is what makes a downgrade expressible on a set dimension.

    A rule says nothing about the dimensions it omits, so omitting the set
    keeps the obligation the data derived. Declaring it empty is the operator
    saying the published column carries none -- which is the whole shape of a
    downgrade on anything but the ladder, and was unreachable while an empty
    list was refused.
    """
    rule = DowngradeRule(
        id="rule_clear",
        owner="example-board",
        reason="an aggregate carries none of the row-level obligations",
        label={"ladder": "rung_low", "held_all_of": []},
        columns=("out_one",),
    )
    result = _propagate_with(rule)
    decision = result.columns[0]

    assert decision.derived.values_for("held_all_of") == ("all_of_a",)
    assert decision.label.values_for("held_all_of") == ()
    assert decision.label.declares("held_all_of")
    assert decision.downgrade.rule == "rule_clear"
