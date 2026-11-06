"""`dag_tools.audit.warehouse` -- how the tag travels, decided as data.

The module executes nothing, so these tests read a plan rather than a
connection. That is the point of the split: the decisions worth pinning here
are the ordering, the binding and the refusals, and none of them needs a
driver CI does not install.

Every object name in here is synthetic, and every tag statement is a stand-in
for the operator's own: this library ships no dialect.
"""

import pytest

from dag_tools.audit.warehouse import (
    COMMENT,
    SESSION,
    TAG_PARAMETER,
    PlanStep,
    QueryPlan,
    TagTransport,
    UnsafeTag,
    UntaggedQuery,
    comment_tagged,
    plan_steps,
    plan_tagged_query,
)

TAG = '{"asset_key":"a/b","principal":"pipeline:p","schema_version":1}'
SET_TAG = "ALTER SESSION SET QUERY_TAG = :tag"


def _session() -> TagTransport:
    return TagTransport(placement=SESSION, statement=SET_TAG)


def test_the_tag_step_runs_before_the_query():
    """The whole of the session form's correctness.

    A tag set after the read describes the next query, so a reordering here
    would attribute every read to whatever ran before it -- off by one, in a
    report whose only job is attribution.
    """
    plan = plan_tagged_query(_session(), TAG, "SELECT 1 FROM t", {"x": 2})

    assert plan_steps(plan) == [
        (SET_TAG, {TAG_PARAMETER: TAG}),
        ("SELECT 1 FROM t", {"x": 2}),
    ]


def test_the_tag_is_bound_and_never_interpolated():
    """The tag carries an asset key, which is the one part anything can shape.

    An asset key reaching SQL text is the injection the rest of this module's
    strictness exists to avoid, so the session form binds it as a value.
    """
    plan = plan_tagged_query(_session(), TAG, "SELECT 1")

    tag_step = plan.prelude[0]
    assert TAG not in tag_step.sql
    assert tag_step.parameters == {TAG_PARAMETER: TAG}


def test_comment_placement_puts_the_tag_in_the_statement():
    plan = plan_tagged_query(TagTransport(placement=COMMENT), TAG, "SELECT 1")

    assert plan.prelude == []
    assert plan.query.sql == f"/* {TAG} */ SELECT 1"
    assert plan.carries_tag


def test_an_untagged_query_is_refused_rather_than_sent():
    """The seal of step 2, at the level that decides it.

    A read that reaches the warehouse untagged comes back out of history
    indistinguishable from a pipeline nobody has attributed -- so refusing is
    loud and sending is a quiet lie about the scope.
    """
    for empty in ("", "   ", None):
        with pytest.raises(UntaggedQuery):
            plan_tagged_query(TagTransport(placement=COMMENT), empty, "SELECT 1")


def test_carries_tag_is_computed_not_asserted():
    """A plan built some other way still says truthfully whether it is tagged.

    This is the check the resource repeats immediately before executing. The
    two look redundant until a later edit grows a path that builds a plan
    without going through `plan_tagged_query`; then this is the one that
    holds.
    """
    bare = QueryPlan(
        tag=TAG,
        placement=COMMENT,
        steps=[PlanStep(kind="query", sql="SELECT 1")],
    )
    assert not bare.carries_tag

    mislabelled = QueryPlan(
        tag=TAG,
        placement=SESSION,
        steps=[
            PlanStep(kind="tag", sql=SET_TAG, parameters={TAG_PARAMETER: "other"}),
            PlanStep(kind="query", sql="SELECT 1"),
        ],
    )
    assert not mislabelled.carries_tag


def test_a_tag_carrying_a_comment_marker_is_refused():
    """It would close the comment and the remainder would be read as SQL.

    JSON escapes neither `*` nor `/`, so an object name or asset key carrying
    `*/` reaches the statement text intact. Escaping would change the bytes
    the report decodes and truncating would produce a tag that does not decode
    at all, so the only safe answer is to refuse.
    """
    with pytest.raises(UnsafeTag):
        comment_tagged("SELECT 1", '{"asset_key":"a*/ DROP TABLE t --"}')
    with pytest.raises(UnsafeTag):
        comment_tagged("SELECT 1", '{"asset_key":"a/* x"}')

    with pytest.raises(UnsafeTag):
        plan_tagged_query(
            TagTransport(placement=COMMENT), '{"a":"*/"}', "SELECT 1"
        )


def test_a_session_statement_must_bind_the_tag_as_a_parameter():
    """Validated at load, because the failure is otherwise invisible.

    A statement that does not bind `:tag` runs happily and every query after
    it is attributed to nothing. The operator would see a window of history
    in which nothing is explained and go looking for an untagged pipeline.
    """
    with pytest.raises(ValueError, match=":tag"):
        TagTransport(placement=SESSION, statement="ALTER SESSION SET X = 1")


def test_a_session_statement_must_be_one_statement():
    with pytest.raises(ValueError, match="one statement"):
        TagTransport(
            placement=SESSION, statement="SET a = :tag; DROP TABLE t"
        )


def test_a_trailing_semicolon_is_still_one_statement():
    """The refusal above is about batching, not about punctuation."""
    assert TagTransport(placement=SESSION, statement=f"{SET_TAG};").statement


def test_session_placement_demands_a_statement_and_guesses_no_dialect():
    with pytest.raises(ValueError, match="does not know your dialect"):
        TagTransport(placement=SESSION)


def test_a_statement_with_comment_placement_is_refused_not_ignored():
    """Silently ignoring it would leave the operator believing it ran."""
    with pytest.raises(ValueError, match="session placement only"):
        TagTransport(placement=COMMENT, statement=SET_TAG)


def test_an_unknown_placement_is_refused():
    with pytest.raises(ValueError, match="placement"):
        TagTransport(placement="query_tag")


def test_a_plan_needs_a_statement():
    with pytest.raises(ValueError, match="statement"):
        plan_tagged_query(TagTransport(placement=COMMENT), TAG, "   ")


def test_the_query_parameters_are_the_callers_and_nothing_else():
    """The transport adds no parameters to the read it carries.

    A transport that injected into the query's own bindings would collide with
    an operator's parameter name sooner or later, and the collision would
    surface as a wrong WHERE rather than as an error.
    """
    plan = plan_tagged_query(_session(), TAG, "SELECT 1 WHERE x = :x", {"x": 7})
    assert plan.query.parameters == {"x": 7}
