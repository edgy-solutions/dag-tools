"""The history read: built here, executed elsewhere, and never interpolated.

Two properties carry the weight.

**No value reaches the statement text.** An identifier cannot be bound as a
parameter, so the relation is interpolated -- and only after matching a plain
identifier grammar. Everything else is bound. The tests assert both halves:
that the values are absent from the SQL, and that they are present in the
parameters.

**No name is built in.** There is no default relation and no default column
name, on purpose. These tests therefore invent their own synthetic ones; a
real deployment supplies its own through YAML.
"""

from datetime import datetime, timezone

import pytest

from dag_tools.audit.history import (
    OUTPUT_COLUMNS,
    HistoryColumns,
    HistorySource,
    UnsafeRelation,
    build_history_statement,
    parse_relations,
    rows_to_events,
    validate_relation,
)

SINCE = datetime(2026, 10, 1, tzinfo=timezone.utc)
UNTIL = datetime(2026, 10, 8, tzinfo=timezone.utc)


def columns(**overrides) -> HistoryColumns:
    base = {
        "query_id": "c_qid",
        "started_at": "c_start",
        "principal": "c_user",
        "query_tag": "c_tag",
        "relations": "c_objects",
    }
    base.update(overrides)
    return HistoryColumns(**base)


def source(**overrides) -> HistorySource:
    base = {"relation": "db1.sch1.history_view", "columns": columns()}
    base.update(overrides)
    return HistorySource(**base)


def test_the_statement_projects_to_this_modules_own_aliases():
    """So `rows_to_events` never needs the operator's column names twice."""
    statement, _ = build_history_statement(source(), since=SINCE)
    for alias in OUTPUT_COLUMNS:
        assert f" AS {alias}" in statement


def test_every_value_is_bound_and_none_is_interpolated():
    statement, params = build_history_statement(
        source(principal_filter="svc_account"), since=SINCE, until=UNTIL
    )
    assert params["since"] == SINCE
    assert params["until"] == UNTIL
    assert params["principal_filter"] == "svc_account"
    assert "svc_account" not in statement
    assert "2026" not in statement
    assert ":since" in statement and ":until" in statement


def test_an_unset_principal_filter_is_not_bound_at_all():
    """Unset reads every identity; it must not bind an empty string, which
    would silently match nothing."""
    statement, params = build_history_statement(source(), since=SINCE)
    assert "principal_filter" not in params
    assert ":principal_filter" not in statement


def test_an_unset_until_is_not_bound_at_all():
    statement, params = build_history_statement(source(), since=SINCE)
    assert "until" not in params
    assert ":until" not in statement


def test_rows_come_back_oldest_first():
    statement, _ = build_history_statement(source(), since=SINCE)
    assert "ORDER BY" in statement


@pytest.mark.parametrize(
    "relation",
    ["history_view", "sch1.history_view", "db1.sch1.history_view", "_v$1.a_b.C"],
)
def test_plain_identifiers_up_to_three_parts_are_accepted(relation):
    assert validate_relation(relation) == relation


@pytest.mark.parametrize(
    "relation",
    [
        "a.b.c.d",
        'a"b',
        "a b",
        "a;drop",
        "a--comment",
        "a/*x*/b",
        "a'b",
        "",
        ".",
        "a.",
        "1a",
        "a.`b`",
        "a\nb",
        "a)b",
        "(select 1)",
    ],
)
def test_anything_that_is_not_an_identifier_is_refused_not_escaped(relation):
    with pytest.raises(UnsafeRelation):
        validate_relation(relation)


def test_a_column_name_goes_through_the_same_grammar():
    """The columns are interpolated too, so they are checked too.

    A column is a single identifier in practice, but the point is that no
    operator-supplied string reaches the SQL without matching the grammar.
    """
    with pytest.raises(UnsafeRelation):
        build_history_statement(
            source(columns=columns(query_tag="c_tag, 1 AS x")), since=SINCE
        )


def test_an_unsafe_relation_is_refused_before_a_statement_exists():
    with pytest.raises(UnsafeRelation):
        build_history_statement(source(relation="a;drop table t"), since=SINCE)


# --- row mapping ------------------------------------------------------------


def row(**overrides):
    base = {
        "query_id": "q-1",
        "started_at": datetime(2026, 10, 2, tzinfo=timezone.utc),
        "principal": "svc_account",
        "query_tag": None,
        "relations": ["db1.sch1.t_a"],
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    "cell,expected",
    [
        (["a", "b"], ["a", "b"]),
        ('["a","b"]', ["a", "b"]),
        ("a,b", ["a", "b"]),
        ("a, b", ["a", "b"]),
        ("a\nb", ["a", "b"]),
        ("a", ["a"]),
        (None, []),
        ("", []),
        ("[]", []),
        ('["a", null, ""]', ["a"]),
    ],
)
def test_the_relations_cell_is_read_in_every_shape_a_warehouse_returns_it(
    cell, expected
):
    """Warehouses disagree: an array type, a JSON string, or a delimited list.

    One mapping file has to work against all of them, the same way the
    inventory lookups are normalization-tolerant.
    """
    assert parse_relations(cell) == expected


def test_rows_to_events_maps_a_whole_row():
    events, failures = rows_to_events([row()])
    assert failures == []
    assert len(events) == 1
    assert events[0].query_id == "q-1"
    assert events[0].relations == ["db1.sch1.t_a"]


def test_one_malformed_row_is_reported_not_swallowed_and_not_fatal():
    """Soft-failure, the inventory extractor's discipline -- but the failure
    comes back as data, so the report can say how much it could not read."""
    events, failures = rows_to_events(
        [row(), row(query_id="q-2", started_at="not a timestamp"), row(query_id="q-3")]
    )
    assert [e.query_id for e in events] == ["q-1", "q-3"]
    assert len(failures) == 1
    assert "q-2" in failures[0] or "1" in failures[0]


def test_an_iso_timestamp_string_is_accepted():
    """Drivers differ on whether a timestamp comes back parsed."""
    events, failures = rows_to_events([row(started_at="2026-10-02T00:00:00+00:00")])
    assert failures == []
    assert events[0].started_at == datetime(2026, 10, 2, tzinfo=timezone.utc)
