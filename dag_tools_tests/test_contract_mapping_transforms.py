"""Transforms whose two routes must agree: `nullif`, `in_set`, and coalesce's arity.

A transform is a row rendering and a SQL rendering of one declaration, and a
claim asserted on one proves nothing about the other. So every semantic claim
here runs the row route (`apply_column`) AND executes the rendered model SQL on
duckdb, then compares. Reading the SQL string would pass on SQL that is wrong.

The failures these prevent are all silent:

  * Python says `0 == False` and a warehouse does not, so a boolean literal
    would match on one route and not the other;
  * SQL `x in (null)` is never true and Python's `x in [None]` is, so a null
    in a list would split the routes on the one row that matters;
  * a bare SQL `in` is NULL for a null column, which a non-nullable contract
    would refuse with a message about nullability, not about the status;
  * a warehouse coerces a text sentinel against an integer column and the row
    route compares the written form, so the two disagree on the sentinel rows;
  * a `coalesce` fallback of a type the contract's column cannot carry is
    refused by the row route and emitted as a bare literal on the SQL route,
    where the warehouse refuses it or quietly coerces it.

The coalesce-arity section is the other shape of the same hazard: both routes
rendered one source plus a `default` and agreed on it, while the validator
forbade reaching it at all -- so the feature read as absent and got documented
that way. Nobody writes two matching route bodies for a shape they meant to
refuse.

Every name in these fixtures is synthetic, apart from the one test that reads
the shipped example to pin its plan hash.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pytest
import yaml

from dag_tools.contract_mapping import (
    apply_to_rows,
    MappingPlanError,
    NormalizerOp,
    build_mapping_plan,
    load_contract,
    load_mapping,
)
from dag_tools.contract_mapping.emit import (
    _literal_equal,
    _sql_transform,
    apply_column,
    render_model_sql,
)
from dag_tools.contract_mapping.mapping import TRANSFORMS, ColumnMapping
from dag_tools.contract_mapping.plan import PlannedColumn


def _contract(column_type: str) -> Dict[str, Any]:
    return {
        "contract_id": "c_one",
        "version": "1.0.0",
        "dataset": {"urn": "urn:li:dataset:(urn:li:dataPlatform:s3,d_one,PROD)"},
        "grain": ["k_one"],
        "cadence": {"write": "whole_table", "set_by": "producer"},
        "columns": [
            {"name": "k_one", "type": "string", "required": True, "produced_since": "1"},
            {"name": "v_one", "type": column_type, "required": False, "produced_since": "1"},
        ],
        "rules": {"duplicate_key": "two rows share the stated key"},
    }


def _mapping(
    source_type: Optional[str] = "string", extra: Optional[Dict[str, Any]] = None, **column: Any
) -> Dict[str, Any]:
    columns = {"A": "string"}
    if source_type is not None:
        columns["B"] = source_type
    document: Dict[str, Any] = {
        "contract": "c_one",
        "source": {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:s3,d_raw,PROD)",
            "table": "t_raw",
            "columns": columns if source_type is not None else {},
        },
        "cadence": {"write": "whole_table", "set_by": "producer"},
        "columns": {"k_one": {"from": "A"}, "v_one": {"from": "B", **column}},
    }
    document.update(extra or {})
    return document


def _plan(column_type: str, source_type: Optional[str] = "string", **column: Any):
    return build_mapping_plan(
        load_contract(_contract(column_type)),
        load_mapping(_mapping(source_type, **column)),
    )


def _both_routes(plan, source_type: str, values: List[Any]):
    """Run one `v_one` column over `values` on the row route and on duckdb."""
    column = next(c for c in plan.columns if c.name == "v_one")
    rows = [{"A": f"k{i}", "B": v} for i, v in enumerate(values)]
    row_answers = [apply_column(column, row) for row in rows]

    connection = duckdb.connect()
    connection.execute(f"create table t_raw (A varchar, B {source_type})")
    for row in rows:
        connection.execute("insert into t_raw values (?, ?)", [row["A"], row["B"]])
    sql = render_model_sql(plan)
    sql_answers = [r[1] for r in connection.execute(f"select * from ({sql}) order by k_one").fetchall()]
    return row_answers, sql_answers, sql


# --- nullif ------------------------------------------------------------------


def test_nullif_turns_only_the_sentinel_into_null():
    """Both routes null the sentinel and nothing near it.

    A single space is the near miss against an empty-string sentinel: a route
    that trimmed, or a warehouse that padded, would swallow it.
    """
    plan = _plan("string", transform="nullif", value="")
    row, sql, _ = _both_routes(plan, "varchar", ["", " ", "x", None])
    assert row == [None, " ", "x", None]
    assert sql == row


@pytest.mark.parametrize(
    "sentinel, kind",
    [(None, "null"), (True, "boolean"), (1.5, "float")],
)
def test_nullif_refuses_an_unsafe_sentinel(sentinel, kind):
    """A null is a no-op in SQL, a boolean equals 0 in Python only, a float is
    an equality on floats -- each would read as working and not agree."""
    with pytest.raises(ValueError):
        ColumnMapping(**{"from": "B", "transform": "nullif", "value": sentinel})


def test_nullif_refuses_a_null_sentinel():
    with pytest.raises(ValueError, match="requires `value`"):
        ColumnMapping(**{"from": "B", "transform": "nullif"})


def test_nullif_refuses_a_boolean_sentinel():
    with pytest.raises(ValueError, match="boolean"):
        ColumnMapping(**{"from": "B", "transform": "nullif", "value": False})


def test_nullif_refuses_a_float_sentinel():
    with pytest.raises(ValueError, match="float"):
        ColumnMapping(**{"from": "B", "transform": "nullif", "value": 0.5})


def test_a_sentinel_carrying_a_quote_survives_the_render():
    """Quote-doubling is asserted by executing the SQL: an undoubled quote is
    a syntax error or, worse, a different literal."""
    sentinel = "it's n/a"
    plan = _plan("string", transform="nullif", value=sentinel)
    row, sql, _ = _both_routes(plan, "varchar", [sentinel, "other"])
    assert row == [None, "other"]
    assert sql == row


def test_a_sentinel_of_the_wrong_type_for_the_source_fails_the_load():
    """The warehouse casts a text literal against an integer column and matches;
    the row route compares `0 == "0"` and does not. Refused at the load, naming
    the column, the sentinel and both types."""
    with pytest.raises(MappingPlanError) as excinfo:
        _plan("integer", source_type="integer", transform="nullif", value="0")
    message = str(excinfo.value)
    assert "v_one" in message and "'0'" in message
    assert "integer" in message and "str" in message
    assert "disagree" in message

    with pytest.raises(MappingPlanError):
        _plan("string", source_type="string", transform="nullif", value=0)


def test_the_mismatch_the_load_refuses_is_a_real_divergence():
    """Guards the refusal against being hypothetical: build the refused plan
    by hand and show the two routes really do answer differently."""
    column = PlannedColumn(
        name="v_one", contract_type="integer", transform="nullif",
        sources=["B"], value="0",
    )
    row = apply_column(column, {"B": 0})
    connection = duckdb.connect()
    connection.execute("create table t (B integer)")
    connection.execute("insert into t values (0)")
    sql = connection.execute(f"select {_sql_transform(column)} from t").fetchone()[0]
    assert row == 0  # not matched: 0 != "0"
    assert sql is None  # matched: the warehouse cast the literal


def test_a_matching_text_sentinel_agrees_across_routes():
    """The accepted case, executed: text sentinel over a text column."""
    plan = _plan("string", source_type="string", transform="nullif", value="0")
    row, sql, _ = _both_routes(plan, "varchar", ["0", "00", "1"])
    assert row == [None, "00", "1"]
    assert sql == row


def test_an_integer_sentinel_over_an_integer_source_agrees_across_routes():
    plan = _plan("integer", source_type="integer", transform="nullif", value=0)
    row, sql, _ = _both_routes(plan, "integer", [0, 7, None])
    assert row == [None, 7, None]
    assert sql == row


def test_a_warehouse_spelling_leaves_the_sentinel_unchecked_rather_than_classified():
    """The vocabulary read here is this library's own, never a warehouse's.

    `varchar` is a text type in every dialect that has one, so a table of
    dialect spellings would classify it -- and that table is precisely what
    this library does not own, the same refusal that keeps `warehouse_types`
    and `cast_templates` the operator's. So the sentinel goes unchecked and the
    plan says which words it does read. Returning at all is the assertion that
    it was not refused; a refusal raises.

    Without this test a later edit re-adds the dialect list and nothing fails.
    """
    for sentinel in ("x", 0):
        plan = _plan("string", source_type="varchar", transform="nullif", value=sentinel)
        assert any("not one of the type words" in n for n in plan.notes)

    # The declared spellings this library does read, and the note it does not
    # emit for them -- so the test above cannot pass by the check being dead.
    for declared in ("string", "integer"):
        plan = _plan(declared, source_type=declared, transform="nullif",
                     value="x" if declared == "string" else 1)
        assert not any("not one of the type words" in n for n in plan.notes)


def test_an_absent_source_schema_notes_the_sentinel_rather_than_refusing_it():
    """No declared schema is the one case where there is nothing to check
    against, and the stance every other check against that block takes is a
    note. Refusing instead would make `source: columns:` mandatory."""
    plan = _plan("string", source_type=None, transform="nullif", value="x")
    assert any("sentinel" in n for n in plan.notes)


def test_nullif_matches_the_sources_written_form_not_the_coerced_one():
    """The comparison runs before coercion. Into a numeric column, the sentinel
    "0" nulls the written "0" but leaves "0.0" alone, though both coerce to a
    number equal to zero. The SQL route compares text with text, so it agrees;
    matching after coercion would have nulled "0.0" on the row route alone."""
    plan = _plan("numeric", source_type="string", transform="nullif", value="0")
    row, sql, _ = _both_routes(plan, "varchar", ["0", "0.0", "5"])
    assert row[0] is None
    assert row[1] == 0.0 and row[1] is not None
    assert sql[0] is None and sql[1] == "0.0"
    assert [r is None for r in row] == [s is None for s in sql]


def test_nullif_is_not_type_changing():
    """Same type or null, so it needs no cast and must not be noted as needing
    one."""
    plan = build_mapping_plan(
        load_contract(_contract("string")),
        load_mapping(
            _mapping(
                transform="nullif",
                value="x",
                extra={"warehouse_types": {"string": "varchar"}},
            )
        ),
    )
    assert not any("does not carry the column's type" in n for n in plan.notes)


# --- in_set ------------------------------------------------------------------


def test_in_set_answers_false_for_a_null_rather_than_null():
    """The load-bearing one. A bare `in` is NULL for a null column; a
    non-nullable boolean would then be refused for its nullability instead of
    for the status nobody mapped."""
    plan = _plan("boolean", transform="in_set", values=["PASSED", "FAILED"])
    row, sql, text = _both_routes(plan, "varchar", ["PASSED", "RUNNING", None])
    assert "coalesce(B in ('PASSED', 'FAILED'), false)" in text
    assert row == [True, False, False]
    assert sql == [True, False, False]
    assert sql[2] is False


def test_in_set_matches_exactly_and_does_not_fold_case():
    """A source whose casing varies has no enum to match; folding or trimming
    here would hide that."""
    plan = _plan("boolean", transform="in_set", values=["PASSED"])
    row, sql, _ = _both_routes(plan, "varchar", ["PASSED", "passed", " PASSED", "PASSED "])
    assert row == [True, False, False, False]
    assert sql == row


def test_in_set_over_integers_agrees_across_routes():
    plan = _plan("boolean", source_type="integer", transform="in_set", values=[1, 3, 3])
    row, sql, text = _both_routes(plan, "integer", [1, 2, 3, None])
    assert "(1, 3, 3)" in text  # duplicates render as written
    assert row == [True, False, True, False]
    assert sql == row


def test_in_set_refuses_a_null_in_the_list_because_the_routes_would_disagree():
    with pytest.raises(ValueError, match="null"):
        ColumnMapping(**{"from": "B", "transform": "in_set", "values": ["a", None]})


def test_in_set_refuses_a_boolean_in_the_list():
    with pytest.raises(ValueError, match="boolean"):
        ColumnMapping(**{"from": "B", "transform": "in_set", "values": [True]})


def test_in_set_refuses_a_float_in_the_list():
    with pytest.raises(ValueError, match="float"):
        ColumnMapping(**{"from": "B", "transform": "in_set", "values": [1.5]})


def test_in_set_refuses_a_mixed_type_list():
    with pytest.raises(ValueError, match="all text or all integer"):
        ColumnMapping(**{"from": "B", "transform": "in_set", "values": ["a", 1]})


def test_in_set_refuses_an_empty_list():
    with pytest.raises(ValueError, match="non-empty"):
        ColumnMapping(**{"from": "B", "transform": "in_set", "values": []})
    with pytest.raises(ValueError, match="requires"):
        ColumnMapping(**{"from": "B", "transform": "in_set"})


def test_in_set_onto_a_non_boolean_column_fails_the_load():
    """A membership test produces a boolean; aimed at a string column,
    coercion would paper over it by stringifying True."""
    with pytest.raises(MappingPlanError) as excinfo:
        _plan("string", transform="in_set", values=["a"])
    assert "v_one" in str(excinfo.value) and "boolean" in str(excinfo.value)


def test_in_set_is_type_changing_so_an_uncast_mapping_is_noted():
    """Under an enforced contract the boolean needs a cast; without the note
    the cast rule is silently absent."""
    def planned(cast: bool):
        extra: Dict[str, Any] = {
            "warehouse_types": {
                "string": "varchar",
                "boolean": "boolean",
            }
        }
        if cast:
            extra["cast_templates"] = {"boolean": "cast({expr} as boolean)"}
        return build_mapping_plan(
            load_contract(_contract("boolean")),
            load_mapping(_mapping(transform="in_set", values=["a"], extra=extra)),
        )

    uncast = planned(False)
    assert any("v_one" in n and "does not carry" in n for n in uncast.notes)
    assert not any("v_one" in n and "does not carry" in n for n in planned(True).notes)


# --- shared ------------------------------------------------------------------


def test_a_boolean_literal_never_equals_a_zero_or_a_one():
    """The helper directly, both directions, then through each transform: a
    later edit that inlines `==` brings `0 == False` back."""
    assert _literal_equal(0, False) is False
    assert _literal_equal(False, 0) is False
    assert _literal_equal(1, True) is False
    assert _literal_equal(True, 1) is False
    assert _literal_equal(True, True) is True
    assert _literal_equal(0, 0) is True
    assert _literal_equal("a", "a") is True
    assert _literal_equal("0", 0) is False

    # Through each transform, with a boolean source value a literal of 0/1
    # must not match.
    nullif = PlannedColumn(
        name="v_one", contract_type="string", transform="nullif", sources=["B"], value=0
    )
    assert apply_column(nullif, {"B": False}) == "False"  # not nulled
    in_set = PlannedColumn(
        name="v_one", contract_type="boolean", transform="in_set",
        sources=["B"], values=[1],
    )
    assert apply_column(in_set, {"B": True}) is False


_RUN_INSTANT = datetime(1999, 1, 1, tzinfo=timezone.utc)
_RUN_SQL = "cast('1999-01-01 00:00:00+00' as timestamptz)"


@pytest.mark.parametrize("name", sorted(TRANSFORMS))
def test_every_transform_the_table_declares_renders_on_both_routes(name: str):
    """The anti-vacuity test for the whole table: a transform added with no SQL
    body or no row body fails here, not at a first materialization. It reads
    TRANSFORMS and skips nothing."""
    arguments: Dict[str, Dict[str, Any]] = {
        "copy": {"from": "B"},
        "constant": {"value": "x"},
        "trim": {"from": "B"},
        "upper": {"from": "B"},
        "lower": {"from": "B"},
        "concat": {"from": ["B", "B"]},
        "coalesce": {"from": ["B", "B"]},
        "scale": {"from": "B", "factor": "2"},
        "nullif": {"from": "B", "value": "x"},
        "in_set": {"from": "B", "values": ["x"]},
        "normalize": {"from": "B", "rules": "n_one"},
        "run_timestamp": {},
    }
    assert name in arguments, f"no case for transform {name!r}: add one"
    entry = ColumnMapping(transform=name, **arguments[name])
    # `normalize`'s ops are resolved from the normalizer document, which this
    # test deliberately does not build: one `upper` needs no dialect template,
    # so the case stays about whether each route has a body at all.
    ops = [NormalizerOp(op="upper")] if name == "normalize" else []
    column = PlannedColumn(
        name="v_one",
        contract_type=(
            "boolean"
            if name == "in_set"
            else "timestamp"
            if name == "run_timestamp"
            else "string"
        ),
        run_timestamp_sql=_RUN_SQL if name == "run_timestamp" else None,
        transform=name,
        sources=list(entry.sources),
        value=entry.value,
        separator=entry.separator,
        factor=entry.factor,
        default=entry.default,
        values=entry.values,
        rules=entry.rules,
        part=entry.part,
        ops=ops,
    )
    rendered = _sql_transform(column)
    assert rendered
    if name == "scale":
        return  # a numeric source; the row route's own tests cover its values
    row_answer = apply_column(column, {"B": "x"}, _RUN_INSTANT)

    connection = duckdb.connect()
    connection.execute("create table t (B varchar)")
    connection.execute("insert into t values ('x')")
    sql_answer = connection.execute(f"select {rendered} from t").fetchone()[0]
    assert sql_answer == row_answer


# --- coalesce arity ----------------------------------------------------------


def test_one_source_and_a_default_is_two_arguments_and_is_accepted():
    """One source plus a constant fallback is a coalesce, and both routes
    already answered it the same way while the validator forbade reaching it.

    Executed on both routes over the null row and the non-null row, because
    the null row is the only one the fallback is written for and a rendering
    that dropped the default would still look right on every other row.
    """
    plan = _plan("string", transform="coalesce", default="")
    row, sql, text = _both_routes(plan, "varchar", ["x", None])
    assert "coalesce(B, '')" in text
    assert row == ["x", ""]
    assert sql == row


def test_a_lone_source_with_no_default_is_still_refused_as_a_no_op():
    """The other half of the relaxation. On its own the test above passes on a
    validator that accepts anything, so this pins what stays refused: a bare
    `coalesce(x)` is a no-op wearing a transform's name, and so is a `default`
    with nothing to fall back from."""
    with pytest.raises(ValueError):
        ColumnMapping(**{"from": "B", "transform": "coalesce"})
    with pytest.raises(ValueError):
        ColumnMapping(**{"from": [], "transform": "coalesce", "default": "x"})


def test_the_refusal_names_the_constant_fallback_rather_than_a_missing_source():
    """The message is the finding, not just the refusal.

    "takes two or more `from`" sends an operator to add a second source they
    do not have -- which is how a feature both routes already implemented came
    to be written up as absent for a release. The message has to name the
    `default` as the second argument.
    """
    with pytest.raises(ValueError) as excinfo:
        ColumnMapping(**{"from": "B", "transform": "coalesce"})
    message = str(excinfo.value)
    assert "`default`" in message and "fallback" in message
    assert "two or more `from`" not in message


def test_concat_is_not_relaxed_by_the_coalesce_change():
    """The symmetrization guard. `concat` and `coalesce` share the `"2+"`
    spelling in the arity table, and a later edit that reads the relaxation as
    being about that spelling would let `concat(x)` through -- a genuine no-op,
    with no fallback to count as a second argument. A `default` buys it
    nothing, because nothing renders it."""
    assert TRANSFORMS["concat"]["sources"] == TRANSFORMS["coalesce"]["sources"]
    with pytest.raises(ValueError, match="two or more `from`"):
        ColumnMapping(**{"from": "B", "transform": "concat", "separator": "-"})
    with pytest.raises(ValueError, match="two or more `from`"):
        ColumnMapping(**{"from": "B", "transform": "concat", "default": "x"})


def test_a_default_the_contract_type_cannot_take_fails_the_load():
    """`default` is `Any`, and the two routes part ways over one the column
    cannot carry: the row route refuses it at materialization, the SQL route
    emits it as a bare literal the warehouse refuses or coerces -- and which
    of those is not this library's to decide. Knowable at the load, so the
    load is where it is refused, naming the column, the default and the type.
    """
    with pytest.raises(MappingPlanError) as excinfo:
        _plan("integer", source_type="integer", transform="coalesce", default="")
    message = str(excinfo.value)
    assert "v_one" in message and "''" in message
    assert "integer" in message
    assert "disagree" in message


def test_a_default_the_contract_type_takes_agrees_on_both_routes():
    """The partner, so the refusal above is known to guard a real divergence
    rather than to be a blanket refusal of defaults over a typed column. The
    fallback row is the one that exercises it, on both routes."""
    plan = _plan("integer", source_type="integer", transform="coalesce", default=0)
    row, sql, text = _both_routes(plan, "integer", [7, None])
    assert "coalesce(B, 0)" in text
    assert row == [7, 0]
    assert sql == row


def test_the_shipped_example_plan_hash_does_not_move():
    """The arity relaxation adds no plan field and changes no rendering, so no
    existing mapping may hash differently. A moved hash means something leaked
    into a shared path -- and the example's emitted SQL carries this hash in a
    comment, so a silent move is a silent rewrite of a shipped artifact."""
    example = Path(__file__).resolve().parents[1] / "examples" / "contract_mapping"
    contract = yaml.safe_load(
        (example / "contracts" / "ev_fixture.yaml").read_text(encoding="utf-8")
    )
    mapping = yaml.safe_load(
        (
            example / "dagster_home" / "components" / "contract_mapping" / "mapping.yaml"
        ).read_text(encoding="utf-8")
    )
    plan = build_mapping_plan(load_contract(contract), load_mapping(mapping))
    assert plan.plan_hash == "5ad849ac21f5"


# --- where: which source rows the mapping reads ----------------------------
#
# `where` is not a column transform: it decides which rows exist for the
# mapping at all. Each semantic claim runs the row route (`apply_to_rows`) and
# executes the model SQL on duckdb, because reading the rendered string would
# pass on SQL that is wrong.

from dag_tools.contract_mapping import apply_to_rows  # noqa: E402
from dag_tools.contract_mapping.mapping import WHERE_OPS  # noqa: E402


def _where_doc(where):
    document = _mapping("string")
    document["source"]["where"] = where
    return document


def _where_plan(where, column_type: str = "string"):
    return build_mapping_plan(
        load_contract(_contract(column_type)), load_mapping(_where_doc(where))
    )


def _filtered_keys(plan, rows: List[Dict[str, Any]]):
    """The `k_one` values each route keeps over `rows`, in key order."""
    row_keys = sorted(r["k_one"] for r in apply_to_rows(plan, rows))
    connection = duckdb.connect()
    connection.execute("create table t_raw (A varchar, B varchar)")
    for row in rows:
        connection.execute("insert into t_raw values (?, ?)", [row["A"], row["B"]])
    sql = render_model_sql(plan)
    sql_keys = sorted(r[0] for r in connection.execute(f"select k_one from ({sql})").fetchall())
    return row_keys, sql_keys, sql


def _rows(*values: Any) -> List[Dict[str, Any]]:
    return [{"A": f"k{i}", "B": v} for i, v in enumerate(values)]


def test_a_mapping_with_no_filters_renders_exactly_what_it_rendered_before():
    """Backwards-compatibility canary: an empty filter list must add no `where`
    line (and not `where true`), or every existing mapping's emitted model
    changes bytes on upgrade."""
    sql = render_model_sql(_plan("string"))
    assert "where" not in sql.lower()
    assert sql.rstrip().endswith("from t_raw")


def test_a_filter_drops_the_row_on_both_routes():
    """A row the predicate excludes must not exist on either route -- and on the
    row route it must not survive as a row of nulls, which a non-nullable
    contract would then refuse for the wrong reason."""
    plan = _where_plan([{"column": "B", "op": "=", "value": "A"}])
    rows = _rows("A", "X", "A")
    kept = apply_to_rows(plan, rows)
    assert len(kept) == 2 and all(r["v_one"] == "A" for r in kept)
    row_keys, sql_keys, _ = _filtered_keys(plan, rows)
    assert row_keys == sql_keys == ["k0", "k2"]


def test_filters_are_anded_in_the_order_written():
    """A filter list that was ORed, or reordered, would admit rows the author
    excluded; the order is also what a reviewer reads in the model."""
    plan = _where_plan(
        [
            {"column": "B", "op": "!=", "value": "X"},
            {"column": "A", "op": "in", "values": ["k0", "k1"]},
        ]
    )
    row_keys, sql_keys, sql = _filtered_keys(plan, _rows("X", "Y", "Z"))
    assert row_keys == sql_keys == ["k1"]
    assert sql.index("B != ") < sql.index("A in (")
    assert "\n  and " in sql


def test_not_equal_excludes_a_null_on_both_routes():
    """The load-bearing one. A bare SQL `!=` is NULL for a null and drops the
    row while Python's `!=` keeps it; the `is not null` guard makes the two
    agree, on the warehouse's reading."""
    plan = _where_plan([{"column": "B", "op": "!=", "value": "A"}])
    row_keys, sql_keys, sql = _filtered_keys(plan, _rows("A", "X", None))
    assert "B is not null" in sql
    assert row_keys == sql_keys == ["k1"]


def test_not_in_excludes_a_null_on_both_routes():
    """The list form of the same guard: a bare `not in` is NULL for a null."""
    plan = _where_plan([{"column": "B", "op": "not_in", "values": ["A", "C"]}])
    row_keys, sql_keys, sql = _filtered_keys(plan, _rows("A", "X", None, "C"))
    assert "B is not null" in sql
    assert row_keys == sql_keys == ["k1"]


@pytest.mark.parametrize(
    "flt, satisfied_by_null",
    [
        ({"op": "=", "value": "A"}, False),
        ({"op": "!=", "value": "A"}, False),
        ({"op": "in", "values": ["A"]}, False),
        ({"op": "not_in", "values": ["A"]}, False),
        ({"op": "is_null"}, True),
        ({"op": "not_null"}, False),
    ],
)
def test_is_null_is_the_only_operator_a_null_satisfies(flt, satisfied_by_null):
    """Over all six operators, on both routes: a null source value is kept only
    by `is_null`. Any other answer means the routes can split on nulls."""
    assert flt["op"] in WHERE_OPS
    plan = _where_plan([{"column": "B", **flt}])
    row_keys, sql_keys, _ = _filtered_keys(plan, _rows(None))
    assert (row_keys == ["k0"]) is satisfied_by_null
    assert row_keys == sql_keys


def test_a_literal_carrying_a_quote_survives_the_render():
    """A value with a quote must be matched, not break the statement or match
    its truncation; asserted by executing."""
    plan = _where_plan([{"column": "B", "op": "=", "value": "o'x"}])
    row_keys, sql_keys, _ = _filtered_keys(plan, _rows("o'x", "o", "x"))
    assert row_keys == sql_keys == ["k0"]
    plan = _where_plan([{"column": "B", "op": "in", "values": ["o'x", "z"]}])
    row_keys, sql_keys, _ = _filtered_keys(plan, _rows("o'x", "o"))
    assert row_keys == sql_keys == ["k0"]


@pytest.mark.parametrize("field", [{"value": "A"}, {"values": ["A"]}, {"value": None}])
@pytest.mark.parametrize("op", ["is_null", "not_null"])
def test_is_null_carrying_a_value_fails_the_load(op, field):
    """A value on a nullness test reads as working and is ignored; refused so
    a knob that says nothing cannot look wired up."""
    with pytest.raises(ValueError, match="neither"):
        load_mapping(_where_doc([{"column": "B", "op": op, **field}]))


def test_a_scalar_operator_given_a_list_fails_the_load():
    """`=` with `values` would be read as the first element or as nothing; the
    converse, a list operator with `value`, as a one-element list or as a text
    scan. Both are a mistake that reads as working."""
    for op in ("=", "!="):
        with pytest.raises(ValueError, match="takes `value`"):
            load_mapping(_where_doc([{"column": "B", "op": op, "values": ["A"]}]))
        with pytest.raises(ValueError, match="takes `value`"):
            load_mapping(_where_doc([{"column": "B", "op": op}]))
    for op in ("in", "not_in"):
        with pytest.raises(ValueError, match="takes `values`"):
            load_mapping(_where_doc([{"column": "B", "op": op, "value": "A"}]))
        with pytest.raises(ValueError, match="empty"):
            load_mapping(_where_doc([{"column": "B", "op": op, "values": []}]))


def test_a_filter_naming_an_undeclared_source_column_fails_the_load():
    """A filter on a column the source lacks would drop every row, or none, at
    the warehouse; with a declared schema it is caught at the load."""
    with pytest.raises(ValueError, match="does not declare"):
        load_mapping(_where_doc([{"column": "ZZ", "op": "not_null"}]))


def test_an_empty_columns_block_notes_rather_than_refuses():
    """With no declared schema the column cannot be checked; same stance as the
    rest of that field: a note, never a refusal."""
    document = _where_doc([{"column": "ZZ", "op": "not_null"}])
    document["source"]["columns"] = {}
    plan = build_mapping_plan(load_contract(_contract("string")), load_mapping(document))
    assert any("`where` filter" in note for note in plan.notes)


def test_an_unknown_operator_fails_the_load_listing_the_six():
    """`like` and range operators are a dialect's collation; the refusal must
    name the op and the operators that exist."""
    with pytest.raises(ValueError) as err:
        load_mapping(_where_doc([{"column": "B", "op": "like", "value": "A%"}]))
    text = str(err.value)
    assert "like" in text and all(op in text for op in WHERE_OPS)


@pytest.mark.parametrize(
    "literals, kind",
    [
        ([None], "null"),
        ([True], "boolean"),
        ([1.5], "float"),
        (["a", 1], "all text or all integer"),
    ],
)
def test_the_literal_rules_are_the_same_as_in_set(literals, kind):
    """`where` and `in_set` share one literal validator; running the refused
    kinds through both stops it drifting apart from either caller."""
    with pytest.raises(ValueError, match=kind):
        ColumnMapping(**{"from": "B", "transform": "in_set", "values": literals})
    with pytest.raises(ValueError, match=kind):
        load_mapping(_where_doc([{"column": "B", "op": "in", "values": literals}]))
    if len(literals) == 1:
        with pytest.raises(ValueError, match=kind):
            load_mapping(_where_doc([{"column": "B", "op": "=", "value": literals[0]}]))


def test_a_filter_changes_the_plan_hash():
    """A hash that did not move would let a filter change ship under a
    reviewed hash; and the empty list must leave it where it was."""
    none = _where_plan([])
    one = _where_plan([{"column": "B", "op": "=", "value": "A"}])
    other = _where_plan([{"column": "B", "op": "=", "value": "C"}])
    assert len({none.plan_hash, one.plan_hash, other.plan_hash}) == 3
    assert none.plan_hash == _plan("string").plan_hash


def test_the_filters_reach_the_plan_so_a_dry_run_shows_them():
    """A dry run shows what the plan holds; a filter that never reached it
    would be applied invisibly."""
    plan = _where_plan(
        [{"column": "B", "op": "=", "value": "A"}, {"column": "A", "op": "not_null"}]
    )
    assert [(f.column, f.op) for f in plan.row_filters] == [("B", "="), ("A", "not_null")]
    assert plan.row_filters[0].value == "A"


@pytest.mark.parametrize(
    "literals, kind",
    [
        ([None], "null"),
        ([True], "boolean"),
        ([1.5], "float"),
        (["a", 1], "all text or all integer"),
    ],
)
def test_the_dlt_row_filters_are_held_to_the_same_literal_rules(literals, kind):
    """The dlt component's `row_filters` and a mapping's `where` are one
    grammar. Binding a value changes neither the float-equality hazard nor the
    text-versus-integer coercion question, so a later edit relaxing one caller
    (as the dlt half once was) must fail here."""
    from dag_tools.asset_wrappers.row_filters import RowFilter as DltRowFilter

    with pytest.raises(ValueError, match=kind):
        load_mapping(_where_doc([{"column": "B", "op": "in", "values": literals}]))
    with pytest.raises(ValueError, match=kind):
        DltRowFilter.model_validate({"column": "B", "op": "in", "values": literals})
    if len(literals) == 1:
        with pytest.raises(ValueError, match=kind):
            DltRowFilter.model_validate({"column": "B", "op": "=", "value": literals[0]})


def test_an_explicit_null_values_is_refused_by_both_callers():
    """`values: null` on a scalar or nullness operator says nothing and must
    not look wired up, on either half."""
    from dag_tools.asset_wrappers.row_filters import RowFilter as DltRowFilter

    for op, extra in (("=", {"value": "A"}), ("is_null", {})):
        flt = {"column": "B", "op": op, "values": None, **extra}
        with pytest.raises(ValueError):
            load_mapping(_where_doc([flt]))
        with pytest.raises(ValueError):
            DltRowFilter.model_validate(flt)


# --- run_timestamp -----------------------------------------------------------
#
# `run_timestamp` is TYPE-CHANGING (it is in `_TYPE_CHANGING_TRANSFORMS` beside
# `constant`): a minted instant is whatever the operator's expression returns,
# so under an enforced contract it needs a cast, same reason a null constant
# does. The proof module is deliberately untouched: it re-renders without an
# instant, so a plan using this transform is refused there until a later PR
# threads one in.

_RUN_DOC = {"run_timestamp_sql": _RUN_SQL}


def _run_plan(extra=_RUN_DOC, column_type: str = "timestamp", **column: Any):
    mapping = _mapping(None, extra=extra)
    mapping["columns"]["v_one"] = {"transform": "run_timestamp", **column}
    return build_mapping_plan(
        load_contract(_contract(column_type)), load_mapping(mapping)
    )


def _run_error(**kwargs) -> str:
    with pytest.raises((MappingPlanError, ValueError)) as error:
        _run_plan(**kwargs)
    return str(error.value)


def test_the_instant_comes_from_the_caller_not_a_clock():
    """Rendered with an instant in 1999: no clock read can answer that."""
    plan = _run_plan()
    rows = apply_to_rows(plan, [{"A": "k1"}], _RUN_INSTANT)
    assert rows[0]["v_one"] == _RUN_INSTANT


def test_every_row_of_one_call_carries_the_same_instant():
    plan = _run_plan()
    rows = apply_to_rows(plan, [{"A": f"k{i}"} for i in range(5)], _RUN_INSTANT)
    assert len({row["v_one"] for row in rows}) == 1
    assert rows[0]["v_one"] == _RUN_INSTANT


def test_a_plan_that_needs_an_instant_and_is_given_none_is_refused():
    plan = _run_plan()
    with pytest.raises(Exception) as error:
        apply_to_rows(plan, [{"A": "k1"}])
    assert "run_timestamp" in str(error.value)


def test_a_naive_instant_is_refused_because_it_names_no_instant():
    plan = _run_plan()
    with pytest.raises(Exception) as error:
        apply_to_rows(plan, [{"A": "k1"}], datetime(1999, 1, 1))
    assert "naive" in str(error.value)


def test_the_sql_route_renders_the_operators_expression_verbatim():
    plan = _run_plan()
    sql = render_model_sql(plan)
    connection = duckdb.connect()
    connection.execute("create table t_raw (A varchar)")
    connection.execute("insert into t_raw values ('k1'), ('k2')")
    answers = [
        r[1]
        for r in connection.execute(f"select * from ({sql}) order by k_one").fetchall()
    ]
    assert len(answers) == 2
    assert all(a == _RUN_INSTANT for a in answers)


def test_a_run_timestamp_column_with_no_sql_expression_fails_the_load():
    assert "run_timestamp_sql" in _run_error(extra={})


def test_run_timestamp_onto_a_non_timestamp_column_fails_the_load():
    message = _run_error(column_type="string")
    assert "timestamp" in message and "v_one" in message


@pytest.mark.parametrize(
    "extra", [{"from": "B"}, {"value": "x"}, {"default": "x"}, {"factor": "2"}]
)
def test_run_timestamp_refuses_a_declared_source_and_every_unused_knob(extra):
    """The message must name the knob the mapping set. Asserting only that
    something raised would pass on a refusal for an unrelated reason, which
    reads to an operator as the knob being rejected on its merits."""
    knob = next(iter(extra))
    message = _run_error(**extra)
    assert knob in message, f"refused, but the message does not name {knob!r}: {message}"


def test_the_plan_hash_moves_when_the_expression_is_set_and_not_when_it_is_absent():
    """Both directions. Moves: a different expression is a different plan.
    Absent: a mapping that carries the mapping-level field but has no
    `run_timestamp` column hashes exactly as one without it."""
    base = _run_plan()
    other = _run_plan(extra={"run_timestamp_sql": "cast('2000-01-01' as timestamptz)"})
    assert base.plan_hash != other.plan_hash

    plain = build_mapping_plan(
        load_contract(_contract("string")), load_mapping(_mapping())
    )
    with_field = build_mapping_plan(
        load_contract(_contract("string")),
        load_mapping(_mapping(extra=_RUN_DOC)),
    )
    assert plain.plan_hash == with_field.plan_hash


def test_the_shipped_example_plan_hash_does_not_move():
    example = Path(__file__).resolve().parents[1] / "examples" / "contract_mapping"
    contract = yaml.safe_load(
        (example / "contracts" / "ev_fixture.yaml").read_text(encoding="utf-8")
    )
    shipped = yaml.safe_load(
        (
            example / "dagster_home" / "components" / "contract_mapping" / "mapping.yaml"
        ).read_text(encoding="utf-8")
    )
    plan = build_mapping_plan(load_contract(contract), load_mapping(shipped))
    assert plan.plan_hash == "5ad849ac21f5"
