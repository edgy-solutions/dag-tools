"""The `normalize` transform and the normalizer document (PR B2c).

Every transform has two routes, so the SQL route is EXECUTED on duckdb here and
compared with the row route; the duckdb spellings of the regex templates live
in this file and nowhere in the library.
"""
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pytest
import yaml

from dag_tools.contract_mapping import (
    MappingPlanError,
    build_mapping_plan,
    load_contract,
    load_mapping,
)
from dag_tools.contract_mapping.emit import apply_column, render_model_sql
from dag_tools.contract_mapping.normalize import (
    check_re2,
    load_normalizers,
    load_normalizers_text,
)

TEMPLATES = {
    "regex_replace": "regexp_replace({expr}, {pattern}, {replacement}, 'g')",
    "regex_extract": "regexp_extract({expr}, {pattern}, {group})",
}
REVISION = r"^(?P<base>[A-Za-z0-9]+?)(?:-(?P<rev>[A-Z]))?$"


def _contract() -> Dict[str, Any]:
    return {
        "contract_id": "c_one",
        "version": "1.0.0",
        "dataset": {"urn": "urn:li:dataset:(urn:li:dataPlatform:s3,d_one,PROD)"},
        "grain": ["k_one"],
        "cadence": {"write": "whole_table", "set_by": "producer"},
        "columns": [
            {"name": "k_one", "type": "string", "required": True, "produced_since": "1"},
            {"name": "v_one", "type": "string", "required": False, "produced_since": "1"},
        ],
        "rules": {"duplicate_key": "two rows share the stated key"},
    }


def _mapping(templates: Optional[Dict[str, str]] = TEMPLATES, table: Optional[str] = "t_raw", **column: Any):
    document: Dict[str, Any] = {
        "contract": "c_one",
        "source": {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:s3,d_raw,PROD)",
            "columns": {"A": "string", "B": "string"},
        },
        "cadence": {"write": "whole_table", "set_by": "producer"},
        "columns": {
            "k_one": {"from": "A"},
            "v_one": {"from": "B", "transform": "normalize", **column},
        },
    }
    if table:
        document["source"]["table"] = table
    if templates is not None:
        document["regex_templates"] = templates
    return document


def _plan(ops: List[Dict[str, Any]], rules: str = "n", templates=TEMPLATES, table="t_raw", **column):
    return build_mapping_plan(
        load_contract(_contract()),
        load_mapping(_mapping(templates, table, rules=rules, **column)),
        normalizers=load_normalizers({"normalizers": {rules: ops}}),
    )


def _col(plan):
    return next(c for c in plan.columns if c.name == "v_one")


def _both(plan, values: List[Optional[str]]):
    column = _col(plan)
    rows = [{"A": f"k{i}", "B": v} for i, v in enumerate(values)]
    row_answers = [apply_column(column, row) for row in rows]
    connection = duckdb.connect()
    connection.execute("create table t_raw (A varchar, B varchar)")
    for row in rows:
        connection.execute("insert into t_raw values (?, ?)", [row["A"], row["B"]])
    sql = render_model_sql(plan)
    sql_answers = [r[1] for r in connection.execute(f"select * from ({sql}) order by k_one").fetchall()]
    return row_answers, sql_answers, sql


def _agree(ops, values, **kw):
    row, sql, text = _both(_plan(ops, **kw), values)
    assert sql == row
    return row, text


# --- ops and the document ----------------------------------------------------


def test_ops_run_left_to_right_each_on_the_previous_result():
    first, _ = _agree([{"op": "upper"}, {"op": "replace", "pattern": "a", "replacement": "_"}], ["a b"])
    second, _ = _agree([{"op": "replace", "pattern": "a", "replacement": "_"}, {"op": "upper"}], ["a b"])
    assert first == ["A B"] and second == ["_ B"]


@pytest.mark.parametrize(
    "op, value, expected",
    [
        ({"op": "trim"}, "  x  ", "x"),
        ({"op": "upper"}, "aB", "AB"),
        ({"op": "lower"}, "aB", "ab"),
        ({"op": "strip", "pattern": "[-_ ]"}, "a-b_c d", "abcd"),
        ({"op": "replace", "pattern": "-+", "replacement": "_"}, "a--b-c", "a_b_c"),
        ({"op": "lstrip_zeros"}, "0010", "10"),
        ({"op": "split_revision", "pattern": REVISION}, "AB1-C", "AB1"),
    ],
)
def test_every_op_agrees_across_the_two_routes(op, value, expected):
    extra = {"part": "base"} if op["op"] == "split_revision" else {}
    row, _ = _agree([op], [value, None], **extra)
    assert row == [expected, None]


def test_a_null_passes_through_every_op():
    ops = [{"op": "trim"}, {"op": "upper"}, {"op": "lstrip_zeros"}]
    row, _ = _agree(ops, [None, " 007 "])
    assert row == [None, "7"]


def test_lstrip_zeros_of_all_zeros_is_empty_not_zero():
    row, _ = _agree([{"op": "lstrip_zeros"}], ["000"])
    assert row == [""]


def test_lstrip_zeros_is_a_regex_replace_and_never_an_ltrim():
    _, text = _agree([{"op": "lstrip_zeros"}], ["0010"])
    assert "regexp_replace" in text and "ltrim" not in text.lower()


def test_an_unknown_op_is_refused_listing_the_ops():
    with pytest.raises(ValueError, match="unknown normalizer op.*trim"):
        load_normalizers({"normalizers": {"n": [{"op": "reverse"}]}})


def test_an_empty_normalizer_is_refused():
    with pytest.raises(ValueError, match="no ops"):
        load_normalizers({"normalizers": {"n": []}})


def test_a_duplicate_normalizer_id_is_refused_at_the_text():
    text = "normalizers:\n  n:\n    - op: trim\n  n:\n    - op: upper\n"
    with pytest.raises(ValueError, match="duplicate key"):
        load_normalizers_text(text)


# --- reference and reuse -----------------------------------------------------


def test_two_mappings_sharing_an_id_produce_identical_values():
    ops = [{"op": "trim"}, {"op": "upper"}, {"op": "lstrip_zeros"}]
    values = [" 0ab ", "x", None]
    one, _ = _agree(ops, values)
    two, _ = _agree(ops, values)
    assert one == two == ["AB", "X", None]


def test_an_unknown_id_fails_listing_the_declared_ones():
    with pytest.raises(MappingPlanError) as error:
        build_mapping_plan(
            load_contract(_contract()),
            load_mapping(_mapping(rules="missing")),
            normalizers=load_normalizers({"normalizers": {"n": [{"op": "trim"}]}}),
        )
    assert "missing" in str(error.value) and "declares n" in str(error.value)


def test_the_hash_moves_on_a_referenced_edit_and_not_on_an_unreferenced_one():
    def hashed(referenced, other):
        return build_mapping_plan(
            load_contract(_contract()),
            load_mapping(_mapping(rules="n")),
            normalizers=load_normalizers(
                {"normalizers": {"n": [{"op": referenced}], "other": [{"op": other}]}}
            ),
        ).plan_hash

    base = hashed("trim", "trim")
    assert hashed("upper", "trim") != base
    assert hashed("trim", "upper") == base


def test_an_absent_or_empty_normalizer_does_not_move_the_hash_and_the_example_stays():
    mapping = _mapping()
    mapping["columns"]["v_one"] = {"from": "B"}
    del mapping["regex_templates"]
    absent = build_mapping_plan(load_contract(_contract()), load_mapping(mapping))
    present = build_mapping_plan(
        load_contract(_contract()),
        load_mapping(mapping),
        normalizers=load_normalizers({"normalizers": {"n": [{"op": "trim"}]}}),
    )
    assert absent.plan_hash == present.plan_hash
    example = Path(__file__).resolve().parents[1] / "examples" / "contract_mapping"
    contract = yaml.safe_load((example / "contracts" / "ev_fixture.yaml").read_text(encoding="utf-8"))
    shipped = yaml.safe_load(
        (example / "dagster_home" / "components" / "contract_mapping" / "mapping.yaml").read_text(
            encoding="utf-8"
        )
    )
    plan = build_mapping_plan(load_contract(contract), load_mapping(shipped))
    assert plan.plan_hash == "5ad849ac21f5"
    assert plan.notes == []


# --- templates ---------------------------------------------------------------


def test_a_regex_op_without_a_template_fails_with_a_table_and_notes_without():
    with pytest.raises(MappingPlanError, match="regex_templates.regex_replace"):
        _plan([{"op": "lstrip_zeros"}], templates=None)
    plan = _plan([{"op": "lstrip_zeros"}], templates=None, table=None)
    assert any("regex_templates.regex_replace" in n for n in plan.notes)


def test_trim_upper_lower_need_no_templates():
    row, _ = _agree([{"op": "trim"}, {"op": "upper"}, {"op": "lower"}], [" Ab "], templates=None)
    assert row == ["ab"]


def test_a_pattern_carrying_a_quote_survives_the_render():
    row, text = _agree([{"op": "strip", "pattern": "'"}], ["it's"])
    assert row == ["its"] and "''" in text


def test_a_template_with_a_wrong_placeholder_is_refused():
    with pytest.raises(ValueError, match="regex template"):
        load_mapping(_mapping(templates={"regex_replace": "r({expr}, {pattern})"}))


# --- split_revision ----------------------------------------------------------


def test_part_selects_the_positional_group_on_both_parts_and_routes():
    op = [{"op": "split_revision", "pattern": REVISION}]
    base, _ = _agree(op, ["AB1-C"], part="base")
    rev, _ = _agree(op, ["AB1-C"], part="rev")
    assert base == ["AB1"] and rev == ["C"]


def test_a_group_that_did_not_participate_is_null_on_both_routes():
    op = [{"op": "split_revision", "pattern": REVISION}]
    row, text = _agree(op, ["AB1"], part="rev")
    assert row == [None] and "nullif(" in text


def test_an_undeclared_part_fails_listing_the_groups():
    with pytest.raises(MappingPlanError, match="base"):
        _plan([{"op": "split_revision", "pattern": REVISION}], part="nope")


def test_split_revision_without_a_part_fails():
    with pytest.raises(MappingPlanError, match="no `part`"):
        _plan([{"op": "split_revision", "pattern": REVISION}])


def test_a_part_without_split_revision_fails():
    with pytest.raises(MappingPlanError, match="no split_revision"):
        _plan([{"op": "trim"}], part="base")


def test_split_revision_must_be_the_last_op():
    with pytest.raises(ValueError, match="must be the last"):
        load_normalizers(
            {"normalizers": {"n": [{"op": "split_revision", "pattern": REVISION}, {"op": "upper"}]}}
        )


# --- RE2 ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "pattern, word",
    [
        ("(?<=a)b", "lookbehind"),
        ("a(?=b)", "lookahead"),
        (r"(a)\1", "backreference"),
        ("(a)?(?(1)b|c)", "conditional"),
        ("(?>a+)b", "atomic"),
        ("a++", "possessive"),
    ],
)
def test_a_construct_re2_lacks_is_refused(pattern, word):
    try:
        re.compile(pattern)
    except re.error:
        pytest.skip("this Python cannot compile it, which is refused first")
    with pytest.raises(ValueError, match=word):
        check_re2(pattern)


def test_a_pattern_python_cannot_compile_is_refused_first():
    with pytest.raises(ValueError, match="does not compile"):
        check_re2("(unclosed")


def test_an_escaped_construct_is_refused_too_by_design():
    """Textual and conservative: a false refusal is a message, a false accept a
    broken model. This pins the stated cost."""
    with pytest.raises(ValueError, match="lookahead"):
        check_re2(r"\(?=")


# --- invertibility -----------------------------------------------------------


def test_idempotent_ops_are_invertible_and_the_plan_says_nothing():
    plan = _plan([{"op": "trim"}, {"op": "upper"}])
    assert _col(plan).invertible and not any("invertible" in n for n in plan.notes)


def test_split_revision_is_not_invertible_and_the_plan_names_the_op():
    plan = _plan([{"op": "split_revision", "pattern": REVISION}], part="base")
    assert not _col(plan).invertible and _col(plan).uninvertible_op == "split_revision"
    assert any("not invertible" in n and "split_revision" in n for n in plan.notes)


def test_replace_is_invertible_only_when_the_replacement_cannot_match_the_pattern():
    safe = _plan([{"op": "replace", "pattern": "a", "replacement": "b"}])
    unsafe = _plan([{"op": "replace", "pattern": "a", "replacement": "aa"}])
    assert _col(safe).invertible and not _col(unsafe).invertible


def test_the_proof_inversion_is_identity_when_invertible_and_refuses_otherwise():
    from dag_tools_tests.test_contract_mapping_proof import _invert

    good = _plan([{"op": "trim"}])
    assert _invert(good, [{"k_one": "k", "v_one": "x"}]) == [{"A": "k", "B": "x"}]
    bad = _plan([{"op": "split_revision", "pattern": REVISION}], part="base")
    with pytest.raises(AssertionError, match="split_revision"):
        _invert(bad, [{"k_one": "k", "v_one": "x"}])


# --- purity ------------------------------------------------------------------


def test_the_engine_imports_none_of_the_orchestration_stack():
    code = (
        "import sys, dag_tools.contract_mapping, dag_tools.contract_mapping.normalize;"
        "bad=[m for m in ('dagster','dbt','dlt','sqlalchemy','jinja2','datahub') if m in sys.modules];"
        "print(','.join(bad)); sys.exit(1 if bad else 0)"
    )
    result = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout


# --- component wiring --------------------------------------------------------


def _component_files(tmp_path, with_block: bool):
    from dag_tools.components.contract_mapping.component import build_contract_mapping_asset
    from dag_tools.components.contract_mapping.schema import ContractMappingSpec

    (tmp_path / "c.yaml").write_text(yaml.safe_dump(_contract()), encoding="utf-8")
    (tmp_path / "m.yaml").write_text(yaml.safe_dump(_mapping(rules="n")), encoding="utf-8")
    (tmp_path / "n.yaml").write_text(
        yaml.safe_dump({"normalizers": {"n": [{"op": "trim"}]}}), encoding="utf-8"
    )
    spec = ContractMappingSpec.from_attributes(
        asset_name="a_one",
        group_name=None,
        io_manager_key=None,
        owners=None,
        tags=None,
        mapping_file="m.yaml",
        contract={"path": "c.yaml"},
        source_schema={},
        emit={"dry_run": True},
        normalizers={"path": "n.yaml"} if with_block else None,
    )
    return build_contract_mapping_asset, spec


def test_a_mapping_using_normalize_with_no_normalizers_fails_the_load_naming_both(tmp_path):
    build, spec = _component_files(tmp_path, with_block=False)
    with pytest.raises(ValueError) as excinfo:
        build(spec, str(tmp_path))
    assert "normalize" in str(excinfo.value) and "normalizers" in str(excinfo.value)


def test_a_path_based_normalizer_document_loads_and_builds(tmp_path):
    build, spec = _component_files(tmp_path, with_block=True)
    assert build(spec, str(tmp_path)) is not None
