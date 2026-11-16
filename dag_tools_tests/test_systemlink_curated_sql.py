"""The curated dbt package: the bytes it ships, and what its SQL answers.

Four properties, and each one is here because the other three can pass while
it is false:

1. **Byte identity.** Every `.sql` file under the shipped package is exactly
   what the generator emits, and the shipped *set* is exactly the set the
   generator emits -- so neither a file edited by hand nor one nobody
   regenerated nor one left behind after a rename passes.

2. **Cross-route agreement.** The same templates, rendered with an engine's
   own extract function instead of the package's macro, are executed on
   duckdb and compared against the Python grammar for the same texts. One
   rule table feeds both routes; this is what says the two readings of it
   agree. Property 1 alone would pass on a generator whose SQL is wrong in
   the same way as its template; property 2 alone would pass on a shipped
   file nobody regenerated.

3. **The contract companion.** Each model's executed column list is exactly
   its contract document's, in the document's order. The generator holds its
   own column lists (library SQL may not read an example file), so these are
   two independent statements that have to agree.

4. **No name from below.** Asserted by construction rather than by a list of
   forbidden words: the input tables are built from the conformed contract
   documents and from nothing else, so a model that read any other column
   fails to execute. A test guards that the fixture adds no column of its
   own, because a helper column in the fixture is how that proof is lost.

Neither duckdb nor jinja2 is optional here -- both are core dependencies of
this library -- so nothing in this module skips.
"""
from __future__ import annotations

import datetime as dt
import subprocess
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import duckdb
import jinja2
import pytest
import yaml

from dag_tools.contract_mapping import load_contract
from dag_tools.systemlink import measurements as grammar
from dag_tools.systemlink import sql

CONTRACTS = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "mfg_trace"
    / "contracts"
    / "mfg_trace.yaml"
)

#: The example deployment's own dbt project: where the four required
#: vars are set, and where the sample comparator table lives. The
#: architect's ruling puts a sample table in the example; a sample
#: nothing exercises is how a documented table turns out not to work.
EXAMPLE_PROJECT = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "mfg_trace"
    / "curated"
    / "dbt_project.yml"
)

# The two conformed assets of the example template. The package names neither:
# it reads them through dbt vars, which is what keeps every name from below in
# the deployment. These are the *example's* own assets, and this module reads
# the example.
CONF_RESULT_ASSET = "conf_test_repo_result"
CONF_STEP_ASSET = "conf_test_repo_step_parameter"

#: The models, in build order. Their names are the contract assets they
#: produce and the file names they are generated into.
MODELS: Tuple[str, ...] = ("test_result", "test_measurement", "test_catalog")

#: The files of the package that are written by hand rather than generated.
#: Named here so the file-set comparison below stays an equality: a set
#: comparison that simply skipped whatever the generator does not emit would
#: pass on any file at all left behind under this directory.
HAND_WRITTEN: Tuple[str, ...] = ("macros/mfg_trace_re_extract.sql",)

#: The relation names the fixture gives the conformed inputs. Deliberately not
#: the conformed assets' own names: the models reach them only through the
#: required vars, and a fixture that happened to name a table the same thing
#: would hide a model that ignored the var and hardcoded the name.
RESULT_RELATION = "input_one"
STEP_RELATION = "input_two"

#: The exact numeric type the tolerance arithmetic is done in. The package
#: declares no default for it; this is the test's choice, as it is a
#: deployment's.
DECIMAL_TYPE = "decimal(38, 9)"

#: The deployment-owned comparator table, as a deployment would declare it.
#: The library ships no codes, so these are this module's. One carries an
#: apostrophe and one is a non-ASCII operator: a code is the deployment's own
#: word, and both have to survive being rendered into SQL.
COMPARATORS: Tuple[grammar.Comparator, ...] = (
    grammar.Comparator(text="<", code="LT", bound="high"),
    grammar.Comparator(text=">", code="GT", bound="low"),
    grammar.Comparator(text="≤", code="LE'Q", bound="high"),
)

#: An operator the table above does not carry. Whole-operator matching is
#: what makes this uncovered: a table with `<` does not cover `<=`.
UNCOVERED_OPERATOR_TEXT = "<= 7"


class Case(NamedTuple):
    """One step-parameter row, and the texts the grammar reads from it."""

    step_id: str
    parameter_name: str
    measurement_text: str
    limits_text: Optional[str] = None
    units: Optional[str] = None
    comparison_type: Optional[str] = None


#: One case per thing the grammar has to get right, each on its own step row
#: so a row can be looked up by its key and compared field by field.
CASES: Tuple[Case, ...] = (
    Case("S01", "A01", "3.3 V", "1.20 ± 0.02"),
    Case("S02", "A02", "1.005", "1.005 ± 0.005"),
    Case("S03", "A03", "12", "< 50"),
    Case("S04", "A04", "0.5", "> 4.2"),
    Case("S05", "A05", "4.2", "≤ 7"),
    Case("S06", "A06", "PASS", "< 50"),
    Case("S07", "A07", "PASS", "FAIL"),
    Case("S08", "A08", "1e999", "< 50"),
    Case("S09", "A09", "-1e999", "> 4.2"),
    Case("S10", "A10", "4.2 mV", "", units="V"),
    Case("S11", "A11", "4.2", "< 50", comparison_type="LTE"),
    Case("S12", "A12", "", ""),
    Case("S13", "A13", "7.0", UNCOVERED_OPERATOR_TEXT),
    Case("S14", "A14", "PASS", UNCOVERED_OPERATOR_TEXT),
    Case("S15", "A15", "-0.5e-3 A", "-1.0 ± 0.5"),
    Case("S16", "A16", "0", "0.0 ± 0.0"),
)

#: The unit under test the attempts below all belong to, and a second unit
#: that never reaches a terminal state.
_UNIT = {"part_number": "P1", "serial_number": "U1", "program_name": "G1"}
_OTHER_UNIT = {"part_number": "P1", "serial_number": "U2", "program_name": "G1"}


def _at(day: int) -> dt.datetime:
    return dt.datetime(2026, 1, day, 12, 0, 0)


#: Three attempts of one unit, the newest of which is still running, and one
#: attempt of a second unit that never reached a terminal state at all. What
#: `is_latest` means is the whole reason for the shape: the newest attempt is
#: not the latest *result*, and a unit with no terminal attempt has none.
RESULT_ROWS: Tuple[Dict[str, Any], ...] = (
    dict(_UNIT, result_id="R1", status_type="done", is_terminal=True, started_at=_at(1)),
    dict(_UNIT, result_id="R2", status_type="done", is_terminal=True, started_at=_at(2)),
    dict(_UNIT, result_id="R3", status_type="running", is_terminal=False, started_at=_at(3)),
    dict(
        _OTHER_UNIT,
        result_id="R4",
        status_type="running",
        is_terminal=False,
        started_at=_at(4),
    ),
)

#: A parameter measured on two attempts, which is what the catalog counts.
REPEATED_PARAMETER = "A99"

#: An attempt id no result row carries. The measurement model's inner join is
#: what drops it, and that is a property worth pinning: a later edit to an
#: outer join would publish a measurement attributable to nothing.
ABSENT_RESULT_ID = "R9"

STEP_PATH = "root/leaf"


def _step_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for case in CASES:
        rows.append(
            {
                "result_id": "R1",
                "step_id": case.step_id,
                "parameter_index": 0,
                "step_path": STEP_PATH,
                "step_name": "leaf",
                "parameter_name": case.parameter_name,
                "measurement_text": case.measurement_text,
                "limits_text": case.limits_text,
                "units": case.units,
                "comparison_type": case.comparison_type,
            }
        )
    for result_id, step_id in (("R1", "S90"), ("R2", "S91")):
        rows.append(
            {
                "result_id": result_id,
                "step_id": step_id,
                "parameter_index": 0,
                "step_path": STEP_PATH,
                "step_name": "leaf",
                "parameter_name": REPEATED_PARAMETER,
                "measurement_text": "1.0",
            }
        )
    rows.append(
        {
            "result_id": ABSENT_RESULT_ID,
            "step_id": "S99",
            "parameter_index": 0,
            "step_path": STEP_PATH,
            "step_name": "leaf",
            "parameter_name": "A98",
            "measurement_text": "1.0",
        }
    )
    return rows


# --------------------------------------------------------------------------
# Reading the contracts, and building the inputs out of them


def _contracts() -> Dict[str, Any]:
    """Every contract document of the template, keyed by the asset it names.

    Keyed by `dataset.producer.asset`, which is also the model's file name --
    the contract's own statement of what produces it, rather than a mapping
    written out here that could name the wrong document while every column
    still lined up.
    """
    with CONTRACTS.open(encoding="utf-8") as handle:
        documents = [document for document in yaml.safe_load_all(handle) if document]
    contracts = {}
    for document in documents:
        contract = load_contract(document)
        asset = contract.dataset.producer.get("asset")
        assert asset, contract.contract_id
        contracts[asset] = contract
    return contracts


#: How this module reads the contracts' declared types. The test's reading,
#: not the library's: `contract_mapping` is where a warehouse type is
#: declared, and it is declared per deployment.
_DUCKDB_TYPES = {
    "string": "varchar",
    "boolean": "boolean",
    "timestamp": "timestamp",
    "float": "double",
    "integer": "integer",
}

_FILLERS: Dict[str, Any] = {
    "string": "",
    "boolean": False,
    "timestamp": dt.datetime(2026, 1, 1),
    "float": None,
    "integer": 0,
}


def _create_input(connection, relation: str, contract) -> None:
    columns = ", ".join(
        '"{0}" {1}'.format(column.name, _DUCKDB_TYPES[column.type])
        for column in contract.columns
    )
    connection.execute("create table {0} ({1})".format(relation, columns))


def _insert_rows(connection, relation: str, contract, rows) -> None:
    names = [column.name for column in contract.columns]
    statement = "insert into {0} values ({1})".format(
        relation, ", ".join("?" for _ in names)
    )
    payload = [
        [
            row.get(name, _FILLERS[contract.column(name).type])
            for name in names
        ]
        for row in rows
    ]
    connection.executemany(statement, payload)


def _variables() -> Dict[str, Any]:
    return {
        "mfg_trace_conf_result_relation": RESULT_RELATION,
        "mfg_trace_conf_step_parameter_relation": STEP_RELATION,
        "mfg_trace_decimal_type": DECIMAL_TYPE,
        "mfg_trace_comparators": [
            {"text": row.text, "code": row.code, "bound": row.bound}
            for row in COMPARATORS
        ],
    }


def _render(text: str, refs: Optional[Dict[str, str]] = None) -> str:
    """Render the dbt-side Jinja a warehouse never sees.

    `var` is the variables mapping's own lookup, so a template reading a
    variable this package has not declared raises rather than rendering an
    empty string -- which would compile to SQL that silently reads nothing.
    """
    resolved = dict(refs or {})
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    environment.globals["var"] = _variables().__getitem__
    environment.globals["ref"] = lambda name: resolved.get(name, name)
    environment.globals["config"] = lambda **kwargs: ""
    return environment.from_string(text).render()


@pytest.fixture(scope="module")
def built():
    """The three models, executed on duckdb over inputs the contracts shape."""
    contracts = _contracts()
    connection = duckdb.connect()
    _create_input(connection, RESULT_RELATION, contracts[CONF_RESULT_ASSET])
    _create_input(connection, STEP_RELATION, contracts[CONF_STEP_ASSET])
    _insert_rows(
        connection, RESULT_RELATION, contracts[CONF_RESULT_ASSET], RESULT_ROWS
    )
    _insert_rows(
        connection, STEP_RELATION, contracts[CONF_STEP_ASSET], _step_rows()
    )
    rendered = {
        "test_result": sql.render_test_result(),
        "test_measurement": sql.render_test_measurement(extract=sql.duckdb_extract),
        "test_catalog": sql.render_test_catalog(),
    }
    for model in MODELS:
        connection.execute(
            "create table {0} as {1}".format(model, _render(rendered[model]))
        )
    return connection


def _rows(connection, statement: str) -> List[Dict[str, Any]]:
    cursor = connection.execute(statement)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _columns(connection, relation: str) -> List[str]:
    cursor = connection.execute("select * from {0} where 1 = 0".format(relation))
    return [column[0] for column in cursor.description]


# --------------------------------------------------------------------------
# 1. Byte identity


def test_the_shipped_package_is_there_and_carries_generated_files():
    """The guard against every byte comparison below running over nothing."""
    assert sql.PACKAGE_DIR.is_dir(), sql.PACKAGE_DIR
    assert len(sql.render_models()) > 1
    assert list(sql.PACKAGE_DIR.rglob("*.sql"))


@pytest.mark.parametrize("relative", sorted(sql.render_models()))
def test_every_generated_file_is_shipped_byte_for_byte(relative):
    """Read as bytes and decoded, never through universal newlines.

    The patterns carry non-ASCII characters, so a file written in any other
    encoding differs here. The decode is deliberately not `read_text`:
    that translates CRLF to LF on the way in, so a file re-saved with this
    platform's line endings would compare equal to an LF render -- the one
    difference a deployment would actually install and the test would not
    see. Both are real differences in what ships.
    """
    shipped = sql.PACKAGE_DIR / relative
    assert shipped.is_file(), shipped
    assert shipped.read_bytes().decode("utf-8") == sql.render_models()[relative]


def test_the_shipped_sql_file_set_is_exactly_what_the_generator_emits():
    """Neither an extra file nor a missing one passes.

    A per-file comparison says nothing about a file left behind by a rename:
    it still parses, dbt still runs it, and it tests or builds something
    nobody generates any more.
    """
    shipped = {
        path.relative_to(sql.PACKAGE_DIR).as_posix()
        for path in sql.PACKAGE_DIR.rglob("*.sql")
    }
    assert shipped == set(sql.render_models()) | set(HAND_WRITTEN)
    for relative in HAND_WRITTEN:
        assert (sql.PACKAGE_DIR / relative).is_file(), relative
        assert relative not in sql.render_models(), relative


def test_the_shipped_package_is_on_a_path_no_vcs_ignore_rule_matches():
    """`dbt_packages/` is a reserved name this repo's .gitignore matches.

    Hatchling honours VCS ignore files, so a package shipped under that name
    is absent from the wheel: the example would install nothing, and these
    tests -- which read the source tree -- would be the only thing that ever
    saw the SQL. A rename back to a reserved name fails here rather than in
    a deployment's `dbt deps`.
    """
    root = Path(__file__).resolve().parents[1]
    paths = [
        str((sql.PACKAGE_DIR / relative).relative_to(root).as_posix())
        for relative in sorted(set(sql.render_models()) | set(HAND_WRITTEN))
    ]
    assert paths
    completed = subprocess.run(
        ["git", "check-ignore", "--stdin"],
        cwd=str(root),
        input="\n".join(paths),
        capture_output=True,
        text=True,
    )
    if completed.returncode not in (0, 1):
        pytest.skip("git check-ignore unavailable: {0}".format(completed.stderr.strip()))
    ignored = [line for line in completed.stdout.splitlines() if line.strip()]
    assert ignored == [], ignored


# --------------------------------------------------------------------------
# 2. Cross-route agreement


@pytest.mark.parametrize("case", CASES, ids=[case.step_id for case in CASES])
def test_every_measurement_case_agrees_between_the_two_routes(built, case):
    """The SQL route and the Python route read one rule table the same way."""
    rows = _rows(
        built,
        "select * from test_measurement where step_id = '{0}'".format(case.step_id),
    )
    assert len(rows) == 1, case
    row = rows[0]
    expected = grammar.parse_row(
        case.measurement_text,
        case.limits_text,
        case.units,
        case.comparison_type,
        COMPARATORS,
    )
    assert row["measurement_num"] == expected.measurement_num, case
    assert row["low_limit_num"] == expected.low_limit_num, case
    assert row["high_limit_num"] == expected.high_limit_num, case
    assert row["units"] == expected.units, case
    assert row["comparison_type"] == expected.comparison_type, case
    assert row["parse_failed"] == expected.parse_failed, case


def test_the_cases_are_not_all_the_same_answer(built):
    """An agreement test over rows that all answer null proves nothing."""
    rows = _rows(built, "select * from test_measurement")
    assert sum(1 for row in rows if row["measurement_num"] is not None) >= 5
    assert sum(1 for row in rows if row["low_limit_num"] is not None) >= 2
    assert sum(1 for row in rows if row["high_limit_num"] is not None) >= 2
    assert sum(1 for row in rows if row["parse_failed"]) >= 1
    assert sum(1 for row in rows if row["comparison_type"] is not None) >= 3


@pytest.mark.parametrize("step_id", ["S08", "S09"])
def test_a_number_that_overflows_a_double_is_absent_not_infinite(built, step_id):
    """Finite decimal notation can still cast to infinity.

    `1e999` is matched by the number rule and is not an `inf` literal, so a
    plain cast looks safe and is not: duckdb answers positive infinity for
    it. The Python route answers absent, the contract says this column holds
    a number, and no numeric path in this repo publishes a non-finite one.
    """
    rows = _rows(
        built,
        "select * from test_measurement where step_id = '{0}'".format(step_id),
    )
    assert rows[0]["measurement_num"] is None
    assert rows[0]["parse_failed"] is True


def test_a_tolerance_is_exact_on_both_routes(built):
    """Binary float arithmetic gets this wrong in a way that reads as right.

    `1.005 - 0.005` in doubles is not 1.0, and a limit of 1.0000000000000002
    rejects a reading that passed. The SQL does the subtraction in the
    operator's own exact numeric type, as the Python route does it in
    `Decimal`.
    """
    rows = _rows(built, "select * from test_measurement where step_id = 'S02'")
    assert rows[0]["low_limit_num"] == 1.0
    assert rows[0]["high_limit_num"] == 1.01


def test_an_uncovered_operator_yields_no_bound_on_either_route(built):
    """A bound the table does not cover is not read as a value.

    Whole-operator matching: a table carrying `<` does not cover `<=`. The
    text stays in `limits_text` either way, so nothing is dropped.
    """
    rows = _rows(built, "select * from test_measurement where step_id = 'S13'")
    assert rows[0]["low_limit_num"] is None
    assert rows[0]["high_limit_num"] is None
    assert rows[0]["comparison_type"] is None
    assert rows[0]["limits_text"] == UNCOVERED_OPERATOR_TEXT


def test_an_uncovered_bound_is_not_a_parse_failure(built):
    """Nothing was mis-read: a bound nobody declared was simply not read."""
    rows = _rows(built, "select * from test_measurement where step_id = 'S14'")
    assert rows[0]["parse_failed"] is False


def test_a_comparator_code_carrying_a_quote_survives_the_render(built):
    """A code is the deployment's own word and may contain an apostrophe.

    Rendered into SQL without doubling it, the quote ends the string literal
    and the model fails to compile -- or worse, compiles into something else.
    The operator is also non-ASCII, which the same render has to carry.
    """
    rows = _rows(built, "select * from test_measurement where step_id = 'S05'")
    assert rows[0]["comparison_type"] == "LE'Q"
    assert rows[0]["high_limit_num"] == 7.0


def test_the_measurement_model_drops_a_step_row_whose_attempt_is_absent(built):
    """The inner join, pinned.

    Every column a measurement repeats from its attempt is required, so a
    row carried through with nulls would be a measurement nobody can
    attribute. An edit to an outer join fails here.
    """
    rows = _rows(
        built,
        "select * from test_measurement where result_id = '{0}'".format(
            ABSENT_RESULT_ID
        ),
    )
    assert rows == []
    assert _rows(built, "select * from test_measurement") != []


def test_the_result_model_marks_the_latest_TERMINAL_attempt(built):
    """Not the newest attempt: the newest attempt that has a result.

    Read as `attempt_seq = max(attempt_seq)` instead, the in-flight third
    attempt is the latest, and a reader filtering to one row per unit gets a
    row whose result is not in yet -- which reads as a correct answer.
    """
    rows = {
        row["result_id"]: row
        for row in _rows(built, "select * from test_result")
    }
    assert [rows[key]["attempt_seq"] for key in ("R1", "R2", "R3")] == [1, 2, 3]
    assert rows["R3"]["is_latest"] is False
    assert rows["R2"]["is_latest"] is True
    assert rows["R1"]["is_latest"] is False
    assert rows["R1"]["is_first"] is True
    assert rows["R2"]["is_first"] is False


def test_a_unit_with_no_terminal_attempt_has_no_latest(built):
    """The window answers null there, and the column may not be null."""
    rows = _rows(built, "select * from test_result where result_id = 'R4'")
    assert rows[0]["is_latest"] is False
    assert rows[0]["attempt_seq"] == 1
    assert rows[0]["is_first"] is True


def test_the_catalog_counts_distinct_attempts_per_parameter(built):
    """And reports when the parameter was first and last measured."""
    rows = _rows(
        built,
        "select * from test_catalog where parameter_name = '{0}'".format(
            REPEATED_PARAMETER
        ),
    )
    assert len(rows) == 1
    assert rows[0]["n_results"] == 2
    assert rows[0]["first_seen"] == _at(1)
    assert rows[0]["last_seen"] == _at(2)


def test_the_catalog_writes_an_absent_unit_as_the_contract_declares(built):
    """The unit is part of the grain, so it cannot be null."""
    rows = _rows(
        built,
        "select * from test_catalog where parameter_name = '{0}'".format(
            REPEATED_PARAMETER
        ),
    )
    assert rows[0]["units"] == ""
    assert [row for row in _rows(built, "select * from test_catalog") if row["units"] is None] == []


# --------------------------------------------------------------------------
# The generated tests, run in both directions


@pytest.mark.parametrize("model, grain", sql.GRAINS)
def test_the_grain_test_is_clean_on_the_built_model(built, model, grain):
    statement = _render(sql.render_duplicate_key_test(model, grain))
    assert _rows(built, statement) == []


@pytest.mark.parametrize("model, grain", sql.GRAINS)
def test_the_grain_test_finds_a_planted_duplicate(built, model, grain):
    """The clean direction alone passes on a test that finds nothing ever."""
    planted = "planted_{0}".format(model)
    built.execute(
        "create or replace table {0} as "
        "select * from {1} union all select * from {1}".format(planted, model)
    )
    statement = _render(
        sql.render_duplicate_key_test(model, grain), refs={model: planted}
    )
    found = _rows(built, statement)
    assert found != []
    assert {row["rule_id"] for row in found} == {sql.RULE_DUPLICATE_KEY}


@pytest.mark.parametrize("child, parent, key", sql.ORPHAN_EDGES)
def test_the_reference_test_is_clean_on_the_built_models(built, child, parent, key):
    statement = _render(sql.render_orphan_reference_test(child, parent, key))
    assert _rows(built, statement) == []


@pytest.mark.parametrize("child, parent, key", sql.ORPHAN_EDGES)
def test_the_reference_test_finds_a_planted_orphan(built, child, parent, key):
    planted = "planted_orphan_{0}".format(child)
    built.execute(
        "create or replace table {0} as select * from {1} "
        "union all select * from {1} limit 1".format(planted, child)
    )
    built.execute(
        "update {0} set {1} = '{2}'".format(planted, key, ABSENT_RESULT_ID)
    )
    statement = _render(
        sql.render_orphan_reference_test(child, parent, key), refs={child: planted}
    )
    found = _rows(built, statement)
    assert found != []
    assert {row["rule_id"] for row in found} == {sql.RULE_ORPHAN_REFERENCE}


def test_the_uncovered_comparator_test_warns_rather_than_failing():
    """An uncovered bound is counted, not an error.

    A bound the deployment's table does not cover is kept as text and
    reported; it is not a defect in the data and it is not something a
    build should refuse to publish over. Shipped at the failing severity,
    the first unfamiliar operator in a real source stops the build, and the
    pressure is then to drop the row rather than to add the row to the
    table.
    """
    text = (sql.PACKAGE_DIR / "tests" / "uncovered_comparator.sql").read_text(
        encoding="utf-8"
    )
    assert "severity='warn'" in text


def test_the_uncovered_comparator_test_counts_the_uncovered_bound(built):
    """Re-derived from the rule and the same table, not from a column.

    The curated contract declares no column for an uncovered operator, so
    there is nothing to read: the test applies the comparator rule to the
    limits text again and asks the table the same question. A predicate over
    the published columns could not tell this row from a limits text that was
    never a bound at all.
    """
    statement = _render(
        sql.render_uncovered_comparator_test(extract=sql.duckdb_extract)
    )
    found = _rows(built, statement)
    assert [row["limits_text"] for row in found].count(UNCOVERED_OPERATOR_TEXT) == 2
    assert len(found) == 2


# --------------------------------------------------------------------------
# 3. The contract companion


@pytest.mark.parametrize("model", MODELS)
def test_every_model_publishes_exactly_its_contracts_columns(built, model):
    """In the document's order, which is the order the model selects in.

    The generator holds its own column lists, because library SQL that read
    an example contract would make the example load-bearing for the package.
    This is the check that keeps the two statements honest.
    """
    contract = _contracts()[model]
    assert contract.column_names, model
    assert _columns(built, model) == contract.column_names


@pytest.mark.parametrize("model", MODELS)
def test_every_column_a_models_contract_requires_is_produced(built, model):
    contract = _contracts()[model]
    required = [column.name for column in contract.must_produce]
    assert required, model
    assert set(required) <= set(_columns(built, model))


# --------------------------------------------------------------------------
# 4. No name from below, and the required variables


def test_the_inputs_carry_exactly_the_conformed_contracts_columns(built):
    """The proof behind every execution above.

    The input tables are built from the conformed contract documents and
    nothing else, so a model reading any other column fails to execute --
    which is a stronger statement than a list of forbidden words, and one
    that stays true as the sources change. A helper column added to the
    fixture is how that proof is quietly lost, so it fails here.
    """
    contracts = _contracts()
    for relation, asset in (
        (RESULT_RELATION, CONF_RESULT_ASSET),
        (STEP_RELATION, CONF_STEP_ASSET),
    ):
        assert _columns(built, relation) == contracts[asset].column_names


def test_the_grammar_and_the_generator_import_no_framework():
    """Both halves are pure: one rule table, readable with no infrastructure.

    `sql` renders text and `measurements` reads text; a dagster, dbt, dlt or
    database import in either is what makes the mapping semantics untestable
    without a warehouse.
    """
    forbidden = ("dagster", "dbt", "dlt", "duckdb", "sqlalchemy", "jinja2")
    for module in (sql, grammar):
        source = Path(module.__file__).read_text(encoding="utf-8")
        lines = [
            line
            for line in source.splitlines()
            if line.startswith("import ") or line.startswith("from ")
        ]
        assert lines
        for line in lines:
            for name in forbidden:
                assert name not in line, (module.__name__, line)


def test_the_package_declares_a_default_for_no_required_variable():
    """`var('x')` with no default fails the parse, which is the point.

    A default would name a relation belonging to whichever deployment wrote
    it, pick an exact numeric type for a warehouse nobody asked about, or --
    worst -- invent comparison codes. An operator learns a variable is
    missing when the project parses, rather than from SQL that compiled
    against a guess.
    """
    project = (sql.PACKAGE_DIR / "dbt_project.yml").read_text(encoding="utf-8")
    assert "name:" in project
    assert yaml.safe_load(project).get("vars") is None
    for relative, text in sql.render_models().items():
        for fragment in text.split("var(")[1:]:
            call = fragment.split(")")[0]
            assert "," not in call, (relative, call)


def test_every_variable_the_shipped_sql_reads_is_declared_required():
    """Equality both ways.

    A variable used and not declared is one an operator is never told to
    set; a variable declared and not used is one they set for nothing, and
    the next reader cannot tell which of the two a mismatch is.
    """
    used = set()
    for text in sql.render_models().values():
        for fragment in text.split("var('")[1:]:
            used.add(fragment.split("'")[0])
    assert used == set(sql.REQUIRED_VARS)


def test_an_undeclared_variable_is_not_quietly_rendered():
    """The anti-vacuity guard for the test above and for every render here.

    A renderer that answered an empty string for an unknown variable would
    compile every model in this module into SQL that reads nothing, and the
    agreement tests would then compare two empty answers.
    """
    with pytest.raises(KeyError):
        _render("select * from {{ var('mfg_trace_not_a_variable') }}")


# --------------------------------------------------------------------------
# The example's sample comparator table, exercised rather than read


def _example_vars() -> Dict[str, Any]:
    with EXAMPLE_PROJECT.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)["vars"]


def _example_comparators() -> Tuple[grammar.Comparator, ...]:
    return tuple(
        grammar.Comparator(text=row["text"], code=row["code"], bound=row["bound"])
        for row in _example_vars()["mfg_trace_comparators"]
    )


def test_the_example_sets_exactly_the_variables_the_package_requires():
    """Equality, so neither an unset variable nor a stale one passes.

    An unset one fails the operator's `dbt parse` and the example is then
    advice that does not work; a stale one is a variable they set for
    nothing, and the next reader cannot tell which of the two it is.
    """
    assert set(_example_vars()) == set(sql.REQUIRED_VARS)


def test_the_example_relations_are_the_conformed_contracts_assets():
    """The example's two relations name the two conformed documents.

    They are the seam the whole slice is built on: the package reads these
    relations' columns as the conformed contracts declare them, so an
    example pointing somewhere else is an example of nothing.
    """
    variables = _example_vars()
    contracts = _contracts()
    for asset in (CONF_RESULT_ASSET, CONF_STEP_ASSET):
        assert asset in contracts
    assert CONF_RESULT_ASSET in variables["mfg_trace_conf_result_relation"]
    assert CONF_STEP_ASSET in variables["mfg_trace_conf_step_parameter_relation"]


def test_the_examples_local_package_entry_resolves_to_the_shipped_package():
    """A `local:` entry is a relative path, and a relative path can dangle.

    The example is the one worked installation of this package, so a path
    that no longer resolves makes it advice that fails at `dbt deps` -- the
    same failure mode a dangling `project:` path already has its own test
    for. Resolved against the package directory the library publishes, so a
    move of either end fails here.
    """
    packages = EXAMPLE_PROJECT.parent / "packages.yml"
    with packages.open(encoding="utf-8") as handle:
        entries = yaml.safe_load(handle)["packages"]
    locals_declared = [entry["local"] for entry in entries if "local" in entry]
    assert len(locals_declared) == 1, entries
    resolved = (packages.parent / locals_declared[0]).resolve()
    assert resolved == sql.PACKAGE_DIR.resolve()
    assert (resolved / "dbt_project.yml").is_file()


def test_the_examples_sample_comparator_table_is_a_shape_both_routes_read():
    """Three keys per row, and a bound that names a column the contract has.

    `low` and `high` are the library's own words, named after the two limit
    columns the curated contract declares. A fourth spelling is a row that
    silently bounds nothing.
    """
    rows = _example_vars()["mfg_trace_comparators"]
    assert rows
    for row in rows:
        assert set(row) == {"text", "code", "bound"}, row
        assert row["bound"] in ("low", "high"), row
        assert row["text"].strip() == row["text"], row
    texts = [row["text"] for row in rows]
    assert len(texts) == len(set(texts)), texts


@pytest.mark.parametrize(
    "row",
    _example_vars()["mfg_trace_comparators"],
    ids=lambda row: "{0}-{1}".format(row["code"], len(row["text"])),
)
def test_every_row_of_the_examples_sample_table_bounds_what_it_says(row):
    """Executed, on both routes, against the example's own table.

    The whole-operator rule makes this worth doing per row rather than once:
    a table carrying `<` does not cover `<=`, so the sample has to carry
    both spellings and each one has to be shown to land on its own side.
    """
    comparators = _example_comparators()
    limits_text = "{0} 7".format(row["text"])
    expected = grammar.parse_row("4.2", limits_text, None, None, comparators)
    assert expected.comparison_type == row["code"]
    assert expected.uncovered_comparator is None
    bounded = {"low": expected.low_limit_num, "high": expected.high_limit_num}
    assert bounded[row["bound"]] == 7.0
    assert bounded["low" if row["bound"] == "high" else "high"] is None

    contracts = _contracts()
    connection = duckdb.connect()
    _create_input(connection, RESULT_RELATION, contracts[CONF_RESULT_ASSET])
    _create_input(connection, STEP_RELATION, contracts[CONF_STEP_ASSET])
    _insert_rows(
        connection, RESULT_RELATION, contracts[CONF_RESULT_ASSET], RESULT_ROWS[:1]
    )
    _insert_rows(
        connection,
        STEP_RELATION,
        contracts[CONF_STEP_ASSET],
        [
            {
                "result_id": "R1",
                "step_id": "S01",
                "parameter_index": 0,
                "step_path": STEP_PATH,
                "step_name": "leaf",
                "parameter_name": "A01",
                "measurement_text": "4.2",
                "limits_text": limits_text,
            }
        ],
    )
    variables = dict(_example_vars())
    variables["mfg_trace_conf_result_relation"] = RESULT_RELATION
    variables["mfg_trace_conf_step_parameter_relation"] = STEP_RELATION
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    environment.globals["var"] = variables.__getitem__
    environment.globals["ref"] = lambda name: name
    environment.globals["config"] = lambda **kwargs: ""
    for model, text in (
        ("test_result", sql.render_test_result()),
        ("test_measurement", sql.render_test_measurement(extract=sql.duckdb_extract)),
    ):
        connection.execute(
            "create table {0} as {1}".format(
                model, environment.from_string(text).render()
            )
        )
    built_row = _rows(connection, "select * from test_measurement")[0]
    assert built_row["comparison_type"] == expected.comparison_type
    assert built_row["low_limit_num"] == expected.low_limit_num
    assert built_row["high_limit_num"] == expected.high_limit_num


def test_the_example_covers_both_spellings_of_each_bound():
    """Otherwise the per-row test above proves nothing about the rule.

    The sample is there to show that a non-ASCII operator is a deployment's
    to cover and that a two-character one needs its own row. A table with
    one spelling of each would pass every assertion above while
    demonstrating neither.
    """
    rows = _example_vars()["mfg_trace_comparators"]
    assert sum(1 for row in rows if row["bound"] == "low") >= 2
    assert sum(1 for row in rows if row["bound"] == "high") >= 2
    assert [row for row in rows if len(row["text"]) > 1]
    assert [row for row in rows if not row["text"].isascii()]


def test_an_operator_the_examples_table_omits_is_uncovered_not_guessed():
    """An equality is deliberately absent, and absent means uncovered.

    The sample's comment says so; this is the assertion behind it. Were a
    prefix read instead, an operator would be covered by the shorter row it
    begins with, which is the failure the whole-operator rule exists to
    prevent.
    """
    parsed = grammar.parse_row("4.2", "== 7", None, None, _example_comparators())
    assert parsed.comparison_type is None
    assert parsed.low_limit_num is None
    assert parsed.high_limit_num is None
    assert parsed.uncovered_comparator == "=="
    assert parsed.parse_failed is False
