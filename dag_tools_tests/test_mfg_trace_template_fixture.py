"""``examples/mfg_trace/fixture/seed.py`` is held to what its README claims.

The README says three things about this fixture that nothing enforced until
this module existed: that the seeded columns are exactly the two stage-1
mappings' declared ``source.columns`` **in both directions**, that each plant
sits on its own change record or serial beside a control that trips nothing,
and that the plants parse through the library's own grammar against the
example's own comparator table. A prose claim about a fixture is the one kind
of claim that reads as true forever, because the fixture is synthetic and
nobody is surprised by it in production.

Every assertion below carries an anti-vacuity guard. A seeder that stopped
creating a relation, a mapping whose ``source`` block moved, or a comparator
table that lost its rows would otherwise pass every check here by finding
nothing to check -- which is the shape of defect this repository keeps going
back to fix.
"""

from __future__ import annotations

import importlib.util
import re
import sqlite3
from pathlib import Path
from typing import Dict, List

import pytest
import yaml

from dag_tools.systemlink import measurements

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = REPO_ROOT / "examples" / "mfg_trace"
SEEDER = EXAMPLE / "fixture" / "seed.py"
CONFORM = EXAMPLE / "conform" / "components"
CURATED_PROJECT = EXAMPLE / "curated" / "dbt_project.yml"
README = EXAMPLE / "README.md"

#: The relation a dbt ``source()`` ref names, so the mapping's own spelling of
#: its table is what gets compared rather than a name repeated here.
_REF_RELATION = re.compile(r"""['"]([A-Za-z0-9_]+)['"]\s*\)""")


def _seeder():
    """Import the seeder by path: the example is not an installed package."""
    spec = importlib.util.spec_from_file_location("mfg_trace_fixture_seed", SEEDER)
    assert spec and spec.loader, SEEDER
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


seed = _seeder()


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    path = seed.seed(tmp_path_factory.mktemp("mfg_trace_fixture") / "fixture.db")
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    yield connection
    connection.close()


def _columns(connection: sqlite3.Connection, table: str) -> List[str]:
    return [row[1] for row in connection.execute("PRAGMA table_info(" + table + ")")]


def _mappings() -> Dict[str, dict]:
    """Each stage-1 mapping document, by the directory it is shipped in."""
    found = {}
    for path in sorted(CONFORM.glob("*/mapping.yaml")):
        found[path.parent.name] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return found


MAPPINGS = _mappings()


def _comparators():
    """The example's own table, read out of the example's own dbt project."""
    project = yaml.safe_load(CURATED_PROJECT.read_text(encoding="utf-8"))
    return tuple(
        measurements.Comparator(row["text"], row["code"], row["bound"])
        for row in project["vars"]["mfg_trace_comparators"]
    )


COMPARATORS = _comparators()


# ---------------------------------------------------------------------------
# The seeder produces what it declares


def test_the_seeder_declares_relations_and_the_example_still_ships_them() -> None:
    """The guard for every test below, each of which reads one of these."""
    assert SEEDER.is_file(), SEEDER
    assert len(seed.RELATIONS) >= 5, seed.RELATIONS
    assert MAPPINGS, CONFORM
    assert len(COMPARATORS) >= 6, COMPARATORS


def test_every_declared_relation_is_created_with_its_columns_and_rows(db) -> None:
    problems = []
    for relation, columns, rows in seed.RELATIONS:
        table = relation["table"]
        seeded = _columns(db, table)
        if seeded != list(columns):
            problems.append(table + ": seeded " + repr(seeded) + " for declared " + repr(list(columns)))
        count = db.execute("SELECT count(*) FROM " + table).fetchone()[0]
        if count != len(rows()):
            problems.append(table + ": " + str(count) + " rows seeded, " + str(len(rows())) + " declared")
        if not count:
            problems.append(table + ": carries no row, so it proves nothing")
    assert not problems, problems


# ---------------------------------------------------------------------------
# The column equality the README claims, in both directions


@pytest.mark.parametrize("name", sorted(MAPPINGS))
def test_every_column_a_conform_mapping_declares_is_seeded_and_no_other(
    name: str, db
) -> None:
    """Both directions, which is what makes this catch either drift.

    One direction alone passes on a mapping that declares nothing, or on a
    fixture that seeds every column any mapping could ever want.
    """
    source = MAPPINGS[name]["source"]
    match = _REF_RELATION.search(source["table"])
    assert match, name + ": cannot read a relation out of " + repr(source["table"])
    table = match.group(1)

    seeded = _columns(db, table)
    assert seeded, name + ": the fixture seeds no relation called " + table
    declared = list(source["columns"])
    assert declared, name + ": the mapping declares no source column"

    missing = [column for column in declared if column not in seeded]
    extra = [column for column in seeded if column not in declared]
    assert not missing, name + ": declared and not seeded: " + repr(missing)
    assert not extra, name + ": seeded and not declared: " + repr(extra)


# ---------------------------------------------------------------------------
# One plant per change record, beside a control


_RECORD_COLUMN = {
    "bom": (seed.BOM["table"], seed.BOM["record"]),
    "install": (seed.INSTALL["table"], seed.INSTALL["record"]),
}


def _records(connection, which: str) -> List[str]:
    table, column = _RECORD_COLUMN[which]
    return [row[0] for row in connection.execute("SELECT " + column + " FROM " + table)]


def test_every_structural_plant_sits_on_its_own_change_record(db) -> None:
    assert seed.PLANTS, "no structural plant is declared"
    keys = [plant["key"] for plant in seed.PLANTS.values()]
    assert len(set(keys)) == len(keys), "two plants share a change record: " + repr(keys)

    controls = {seed.CONTROL_BOM["key"], seed.CONTROL_INSTALL["key"]}
    assert not controls & set(keys), "a control shares a plant's change record"

    problems = []
    for name, plant in seed.PLANTS.items():
        if plant["key"] not in _records(db, plant["table"]):
            problems.append(name + ": " + plant["key"] + " reaches no row of " + plant["table"])
    assert not problems, problems


def test_the_structural_controls_carry_a_plain_row_each(db) -> None:
    """A control that is itself remarkable separates nothing.

    The bom control is one ordinary edge: a child, on its own level, with a
    find number and exactly one designator. The install control is one open
    installation. If either grew a second row or a blank, a plant's reading
    would be indistinguishable from the fixture simply being malformed.
    """
    bom = seed.BOM
    edges = db.execute(
        "SELECT * FROM " + bom["table"] + " WHERE " + bom["record"] + " = ?",
        (seed.CONTROL_BOM["key"],),
    ).fetchall()
    assert len(edges) == 1, "the bom control carries " + str(len(edges)) + " edges"
    edge = edges[0]
    assert edge[bom["kind"]] == seed.KIND_CHILD
    assert edge[bom["find"]] not in (seed.BLANK, None), "the control has no find number"
    assert len(edge[bom["designators"]].split()) == 1, "the control has several designators"

    install = seed.INSTALL
    rows = db.execute(
        "SELECT * FROM " + install["table"] + " WHERE " + install["record"] + " = ?",
        (seed.CONTROL_INSTALL["key"],),
    ).fetchall()
    assert len(rows) == 1, "the install control carries " + str(len(rows)) + " rows"
    assert rows[0][install["valid_to"]] == seed.OPEN_END, "the control is already closed"


# ---------------------------------------------------------------------------
# One measurement plant per serial, beside a control


def _parameters(connection, serial: str):
    result, parameter = seed.RESULT, seed.PARAMETER
    return connection.execute(
        "SELECT p.* FROM " + parameter["table"] + " p"
        " JOIN " + result["table"] + " r ON r." + result["id"] + " = p." + parameter["result"] +
        " WHERE r." + result["serial"] + " = ?"
        " ORDER BY p." + parameter["result"] + ", p." + parameter["index"],
        (serial,),
    ).fetchall()


def test_every_measurement_plant_sits_on_its_own_serial(db) -> None:
    assert seed.RESULT_PLANTS, "no measurement plant is declared"
    serials = [plant["serial"] for plant in seed.RESULT_PLANTS.values()]
    assert len(set(serials)) == len(serials), "two plants share a serial: " + repr(serials)
    assert seed.CONTROL_RESULT["serial"] not in serials, "the control is also a plant"

    result = seed.RESULT
    seeded = {
        row[0]
        for row in db.execute("SELECT " + result["serial"] + " FROM " + result["table"])
    }
    wanted = serials + [seed.CONTROL_RESULT["serial"]]
    missing = [serial for serial in wanted if serial not in seeded]
    assert not missing, "serials reaching no result row: " + repr(missing)


def test_the_plants_leave_the_comparator_empty_and_the_control_supplies_one(db) -> None:
    """The two paths into a comparison code, and which rows exercise which.

    A plant's operator has to travel in the limits text, or the grammar is
    never read at all; the control's is supplied directly, which is the other
    path and the one a supplied value has to win on.
    """
    parameter = seed.PARAMETER
    problems = []
    for name, plant in seed.RESULT_PLANTS.items():
        rows = _parameters(db, plant["serial"])
        if name == "non_terminal":
            if rows:
                problems.append(name + ": carries " + str(len(rows)) + " parameter rows, expected none")
            continue
        if not rows:
            problems.append(name + ": carries no parameter row, so it measures nothing")
            continue
        supplied = [row[parameter["comparator"]] for row in rows]
        if any(value not in (seed.BLANK, None) for value in supplied):
            problems.append(name + ": supplies a comparator " + repr(supplied))
    assert not problems, problems

    control = _parameters(db, seed.CONTROL_RESULT["serial"])
    assert control, "the control carries no parameter row"
    assert all(
        row[parameter["comparator"]] not in (seed.BLANK, None) for row in control
    ), "the control supplies no comparison code, so that path is unexercised"


# ---------------------------------------------------------------------------
# The plants read through the library's grammar


def test_the_comparator_table_is_the_examples_own_and_omits_an_equality() -> None:
    """The guard for the parse tests, and for one case in particular.

    ``comparator_uncovered`` is only a case because this table carries no
    equality. A table that grew one would make that plant indistinguishable
    from ``comparator_high``, and the plant would still be seeded, still be
    parsed and still pass a looser assertion.
    """
    texts = [row.text for row in COMPARATORS]
    assert len(set(texts)) == len(texts), "the table repeats an operator: " + repr(texts)
    assert {row.bound for row in COMPARATORS} == {"low", "high"}, COMPARATORS
    assert "==" not in texts, "the table now covers an equality"
    assert any(len(text) > 1 for text in texts), "no multi-character operator"
    assert any(ord(char) > 127 for text in texts for char in text), "no non-ASCII operator"


def _is_tolerance(parse) -> bool:
    return (
        parse.low_limit_num is not None
        and parse.high_limit_num is not None
        and parse.low_limit_num < parse.high_limit_num
        and parse.comparison_type is None
        and not parse.parse_failed
    )


#: What each plant is, read as the grammar's own answer. Nothing here repeats
#: a number the fixture writes: every expectation is a property of the parse.
_EXPECTED = {
    "tolerance": _is_tolerance,
    "comparator_low": lambda p: (
        p.low_limit_num is not None
        and p.high_limit_num is None
        and p.comparison_type is not None
        and not p.parse_failed
    ),
    "comparator_high": lambda p: (
        p.high_limit_num is not None
        and p.low_limit_num is None
        and p.comparison_type is not None
        and not p.parse_failed
    ),
    "comparator_uncovered": lambda p: (
        p.uncovered_comparator is not None
        and p.low_limit_num is None
        and p.high_limit_num is None
        and p.comparison_type is None
        and not p.parse_failed
    ),
    "bare_number_limits": lambda p: (
        p.measurement_num is not None
        and p.low_limit_num is None
        and p.high_limit_num is None
        and p.uncovered_comparator is None
        and not p.parse_failed
    ),
    "parse_failed": lambda p: p.parse_failed and p.measurement_num is None,
    "empty_measurement": lambda p: (
        p.measurement_num is None
        and not p.parse_failed
        and p.low_limit_num is None
        and p.high_limit_num is None
    ),
    "empty_units": lambda p: (
        p.units is None and p.measurement_num is not None and not p.parse_failed
    ),
    "repeated_parameter_name": lambda p: (
        p.measurement_num is not None and not p.parse_failed
    ),
    "rerun": lambda p: p.measurement_num is not None and not p.parse_failed,
}


def _parse(row):
    parameter = seed.PARAMETER
    return measurements.parse_row(
        row[parameter["measurement"]],
        row[parameter["limits"]],
        row[parameter["units"]],
        row[parameter["comparator"]],
        COMPARATORS,
    )


def test_every_plant_with_a_measurement_has_a_declared_reading() -> None:
    """The guard: a plant added to the seeder with no expectation here."""
    unexpected = set(seed.RESULT_PLANTS) - set(_EXPECTED) - {"non_terminal"}
    assert not unexpected, "plants with no declared reading: " + repr(sorted(unexpected))
    stale = set(_EXPECTED) - set(seed.RESULT_PLANTS)
    assert not stale, "readings for plants the fixture dropped: " + repr(sorted(stale))


@pytest.mark.parametrize("name", sorted(_EXPECTED))
def test_each_measurement_plant_parses_as_the_case_it_says_it_is(name: str, db) -> None:
    rows = _parameters(db, seed.RESULT_PLANTS[name]["serial"])
    assert rows, name + ": no parameter row to parse"
    wrong = [parse for parse in (_parse(row) for row in rows) if not _EXPECTED[name](parse)]
    assert not wrong, name + ": " + repr(wrong)


def test_the_plants_are_not_all_the_same_answer(db) -> None:
    """Anti-vacuity for the parametrized test above.

    Ten plants whose parses were identical would satisfy any predicate loose
    enough to pass all of them, and the suite would say nothing about the
    grammar.
    """
    shapes = set()
    for name in _EXPECTED:
        for row in _parameters(db, seed.RESULT_PLANTS[name]["serial"]):
            parse = _parse(row)
            shapes.add(
                (
                    parse.measurement_num is None,
                    parse.low_limit_num is None,
                    parse.high_limit_num is None,
                    parse.comparison_type,
                    parse.uncovered_comparator,
                    parse.parse_failed,
                    parse.units is None,
                )
            )
    assert len(shapes) >= 6, shapes


def test_the_repeated_parameter_name_is_told_apart_by_its_index(db) -> None:
    """The plant's whole point: the index is part of the grain."""
    parameter = seed.PARAMETER
    rows = _parameters(db, seed.RESULT_PLANTS["repeated_parameter_name"]["serial"])
    assert len(rows) >= 2, rows
    assert len({row[parameter["name"]] for row in rows}) == 1, "the names differ"
    indexes = [row[parameter["index"]] for row in rows]
    assert len(set(indexes)) == len(indexes), "the indexes repeat: " + repr(indexes)


def test_the_rerun_serial_carries_two_results_with_different_outcomes(db) -> None:
    result = seed.RESULT
    rows = db.execute(
        "SELECT * FROM " + result["table"] + " WHERE " + result["serial"] + " = ?",
        (seed.RESULT_PLANTS["rerun"]["serial"],),
    ).fetchall()
    assert len(rows) == 2, "the rerun carries " + str(len(rows)) + " results"
    assert len({row[result["status"]] for row in rows}) == 2, "both attempts agree"
    assert len({row[result["started"]] for row in rows}) == 2, "both attempts are at once"


def test_the_non_terminal_result_is_not_in_the_examples_terminal_list(db) -> None:
    """What makes it the non-terminal plant, read from the example's own YAML."""
    component = yaml.safe_load(
        (EXAMPLE / "systemlink" / "component.yaml").read_text(encoding="utf-8")
    )
    terminal = component["attributes"]["source_config"]["filters"]["terminal_statuses"]
    assert terminal, "the example declares no terminal status"
    result = seed.RESULT
    rows = db.execute(
        "SELECT * FROM " + result["table"] + " WHERE " + result["serial"] + " = ?",
        (seed.RESULT_PLANTS["non_terminal"]["serial"],),
    ).fetchall()
    assert rows, "the non-terminal plant reaches no result row"
    assert all(row[result["status"]] not in terminal for row in rows), rows
    assert any(
        row[result["status"]] in terminal
        for row in db.execute("SELECT * FROM " + result["table"])
    ), "no row carries a terminal status, so the exclusion proves nothing"


# ---------------------------------------------------------------------------
# What the fixture deliberately carries unchanged


def test_the_open_end_sentinel_reaches_raw_rather_than_a_null(db) -> None:
    """Turning it into a null is a ``nullif`` the curated layer declares.

    A fixture that pre-nulled it would hide the one transform the curated
    layer has to get right, and the SQL would look correct against it.
    """
    problems = []
    for relation in (seed.BOM, seed.INSTALL):
        values = [
            row[0]
            for row in db.execute(
                "SELECT " + relation["valid_to"] + " FROM " + relation["table"]
            )
        ]
        if seed.OPEN_END not in values:
            problems.append(relation["table"] + ": no row carries the sentinel")
        if any(value is None for value in values):
            problems.append(relation["table"] + ": a row is already null")
    assert not problems, problems


def test_the_blank_find_number_and_designator_are_blank_rather_than_null(db) -> None:
    bom = seed.BOM
    rows = db.execute(
        "SELECT * FROM " + bom["table"] + " WHERE " + bom["record"] + " = ?",
        (seed.PLANTS["no_find_number"]["key"],),
    ).fetchall()
    assert rows, "the no-find-number plant reaches no row"
    for row in rows:
        assert row[bom["find"]] == seed.BLANK, repr(row[bom["find"]])
        assert row[bom["designators"]] == seed.BLANK, repr(row[bom["designators"]])


# ---------------------------------------------------------------------------
# The README is held to the fixture, not the other way round


def _fixture_section() -> str:
    """The README's own section about this fixture, and nothing else."""
    body = README.read_text(encoding="utf-8")
    start = body.index("## The template fixture")
    end = body.index("\n## ", start + 1)
    return body[start:end]


def _tables(section: str) -> List[List[str]]:
    """Each markdown table in the section, as its data rows."""
    tables: List[List[str]] = []
    current: List[str] = []
    for line in section.splitlines():
        if line.startswith("|"):
            current.append(line)
        elif current:
            tables.append([row for row in current[2:]])  # drop header and rule
            current = []
    if current:
        tables.append([row for row in current[2:]])
    return tables


def test_the_readme_cites_this_test_and_tabulates_every_plant_once() -> None:
    """The README's two tables are what an operator reads instead of the seeder.

    They name the plants in prose rather than by the seeder's keys, so the
    check is on the count: a plant added to the fixture and left out of the
    document, or a table row describing a plant nothing seeds, fails here.
    """
    section = _fixture_section()
    assert "test_mfg_trace_template_fixture.py" in section, "the README cites no test"

    tables = _tables(section)
    assert len(tables) == 2, "the section no longer carries two plant tables"
    structural, measurement = tables
    assert structural and measurement, tables

    assert len(structural) == len(seed.PLANTS), (
        str(len(structural)) + " structural rows for " + str(len(seed.PLANTS)) + " plants"
    )
    # One row per measurement plant, plus the control's own row.
    assert len(measurement) == len(seed.RESULT_PLANTS) + 1, (
        str(len(measurement)) + " measurement rows for "
        + str(len(seed.RESULT_PLANTS)) + " plants and a control"
    )


def test_every_limits_text_the_fixture_seeds_is_quoted_in_the_readme(db) -> None:
    """The one value in that table a reader acts on, held to the fixture.

    The limits column is the whole subject of the measurement table; a plant
    whose text changed in the seeder and not in the document would leave the
    README explaining a case that is no longer planted.
    """
    parameter = seed.PARAMETER
    seeded = {
        row[0]
        for row in db.execute("SELECT " + parameter["limits"] + " FROM " + parameter["table"])
        if row[0] not in (seed.BLANK, None)
    }
    assert len(seeded) >= 5, "too few distinct limits texts to prove anything: " + repr(seeded)
    section = _fixture_section()
    missing = sorted(text for text in seeded if "`" + text + "`" not in section)
    assert not missing, "limits texts the README does not quote: " + repr(missing)
