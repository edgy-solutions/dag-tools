"""Build the sqlite stand-in for the two sources the mfg_trace template needs.

Five tables, in two halves.

The as-designed / as-built half is three relations: a parent-to-child
structure with revisions and effectivity, the serialized units that were
built, and where each unit was installed. One unit is removed and replaced,
so as-built and current differ, and one unit is installed in a parent this
example's part list cannot see.

The test-data half is two relations shaped like what a stage-1 mapping
reads: one row per test result, and one row per parameter measured in a
step. Their columns are exactly the two conformed mappings' declared
`source.columns`, which is what makes this fixture real input for stage 1
rather than a picture of it.

Each thing the curated layer has to get right is planted on its OWN key, and
one more key is planted with nothing, so a fixture that is broken everywhere
is distinguishable from one that is planted somewhere. The structural plants
are keyed by change record; the measurement plants are keyed by serial
number, because a measurement is a property of a unit's test.

There is ONE load. Unlike the BW fixture beside it, nothing here is
extracted through dlt -- the step-parameter relation is shaped like a view
over dlt's own child table, so the `_dlt_*` columns are already dlt's output
and extracting them again would be dlt reading itself. So there is no scd2
history to observe across two loads, and the replacement is carried as a
closed row and an open row inside the single load, which is how the source
system records it.

Every name in here belongs to this example's sqlite file. A real
deployment's relation and column names live in its own YAML; dag-tools names
none of them. The roles below (`parent`, `serial`, ...) are this file's own
labels, so the example's tests can find a column without restating it.

Nothing here is the curated layer's job done early. The open-interval
sentinel, the blank find number, the empty measurement and the operator
reference are planted as the source writes them and are expected to reach
raw unchanged; turning the sentinel into a null is a `nullif` the curated
layer declares, not something this file does.
"""

import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

DEFAULT_DB = Path(__file__).resolve().parent / "fixture.db"

# ---------------------------------------------------------------------------
# The as-designed / as-built half

BOM = {
    "table": "PDM_BOM_EDGE",
    "parent": "parent_part",
    "parent_rev": "parent_rev",
    "child": "child_part",
    "child_rev": "child_rev",
    "quantity": "qty",
    "unit": "uom",
    "find": "find_no",
    "designators": "ref_des",
    "kind": "edge_kind",
    "valid_from": "eff_from",
    "valid_to": "eff_to",
    "record": "src_record",
    "changed": "changed_at",
}

UNIT = {
    "table": "MES_UNIT",
    "part": "unit_part",
    "serial": "unit_serial",
    "built": "built_at",
    "state": "mes_state",
    "record": "src_record",
    "changed": "changed_at",
}

INSTALL = {
    "table": "MES_INSTALL",
    "id": "install_id",
    "parent": "parent_part",
    "parent_serial": "parent_serial",
    "child": "child_part",
    "child_serial": "child_serial",
    "position": "position_ref",
    "valid_from": "from_ts",
    "valid_to": "to_ts",
    "record": "src_record",
    "changed": "changed_at",
}

BOM_COLUMNS: Tuple[str, ...] = (
    BOM["parent"], BOM["parent_rev"], BOM["child"], BOM["child_rev"],
    BOM["quantity"], BOM["unit"], BOM["find"], BOM["designators"],
    BOM["kind"], BOM["valid_from"], BOM["valid_to"],
    BOM["record"], BOM["changed"],
)

UNIT_COLUMNS: Tuple[str, ...] = (
    UNIT["part"], UNIT["serial"], UNIT["built"], UNIT["state"],
    UNIT["record"], UNIT["changed"],
)

INSTALL_COLUMNS: Tuple[str, ...] = (
    INSTALL["id"], INSTALL["parent"], INSTALL["parent_serial"],
    INSTALL["child"], INSTALL["child_serial"], INSTALL["position"],
    INSTALL["valid_from"], INSTALL["valid_to"],
    INSTALL["record"], INSTALL["changed"],
)

# ---------------------------------------------------------------------------
# The test-data half. Column names are the two conformed mappings' declared
# `source.columns`, and the example's own test asserts that equality in both
# directions -- so a column added to a mapping fails here until it is seeded.

RESULT = {
    "table": "sl_results",
    "id": "id",
    "updated": "updated_at",
    "status": "status__status_type",
    "program": "program_name",
    "operator": "operator",
    "workspace": "workspace",
    "part": "part_number",
    "serial": "serial_number",
    "started": "started_at",
    "duration": "duration_s",
    "system": "system_id",
    "host": "host_name",
    "source_system": "_source_system",
    "extracted": "_extracted_at",
}

PARAMETER = {
    "table": "sl_steps__data__parameters",
    "result": "result_id",
    "step": "step_id",
    "step_path": "step_path",
    "step_name": "step_name",
    "name": "name",
    "measurement": "measurement",
    "units": "units",
    "limits": "limits",
    "comparator": "comparator",
    "status": "status",
    "index": "_dlt_list_idx",
    "record": "_dlt_id",
    "source_system": "_source_system",
    "extracted": "_extracted_at",
}

RESULT_COLUMNS: Tuple[str, ...] = (
    RESULT["id"], RESULT["updated"], RESULT["status"], RESULT["program"],
    RESULT["operator"], RESULT["workspace"], RESULT["part"], RESULT["serial"],
    RESULT["started"], RESULT["duration"], RESULT["system"], RESULT["host"],
    RESULT["source_system"], RESULT["extracted"],
)

PARAMETER_COLUMNS: Tuple[str, ...] = (
    PARAMETER["result"], PARAMETER["step"], PARAMETER["step_path"],
    PARAMETER["step_name"], PARAMETER["name"], PARAMETER["measurement"],
    PARAMETER["units"], PARAMETER["limits"], PARAMETER["comparator"],
    PARAMETER["status"], PARAMETER["index"], PARAMETER["record"],
    PARAMETER["source_system"], PARAMETER["extracted"],
)

#: Columns this fixture writes as something other than text.
NUMERIC_COLUMNS: Dict[str, str] = {RESULT["duration"]: "REAL"}
INTEGER_COLUMNS: Dict[str, str] = {PARAMETER["index"]: "INTEGER"}

# ---------------------------------------------------------------------------
# Values

#: Carried through unchanged, exactly as the source system writes it. Turning
#: it into a null is a `nullif` the curated layer declares.
OPEN_END = "9999-12-31T00:00:00+00:00"

BLANK = ""

#: The dispatch's sample names. The parts whose numbers begin PN-1000 and
#: PN-1100 belong to one program, PN-9000 to the other.
PROGRAM_A = "SAMPLE-PROGRAM-A"
PROGRAM_B = "SAMPLE-PROGRAM-B"

#: Synthetic, as the dispatch requires: the operator reference is already
#: pseudonymised where this fixture picks it up.
OPERATOR = "OP-0001"
WORKSPACE = "SAMPLE-WORKSPACE"
SYSTEM = "SYS-0001"
STATION = "TEST-STAND-01"
SOURCE_SYSTEM = "test_repo"
EXTRACTED_AT = "2024-04-03T00:00:00+00:00"

TOP_A = "PN-1000-A"
SUB_A = "PN-1100-A"
SUB_B = "PN-1100-B"
PART_A = "PN-1110-A"
TOP_B = "PN-9000-A"

REV_A = "A"
REV_B = "B"

EACH = "EA"
KIND_CHILD = "child"
KIND_SUBSTITUTE = "substitute"
POSITION = "0010"

STATE_COMPLETE = "COMPLETE"
STATE_REMOVED = "REMOVED"

#: Statuses this fixture's rows carry. The terminal ones are a subset of the
#: sample list in `../systemlink/component.yaml`; the non-terminal one is
#: deliberately absent from it.
STATUS_PASSED = "PASSED"
STATUS_FAILED = "FAILED"
STATUS_RUNNING = "RUNNING"

# ---------------------------------------------------------------------------
# One structural plant per change record, and the control.

PLANTS = {
    "multi_level": {"table": "bom", "key": "BOM-MULTI"},
    "substitute": {"table": "bom", "key": "BOM-SUBST"},
    "ref_designators_multi": {"table": "bom", "key": "BOM-REFDES"},
    "no_find_number": {"table": "bom", "key": "BOM-NOFIND"},
    "replacement": {"table": "install", "key": "INST-SWAP"},
    "installed_in_invisible_parent": {"table": "install", "key": "INST-HIDE"},
}

CONTROL_BOM = {"table": "bom", "key": "BOM-CTRL"}
CONTROL_INSTALL = {"table": "install", "key": "UNIT-CTRL"}

# ---------------------------------------------------------------------------
# One measurement plant per serial number, and the control.
#
# Every plant leaves the comparator column empty, so the operator travels in
# the limits text -- the case the grammar exists for. The CONTROL supplies a
# comparison code directly, which exercises the other path: a supplied value
# wins over anything the text would yield.

RESULT_PLANTS = {
    "tolerance": {"serial": "SN-1000-0011"},
    "comparator_low": {"serial": "SN-1000-0012"},
    "comparator_high": {"serial": "SN-9000-0010"},
    "comparator_uncovered": {"serial": "SN-1000-0013"},
    "bare_number_limits": {"serial": "SN-1000-0014"},
    "parse_failed": {"serial": "SN-1000-0015"},
    "empty_measurement": {"serial": "SN-1000-0016"},
    "empty_units": {"serial": "SN-1000-0017"},
    "repeated_parameter_name": {"serial": "SN-1000-0018"},
    "rerun": {"serial": "SN-1000-0019"},
    "non_terminal": {"serial": "SN-9000-0011"},
}

CONTROL_RESULT = {"serial": "SN-1000-0010"}

VOLTS = "V"


def _program_of(part: str) -> str:
    """Which program a part belongs to, by the dispatch's sample mapping."""
    return PROGRAM_B if part == TOP_B else PROGRAM_A


def _part_of(serial: str) -> str:
    """The part a serial number belongs to, by its own prefix."""
    return TOP_B if serial.startswith("SN-9000") else TOP_A


# ---------------------------------------------------------------------------
# Rows


def bom_rows() -> List[Tuple[Any, ...]]:
    """The as-designed structure. Only the control and the plants' own rows.

    Two of the edges share one change record on purpose: that is what gives
    the structure a second level, and a closure over it a depth of two.
    """
    multi = PLANTS["multi_level"]["key"]
    return [
        # The control: one plain edge, one level, one designator, a find
        # number, on its own revision of the top part.
        (TOP_A, REV_B, PART_A, REV_A, "1", EACH, "0020", "D1",
         KIND_CHILD, "2024-01-01T00:00:00+00:00", OPEN_END,
         CONTROL_BOM["key"], "2024-01-15T12:00:00+00:00"),
        (TOP_A, REV_A, SUB_A, REV_A, "1", EACH, POSITION, "A1",
         KIND_CHILD, "2024-01-01T00:00:00+00:00", OPEN_END,
         multi, "2024-01-15T12:00:00+00:00"),
        (SUB_A, REV_A, PART_A, REV_A, "2", EACH, POSITION, "B1",
         KIND_CHILD, "2024-01-01T00:00:00+00:00", OPEN_END,
         multi, "2024-01-15T12:00:00+00:00"),
        (TOP_A, REV_A, SUB_B, REV_A, "1", EACH, POSITION, "A1",
         KIND_SUBSTITUTE, "2024-01-01T00:00:00+00:00", OPEN_END,
         PLANTS["substitute"]["key"], "2024-01-16T12:00:00+00:00"),
        (TOP_B, REV_A, PART_A, REV_A, "3", EACH, POSITION, "C1 C2 C3",
         KIND_CHILD, "2024-01-01T00:00:00+00:00", OPEN_END,
         PLANTS["ref_designators_multi"]["key"], "2024-01-17T12:00:00+00:00"),
        # A real source writes an edge with neither a find number nor a
        # designator. Both are blank, not null: that is how it arrives.
        (TOP_B, REV_A, SUB_A, REV_A, "1", EACH, BLANK, BLANK,
         KIND_CHILD, "2024-01-01T00:00:00+00:00", OPEN_END,
         PLANTS["no_find_number"]["key"], "2024-01-18T12:00:00+00:00"),
    ]


def unit_rows() -> List[Tuple[Any, ...]]:
    """The serialized units that were built."""
    swap = PLANTS["replacement"]["key"]
    hide = PLANTS["installed_in_invisible_parent"]["key"]
    control = CONTROL_INSTALL["key"]
    return [
        (TOP_A, "SN-1000-0001", "2024-02-01T00:00:00+00:00", STATE_COMPLETE,
         control, "2024-02-01T08:00:00+00:00"),
        (PART_A, "SN-1110-0002", "2024-01-20T00:00:00+00:00", STATE_COMPLETE,
         control, "2024-01-20T08:00:00+00:00"),
        (TOP_A, "SN-1000-0002", "2024-02-02T00:00:00+00:00", STATE_COMPLETE,
         swap, "2024-02-02T08:00:00+00:00"),
        # Removed from its parent and never reinstalled. The unit still
        # exists; what changed is where it is.
        (SUB_A, "SN-1100-0001", "2024-01-21T00:00:00+00:00", STATE_REMOVED,
         swap, "2024-03-01T08:00:00+00:00"),
        (SUB_B, "SN-1100-0002", "2024-02-28T00:00:00+00:00", STATE_COMPLETE,
         swap, "2024-03-01T08:00:00+00:00"),
        (TOP_B, "SN-9000-0001", "2024-02-03T00:00:00+00:00", STATE_COMPLETE,
         hide, "2024-02-03T08:00:00+00:00"),
        (PART_A, "SN-1110-0001", "2024-01-22T00:00:00+00:00", STATE_COMPLETE,
         hide, "2024-01-22T08:00:00+00:00"),
    ]


def install_rows() -> List[Tuple[Any, ...]]:
    """Where each unit was installed.

    The replacement is two rows: the first closed on the day the second
    opens, at the same position of the same parent unit. Read as current,
    only the second is installed; read as built, both happened.
    """
    swap = PLANTS["replacement"]["key"]
    return [
        ("INS-0001", TOP_A, "SN-1000-0001", PART_A, "SN-1110-0002", POSITION,
         "2024-02-01T00:00:00+00:00", OPEN_END,
         CONTROL_INSTALL["key"], "2024-02-01T08:00:00+00:00"),
        ("INS-0002", TOP_A, "SN-1000-0002", SUB_A, "SN-1100-0001", POSITION,
         "2024-02-02T00:00:00+00:00", "2024-03-01T00:00:00+00:00",
         swap, "2024-03-01T08:00:00+00:00"),
        ("INS-0003", TOP_A, "SN-1000-0002", SUB_B, "SN-1100-0002", POSITION,
         "2024-03-01T00:00:00+00:00", OPEN_END,
         swap, "2024-03-01T08:00:00+00:00"),
        # The parent part is outside the part list the sample viewer reads,
        # so this unit is installed somewhere that viewer cannot show.
        ("INS-0004", TOP_B, "SN-9000-0001", PART_A, "SN-1110-0001", POSITION,
         "2024-02-03T00:00:00+00:00", OPEN_END,
         PLANTS["installed_in_invisible_parent"]["key"],
         "2024-02-03T08:00:00+00:00"),
    ]


def _result(result_id, serial, status, started, updated, duration):
    part = _part_of(serial)
    return (
        result_id, updated, status, _program_of(part), OPERATOR, WORKSPACE,
        part, serial, started, duration, SYSTEM, STATION,
        SOURCE_SYSTEM, EXTRACTED_AT,
    )


def result_rows() -> List[Tuple[Any, ...]]:
    """One row per test result. One serial number is tested twice."""
    p = RESULT_PLANTS
    return [
        _result("RES-0001", CONTROL_RESULT["serial"], STATUS_PASSED,
                "2024-04-01T08:00:00+00:00", "2024-04-01T08:02:00+00:00", 120.0),
        _result("RES-0002", p["tolerance"]["serial"], STATUS_PASSED,
                "2024-04-01T08:10:00+00:00", "2024-04-01T08:12:00+00:00", 121.5),
        _result("RES-0003", p["comparator_low"]["serial"], STATUS_PASSED,
                "2024-04-01T08:20:00+00:00", "2024-04-01T08:22:00+00:00", 119.0),
        _result("RES-0004", p["comparator_high"]["serial"], STATUS_PASSED,
                "2024-04-01T08:30:00+00:00", "2024-04-01T08:32:00+00:00", 118.25),
        _result("RES-0005", p["comparator_uncovered"]["serial"], STATUS_PASSED,
                "2024-04-01T08:40:00+00:00", "2024-04-01T08:42:00+00:00", 122.0),
        _result("RES-0006", p["bare_number_limits"]["serial"], STATUS_PASSED,
                "2024-04-01T08:50:00+00:00", "2024-04-01T08:52:00+00:00", 120.5),
        _result("RES-0007", p["parse_failed"]["serial"], STATUS_FAILED,
                "2024-04-01T09:00:00+00:00", "2024-04-01T09:02:00+00:00", 117.0),
        _result("RES-0008", p["empty_measurement"]["serial"], STATUS_PASSED,
                "2024-04-01T09:10:00+00:00", "2024-04-01T09:12:00+00:00", 120.0),
        _result("RES-0009", p["empty_units"]["serial"], STATUS_PASSED,
                "2024-04-01T09:20:00+00:00", "2024-04-01T09:22:00+00:00", 120.0),
        _result("RES-0010", p["repeated_parameter_name"]["serial"], STATUS_PASSED,
                "2024-04-01T09:30:00+00:00", "2024-04-01T09:32:00+00:00", 123.0),
        # The rerun: the same unit tested twice, the second attempt later and
        # with a different outcome. Which one is latest is the curated
        # layer's to decide, and it decides it on the terminal attempts.
        _result("RES-0011", p["rerun"]["serial"], STATUS_PASSED,
                "2024-04-02T08:00:00+00:00", "2024-04-02T08:02:00+00:00", 120.0),
        _result("RES-0012", p["rerun"]["serial"], STATUS_FAILED,
                "2024-04-02T09:28:00+00:00", "2024-04-02T09:30:00+00:00", 125.75),
        # Still running when the extraction ran: its status is not in the
        # example's terminal list, and it has no parameter rows at all.
        _result("RES-0013", p["non_terminal"]["serial"], STATUS_RUNNING,
                "2024-04-02T10:00:00+00:00", "2024-04-02T10:00:30+00:00", 30.0),
    ]


def _parameter(result_id, step, index, name, measurement, units, limits,
               comparator, status):
    return (
        result_id, step, "Main/" + step, step, name, measurement, units,
        limits, comparator, status, index,
        result_id + "-" + step + "-" + str(index),
        SOURCE_SYSTEM, EXTRACTED_AT,
    )


def parameter_rows() -> List[Tuple[Any, ...]]:
    """One row per parameter measured in a step.

    The limits text is where each measurement plant lives. Nothing here
    states what a row should parse to: the example's test parses these rows
    through the library's own grammar against the deployment's own comparator
    table, which is the only reading that proves anything.
    """
    return [
        # The control. Its comparison code is supplied, so the limits text
        # does not have to be read to get one -- the other path.
        _parameter("RES-0001", "STEP-01", 0, "VOUT", "12.0", VOLTS,
                   "≤ 50", "LTE", STATUS_PASSED),
        _parameter("RES-0002", "STEP-01", 0, "VOUT", "1.006", VOLTS,
                   "1.005 ± 0.005 V", BLANK, STATUS_PASSED),
        _parameter("RES-0003", "STEP-01", 0, "VOUT", "4.2", VOLTS,
                   ">= 3", BLANK, STATUS_PASSED),
        _parameter("RES-0004", "STEP-01", 0, "VOUT", "12.5", VOLTS,
                   "< 50", BLANK, STATUS_PASSED),
        # An operator the deployment's comparator table does not carry.
        _parameter("RES-0005", "STEP-01", 0, "VOUT", "5.0", VOLTS,
                   "== 5", BLANK, STATUS_PASSED),
        # Limits text that is a bare number: it bounds neither side.
        _parameter("RES-0006", "STEP-01", 0, "VOUT", "7.1", VOLTS,
                   "7", BLANK, STATUS_PASSED),
        # A measurement that is not a number, against limits that are.
        _parameter("RES-0007", "STEP-01", 0, "VOUT", "OPEN", VOLTS,
                   ">= 3", BLANK, STATUS_FAILED),
        # Nothing was measured and nothing was bounded. Not a parse failure:
        # no number was expected.
        _parameter("RES-0008", "STEP-01", 0, "VOUT", BLANK, VOLTS,
                   BLANK, BLANK, STATUS_PASSED),
        _parameter("RES-0009", "STEP-01", 0, "VOUT", "4.2", BLANK,
                   ">= 3", BLANK, STATUS_PASSED),
        # One step, two parameters of the same name: the index is what tells
        # them apart, which is why it is part of the grain.
        _parameter("RES-0010", "STEP-01", 0, "VOUT", "4.2", VOLTS,
                   ">= 3", BLANK, STATUS_PASSED),
        _parameter("RES-0010", "STEP-01", 1, "VOUT", "4.3", VOLTS,
                   ">= 3", BLANK, STATUS_PASSED),
        _parameter("RES-0011", "STEP-01", 0, "VOUT", "4.2", VOLTS,
                   ">= 3", BLANK, STATUS_PASSED),
        _parameter("RES-0012", "STEP-01", 0, "VOUT", "2.9", VOLTS,
                   ">= 3", BLANK, STATUS_FAILED),
    ]


# ---------------------------------------------------------------------------
# Writing it


def _column_type(name: str) -> str:
    if name in NUMERIC_COLUMNS:
        return NUMERIC_COLUMNS[name]
    if name in INTEGER_COLUMNS:
        return INTEGER_COLUMNS[name]
    return "TEXT"


def _create(table: str, columns: Tuple[str, ...]) -> str:
    declared = ", ".join(f"{name} {_column_type(name)}" for name in columns)
    return f"CREATE TABLE {table} ({declared})"


def _insert(table: str, columns: Tuple[str, ...]) -> str:
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {table} VALUES ({placeholders})"


#: Every relation, with the columns it declares and the rows it carries.
RELATIONS: Tuple[Tuple[Dict[str, Any], Tuple[str, ...], Any], ...] = (
    (BOM, BOM_COLUMNS, bom_rows),
    (UNIT, UNIT_COLUMNS, unit_rows),
    (INSTALL, INSTALL_COLUMNS, install_rows),
    (RESULT, RESULT_COLUMNS, result_rows),
    (PARAMETER, PARAMETER_COLUMNS, parameter_rows),
)


def _ddl() -> Iterator[str]:
    for relation, columns, _rows in RELATIONS:
        yield _create(relation["table"], columns)


def seed(db_path=None) -> str:
    """Create every relation with its rows. Returns the db path."""
    path = str(db_path or DEFAULT_DB)
    if os.path.exists(path):
        os.remove(path)
    connection = sqlite3.connect(path)
    try:
        for statement in _ddl():
            connection.execute(statement)
        for relation, columns, rows in RELATIONS:
            connection.executemany(
                _insert(relation["table"], columns), rows()
            )
        connection.commit()
    finally:
        connection.close()
    return path


if __name__ == "__main__":
    target = seed(sys.argv[1] if len(sys.argv) > 1 else None)
    print(f"seeded {target}")
    print()
    print("A stage-1 mapping reads it as a sql_database source:")
    print(f"  sqlite:///{Path(target).as_posix()}")
