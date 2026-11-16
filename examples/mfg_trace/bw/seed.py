"""Build the sqlite stand-in for a BW-shaped source this example extracts.

Two tables: a master-data-shaped one (versioned, time-dependent, with a blank
key row) and an as-built-shaped one (one row per installed component
instance). Each thing the extraction side has to get right is planted on its
OWN key, and one more key is planted with nothing, so a fixture that is
broken everywhere is distinguishable from one that is planted somewhere.

There are two loads. `seed()` writes the first; `apply_second_load()` is what
changed in the source before the next extraction. History (scd2) is only
observable across the two.

Every name in here belongs to this example's sqlite file. A real deployment's
relation and column names live in its own YAML; dag-tools names none of them.
The roles below (`key`, `version`, ...) are this file's own labels, so the
example's tests can find a column without restating it.

Nothing here is the curated layer's job done early: the blank-key row and the
sentinel end date are planted and are expected to reach raw unchanged.
"""
import os
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = Path(__file__).resolve().parent / "source.db"

MASTER = {
    "table": "BW_MATERIAL_ATTR",
    "key": "MATNR",
    "version": "OBJVERS",
    "valid_from": "DATEFROM",
    "valid_to": "DATETO",
    "text": "MATDESC",
}
ASBUILT = {
    "table": "BW_IBASE_COMP",
    "instance": "IB_INSTANCE",
    "position": "IB_POSITION",
    "part": "IB_PART",
    "serial": "IB_SERIAL",
    "valid_from": "IB_FROM",
    "valid_to": "IB_TO",
}

ACTIVE = "A"
INACTIVE = "M"
SENTINEL = "99991231"
BLANK = ""

# One key per plant, and the control. `table` says which relation carries it.
PLANTS = {
    "version_mix": {"table": "master", "key": "MAT-VERS"},
    "open_end_sentinel": {"table": "master", "key": "MAT-OPEN"},
    "blank_key": {"table": "master", "key": BLANK},
    "absent_from_second_load": {"table": "asbuilt", "key": "INS-GONE"},
    "replaced_child": {"table": "asbuilt", "key": "INS-SWAP"},
}
CONTROL = {"table": "master", "key": "MAT-CTRL"}
CONTROL_ASBUILT = {"table": "asbuilt", "key": "INS-CTRL"}

POSITION = "0010"


def master_rows():
    """(key, version, from, to, text). Only the control and the plants' own rows."""
    return [
        ("MAT-CTRL", ACTIVE, "20200101", "20991231", "control"),
        ("MAT-VERS", ACTIVE, "20200101", "20991231", "current"),
        ("MAT-VERS", INACTIVE, "20190101", "20191231", "superseded"),
        ("MAT-OPEN", ACTIVE, "20200101", SENTINEL, "open ended"),
        (BLANK, ACTIVE, "20200101", "20991231", "blank key"),
    ]


def asbuilt_rows_first():
    return [
        ("INS-CTRL", POSITION, "PART-C", "SN-C", "20240101", "20991231"),
        ("INS-GONE", POSITION, "PART-G", "SN-G", "20240101", "20991231"),
        ("INS-SWAP", POSITION, "PART-OLD", "SN-OLD", "20240101", "20991231"),
    ]


def _ddl():
    m, a = MASTER, ASBUILT
    yield (
        f"CREATE TABLE {m['table']} ({m['key']} TEXT, {m['version']} TEXT, "
        f"{m['valid_from']} TEXT, {m['valid_to']} TEXT, {m['text']} TEXT)"
    )
    yield (
        f"CREATE TABLE {a['table']} ({a['instance']} TEXT, {a['position']} TEXT, "
        f"{a['part']} TEXT, {a['serial']} TEXT, {a['valid_from']} TEXT, "
        f"{a['valid_to']} TEXT)"
    )


def seed(db_path=None) -> str:
    """Create both relations with the first load. Returns the db path."""
    path = str(db_path or DEFAULT_DB)
    if os.path.exists(path):
        os.remove(path)
    connection = sqlite3.connect(path)
    try:
        for statement in _ddl():
            connection.execute(statement)
        connection.executemany(
            f"INSERT INTO {MASTER['table']} VALUES (?, ?, ?, ?, ?)", master_rows()
        )
        connection.executemany(
            f"INSERT INTO {ASBUILT['table']} VALUES (?, ?, ?, ?, ?, ?)",
            asbuilt_rows_first(),
        )
        connection.commit()
    finally:
        connection.close()
    return path


def apply_second_load(db_path=None) -> str:
    """What changed before the next extraction: one child gone, one replaced."""
    path = str(db_path or DEFAULT_DB)
    a = ASBUILT
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            f"DELETE FROM {a['table']} WHERE {a['instance']} = ?",
            (PLANTS["absent_from_second_load"]["key"],),
        )
        connection.execute(
            f"UPDATE {a['table']} SET {a['part']} = 'PART-NEW', {a['serial']} = 'SN-NEW' "
            f"WHERE {a['instance']} = ?",
            (PLANTS["replaced_child"]["key"],),
        )
        connection.commit()
    finally:
        connection.close()
    return path


if __name__ == "__main__":
    target = seed(sys.argv[1] if len(sys.argv) > 1 else None)
    print(f"seeded {target}")
    print()
    print("Now point the component at it:")
    print(f'  export BW_EXAMPLE_SOURCE_DB="sqlite:///{Path(target).as_posix()}"')
    print('  export BW_EXAMPLE_DB="bw_example.duckdb"')
