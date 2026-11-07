"""Build the sqlite stand-in warehouse this example observes.

The source is one table with five columns and a handful of rows. The point of
the fixture is what the observation does *not* do with it: it counts the rows
and names the columns, and no row of it ever reaches Dagster, the catalog or
this process's memory.

One column is deliberately left out of the component's `columns:` list, so the
example shows deny-by-default working -- it is published as most restrictive
in every dimension, and the materialization says so as a warning and a number.

Every name in here belongs to this example's sqlite file. A real deployment's
relation and column names live in its own YAML; dag-tools names none of them.
"""
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

SOURCE_RELATION = "party_extract"
DEFAULT_DB = Path(__file__).resolve().parent / "source.db"


def rows():
    """Five rows, and one of them carries the watermark the as-of reads."""
    now = datetime.now(timezone.utc).replace(microsecond=0)

    def at(days):
        return (now - timedelta(days=days)).isoformat(" ")

    return [
        (1, "ref-0001", "open", 1200, at(4)),
        (2, "ref-0002", "open", 450, at(3)),
        (3, "ref-0003", "closed", 0, at(2)),
        (4, "ref-0004", "open", 98000, at(1)),
        (5, "ref-0005", "pending", 17, at(0)),
    ]


def seed(db_path=None) -> str:
    """Create the source relation and its rows. Returns the db path."""
    path = str(db_path or DEFAULT_DB)
    if os.path.exists(path):
        os.remove(path)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            f"CREATE TABLE {SOURCE_RELATION} ("
            "party_id INTEGER, external_ref TEXT, state TEXT, "
            "exposure INTEGER, loaded_at TEXT)"
        )
        connection.executemany(
            f"INSERT INTO {SOURCE_RELATION} VALUES (?, ?, ?, ?, ?)", rows()
        )
        connection.commit()
    finally:
        connection.close()
    return path


if __name__ == "__main__":
    target = seed(sys.argv[1] if len(sys.argv) > 1 else None)
    here = Path(__file__).resolve().parent
    print(f"seeded {target}")
    print()
    print("Now point the example at it and start Dagster:")
    print(f'  export WAREHOUSE_SOURCE_DB="{target}"')
    print(
        '  export DAGTOOLS_CLASSIFICATION_LEVELS='
        f'"{here / "classification" / "dimensions.yaml"}"'
    )
    print("  cd dagster_home && dagster dev")
