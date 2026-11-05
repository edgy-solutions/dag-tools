"""Build the sqlite stand-in warehouse this example reads.

sqlite has no query history of its own, so the example brings its own: one
table shaped like a warehouse's history view, with three rows that between
them cover the three outcomes worth seeing.

**The tagged row's tag is produced by the resource, not typed by hand.** That
is the whole point of the fixture: the bytes the resource would send are the
bytes the report decodes, so if the two halves of the attribution contract
ever drift apart, this example stops explaining its read -- loudly, as a
number.

Run it directly, or import `seed` from a test. Every name in here belongs to
this example's sqlite file; a real deployment's names live in its own YAML.
"""
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dag_tools.audit.warehouse import COMMENT  # noqa: E402
from dag_tools.resources.warehouse import TaggedWarehouseResource  # noqa: E402

PIPELINE_NAME = "read_audit_demo"
HISTORY_RELATION = "query_history"
DEFAULT_DB = Path(__file__).resolve().parent / "warehouse.db"


def demo_tag(asset_key: str, run_id: str) -> str:
    """The tag the shipped resource really sends for that asset and run.

    Read off a plan the resource built, so the fixture cannot drift from the
    sender: if the plan comes out untagged, this fails here rather than
    producing a fixture that quietly explains nothing.
    """
    from dag_tools.audit.tag import encode

    resource = TaggedWarehouseResource(
        tag_placement=COMMENT,
        pipeline_name=PIPELINE_NAME,
        drivername="sqlite",
        database=str(DEFAULT_DB),
    )
    attribution = resource.attribution(asset_key=asset_key, run_id=run_id)
    plan = resource.plan("SELECT 1", asset_key=asset_key)
    if not plan.carries_tag:
        raise AssertionError("the shipped resource planned an untagged query")
    return encode(attribution)


def rows():
    """Three history rows: one explained, one untagged, one not ours."""
    now = datetime.now(timezone.utc).replace(microsecond=0)

    def at(minutes):
        return (now - timedelta(minutes=minutes)).isoformat(" ")

    return [
        # Issued through the tagged resource: the report names the asset
        # directly, with no time correlation and no guessing.
        (
            "q-1",
            at(30),
            f"pipeline:{PIPELINE_NAME}",
            demo_tag("orders_summary", "run-aaa"),
            json.dumps(["analytics.public.orders", "analytics.public.customers"]),
        ),
        # A read with no tag at all -- a pipeline not yet on the resource.
        # This is the number step 2 exists to drive to zero.
        (
            "q-2",
            at(20),
            f"pipeline:{PIPELINE_NAME}",
            None,
            json.dumps(["analytics.public.payments"]),
        ),
        # Somebody else's tag: it decodes, but it names a kind this contract
        # does not know, so it is denied rather than attributed. History is
        # full of these, and guessing one onto an asset would be worse than
        # counting it as unexplained.
        (
            "q-3",
            at(10),
            "bi_service_account",
            json.dumps({"principal": "dashboard:weekly", "schema_version": 1}),
            json.dumps(["analytics.public.revenue_daily"]),
        ),
    ]


def seed(db_path=None) -> str:
    """Create the history table and its three rows. Returns the db path."""
    path = str(db_path or DEFAULT_DB)
    if os.path.exists(path):
        os.remove(path)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            f"CREATE TABLE {HISTORY_RELATION} ("
            "query_id TEXT, started_at TEXT, principal TEXT, "
            "query_tag TEXT, objects_read TEXT)"
        )
        connection.executemany(
            f"INSERT INTO {HISTORY_RELATION} VALUES (?, ?, ?, ?, ?)", rows()
        )
        connection.commit()
    finally:
        connection.close()
    return path


if __name__ == "__main__":
    target = seed(sys.argv[1] if len(sys.argv) > 1 else None)
    print(f"seeded {target}")
    print()
    print("Now point the example at it and start Dagster:")
    print(f'  export TAGGED_WAREHOUSE_DB="{target}"')
    print("  cd dagster_home && dagster dev")
