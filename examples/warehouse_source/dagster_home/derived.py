"""The derived table whose labels are composed from what it read.

This is the one piece of the example that is NOT a dag-tools component, and
deliberately so: what builds a derived dataset is the deployment's own
business -- dbt, dlt, a Python asset like this one. What dag-tools contributes
is the component next door, which reads this asset's **column lineage** and
publishes a label per output column.

The aggregate is a real read of the source, through the tagged warehouse
resource, so it lands in query history attributed to this asset like every
other read. The write is this example's own business and goes through sqlite
directly, exactly as `seed.py` does.

Three columns, chosen to show the two bases and one downgrade:

  * `state` reads the source's `state`, which is labelled openly.
  * `exposure_total` reads the source's `exposure`, which is labelled
    restricted -- and carries a declared downgrade in the component's YAML,
    because a state-level total is not a party-level exposure.
  * `party_count` declares NO lineage. It therefore publishes the whole
    table's ceiling, which here includes the unlabelled `loaded_at` column's
    deny-by-default label. That is the fallback working, and it is also what
    declaring one more lineage entry would buy.

Every relation and column name here belongs to this example's sqlite file.
"""
import os
import sqlite3

from dagster import (
    AssetKey,
    MaterializeResult,
    MetadataValue,
    TableColumn,
    TableColumnDep,
    TableColumnLineage,
    TableSchema,
    asset,
)

from dag_tools.resources.warehouse import TaggedWarehouseResource

SOURCE_RELATION = "party_extract"
DERIVED_RELATION = "party_exposure"

AGGREGATE = (
    "SELECT state, sum(exposure) AS exposure_total, count(*) AS party_count "
    f"FROM {SOURCE_RELATION} GROUP BY state"
)


@asset(
    key=["marts", DERIVED_RELATION],
    deps=[AssetKey(SOURCE_RELATION)],
    group_name="classified_marts",
    description="State-level exposure, aggregated from the observed source.",
)
def party_exposure(context, warehouse: TaggedWarehouseResource) -> MaterializeResult:
    rows = warehouse.for_asset(context).fetch_rows(AGGREGATE)

    # The write is the example's, not the library's. It reaches the same file
    # the resource is pointed at, which in a real deployment would be the
    # warehouse your own loader writes to.
    connection = sqlite3.connect(os.environ["WAREHOUSE_SOURCE_DB"])
    try:
        connection.execute(f"DROP TABLE IF EXISTS {DERIVED_RELATION}")
        connection.execute(
            f"CREATE TABLE {DERIVED_RELATION} ("
            "state TEXT, exposure_total INTEGER, party_count INTEGER)"
        )
        connection.executemany(
            f"INSERT INTO {DERIVED_RELATION} VALUES (?, ?, ?)",
            [
                (row["state"], row["exposure_total"], row["party_count"])
                for row in rows
            ],
        )
        connection.commit()
    finally:
        connection.close()

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(len(rows)),
            # What the propagation reads. Both keys are Dagster's own.
            "dagster/column_schema": TableSchema(
                columns=[
                    TableColumn(name="state"),
                    TableColumn(name="exposure_total"),
                    TableColumn(name="party_count"),
                ]
            ),
            "dagster/column_lineage": TableColumnLineage(
                deps_by_column={
                    "state": [
                        TableColumnDep(
                            asset_key=AssetKey(SOURCE_RELATION),
                            column_name="state",
                        )
                    ],
                    "exposure_total": [
                        TableColumnDep(
                            asset_key=AssetKey(SOURCE_RELATION),
                            column_name="exposure",
                        )
                    ],
                    # `party_count` is left out on purpose. See the docstring.
                }
            ),
        }
    )
