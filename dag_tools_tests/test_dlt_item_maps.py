"""Backend-shape handling for the dlt `add_map` transforms.

`add_map` fires once per item a resource yields, and the item's shape is a
property of the backend, not of the transform: `sqlalchemy` yields one dict
per row, while `pyarrow` and the filesystem + `read_parquet` path yield an
entire chunk as a table or a record batch.

Both transforms used to assume the dict shape, so turning on `add_timestamp`
or `select_columns` for an arrow-shaped source failed the whole extract step
with `'pyarrow.lib.Table' object is not a mapping`. These pin every shape.
"""
import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa

from dag_tools.asset_wrappers.dlt_assets_factory import (
    add_timestamp_f,
    select_columns_f,
)


# ---------------------------------------------------------------------------
# add_timestamp_f
# ---------------------------------------------------------------------------


def test_timestamp_on_dict():
    out = add_timestamp_f({"id": 1})
    assert out["id"] == 1
    assert out["_updated_at"].tzinfo is not None


def test_timestamp_on_list_of_dicts():
    out = add_timestamp_f([{"id": 1}, {"id": 2}])
    assert [r["id"] for r in out] == [1, 2]
    assert all(r["_updated_at"].tzinfo is not None for r in out)


def test_timestamp_on_arrow_table():
    out = add_timestamp_f(pa.table({"id": [1, 2, 3]}))
    assert out.column_names == ["id", "_updated_at"]
    assert out.num_rows == 3
    assert out.schema.field("_updated_at").type == pa.timestamp("us", tz="UTC")


def test_timestamp_on_record_batch():
    batch = pa.record_batch([pa.array([1, 2])], names=["id"])
    out = add_timestamp_f(batch)
    assert out.schema.names == ["id", "_updated_at"]
    assert out.num_rows == 2


def test_timestamp_overwrites_existing_arrow_column():
    """append_column would leave two `_updated_at` columns and fail normalization."""
    stale = pa.array([0, 0], type=pa.timestamp("us", tz="UTC"))
    out = add_timestamp_f(pa.table({"id": [1, 2], "_updated_at": stale}))
    assert out.column_names == ["id", "_updated_at"]
    assert out.column("_updated_at")[0].as_py().year > 2000


def test_timestamp_rejects_unknown_shape():
    """Silently passing the item through would drop the column with no error."""
    with pytest.raises(TypeError, match="cannot stamp"):
        add_timestamp_f("not an item")


# ---------------------------------------------------------------------------
# select_columns_f
# ---------------------------------------------------------------------------


def test_select_on_dict():
    assert select_columns_f({"a": 1, "b": 2, "c": 3}, ["a", "c"]) == {"a": 1, "c": 3}


def test_select_on_arrow_table():
    out = select_columns_f(pa.table({"a": [1], "b": [2], "c": [3]}), ["a", "c"])
    assert out.column_names == ["a", "c"]


def test_select_ignores_missing_columns():
    """A column absent from the source is skipped, not an error — as for dicts."""
    out = select_columns_f(pa.table({"a": [1]}), ["a", "nope"])
    assert out.column_names == ["a"]


def test_select_without_columns_is_identity():
    table = pa.table({"a": [1]})
    assert select_columns_f(table, None) is table
