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

# dlt_assets_factory imports dlt at module scope, so without this the
# module raises ModuleNotFoundError at COLLECTION time, which aborts the
# whole pytest run rather than skipping one file. That is what took the
# dagster-floor CI job to zero tests.
pytest.importorskip("dlt")

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


# ---------------------------------------------------------------------------
# Through dlt's MapItem -- the ONLY way these are called in production
# ---------------------------------------------------------------------------
#
# Everything above calls the transform functions directly, which is how the
# arrow-shape work got merged with this whole class of failure intact. dlt
# picks its calling convention by COUNTING parameters:
#
#     if len(sig.parameters) == 1:  self._f = transform_f
#     else:                         self._f_meta = transform_f
#
# so a two-parameter function is invoked as f(item, meta) and its second
# argument -- `column`, or `cols` in the old lambda -- is silently bound to
# meta, normally None. Direct-call tests cannot see any of it.

import inspect

from dlt.extract.items_transform import MapItem

from dag_tools.asset_wrappers.dlt_assets_factory import (
    make_add_timestamp,
    make_select_columns,
)


def test_the_stamper_handed_to_add_map_takes_exactly_one_argument():
    """The whole bug in one assertion. Two parameters and dlt overwrites
    the second with meta."""
    assert len(inspect.signature(make_add_timestamp()).parameters) == 1


def test_the_column_filter_handed_to_add_map_takes_exactly_one_argument():
    assert len(inspect.signature(make_select_columns(["a"])).parameters) == 1


def test_arrow_stamping_survives_a_real_map_item():
    """Reproduces the production traceback:
    `get_field_index(None)` -> TypeError: expected bytes, NoneType found."""
    out = MapItem(make_add_timestamp())(pa.table({"id": [1, 2]}), meta=None)
    assert "_updated_at" in out.schema.names
    assert None not in out.schema.names


def test_dict_stamping_through_map_item_names_the_column():
    """The quieter half: no crash, just a column literally named None,
    which reaches the destination schema as one."""
    out = MapItem(make_add_timestamp())({"id": 1}, meta=None)
    assert "_updated_at" in out
    assert None not in out


def test_a_custom_column_name_is_not_clobbered_by_meta():
    out = MapItem(make_add_timestamp("_ingested_at"))({"id": 1}, meta=None)
    assert "_ingested_at" in out


def test_column_selection_actually_filters_through_map_item():
    """The quietest failure of the three: cols bound to meta hit
    select_columns_f's `if not select_columns: return doc` guard, so every
    configured selection passed every column through untouched."""
    out = MapItem(make_select_columns(["a", "c"]))({"a": 1, "b": 2, "c": 3}, meta=None)
    assert out == {"a": 1, "c": 3}


def test_arrow_column_selection_through_map_item():
    out = MapItem(make_select_columns(["a", "c"]))(
        pa.table({"a": [1], "b": [2], "c": [3]}), meta=None
    )
    assert out.schema.names == ["a", "c"]


def test_map_item_passes_a_list_of_rows_through_element_by_element():
    """MapItem unwraps lists itself, so each element arrives alone."""
    out = MapItem(make_add_timestamp())([{"id": 1}, {"id": 2}], meta=None)
    assert all("_updated_at" in row for row in out)
