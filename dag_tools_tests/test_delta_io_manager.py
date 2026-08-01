"""DeltaIOManager write/read round trips.

These exist because the write path silently stopped working: it was
written against an older deltalake, the library evolved, and the tests
that would have caught it were skipping because ``deltalake`` -- a
declared dependency -- was not installed in the dev environment. A
skipped test and a passing test look the same in a summary line.

deltalake 1.x changed three things about ``write_deltalake``:

  * ``overwrite_schema=`` became ``schema_mode=``
  * ``filesystem=`` was removed outright
  * ``schema=`` was removed outright

Reads were NOT changed -- ``to_pandas`` / ``to_pyarrow_table`` /
``to_pyarrow_dataset`` all still take ``filesystem`` -- so the PyArrow
filesystem backends still matter for reading and caching, and only
writes had to move to ``storage_options``.
"""
import pytest

pytest.importorskip("deltalake")
pytest.importorskip("pyarrow")
pytest.importorskip("polars")
pytest.importorskip("pandas")

import pandas as pd
import polars as pl
import pyarrow as pa
from pyarrow import dataset as ds
from dagster import AssetKey, Definitions, asset, materialize

from dag_tools.io_managers.delta import (
    ConfigurableDeltaIOManager,
    DeltaIOManager,
    LocalFSConfig,
    S3FSCommonConfig,
    S3FSConfig,
)


def _iom(tmp_path) -> ConfigurableDeltaIOManager:
    return ConfigurableDeltaIOManager(fs=LocalFSConfig(), uri_base=str(tmp_path))


def _read_back(tmp_path, name="t"):
    from deltalake import DeltaTable

    root = next(p for p in tmp_path.rglob("_delta_log")).parent
    return DeltaTable(str(root)).to_pyarrow_table()


# ---------------------------------------------------------------------------
# Write: every input shape the manager claims to accept
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind",
    ["pandas", "arrow_table", "arrow_batch", "arrow_reader", "arrow_dataset",
     "polars_df", "polars_lazy"],
)
def test_write_accepts_every_declared_type(tmp_path, kind):
    table = pa.table({"id": [1, 2, 3], "region": ["a", "b", "c"]})
    payloads = {
        "pandas": lambda: table.to_pandas(),
        "arrow_table": lambda: table,
        "arrow_batch": lambda: table.to_batches()[0],
        "arrow_reader": lambda: pa.RecordBatchReader.from_batches(
            table.schema, table.to_batches(1)
        ),
        "arrow_dataset": lambda: ds.dataset(table),
        "polars_df": lambda: pl.DataFrame({"id": [1, 2, 3], "region": ["a", "b", "c"]}),
        "polars_lazy": lambda: pl.LazyFrame({"id": [1, 2, 3], "region": ["a", "b", "c"]}),
    }

    @asset(name="t", io_manager_key="iom")
    def t():
        return payloads[kind]()

    result = materialize([t], resources={"iom": _iom(tmp_path)})
    assert result.success, kind
    out = _read_back(tmp_path)
    assert out.num_rows == 3
    assert set(out.column_names) == {"id", "region"}


def test_streaming_reader_is_not_drained_by_the_row_count_log(tmp_path):
    """The write logs a row count, but a RecordBatchReader is one-shot and
    has no length -- calling len() on it raised TypeError, and consuming it
    to count would leave nothing to write. Same defect that was in the
    Arrow manager."""
    table = pa.table({"id": list(range(100))})

    @asset(name="t", io_manager_key="iom")
    def t():
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(10))

    assert materialize([t], resources={"iom": _iom(tmp_path)}).success
    assert _read_back(tmp_path).num_rows == 100


def test_unsupported_type_fails_loudly(tmp_path):
    """Silently dropping data would be far worse than an error."""
    @asset(name="t", io_manager_key="iom")
    def t():
        return {"not": "a frame"}

    result = materialize([t], resources={"iom": _iom(tmp_path)}, raise_on_error=False)
    assert not result.success


# ---------------------------------------------------------------------------
# Schema evolution -- what schema_mode='overwrite' buys
# ---------------------------------------------------------------------------


def test_rewrite_with_a_changed_schema(tmp_path):
    """A later materialization may not have the same columns. The old
    version stays in the transaction log, so time travel still works."""
    from deltalake import DeltaTable

    state = {"cols": {"id": [1, 2]}}

    @asset(name="t", io_manager_key="iom")
    def t():
        return pa.table(state["cols"])

    iom = _iom(tmp_path)
    assert materialize([t], resources={"iom": iom}).success

    state["cols"] = {"id": [1, 2], "extra": ["x", "y"]}
    assert materialize([t], resources={"iom": iom}).success

    root = next(p for p in tmp_path.rglob("_delta_log")).parent
    assert set(DeltaTable(str(root)).to_pyarrow_table().column_names) == {"id", "extra"}
    # The pre-evolution snapshot is still reachable.
    assert DeltaTable(str(root), version=0).to_pyarrow_table().column_names == ["id"]


def test_rewrite_replaces_rather_than_appends(tmp_path):
    @asset(name="t", io_manager_key="iom")
    def t():
        return pa.table({"id": [1, 2, 3]})

    iom = _iom(tmp_path)
    materialize([t], resources={"iom": iom})
    materialize([t], resources={"iom": iom})
    assert _read_back(tmp_path).num_rows == 3


# ---------------------------------------------------------------------------
# Round trip: the manager reads back what it wrote, as the declared type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "declared,check",
    [
        (pd.DataFrame, lambda v: isinstance(v, pd.DataFrame) and len(v) == 3),
        (pa.Table, lambda v: isinstance(v, pa.Table) and v.num_rows == 3),
        (pl.DataFrame, lambda v: isinstance(v, pl.DataFrame) and v.height == 3),
        (pl.LazyFrame, lambda v: isinstance(v, pl.LazyFrame) and v.collect().height == 3),
    ],
)
def test_round_trip_honours_the_declared_input_type(tmp_path, declared, check):
    """Reads still take a PyArrow filesystem; only writes changed. This
    covers the read half against the same deltalake version."""
    @asset(name="up", io_manager_key="iom")
    def up():
        return pa.table({"id": [1, 2, 3], "region": ["a", "b", "c"]})

    @asset(name="down")
    def down(up: declared):
        assert check(up), f"got {type(up)}"
        return True

    iom = _iom(tmp_path)
    result = materialize([up, down], resources={"iom": iom})
    assert result.success
    assert result.output_for_node("down") is True


# ---------------------------------------------------------------------------
# Credentials reach a write
# ---------------------------------------------------------------------------


def _s3_common():
    return S3FSCommonConfig(
        access_key_id="key",
        secret_access_key="secret",
        end_point="http://minio:9000",
        region="us-east-1",
        allow_http=True,
    )


def test_every_s3_backend_builds_storage_options():
    """Credentials used to travel to write_deltalake as a PyArrow
    filesystem. deltalake 1.x removed that argument, so storage_options is
    now the ONLY way they reach a write -- a backend that leaves it unset
    would write uncredentialed and fail against real S3. The PyArrow
    filesystem backends never built one before, because they did not need
    to."""
    from dag_tools.io_managers.delta import (
        ArrowS3FSConfig,
        FsspecS3FSConfig,
        PolarsS3FSConfig,
    )

    configs = [
        S3FSConfig(common=_s3_common()),
        PolarsS3FSConfig(common=_s3_common()),
        ArrowS3FSConfig(common=_s3_common()),
        FsspecS3FSConfig(common=_s3_common(), cache_storage="/tmp/deltacache"),
    ]
    for cfg in configs:
        iom = DeltaIOManager(config=cfg, uri_base="s3://bucket/x")
        opts = iom._storage_options
        assert opts, f"{type(cfg).__name__} has no storage_options"
        assert opts["AWS_ACCESS_KEY_ID"] == "key"
        assert opts["AWS_SECRET_ACCESS_KEY"] == "secret"
        assert opts["AWS_ENDPOINT_URL"] == "http://minio:9000"


def test_local_backend_needs_no_credentials(tmp_path):
    iom = DeltaIOManager(config=LocalFSConfig(), uri_base=str(tmp_path))
    assert iom._storage_options is None


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def test_publishes_column_schema(tmp_path):
    @asset(name="t", io_manager_key="iom")
    def t():
        return pa.table({"id": [1], "region": ["a"]})

    result = materialize([t], resources={"iom": _iom(tmp_path)})
    md = (
        result.get_asset_materialization_events()[0]
        .step_materialization_data.materialization.metadata
    )
    assert [c.name for c in md["dagster/column_schema"].schema.columns] == [
        "id",
        "region",
    ]


# ---------------------------------------------------------------------------
# Mesh advertisement
# ---------------------------------------------------------------------------


def test_advertised_uri_matches_the_actual_write_location(tmp_path):
    """Materialize for real, then assert the ticket points at the same
    relative location.

    The ticket used to insert a ``storage/`` segment, which comes from
    get_op_output_relative_path -- that is the OP-output layout.
    ASSETS land at <uri_base>/<asset key>, so the advertisement pointed at
    an empty prefix and consumers got "No files in log segment" from a
    route the gateway served with full confidence. A ticket to nowhere is
    worse than no ticket."""
    @asset(name="mesh_orders", key_prefix=["sales"], io_manager_key="iom")
    def mesh_orders():
        return pa.table({"id": [1]})

    assert materialize([mesh_orders], resources={"iom": _iom(tmp_path)}).success

    root = next(p for p in tmp_path.rglob("_delta_log")).parent
    actual_rel = root.relative_to(tmp_path).as_posix()

    ticket = ConfigurableDeltaIOManager(
        fs=S3FSConfig(common=_s3_common()), uri_base="s3://lake/delta"
    ).physical_coordinates(["sales", "mesh_orders"])
    assert ticket["physical_uri"] == f"s3://lake/delta/{actual_rel}"


def test_s3_backend_advertises_a_delta_ticket():
    iom = ConfigurableDeltaIOManager(
        fs=S3FSConfig(common=_s3_common()), uri_base="s3://lake/delta"
    )
    ticket = iom.physical_coordinates(["sales", "orders"])
    assert ticket["source_type"] == "s3_delta"
    assert ticket["credentials"]["aws_access_key_id"] == "key"


def test_local_backend_is_not_advertised(tmp_path):
    """Local disk exists on one pod only."""
    assert _iom(tmp_path).physical_coordinates(["x"]) is None


def test_factory_and_inner_manager_agree():
    """The broker reads whatever is in Definitions(resources=...) -- the
    FACTORY -- so the factory is the object that must answer. Computing
    the ticket in two places is how an advertised location drifts from
    the real one, so both go through the same function."""
    factory = ConfigurableDeltaIOManager(
        fs=S3FSConfig(common=_s3_common()), uri_base="s3://lake/delta"
    )
    inner = factory.create_io_manager(None)
    key = ["sales", "orders"]
    assert factory.physical_coordinates(key) == inner.physical_coordinates(key)
