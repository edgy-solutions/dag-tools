"""DuckDBIOManager — writing queries rather than frames.

The contracts that matter here:

1. A relation is lazy, so the write happens in handle_output, after the
   asset returned. The connection behind it must still be open — and when
   it isn't, the failure has to say so.
2. Output shape matches ArrowIOManager (a directory of parts at the same
   path), so the two are interchangeable to a reader.
3. physical_coordinates advertises only what a mesh consumer can read.
"""
import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")
pytest.importorskip("polars")

import polars as pl
from dagster import asset, materialize

from dag_tools.io_managers.duckdb import (
    ConfigurableDuckDBIOManager,
    DuckDBIOManager,
    asset_uri,
)
from dag_tools.resources.duckdb import DuckDBResource


def _iom(tmp_path, **kw) -> ConfigurableDuckDBIOManager:
    return ConfigurableDuckDBIOManager(
        duckdb=DuckDBResource(), uri_base=str(tmp_path).replace("\\", "/"), **kw
    )


def _s3_iom(uri_base="s3://dag-lake/pub", **kw) -> ConfigurableDuckDBIOManager:
    return ConfigurableDuckDBIOManager(
        duckdb=DuckDBResource(
            aws_access_key_id="key",
            aws_secret_access_key="secret",
            endpoint_url="http://minio:9000",
            aws_region="us-east-1",
        ),
        uri_base=uri_base,
        **kw,
    )


# ---------------------------------------------------------------------------
# The streaming path: an asset returns a query, not a frame
# ---------------------------------------------------------------------------


def test_writes_a_relation(tmp_path):
    @asset(name="orders", io_manager_key="iom")
    def orders(duck: DuckDBResource):
        # connect(), not get_connection(): the relation is lazy and the
        # write happens after this returns.
        con = duck.connect()
        return con.sql("SELECT i AS id, i % 3 AS bucket FROM range(50) t(i)")

    result = materialize(
        [orders], resources={"iom": _iom(tmp_path), "duck": DuckDBResource()}
    )
    assert result.success
    written = list((tmp_path / "orders").rglob("*.parquet"))
    assert written, "relation produced no parquet output"
    assert pl.scan_parquet(tmp_path / "orders").select(pl.len()).collect().item() == 50


def test_reports_row_count_and_uri_metadata(tmp_path):
    """Row count comes from the Parquet footer after the write, so it costs
    a metadata read rather than executing the query a second time."""
    @asset(name="counted", io_manager_key="iom")
    def counted(duck: DuckDBResource):
        return duck.connect().sql("SELECT i FROM range(42) t(i)")

    result = materialize(
        [counted], resources={"iom": _iom(tmp_path), "duck": DuckDBResource()}
    )
    md = result.get_asset_materialization_events()[0] \
        .step_materialization_data.materialization.metadata
    assert md["dagster/row_count"].value == 42
    assert "counted" in md["uri"].value


def test_publishes_column_schema(tmp_path):
    """Column-level schema in the catalog used to come from the cortex IO
    manager's direct DataHub emit. That was removed and nothing replaced
    it, so datasets registered with lineage but no columns. The writer is
    the natural source -- the relation already carries its schema, so this
    costs no query."""
    @asset(name="typed", io_manager_key="iom")
    def typed(duck: DuckDBResource):
        return duck.connect().sql(
            "SELECT 1 AS id, 'x' AS label, 2.5 AS amount"
        )

    result = materialize(
        [typed], resources={"iom": _iom(tmp_path), "duck": DuckDBResource()}
    )
    md = (
        result.get_asset_materialization_events()[0]
        .step_materialization_data.materialization.metadata
    )
    cols = md["dagster/column_schema"].schema.columns
    assert [c.name for c in cols] == ["id", "label", "amount"]
    # Types are DuckDB's own names, which is what the catalog will show.
    assert all(c.type for c in cols)


def test_output_is_a_directory_of_parts(tmp_path):
    """Matches ArrowIOManager's shape so a reader cannot tell them apart,
    and so a large asset can split across files."""
    @asset(name="wide", io_manager_key="iom")
    def wide(duck: DuckDBResource):
        return duck.connect().sql("SELECT i FROM range(10) t(i)")

    materialize([wide], resources={"iom": _iom(tmp_path), "duck": DuckDBResource()})
    out = tmp_path / "wide"
    assert out.is_dir(), "expected a directory of parts, got a single file"
    assert [p.name for p in out.iterdir()] == ["data_0.parquet"]


def test_large_output_splits_across_parts(tmp_path):
    @asset(name="big", io_manager_key="iom")
    def big(duck: DuckDBResource):
        return duck.connect().sql(
            "SELECT i, repeat('x', 200) AS pad FROM range(400000) t(i)"
        )

    materialize(
        [big],
        resources={"iom": _iom(tmp_path, file_size_bytes="400KB"), "duck": DuckDBResource()},
    )
    parts = list((tmp_path / "big").iterdir())
    assert len(parts) > 1, f"expected a split, got {[p.name for p in parts]}"
    assert pl.scan_parquet(tmp_path / "big").select(pl.len()).collect().item() == 400000


def test_single_file_when_file_size_bytes_disabled(tmp_path):
    @asset(name="one", io_manager_key="iom")
    def one(duck: DuckDBResource):
        return duck.connect().sql("SELECT i FROM range(5) t(i)")

    materialize(
        [one],
        resources={"iom": _iom(tmp_path, file_size_bytes=None), "duck": DuckDBResource()},
    )
    assert (tmp_path / "one").is_file()


# ---------------------------------------------------------------------------
# In-memory frames still work — pipelines mix the two
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["polars", "polars_lazy", "arrow", "pandas"])
def test_writes_in_memory_frames(tmp_path, kind):
    import pandas as pd
    import pyarrow as pa

    payloads = {
        "polars": lambda: pl.DataFrame({"id": [1, 2, 3]}),
        "polars_lazy": lambda: pl.LazyFrame({"id": [1, 2, 3]}),
        "arrow": lambda: pa.table({"id": [1, 2, 3]}),
        "pandas": lambda: pd.DataFrame({"id": [1, 2, 3]}),
    }

    @asset(name="frame", io_manager_key="iom")
    def frame():
        return payloads[kind]()

    assert materialize([frame], resources={"iom": _iom(tmp_path)}).success
    assert pl.scan_parquet(tmp_path / "frame").select(pl.len()).collect().item() == 3


def test_unsupported_type_names_the_alternatives(tmp_path):
    @asset(name="bad", io_manager_key="iom")
    def bad():
        return {"not": "a frame"}

    result = materialize([bad], resources={"iom": _iom(tmp_path)}, raise_on_error=False)
    assert not result.success
    detail = str(result.filter_events(lambda e: e.is_step_failure)[0].event_specific_data.error)
    assert "DuckDBPyRelation" in detail


def test_none_output_is_skipped(tmp_path):
    """Assets that write their own output have nothing for us to store."""
    @asset(name="selfwriting", io_manager_key="iom")
    def selfwriting():
        return None

    assert materialize([selfwriting], resources={"iom": _iom(tmp_path)}).success
    assert not (tmp_path / "selfwriting").exists()


# ---------------------------------------------------------------------------
# The lifetime trap — the whole reason connect() and get_connection() differ
# ---------------------------------------------------------------------------


def test_closed_connection_explains_the_lifetime_rule(tmp_path):
    """get_connection() closes on block exit, but a relation only executes
    later, in handle_output. The bare DuckDB error gives no hint that the
    cause is how the connection was acquired."""
    @asset(name="premature", io_manager_key="iom")
    def premature(duck: DuckDBResource):
        with duck.get_connection() as con:
            return con.sql("SELECT i FROM range(5) t(i)")

    result = materialize(
        [premature],
        resources={"iom": _iom(tmp_path), "duck": DuckDBResource()},
        raise_on_error=False,
    )
    assert not result.success
    detail = str(result.filter_events(lambda e: e.is_step_failure)[0].event_specific_data.error)
    assert "connect()" in detail
    assert "get_connection()" in detail


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


def test_downstream_reads_upstream_output(tmp_path):
    """Inputs are loaded by the UPSTREAM asset's IO manager, so this
    exercises load_input on the directory-of-parts shape."""
    @asset(name="up", io_manager_key="iom")
    def up(duck: DuckDBResource):
        return duck.connect().sql("SELECT i AS id FROM range(20) t(i)")

    # `down` returns a scalar, so it keeps the default IO manager — only
    # its INPUT goes through the duckdb manager, which is the point.
    @asset(name="down")
    def down(up) -> int:
        # up arrives as a lazy relation; count without materializing it.
        return up.aggregate("count(*)").fetchone()[0]

    result = materialize(
        [up, down], resources={"iom": _iom(tmp_path), "duck": DuckDBResource()}
    )
    assert result.success
    assert result.output_for_node("down") == 20


def test_file_scheme_uri_base(tmp_path):
    """A file:// uri_base is the normal local/dev configuration. DuckDB
    addresses local files by plain path, so the URI has to be converted --
    and the parent-directory guard has to run for it too, which it did not
    when it tested for '://' before the conversion."""
    iom = ConfigurableDuckDBIOManager(
        duckdb=DuckDBResource(), uri_base=(tmp_path / "lake").as_uri()
    )

    @asset(name="orders", key_prefix=["publog"], io_manager_key="iom")
    def orders(duck: DuckDBResource):
        return duck.connect().sql("SELECT 1 AS id")

    assert materialize(
        [orders], resources={"iom": iom, "duck": DuckDBResource()}
    ).success
    assert (tmp_path / "lake" / "publog" / "orders").is_dir()


def test_nested_asset_key_path(tmp_path):
    @asset(name="orders", key_prefix=["sales"], io_manager_key="iom")
    def orders(duck: DuckDBResource):
        return duck.connect().sql("SELECT 1 AS id")

    materialize([orders], resources={"iom": _iom(tmp_path), "duck": DuckDBResource()})
    assert (tmp_path / "sales" / "orders").is_dir()


# ---------------------------------------------------------------------------
# Mesh advertisement
# ---------------------------------------------------------------------------


def test_advertised_uri_matches_actual_write_path(tmp_path):
    """A ticket pointing where the data isn't is worse than no ticket —
    the gateway routes consumers to it regardless."""
    @asset(name="mesh_customers", io_manager_key="iom")
    def mesh_customers(duck: DuckDBResource):
        return duck.connect().sql("SELECT 1 AS id")

    materialize([mesh_customers], resources={"iom": _iom(tmp_path), "duck": DuckDBResource()})
    actual_rel = (tmp_path / "mesh_customers").relative_to(tmp_path).as_posix()

    ticket = _s3_iom().physical_coordinates(["mesh_customers"])
    assert ticket["physical_uri"] == f"s3://dag-lake/pub/{actual_rel}/"


def test_asset_uri_is_the_single_source_of_truth():
    """The writer and the advertisement used to build the path separately,
    and drifted -- which is how the missing trailing slash shipped. Both now
    go through asset_uri, and external callers (a freshness check that needs
    to stat the output without owning it) can use the same function."""
    iom = _s3_iom()
    advertised = iom.physical_coordinates(["sales", "orders"])["physical_uri"]
    assert advertised == asset_uri("s3://dag-lake/pub", ["sales", "orders"])
    # The writer target is the same location without the directory marker.
    assert advertised.rstrip("/") == asset_uri(
        "s3://dag-lake/pub", ["sales", "orders"], directory=False
    )


def test_asset_uri_leaves_an_explicit_parquet_suffix_alone():
    assert asset_uri("s3://b", ["report.parquet"]) == "s3://b/report.parquet/"


def test_advertised_uri_marks_the_directory_with_a_trailing_slash():
    """The client calls pl.scan_parquet(physical_uri) verbatim. Against S3
    a slash-less path is read as an object key and HEADs to a 404, though
    polars globs the same path fine locally — so a local test passes while
    every real mesh read fails. Verified against MinIO."""
    for key in (["mesh_customers"], ["sales", "orders"]):
        assert _s3_iom().physical_coordinates(key)["physical_uri"].endswith("/")


def test_single_file_config_advertises_an_object_not_a_directory():
    """With file_size_bytes unset the output really is one object, so the
    trailing slash would point at a prefix that does not exist."""
    ticket = _s3_iom(file_size_bytes=None).physical_coordinates(["orders"])
    assert ticket["physical_uri"] == "s3://dag-lake/pub/orders"


def test_advertised_ticket_shape_is_client_readable():
    ticket = _s3_iom().physical_coordinates(["mesh_customers"])
    assert ticket["source_type"] == "s3_parquet"
    creds = ticket["credentials"]
    assert creds["aws_access_key_id"] == "key"
    assert creds["aws_secret_access_key"] == "secret"
    assert creds["aws_endpoint_url"] == "http://minio:9000"


def test_advertises_nested_asset_key():
    ticket = _s3_iom().physical_coordinates(["sales", "orders"])
    assert ticket["physical_uri"] == "s3://dag-lake/pub/sales/orders/"


def test_local_path_is_not_advertised(tmp_path):
    assert _iom(tmp_path).physical_coordinates(["anything"]) is None


def test_empty_asset_key_is_not_advertised():
    assert _s3_iom().physical_coordinates([]) is None


def test_layout_matches_arrow_io_manager():
    """The two managers must be interchangeable to a reader — an asset
    should be movable between them without consumers noticing."""
    pytest.importorskip("pandas")
    pytest.importorskip("s3fs")
    from dag_tools.io_managers.arrow import (
        ConfigurableArrowIOManager,
        S3FSCommonConfig,
        S3FSConfig,
    )

    arrow = ConfigurableArrowIOManager(
        uri_base="s3://dag-lake/pub",
        fs=S3FSConfig(
            common=S3FSCommonConfig(
                access_key_id="key", secret_access_key="secret",
                end_point="http://minio:9000", region="us-east-1", allow_http=True,
            )
        ),
    )
    for key in (["orders"], ["sales", "orders"]):
        assert (
            _s3_iom().physical_coordinates(key)["physical_uri"]
            == arrow.physical_coordinates(key)["physical_uri"]
        )


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def test_s3_output_declares_platform_for_the_catalog():
    """The manager names the platform in ITS vocabulary -- the same
    source_type the mesh ticket carries -- and the catalog sensor
    translates it to DataHub's name. An IO manager should not have to
    know what DataHub calls things."""
    iom = DuckDBIOManager(DuckDBResource(), "s3://dag-lake/pub")
    declared = iom.get_metadata()["destination_name"].text
    ticket = _s3_iom().physical_coordinates(["x"])
    assert declared == ticket["source_type"] == "s3_parquet"


def test_local_output_declares_no_platform():
    assert DuckDBIOManager(DuckDBResource(), "/tmp/x").get_metadata() == {}
