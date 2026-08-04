"""Producer/consumer boundary for mesh IO managers.

Two rules this file pins down:

1. ``ConfigurableArrowIOManager`` is a PRODUCER — it writes parquet and
   advertises that location through ``physical_coordinates``. The
   advertised URI must be exactly where the data actually lands, and it
   must refuse to advertise anything a mesh consumer couldn't read.

2. ``CortexPolarsIOManager`` is a read-only CONSUMER facade. It must not
   write, and must not carry ``physical_coordinates`` — it gets bound to
   assets another deployment may own, so announcing them would make this
   deployment claim ownership it doesn't have (and fight the real owner
   for the gateway's routing key).
"""
import pathlib

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("polars")
pytest.importorskip("pandas")   # arrow.py imports pandas at module level
pytest.importorskip("s3fs")     # ...and s3fs

import polars as pl
from dagster import asset, materialize

from dag_tools.io_managers.arrow import (
    ConfigurableArrowIOManager,
    LocalFSConfig,
    S3FSCommonConfig,
    S3FSConfig,
)
from dag_tools.io_managers.cortex_io_manager import CortexPolarsIOManager


def _s3_iom(uri_base="s3://dag-lake/mesh_demo") -> ConfigurableArrowIOManager:
    return ConfigurableArrowIOManager(
        uri_base=uri_base,
        fs=S3FSConfig(
            common=S3FSCommonConfig(
                access_key_id="key",
                secret_access_key="secret",
                end_point="http://minio:9000",
                region="us-east-1",
                allow_http=True,
            )
        ),
    )


# ---------------------------------------------------------------------------
# Arrow accepts Polars (the mesh's standard frame)
# ---------------------------------------------------------------------------


def test_arrow_writes_polars_dataframe(tmp_path):
    @asset(name="polars_df_asset", io_manager_key="iom")
    def polars_df_asset():
        return pl.DataFrame({"id": [1, 2, 3], "region": ["a", "b", "c"]})

    iom = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(tmp_path))
    assert materialize([polars_df_asset], resources={"iom": iom}).success
    written = list(tmp_path.rglob("*.parquet"))
    assert written, "polars DataFrame produced no parquet output"


def test_arrow_writes_streaming_record_batch_reader(tmp_path):
    """RecordBatchReader is in dump_to_path's accepted-types tuple, but the
    row-count log called len() on it — which streaming readers don't
    support — so the advertised streaming path always raised TypeError.

    This is the path duckdb uses (.fetch_arrow_reader()), which is how a
    SQL-shaped asset streams to parquet without materializing in RAM."""
    import pyarrow as pa

    table = pa.table({"id": [1, 2, 3], "region": ["a", "b", "c"]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(1))

    @asset(name="streamed_asset", io_manager_key="iom")
    def streamed_asset():
        return reader

    iom = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(tmp_path))
    assert materialize([streamed_asset], resources={"iom": iom}).success
    written = list(tmp_path.rglob("part-*.parquet"))
    assert written, "streaming reader produced no parquet output"
    assert pl.read_parquet(written[0]).height == 3


@pytest.mark.parametrize("payload", ["polars", "lazy", "arrow", "pandas"])
def test_arrow_publishes_column_schema(tmp_path, payload):
    """Column-level schema reached the catalog through the cortex IO
    manager's direct DataHub emit. That was removed and nothing replaced
    it, so mesh_demo_customers kept a fossil schema from the last cortex
    write while its properties went on being refreshed."""
    import pandas as pd
    import pyarrow as pa

    make = {
        "polars": lambda: pl.DataFrame({"id": [1], "region": ["a"]}),
        "lazy": lambda: pl.LazyFrame({"id": [1], "region": ["a"]}),
        "arrow": lambda: pa.table({"id": [1], "region": ["a"]}),
        "pandas": lambda: pd.DataFrame({"id": [1], "region": ["a"]}),
    }[payload]

    @asset(name="schema_asset", io_manager_key="iom")
    def schema_asset():
        return make()

    iom = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(tmp_path))
    result = materialize([schema_asset], resources={"iom": iom})
    md = (
        result.get_asset_materialization_events()[0]
        .step_materialization_data.materialization.metadata
    )
    cols = md["dagster/column_schema"].schema.columns
    assert [c.name for c in cols] == ["id", "region"]


def test_arrow_schema_does_not_drain_a_streaming_reader(tmp_path):
    """A RecordBatchReader is one-shot. Reading its declared schema is
    fine; consuming it to inspect columns would leave nothing to write."""
    import pyarrow as pa

    table = pa.table({"id": [1, 2, 3]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(1))

    @asset(name="streamed_schema", io_manager_key="iom")
    def streamed_schema():
        return reader

    iom = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(tmp_path))
    result = materialize([streamed_schema], resources={"iom": iom})
    assert result.success
    md = (
        result.get_asset_materialization_events()[0]
        .step_materialization_data.materialization.metadata
    )
    assert [c.name for c in md["dagster/column_schema"].schema.columns] == ["id"]
    # The rows still landed -- the schema read did not consume the stream.
    written = list(tmp_path.rglob("part-*.parquet"))
    assert pl.read_parquet(written[0]).height == 3


def test_arrow_writes_polars_lazyframe(tmp_path):
    @asset(name="polars_lf_asset", io_manager_key="iom")
    def polars_lf_asset():
        return pl.LazyFrame({"id": [1, 2]})

    iom = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(tmp_path))
    assert materialize([polars_lf_asset], resources={"iom": iom}).success
    assert list(tmp_path.rglob("*.parquet"))


# ---------------------------------------------------------------------------
# THE invariant: advertised location == actual write location
# ---------------------------------------------------------------------------


def test_advertised_uri_matches_actual_write_path(tmp_path):
    """A routing ticket that points somewhere the data isn't is worse than
    no ticket at all — the gateway will confidently route consumers to it.
    Materialize locally, then assert physical_coordinates would advertise
    the same relative location."""
    @asset(name="mesh_demo_customers", io_manager_key="iom")
    def mesh_demo_customers():
        return pl.DataFrame({"id": [1]})

    local = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(tmp_path))
    assert materialize([mesh_demo_customers], resources={"iom": local}).success

    # Where the dataset actually landed, relative to uri_base.
    part = next(iter(tmp_path.rglob("*/part-0.parquet")))
    actual_rel = part.parent.relative_to(tmp_path).as_posix()

    ticket = _s3_iom().physical_coordinates(["mesh_demo_customers"])
    assert ticket["physical_uri"] == f"s3://dag-lake/mesh_demo/{actual_rel}/"


def test_advertised_uri_marks_the_directory_with_a_trailing_slash():
    """The client calls pl.scan_parquet(physical_uri) verbatim. Against S3
    a slash-less path is read as an object key and HEADs to a 404, even
    though polars globs the same path fine on a local disk — so a local
    test passes while every real mesh read fails. Verified against MinIO."""
    for key in (["mesh_demo_customers"], ["sales", "orders"]):
        assert _s3_iom().physical_coordinates(key)["physical_uri"].endswith("/")


def test_advertised_ticket_shape_is_client_readable():
    ticket = _s3_iom().physical_coordinates(["mesh_demo_customers"])
    # source_type must be one the cortex data client can dispatch on.
    assert ticket["source_type"] == "s3_parquet"
    creds = ticket["credentials"]
    assert creds["aws_access_key_id"] == "key"
    assert creds["aws_secret_access_key"] == "secret"
    assert creds["aws_endpoint_url"] == "http://minio:9000"


def test_advertises_nested_asset_key():
    ticket = _s3_iom().physical_coordinates(["sales", "orders"])
    assert ticket["physical_uri"] == "s3://dag-lake/mesh_demo/sales/orders/"


# ---------------------------------------------------------------------------
# Arrow refuses to advertise what a consumer couldn't read
# ---------------------------------------------------------------------------


def test_local_fs_is_not_advertised(tmp_path):
    """Local disk exists on exactly one pod — advertising it hands
    consumers a path they can't reach."""
    iom = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(tmp_path))
    assert iom.physical_coordinates(["anything"]) is None


def test_non_s3_uri_base_is_not_advertised():
    assert _s3_iom(uri_base="/mnt/data").physical_coordinates(["x"]) is None


def test_csv_output_is_not_advertised():
    """The client has no CSV read path (s3_parquet/s3_delta/s3_iceberg/
    postgres/clickhouse only)."""
    assert _s3_iom().physical_coordinates(["report.csv"]) is None


def test_empty_asset_key_is_not_advertised():
    assert _s3_iom().physical_coordinates([]) is None


# ---------------------------------------------------------------------------
# Cortex is a read-only consumer facade
# ---------------------------------------------------------------------------


def _cortex() -> CortexPolarsIOManager:
    return CortexPolarsIOManager(
        broker_url="http://gateway:8090", client_id="cid", client_secret="sec"
    )


def test_cortex_does_not_implement_physical_coordinates():
    """The broker advertises any IO manager exposing this method. Cortex
    is bound to assets other deployments may own, so it must not expose
    it — otherwise this deployment advertises phantom locations and
    competes for the real owner's routing key."""
    assert not hasattr(_cortex(), "physical_coordinates")


def test_cortex_handle_output_raises_with_actionable_message():
    @asset(name="should_not_write", io_manager_key="cortex")
    def should_not_write():
        return pl.DataFrame({"id": [1]})

    result = materialize(
        [should_not_write], resources={"cortex": _cortex()}, raise_on_error=False
    )
    assert not result.success
    failures = result.filter_events(lambda e: e.is_step_failure)
    detail = str(failures[0].event_specific_data.error) if failures else ""
    assert "READ-ONLY" in detail
    # Points at the producer alternatives.
    assert "ConfigurableArrowIOManager" in detail


def test_cortex_still_reads(monkeypatch):
    """The read half — the manager's actual purpose — is untouched."""
    import dag_tools.cortex_data.client as client_mod

    captured = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def get_dataframe(self, urn):
            captured["urn"] = urn
            return pl.LazyFrame({"id": [1]})

    monkeypatch.setattr(client_mod, "CortexDataClient", FakeClient)

    @asset(name="upstream_foreign", io_manager_key="cortex")
    def upstream_foreign():  # never materialized; stub for the read path
        raise AssertionError("should not execute")

    from dagster import build_input_context

    ctx = build_input_context(asset_key=upstream_foreign.key)
    out = _cortex().load_input(ctx)
    assert out.collect().height == 1
    assert "upstream_foreign" in captured["urn"]
