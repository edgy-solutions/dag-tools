"""Column schema on materialization metadata.

Every producing IO manager publishes the columns of what it wrote. The
Dagster UI renders them and the DataHub catalog sensor turns them into a
schemaMetadata aspect — without it a dataset registers with properties
and lineage but no columns.

This is worth pinning because the failure is invisible: a manager that
skipped it looked exactly like one that worked, until someone opened the
catalog. That is how it went unnoticed when the cortex IO manager's
direct DataHub emit was removed and nothing replaced it.
"""
import pytest

pytest.importorskip("pyarrow")

from dag_tools.io_managers.column_schema import (
    COLUMN_SCHEMA_KEY,
    add_column_schema,
    column_schema_metadata,
)


def _cols(obj):
    v = column_schema_metadata(obj)
    return None if v is None else [(c.name, c.type) for c in v.schema.columns]


# ---------------------------------------------------------------------------
# Every frame type a producer might be handed
# ---------------------------------------------------------------------------


def test_polars_dataframe():
    pl = pytest.importorskip("polars")
    assert _cols(pl.DataFrame({"id": [1], "region": ["a"]})) == [
        ("id", "Int64"),
        ("region", "String"),
    ]


def test_polars_lazyframe_is_not_collected():
    """collect_schema() reads the plan; collecting would execute the query
    just to name its columns."""
    pl = pytest.importorskip("polars")
    assert [n for n, _ in _cols(pl.LazyFrame({"id": [1], "region": ["a"]}))] == [
        "id",
        "region",
    ]


def test_pandas_dataframe():
    pd = pytest.importorskip("pandas")
    assert [n for n, _ in _cols(pd.DataFrame({"id": [1], "region": ["a"]}))] == [
        "id",
        "region",
    ]


def test_pyarrow_table():
    import pyarrow as pa

    assert _cols(pa.table({"id": [1], "region": ["a"]})) == [
        ("id", "int64"),
        ("region", "string"),
    ]


def test_record_batch_reader_is_not_drained():
    """A RecordBatchReader is one-shot. Reading its declared schema must
    leave every row available to the writer."""
    import pyarrow as pa

    table = pa.table({"id": [1, 2, 3]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(1))

    assert [n for n, _ in _cols(reader)] == ["id"]
    assert reader.read_all().num_rows == 3


def test_duckdb_relation():
    duckdb = pytest.importorskip("duckdb")

    rel = duckdb.connect().sql("SELECT 1 AS id, 'x' AS label")
    assert [n for n, _ in _cols(rel)] == ["id", "label"]


def test_multi_output_dict_describes_the_first():
    """Managers that write a dict of outputs still have one metadata slot."""
    pl = pytest.importorskip("polars")
    assert [n for n, _ in _cols({"a": pl.DataFrame({"id": [1]})})] == ["id"]


# ---------------------------------------------------------------------------
# Degrading, never failing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("obj", [None, object(), "text", 42, {}, []])
def test_undeterminable_returns_none_rather_than_raising(obj):
    """A missing schema costs one metadata field. It must never fail a
    materialization whose data is already written."""
    assert column_schema_metadata(obj) is None


def test_add_column_schema_leaves_metadata_alone_when_undeterminable():
    md = {"uri": "s3://x"}
    assert add_column_schema(md, object()) == {"uri": "s3://x"}


def test_add_column_schema_uses_the_conventional_key():
    """dagster-dbt attaches column schema under this name, so ours and
    dbt's are handled by a single path in the catalog sensor."""
    pl = pytest.importorskip("polars")
    md = add_column_schema({}, pl.DataFrame({"id": [1]}))
    assert COLUMN_SCHEMA_KEY == "dagster/column_schema"
    assert COLUMN_SCHEMA_KEY in md


# ---------------------------------------------------------------------------
# Each producing IO manager actually publishes it
# ---------------------------------------------------------------------------


def _schema_names(result):
    md = (
        result.get_asset_materialization_events()[0]
        .step_materialization_data.materialization.metadata
    )
    assert COLUMN_SCHEMA_KEY in md, f"no column schema published; got {sorted(md)}"
    return [c.name for c in md[COLUMN_SCHEMA_KEY].schema.columns]


def test_arrow_io_manager_publishes_schema(tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("s3fs")
    pl = pytest.importorskip("polars")
    from dagster import asset, materialize

    from dag_tools.io_managers.arrow import ConfigurableArrowIOManager, LocalFSConfig

    @asset(name="arrow_schema", io_manager_key="iom")
    def arrow_schema():
        return pl.DataFrame({"id": [1], "region": ["a"]})

    iom = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(tmp_path))
    assert _schema_names(
        materialize([arrow_schema], resources={"iom": iom})
    ) == ["id", "region"]


def test_duckdb_io_manager_publishes_schema(tmp_path):
    pytest.importorskip("duckdb")
    from dagster import asset, materialize

    from dag_tools.io_managers.duckdb import ConfigurableDuckDBIOManager
    from dag_tools.resources.duckdb import DuckDBResource

    @asset(name="duck_schema", io_manager_key="iom")
    def duck_schema(duck: DuckDBResource):
        return duck.connect().sql("SELECT 1 AS id, 'a' AS region")

    iom = ConfigurableDuckDBIOManager(
        duckdb=DuckDBResource(), uri_base=str(tmp_path).replace("\\", "/")
    )
    assert _schema_names(
        materialize(
            [duck_schema], resources={"iom": iom, "duck": DuckDBResource()}
        )
    ) == ["id", "region"]


def test_delta_io_manager_publishes_schema(tmp_path):
    """get_metadata is exercised directly rather than through a
    materialization.

    DeltaIOManager's deltalake write path does not run against the
    deltalake version this package declares (>=0.20, resolving to 1.x):
    write_deltalake dropped `overwrite_schema` in favour of `schema_mode`
    and removed `filesystem` and `schema` outright, while delta.py still
    passes all three. That is a separate defect from column schema, so it
    is not allowed to hide whether this metadata is published.
    """
    pytest.importorskip("deltalake")
    pl = pytest.importorskip("polars")
    from dagster import AssetKey, build_output_context

    from dag_tools.io_managers.delta import ConfigurableDeltaIOManager, LocalFSConfig

    iom = ConfigurableDeltaIOManager(
        fs=LocalFSConfig(), uri_base=str(tmp_path)
    ).create_io_manager(None)

    ctx = build_output_context(asset_key=AssetKey(["delta_schema"]))
    md = iom.get_metadata(ctx, pl.DataFrame({"id": [1], "region": ["a"]}))
    assert COLUMN_SCHEMA_KEY in md, f"no column schema published; got {sorted(md)}"
    assert [c.name for c in md[COLUMN_SCHEMA_KEY].schema.columns] == ["id", "region"]


def _sql_manager(protocol="postgres"):
    from dag_tools.io_managers.sql import SQLConfig, SQLIOManager

    return SQLIOManager(
        SQLConfig(
            protocol=protocol, host="h", port=5432,
            database="db", username="u", password="p",
        )
    )


def test_sql_io_manager_publishes_schema():
    """SQLIOManager writes to a live database, so the metadata call is
    exercised directly against a captured context."""
    pd = pytest.importorskip("pandas")

    captured = {}

    class Ctx:
        def add_output_metadata(self, md):
            captured.update(md)

    _sql_manager()._emit_output_metadata(
        Ctx(), pd.DataFrame({"id": [1], "region": ["a"]})
    )
    assert COLUMN_SCHEMA_KEY in captured
    assert [c.name for c in captured[COLUMN_SCHEMA_KEY].schema.columns] == [
        "id",
        "region",
    ]


def test_sql_metadata_failure_never_fails_the_write():
    """The rows are already in the database by the time metadata is
    emitted; a metadata problem must not turn a successful write into a
    failed materialization."""
    pd = pytest.importorskip("pandas")

    class Hostile:
        def add_output_metadata(self, md):
            raise RuntimeError("metadata already set")

    _sql_manager()._emit_output_metadata(Hostile(), pd.DataFrame({"id": [1]}))
