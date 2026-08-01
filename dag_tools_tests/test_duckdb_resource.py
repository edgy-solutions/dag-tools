"""DuckDBResource — the S3-addressable connection.

What matters here is the wiring the resource exists to hide: the endpoint
reshaping DuckDB needs (which differs from every other AWS client), the
httpfs load and its failure mode, and the connection-lifetime contract
that lazy results depend on.
"""
import os

import pytest

pytest.importorskip("duckdb")

from dag_tools.resources.duckdb import DuckDBResource, duckdb_path, split_endpoint


# ---------------------------------------------------------------------------
# Endpoint reshaping — DuckDB wants host:port + a bool, not a URL
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url,expect_host,expect_ssl",
    [
        ("http://minio:9000", "minio:9000", False),
        ("https://s3.example.com", "s3.example.com", True),
        ("https://s3.example.com:9021", "s3.example.com:9021", True),
        # Bare host:port has no scheme, so urlparse puts it in `path`, not
        # `netloc` — it must still come back usable rather than as None.
        ("minio:9000", "minio:9000", True),
        (None, None, True),
        ("", None, True),
    ],
)
def test_split_endpoint(url, expect_host, expect_ssl):
    assert split_endpoint(url) == (expect_host, expect_ssl)


def test_duckdb_path_passes_object_store_urls_through():
    """s3:// is how DuckDB addresses object storage — it must not be
    rewritten into a local path."""
    assert duckdb_path("s3://bucket/key.parquet") == "s3://bucket/key.parquet"


def test_duckdb_path_strips_file_scheme():
    out = duckdb_path("file:///data/x.parquet")
    assert not out.startswith("file://")
    assert out.endswith("/data/x.parquet")


@pytest.mark.skipif(os.name != "nt", reason="Windows drive-letter form")
def test_duckdb_path_windows_drive_letter():
    """urlparse turns file:///C:/x into /C:/x, which Windows cannot open."""
    assert duckdb_path("file:///C:/data/x.parquet") == "C:/data/x.parquet"


# ---------------------------------------------------------------------------
# Settings actually land on the connection
# ---------------------------------------------------------------------------


def _setting(con, name):
    return con.execute(f"SELECT current_setting('{name}')").fetchone()[0]


def test_s3_settings_are_applied():
    res = DuckDBResource(
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        aws_region="eu-west-1",
        endpoint_url="http://minio:9000",
    )
    with res.get_connection() as con:
        assert _setting(con, "s3_endpoint") == "minio:9000"
        assert _setting(con, "s3_use_ssl") is False
        assert _setting(con, "s3_region") == "eu-west-1"
        assert _setting(con, "s3_access_key_id") == "key"


def test_url_style_defaults_to_path_when_endpoint_set():
    """MinIO serves path-style buckets; vhost style resolves to a hostname
    that does not exist there, so a custom endpoint implies path."""
    res = DuckDBResource(endpoint_url="http://minio:9000")
    with res.get_connection() as con:
        assert _setting(con, "s3_url_style") == "path"


def test_url_style_is_overridable():
    res = DuckDBResource(endpoint_url="http://minio:9000", url_style="vhost")
    with res.get_connection() as con:
        assert _setting(con, "s3_url_style") == "vhost"


def test_no_endpoint_leaves_aws_defaults_alone():
    """Real AWS needs no endpoint override — setting one would break it."""
    res = DuckDBResource(aws_region="us-west-2")
    with res.get_connection() as con:
        assert not _setting(con, "s3_endpoint")
        assert _setting(con, "s3_region") == "us-west-2"


def test_memory_limit_applied():
    """Capping DuckDB is what keeps a container from being OOMKilled
    instead of spilling. DuckDB reads '256MB' as 256e6 bytes and reports
    it back in MiB."""
    res = DuckDBResource(memory_limit="256MB")
    with res.get_connection() as con:
        assert _setting(con, "memory_limit") == "244.1 MiB"


def test_memory_limit_unset_leaves_duckdb_default():
    with DuckDBResource().get_connection() as con:
        assert _setting(con, "memory_limit") != "244.1 MiB"


def test_extra_settings_applied():
    # DuckDB reads "512MB" as 512e6 bytes and reports it back in MiB, so
    # assert the setting took effect rather than pinning its rendering.
    res = DuckDBResource(extra_settings={"memory_limit": "512MB"})
    with res.get_connection() as con:
        assert _setting(con, "memory_limit") == "488.2 MiB"


# ---------------------------------------------------------------------------
# httpfs is the whole point — it must load, and fail loudly when it can't
# ---------------------------------------------------------------------------


def test_httpfs_is_loaded():
    res = DuckDBResource()
    with res.get_connection() as con:
        loaded = con.execute(
            "SELECT loaded FROM duckdb_extensions() WHERE extension_name='httpfs'"
        ).fetchone()
        assert loaded and loaded[0], "httpfs not loaded; s3:// would be unreachable"


def test_missing_baked_extension_raises_actionable_error(tmp_path):
    """When extension_directory is set the image is meant to be
    self-contained, so a missing extension must raise here rather than
    silently reaching out to duckdb.org — which in a restricted cluster
    means a long hang and a confusing error, once per asset."""
    res = DuckDBResource(extension_directory=str(tmp_path / "empty"))
    with pytest.raises(RuntimeError) as exc:
        res.connect()
    msg = str(exc.value)
    assert "httpfs" in msg
    assert "rebuild" in msg.lower()


def test_extension_directory_falls_back_to_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_EXTENSION_DIRECTORY", str(tmp_path / "empty"))
    with pytest.raises(RuntimeError):
        DuckDBResource().connect()


# ---------------------------------------------------------------------------
# Connection lifetime — the contract lazy results depend on
# ---------------------------------------------------------------------------


def test_get_connection_closes_on_exit():
    res = DuckDBResource()
    with res.get_connection() as con:
        pass
    with pytest.raises(Exception):
        con.execute("SELECT 1")


def test_connect_returns_unmanaged_connection_for_lazy_results():
    """The Arrow reader streams FROM the connection — an IO manager
    consuming it in handle_output would break if the resource closed the
    connection first, so connect() must leave it open."""
    res = DuckDBResource()
    con = res.connect()
    try:
        reader = res.arrow_reader(con.sql("SELECT i FROM range(5) t(i)"), batch_size=2)
        assert reader.read_all().num_rows == 5
    finally:
        con.close()


def test_arrow_reader_survives_the_duckdb_rename():
    """fetch_arrow_reader was renamed to to_arrow_reader; consumers pin a
    range that spans the rename, so the resource picks whichever exists."""
    import pyarrow as pa

    res = DuckDBResource()
    with res.get_connection() as con:
        reader = res.arrow_reader(con.sql("SELECT i FROM range(3) t(i)"))
        assert isinstance(reader, pa.RecordBatchReader)
        assert reader.read_all().num_rows == 3


# ---------------------------------------------------------------------------
# Dagster integration
# ---------------------------------------------------------------------------


def test_usable_as_a_dagster_resource(tmp_path):
    from dagster import asset, materialize

    @asset
    def counted(duck: DuckDBResource) -> int:
        with duck.get_connection() as con:
            return con.execute("SELECT count(*) FROM range(10)").fetchone()[0]

    result = materialize([counted], resources={"duck": DuckDBResource()})
    assert result.success
    assert result.output_for_node("counted") == 10


def test_configure_from_dict():
    res = DuckDBResource.configure(
        {"aws_access_key_id": "k", "endpoint_url": "http://minio:9000"}
    )
    assert res.aws_access_key_id == "k"
    assert res.endpoint_url == "http://minio:9000"


def test_from_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "envkey")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "envsecret")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
    res = DuckDBResource.from_env()
    assert res.aws_access_key_id == "envkey"
    assert res.endpoint_url == "http://minio:9000"
    assert res.aws_region == "eu-central-1"


def test_from_env_overrides_win(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    assert DuckDBResource.from_env(endpoint_url=None).endpoint_url is None
