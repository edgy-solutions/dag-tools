"""Producer-declared platforms, translated into DataHub's vocabulary.

The platform used to be inferred from asset-key prefixes matched against
a hardcoded list of platform names. That kept naming uniform, but it only
worked for keys spelled the way the inference expected, and adding a
backend meant editing a list that lived nowhere near the backend.

Now the producer names its own platform — it is the only party that
actually knows what it wrote — and this mapping turns that into what
DataHub calls the same thing. The two vocabularies genuinely differ: a
Delta table on S3 is ``s3_delta`` to an IO manager and ``delta-lake`` to
DataHub, because DataHub classifies the table format, not the bucket.
"""
import pytest

from dag_tools.components.datahub_lineage.platforms import (
    PRODUCER_ALIASES,
    SOURCE_TYPE_PLATFORMS,
    UNKNOWN_PLATFORM,
    resolve_platform,
)

# Platform names this DataHub deployment actually has, confirmed by
# querying its dataPlatform entities. Pinned here so a mapping to a
# platform that does not exist is a test failure rather than a dangling
# URN nobody notices.
KNOWN_DATAHUB_PLATFORMS = {
    "s3", "postgres", "clickhouse", "delta-lake", "iceberg", "dagster",
    "file", "gcs", "adlsGen2", "adlsGen1", "mysql", "snowflake",
    "databricks", "unknown",
}


# ---------------------------------------------------------------------------
# The translation that motivated the whole thing
# ---------------------------------------------------------------------------


def test_table_formats_are_not_collapsed_into_the_object_store():
    """The case that makes a mapping necessary rather than optional.

    Delta and Iceberg tables live in an S3 bucket, but calling them "s3"
    would throw away the thing that makes them worth cataloguing."""
    assert resolve_platform("s3_delta") == "delta-lake"
    assert resolve_platform("s3_iceberg") == "iceberg"
    assert resolve_platform("s3_parquet") == "s3"


def test_databases_map_to_themselves():
    assert resolve_platform("postgres") == "postgres"
    assert resolve_platform("clickhouse") == "clickhouse"


def test_dlt_short_names_are_translated():
    """dlt writes its own destination_name -- "abs" for Azure Blob
    Storage, which DataHub calls adlsGen2. Unmapped, that created a
    platform entity that does not exist."""
    assert resolve_platform("abs") == "adlsGen2"
    assert resolve_platform("filesystem") == "file"


# ---------------------------------------------------------------------------
# Every mapping target must be a platform DataHub really has
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source_type,platform", sorted(SOURCE_TYPE_PLATFORMS.items()))
def test_source_type_maps_to_a_real_platform(source_type, platform):
    assert platform in KNOWN_DATAHUB_PLATFORMS, (
        f"{source_type} -> {platform!r} is not a DataHub platform; the URN "
        f"would dangle"
    )


@pytest.mark.parametrize("alias,platform", sorted(PRODUCER_ALIASES.items()))
def test_alias_maps_to_a_real_platform(alias, platform):
    assert platform in KNOWN_DATAHUB_PLATFORMS


# ---------------------------------------------------------------------------
# Behaviour at the edges
# ---------------------------------------------------------------------------


def test_unrecognised_names_pass_through():
    """DataHub has ~100 platforms and the table only covers what dag-tools
    produces, so an unlisted name is far more likely a valid platform than
    a mistake. Forcing it to 'unknown' would be the same closed-set
    fragility this replaced."""
    assert resolve_platform("snowflake") == "snowflake"
    assert resolve_platform("bigquery") == "bigquery"


def test_mapping_preserves_the_dataset_name_layout():
    """The name format is chosen by platform, so mapping a filesystem
    platform to a name that is not itself listed as one would silently
    change every dataset's NAME -- and a different name is a different
    entity, so the catalog grows a duplicate instead of updating the
    original. Caught by comparing the pre- and post-mapping URNs."""
    from dag_tools.components.datahub_lineage.component import (
        asset_keys_to_dataset_urn_converter as to_urn,
    )
    from dag_tools.components.datahub_lineage.platforms import FILESYSTEM_PLATFORMS

    key = ["sales", "orders", "fact"]
    for legacy in ("abs", "filesystem"):
        mapped = resolve_platform(legacy)
        before = to_urn(key, platform=legacy, filesystem_platforms=FILESYSTEM_PLATFORMS)
        after = to_urn(key, platform=mapped, filesystem_platforms=FILESYSTEM_PLATFORMS)
        assert before.name == after.name, (
            f"{legacy} -> {mapped} changed the dataset name "
            f"{before.name!r} -> {after.name!r}"
        )


def test_every_mapped_filesystem_target_is_listed():
    """Guards the pairing directly, so adding an alias without updating
    the layout list fails here rather than in the catalog."""
    from dag_tools.components.datahub_lineage.platforms import FILESYSTEM_PLATFORMS

    for legacy, mapped in PRODUCER_ALIASES.items():
        if legacy in FILESYSTEM_PLATFORMS:
            assert mapped in FILESYSTEM_PLATFORMS, (
                f"{legacy} is a filesystem platform but maps to {mapped!r}, "
                f"which is not — the dataset name layout would change"
            )


def test_absent_name_is_unknown():
    for empty in (None, "", "   "):
        assert resolve_platform(empty) == UNKNOWN_PLATFORM


def test_overrides_win():
    """A new backend or a renamed DataHub platform should be handleable
    from config rather than a release."""
    assert resolve_platform("s3_delta", {"s3_delta": "custom-delta"}) == "custom-delta"
    assert resolve_platform("weird", {"weird": "postgres"}) == "postgres"
    # Overrides that don't apply leave the defaults alone.
    assert resolve_platform("s3_delta", {"other": "x"}) == "delta-lake"


# ---------------------------------------------------------------------------
# Producers declare a platform this mapper understands
# ---------------------------------------------------------------------------


def _declared_and_advertised():
    """(label, declared destination_name, advertised source_type) per manager."""
    out = []

    try:
        from dag_tools.io_managers.arrow import (
            ConfigurableArrowIOManager,
            S3FSCommonConfig,
            S3FSConfig,
            SOURCE_TYPE,
        )
        from dagster import build_output_context

        common = S3FSCommonConfig(
            access_key_id="k", secret_access_key="s",
            end_point="http://minio:9000", region="us-east-1", allow_http=True,
        )
        factory = ConfigurableArrowIOManager(
            uri_base="s3://lake/a", fs=S3FSConfig(common=common)
        )
        inner = factory.create_io_manager(None)
        import pyarrow as pa

        md = inner.get_metadata(
            build_output_context(asset_key=["t"]), pa.table({"id": [1]})
        )
        out.append((
            "arrow",
            md["destination_name"].text,
            factory.physical_coordinates(["t"])["source_type"],
        ))
    except ImportError:
        pass

    try:
        from dag_tools.io_managers.duckdb import ConfigurableDuckDBIOManager
        from dag_tools.resources.duckdb import DuckDBResource

        factory = ConfigurableDuckDBIOManager(
            duckdb=DuckDBResource(), uri_base="s3://lake/d"
        )
        out.append((
            "duckdb",
            factory.create_io_manager(None).get_metadata()["destination_name"].text,
            factory.physical_coordinates(["t"])["source_type"],
        ))
    except ImportError:
        pass

    try:
        from dagster import build_output_context
        import pyarrow as pa

        from dag_tools.io_managers.delta import (
            ConfigurableDeltaIOManager,
            S3FSCommonConfig as DC,
            S3FSConfig as DS3,
        )

        factory = ConfigurableDeltaIOManager(
            uri_base="s3://lake/dl",
            fs=DS3(common=DC(
                access_key_id="k", secret_access_key="s",
                end_point="http://minio:9000", region="us-east-1", allow_http=True,
            )),
        )
        md = factory.create_io_manager(None).get_metadata(
            build_output_context(asset_key=["t"]), pa.table({"id": [1]})
        )
        out.append((
            "delta",
            md["destination_name"].text,
            factory.physical_coordinates(["t"])["source_type"],
        ))
    except ImportError:
        pass

    try:
        from dag_tools.io_managers.sql import ConfigurableSQLIOManager, SQLConfig

        factory = ConfigurableSQLIOManager(
            config=SQLConfig(
                protocol="postgres", host="h", port=5432,
                database="db", username="u", password="p",
            )
        )
        captured = {}

        class Ctx:
            def add_output_metadata(self, md):
                captured.update(md)

        import pandas as pd

        factory.create_io_manager(None)._emit_output_metadata(
            Ctx(), pd.DataFrame({"id": [1]})
        )
        out.append((
            "sql",
            captured["destination_name"],
            factory.physical_coordinates(["t"])["source_type"],
        ))
    except ImportError:
        pass

    return out


PRODUCERS = _declared_and_advertised()


def test_every_producer_was_collected():
    """Name them explicitly rather than just asserting non-empty.

    The collector swallows ImportError so an absent optional dependency
    does not fail the file -- but that also means a renamed symbol makes a
    producer quietly disappear from the matrix, and the suite still passes
    with fewer cases than it claims to cover. (It happened: renaming
    _SOURCE_TYPE to SOURCE_TYPE silently dropped arrow.)
    """
    collected = {label for label, _, _ in PRODUCERS}
    expected = {"arrow", "duckdb", "delta", "sql"}
    missing = expected - collected
    assert not missing, (
        f"producers missing from the matrix: {sorted(missing)} — an import "
        f"failed silently, so they are not actually being checked"
    )


@pytest.mark.parametrize(
    "label,declared,advertised", PRODUCERS, ids=[p[0] for p in _declared_and_advertised()]
)
def test_declared_platform_equals_advertised_source_type(label, declared, advertised):
    """One string, two consumers. The catalog and the mesh routing ticket
    must agree about what an asset is, so both read the same constant."""
    assert declared == advertised, (
        f"{label}: catalog says {declared!r}, mesh ticket says {advertised!r}"
    )


@pytest.mark.parametrize(
    "label,declared,advertised", PRODUCERS, ids=[p[0] for p in _declared_and_advertised()]
)
def test_declared_platform_resolves_to_a_real_platform(label, declared, advertised):
    """Every producer must land somewhere real -- not 'unknown', which is
    where Delta and SQL assets went before they declared anything."""
    platform = resolve_platform(declared)
    assert platform != UNKNOWN_PLATFORM, f"{label} resolved to unknown"
    assert platform in KNOWN_DATAHUB_PLATFORMS, f"{label} -> {platform!r}"


# ---------------------------------------------------------------------------
# Key / URN / path must be three views of one fact
# ---------------------------------------------------------------------------


def test_key_urn_and_path_agree_for_a_location_encoding_key():
    """The whole point of prefixing an asset key with
    <platform_instance>/<bucket>: what Dagster calls the asset, what
    DataHub calls the dataset, and where the bytes live all derive from
    one string.

    A DataHub s3 recipe with `platform_instance: minio-svc` over
    `s3://publog-lake/publog/{table}/*` discovers exactly the URN below,
    so the crawled entity and the emitted one converge instead of
    becoming two disconnected halves of one table."""
    from dag_tools.components.datahub_lineage.component import (
        asset_keys_to_dataset_urn_converter as to_urn,
    )
    from dag_tools.io_managers.duckdb import asset_uri, split_endpoint_instance

    endpoint = "http://minio-svc.namespace.svc.cluster.local:9000"
    instance = split_endpoint_instance(endpoint)
    assert instance == "minio-svc"

    key = [instance, "publog-lake", "publog", "p_cage"]

    assert to_urn(key, platform="s3").urn() == (
        "urn:li:dataset:(urn:li:dataPlatform:s3,"
        "minio-svc.publog-lake/publog/p_cage,PROD)"
    )
    assert asset_uri("s3://publog-lake", key, key_encodes_location=True) == (
        "s3://publog-lake/publog/p_cage/"
    )


def test_the_original_dlt_convention_is_unchanged():
    """Regression guard for the shape that already works in production:
    dlt/<instance>/<bucket>/<path> -> <instance>.<bucket>/<path>."""
    from dag_tools.components.datahub_lineage.component import (
        asset_keys_to_dataset_urn_converter as to_urn,
    )

    key = ["dlt", "minio-svc", "staging", "vdspc_axi", "dbo", "board_mapping"]
    assert to_urn(key, platform="s3").urn() == (
        "urn:li:dataset:(urn:li:dataPlatform:s3,"
        "minio-svc.staging/vdspc_axi/dbo/board_mapping,PROD)"
    )


def test_bucket_mismatch_between_key_and_uri_base_is_refused():
    """Silently writing to the wrong bucket is the worst failure here."""
    import pytest as _pytest

    from dag_tools.io_managers.duckdb import asset_uri

    with _pytest.raises(ValueError, match="refusing to guess"):
        asset_uri(
            "s3://some-other-bucket",
            ["minio-svc", "publog-lake", "publog", "p_cage"],
            key_encodes_location=True,
        )
