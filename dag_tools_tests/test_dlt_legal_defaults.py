"""A legal default must not take out a node name or an asset group.

Two defects, found by loading a converted code location rather than by review.
Both are the same shape: a field that is ``Optional`` with a ``None`` default --
so leaving it unset is inside the contract -- was read as though it could not be
absent.

* ``DltAssetGroupConfig.name`` was interpolated raw into the Dagster op name, so
  a caller that set no name rendered the literal text ``"None"`` there. Two
  pipelines over one source schema then built the SAME op name and Dagster
  failed the WHOLE code location.
* ``get_destination_distinctions`` read four ``Optional[str]`` credential fields
  unguarded. ``AwsCredentials.endpoint_url`` is ``None`` for plain AWS S3 -- the
  ordinary case -- so the ordinary case raised ``TypeError``, and the caller's
  ``except`` swallowed it into a ``print``: a whole asset group vanished from the
  code location for a destination that was configured correctly.

The op-name tests force JOB construction, not just ``Definitions``. Measured on
the unfixed tree: ``Definitions(...)`` and ``get_repository_def()`` BOTH resolve
with colliding op names, and only ``get_all_jobs()`` raises. An assertion that
stopped at ``get_repository_def()`` would have passed before the fix.
"""
import re
import sqlite3

import pytest

pytest.importorskip("dlt")
pytest.importorskip("dagster_dlt")

from dagster import (  # noqa: E402
    AssetKey,
    AssetSpec,
    AssetsDefinition,
    Definitions,
    MaterializeResult,
    multi_asset,
)
from dagster._core.errors import DagsterInvalidDefinitionError  # noqa: E402
from dagster_dlt import DagsterDltResource  # noqa: E402
from dlt.common.configuration.specs.azure_credentials import AzureCredentials  # noqa: E402
from dlt.common.storages.configuration import AwsCredentials  # noqa: E402
from dlt.destinations.impl.databricks.configuration import DatabricksCredentials  # noqa: E402
from dlt.sources.credentials import ConnectionStringCredentials  # noqa: E402

from dag_tools.asset_wrappers import dlt_assets_parsing as parsing  # noqa: E402
from dag_tools.asset_wrappers.dlt_assets_factory import (  # noqa: E402
    CustomDagsterDltTranslator,
    DltAssetGroupConfig,
)


# --------------------------------------------------------------------------
# 1. the op name
# --------------------------------------------------------------------------
@pytest.fixture
def source_db(tmp_path):
    path = tmp_path / "source.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO widgets VALUES (1, 'a')")
    conn.commit()
    conn.close()
    return path


def _source(db, path):
    return {
        "type": "sql_database",
        "drivername": "sqlite",
        "credentials": f"sqlite:///{path}",
        "database": db,
        "schema": "main",
    }


def _dest(db, tmp_path):
    return {
        "type": "duckdb",
        "drivername": "duckdb",
        "credentials": str(tmp_path / f"{db}.duckdb"),
        "database": db,
        "schema": "raw",
    }


def _op_names(assets):
    # create_dlt_assets returns a MIX of AssetSpec and AssetsDefinition; the op
    # name lives only on the latter.
    return [a.op.name for a in assets if isinstance(a, AssetsDefinition)]


def _two_unnamed_groups(tmp_path, source_db):
    """Two pipelines, two source databases, the SAME source schema.

    ``_prepare_units`` keys its mapping on ``(database, schema)``, so this is one
    unit per call. The DESTINATION databases differ so the asset keys do not
    collide too -- what is under test is the op name alone.
    """
    a = parsing.create_dlt_assets(
        ["widgets"],
        _source("db_one", source_db),
        _dest("wh_one", tmp_path),
        DltAssetGroupConfig(dest_schema="raw"),
    )
    b = parsing.create_dlt_assets(
        ["widgets"],
        _source("db_two", source_db),
        _dest("wh_two", tmp_path),
        DltAssetGroupConfig(dest_schema="raw"),
    )
    return a, b


def test_two_groups_with_no_name_do_not_share_an_op_name(tmp_path, source_db):
    a, b = _two_unnamed_groups(tmp_path, source_db)
    assert _op_names(a) == ["dlt_db_one_main_asset"]
    assert _op_names(b) == ["dlt_db_two_main_asset"]
    assert "None" not in _op_names(a)[0]


def test_two_groups_with_no_name_still_build_the_code_locations_jobs(tmp_path, source_db):
    a, b = _two_unnamed_groups(tmp_path, source_db)
    repo = Definitions(
        assets=[*a, *b], resources={"dlt": DagsterDltResource()}
    ).get_repository_def()
    # The forcing call. get_repository_def() alone resolves even with colliding
    # op names (measured on the unfixed tree), so stopping there would be
    # vacuous; building the jobs is what raised the operator's error.
    assert [j.name for j in repo.get_all_jobs()] == ["__ASSET_JOB"]


def test_control_two_assets_sharing_an_op_name_really_do_fail_job_construction():
    """Without this the test above would pass under any op name at all.

    It plants the defect's shape directly: two definitions whose op names are
    equal, asset keys distinct. The error text is the operator's own.
    """

    def _defn(key):
        @multi_asset(name="dlt_None_main_asset", specs=[AssetSpec(key)])
        def _inner():
            return MaterializeResult(asset_key=key)

        return _inner

    repo = Definitions(assets=[_defn("one"), _defn("two")]).get_repository_def()
    with pytest.raises(DagsterInvalidDefinitionError) as exc:
        repo.get_all_jobs()
    assert "conflicting node definitions" in str(exc.value)
    assert "dlt_None_main_asset" in str(exc.value)


def test_a_group_that_names_itself_renders_exactly_what_it_named(tmp_path, source_db):
    """The change is confined to the None case (AGENTS.md §6).

    Every shipped component sets a name -- all three do
    ``name=pipeline_attrs.pop("name", pipeline_key)`` -- so this is the string
    every deployed caller gets, and it must be byte-identical to before.
    """
    assets = parsing.create_dlt_assets(
        ["widgets"],
        _source("db_one", source_db),
        _dest("wh_one", tmp_path),
        DltAssetGroupConfig(name="pipe", dest_schema="raw"),
    )
    assert _op_names(assets) == ["dlt_pipe_main_asset"]


# --------------------------------------------------------------------------
# 2. the destination distinctions
# --------------------------------------------------------------------------
class _Destination:
    """The one attribute ``get_destination_distinctions`` reads.

    It is a ``@staticmethod``, so no translator has to be constructed.
    """

    def __init__(self, **config_params):
        self.config_params = config_params


_distinctions = CustomDagsterDltTranslator.get_destination_distinctions


def test_plain_aws_s3_has_no_endpoint_and_is_not_an_error():
    """``endpoint_url`` is None for ordinary AWS S3 and set only for an
    S3-COMPATIBLE store, so the common case was the failing one."""
    creds = AwsCredentials()
    assert creds.endpoint_url is None
    assert _distinctions(
        _Destination(credentials=creds, bucket_url="s3://my-bucket/prefix")
    ) == ["my-bucket"]


def test_control_the_old_shape_raised_on_that_same_credential():
    """Re-implements the old unguarded read locally.

    Without this the test above would pass under any guard whatsoever and the
    actual shape of the defect would be recorded nowhere.
    """
    creds = AwsCredentials()
    with pytest.raises(TypeError) as exc:
        re.search(r"[^:]*://([^\.]*)\..*", creds.endpoint_url)
    assert "NoneType" in str(exc.value)


def test_an_s3_compatible_endpoint_still_prefixes_the_asset_keys():
    """Nothing that resolves today moves: the endpoint component is still
    derived exactly as before when the field is set."""
    creds = AwsCredentials()
    creds.endpoint_url = "https://minio.example.test:9000"
    assert _distinctions(
        _Destination(credentials=creds, bucket_url="s3://my-bucket/prefix")
    ) == ["minio", "my-bucket"]


def test_an_azure_bucket_url_that_matches_is_read_exactly_as_before():
    assert _distinctions(
        _Destination(
            credentials=AzureCredentials(),
            bucket_url="az://container@acct.blob.zone/path/x",
        )
    ) == ["acct.blob.zone", "container", "path"]


@pytest.mark.parametrize(
    "credentials, field",
    [
        (AzureCredentials(), "azure_account_host"),
        (DatabricksCredentials(), "server_hostname"),
        (ConnectionStringCredentials(), "host"),
    ],
)
def test_an_absent_prefix_field_is_refused_by_name(credentials, field):
    """These three arms REFUSE rather than recover, and the message names the
    field.

    There is no second field to fall back to, and what comes back is spliced
    into asset keys -- so a stand-in would name something the destination does
    not have. The caller swallows this into a ``print`` and that print is all an
    operator sees, which is why it has to say which field was empty.
    """
    assert getattr(credentials, field) is None
    with pytest.raises(ValueError) as exc:
        _distinctions(_Destination(credentials=credentials, bucket_url=None))
    assert field in str(exc.value)


def test_control_a_none_prefix_would_not_have_built_an_asset_key_anyway():
    """Why refusing is not a behaviour loss.

    The azure arm already fell back to ``[creds.azure_account_host]``, so an
    absent host produced ``["dlt", None, ...]`` -- which AssetKey refuses with a
    CheckError raised far from the field that was empty.
    """
    from dagster._check import CheckError

    with pytest.raises(CheckError):
        AssetKey(["dlt", None, "x"])
