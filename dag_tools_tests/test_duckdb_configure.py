"""``ConfigurableDuckDBIOManager`` must honour the ``ConfigureFromDict`` seam.

It mixed nothing in and defined no ``configure``, so a deployment that
configures its IO managers from a config file -- the way the Arrow, Delta,
S3 file and SQL managers and ``DuckDBResource`` itself are configured -- got
``AttributeError: type object 'ConfigurableDuckDBIOManager' has no attribute
'configure'`` at the first line of its code location. Reported, not fixed,
in the change that gave the Arrow manager its own ``configure``.
"""
import pytest

pytest.importorskip("duckdb")
from dagster import asset, materialize

from dag_tools.io_managers.duckdb import ConfigurableDuckDBIOManager
from dag_tools.resources.duckdb import DuckDBResource
from dag_tools.utils.helper import ConfigureFromDict


def test_the_inner_block_configures_the_manager_and_its_nested_resource():
    manager = ConfigurableDuckDBIOManager.configure({
        "duckdb": {"aws_region": "eu-west-1"},
        "uri_base": "s3://bucket/prefix",
        "compression": "zstd",
    })
    assert isinstance(manager, ConfigurableDuckDBIOManager)
    assert isinstance(manager.duckdb, DuckDBResource)
    assert manager.duckdb.aws_region == "eu-west-1"
    assert manager.uri_base == "s3://bucket/prefix"
    assert manager.compression == "zstd"


def test_a_pre_built_resource_is_accepted_as_it_is():
    resource = DuckDBResource(aws_region="ap-south-1")
    manager = ConfigurableDuckDBIOManager.configure({
        "duckdb": resource, "uri_base": "s3://bucket/prefix",
    })
    assert manager.duckdb.aws_region == "ap-south-1"


def test_the_outer_block_is_refused_rather_than_read():
    """``ConfigurableSQLIOManager.configure`` takes the OUTER block; this
    model has no ``config:`` wrapper field, so handing it that level must
    fail on the missing fields instead of building a manager from defaults."""
    with pytest.raises(Exception) as excinfo:
        ConfigurableDuckDBIOManager.configure({
            "config": {"duckdb": {}, "uri_base": "s3://bucket/prefix"},
        })
    assert "uri_base" in str(excinfo.value)


def test_the_method_is_the_classs_own_not_the_mixins_stub():
    """The control: the mixin's own ``configure`` raises NotImplementedError,
    so a class that mixed it in and stopped there would still pass a
    ``hasattr`` check and fail at the first call."""
    assert issubclass(ConfigurableDuckDBIOManager, ConfigureFromDict)
    assert (
        ConfigurableDuckDBIOManager.configure.__func__
        is not ConfigureFromDict.configure.__func__
    )
    with pytest.raises(NotImplementedError):
        ConfigureFromDict.configure({})


def test_a_configured_manager_writes_and_reads_back(tmp_path):
    """Built from a dict, the manager is the same manager: an asset written
    through it is read back by its downstream."""
    manager = ConfigurableDuckDBIOManager.configure({
        "duckdb": {}, "uri_base": str(tmp_path).replace("\\", "/"),
    })

    @asset(io_manager_key="iom")
    def written(duck: DuckDBResource):
        return duck.connect().sql("SELECT 1 AS id, 'a' AS region")

    # `read_back` keeps the default IO manager: only its INPUT goes through
    # the configured one, which arrives as a lazy relation.
    @asset
    def read_back(written) -> list:
        return written.fetchall()

    result = materialize(
        [written, read_back],
        resources={"iom": manager, "duck": DuckDBResource()},
    )
    assert result.success
    assert result.output_for_node("read_back") == [(1, "a")]
