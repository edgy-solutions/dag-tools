"""The synthetic earned-value fixture, as a consumer of the asset would see it.

A reader selects these columns by name and computes over them, so what is worth
pinning is the shape after the parquet round trip rather than the CSV on disk: a
control account such as ``1.1`` is the failure an inferring reader would produce,
and it only shows up once the frame has been built.

The figures are not pinned here. They are whatever the CSV says. What is pinned
is which level of the fixture each figure belongs to, because that is what a
reader has to know to compute with it: a work-package figure summed over the
work package's periods is counted once per period, and the sum still looks
like money.

No test names a program. Nothing in this repository should.
"""
from __future__ import annotations

import types

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("polars")
pytest.importorskip("pandas")   # arrow.py imports pandas at module level
pytest.importorskip("s3fs")     # ...and s3fs

import polars as pl
from dagster import materialize

from dag_tools.domain_broker.main import physical_urn_for
from dag_tools.io_managers.arrow import ConfigurableArrowIOManager, LocalFSConfig
from dag_tools.user_deployment.mesh_demo_assets import (
    EV_FIXTURE_CSV,
    EV_FIXTURE_FLOAT_COLUMNS,
    EV_FIXTURE_INTEGER_COLUMNS,
    EV_FIXTURE_STRING_COLUMNS,
    build_demo_defs,
    ev_fixture,
)

COLUMNS = EV_FIXTURE_STRING_COLUMNS + EV_FIXTURE_INTEGER_COLUMNS + EV_FIXTURE_FLOAT_COLUMNS


@pytest.fixture(scope="module")
def frame(tmp_path_factory) -> pl.DataFrame:
    """The rows as a reader gets them: materialized, then read back from parquet."""
    root = tmp_path_factory.mktemp("ev_fixture")
    iom = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(root))
    assert materialize([ev_fixture], resources={"mesh_demo_io": iom}).success
    written = list(root.rglob("*.parquet"))
    assert written, "the asset produced no parquet"
    return pl.read_parquet(written)


def test_the_columns_are_the_contract(frame):
    assert sorted(frame.columns) == sorted(COLUMNS)


def test_the_types_a_reader_gets(frame):
    """Written out rather than read from the asset's tuples, which would agree with any move."""
    assert dict(frame.schema) == {
        "program_id": pl.String,
        "program_name": pl.String,
        "program_bac": pl.Int64,
        "control_account": pl.String,
        "work_package": pl.String,
        "bac": pl.Int64,
        "eac": pl.Int64,
        "period": pl.String,
        "bcws": pl.Int64,
        "bcwp": pl.Float64,
        "acwp": pl.Float64,
    }


def test_no_value_is_null(frame):
    assert frame.null_count().sum_horizontal().item() == 0


def test_one_row_per_work_package_and_period(frame):
    assert frame.select("work_package", "period").is_duplicated().sum() == 0


def test_every_work_package_reports_every_period(frame):
    packages = frame["work_package"].n_unique()
    periods = frame["period"].n_unique()
    assert packages > 1 and periods > 1
    assert frame.height == packages * periods


def test_a_program_has_one_name_and_one_budget(frame):
    per_program = frame.group_by("program_id").agg(
        pl.col("program_name").n_unique().alias("names"),
        pl.col("program_bac").n_unique().alias("budgets"),
    )
    assert per_program["names"].to_list() == [1] * per_program.height
    assert per_program["budgets"].to_list() == [1] * per_program.height


def test_a_work_package_has_one_control_account(frame):
    per_package = frame.group_by("work_package").agg(
        pl.col("control_account").n_unique().alias("accounts")
    )
    assert per_package["accounts"].to_list() == [1] * per_package.height


# ---------------------------------------------------------------------------
# The completion figures. Each is a work-package figure at a status date, so it
# repeats on every one of that work package's rows -- the same way
# ``program_bac`` repeats on every row of the program.
# ---------------------------------------------------------------------------
COMPLETION_FIGURES = ("bac", "eac")


@pytest.mark.parametrize("column", COMPLETION_FIGURES)
def test_a_completion_figure_is_the_same_on_every_row_of_its_work_package(frame, column):
    """A reader takes the value once per work package, so there has to be one to take."""
    per_package = frame.group_by("work_package").agg(
        pl.col(column).n_unique().alias("values")
    )
    assert per_package["values"].to_list() == [1] * per_package.height


@pytest.mark.parametrize("column", COMPLETION_FIGURES)
def test_a_completion_figure_cannot_be_non_finite(frame, column):
    """Integers, so no arithmetic a reader does with them can produce a NaN or an
    infinity. The schema test above pins the type; this one says what the type buys."""
    assert frame.schema[column].is_integer()


def test_the_work_package_budgets_sum_to_the_program_budget_once_each(frame):
    """The roll-up holds over work packages, and only over work packages.

    Summed over rows instead it comes to one program budget per period: a figure
    that is wrong by a factor of six here and still conserves, which is why the
    level a figure belongs to is part of the contract rather than a detail.
    """
    program_bac = frame["program_bac"].unique().item()
    per_package = frame.group_by("work_package").agg(pl.col("bac").first())
    assert per_package["bac"].sum() == program_bac

    periods = frame["period"].n_unique()
    assert frame["bac"].sum() == program_bac * periods


def test_the_fixture_has_a_work_package_on_either_side_of_its_budget(frame):
    """Both signs of a completion variance, so a reader of this fixture meets both."""
    per_package = frame.group_by("work_package").agg(
        pl.col("bac").first(), pl.col("eac").first()
    )
    variance = (per_package["eac"] - per_package["bac"]).to_list()
    assert any(v > 0 for v in variance), "no work package forecasts an overrun"
    assert any(v < 0 for v in variance), "no work package forecasts an underrun"


def test_no_completion_estimate_is_below_the_cost_already_booked(frame):
    """An estimate to complete the work for less than has already been spent on it
    would be incoherent, and a reader computing a remaining cost would go negative."""
    per_package = frame.group_by("work_package").agg(
        pl.col("eac").first(), pl.col("acwp").sum().alias("booked")
    )
    below = per_package.filter(pl.col("eac") < pl.col("booked"))
    assert below.height == 0, below.to_dicts()


def test_the_csv_ships_inside_the_package():
    """The wheel packages ``dag_tools`` alone, so the file must sit beneath it."""
    parts = EV_FIXTURE_CSV.parts
    assert "dag_tools" in parts and EV_FIXTURE_CSV.is_file()


def test_the_demo_definitions_carry_it():
    keys = {key.to_user_string() for key in build_demo_defs().resolve_asset_graph().get_all_asset_keys()}
    assert {"ev_fixture", "mesh_demo_customers"} <= keys


def test_a_deployment_with_demo_mode_off_does_not_have_it(monkeypatch):
    from dag_tools.user_deployment import definitions

    monkeypatch.delenv("DAG_TOOLS_GRIST_CONFIG", raising=False)
    monkeypatch.delenv("DATAHUB_SERVER", raising=False)

    def keys() -> set[str]:
        graph = definitions._build_combined_defs().resolve_asset_graph()
        return {key.to_user_string() for key in graph.get_all_asset_keys()}

    monkeypatch.setenv("DAG_TOOLS_DEMO_MODE", "true")
    assert "ev_fixture" in keys()  # the control: the same call, the switch on
    monkeypatch.delenv("DAG_TOOLS_DEMO_MODE")
    assert "ev_fixture" not in keys()


def test_the_broker_and_the_catalog_name_it_alike(monkeypatch):
    """The URN a grant has to name is this one, so it is derived and never typed."""
    datahub = pytest.importorskip(
        "dag_tools.components.datahub_lineage.component",
        reason="datahub plugin absent — the converter under test cannot be imported",
    )
    from dag_tools.components.datahub_lineage.platforms import (
        FILESYSTEM_PLATFORMS,
        resolve_platform,
    )

    monkeypatch.setenv("DAG_TOOLS_DEMO_S3_BUCKET", "a-bucket")
    monkeypatch.setenv("MESH_DEMO_PREFIX", "a-prefix")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://object-store:9000")
    io_manager = build_demo_defs().resources["mesh_demo_io"]
    record = types.SimpleNamespace(asset_key=["ev_fixture"], io_manager_key="mesh_demo_io")

    broker_urn = physical_urn_for(record, io_manager)
    catalog_urn = datahub.asset_keys_to_dataset_urn_converter(
        ["ev_fixture"],
        platform=resolve_platform("s3_parquet"),
        filesystem_platforms=list(FILESYSTEM_PLATFORMS),
    ).urn()

    assert broker_urn == catalog_urn
    assert ":s3," in broker_urn
