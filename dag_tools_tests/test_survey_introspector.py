"""Tests for the per-Definitions introspection primitives — sensors,
schedules, asset checks, io-manager summary."""
import pytest

pytest.importorskip("dagster")

from dag_tools.qual.survey.introspector import (
    introspect_assets,
    introspect_automation,
    introspect_dbt_projects,
    summarize_io_managers,
)


def _build_defs_with_automation():
    """A Definitions with one asset, one sensor, one schedule, one asset check."""
    from dagster import (
        AssetCheckResult,
        AssetSelection,
        Definitions,
        InMemoryIOManager,
        RunRequest,
        ScheduleDefinition,
        SensorDefinition,
        SkipReason,
        asset,
        asset_check,
        define_asset_job,
        sensor,
    )

    @asset
    def hello():
        return 1

    @asset_check(asset="hello")
    def hello_is_nonzero(hello):
        return AssetCheckResult(passed=hello != 0)

    hello_job = define_asset_job("hello_job", selection=AssetSelection.assets(hello))

    @sensor(job=hello_job, minimum_interval_seconds=30)
    def hello_sensor(context):
        return SkipReason("not now")

    hello_schedule = ScheduleDefinition(
        name="daily_hello",
        job=hello_job,
        cron_schedule="0 9 * * *",
        execution_timezone="America/New_York",
    )

    return Definitions(
        assets=[hello],
        asset_checks=[hello_is_nonzero],
        sensors=[hello_sensor],
        schedules=[hello_schedule],
        jobs=[hello_job],
        resources={"io_manager": InMemoryIOManager()},
    )


def test_introspect_assets_returns_records_and_manifest():
    defs = _build_defs_with_automation()
    records, manifest = introspect_assets(defs, location="loc1")
    assert len(records) == 1
    assert records[0].asset_key == ["hello"]
    assert records[0].location == "loc1"
    assert manifest.records[0]["asset_key"] == ["hello"]
    assert manifest.inventory_schema_version >= 1


def test_introspect_automation_finds_sensors_schedules_checks():
    defs = _build_defs_with_automation()
    auto = introspect_automation(defs, location="loc1")
    assert len(auto.sensors) == 1
    assert auto.sensors[0].name == "hello_sensor"
    assert auto.sensors[0].minimum_interval_seconds == 30
    assert auto.sensors[0].location == "loc1"
    assert auto.sensors[0].sensor_type  # some class name

    assert len(auto.schedules) == 1
    sched = auto.schedules[0]
    assert sched.name == "daily_hello"
    assert sched.cron_schedule == "0 9 * * *"
    assert sched.execution_timezone == "America/New_York"

    assert len(auto.asset_checks) == 1
    chk = auto.asset_checks[0]
    assert chk.asset_key == ["hello"]


def test_summarize_io_managers_groups_by_class_and_family():
    defs = _build_defs_with_automation()
    records, _ = introspect_assets(defs)
    summary = summarize_io_managers(records)
    assert len(summary.entries) == 1
    entry = summary.entries[0]
    assert entry.asset_count == 1
    assert entry.io_manager_class is not None
    assert entry.io_manager_class.endswith("InMemoryIOManager")
    assert entry.asset_keys_sample == [["hello"]]


def test_introspect_dbt_projects_returns_empty_when_no_dbt_resource():
    """defs without any DbtCliResource → empty list, no error."""
    defs = _build_defs_with_automation()
    result = introspect_dbt_projects(defs)
    assert result.projects == []


def test_introspect_automation_handles_empty_defs():
    """A Definitions with no sensors/schedules/checks → empty inventories."""
    from dagster import Definitions, InMemoryIOManager, asset

    @asset
    def lonely():
        return 1

    defs = Definitions(
        assets=[lonely], resources={"io_manager": InMemoryIOManager()}
    )

    auto = introspect_automation(defs)
    assert auto.sensors == []
    assert auto.schedules == []
    assert auto.asset_checks == []
