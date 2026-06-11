"""End-to-end tests for ``run_survey`` — load gate, publish ordering,
and the critical recipe invariant: nothing is published when any code
location fails to load.
"""
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("dagster")
pytest.importorskip("moto")
pytest.importorskip("boto3")

import boto3
from moto import mock_aws

from dag_tools.qual.registry import (
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)
from dag_tools.qual.survey import run_survey


BUCKET = "dag-tools-survey-test"


@pytest.fixture
def registry():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


@pytest.fixture
def good_module(tmp_path: Path) -> Path:
    """A loadable Definitions with sensors / schedules / checks for end-to-end
    publish exercise."""
    py = tmp_path / "g.py"
    py.write_text(textwrap.dedent("""
        from dagster import (
            AssetCheckResult,
            Definitions,
            InMemoryIOManager,
            ScheduleDefinition,
            SkipReason,
            asset,
            asset_check,
            define_asset_job,
            sensor,
        )

        @asset
        def hello():
            return 1

        @asset_check(asset='hello')
        def hello_ok(hello):
            return AssetCheckResult(passed=True)

        hello_job = define_asset_job('hello_job', selection=[hello.key])

        @sensor(job=hello_job)
        def hello_sensor(context):
            return SkipReason('not now')

        hello_schedule = ScheduleDefinition(
            name='daily_hello',
            job=hello_job,
            cron_schedule='0 9 * * *',
            execution_timezone='America/New_York',
        )

        defs = Definitions(
            assets=[hello],
            asset_checks=[hello_ok],
            sensors=[hello_sensor],
            schedules=[hello_schedule],
            jobs=[hello_job],
            resources={'io_manager': InMemoryIOManager()},
        )
    """))
    return py


@pytest.fixture
def broken_module(tmp_path: Path) -> Path:
    py = tmp_path / "broken.py"
    py.write_text("raise RuntimeError('survey publisher load-fail fixture')\n")
    return py


# ---------------------------------------------------------------------------
# Happy path: end-to-end publish.
# ---------------------------------------------------------------------------


def test_run_survey_publishes_all_artifacts_and_pointer(registry, good_module):
    outcome = run_survey(
        locations_spec=str(good_module),
        repo="patriot",
        git_sha="abc123",
        registry=registry,
        build_id="build-42",
        dagster_version="1.13.1",
        dagtools_version="0.1.0",
    )
    assert outcome.published is True
    assert outcome.load_validation.loads is True
    assert outcome.pointer_sha == "abc123"

    # Pointer is reachable.
    pointer = registry.read_latest_pointer("patriot")
    assert pointer is not None and pointer.git_sha == "abc123"
    assert pointer.build_id == "build-42"

    # Every expected artifact is present.
    for filename in (
        layout.META_FILE,
        layout.ASSETS_FILE,
        layout.AUTOMATION_FILE,
        layout.IO_MANAGERS_FILE,
        layout.DBT_PROJECTS_FILE,
        layout.LOAD_VALIDATION_FILE,
    ):
        body = registry.read_build_artifact("patriot", "abc123", filename)
        assert body is not None, f"missing artifact: {filename}"


def test_run_survey_captures_sensor_and_schedule_in_automation(registry, good_module):
    outcome = run_survey(
        locations_spec=str(good_module),
        repo="patriot",
        git_sha="abc123",
        registry=registry,
    )
    automation = registry.read_build_json("patriot", "abc123", layout.AUTOMATION_FILE)
    assert automation is not None
    sensor_names = [s["name"] for s in automation["sensors"]]
    schedule_names = [s["name"] for s in automation["schedules"]]
    assert "hello_sensor" in sensor_names
    assert "daily_hello" in schedule_names


# ---------------------------------------------------------------------------
# The recipe's load-gate invariant. THE test.
# ---------------------------------------------------------------------------


def test_run_survey_refuses_to_publish_when_load_fails(registry, broken_module):
    """Per the recipe: a load failure means **nothing is published**."""
    outcome = run_survey(
        locations_spec=str(broken_module),
        repo="patriot",
        git_sha="abc123",
        registry=registry,
    )
    assert outcome.published is False
    assert outcome.load_validation.loads is False
    assert len(outcome.load_validation.failures) == 1
    assert "RuntimeError" in outcome.load_validation.failures[0].error

    # Critically: NOTHING was published. No artifacts, no pointer.
    assert registry.read_latest_pointer("patriot") is None
    for filename in (
        layout.META_FILE,
        layout.ASSETS_FILE,
        layout.AUTOMATION_FILE,
        layout.IO_MANAGERS_FILE,
        layout.DBT_PROJECTS_FILE,
        layout.LOAD_VALIDATION_FILE,
    ):
        assert registry.read_build_artifact("patriot", "abc123", filename) is None, (
            f"registry should be untouched on load failure but found {filename}"
        )


def test_run_survey_partial_load_failure_also_refuses_publish(
    registry, good_module, broken_module, tmp_path
):
    """One workspace.yaml with two locations: one loads, the other doesn't.
    The recipe's contract is binary: any load failure means refuse publish.
    """
    workspace = tmp_path / "workspace.yaml"
    workspace.write_text(textwrap.dedent(f"""
        load_from:
          - python_file: {{ relative_path: {good_module.name}, location_name: good }}
          - python_file: {{ relative_path: {broken_module.name}, location_name: broken }}
    """))
    outcome = run_survey(
        locations_spec=str(workspace),
        repo="patriot",
        git_sha="abc123",
        registry=registry,
    )
    assert outcome.published is False
    assert outcome.load_validation.loads is False
    # The successful location is still reported in load_validation.locations
    # so operators can see partial progress.
    assert any(l.name == "good" for l in outcome.load_validation.locations)
    assert any(f.name == "broken" for f in outcome.load_validation.failures)
    # But nothing landed in the registry.
    assert registry.read_latest_pointer("patriot") is None


def test_run_survey_immutable_for_same_sha(registry, good_module):
    """Re-publishing the same SHA raises through publish_build's immutability."""
    run_survey(
        locations_spec=str(good_module),
        repo="patriot", git_sha="abc123", registry=registry,
    )
    # Second invocation should fail at the immutable-write boundary —
    # publisher does not silently swallow this.
    from dag_tools.qual.registry import ImmutableKeyExists
    with pytest.raises(ImmutableKeyExists):
        run_survey(
            locations_spec=str(good_module),
            repo="patriot", git_sha="abc123", registry=registry,
        )


def test_run_survey_skip_publish_runs_introspection_but_writes_nothing(
    registry, good_module
):
    outcome = run_survey(
        locations_spec=str(good_module),
        repo="patriot", git_sha="abc123",
        registry=registry, skip_publish=True,
    )
    # Introspection succeeded; publish was suppressed by request.
    assert outcome.published is False
    assert outcome.load_validation.loads is True
    assert registry.read_latest_pointer("patriot") is None
