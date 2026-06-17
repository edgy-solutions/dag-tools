"""End-to-end tests for run_side — moto-backed registry + mocked
DagsterGraphQLClient. Exercises the resumability invariants the recipe
calls out.
"""
import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")

import boto3
from moto import mock_aws

from dag_tools.inventory import SCHEMA_VERSION as INV_VER
from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    EventLogEntry,
    RunStatusInfo,
)
from dag_tools.qual.qualify import (
    Deployment,
    VersionTarget,
    create_qualification,
)
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)
from dag_tools.qual.runs import RepStatus, run_side


BUCKET = "dag-tools-runs-test"


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        registry = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        yield registry, tmp_path


def _seed_qual(registry, runnability_tags=None, tag_map=None):
    """Publish one inventory, init qual, build + publish class matrix.
    Returns the qual_id."""
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    tags = tag_map or {}
    rec = {
        "schema_version": INV_VER,
        "asset_key": ["hello"],
        "compute_kind": "python",
        "io_manager_key": "io_manager",
        "io_manager_class": "dagster.InMemoryIOManager",
        "io_manager_family": "in_memory",
        "partitions_def_class": None,
        "partition_mapping_classes": [],
        "resource_keys": ["io_manager"],
        "resource_classes": {"io_manager": "dagster.InMemoryIOManager"},
        "integration_libs": [],
        "has_asset_checks": False,
        "automation_condition_type": None,
        "tags": tags,
    }
    artifacts = {
        layout.ASSETS_FILE: json.dumps({
            "schema_version": 1,
            "inventory_schema_version": INV_VER,
            "records": [rec],
        }).encode("utf-8"),
        layout.AUTOMATION_FILE: b'{"schema_version":1,"sensors":[],"schedules":[],"asset_checks":[]}',
        layout.IO_MANAGERS_FILE: b'{"schema_version":1,"entries":[]}',
        layout.DBT_PROJECTS_FILE: b'{"schema_version":1,"projects":[]}',
        layout.LOAD_VALIDATION_FILE: b'{"schema_version":1,"timestamp":"2026-06-15T12:00:00+00:00","loads":true,"locations":[],"failures":[],"warnings":[]}',
    }
    registry.publish_build(
        repo="alpha", git_sha="sha-alpha", artifacts=artifacts,
        meta=BuildMeta(repo="alpha", git_sha="sha-alpha", timestamp=when),
    )
    create_qualification(
        qual_id="q-test", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://dagster-test/graphql"),
    )
    matrix = build_class_matrix("q-test", registry=registry)
    publish_class_matrix(matrix, registry=registry)
    return "q-test"


def _fake_client(
    *,
    launch_run_id: str = "run-1",
    statuses: Optional[List[RunStatusInfo]] = None,
    events: Optional[List[EventLogEntry]] = None,
) -> DagsterGraphQLClient:
    """Build a mocked DagsterGraphQLClient with canned behavior."""
    client = MagicMock(spec=DagsterGraphQLClient)
    client.launch_asset_run.return_value = launch_run_id

    if statuses is None:
        statuses = [RunStatusInfo(run_id=launch_run_id, status="SUCCESS",
                                  start_time=0.0, end_time=10.0)]
    if events is None:
        events = [
            EventLogEntry(
                event_type="MaterializationEvent",
                asset_key=["hello"], step_key="step1",
                metadata_keys=["row_count"],
            )
        ]
    # poll_to_completion returns the LAST status (assumed terminal).
    client.poll_to_completion.return_value = statuses[-1]
    client.get_event_log.return_value = events
    client.get_run_status.side_effect = lambda rid: statuses[-1]
    client.close.return_value = None
    return client


def _factory(client):
    return lambda manifest: client


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_run_side_launches_polls_and_persists_record(setup):
    registry, _ = setup
    qual_id = _seed_qual(registry)

    client = _fake_client()
    outcome = run_side(
        qual_id, "baseline",
        registry=registry,
        client_factory=_factory(client),
        poll_interval_seconds=0,
        sleep=lambda _: None,
    )
    assert outcome.summary.passed == 1
    assert outcome.summary.failed == 0
    # Run record persisted at the recipe-specified key.
    state = outcome.state
    rep = next(iter(state.reps.values()))
    body = registry.read_run_record(qual_id, "baseline", rep.class_hash, "run-1")
    assert body is not None
    rec = json.loads(body)
    assert rec["success"] is True
    assert rec["asset_key"] == ["hello"]


def test_run_side_marks_failed_when_run_status_is_failure(setup):
    registry, _ = setup
    qual_id = _seed_qual(registry)

    client = _fake_client(
        statuses=[RunStatusInfo(run_id="run-1", status="FAILURE",
                                start_time=0.0, end_time=10.0)],
        events=[
            EventLogEntry(
                event_type="ExecutionStepFailureEvent",
                step_key="bad_step", message="boom",
            )
        ],
    )
    outcome = run_side(
        qual_id, "baseline",
        registry=registry,
        client_factory=_factory(client),
        sleep=lambda _: None,
    )
    assert outcome.summary.failed == 1
    assert outcome.summary.passed == 0
    rep = next(iter(outcome.state.reps.values()))
    assert rep.status == RepStatus.FAILED


# ---------------------------------------------------------------------------
# Resumability — THE recipe invariant
# ---------------------------------------------------------------------------


def test_run_side_skips_passed_reps_on_re_invocation(setup):
    """First invocation passes the rep. Second invocation must NOT
    re-launch it — the recipe's "re-invocation processes only non-passed
    entries" rule."""
    registry, _ = setup
    qual_id = _seed_qual(registry)

    first_client = _fake_client()
    run_side(
        qual_id, "baseline",
        registry=registry,
        client_factory=_factory(first_client),
        sleep=lambda _: None,
    )
    first_launch_calls = first_client.launch_asset_run.call_count
    assert first_launch_calls == 1

    second_client = _fake_client()
    outcome = run_side(
        qual_id, "baseline",
        registry=registry,
        client_factory=_factory(second_client),
        sleep=lambda _: None,
    )
    # No new launches: the rep was already PASSED.
    assert second_client.launch_asset_run.call_count == 0
    assert outcome.summary.passed == 1


def test_run_side_reconciles_launched_via_poll_not_relaunch(setup):
    """Simulate a desktop crash: state has a rep stuck in LAUNCHED with a
    run_id. The next invocation MUST poll that run_id, not launch a new run."""
    registry, _ = setup
    qual_id = _seed_qual(registry)

    # Pre-seed state with one rep already LAUNCHED.
    matrix_body = registry.read_qualification_classes_json(qual_id)
    matrix = json.loads(matrix_body)
    cls = matrix["classes"][0]
    rep = cls["representatives"][0]
    pre_state = {
        "schema_version": 1,
        "qual_id": qual_id, "side": "baseline",
        "started_at": "2026-06-15T00:00:00+00:00",
        "updated_at": "2026-06-15T00:00:00+00:00",
        "reps": {
            f"{cls['class_hash']}:{'/'.join(rep['asset_key'])}": {
                "rep_id": f"{cls['class_hash']}:{'/'.join(rep['asset_key'])}",
                "class_hash": cls["class_hash"],
                "asset_key": rep["asset_key"],
                "repo": rep["repo"],
                "git_sha": rep["git_sha"],
                "runnability": rep["runnability"],
                "status": "launched",
                "run_id": "stranded-run-id",
                "attempts": 1,
            }
        },
    }
    registry.put_side_state(qual_id, "baseline", json.dumps(pre_state).encode())

    client = _fake_client(launch_run_id="should-not-be-called")
    outcome = run_side(
        qual_id, "baseline",
        registry=registry,
        client_factory=_factory(client),
        sleep=lambda _: None,
    )
    # The launcher was NOT called — we reconciled via the existing run_id.
    assert client.launch_asset_run.call_count == 0
    # poll_to_completion + get_event_log WERE called against the stranded id.
    assert client.poll_to_completion.call_count == 1
    poll_call = client.poll_to_completion.call_args
    assert poll_call.args[0] == "stranded-run-id"
    assert outcome.summary.passed == 1


def test_run_side_state_is_mirrored_to_registry_after_each_transition(setup):
    """The registry-mirrored state file must end up consistent with the
    operator's local copy after the run finishes."""
    registry, home = setup
    qual_id = _seed_qual(registry)

    client = _fake_client()
    run_side(
        qual_id, "baseline",
        registry=registry,
        client_factory=_factory(client),
        sleep=lambda _: None,
    )

    registry_body = registry.read_side_state(qual_id, "baseline")
    assert registry_body is not None
    registry_state = json.loads(registry_body)
    assert all(
        r["status"] == "passed" for r in registry_state["reps"].values()
    )

    local_path = home / "quals" / qual_id / "baseline-state.json"
    assert local_path.exists()
    local_state = json.loads(local_path.read_text())
    assert local_state["reps"] == registry_state["reps"]


# ---------------------------------------------------------------------------
# Runnability filtering — non-runnable reps get SKIPPED
# ---------------------------------------------------------------------------


def test_run_side_skips_synthetic_required_reps(setup):
    """Synthetic_required is a Q5 concern; Q2 leaves them as SKIPPED."""
    registry, _ = setup
    qual_id = _seed_qual(
        registry,
        tag_map={"synthetic_required": "true"},
    )
    client = _fake_client()
    outcome = run_side(
        qual_id, "baseline",
        registry=registry,
        client_factory=_factory(client),
        sleep=lambda _: None,
    )
    assert client.launch_asset_run.call_count == 0
    assert outcome.summary.skipped == 1


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_run_side_failed_launch_becomes_failed_status(setup):
    registry, _ = setup
    qual_id = _seed_qual(registry)

    client = MagicMock(spec=DagsterGraphQLClient)
    client.launch_asset_run.side_effect = DagsterGraphQLError("not allowed")
    client.close.return_value = None

    outcome = run_side(
        qual_id, "baseline",
        registry=registry,
        client_factory=_factory(client),
        sleep=lambda _: None,
    )
    assert outcome.summary.failed == 1
    rep = next(iter(outcome.state.reps.values()))
    assert "launch failed" in (rep.error or "")


def test_run_side_raises_when_classes_missing(setup):
    """Manifest exists but classes haven't been built yet: actionable error."""
    registry, _ = setup
    create_qualification(
        qual_id="naked", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://x/graphql"),
    )
    client = _fake_client()
    with pytest.raises(FileNotFoundError, match="run `dagtools qual classes"):
        run_side(
            "naked", "baseline",
            registry=registry,
            client_factory=_factory(client),
            sleep=lambda _: None,
        )
