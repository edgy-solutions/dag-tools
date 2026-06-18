"""End-to-end tests for run_probes_side — moto-backed registry +
mocked DagsterGraphQLClient.

Exercises the same resumability + LAUNCHED-reconciliation invariants
as the Q2 runner tests, plus the contract that probe launches target
the dag-tools-probes location with the downstream asset key.
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
from dag_tools.qual.probes import (
    ProbeRepStatus,
    ProbeRunState,
    run_probes_side,
)
from dag_tools.qual.probes.runner import (
    PROBES_LOCATION_NAME,
    PROBES_JOB_NAME,
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
from dag_tools.qual.synthetic import generate_bundle, publish_bundle


BUCKET = "dag-tools-probes-runner-test"


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


def _publish_asset(registry, repo, sha, *,
                   asset_key, io_manager_class, tags=None):
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    rec = {
        "schema_version": INV_VER,
        "asset_key": list(asset_key),
        "compute_kind": "python",
        "io_manager_key": "io_manager",
        "io_manager_class": io_manager_class,
        "io_manager_family": "snowflake",
        "partitions_def_class": None,
        "partition_mapping_classes": [],
        "resource_keys": ["io_manager"],
        "resource_classes": {"io_manager": io_manager_class},
        "integration_libs": [],
        "has_asset_checks": False,
        "automation_condition_type": None,
        "tags": tags or {},
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
        repo=repo, git_sha=sha, artifacts=artifacts,
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def _seed_qual_with_probes(registry, qual_id="q-test"):
    """Set up a qual with at least one SYNTHETIC_REQUIRED class so the
    probe manifest has something to drive the runner against."""
    _publish_asset(
        registry, "alpha", "shaA",
        asset_key=["sales"],
        io_manager_class="dagster_snowflake.SnowflakeIOManager",
        tags={"synthetic_required": "true"},
    )
    create_qualification(
        qual_id=qual_id, registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://dagster-test/graphql"),
    )
    matrix = build_class_matrix(qual_id, registry=registry)
    publish_class_matrix(matrix, registry=registry)
    bundle = generate_bundle(qual_id, registry=registry)
    publish_bundle(bundle, registry=registry)
    return qual_id, bundle


def _fake_client(
    *,
    launch_run_id: str = "probe-run-1",
    statuses: Optional[List[RunStatusInfo]] = None,
    events: Optional[List[EventLogEntry]] = None,
) -> DagsterGraphQLClient:
    client = MagicMock(spec=DagsterGraphQLClient)
    client.launch_asset_run.return_value = launch_run_id

    if statuses is None:
        statuses = [RunStatusInfo(run_id=launch_run_id, status="SUCCESS",
                                  start_time=0.0, end_time=2.0)]
    if events is None:
        events = []  # Probe events not asserted in most tests.
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


def test_run_probes_side_launches_and_passes_one_probe(setup):
    registry, _ = setup
    qual_id, bundle = _seed_qual_with_probes(registry)
    client = _fake_client()

    outcome = run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(client),
    )

    assert outcome.summary.probe_total == 1
    assert outcome.summary.passed == 1
    assert outcome.summary.failed == 0
    # One probe per SYNTHETIC_REQUIRED class.
    probe_state = next(iter(outcome.state.probes.values()))
    assert probe_state.status == ProbeRepStatus.PASSED
    assert probe_state.run_id == "probe-run-1"


def test_run_probes_side_launches_against_dag_tools_probes_location(setup):
    """Contract with dag_tools.probes_location: launches MUST target
    PROBES_LOCATION_NAME with the downstream asset key."""
    registry, _ = setup
    qual_id, bundle = _seed_qual_with_probes(registry)
    client = _fake_client()

    run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(client),
    )

    args = client.launch_asset_run.call_args
    assert args.kwargs["location_name"] == PROBES_LOCATION_NAME
    assert args.kwargs["job_name"] == PROBES_JOB_NAME
    # Downstream key only — deps pulls upstream automatically.
    probe = bundle.manifest.probes[0]
    assert args.kwargs["asset_selection"] == [
        [f"{probe.module_name}_downstream"]
    ]
    # Probe-marker tag distinguishes probe runs from rep runs.
    assert args.kwargs["tags"]["dagtools/probe"] == "true"
    assert args.kwargs["tags"]["dagtools/class_hash"] == probe.class_hash


def test_run_probes_side_persists_run_record(setup):
    registry, _ = setup
    qual_id, bundle = _seed_qual_with_probes(registry)
    client = _fake_client()

    run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(client),
    )

    probe = bundle.manifest.probes[0]
    stored = registry.read_probe_run_record(
        qual_id, "baseline", probe.class_hash, "probe-run-1",
    )
    assert stored is not None
    record = json.loads(stored)
    assert record["class_hash"] == probe.class_hash
    assert record["success"] is True


def test_run_probes_side_writes_summary_to_registry(setup):
    registry, _ = setup
    qual_id, _ = _seed_qual_with_probes(registry)
    client = _fake_client()

    run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(client),
    )

    body = registry.read_probes_summary(qual_id, "baseline")
    assert body is not None
    summary = json.loads(body)
    assert summary["probe_total"] == 1
    assert summary["passed"] == 1


# ---------------------------------------------------------------------------
# Resumability + LAUNCHED reconciliation
# ---------------------------------------------------------------------------


def test_run_probes_side_skips_passed_probes_on_re_invocation(setup):
    """PASSED is sacred — same invariant as the Q2 runner."""
    registry, _ = setup
    qual_id, _ = _seed_qual_with_probes(registry)

    first = _fake_client(launch_run_id="probe-run-1")
    run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(first),
    )
    assert first.launch_asset_run.call_count == 1

    second = _fake_client(launch_run_id="should-not-be-called")
    run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(second),
    )
    assert second.launch_asset_run.call_count == 0


def test_run_probes_side_reconciles_launched_via_poll_not_relaunch(setup, monkeypatch):
    """Operator's desktop died after launch but before terminal; the
    next invocation MUST poll the existing run_id, not relaunch."""
    registry, _ = setup
    qual_id, bundle = _seed_qual_with_probes(registry)

    # Simulate the first run leaving a LAUNCHED entry behind.
    probe = bundle.manifest.probes[0]
    pre_state = ProbeRunState(
        qual_id=qual_id, side="baseline",
        started_at=datetime.now(tz=timezone.utc),
        updated_at=datetime.now(tz=timezone.utc),
        probes={probe.class_hash: __import__(
            "dag_tools.qual.probes.state", fromlist=["ProbeRepState"]
        ).ProbeRepState(
            class_hash=probe.class_hash,
            module_name=probe.module_name,
            status=ProbeRepStatus.LAUNCHED,
            run_id="prior-run-99",
        )},
    )
    registry.put_probes_state(
        qual_id, "baseline",
        pre_state.model_dump_json().encode("utf-8"),
    )

    client = _fake_client(launch_run_id="should-not-be-called")
    client.poll_to_completion.return_value = RunStatusInfo(
        run_id="prior-run-99", status="SUCCESS",
        start_time=0.0, end_time=2.0,
    )

    outcome = run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(client),
    )
    assert client.launch_asset_run.call_count == 0
    # Reconciled to PASSED using the prior run_id.
    probe_state = outcome.state.probes[probe.class_hash]
    assert probe_state.status == ProbeRepStatus.PASSED
    assert probe_state.run_id == "prior-run-99"


def test_run_probes_side_mirrors_state_to_registry_after_each_transition(setup):
    """After a successful launch+pass, the registry-mirrored state
    must reflect the terminal status — the resumability contract."""
    registry, _ = setup
    qual_id, bundle = _seed_qual_with_probes(registry)
    client = _fake_client()

    run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(client),
    )

    body = registry.read_probes_state(qual_id, "baseline")
    assert body is not None
    state = ProbeRunState.model_validate_json(body)
    probe = bundle.manifest.probes[0]
    assert state.probes[probe.class_hash].status == ProbeRepStatus.PASSED


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


def test_run_probes_side_records_launch_failure(setup):
    registry, _ = setup
    qual_id, bundle = _seed_qual_with_probes(registry)
    client = _fake_client()
    client.launch_asset_run.side_effect = DagsterGraphQLError("location not loaded")

    outcome = run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(client),
    )
    probe = bundle.manifest.probes[0]
    probe_state = outcome.state.probes[probe.class_hash]
    assert probe_state.status == ProbeRepStatus.FAILED
    assert "location not loaded" in probe_state.error
    # And no run record was written — there's no run_id.
    assert outcome.summary.failed == 1


def test_run_probes_side_failed_run_records_record(setup):
    """When the poll returns a non-success terminal, the probe is
    FAILED but a RunRecord is still persisted so Q6 can diff."""
    registry, _ = setup
    qual_id, bundle = _seed_qual_with_probes(registry)
    client = _fake_client()
    client.poll_to_completion.return_value = RunStatusInfo(
        run_id="probe-run-1", status="FAILURE",
        start_time=0.0, end_time=2.0,
    )

    outcome = run_probes_side(
        qual_id=qual_id, side="baseline", registry=registry,
        client_factory=_factory(client),
    )
    probe = bundle.manifest.probes[0]
    probe_state = outcome.state.probes[probe.class_hash]
    assert probe_state.status == ProbeRepStatus.FAILED
    # Record still got persisted under the run_id.
    body = registry.read_probe_run_record(
        qual_id, "baseline", probe.class_hash, "probe-run-1",
    )
    assert body is not None
    assert json.loads(body)["success"] is False


# ---------------------------------------------------------------------------
# Missing prerequisites
# ---------------------------------------------------------------------------


def test_run_probes_side_raises_when_probe_manifest_missing(setup):
    """Helpful error pointing the operator at the missing step."""
    registry, _ = setup
    qual_id = "no-probes-yet"
    # Seed the qual but skip generating probes.
    _publish_asset(
        registry, "alpha", "shaA",
        asset_key=["x"],
        io_manager_class="dagster.InMemoryIOManager",
    )
    create_qualification(
        qual_id=qual_id, registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://dagster-test/graphql"),
    )

    with pytest.raises(FileNotFoundError, match="no probe manifest"):
        run_probes_side(
            qual_id=qual_id, side="baseline", registry=registry,
            client_factory=_factory(_fake_client()),
        )


def test_run_probes_side_raises_when_qual_manifest_missing(setup):
    registry, _ = setup
    with pytest.raises(FileNotFoundError, match="no qualification manifest"):
        run_probes_side(
            qual_id="never-existed", side="baseline", registry=registry,
            client_factory=_factory(_fake_client()),
        )
