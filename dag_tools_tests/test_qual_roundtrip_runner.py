"""run_roundtrip_side: moto-backed registry + a mocked GraphQL client.

Pins the gates (representative passed, upstream not rematerialized), the
partition the Q2 run actually used, the launch shape, and the PASSED /
LAUNCHED invariants shared with the Q2 and probe runners.
"""
import json
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")

import boto3
from moto import mock_aws

from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    RunStatusInfo,
)
from dag_tools.qual.graphql.client import CodeLocationStatus
from dag_tools.qual.qualify import Deployment, VersionTarget, create_qualification
from dag_tools.qual.registry import InventoryRegistry, S3Storage, StorageSettings
from dag_tools.qual.runs import (
    RoundTripRunState,
    RoundTripStatus,
    run_roundtrip_side,
)
from dag_tools.qual.runs.records import RunRecord
from dag_tools.qual.runs.roundtrip import (
    PROBES_JOB_NAME,
    PROBES_LOCATION_NAME,
    PROBES_REPOSITORY_NAME,
    SKIP_REMATERIALIZED,
    SKIP_REP_NOT_PASSED,
)
from dag_tools.qual.runs.state import QualRunState, RepState, RepStatus
from dag_tools.qual.synthetic import generate_bundle, publish_bundle
from dag_tools_tests.test_probes_runner import _publish_asset

BUCKET = "dag-tools-roundtrip-runner-test"
Q2_RUN = "q2-run-1"


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _seed(registry, *, partitions=None, rep_status=RepStatus.PASSED,
          record_partition: Optional[str] = None, qual_id="q-rt"):
    _publish_asset(
        registry, "alpha", "shaA", asset_key=["sales"],
        io_manager_class="dagster_snowflake.SnowflakeIOManager",
        partitions_def_class=partitions,
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
    probe = bundle.manifest.roundtrip_probes[0]

    now = datetime.now(tz=timezone.utc)
    state = QualRunState(
        qual_id=qual_id, side="baseline", started_at=now, updated_at=now,
        reps={probe.rep_id: RepState(
            rep_id=probe.rep_id, class_hash=probe.class_hash,
            asset_key=["sales"], repo="alpha", git_sha="shaA",
            runnability="runnable", status=rep_status,
            run_id=Q2_RUN if rep_status != RepStatus.PENDING else None,
        )},
    )
    registry.put_side_state(qual_id, "baseline", state.model_dump_json().encode())
    record = RunRecord(
        qual_id=qual_id, side="baseline", class_hash=probe.class_hash,
        asset_key=["sales"], repo="alpha", git_sha="shaA",
        run_id=Q2_RUN, success=True, status="SUCCESS",
        partition_key=record_partition,
    )
    registry.put_run_record(
        qual_id, "baseline", probe.class_hash, Q2_RUN,
        record.model_dump_json().encode(),
    )
    return qual_id, probe


def _client(*, latest=Q2_RUN, tags=None, locations=(PROBES_LOCATION_NAME,),
            run_id="rt-run-1"):
    c = MagicMock(spec=DagsterGraphQLClient)
    c.get_code_locations.return_value = [
        CodeLocationStatus(name=n, load_status="LOADED") for n in locations
    ]
    c.get_latest_materialization_run_id.return_value = latest
    c.get_run_tags.return_value = tags or {}
    c.add_dynamic_partition.return_value = "AddDynamicPartitionSuccess"
    c.launch_asset_run.return_value = run_id
    c.poll_to_completion.return_value = RunStatusInfo(
        run_id=run_id, status="SUCCESS", start_time=0.0, end_time=1.0,
    )
    c.get_event_log.return_value = []
    return c


def _run(registry, qual_id, client, **kw):
    return run_roundtrip_side(
        qual_id, "baseline", registry=registry,
        client_factory=lambda manifest: client, **kw,
    )


def _status(outcome, probe):
    return outcome.state.probes[probe.class_hash]


def test_a_rep_that_did_not_pass_skips_its_roundtrip(setup):
    qual_id, probe = _seed(setup, rep_status=RepStatus.FAILED)
    client = _client()
    out = _run(setup, qual_id, client)
    st = _status(out, probe)
    assert st.status == RoundTripStatus.SKIPPED
    assert st.error == SKIP_REP_NOT_PASSED
    client.launch_asset_run.assert_not_called()


def test_rematerialized_upstream_skips_instead_of_loading_stale_output(setup):
    qual_id, probe = _seed(setup)
    client = _client(latest="a-different-run")
    out = _run(setup, qual_id, client)
    st = _status(out, probe)
    assert st.status == RoundTripStatus.SKIPPED
    assert st.error == SKIP_REMATERIALIZED
    client.launch_asset_run.assert_not_called()


def test_partition_comes_from_the_q2_record_not_last_partition_key(setup):
    qual_id, probe = _seed(
        setup, partitions="DailyPartitionsDefinition",
        record_partition="2026-01-05",
    )
    client = _client()
    out = _run(setup, qual_id, client)
    assert _status(out, probe).status == RoundTripStatus.PASSED
    assert client.add_dynamic_partition.call_args.kwargs["partition_key"] == "2026-01-05"
    assert client.get_latest_materialization_run_id.call_args.args[1] == "2026-01-05"
    assert client.launch_asset_run.call_args.kwargs["tags"]["dagster/partition"] == "2026-01-05"
    client.get_run_tags.assert_not_called()  # the record answered it
    assert not hasattr(client, "last_partition_key")


def test_old_record_without_partition_reads_the_run_tag(setup):
    qual_id, probe = _seed(setup, partitions="DailyPartitionsDefinition")
    client = _client(tags={"dagster/partition": "2025-12-31"})
    out = _run(setup, qual_id, client)
    assert _status(out, probe).status == RoundTripStatus.PASSED
    client.get_run_tags.assert_called_once_with(Q2_RUN)
    assert client.add_dynamic_partition.call_args.kwargs["partition_key"] == "2025-12-31"


def test_undeterminable_partition_skips_it_does_not_fail(setup):
    qual_id, probe = _seed(setup, partitions="DailyPartitionsDefinition")
    out = _run(setup, qual_id, _client(tags={}))
    assert _status(out, probe).status == RoundTripStatus.SKIPPED


def test_duplicate_dynamic_partition_is_idempotent(setup):
    qual_id, probe = _seed(
        setup, partitions="DailyPartitionsDefinition", record_partition="p1",
    )
    client = _client()
    client.add_dynamic_partition.return_value = "DuplicateDynamicPartitionError"
    out = _run(setup, qual_id, client)
    assert _status(out, probe).status == RoundTripStatus.PASSED

    # The client treats a duplicate as success and anything else as an error.
    real = DagsterGraphQLClient("http://x/graphql")
    kw = dict(location_name="l", repository_name="r",
              partitions_def_name="d", partition_key="k")
    real.post = lambda q, v=None: {
        "addDynamicPartition": {"__typename": "DuplicateDynamicPartitionError"}}
    assert real.add_dynamic_partition(**kw) == "DuplicateDynamicPartitionError"
    real.post = lambda q, v=None: {
        "addDynamicPartition": {"__typename": "UnauthorizedError", "message": "no"}}
    with pytest.raises(DagsterGraphQLError):
        real.add_dynamic_partition(**kw)


def test_roundtrip_launches_the_probe_alone_on_the_probes_location(setup):
    qual_id, probe = _seed(setup)
    client = _client()
    _run(setup, qual_id, client)
    kw = client.launch_asset_run.call_args.kwargs
    assert kw["location_name"] == PROBES_LOCATION_NAME
    assert kw["repository_name"] == PROBES_REPOSITORY_NAME
    assert kw["job_name"] == PROBES_JOB_NAME
    # Alone: selecting the representative too would rewrite the output under test.
    assert kw["asset_selection"] == [list(probe.probe_asset_key)]
    assert kw["tags"]["dagtools/roundtrip"] == "true"
    assert kw["tags"]["dagtools/class_hash"] == probe.class_hash
    assert kw["tags"]["dagtools/side"] == "baseline"
    assert "dagster/partition" not in kw["tags"]
    stored = setup.read_roundtrip_run_record(
        qual_id, "baseline", probe.class_hash, "rt-run-1")
    assert json.loads(stored)["success"] is True


def test_passed_roundtrip_is_never_relaunched(setup):
    qual_id, probe = _seed(setup)
    _run(setup, qual_id, _client())
    second = _client(run_id="must-not-launch")
    out = _run(setup, qual_id, second)
    assert second.launch_asset_run.call_count == 0
    assert _status(out, probe).status == RoundTripStatus.PASSED


def test_a_roundtrip_poll_timeout_stays_launched(setup):
    qual_id, probe = _seed(setup)
    client = _client()
    client.poll_to_completion.side_effect = DagsterGraphQLError("timed out")
    out = _run(setup, qual_id, client)
    st = _status(out, probe)
    assert st.status == RoundTripStatus.LAUNCHED
    assert st.run_id == "rt-run-1"

    again = _client(run_id="must-not-launch")
    again.poll_to_completion.return_value = RunStatusInfo(
        run_id="rt-run-1", status="SUCCESS", start_time=0.0, end_time=1.0,
    )
    out2 = _run(setup, qual_id, again)
    assert again.launch_asset_run.call_count == 0
    assert _status(out2, probe).status == RoundTripStatus.PASSED


def test_absent_probes_location_records_nothing_and_does_not_fail_qual_run(setup):
    qual_id, probe = _seed(setup)
    client = _client(locations=("some-other-location",))
    out = _run(setup, qual_id, client)
    assert out.recorded is False
    assert "absent" in out.note
    client.launch_asset_run.assert_not_called()
    assert setup.read_roundtrip_state(qual_id, "baseline") is None


def test_state_is_mirrored_after_each_transition(setup):
    qual_id, probe = _seed(setup)
    seen = []
    real_put = setup.put_roundtrip_state

    def spy(q, side, body):
        seen.append(
            RoundTripRunState.model_validate_json(body).probes[probe.class_hash].status)
        return real_put(q, side, body)

    setup.put_roundtrip_state = spy
    _run(setup, qual_id, _client())
    assert RoundTripStatus.LAUNCHED in seen
    assert seen[-1] == RoundTripStatus.PASSED
    stored = RoundTripRunState.model_validate_json(
        setup.read_roundtrip_state(qual_id, "baseline")
    )
    assert stored.probes[probe.class_hash].status == RoundTripStatus.PASSED
