"""``dagtools qual stage``: materialize the computed upstreams Q2 will load.

Covers selection, partition grouping, launching, state, and the two
isolation properties that matter most: Q6 records stage status without ever
comparing it, and Q2's one-representative-alone selection is untouched.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("yaml")

import boto3
from moto import mock_aws

from dag_tools.qual.classes import (
    ClassKeyComponents,
    ClassMatrix,
    ClassMember,
    EquivalenceClass,
    Representative,
    Runnability,
    StagingNeed,
    publish_class_matrix,
)
from dag_tools.qual.graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    DagsterSubsetNotSupportedError,
    EventLogEntry,
    RunStatusInfo,
)
from dag_tools.qual.qualify import Deployment, VersionTarget, create_qualification
from dag_tools.qual.registry import InventoryRegistry, S3Storage, StorageSettings
from dag_tools.qual.runs.launcher import PARTITION_NAME_TAG, launch_representative
from dag_tools.qual.runs.runner import _warn_if_stage_missing
from dag_tools.qual.runs.staging import StagingOverridesNotResolved
from dag_tools.qual.runs.state import rep_id_for
from dag_tools.qual.stage import (
    LaunchInfoUnavailable,
    StageState,
    StageStatus,
    StageTooLarge,
    build_stage_plan,
    launch_stage_group,
    run_stage,
)
from dag_tools.qual.stage.state import new_state, transition
from dag_tools.qual.verdict import GapAcceptance, build_verdict, render_markdown
from dag_tools_tests.test_verdict import (
    _publish_inventory,
    _publish_preflight,
    _publish_state_and_records,
    _asset,
)


BUCKET = "dag-tools-stage-test"
QUAL = "q-stage"
HASH = "aaaaaaaaaaaa"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


Key = Tuple[str, ...]


def _member(key: Key, ups: Optional[List[Key]], *, executable: Optional[bool] = True,
            repo: str = "alpha") -> ClassMember:
    return ClassMember(
        repo=repo, git_sha="s" * 7, asset_key=list(key),
        is_executable=executable,
        upstream_asset_keys=None if ups is None else [list(u) for u in ups],
    )


def _rep(key: Key, *, need: StagingNeed = StagingNeed.REQUIRED,
         runnability: Runnability = Runnability.RUNNABLE) -> Representative:
    return Representative(
        repo="alpha", git_sha="s" * 7, asset_key=list(key),
        runnability=runnability, runnability_reason="test",
        staging_need=need, staging_need_reason="test",
    )


def _matrix(members: List[ClassMember], reps: List[Representative],
            qual_id: str = QUAL) -> ClassMatrix:
    cls = EquivalenceClass(
        class_hash=HASH, key=ClassKeyComponents(), member_count=len(members),
        member_repo_count=1, members=members, representatives=reps,
    )
    return ClassMatrix(
        qual_id=qual_id, generated_at=datetime.now(tz=timezone.utc),
        asset_count=len(members), class_count=1, classes=[cls],
    )


def _chain_matrix() -> ClassMatrix:
    """rep -> m1 -> m0 -> src(external)."""
    return _matrix(
        [
            _member(("rep",), [("m1",)]),
            _member(("m1",), [("m0",)]),
            _member(("m0",), [("src",)]),
            _member(("src",), [], executable=False),
        ],
        [_rep(("rep",))],
    )


def _li(*keys: str, partitions: Optional[Dict[str, List[str]]] = None,
        ops: Optional[Dict[str, str]] = None, external: Tuple[str, ...] = ()
        ) -> Dict[str, Dict[str, Any]]:
    """launch_info for the given "/"-joined keys."""
    partitions = partitions or {}
    ops = ops or {}
    out: Dict[str, Dict[str, Any]] = {}
    for k in keys:
        pk = partitions.get(k)
        out[k] = {
            "asset_key": k.split("/"),
            "is_executable": k not in external,
            "is_partitioned": pk is not None,
            "partition_keys": pk or [],
            "op_names": [ops.get(k, "op_" + k.replace("/", "_"))],
        }
    return out


def _rid(key: Key) -> str:
    return rep_id_for(HASH, list(key))


def _plan(matrix: ClassMatrix, info: Dict[str, Dict[str, Any]]):
    return build_stage_plan(matrix, info)


def _keys(plan) -> List[List[List[str]]]:
    return [g.selection for g in plan.groups]


# ---------------------------------------------------------------------------
# Registry + client fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def registry(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _seed(registry, matrix: ClassMatrix, *, staging_overrides: Optional[str] = None):
    _publish_inventory(registry, "alpha", "sha-alpha", [_asset(["hello"])])
    create_qualification(
        qual_id=matrix.qual_id, registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(
            graphql_url="http://test/graphql", location_name="loc", job_name="__ASSET_JOB",
        ),
        staging_overrides=staging_overrides,
    )
    publish_class_matrix(matrix, registry=registry, allow_overwrite=True)


class FakeDeployment:
    """A MagicMock client whose runs succeed unless told otherwise."""

    def __init__(self, info: Dict[str, Dict[str, Any]],
                 fail_if_selects: Tuple[Key, ...] = ()):
        self.info = info
        self.fail_if_selects = {tuple(k) for k in fail_if_selects}
        self.runs: Dict[str, List[List[str]]] = {}
        self.client = MagicMock(spec=DagsterGraphQLClient)
        self.client.get_asset_launch_info.return_value = info
        self.client.launch_asset_run.side_effect = self._launch
        self.client.poll_to_completion.side_effect = self._poll
        self.client.get_event_log.side_effect = self._events

    def _launch(self, **kwargs):
        run_id = f"run-{len(self.runs) + 1}"
        self.runs[run_id] = [list(k) for k in kwargs["asset_selection"]]
        return run_id

    def _failed(self, run_id: str) -> bool:
        return any(tuple(k) in self.fail_if_selects for k in self.runs[run_id])

    def _poll(self, run_id, **kwargs):
        return RunStatusInfo(
            run_id=run_id, status="FAILURE" if self._failed(run_id) else "SUCCESS",
            start_time=1.0, end_time=2.0,
        )

    def _events(self, run_id):
        if self._failed(run_id):
            return []
        return [
            EventLogEntry(event_type="MaterializationEvent", asset_key=k)
            for k in self.runs[run_id]
        ]

    def factory(self, manifest):
        return self.client

    @property
    def launches(self) -> List[Dict[str, Any]]:
        return [c.kwargs for c in self.client.launch_asset_run.call_args_list]


def _run(registry, dep: FakeDeployment, tmp_path, **kw):
    return run_stage(
        QUAL, "baseline", registry=registry, client_factory=dep.factory,
        local_state_path=tmp_path / "stage-state.json",
        sleep=lambda s: None, **kw,
    )


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


def test_selection_is_the_transitive_closure_of_computed_upstreams():
    plan = _plan(_chain_matrix(), _li("rep", "m1", "m0", "src", external=("src",)))
    assert _keys(plan) == [[["m0"], ["m1"]]]
    assert plan.rep_requirements[_rid(("rep",))] == [["m0"], ["m1"]]
    assert plan.key_count == 2


def test_closure_stops_at_external_sources():
    # src is non-executable in the inventory: never walked, never planned.
    plan = _plan(_chain_matrix(), _li("rep", "m1", "m0", "src"))
    flat = [k for g in _keys(plan) for k in g]
    assert ["src"] not in flat
    # An inventory-computed key the deployment calls external is not staged
    # either: there is nothing to materialize.
    members = [
        _member(("rep",), [("e",)]),
        _member(("e",), [], executable=True),
    ]
    plan = _plan(_matrix(members, [_rep(("rep",))]),
                 _li("rep", "e", external=("e",)))
    assert plan.groups == []
    assert plan.unstageable_keys == []


def test_computed_in_any_pinned_repo_is_staged():
    members = [
        _member(("rep",), [("x",)]),
        _member(("x",), [], executable=False, repo="alpha"),  # stub
        _member(("x",), [], executable=True, repo="beta"),    # the real one
    ]
    plan = _plan(_matrix(members, [_rep(("rep",))]), _li("rep", "x"))
    assert _keys(plan) == [[["x"]]]


def test_unknown_reps_are_reported_not_planned():
    members = [
        _member(("rep",), [("m1",)]),
        _member(("m1",), []),
        _member(("old",), None),  # survey predates upstream_asset_keys
    ]
    reps = [_rep(("rep",)), _rep(("old",), need=StagingNeed.UNKNOWN)]
    plan = _plan(_matrix(members, reps), _li("rep", "m1", "old"))
    assert plan.unknown_reps == [_rid(("old",))]
    assert _rid(("old",)) not in plan.rep_requirements
    assert _keys(plan) == [[["m1"]]]


def test_key_absent_from_the_deployment_is_unstageable_and_names_its_reps():
    plan = _plan(_chain_matrix(), _li("rep", "m1"))  # m0 missing
    assert [k.asset_key for k in plan.unstageable_keys] == [["m0"]]
    assert plan.unstageable_keys[0].reps == [_rid(("rep",))]
    assert _keys(plan) == [[["m1"]]]


# ---------------------------------------------------------------------------
# Staging overrides
# ---------------------------------------------------------------------------


def test_staging_overrides_reach_the_stage_run_config(registry, tmp_path):
    doc = tmp_path / "overrides.json"
    doc.write_text(json.dumps(
        {"resources": {"io_manager": {"config": {"bucket": "staging"}}}}
    ))
    _seed(registry, _chain_matrix(), staging_overrides=str(doc))
    dep = FakeDeployment(_li("rep", "m1", "m0", "src", external=("src",)))
    _run(registry, dep, tmp_path)
    assert dep.launches[0]["run_config"] == {
        "resources": {"io_manager": {"config": {"bucket": "staging"}}}
    }
    assert set(dep.launches[0]["run_config"]) <= {
        "ops", "resources", "execution", "loggers"
    }


def _manifest_ns(pointer: Optional[str]):
    return SimpleNamespace(
        staging_overrides=pointer,
        deployment=SimpleNamespace(location_name="loc", job_name="__ASSET_JOB"),
    )


def _group(plan):
    return plan.groups[0]


def test_unresolved_staging_pointer_raises_instead_of_launching():
    plan = _plan(_chain_matrix(), _li("rep", "m1", "m0", "src", external=("src",)))
    client = MagicMock(spec=DagsterGraphQLClient)
    with pytest.raises(StagingOverridesNotResolved):
        launch_stage_group(
            client, _group(plan), qual_id=QUAL, side="baseline",
            selection_hash=plan.selection_hash,
            manifest=_manifest_ns("s3://b/overrides.yaml"),
            launch_info=_li("m1", "m0"), staging_overrides=None,
        )
    client.launch_asset_run.assert_not_called()


# ---------------------------------------------------------------------------
# Launching
# ---------------------------------------------------------------------------


def test_stage_run_tags_carry_purpose_and_no_class_hash():
    plan = _plan(_chain_matrix(), _li("rep", "m1", "m0", "src", external=("src",)))
    client = MagicMock(spec=DagsterGraphQLClient)
    client.launch_asset_run.return_value = "run-1"
    launch_stage_group(
        client, _group(plan), qual_id=QUAL, side="baseline",
        selection_hash=plan.selection_hash, manifest=_manifest_ns(None),
        launch_info=_li("m1", "m0"),
    )
    tags = client.launch_asset_run.call_args.kwargs["tags"]
    assert tags["dagtools/purpose"] == "stage"
    assert tags["dagtools/stage_selection_hash"] == plan.selection_hash
    assert tags["dagtools/qual"] == QUAL and tags["dagtools/side"] == "baseline"
    assert not any("class_hash" in k for k in tags)


def test_refused_subset_widens_that_group_to_op_mates_once():
    info = _li("rep", "m1", "m0", "mate", "src", external=("src",),
               ops={"m1": "shared_op", "mate": "shared_op"})
    plan = _plan(_chain_matrix(), info)
    client = MagicMock(spec=DagsterGraphQLClient)
    client.launch_asset_run.side_effect = [
        DagsterSubsetNotSupportedError("refused"), "run-2",
    ]
    run_id, launched = launch_stage_group(
        client, _group(plan), qual_id=QUAL, side="baseline",
        selection_hash=plan.selection_hash, manifest=_manifest_ns(None),
        launch_info=info,
    )
    assert run_id == "run-2"
    assert ["mate"] in launched and ["m1"] in launched and ["m0"] in launched
    assert client.launch_asset_run.call_count == 2

    # A second refusal is not retried again.
    client2 = MagicMock(spec=DagsterGraphQLClient)
    client2.launch_asset_run.side_effect = DagsterSubsetNotSupportedError("refused")
    with pytest.raises(DagsterSubsetNotSupportedError):
        launch_stage_group(
            client2, _group(plan), qual_id=QUAL, side="baseline",
            selection_hash=plan.selection_hash, manifest=_manifest_ns(None),
            launch_info=info,
        )
    assert client2.launch_asset_run.call_count == 2


def test_empty_launch_info_refuses_to_launch(registry, tmp_path):
    _seed(registry, _chain_matrix())
    dep = FakeDeployment({})
    with pytest.raises(LaunchInfoUnavailable):
        _run(registry, dep, tmp_path)
    dep.client.launch_asset_run.assert_not_called()
    assert registry.read_stage_state(QUAL, "baseline") is None


# ---------------------------------------------------------------------------
# Partition groups
# ---------------------------------------------------------------------------


def _two_group_matrix():
    """rep -> b (partition def 2) -> a (partition def 1); c independent def 3."""
    return _matrix(
        [
            _member(("rep",), [("b",), ("c",)]),
            _member(("b",), [("a",)]),
            _member(("a",), []),
            _member(("c",), []),
        ],
        [_rep(("rep",))],
    )


def _two_group_info():
    return _li("rep", "a", "b", "c", partitions={
        "a": ["2024-01", "2024-02"],
        "b": ["x", "y", "z"],
        "c": ["k1", "k2"],
    })


def test_one_partitioned_group_and_unpartitioned_keys_is_one_run():
    members = [
        _member(("rep",), [("p",), ("u",)]),
        _member(("p",), []),
        _member(("u",), []),
    ]
    plan = _plan(_matrix(members, [_rep(("rep",))]),
                 _li("rep", "p", "u", partitions={"p": ["d1", "d2"]}))
    assert len(plan.groups) == 1
    assert plan.groups[0].selection == [["p"], ["u"]]
    assert plan.groups[0].partition_key == "d2"


def test_two_partition_definitions_launch_as_two_runs_in_dependency_order(
        registry, tmp_path):
    _seed(registry, _two_group_matrix())
    dep = FakeDeployment(_two_group_info())
    out = _run(registry, dep, tmp_path)
    assert out.status == "passed"
    sels = [l["asset_selection"] for l in dep.launches]
    assert len(sels) == 3
    assert sels.index([["a"]]) < sels.index([["b"]])


def test_each_group_is_tagged_with_its_last_partition_key(registry, tmp_path):
    _seed(registry, _two_group_matrix())
    dep = FakeDeployment(_two_group_info())
    _run(registry, dep, tmp_path)
    by_sel = {json.dumps(l["asset_selection"]): l["tags"] for l in dep.launches}
    assert by_sel['[["a"]]'][PARTITION_NAME_TAG] == "2024-02"
    assert by_sel['[["b"]]'][PARTITION_NAME_TAG] == "z"
    assert by_sel['[["c"]]'][PARTITION_NAME_TAG] == "k2"


def test_a_failed_group_skips_only_the_groups_that_depend_on_it(
        registry, tmp_path):
    members = [
        _member(("rep",), [("b",), ("c",)]),
        _member(("b",), [("a",)]),
        _member(("a",), []),
        _member(("c",), []),
    ]
    _seed(registry, _matrix(members, [_rep(("rep",))]))
    dep = FakeDeployment(_two_group_info(), fail_if_selects=(("a",),))
    out = _run(registry, dep, tmp_path)
    status = {tuple(g.selection[0]): g.status for g in out.state.groups}
    assert status[("a",)] == StageStatus.FAILED
    assert status[("b",)] == StageStatus.SKIPPED
    assert status[("c",)] == StageStatus.PASSED
    assert {json.dumps(l["asset_selection"]) for l in dep.launches} == {
        '[["a"]]', '[["c"]]',
    }
    assert out.status == "failed"
    skipped = [g for g in out.state.groups if g.status == StageStatus.SKIPPED][0]
    assert "depends on failed" in skipped.error
    assert _rid(("rep",)) in out.reps_not_ready


def test_cross_group_cycle_keys_are_reported_and_not_launched(registry, tmp_path):
    members = [
        _member(("rep",), [("x",)]),
        _member(("x",), [("y",)]),
        _member(("y",), [("x",)]),
    ]
    matrix = _matrix(members, [_rep(("rep",))])
    info = _li("rep", "x", "y", partitions={"x": ["a"], "y": ["b"]})
    plan = _plan(matrix, info)
    assert plan.groups == []
    assert sorted(k.asset_key for k in plan.uncovered_cross_group_keys) == [["x"], ["y"]]

    _seed(registry, matrix)
    dep = FakeDeployment(info)
    out = _run(registry, dep, tmp_path)
    dep.client.launch_asset_run.assert_not_called()
    assert out.status == "nothing_to_stage"
    assert out.reps_not_ready == [_rid(("rep",))]


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


CHAIN_INFO = _li("rep", "m1", "m0", "src", external=("src",))


def test_passed_is_sacred_for_the_same_selection_hash(registry, tmp_path):
    _seed(registry, _chain_matrix())
    dep = FakeDeployment(CHAIN_INFO)
    first = _run(registry, dep, tmp_path)
    again = _run(registry, dep, tmp_path)
    assert first.status == again.status == "passed"
    assert dep.client.launch_asset_run.call_count == 1
    assert again.reps_ready == [_rid(("rep",))]


def test_changed_selection_hash_launches_again(registry, tmp_path):
    members = [
        _member(("rep",), [("p",)]),
        _member(("p",), []),
    ]
    _seed(registry, _matrix(members, [_rep(("rep",))]))
    dep = FakeDeployment(_li("rep", "p", partitions={"p": ["d1"]}))
    _run(registry, dep, tmp_path)
    assert dep.client.launch_asset_run.call_count == 1

    # The deployment's view moved on: a new partition key changes the plan.
    dep.client.get_asset_launch_info.return_value = _li(
        "rep", "p", partitions={"p": ["d1", "d2"]})
    _run(registry, dep, tmp_path)
    assert dep.client.launch_asset_run.call_count == 2


def test_launched_reconciles_via_poll_not_relaunch(registry, tmp_path):
    matrix = _chain_matrix()
    _seed(registry, matrix)
    plan = _plan(matrix, CHAIN_INFO)
    state = new_state(QUAL, "baseline", plan)
    state.groups[0] = transition(
        state.groups[0], status=StageStatus.LAUNCHED, run_id="run-existing",
        bump_attempts=True,
    )
    registry.put_stage_state(QUAL, "baseline", state.model_dump_json().encode())

    dep = FakeDeployment(CHAIN_INFO)
    dep.runs["run-existing"] = [["m0"], ["m1"]]
    out = _run(registry, dep, tmp_path)
    dep.client.launch_asset_run.assert_not_called()
    assert dep.client.poll_to_completion.call_args.args[0] == "run-existing"
    assert out.status == "passed"
    assert out.state.groups[0].run_id == "run-existing"


def test_state_is_mirrored_after_each_transition(registry, tmp_path, monkeypatch):
    _seed(registry, _chain_matrix())
    seen: List[str] = []
    real = registry.put_stage_state

    def spy(qual_id, side, body):
        seen.append(json.loads(body)["groups"][0]["status"])
        return real(qual_id, side, body)

    monkeypatch.setattr(registry, "put_stage_state", spy)
    dep = FakeDeployment(CHAIN_INFO)
    _run(registry, dep, tmp_path)
    assert seen == ["launched", "passed"]
    # And the immutable per-run record landed in the stage slot.
    assert registry.read_stage_run_record(QUAL, "baseline", "run-1") is not None


# ---------------------------------------------------------------------------
# CLI options (exercised through run_stage, which the CLI wraps)
# ---------------------------------------------------------------------------


def test_plan_launches_and_writes_nothing(registry, tmp_path):
    _seed(registry, _chain_matrix())
    dep = FakeDeployment(CHAIN_INFO)
    state_path = tmp_path / "stage-state.json"
    out = _run(registry, dep, tmp_path, plan_only=True)
    assert out.plan_only is True and out.plan.key_count == 2
    dep.client.launch_asset_run.assert_not_called()
    assert registry.read_stage_state(QUAL, "baseline") is None
    assert not state_path.exists()


def test_max_keys_refuses_an_oversized_selection(registry, tmp_path):
    _seed(registry, _chain_matrix())
    dep = FakeDeployment(CHAIN_INFO)
    with pytest.raises(StageTooLarge):
        _run(registry, dep, tmp_path, max_keys=1)
    dep.client.launch_asset_run.assert_not_called()
    assert registry.read_stage_state(QUAL, "baseline") is None
    # At the limit it is allowed.
    assert _run(registry, dep, tmp_path, max_keys=2).status == "passed"


# ---------------------------------------------------------------------------
# Q6 isolation
# ---------------------------------------------------------------------------


@pytest.fixture
def qualified(registry):
    """A complete, GO-able qualification (no stage artifacts yet)."""
    _publish_inventory(registry, "alpha", "sha-alpha", [_asset(["hello"])])
    create_qualification(
        qual_id="q-test", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://test/graphql"),
    )
    from dag_tools.qual.classes import build_class_matrix
    matrix = build_class_matrix("q-test", registry=registry)
    publish_class_matrix(matrix, registry=registry)
    for side in ("baseline", "candidate"):
        _publish_state_and_records(registry, "q-test", side, matrix)
    _publish_preflight(registry, "q-test", "candidate", passed=True)
    return registry


def _put_stage(registry, side: str, status: StageStatus):
    plan = _plan(_chain_matrix(), CHAIN_INFO)
    state = new_state("q-test", side, plan)
    state.groups[0] = transition(
        state.groups[0], status=status, run_id="run-s",
        error=None if status == StageStatus.PASSED else "boom",
    )
    registry.put_stage_state("q-test", side, state.model_dump_json().encode())
    registry.put_stage_run_record(
        "q-test", side, "run-s", json.dumps({"run_id": "run-s"}).encode(),
        allow_overwrite=True)


_GAPS = GapAcceptance(orchestration_deferred=True)


def _comparable(v) -> Dict[str, Any]:
    d = v.model_dump(mode="json", exclude={"stage_by_side", "generated_at"})
    return d


def test_q6_status_is_identical_with_and_without_stage_artifacts(qualified):
    bare = build_verdict("q-test", registry=qualified, gaps=_GAPS)
    for status in (StageStatus.PASSED, StageStatus.FAILED):
        for side in ("baseline", "candidate"):
            _put_stage(qualified, side, status)
        with_stage = build_verdict("q-test", registry=qualified, gaps=_GAPS)
        assert _comparable(with_stage) == _comparable(bare)
        assert with_stage.stage_by_side != bare.stage_by_side


def test_report_records_stage_not_run_and_passed_at_time_per_side(qualified):
    _put_stage(qualified, "candidate", StageStatus.PASSED)
    v = build_verdict("q-test", registry=qualified, gaps=_GAPS)
    assert v.stage_by_side["baseline"].status == "not_run"
    assert v.stage_by_side["baseline"].finished_at is None
    cand = v.stage_by_side["candidate"]
    assert cand.status == "passed" and cand.finished_at is not None
    assert cand.groups_total == cand.groups_passed == 1
    md = render_markdown(v)
    assert "Staging (baseline, informational)" in md
    assert "Staging (candidate, informational)" in md


def test_stage_status_never_reaches_blocking_issues(qualified):
    for side in ("baseline", "candidate"):
        _put_stage(qualified, side, StageStatus.FAILED)
    v = build_verdict("q-test", registry=qualified, gaps=_GAPS)
    assert v.stage_by_side["candidate"].status == "failed"
    assert not any("stag" in i.lower() for i in v.blocking_issues)
    assert v.status.value == "go"


# ---------------------------------------------------------------------------
# Q2 isolation
# ---------------------------------------------------------------------------


def test_q2_selection_is_unchanged_by_stage():
    """A REQUIRED representative is still launched ALONE: stage's keys never
    join its selection, whether or not stage has run."""
    info = _li("rep", "m1", "m0", "src", external=("src",))
    rep = _rep(("rep",))
    client = MagicMock(spec=DagsterGraphQLClient)
    client.launch_asset_run.return_value = "run-q2"
    launch_representative(
        client, rep, qual_id=QUAL, side="baseline",
        manifest=_manifest_ns(None), launch_info=info,
    )
    assert client.launch_asset_run.call_args.kwargs["asset_selection"] == [["rep"]]


def test_q2_warns_when_required_reps_exist_and_stage_has_not_passed(
        registry, caplog):
    matrix = _chain_matrix()
    with caplog.at_level(logging.WARNING, logger="dag_tools.qual.runs.runner"):
        _warn_if_stage_missing(registry, matrix, CHAIN_INFO, QUAL, "baseline")
    assert any("qual stage" in r.getMessage() for r in caplog.records)

    # A passed stage for the current selection silences it.
    plan = _plan(matrix, CHAIN_INFO)
    state = new_state(QUAL, "baseline", plan)
    state.groups[0] = transition(state.groups[0], status=StageStatus.PASSED,
                                 run_id="r")
    registry.put_stage_state(QUAL, "baseline", state.model_dump_json().encode())
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="dag_tools.qual.runs.runner"):
        _warn_if_stage_missing(registry, matrix, CHAIN_INFO, QUAL, "baseline")
    assert not caplog.records

    # A rep needing nothing never warns, with or without stage state.
    none_needed = _matrix([_member(("rep",), [])],
                          [_rep(("rep",), need=StagingNeed.NONE_NEEDED)])
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="dag_tools.qual.runs.runner"):
        _warn_if_stage_missing(registry, none_needed, CHAIN_INFO, QUAL, "candidate")
    assert not caplog.records


# ---------------------------------------------------------------------------
# Resumption edges
# ---------------------------------------------------------------------------


def test_a_key_external_on_the_deployment_does_not_hold_its_reps_not_ready():
    # m0 is computed in a pinned repo, but this deployment treats it as an
    # external source: nothing to stage, so it is no requirement either.
    matrix = _chain_matrix()
    info = _li("rep", "m1", "m0", "src", external=("src", "m0"))
    plan = _plan(matrix, info)
    assert _keys(plan) == [[["m1"]]]
    rid = rep_id_for(HASH, ["rep"])
    assert plan.rep_requirements[rid] == [["m1"]]


def test_a_poll_failure_leaves_the_group_launched_for_reconcile(registry, tmp_path):
    _seed(registry, _chain_matrix())
    dep = FakeDeployment(CHAIN_INFO)
    real_poll = dep._poll
    dep.client.poll_to_completion.side_effect = DagsterGraphQLError("timed out")
    first = _run(registry, dep, tmp_path)
    assert first.state.groups[0].status == StageStatus.LAUNCHED
    assert first.status == "incomplete"

    dep.client.poll_to_completion.side_effect = real_poll
    second = _run(registry, dep, tmp_path)
    assert dep.client.launch_asset_run.call_count == 1  # never relaunched
    assert second.status == "passed"
    assert second.state.groups[0].run_id == "run-1"


def test_reconcile_tolerates_a_record_already_banked(registry, tmp_path):
    _seed(registry, _chain_matrix())
    plan = _plan(_chain_matrix(), CHAIN_INFO)
    state = new_state(QUAL, "baseline", plan)
    state.groups[0] = transition(
        state.groups[0], status=StageStatus.LAUNCHED, run_id="run-existing",
        bump_attempts=True,
    )
    registry.put_stage_state(QUAL, "baseline", state.model_dump_json().encode())
    # An earlier pass wrote the record, then stopped before saving state.
    registry.put_stage_run_record(QUAL, "baseline", "run-existing", b"{}")

    dep = FakeDeployment(CHAIN_INFO)
    dep.runs["run-existing"] = [["m0"], ["m1"]]
    out = _run(registry, dep, tmp_path)
    assert out.status == "passed"


def test_a_stage_run_that_no_longer_exists_fails_so_the_next_pass_relaunches(
    registry, tmp_path
):
    from dag_tools.qual.graphql import DagsterRunNotFoundError

    _seed(registry, _chain_matrix())
    dep = FakeDeployment(CHAIN_INFO)
    real_poll = dep._poll
    dep.client.poll_to_completion.side_effect = DagsterRunNotFoundError("gone")
    first = _run(registry, dep, tmp_path)
    assert first.state.groups[0].status == StageStatus.FAILED

    dep.client.poll_to_completion.side_effect = real_poll
    second = _run(registry, dep, tmp_path)
    assert dep.client.launch_asset_run.call_count == 2
    assert second.status == "passed"
