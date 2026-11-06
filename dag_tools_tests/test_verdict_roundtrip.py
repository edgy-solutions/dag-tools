"""Q6 and the Q2 IO round-trip probe: covered / red / uncovered / not applicable.

Red (a probe passed on one side and failed on the other, or loaded a
different type or columns) blocks GO with no opt-out. Uncovered blocks unless
``roundtrip_missing`` is accepted. Not-applicable classes never block.
"""
import json
from datetime import datetime, timezone
from typing import Optional

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("yaml")

from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.qualify import Deployment, VersionTarget, create_qualification
from dag_tools.qual.runs.records import RunRecord
from dag_tools.qual.runs.roundtrip import (
    RoundTripRunState,
    RoundTripState,
    RoundTripStatus,
)
from dag_tools.qual.verdict import GapAcceptance, VerdictStatus, build_verdict
from dag_tools_tests.test_verdict import (  # noqa: F401  (setup is a fixture)
    _asset,
    _publish_inventory,
    _publish_preflight,
    _publish_state_and_records,
    setup,
)

QUAL = "q-rt"
ALL_ACCEPTED = GapAcceptance(
    orchestration_deferred=True,
    synthetic_coverage_missing=True,
    co_upgrade_risks=True,
    roundtrip_missing=True,
)
ONLY_ORCHESTRATION = GapAcceptance(orchestration_deferred=True)


def _seed(registry, record=None):
    _publish_inventory(registry, "alpha", "sha-alpha", [record or _asset(["hello"])])
    create_qualification(
        qual_id=QUAL, registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://test/graphql"),
    )
    matrix = build_class_matrix(QUAL, registry=registry)
    publish_class_matrix(matrix, registry=registry)
    for side in ("baseline", "candidate"):
        _publish_state_and_records(registry, QUAL, side, matrix)
    _publish_preflight(registry, QUAL, "candidate", passed=True)
    return matrix.classes[0].class_hash


def _put_rt(registry, class_hash, side, status, *, loaded_type="lib.Frame",
            columns=("a", "b"), loaded_len: Optional[int] = None, record=True):
    run_id = f"rt-{side}-{loaded_len}"
    now = datetime.now(tz=timezone.utc)
    state = RoundTripRunState(
        qual_id=QUAL, side=side, started_at=now, updated_at=now,
        probes={class_hash: RoundTripState(
            class_hash=class_hash, rep_id=f"{class_hash}:hello",
            status=status, run_id=run_id,
        )},
    )
    registry.put_roundtrip_state(QUAL, side, state.model_dump_json().encode())
    if not record:
        return
    meta = {
        "dagtools/loaded_type": loaded_type,
        "dagtools/loaded_columns": json.dumps(list(columns)),
    }
    if loaded_len is not None:
        meta["dagtools/loaded_len"] = str(loaded_len)
    rec = RunRecord(
        qual_id=QUAL, side=side, class_hash=class_hash,
        asset_key=["dagtools_roundtrip_x"], repo="dag-tools-probes",
        git_sha=class_hash[:12], run_id=run_id,
        success=status == RoundTripStatus.PASSED,
        status="SUCCESS" if status == RoundTripStatus.PASSED else "FAILURE",
        metadata_keys=list(meta), probe_metadata=meta,
    )
    registry.put_roundtrip_run_record(
        QUAL, side, class_hash, run_id, rec.model_dump_json().encode(),
    )


def _blocking(verdict, fragment):
    return [i for i in verdict.blocking_issues if fragment in i]


def test_type_divergence_is_red_even_with_accept_flag(setup):
    ch = _seed(setup)
    _put_rt(setup, ch, "baseline", RoundTripStatus.PASSED, loaded_type="lib.Frame")
    _put_rt(setup, ch, "candidate", RoundTripStatus.PASSED, loaded_type="lib.Other")
    v = build_verdict(QUAL, registry=setup, gaps=ALL_ACCEPTED)
    assert v.status == VerdictStatus.NO_GO
    assert v.roundtrip_classes_red == [ch]
    assert _blocking(v, "failing round-trip probes")
    assert v.class_verdicts[0].roundtrip_diff.probe_shape_parity is False


def test_columns_divergence_is_red(setup):
    ch = _seed(setup)
    _put_rt(setup, ch, "baseline", RoundTripStatus.PASSED, columns=("a", "b"))
    _put_rt(setup, ch, "candidate", RoundTripStatus.PASSED, columns=("a",))
    v = build_verdict(QUAL, registry=setup, gaps=ALL_ACCEPTED)
    assert v.roundtrip_classes_red == [ch]


def test_one_side_failed_is_red_even_with_accept_flag(setup):
    ch = _seed(setup)
    _put_rt(setup, ch, "baseline", RoundTripStatus.PASSED)
    _put_rt(setup, ch, "candidate", RoundTripStatus.FAILED, record=False)
    v = build_verdict(QUAL, registry=setup, gaps=ALL_ACCEPTED)
    assert v.status == VerdictStatus.NO_GO
    assert v.roundtrip_classes_red == [ch]


def test_both_sides_failed_is_uncovered_not_red(setup):
    ch = _seed(setup)
    _put_rt(setup, ch, "baseline", RoundTripStatus.FAILED, record=False)
    _put_rt(setup, ch, "candidate", RoundTripStatus.FAILED, record=False)
    v = build_verdict(QUAL, registry=setup, gaps=ONLY_ORCHESTRATION)
    assert v.roundtrip_classes_red == []
    assert v.roundtrip_classes_uncovered == 1
    assert v.status == VerdictStatus.NO_GO
    # The accept flag excuses it: it is missing coverage, not a regression.
    ok = build_verdict(QUAL, registry=setup, gaps=ALL_ACCEPTED)
    assert ok.status == VerdictStatus.GO


@pytest.mark.parametrize("other", [RoundTripStatus.SKIPPED, RoundTripStatus.PENDING, None])
def test_failed_opposite_a_side_that_never_ran_is_uncovered_not_red(setup, other):
    # Red means both sides ran and disagree. A FAILED side facing a side
    # that was skipped, is still pending, or has no state at all has
    # nothing to disagree with: that is missing coverage, which the
    # accept flag excuses, not a regression with no opt-out.
    ch = _seed(setup)
    _put_rt(setup, ch, "baseline", RoundTripStatus.FAILED, record=False)
    if other is not None:
        _put_rt(setup, ch, "candidate", other, record=False)
    v = build_verdict(QUAL, registry=setup, gaps=ONLY_ORCHESTRATION)
    assert v.roundtrip_classes_red == []
    assert v.roundtrip_classes_uncovered == 1
    assert v.status == VerdictStatus.NO_GO
    ok = build_verdict(QUAL, registry=setup, gaps=ALL_ACCEPTED)
    assert ok.status == VerdictStatus.GO


def test_missing_roundtrip_blocks_go_without_accept_flag(setup):
    _seed(setup)
    v = build_verdict(QUAL, registry=setup, gaps=ONLY_ORCHESTRATION)
    assert v.status == VerdictStatus.NO_GO
    assert v.roundtrip_classes_uncovered == 1
    [msg] = _blocking(v, "no round-trip coverage")
    assert "dagtools qual synthetic" in msg
    assert "--accept-roundtrip-missing" in msg
    assert build_verdict(QUAL, registry=setup, gaps=ALL_ACCEPTED).status == VerdictStatus.GO


@pytest.mark.parametrize("variant", ["dbt", "no_io_manager"])
def test_not_applicable_classes_never_block(setup, variant):
    rec = _asset(["hello"])
    if variant == "dbt":
        rec["integration_libs"] = ["dagster_dbt"]
    else:
        rec["io_manager_class"] = None
    ch = _seed(setup, rec)
    v = build_verdict(QUAL, registry=setup, gaps=ONLY_ORCHESTRATION)
    assert [e["class_hash"] for e in v.roundtrip_not_applicable] == [ch]
    assert v.roundtrip_classes_uncovered == 0
    assert not _blocking(v, "round-trip")
    assert v.status == VerdictStatus.GO


def test_loaded_len_difference_does_not_block(setup):
    ch = _seed(setup)
    _put_rt(setup, ch, "baseline", RoundTripStatus.PASSED, loaded_len=3)
    _put_rt(setup, ch, "candidate", RoundTripStatus.PASSED, loaded_len=9000)
    v = build_verdict(QUAL, registry=setup, gaps=ONLY_ORCHESTRATION)
    assert v.roundtrip_classes_covered == 1
    assert v.roundtrip_classes_red == []
    assert v.status == VerdictStatus.GO

    # Present on one side only: the metadata key sets differ, shape does not.
    _put_rt(setup, ch, "candidate", RoundTripStatus.PASSED, loaded_len=None)
    v2 = build_verdict(QUAL, registry=setup, gaps=ONLY_ORCHESTRATION)
    assert v2.roundtrip_classes_covered == 1
    assert v2.status == VerdictStatus.GO
