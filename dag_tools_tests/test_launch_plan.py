"""How a representative gets turned into an actual launch.

Every rule here comes from launching a REAL code location rather than a
hand-built fixture, and each failed in a way a mock deployment would never
have shown:

  * one output of a non-subsettable multi_asset, selected alone, is
    rejected outright with DagsterInvalidSubsetError;
  * a partitioned asset launched with no partition produces a
    non-partitioned run, and dies the moment the asset body reads
    context.partition_key;
  * and -- reported by a consumer against a 22-model dbt project -- the
    first of those rules, applied PRE-EMPTIVELY, made every dbt
    representative build the entire project. @dbt_assets IS subsettable,
    so it never needed widening; the op-mates the widening dragged in
    brought their own unmet upstream requirements and mostly failed.

Neither subsettability nor partitioning is carried on a Representative,
and `AssetNode` exposes no `canSubset` field, so the launcher asks the
deployment for what it can and learns the rest from the refusal.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dag_tools.qual.classes import Representative
from dag_tools.qual.graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    DagsterSubsetNotSupportedError,
)
from dag_tools.qual.runs.launcher import (
    PARTITION_NAME_TAG,
    expand_to_op_siblings,
    launch_representative,
    plan_asset_selection,
)


# Shape returned by DagsterGraphQLClient.get_asset_launch_info(), taken
# from a live 1.10.19 deployment.
LAUNCH_INFO = {
    "customers": {
        "asset_key": ["customers"],
        "is_partitioned": False,
        "partition_keys": [],
        "op_names": ["customers"],
    },
    "daily_events": {
        "asset_key": ["daily_events"],
        "is_partitioned": True,
        "partition_keys": ["2026-07-29", "2026-07-30", "2026-07-31"],
        "op_names": ["daily_events"],
    },
    # One op, two assets — a non-subsettable multi_asset.
    "orders": {
        "asset_key": ["orders"],
        "is_partitioned": False,
        "partition_keys": [],
        "op_names": ["orders_and_lines"],
    },
    "order_lines": {
        "asset_key": ["order_lines"],
        "is_partitioned": False,
        "partition_keys": [],
        "op_names": ["orders_and_lines"],
    },
}


# A dbt project: every model shares ONE op, and the definition IS
# subsettable, so a one-key selection is honoured as a one-model --select.
DBT_OP = "my_project_dbt_assets"
DBT_LAUNCH_INFO = {
    f"model_{i:02d}": {
        "asset_key": [f"model_{i:02d}"],
        "is_partitioned": False,
        "partition_keys": [],
        "op_names": [DBT_OP],
    }
    for i in range(22)
}


def _rep(asset_key):
    return Representative(
        repo="alcs-tools",
        git_sha="a" * 40,
        asset_key=asset_key,
        runnability="runnable",
        runnability_reason="",
    )


def _manifest():
    # launch_representative reads exactly one attribute off the manifest.
    return SimpleNamespace(staging_overrides=None)


def _client(launch_side_effect=None):
    client = MagicMock(spec=DagsterGraphQLClient)
    if launch_side_effect is None:
        client.launch_asset_run.return_value = "run-1"
    else:
        client.launch_asset_run.side_effect = launch_side_effect
    return client


def _selections(client):
    """Every asset_selection the launcher actually submitted, in order."""
    return [c.kwargs["asset_selection"] for c in client.launch_asset_run.call_args_list]


def _launch(client, asset_key, info):
    return launch_representative(
        client,
        _rep(asset_key),
        qual_id="q-test",
        side="baseline",
        manifest=_manifest(),
        location_name="alcs-tools",
        job_name="__ASSET_JOB",
        launch_info=info,
    )


# ---------------------------------------------------------------------------
# A representative is launched ALONE
# ---------------------------------------------------------------------------


def test_representative_is_launched_alone_even_with_op_mates():
    """The plan selects the representative and nothing else. Q2 compares
    one asset per equivalence class, so an op-mate is compute the
    qualification never reads."""
    selection, partition = plan_asset_selection(["order_lines"], LAUNCH_INFO)
    assert selection == [["order_lines"]]
    assert partition is None


def test_a_subsettable_dbt_op_does_not_drag_in_the_whole_project():
    """The reported bug. All 22 models share one op; selecting one must
    stay one. Widening here is what made a single-model qualification
    build the entire project -- 22 materializations, most of them failing
    on upstreams the selection never asked for."""
    selection, _ = plan_asset_selection(["model_07"], DBT_LAUNCH_INFO)
    assert selection == [["model_07"]]


def test_the_whole_project_is_not_launched_through_the_real_entrypoint():
    """Same claim one level up, where the consumer actually saw it: the
    submitted selection carries one key, so dbt builds one model."""
    client = _client()
    _launch(client, ["model_07"], DBT_LAUNCH_INFO)
    assert _selections(client) == [[["model_07"]]]


# ---------------------------------------------------------------------------
# Widening is a RETRY, driven by the deployment's refusal
# ---------------------------------------------------------------------------


def test_op_siblings_are_recoverable_for_the_retry():
    """DagsterInvalidSubsetError: 'contains asset keys [order_lines,
    orders] ... but attempted to select only [order_lines]. This
    AssetsDefinition does not support subsetting.'"""
    assert expand_to_op_siblings(["order_lines"], LAUNCH_INFO) == [
        ["order_lines"],
        ["orders"],
    ]


def test_sibling_expansion_is_symmetric():
    """Whichever output was picked as the representative, the same pair is
    launched — so baseline and candidate cannot select differently."""
    assert expand_to_op_siblings(["orders"], LAUNCH_INFO) == expand_to_op_siblings(
        ["order_lines"], LAUNCH_INFO
    )


def test_expansion_skips_external_assets():
    """An external asset has no compute; dragging one into the selection
    fails the launch for the whole op."""
    info = dict(LAUNCH_INFO)
    info["raw_orders"] = {
        "asset_key": ["raw_orders"],
        "is_partitioned": False,
        "partition_keys": [],
        "op_names": ["orders_and_lines"],
        "is_executable": False,
    }
    assert ["raw_orders"] not in expand_to_op_siblings(["order_lines"], info)


def test_a_refused_partial_selection_is_retried_with_the_op_mates():
    """The non-subsettable case still works -- it just costs one rejected
    launch instead of widening every launch in the fleet."""
    client = _client(
        launch_side_effect=[
            DagsterSubsetNotSupportedError("launch failed (InvalidSubsetError): nope"),
            "run-2",
        ]
    )
    assert _launch(client, ["order_lines"], LAUNCH_INFO) == "run-2"
    assert _selections(client) == [
        [["order_lines"]],
        [["order_lines"], ["orders"]],
    ]


def test_a_dbt_launch_is_never_retried_because_it_is_never_refused():
    """Guards the cost claim: the subsettable path submits once."""
    client = _client()
    _launch(client, ["model_07"], DBT_LAUNCH_INFO)
    assert client.launch_asset_run.call_count == 1


def test_other_launch_failures_are_not_retried():
    """Only InvalidSubsetError means 'select the whole op'. Retrying a
    config-validation failure would double every such failure and bury
    the real message."""
    client = _client(
        launch_side_effect=DagsterGraphQLError(
            "launch failed (RunConfigValidationInvalid): ops ..."
        )
    )
    with pytest.raises(DagsterGraphQLError, match="RunConfigValidationInvalid"):
        _launch(client, ["order_lines"], LAUNCH_INFO)
    assert client.launch_asset_run.call_count == 1


def test_a_refusal_with_nothing_to_widen_to_reraises_the_original():
    """If the deployment reported no siblings, the refusal is about
    something else; a second identical launch would just lose the
    original message."""
    client = _client(
        launch_side_effect=DagsterSubsetNotSupportedError(
            "launch failed (InvalidSubsetError): key not in job"
        )
    )
    with pytest.raises(DagsterSubsetNotSupportedError, match="key not in job"):
        _launch(client, ["customers"], LAUNCH_INFO)
    assert client.launch_asset_run.call_count == 1


def test_the_retry_keeps_the_partition_tag():
    """A non-subsettable PARTITIONED multi_asset must not lose its
    partition on the way through the retry."""
    info = {
        "p_one": {
            "asset_key": ["p_one"],
            "is_partitioned": True,
            "partition_keys": ["2026-10-26", "2026-10-27"],
            "op_names": ["pair"],
        },
        "p_two": {
            "asset_key": ["p_two"],
            "is_partitioned": True,
            "partition_keys": ["2026-10-26", "2026-10-27"],
            "op_names": ["pair"],
        },
    }
    client = _client(
        launch_side_effect=[
            DagsterSubsetNotSupportedError("launch failed (InvalidSubsetError): nope"),
            "run-2",
        ]
    )
    _launch(client, ["p_one"], info)
    retry_tags = client.launch_asset_run.call_args_list[-1].kwargs["tags"]
    assert retry_tags[PARTITION_NAME_TAG] == "2026-10-27"


# ---------------------------------------------------------------------------
# partitions
# ---------------------------------------------------------------------------


def test_partitioned_asset_gets_the_latest_partition():
    """Without this the run is not partitioned at all, and the asset
    raises 'Cannot access partition_key for a non-partitioned run'."""
    selection, partition = plan_asset_selection(["daily_events"], LAUNCH_INFO)
    assert selection == [["daily_events"]]
    assert partition == "2026-07-31"


def test_unpartitioned_asset_gets_no_partition():
    _, partition = plan_asset_selection(["customers"], LAUNCH_INFO)
    assert partition is None


def test_partitioned_asset_with_no_partitions_yet():
    """A partition definition that has produced no keys must not yield a
    bogus partition — better a plain launch than an invented key."""
    info = {
        "empty": {
            "asset_key": ["empty"],
            "is_partitioned": True,
            "partition_keys": [],
            "op_names": ["empty"],
        }
    }
    _, partition = plan_asset_selection(["empty"], info)
    assert partition is None


# ---------------------------------------------------------------------------
# Degrading
# ---------------------------------------------------------------------------


def test_unknown_asset_falls_back_to_plain_selection():
    """If the deployment did not describe this asset, launch it the way
    the launcher always did rather than refusing."""
    selection, partition = plan_asset_selection(["mystery"], LAUNCH_INFO)
    assert selection == [["mystery"]]
    assert partition is None


def test_empty_launch_info_is_the_old_behaviour():
    selection, partition = plan_asset_selection(["order_lines"], {})
    assert selection == [["order_lines"]]
    assert partition is None


def test_expansion_of_an_unknown_asset_is_just_itself():
    """The retry must always produce a launchable selection, even when the
    deployment told us nothing about the key."""
    assert expand_to_op_siblings(["mystery"], {}) == [["mystery"]]


def test_partition_tag_name_matches_dagster():
    """The qual CLI installs WITHOUT dagster, so this constant is
    hardcoded and cannot be checked by importing. Pin it against the
    real value — if it drifts, partitioned launches silently become
    unpartitioned again."""
    assert PARTITION_NAME_TAG == "dagster/partition"
    dagster = pytest.importorskip("dagster")
    from dagster._core.storage.tags import PARTITION_NAME_TAG as REAL

    assert PARTITION_NAME_TAG == REAL
