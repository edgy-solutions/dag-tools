"""How a representative gets turned into an actual launch.

Both rules here come from launching a REAL code location on Dagster
1.10.19 rather than a hand-built fixture, and both failed in ways that a
mock deployment would never have shown:

  * one output of a non-subsettable multi_asset, selected alone, is
    rejected outright with DagsterInvalidSubsetError;
  * a partitioned asset launched with no partition produces a
    non-partitioned run, and dies the moment the asset body reads
    context.partition_key.

Neither fact is carried on a Representative, so the launcher asks the
deployment — which is the authority anyway.
"""
import pytest

from dag_tools.qual.runs.launcher import PARTITION_NAME_TAG, plan_asset_selection


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


# ---------------------------------------------------------------------------
# multi_asset siblings
# ---------------------------------------------------------------------------


def test_multi_asset_siblings_are_selected_together():
    """DagsterInvalidSubsetError: 'contains asset keys [order_lines,
    orders] ... but attempted to select only [order_lines]. This
    AssetsDefinition does not support subsetting.'"""
    selection, partition = plan_asset_selection(["order_lines"], LAUNCH_INFO)
    assert selection == [["order_lines"], ["orders"]]
    assert partition is None


def test_sibling_expansion_is_symmetric():
    """Whichever output was picked as the representative, the same pair
    is launched — so baseline and candidate cannot select differently."""
    a, _ = plan_asset_selection(["orders"], LAUNCH_INFO)
    b, _ = plan_asset_selection(["order_lines"], LAUNCH_INFO)
    assert a == b


def test_single_output_asset_is_not_expanded():
    selection, _ = plan_asset_selection(["customers"], LAUNCH_INFO)
    assert selection == [["customers"]]


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


def test_partition_tag_name_matches_dagster():
    """The qual CLI installs WITHOUT dagster, so this constant is
    hardcoded and cannot be checked by importing. Pin it against the
    real value — if it drifts, partitioned launches silently become
    unpartitioned again."""
    assert PARTITION_NAME_TAG == "dagster/partition"
    dagster = pytest.importorskip("dagster")
    from dagster._core.storage.tags import PARTITION_NAME_TAG as REAL

    assert PARTITION_NAME_TAG == REAL
