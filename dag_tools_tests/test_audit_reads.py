"""The judgment in the read audit: which asset explains a read, and when none does.

This is the module the board number comes out of, so the tests are weighted
towards the ways a roll-up can flatter itself. The rule throughout: a read
this library cannot attribute stays in the gap. It is never dropped, never
attributed to a plausible neighbour, and the coverage figure is never
computed in a way that makes an empty window look complete.

Every identifier here is synthetic. The audit carries the operator's object
names as data; none of them belongs in this repo.
"""

from datetime import datetime, timedelta, timezone

import pytest

from dag_tools.audit.reads import (
    AMBIGUOUS,
    BY_ASSET_KEY,
    BY_CORRELATION,
    BY_RUN_ID,
    NOT_A_PIPELINE,
    UNATTRIBUTED,
    ReadEvent,
    RunWindow,
    attribute_event,
    build_reads_report,
)
from dag_tools.audit.tag import (
    QueryAttribution,
    encode,
    on_behalf_of_principal,
    pipeline_principal,
)

T0 = datetime(2026, 10, 29, 1, 0, tzinfo=timezone.utc)
SVC = pipeline_principal("daily_load")


def at(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


def tag(asset_key=None, run_id=None, principal=SVC) -> str:
    return encode(
        QueryAttribution(principal=principal, asset_key=asset_key, run_id=run_id)
    )


def event(minutes: int, relations, query_tag=None, principal="svc_account") -> ReadEvent:
    return ReadEvent(
        query_id=f"q-{minutes}",
        started_at=at(minutes),
        principal=principal,
        query_tag=query_tag,
        relations=list(relations),
    )


def window(run_id, asset_keys, start, end) -> RunWindow:
    return RunWindow(
        run_id=run_id,
        asset_keys=list(asset_keys),
        started_at=at(start),
        ended_at=None if end is None else at(end),
    )


def attribute(ev, runs=()):
    return attribute_event(ev, {run.run_id: run for run in runs}, list(runs))


def test_a_tagged_asset_key_needs_nothing_else():
    method, asset_key, _ = attribute(event(0, ["t_a"], tag(asset_key="orders_clean")))
    assert (method, asset_key) == (BY_ASSET_KEY, "orders_clean")


def test_a_run_id_resolves_through_a_single_asset_run():
    runs = [window("run-1", ["orders_clean"], 0, 30)]
    method, asset_key, _ = attribute(event(10, ["t_a"], tag(run_id="run-1")), runs)
    assert (method, asset_key) == (BY_RUN_ID, "orders_clean")


def test_a_multi_asset_run_cannot_attribute():
    """Two assets in one run is not one asset.

    Picking either would put the operator's object under a label it may not
    deserve, and nothing downstream could tell that from a real attribution.
    """
    runs = [window("run-1", ["orders_clean", "orders_enriched"], 0, 30)]
    method, asset_key, _ = attribute(event(10, ["t_a"], tag(run_id="run-1")), runs)
    assert (method, asset_key) == (AMBIGUOUS, None)


def test_a_run_id_nothing_knows_about_is_ambiguous_not_attributed():
    method, asset_key, _ = attribute(event(10, ["t_a"], tag(run_id="run-missing")))
    assert (method, asset_key) == (AMBIGUOUS, None)


def test_an_untagged_read_correlates_through_one_single_asset_window():
    """The correlation branch must be reachable without a tag.

    It once was not: the row's own identity was tested against the
    `pipeline:<name>` grammar, and a warehouse username never matches it, so
    every untagged read was reported as not-a-pipeline and this whole branch
    was dead. The row principal here is deliberately an opaque warehouse
    account name.
    """
    runs = [window("run-1", ["orders_clean"], 0, 30)]
    method, asset_key, _ = attribute(event(10, ["t_a"], None, principal="svc_wh"), runs)
    assert (method, asset_key) == (BY_CORRELATION, "orders_clean")


def test_two_overlapping_windows_are_ambiguous():
    runs = [
        window("run-a", ["metrics_daily"], 0, 30),
        window("run-b", ["metrics_hourly"], 5, 35),
    ]
    method, asset_key, _ = attribute(event(10, ["t_a"], None), runs)
    assert (method, asset_key) == (AMBIGUOUS, None)


def test_an_open_window_still_contains_a_later_read():
    runs = [window("run-1", ["orders_clean"], 0, None)]
    method, asset_key, _ = attribute(event(600, ["t_a"], None), runs)
    assert (method, asset_key) == (BY_CORRELATION, "orders_clean")


def test_a_read_outside_every_window_is_unattributed():
    runs = [window("run-1", ["orders_clean"], 0, 30)]
    method, asset_key, _ = attribute(event(90, ["t_a"], None), runs)
    assert (method, asset_key) == (UNATTRIBUTED, None)


def test_a_person_behind_the_tag_is_not_a_pipeline():
    """The gateway's verified on_behalf_of.

    It carries an asset key, and that key is still not an explanation: what a
    person may read is a brokering decision (step 5), not an attribution gap
    a query tag can close.
    """
    method, asset_key, _ = attribute(
        event(
            0,
            ["t_a"],
            tag(asset_key="orders_clean", principal=on_behalf_of_principal("analyst")),
        )
    )
    assert (method, asset_key) == (NOT_A_PIPELINE, None)


def test_an_unknown_principal_kind_in_a_tag_is_not_a_pipeline():
    method, asset_key, _ = attribute(
        event(0, ["t_a"], tag(asset_key="orders_clean", principal="robot:x"))
    )
    assert (method, asset_key) == (NOT_A_PIPELINE, None)


def test_an_opaque_row_identity_is_not_evidence_either_way():
    """A warehouse username is in the warehouse's namespace, not ours.

    Narrowing the audit to the service identity is the operator's
    `principal_filter`; it is not an inference this module makes from a name.
    """
    method, _, _ = attribute(event(0, ["t_a"], None, principal="pipeline:daily_load"))
    assert method == UNATTRIBUTED


# --- the roll-up ------------------------------------------------------------


def test_the_gap_contains_every_read_no_asset_explains():
    runs = [window("run-multi", ["a", "b"], 0, 30)]
    report = build_reads_report(
        [
            event(1, ["t_explained"], tag(asset_key="orders_clean")),
            event(2, ["t_ambiguous"], tag(run_id="run-multi")),
            # Outside the window on purpose: inside it, a multi-asset run
            # would make this ambiguous rather than unattributed.
            event(90, ["t_untagged"], None),
            event(
                4,
                ["t_person"],
                tag(asset_key="orders_clean", principal=on_behalf_of_principal("x")),
            ),
        ],
        runs,
    )

    assert report.explained_relations == ["t_explained"]
    # Each of the three uncategorised reads is in the headline gap AND broken
    # out. A reader who only looked at one list could not be misled by the
    # other.
    assert report.unexplained_relations == ["t_ambiguous", "t_person", "t_untagged"]
    assert report.ambiguous_relations == ["t_ambiguous"]
    assert report.untagged_relations == ["t_untagged"]
    assert report.non_pipeline_relations == ["t_person"]


def test_an_object_read_both_ways_counts_as_explained():
    """One asset explaining it is enough; the other read does not un-explain it."""
    report = build_reads_report(
        [
            event(1, ["t_shared"], tag(asset_key="orders_clean")),
            event(2, ["t_shared"], None),
        ]
    )
    assert report.explained_relations == ["t_shared"]
    assert report.unexplained_relations == []
    # Still counted as an untagged read -- the method tally is about reads,
    # the relation lists are about objects.
    assert report.method_counts[UNATTRIBUTED] == 1


def test_coverage_is_none_when_nothing_was_read():
    """0/0 is not full coverage.

    A window in which the audit saw nothing must not report 100%: that is the
    shape of every vacuous pass this project has had to go back and fix.
    """
    report = build_reads_report([])
    assert report.relation_count == 0
    assert report.coverage is None


def test_coverage_is_the_explained_fraction():
    report = build_reads_report(
        [
            event(1, ["t_a", "t_b"], tag(asset_key="orders_clean")),
            event(2, ["t_c", "t_d"], None),
        ]
    )
    assert report.coverage == pytest.approx(0.5)


def test_assets_per_relation_and_relations_per_asset_agree():
    report = build_reads_report(
        [
            event(1, ["t_a", "t_b"], tag(asset_key="asset_one")),
            event(2, ["t_b"], tag(asset_key="asset_two")),
        ]
    )
    assert report.relations_per_asset == {
        "asset_one": ["t_a", "t_b"],
        "asset_two": ["t_b"],
    }
    assert report.assets_per_relation == {
        "t_a": ["asset_one"],
        "t_b": ["asset_one", "asset_two"],
    }


def test_every_method_is_counted_even_at_zero():
    """A method missing from the tally reads as not-measured, not as zero."""
    report = build_reads_report([event(1, ["t_a"], tag(asset_key="asset_one"))])
    assert set(report.method_counts) == {
        BY_ASSET_KEY,
        BY_RUN_ID,
        BY_CORRELATION,
        AMBIGUOUS,
        UNATTRIBUTED,
        NOT_A_PIPELINE,
    }
    assert report.method_counts[BY_ASSET_KEY] == 1
    assert report.method_counts[UNATTRIBUTED] == 0


def test_a_read_that_touched_nothing_is_reported_separately():
    report = build_reads_report([event(1, [], tag(asset_key="asset_one"))])
    assert report.event_count == 1
    assert report.relation_count == 0
    assert report.events_without_relations == 1


def test_lists_are_sorted_and_deduplicated():
    """The report is diffed between windows, so ordering cannot wander."""
    report = build_reads_report(
        [
            event(1, ["t_b", "t_a", "t_a"], tag(asset_key="asset_one")),
            event(2, ["t_a"], tag(asset_key="asset_one")),
        ]
    )
    assert report.relations_per_asset["asset_one"] == ["t_a", "t_b"]
    assert report.relation_count == 2


def test_with_no_windows_every_untagged_read_stays_in_the_gap():
    report = build_reads_report([event(1, ["t_a"], None), event(2, ["t_b"], None)])
    assert report.unexplained_relations == ["t_a", "t_b"]
    assert report.untagged_relations == ["t_a", "t_b"]
