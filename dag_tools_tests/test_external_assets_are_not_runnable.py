"""External/source assets must never be launched as representatives.

The failure this pins, reported from a real fleet:

    launch failed (InvalidSubsetError): DagsterInvalidDefinitionError:
    Selected keys must be a subset of existing executable asset keys.
    Invalid selected keys: {AssetKey(['resultsandmetainfo_demo',
    'resultsandmetainfo_demo', 'function_evaluations'])}

That key is a MariaDB source table a ``DltPipelineComponent`` reads from.
dlt names its upstream tables with ``deps=[AssetKey(...)]``, and Dagster
auto-creates a stub asset for any dep it can't resolve inside the
Definitions. The stub is a first-class member of ``resolve_all_asset_specs()``
and carries no tag, group, or key shape that distinguishes it from an
asset with real compute -- so runnability, which was purely tag-driven,
defaulted every one of them to RUNNABLE. A fleet with one dlt pipeline
therefore accrued one guaranteed launch failure per source table.

Three layers are pinned, because they fail independently:

  * the extractor records ``is_executable`` from Dagster itself;
  * Q1 classification forces OBSERVE_ONLY on ``is_executable=False``;
  * Q2 asks the DEPLOYMENT before launching, which is what fixes
    inventories published before the field existed -- without those, the
    fix would require re-surveying every repo in the fleet first.
"""
import pytest

from dagster import AssetKey, AssetSpec, Definitions, asset

from dag_tools.inventory import extract_records
from dag_tools.qual.classes.key import ClassMember, Runnability
from dag_tools.qual.classes.selection import classify_runnability
from dag_tools.qual.runs.launcher import is_launchable, plan_asset_selection


SOURCE_KEY = ["resultsandmetainfo_demo", "resultsandmetainfo_demo",
              "function_evaluations"]


@pytest.fixture
def defs():
    """A dlt-shaped Definitions: one real asset reading a source table
    that lives outside the Definitions entirely."""

    @asset(deps=[AssetKey(SOURCE_KEY)])
    def function_evaluations_ingested():
        return 1

    return Definitions(assets=[function_evaluations_ingested])


# ---------------------------------------------------------------------------
# Layer 1 -- the extractor
# ---------------------------------------------------------------------------


def test_dep_stub_is_recorded_as_not_executable(defs):
    """Dagster distinguishes these; the inventory has to carry it through,
    since Q1 runs against the published JSON and never sees Definitions."""
    by_key = {"/".join(r.asset_key): r for r in extract_records(defs)}

    assert by_key["/".join(SOURCE_KEY)].is_executable is False
    assert by_key["function_evaluations_ingested"].is_executable is True


def test_explicit_asset_spec_is_recorded_as_not_executable():
    """The other external shape: a bare AssetSpec, no auto-creation
    involved."""
    d = Definitions(assets=[AssetSpec(key=AssetKey(["ext", "table"]))])
    (record,) = extract_records(d)
    assert record.is_executable is False


def test_unknown_executability_stays_none_not_false():
    """A Dagster whose API moved must leave the field None. False would
    make the classifier skip real assets fleet-wide and report the run
    green -- a false PASS is worse than the launch failure this fixes."""
    from dag_tools.inventory.extractors import _is_executable

    class NoSuchAttr:
        pass

    assert _is_executable(NoSuchAttr()) is None
    assert _is_executable(None) is None


# ---------------------------------------------------------------------------
# Layer 2 -- Q1 classification
# ---------------------------------------------------------------------------


def _member(**kw):
    return ClassMember(repo="r", git_sha="s", asset_key=SOURCE_KEY, **kw)


def test_non_executable_classifies_observe_only():
    runnability, reason = classify_runnability(_member(is_executable=False))
    assert runnability is Runnability.OBSERVE_ONLY
    assert "not executable" in reason


def test_executability_outranks_tags():
    """An operator can't tag an external asset into being launchable --
    honoring the tag would only produce a guaranteed launch failure."""
    runnability, _ = classify_runnability(
        _member(is_executable=False, tags={"synthetic_required": "true"})
    )
    assert runnability is Runnability.OBSERVE_ONLY


def test_unknown_executability_classifies_as_before():
    """Inventories published before schema_version 2 carry None. They must
    classify exactly as they did, or upgrading dag-tools silently changes
    the verdict for every repo that hasn't re-surveyed."""
    runnability, reason = classify_runnability(_member(is_executable=None))
    assert runnability is Runnability.RUNNABLE
    assert reason == "default (no opt-out tag)"


# ---------------------------------------------------------------------------
# Layer 3 -- Q2, asking the deployment
# ---------------------------------------------------------------------------


LAUNCH_INFO = {
    "/".join(SOURCE_KEY): {
        "asset_key": SOURCE_KEY,
        "is_executable": False,
        "is_partitioned": False,
        "partition_keys": [],
        "op_names": [],
    },
    "function_evaluations_ingested": {
        "asset_key": ["function_evaluations_ingested"],
        "is_executable": True,
        "is_partitioned": False,
        "partition_keys": [],
        "op_names": ["function_evaluations_ingested"],
    },
}


def test_deployment_veto_skips_the_launch():
    ok, why = is_launchable(SOURCE_KEY, LAUNCH_INFO)
    assert ok is False
    assert "not executable" in why
    # The operator has to learn how to make it stick past this run.
    assert "survey" in why


def test_executable_asset_still_launches():
    assert is_launchable(["function_evaluations_ingested"], LAUNCH_INFO) == (True, "")


def test_silence_is_not_a_veto():
    """A failed or unsupported introspection query yields {} / None. If
    that read as 'not executable', one bad query would skip every
    representative and the side would report green having run nothing."""
    assert is_launchable(SOURCE_KEY, {})[0] is True
    assert is_launchable(
        SOURCE_KEY,
        {"/".join(SOURCE_KEY): {"asset_key": SOURCE_KEY, "op_names": []}},
    )[0] is True


def test_sibling_sweep_never_drags_in_an_external_asset():
    """Sibling expansion matches on shared opNames. An external asset with
    a coincidentally-overlapping name would fail the launch for the whole
    op, not just itself."""
    info = {
        "orders": {
            "asset_key": ["orders"], "is_executable": True,
            "is_partitioned": False, "partition_keys": [],
            "op_names": ["orders_and_lines"],
        },
        "src/orders": {
            "asset_key": ["src", "orders"], "is_executable": False,
            "is_partitioned": False, "partition_keys": [],
            "op_names": ["orders_and_lines"],
        },
    }
    selection, _ = plan_asset_selection(["orders"], info)
    assert selection == [["orders"]]
