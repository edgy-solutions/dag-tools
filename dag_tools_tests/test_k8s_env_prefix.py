"""Tests for the shared env-prefix -> op-tags helper in dag_tools.utils.k8s,
and the dbt component's use of it.

The env-prefix convention (name a prefix, the deployment sets
<PREFIX>_CPU_REQUEST etc.) is the pattern used on plain @assets elsewhere
in the fleet; these tests pin the shared resolve+merge behavior that both
the dlt and dbt components rely on.
"""
import pytest

from dag_tools.utils.k8s import (
    deep_merge_dicts,
    resolve_op_tags_with_env_prefix,
)


# ---------------------------------------------------------------------------
# deep_merge_dicts
# ---------------------------------------------------------------------------


def test_deep_merge_override_wins_on_leaf():
    base = {"a": {"b": 1, "c": 2}}
    override = {"a": {"b": 9}}
    assert deep_merge_dicts(base, override) == {"a": {"b": 9, "c": 2}}


def test_deep_merge_adds_sibling_keys():
    base = {"a": {"b": 1}}
    override = {"a": {"c": 2}, "d": 3}
    assert deep_merge_dicts(base, override) == {"a": {"b": 1, "c": 2}, "d": 3}


def test_deep_merge_non_dict_replaces():
    base = {"a": {"b": 1}}
    override = {"a": "scalar"}
    assert deep_merge_dicts(base, override) == {"a": "scalar"}


def test_deep_merge_does_not_mutate_inputs():
    base = {"a": {"b": 1}}
    override = {"a": {"c": 2}}
    deep_merge_dicts(base, override)
    assert base == {"a": {"b": 1}}
    assert override == {"a": {"c": 2}}


# ---------------------------------------------------------------------------
# resolve_op_tags_with_env_prefix
# ---------------------------------------------------------------------------


def test_resolve_op_tags_no_prefix_returns_explicit_copy():
    explicit = {"dagster-k8s/config": {"pod_spec_config": {"node_selector": {"x": "y"}}}}
    out = resolve_op_tags_with_env_prefix(None, explicit)
    assert out == explicit
    assert out is not explicit  # copy, not alias


def test_resolve_op_tags_none_explicit_no_prefix_is_empty():
    assert resolve_op_tags_with_env_prefix(None, None) == {}


def test_resolve_op_tags_prefix_resolves_env(monkeypatch):
    monkeypatch.setenv("BUILD_CPU_REQUEST", "4")
    monkeypatch.setenv("BUILD_MEM_REQUEST", "16Gi")
    monkeypatch.setenv("BUILD_CPU_LIMIT", "8")
    monkeypatch.setenv("BUILD_MEM_LIMIT", "32Gi")
    out = resolve_op_tags_with_env_prefix("BUILD", None)
    res = out["dagster-k8s/config"]["container_config"]["resources"]
    assert res["requests"] == {"cpu": "4", "memory": "16Gi"}
    assert res["limits"] == {"cpu": "8", "memory": "32Gi"}


def test_resolve_op_tags_explicit_merges_over_prefix(monkeypatch):
    monkeypatch.setenv("BLD_CPU_REQUEST", "2")
    monkeypatch.setenv("BLD_MEM_REQUEST", "8Gi")
    out = resolve_op_tags_with_env_prefix(
        "BLD",
        {"dagster-k8s/config": {
            "container_config": {"resources": {"requests": {"cpu": "99"}}},
            "pod_spec_config": {"node_selector": {"disktype": "ssd"}},
        }},
    )
    cfg = out["dagster-k8s/config"]
    assert cfg["container_config"]["resources"]["requests"]["cpu"] == "99"       # explicit wins
    assert cfg["container_config"]["resources"]["requests"]["memory"] == "8Gi"   # env kept
    assert cfg["pod_spec_config"]["node_selector"] == {"disktype": "ssd"}        # sibling kept


# ---------------------------------------------------------------------------
# dbt component uses it (skipped when dagster_dbt not installed)
# ---------------------------------------------------------------------------


def test_dbt_component_get_op_spec_injects_env_prefix(monkeypatch):
    pytest.importorskip("dagster_dbt")
    from dag_tools.components.dbt_project.component import CustomDbtProjectComponent

    monkeypatch.setenv("DBT_BUILD_CPU_REQUEST", "3000m")
    monkeypatch.setenv("DBT_BUILD_MEM_REQUEST", "12Gi")

    # Construct without triggering dbt project resolution — only exercise
    # _get_op_spec, which calls super()._get_op_spec(project).
    comp = CustomDbtProjectComponent.__new__(CustomDbtProjectComponent)
    comp.datahub_config = None
    comp.k8s_resource_env_prefix = "DBT_BUILD"
    comp.k8s_default_cpu = "500m"
    comp.k8s_default_mem = "1Gi"
    comp.op = None
    comp.select = "fqn:*"
    comp.exclude = ""
    comp.selector = ""

    class _FakeProject:
        name = "demo_dbt"

    spec = comp._get_op_spec(_FakeProject())
    if spec is None:
        pytest.skip(
            "dagster-dbt on this version has no DbtProjectComponent._get_op_spec; "
            "the component degrades instead of raising (see the graceful-"
            "degradation test below)"
        )
    res = spec.tags["dagster-k8s/config"]["container_config"]["resources"]
    assert res["requests"] == {"cpu": "3000m", "memory": "12Gi"}


def test_dbt_component_survives_a_missing_private_hook(monkeypatch):
    """``_get_op_spec`` is a PRIVATE hook on dagster-dbt's
    DbtProjectComponent and is not present in every release -- notably not
    in 0.26.19, the one that pairs with Dagster 1.10.19.

    Overriding a private hook means accepting that it can move. What must
    NOT happen is an AttributeError at definition-load time, because that
    takes the whole code location down rather than just this component.
    Caught by the CI job pinned to the Dagster floor, where the real
    failure was:

        AttributeError: 'super' object has no attribute '_get_op_spec'
    """
    pytest.importorskip("dagster_dbt")
    import dagster_dbt

    from dag_tools.components.dbt_project.component import CustomDbtProjectComponent

    # Simulate the older dagster-dbt by removing the hook from the base.
    monkeypatch.delattr(
        dagster_dbt.DbtProjectComponent, "_get_op_spec", raising=False
    )

    comp = CustomDbtProjectComponent.__new__(CustomDbtProjectComponent)
    comp.datahub_config = None
    comp.k8s_resource_env_prefix = "DBT_BUILD"
    comp.k8s_default_cpu = "500m"
    comp.k8s_default_mem = "1Gi"
    comp.op = None
    comp.select = "fqn:*"
    comp.exclude = ""
    comp.selector = ""

    class _FakeProject:
        name = "demo_dbt"

    # Degrades to None rather than raising; the caller treats "no op spec"
    # as "use the default", which is what happened before this component
    # existed.
    assert comp._get_op_spec(_FakeProject()) is None
