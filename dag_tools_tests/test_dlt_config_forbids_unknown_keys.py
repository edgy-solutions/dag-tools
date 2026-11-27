"""``DltAssetGroupConfig`` refuses an unknown key, and the pops that makes
load-bearing are proven sufficient for every component that builds it.

A misspelt setting that is silently ignored is a defect, not a convenience.
This model is built from an operator's YAML with ``**pipeline_attrs``, so
before ``extra="forbid"`` a typo was dropped on the floor and the pipeline
loaded with that setting absent -- no error, no warning, a load that reads as
a success. The failures are not cosmetic: a mistyped ``cursor`` is a full
reload every run, a mistyped ``row_filters`` extracts every row of every table
while the YAML reads as filtered.

The second half of this module is the part that keeps the first half honest.
Refusing unknown keys turns each component's "pop the component-only keys"
step from a defensive habit into a correctness requirement, so every such key
is asserted to be popped -- by *running* the component's own pop list against
the model, not by reading it.
"""
import importlib
import inspect
import pathlib
import re

import pydantic
import pytest

from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig


def test_an_unknown_key_is_refused_rather_than_ignored():
    """The seal. A key the model does not declare fails, and names itself."""
    with pytest.raises(pydantic.ValidationError) as excinfo:
        DltAssetGroupConfig(name="p", not_a_field="x")
    assert "not_a_field" in str(excinfo.value)


def test_a_misspelt_real_setting_is_refused():
    """The case that actually happens: a near-miss of a field that exists.

    ``table_prefix`` is real; ``table_prefixes`` is not. Before this change the
    pipeline loaded with no prefix at all and wrote to the unprefixed tables.
    """
    with pytest.raises(pydantic.ValidationError) as excinfo:
        DltAssetGroupConfig(name="p", table_prefixes="stg_")
    assert "table_prefixes" in str(excinfo.value)


def test_control_every_declared_field_is_still_accepted():
    """Without this, the seal above would pass on a model that refuses
    everything -- including a model whose fields had all been renamed away."""
    cfg = DltAssetGroupConfig(
        name="p",
        dest_schema="raw",
        backend="sqlalchemy",
        table_prefix="stg_",
        add_timestamp=True,
        io_manager_key="io_manager",
        ingest_runs=True,
        pool="heavy",
        op_tags={"a": "b"},
    )
    assert cfg.name == "p"
    assert cfg.table_prefix == "stg_"
    assert cfg.ingest_runs is True


def test_the_default_construction_still_works():
    """A no-argument construct is how several callers use it; ``forbid``
    constrains unknown keys only, never absent ones."""
    assert DltAssetGroupConfig().name is None


# --- the pops this change makes load-bearing -------------------------------

#: What each component pops from ``pipeline_attrs`` before building the model.
#: Kept here rather than imported because the point is to compare the
#: component's behaviour against an independently-written list: a list read out
#: of the component could not disagree with it.
COMPONENT_POPS = {
    "DltPipelineComponent": {
        "sources", "schedule", "table_config", "row_filters", "hints", "name",
    },
    "RestateDltSyncComponent": {
        "sources", "stats_table", "cycle_sensor", "table_config", "row_ack",
        "control_table", "mei_table", "primary_key", "hints", "name",
    },
    "RestateApiSyncComponent": {"primary_key", "api_path", "sources", "name"},
}


@pytest.mark.parametrize("component", sorted(COMPONENT_POPS))
def test_every_component_only_key_is_popped_not_passed(component):
    """Each of these keys is one the model would now refuse, which is what
    makes the pop load-bearing rather than tidy. A component that grows a YAML
    key and forgets its pop therefore fails the load -- where before this
    change it shipped as a feature that silently did nothing.

    This test does not watch the components; ``test_the_pop_lists_match_the
    _components`` below does that, and the sweep's allowed set depends on it.
    """
    for key in COMPONENT_POPS[component]:
        if key in DltAssetGroupConfig.model_fields:
            # `name` and `hints` are real fields: the component pops them to
            # supply its own value, not to hide them from the model.
            continue
        with pytest.raises(pydantic.ValidationError):
            DltAssetGroupConfig(name="p", **{key: "whatever"})


#: The pop call shapes the components actually use. One expression, because a
#: second spelling here would silently stop matching a component that changed.
_POP_RE = re.compile(r"""pipeline_attrs\.pop\(\s*["']([A-Za-z_][A-Za-z0-9_]*)["']""")

_COMPONENT_MODULES = {
    "DltPipelineComponent": "dag_tools.components.dlt_pipeline.component",
    "RestateDltSyncComponent": "dag_tools.components.restate_dlt_sync.component",
    "RestateApiSyncComponent": "dag_tools.components.restate_api_sync.component",
}


@pytest.mark.parametrize("component", sorted(COMPONENT_POPS))
def test_the_pop_lists_match_the_components(component):
    """``COMPONENT_POPS`` is the compatibility proof's allowed set, so a stale
    entry there would make that proof say the wrong thing: a key dropped from
    this list reads as refused when it is popped, and a pop this list has not
    heard about widens the allowed set for nobody. Read out of the component's
    source, asserted as an equality in both directions, so either kind of
    drift fails here rather than at an operator's load.
    """
    module = importlib.import_module(_COMPONENT_MODULES[component])
    found = set(_POP_RE.findall(inspect.getsource(module)))
    assert found, "the pop expression matched nothing -- this check is vacuous"
    assert found == COMPONENT_POPS[component]


def test_control_the_pop_expression_matches_a_real_call_shape():
    """Without this, a regex that matches nothing would fail the assertion
    above for the wrong reason and a regex typo would look like component
    drift."""
    assert _POP_RE.findall('x = pipeline_attrs.pop("row_filters", None) or {}') == [
        "row_filters"
    ]


def test_the_dev_loader_filters_by_the_models_own_fields():
    """``dlt_dev.loader`` passes an operator's whole attrs dict, so it filters
    by ``model_fields`` rather than popping -- which is what keeps the dev loop
    accepting the same YAML the component does. Asserted as the model's own
    field set, so it cannot drift into a copied list."""
    from dag_tools.dlt_dev import loader

    assert "set(DltAssetGroupConfig.model_fields)" in inspect.getsource(loader)


# --- no shipped example may break --------------------------------------------


def _pipeline_blocks():
    """Every ``pipelines:`` block in a shipped example, with the component
    that owns it. Read off the YAML rather than by loading the components,
    which would need credentials and a warehouse."""
    yaml = pytest.importorskip("yaml")
    root = pathlib.Path(__file__).resolve().parents[1]
    out = []
    for path in sorted((root / "examples").rglob("*.yaml")):
        try:
            docs = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        except Exception:
            continue
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            attrs = doc.get("attributes") or {}
            pipelines = attrs.get("pipelines") if isinstance(attrs, dict) else None
            if not isinstance(pipelines, dict):
                continue
            component = next(
                (c for c in COMPONENT_POPS if str(doc.get("type", "")).endswith(c)),
                None,
            )
            if component is None:
                continue  # a component that does not build this model
            for name, pipeline_attrs in pipelines.items():
                if isinstance(pipeline_attrs, dict):
                    out.append((path.relative_to(root), component, name,
                                set(pipeline_attrs)))
    return out


def test_no_shipped_example_carries_a_key_this_model_would_refuse():
    """The compatibility proof. Every pipeline block in every shipped example
    uses only keys the model declares or its component pops -- so making the
    model strict breaks none of them."""
    blocks = _pipeline_blocks()
    assert blocks, "found no pipeline blocks to check -- the sweep is vacuous"
    offenders = []
    for path, component, name, keys in blocks:
        allowed = set(DltAssetGroupConfig.model_fields) | COMPONENT_POPS[component]
        extra = sorted(keys - allowed)
        if extra:
            offenders.append(f"{path} [{component}] {name}: {extra}")
    assert not offenders, (
        "shipped examples carry keys the model refuses: "
        + "; ".join(offenders)
    )


def test_control_the_sweep_would_catch_a_planted_key():
    """Without this the sweep above passes on a check that looks at nothing."""
    allowed = set(DltAssetGroupConfig.model_fields) | COMPONENT_POPS["DltPipelineComponent"]
    assert sorted({"sources", "cursr"} - allowed) == ["cursr"]
