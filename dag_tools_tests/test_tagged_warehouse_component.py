"""`TaggedWarehouseComponent` -- the YAML surface, loaded the way a deployment loads it.

The component is the primary surface: a deployment writes `component.yaml`,
not Python. So these tests go through `build_component_defs` over the real
example rather than constructing the class, which is the only way to catch
the failures that matter here -- a field Dagster rejects, a resource nothing
ends up requiring, a block that is accepted and then ignored.

**The drop-in is the claim under test.** The example's second component is
step 1's `ReadAuditComponent`, unchanged, reading through this resource by
key. If the seam between them ever needs a change on the audit side, the
end-to-end test below is where that shows up.

**The compat floor is probed, not assumed.** 1.10.19 -- the version
production runs -- rejects unknown keys in a component file outright and
fails the whole code-location load. This module loads the example on whatever
Dagster is installed and, if the load is refused for that reason, skips while
naming the field. Anything else that goes wrong still fails loudly.

Every object name in the fixtures is synthetic; the warehouse is sqlite.
"""

import importlib.util
import shutil
from pathlib import Path

import dagster as dg
import pytest
from dagster.components import build_component_defs

EXAMPLE_ROOT = Path(__file__).resolve().parents[1] / "examples" / "tagged_warehouse"
EXAMPLE = EXAMPLE_ROOT / "dagster_home" / "components"


def _chain_text(exc: BaseException) -> str:
    """Every message in the exception chain, as one string."""
    parts = []
    seen = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _skip_if_the_component_file_is_refused(exc: Exception) -> None:
    """Skip when this Dagster will not accept this component file.

    Matched on the raised error rather than on a version number: the field
    the loader rejects is the fact, and a version is only a proxy for it.
    """
    text = _chain_text(exc)
    if "extra_forbidden" in text or "Extra inputs are not permitted" in text:
        pytest.skip(f"this Dagster rejects a key in the component file: {text}")


def _load(components_dir: Path) -> dg.Definitions:
    try:
        return build_component_defs(components_dir)
    except Exception as exc:  # pragma: no cover - only on an old Dagster
        _skip_if_the_component_file_is_refused(exc)
        raise


def _seed_module():
    spec = importlib.util.spec_from_file_location(
        "tagged_warehouse_example_seed", EXAMPLE_ROOT / "seed.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def example(tmp_path, monkeypatch) -> Path:
    """A copy of the shipped example, pointed at a freshly seeded sqlite file.

    `build_component_defs` imports the folder as `<tmp dir>.components`,
    exactly as the example's own definitions.py does from dagster_home, so
    the parent has to be importable.
    """
    monkeypatch.syspath_prepend(str(tmp_path.parent))
    monkeypatch.setenv(
        "TAGGED_WAREHOUSE_DB", _seed_module().seed(tmp_path / "warehouse.db")
    )
    target = tmp_path / "components"
    shutil.copytree(EXAMPLE, target, ignore=shutil.ignore_patterns("__pycache__"))
    return target


def test_the_shipped_example_loads(example):
    """The example is part of the contract.

    A component whose one worked example cannot load has never been run --
    which is exactly how the dbt example shipped pointing at two projects
    that did not exist.
    """
    defs = _load(example)

    assert sorted(defs.resources or {}) == ["warehouse"]
    assert {
        key.to_user_string()
        for key in defs.resolve_asset_graph().get_all_asset_keys()
    } == {"read_audit"}


def test_the_component_contributes_a_resource_and_no_assets(example):
    """It is a resource provider. An asset appearing here would be a surprise
    in every deployment that loaded the directory."""
    from dag_tools.components.tagged_warehouse.component import (
        TaggedWarehouseComponent,
    )

    defs = TaggedWarehouseComponent(
        tag={"placement": "comment"},
        connection={"drivername": "sqlite", "database": ":memory:"},
        principal={"pipeline_name": "svc"},
    ).build_defs(None)

    assert sorted(defs.resources or {}) == ["warehouse"]
    assert not list(defs.assets or [])


def test_the_resource_drops_in_under_an_unchanged_read_audit(example):
    """End to end: the step-1 asset reads real rows through this resource.

    The numbers are the point. One read was issued through the resource and
    is explained by asset key; one carries no tag; one carries somebody
    else's. A change that collapsed any of those into another would show up
    here as a different count rather than as a passing test.
    """
    defs = _load(example)

    result = dg.materialize(list(defs.assets or []), resources=defs.resources)
    assert result.success

    output = result.output_for_node("read_audit")
    assert output["rows_read"] == 3
    assert output["row_failures"] == []
    assert output["relations_per_asset"] == {
        "orders_summary": [
            "analytics.public.customers",
            "analytics.public.orders",
        ]
    }
    assert output["unexplained_relations"] == [
        "analytics.public.payments",
        "analytics.public.revenue_daily",
    ]
    assert output["method_counts"]["asset_key"] == 1
    assert output["method_counts"]["unattributed"] == 1
    assert output["method_counts"]["not_a_pipeline"] == 1

    metadata = result.asset_materializations_for_node("read_audit")[0].metadata
    assert metadata["coverage"].value == pytest.approx(0.5, abs=1e-4)


def test_the_tag_in_the_fixture_is_the_tag_the_resource_sends(example):
    """The fixture is produced by the shipped resource, not typed by hand.

    This is what makes the end-to-end number meaningful: if the writer and
    the reader of the attribution contract ever drift apart, the example
    stops explaining its read instead of agreeing with a stale literal.
    """
    from dag_tools.audit.tag import decode

    tag = _seed_module().demo_tag("orders_summary", "run-aaa")
    attribution = decode(tag)

    assert attribution.asset_key == "orders_summary"
    assert attribution.principal.startswith("pipeline:")
    assert attribution.run_id == "run-aaa"


def test_the_resource_key_in_the_yaml_is_the_key_the_resource_lands_under(example):
    """The audit names the resource by key, so the key is a contract.

    Hard-coding it would make two components in one directory impossible,
    and a deployment reading two warehouses is the ordinary case.
    """
    yaml_path = example / "warehouse" / "component.yaml"
    yaml_path.write_text(
        yaml_path.read_text(encoding="utf-8").replace(
            "resource_key: warehouse", "resource_key: analytics_warehouse"
        ),
        encoding="utf-8",
    )

    defs = _load(example)
    assert sorted(defs.resources or {}) == ["analytics_warehouse"]


def test_a_tag_statement_without_the_bound_tag_fails_the_load_not_the_run(example):
    """The failure this validation exists for is otherwise invisible.

    A statement that does not bind `:tag` runs happily and every query after
    it is attributed to nothing: the operator sees a window in which nothing
    is explained and goes looking for an untagged pipeline. Failing at load
    names the real problem.
    """
    yaml_path = example / "warehouse" / "component.yaml"
    yaml_path.write_text(
        yaml_path.read_text(encoding="utf-8").replace(
            "  tag:\n    placement: comment\n",
            "  tag:\n    placement: session\n"
            "    statement: ALTER SESSION SET QUERY_TAG = 'fixed'\n",
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception) as excinfo:
        build_component_defs(example)
    _skip_if_the_component_file_is_refused(excinfo.value)
    assert ":tag" in _chain_text(excinfo.value)


def test_a_connection_naming_nothing_fails_the_load(example):
    """Resolution happens at load because it opens nothing.

    The split that makes this safe is the same one `_staging_url` records: a
    URL is resolved, an engine is not built, so no driver is imported while a
    code location loads.
    """
    yaml_path = example / "warehouse" / "component.yaml"
    yaml_path.write_text(
        yaml_path.read_text(encoding="utf-8").replace(
            '    drivername: sqlite\n'
            '    database: "{{ env(\'TAGGED_WAREHOUSE_DB\') }}"\n',
            "    port: 443\n",
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception) as excinfo:
        build_component_defs(example)
    _skip_if_the_component_file_is_refused(excinfo.value)
    assert "no connection" in _chain_text(excinfo.value)


def test_an_unknown_key_in_a_block_is_refused():
    """Blocks are validated with `extra="forbid"`.

    A typo that is quietly ignored is worse here than elsewhere: the
    deployment would believe it had configured a placement or a principal
    that nothing reads.

    Asserted on the models rather than through a load, because the floor
    probe above recognises a refused component file by exactly this pydantic
    error -- so a load-based version of this test would skip itself.
    """
    from dag_tools.components.tagged_warehouse.schema import (
        ConnectionBlock,
        PrincipalBlock,
        TagBlock,
    )

    with pytest.raises(Exception):
        PrincipalBlock.model_validate({"pipline_name": "typo"})
    with pytest.raises(Exception):
        TagBlock.model_validate({"placement": "comment", "statment": "x"})
    with pytest.raises(Exception):
        ConnectionBlock.model_validate({"drivername": "sqlite", "databse": "x"})


# --- the compat floor -------------------------------------------------------
#
# Same two probes as the read audit's, for the same reason: the floor is
# 1.10.19 and the installed Dagster in CI is newer. These pin why the
# component stays inside `type:` + `attributes:` with plain field shapes, so
# a later edit that would move the floor fails here rather than at a
# deployment's code-location load.


def test_the_component_file_uses_no_top_level_key_beyond_type_and_attributes():
    """What 1.10.19 rejects is an unknown key in the component file itself,
    and the rejection fails the entire code-location load rather than just
    the unknown block."""
    import yaml

    for name in ("warehouse", "read_audit"):
        document = yaml.safe_load(
            (EXAMPLE / name / "component.yaml").read_text(encoding="utf-8")
        )
        assert set(document) <= {"type", "attributes"}, name


def test_the_example_names_env_vars_the_way_the_floor_resolves_them():
    """`{{ env.X }}` does not resolve on 1.10.19. `{{ env('X') }}` does.

    Measured on both, loading this very example: 1.13.1 accepts either, and
    on 1.10.19 the attribute form raises `UndefinedError: 'function object'
    has no attribute ...` -- `env` is a plain function in that version's
    template scope, so the attribute access never reaches an environment
    lookup. The rejection fails the whole code-location load, which is the
    same blast radius as an unknown top-level key.

    This example claims the floor, so it spells every reference as a call.
    The claim is cheap to keep and the failure it prevents reads as a
    missing variable rather than as a version problem.
    """
    for name in ("warehouse", "read_audit"):
        body = (EXAMPLE / name / "component.yaml").read_text(encoding="utf-8")
        assert "{{ env." not in body, name


def test_every_component_field_is_a_shape_the_floor_resolves():
    """Blocks arrive as `Dict[str, Any]` and are validated in `schema.py`.

    A nested `Resolvable` model per block would read better in a schema dump
    and would put this component's deployability at the mercy of whatever the
    floor's resolver accepts. Validating ourselves costs one indirection and
    keeps the YAML surface to types every version resolves identically.
    """
    from dag_tools.components.tagged_warehouse.component import (
        TaggedWarehouseComponent,
    )

    allowed = {"typing.Dict[str, typing.Any]", "str"}
    for name, field in TaggedWarehouseComponent.model_fields.items():
        annotation = field.annotation
        rendered = (
            annotation.__name__ if isinstance(annotation, type) else str(annotation)
        )
        assert rendered in allowed, f"{name}: {rendered}"


def test_the_component_is_registered_for_yaml_by_its_public_name():
    """`type: dag_tools.TaggedWarehouseComponent` has to resolve.

    The lazy lookup in `dag_tools/__init__.py` is what makes that name work,
    and a component that is built but not registered is a component no YAML
    can name.
    """
    import dag_tools

    assert dag_tools.TaggedWarehouseComponent.__name__ == "TaggedWarehouseComponent"
    assert "TaggedWarehouseComponent" in dag_tools.__all__
