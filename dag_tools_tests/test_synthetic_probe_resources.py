"""Phase C3: opt-in binding of the class's non-IO-manager resources."""
import ast
import importlib.util

import pytest
from pydantic import ValidationError

from dag_tools.qual.qualify import ProbeSettings
from dag_tools.qual.synthetic import generate_probe_module
from dag_tools_tests.test_synthetic_generator import _class

import dagster as dg

IO_FQN = "dagster._core.storage.mem_io_manager.InMemoryIOManager"
HERE = "dag_tools_tests.test_synthetic_probe_resources"
WH = f"{HERE}.WarehouseResource"
BAD = "no_such_pkg_xyz.Missing"


class WarehouseResource(dg.ConfigurableResource):
    host: str = "localhost"
    port: int = 5432


def _cls(resources=None, class_hash="abc123def456"):
    c = _class(class_hash=class_hash, io_manager_class=IO_FQN)
    c.representatives[0].resource_classes = dict(
        {"warehouse": WH} if resources is None else resources
    )
    return c


def _gen(settings=None, cls=None):
    probe, source = generate_probe_module(
        cls or _cls(), qual_id="q1", probe_settings=settings,
    )
    assert probe is not None
    return probe, source


def _load(source, tmp_path, name="probe_mod"):
    path = tmp_path / f"{name}.py"
    path.write_text(source, encoding="utf-8")
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_no_resource_config_emits_source_identical_to_c1():
    _, with_resources = _gen(ProbeSettings())
    plain = _class(class_hash="abc123def456", io_manager_class=IO_FQN)
    _, baseline = generate_probe_module(plain, qual_id="q1", probe_settings=ProbeSettings())
    assert with_resources == baseline
    assert "_RESOURCES" not in with_resources and "context" not in with_resources


def test_a_configured_resource_is_bound_under_a_class_unique_key():
    probe, source = _gen(ProbeSettings(resource_config={WH: {"host": "h"}}))
    assert probe.resource_bindings == {"warehouse_abc123de": WH}
    assert "'warehouse': 'warehouse_abc123de'" in source
    assert probe.resource_config_keys == {"warehouse_abc123de": ["host"]}


def test_two_probes_with_the_same_resource_key_do_not_collide_on_merge(tmp_path):
    s = ProbeSettings(resource_config={WH: {}})
    mods = []
    for h in ("aaaaaaaa1111", "bbbbbbbb2222"):
        _, src = _gen(s, _cls(class_hash=h))
        mods.append(_load(src, tmp_path, name=f"probe_{h[:8]}"))
    merged = dg.Definitions.merge(mods[0].defs, mods[1].defs)
    assert {"warehouse_aaaaaaaa", "warehouse_bbbbbbbb"} <= set(merged.resources)


def test_an_empty_config_dict_binds_the_resource_with_no_arguments(tmp_path):
    probe, src = _gen(ProbeSettings(resource_config={WH: {}}))
    assert probe.resource_bindings
    mod = _load(src, tmp_path)
    assert isinstance(mod.defs.resources["warehouse_abc123de"], WarehouseResource)


def test_a_resource_without_a_config_entry_is_not_bound_and_is_noted():
    probe, source = _gen(ProbeSettings(resource_config={"other.Thing": {}}))
    assert probe.resource_bindings == {}
    assert any("warehouse" in n and WH in n for n in probe.notes)
    assert "_RESOURCES" not in source


def test_env_reference_in_resource_config_never_renders_the_literal(monkeypatch):
    sentinel = "SENTINEL-r3s0urce-do-not-persist"
    monkeypatch.setenv("QUAL_PROBE_WH_PW", sentinel)
    probe, source = _gen(
        ProbeSettings(resource_config={WH: {"host": {"env": "QUAL_PROBE_WH_PW"}}})
    )
    assert "os.environ['QUAL_PROBE_WH_PW']" in source
    assert "import os\n" in source
    assert sentinel not in source
    assert sentinel not in probe.model_dump_json()
    ast.parse(source)


def test_each_resource_construct_is_in_its_own_try_block():
    cls = _cls({"warehouse": WH, "second": WH})
    _, source = _gen(ProbeSettings(resource_config={WH: {}}), cls)
    tree = ast.parse(source)
    tries = [n for n in tree.body if isinstance(n, ast.Try)]
    assert len(tries) == 3  # IO manager + 2 resources
    inside = {id(c) for t in tries for n in t.body for c in ast.walk(n)}
    seen = 0
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id.startswith(("_IO_MANAGER_CLASS", "_RESOURCE_CLASS_"))
        ):
            seen += 1
            assert id(node) in inside
    assert seen == 3


def test_a_failing_resource_does_not_reach_the_definitions_resources(tmp_path):
    cls = _cls({"warehouse": WH, "broken": BAD})
    _, src = _gen(ProbeSettings(resource_config={WH: {}, BAD: {}}), cls)
    mod = _load(src, tmp_path)
    assert list(mod._RESOURCE_ERRORS) == ["broken_abc123de"]
    assert "broken_abc123de" not in mod.defs.resources
    assert "warehouse_abc123de" in mod.defs.resources


def test_the_probe_asset_requires_every_bound_resource_key(tmp_path):
    _, src = _gen(ProbeSettings(resource_config={WH: {}}))
    mod = _load(src, tmp_path)
    down = next(a for a in mod.defs.assets if "downstream" in str(a.key))
    assert "warehouse_abc123de" in down.required_resource_keys


def test_the_probe_materializes_with_a_bound_resource(tmp_path):
    _, src = _gen(ProbeSettings(resource_config={WH: {}}))
    mod = _load(src, tmp_path)
    result = mod.defs.get_implicit_global_asset_job_def().execute_in_process()
    assert result.success


def test_a_failed_resource_fails_the_probe_at_materialization_not_at_load(tmp_path):
    cls = _cls({"broken": BAD})
    _, src = _gen(ProbeSettings(resource_config={BAD: {}}), cls)
    mod = _load(src, tmp_path)
    result = mod.defs.get_implicit_global_asset_job_def().execute_in_process(
        raise_on_error=False
    )
    assert not result.success


def test_the_io_manager_round_trip_assertion_is_unchanged():
    _, plain = _gen(ProbeSettings(), _class(io_manager_class=IO_FQN))
    _, bound = _gen(ProbeSettings(resource_config={WH: {}}))
    for line in (
        'assert isinstance(payload, dict), "payload was not a dict after round-trip"',
        'assert payload.get("class_hash") == CLASS_HASH, (',
        '"payload corrupted after IO manager round-trip"',
        "payload = probe_abc123de_upstream",
    ):
        assert line in plain and line in bound


@pytest.mark.parametrize("bad", [
    {"1bad": 1},
    {"class": 1},
    {"k": {"env": "X", "extra": 1}},
    {"k": {"env": ""}},
    {"k": object()},
    {"k": {"nested": {"env": "X"}}},
])
def test_resource_config_values_are_validated_like_io_manager_config(bad):
    with pytest.raises(ValidationError) as exc:
        ProbeSettings(resource_config={WH: bad})
    assert WH in str(exc.value)
    with pytest.raises(ValidationError):
        ProbeSettings(io_manager_config={WH: bad})


def test_probe_config_yaml_block_reaches_resource_config():
    # `qual init --probe-config` validates the whole YAML via ProbeSettings.
    settings = ProbeSettings.model_validate({"resource_config": {WH: {"port": 1}}})
    assert settings.has_resource_config(WH)
    assert not settings.has_resource_config("other.Thing")
    assert settings.resource_kwargs(WH) == {"port": 1}


def test_generated_source_parses_as_python():
    _, source = _gen(
        ProbeSettings(resource_config={WH: {"host": {"env": "H"}, "port": 1}})
    )
    ast.parse(source)


def test_probe_source_still_does_not_import_dag_tools_qual():
    _, source = _gen(ProbeSettings(resource_config={WH: {"host": {"env": "H"}}}))
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("dag_tools.qual")
        if isinstance(node, ast.Import):
            assert not any(a.name.startswith("dag_tools.qual") for a in node.names)
