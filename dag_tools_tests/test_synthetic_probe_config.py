"""Phase C1: manifest-supplied constructor config for probe IO managers."""
import ast

import pytest
from pydantic import ValidationError

from dag_tools.qual.qualify import ProbeSettings
from dag_tools.qual.synthetic import generate_probe_module
from dag_tools_tests.test_synthetic_generator import _class

FQN = "dag_tools.io_managers.arrow.ConfigurableArrowIOManager"


def _gen(settings=None, **kw):
    probe, source = generate_probe_module(
        _class(**kw), qual_id="q1", probe_settings=settings,
    )
    assert probe is not None
    return probe, source


def _construct_call(tree):
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_IO_MANAGER_CLASS"
        ):
            return node
    raise AssertionError("no _IO_MANAGER_CLASS(...) call")


def test_no_config_emits_the_no_arg_construct_unchanged():
    _, source = _gen()
    assert "_IO_MANAGER = _IO_MANAGER_CLASS()  # operator: override with staging config" in source
    assert "import os" not in source
    _, source2 = _gen(ProbeSettings())
    assert source2 == source


def test_literal_config_renders_as_constructor_kwargs():
    s = ProbeSettings(io_manager_config={FQN: {"base_path": "/tmp/p", "retries": 3, "flag": True}})
    probe, source = _gen(s)
    tree = ast.parse(source)
    call = _construct_call(tree)
    assert {k.arg: ast.literal_eval(k.value) for k in call.keywords} == {
        "base_path": "/tmp/p", "retries": 3, "flag": True,
    }
    assert probe.io_manager_config_keys == ["base_path", "flag", "retries"]


def test_env_reference_renders_os_environ_not_the_literal_value(monkeypatch):
    sentinel = "SENTINEL-s3cr3t-do-not-persist"
    monkeypatch.setenv("QUAL_PROBE_TOKEN", sentinel)
    s = ProbeSettings(io_manager_config={FQN: {"token": {"env": "QUAL_PROBE_TOKEN"}}})
    probe, source = _gen(s)
    assert "os.environ['QUAL_PROBE_TOKEN']" in source or 'os.environ["QUAL_PROBE_TOKEN"]' in source
    assert sentinel not in source
    assert sentinel not in probe.model_dump_json()
    ast.parse(source)


def test_os_is_imported_only_when_an_env_reference_is_rendered():
    lit = ProbeSettings(io_manager_config={FQN: {"base_path": "/x"}})
    _, source = _gen(lit)
    assert "import os" not in source
    env = ProbeSettings(io_manager_config={FQN: {"base_path": {"env": "P"}}})
    _, source = _gen(env)
    assert "import os\n" in source


def test_class_hash_override_beats_the_fqn_entry():
    s = ProbeSettings(
        io_manager_config={FQN: {"base_path": "/fqn"}},
        io_manager_config_by_class={"abc123def456": {"base_path": "/special"}},
    )
    _, source = _gen(s)
    assert "/special" in source and "/fqn" not in source
    _, other = _gen(s, class_hash="ffffffffffff")
    assert "/fqn" in other and "/special" not in other


def test_kwargs_are_sorted_so_regeneration_is_byte_identical():
    a = ProbeSettings(io_manager_config={FQN: {"zeta": 1, "alpha": 2, "mid": 3}})
    b = ProbeSettings(io_manager_config={FQN: {"mid": 3, "alpha": 2, "zeta": 1}})
    _, sa = _gen(a)
    _, sb = _gen(b)
    assert sa == sb
    assert sa.index("alpha=") < sa.index("mid=") < sa.index("zeta=")


@pytest.mark.parametrize("bad", [
    object(),
    {"env": "X", "extra": "y"},
    {"env": ""},
    [object()],
    {"nested": {"env": "X"}},
])
def test_unrenderable_config_value_is_rejected_at_validation_not_emitted(bad):
    with pytest.raises(ValidationError) as exc:
        ProbeSettings(io_manager_config={FQN: {"k": bad}})
    assert FQN in str(exc.value) and "k" in str(exc.value)


def test_env_dict_with_extra_keys_error_says_so():
    with pytest.raises(ValidationError, match="extra keys"):
        ProbeSettings(io_manager_config={FQN: {"k": {"env": "X", "y": 1}}})


@pytest.mark.parametrize("name", ["not-ident", "1abc", "class", "a b"])
def test_non_identifier_kwarg_name_is_rejected(name):
    with pytest.raises(ValidationError, match="identifier"):
        ProbeSettings(io_manager_config={FQN: {name: 1}})
    with pytest.raises(ValidationError, match="identifier"):
        ProbeSettings(io_manager_config_by_class={"abc": {name: 1}})


def test_the_construct_stays_inside_the_try_block():
    s = ProbeSettings(io_manager_config={FQN: {"a": 1, "t": {"env": "T"}}})
    _, source = _gen(s)
    tree = ast.parse(source)
    call = _construct_call(tree)
    tries = [n for n in tree.body if isinstance(n, ast.Try)]
    assert len(tries) == 1, "no second try/except may be added"
    assert any(call is n for stmt in tries[0].body for n in ast.walk(stmt))
    outside = [n for n in tree.body if not isinstance(n, ast.Try)]
    assert not any(call is n for stmt in outside for n in ast.walk(stmt))


def test_missing_config_for_an_io_manager_is_noted():
    probe, _ = _gen()
    assert any(f"no constructor config supplied for `{FQN}`" in n for n in probe.notes)
    cfg = ProbeSettings(io_manager_config={FQN: {"a": 1}})
    probe, _ = _gen(cfg)
    assert not any("no constructor config" in n for n in probe.notes)
    by_class = ProbeSettings(io_manager_config_by_class={"abc123def456": {"a": 1}})
    probe, _ = _gen(by_class)
    assert not any("no constructor config" in n for n in probe.notes)
    probe, _ = _gen(io_manager_class=None)
    assert not any("no constructor config" in n for n in probe.notes)
