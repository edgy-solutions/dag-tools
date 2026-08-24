"""Every Restate handler is reachable, and the chart says so.

Two separate ways a handler can exist but never run:

  * it is not in SERVICE_REGISTRY, so RESTATE_SERVICES rejects the key and
    the worker refuses to start;
  * it IS in the registry but the Helm chart's documented key list omits
    it, so no operator ever knows to select it. That is not a cosmetic
    problem -- the list in values.yaml is the only place an operator looks
    to find out what a worker can host, and oracle_control was missing
    from it while being required for the whole PDM cycle.

The second is what these tests exist for. Nothing enforced the comment
against the code, so it drifted the moment a handler was added.
"""
import re
from pathlib import Path

import pytest

pytest.importorskip("restate")

from dag_tools.restate_handlers import serve


VALUES = Path(__file__).parent.parent / "helm" / "dag-tools" / "values.yaml"


def test_the_registry_is_not_empty():
    """Guards the guard: an empty registry would make everything below
    vacuously pass."""
    assert len(serve.SERVICE_REGISTRY) >= 4, serve.SERVICE_REGISTRY


@pytest.mark.parametrize("key", sorted(serve.SERVICE_REGISTRY))
def test_every_registered_service_imports_and_exposes_a_service(key):
    """A registry entry pointing at a missing module or a module without a
    module-level `service` fails at worker STARTUP, taking down every other
    handler in the same pod with it."""
    import importlib

    module = importlib.import_module(serve.SERVICE_REGISTRY[key])
    assert hasattr(module, "service"), f"{key} exposes no module-level `service`"


@pytest.mark.parametrize("key", sorted(serve.SERVICE_REGISTRY))
def test_every_registered_service_is_documented_in_the_chart(key):
    """values.yaml is where an operator learns which keys are valid."""
    text = VALUES.read_text(encoding="utf-8")
    assert re.search(rf"\b{re.escape(key)}\b", text), (
        f"{key!r} is in SERVICE_REGISTRY but appears nowhere in "
        f"helm/dag-tools/values.yaml, so nobody deploying the chart would "
        f"know it can be hosted."
    )


def test_the_chart_default_is_a_valid_selection():
    """A default naming an unknown key would make the chart fail to start
    out of the box."""
    text = VALUES.read_text(encoding="utf-8")
    match = re.search(r'^\s+services:\s*"([^"]*)"', text, re.MULTILINE)
    assert match, "no `services:` default found in values.yaml"
    selected = [k.strip() for k in match.group(1).split(",") if k.strip()]
    unknown = [k for k in selected if k not in serve.SERVICE_REGISTRY]
    assert not unknown, unknown


def test_selecting_the_pdm_pair_builds(monkeypatch):
    """The PDM cycle needs both oracle handlers. The ack alone leaves the
    MEI request and the completion row with nowhere to POST."""
    monkeypatch.setenv("RESTATE_SERVICES", "oracle_ack,oracle_control")
    assert serve._selected_service_keys() == ["oracle_ack", "oracle_control"]
    assert serve.build_app() is not None


def test_an_unknown_key_is_refused_by_name(monkeypatch):
    """Silently ignoring it would start a worker hosting nothing the
    operator asked for."""
    monkeypatch.setenv("RESTATE_SERVICES", "oracle_ack,oracle_contol")
    with pytest.raises(ValueError, match="oracle_contol"):
        serve._selected_service_keys()
