"""The withheld-asset tally reaches a LOG, not ``/health``.

``_unadvertisable_reason``'s docstring promised that its return values were
"stable keys for ``/health.adr0044.unadvertised``". No such key has ever been
served: ``health()`` carries ``echoed_credentials_dropped``,
``unprotected_source_types`` and ``non_fqdn_hosts``, and the withheld-asset
reasons go to ``logger.info`` per asset plus one ``logger.warning`` carrying
``dict(UNADVERTISED_ASSETS)`` at the end of the pass. The same claim was in
``docs/publishing-io-managers-to-the-mesh.md``.

That is the worst shape a doc defect can take: an operator diagnosing a
deployment that advertises nothing is sent to a payload, finds the field
absent, and cannot tell "no assets were withheld" from "I am looking in the
wrong place" -- which is precisely the confusion ``_unadvertisable_reason``
exists to end. So the prose is corrected and pinned here, in both directions:
the payload must not grow the key behind the prose's back, and the prose must
not claim it again.

Whether ``/health`` SHOULD serve the tally is a separate question about the
broker's posture numbers and is deliberately not decided here; this module
only holds the code and its description to each other.
"""
from __future__ import annotations

import ast
import asyncio
import pathlib

import pytest

from dag_tools.domain_broker import main as broker

_MAIN = pathlib.Path(broker.__file__)
_DOC = _MAIN.parents[2] / "docs" / "publishing-io-managers-to-the-mesh.md"
_CLAIM = "adr0044.unadvertised"


@pytest.fixture(autouse=True)
def _reset():
    broker.UNADVERTISED_ASSETS.clear()
    yield
    broker.UNADVERTISED_ASSETS.clear()


def _health():
    broker.DEFINITIONS_LOADED = True
    broker.DEFINITIONS_ERROR = None
    return asyncio.run(broker.health())


def _flat_keys(obj):
    """Every mapping key at every depth, so a nested key cannot hide."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k
            yield from _flat_keys(v)


# ── the payload ────────────────────────────────────────────────────────────

def test_a_non_empty_tally_still_reaches_no_key_on_the_health_payload():
    """The tally is populated and the payload still does not carry it.

    Planting the entry is what makes this non-vacuous: a version of this test
    that asked an empty tally whether it was served would pass against a
    ``/health`` that serves the tally perfectly well.
    """
    broker.UNADVERTISED_ASSETS["no IO manager bound"] = 3

    payload = _health()

    assert broker.UNADVERTISED_ASSETS == {"no IO manager bound": 3}
    assert not [k for k in _flat_keys(payload) if "unadvertised" in k], payload


def test_the_posture_block_is_served_so_the_absence_above_is_about_this_tally():
    """The block the claim named EXISTS; it is the one field that does not.

    Without this, the test above would also pass against a broker serving no
    posture numbers at all -- "the key is absent" would be a fact about the
    whole payload rather than about the tally. The block's membership is
    deliberately not pinned here: a new posture number is somebody else's
    change and has no business failing this module.
    """
    assert _health()["adr0044"]


# ── the code, read rather than described ───────────────────────────────────

def _function(name):
    tree = ast.parse(_MAIN.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in {_MAIN}")


def test_the_health_function_does_not_read_the_tally_at_all():
    """A structural check, not a string scan: the name is absent from health()."""
    names = {n.id for n in ast.walk(_function("health")) if isinstance(n, ast.Name)}

    assert "UNADVERTISED_ASSETS" not in names


def test_the_tally_is_carried_by_a_logger_warning():
    """The other half of the corrected docstring: it goes to the log."""
    warnings_carrying_it = [
        call for call in ast.walk(_function("load_dagster_definitions"))
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "warning"
        and "UNADVERTISED_ASSETS" in {
            n.id for n in ast.walk(call) if isinstance(n, ast.Name)
        }
    ]

    assert len(warnings_carrying_it) == 1, warnings_carrying_it


# ── the prose ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", [_MAIN, _DOC], ids=lambda p: p.name)
def test_no_text_promises_a_health_key_the_payload_does_not_carry(path):
    text = path.read_text(encoding="utf-8")

    assert _CLAIM not in text, f"{path} claims /health.{_CLAIM}"


def test_control_the_scan_finds_the_claim_when_it_is_there():
    """Without this, deleting ``_CLAIM`` would make the test above vacuous."""
    planted = "the reason is counted in `/health.adr0044.unadvertised`."

    assert _CLAIM in planted
