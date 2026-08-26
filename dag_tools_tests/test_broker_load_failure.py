"""A broker that could not load its definitions must not claim to own nothing.

An empty ``LOCAL_ASSETS`` is ambiguous. A deployment with no mesh assets
and a deployment whose ``Definitions`` import raised look identical from
the outside, and the second used to:

  * log the traceback once, then continue;
  * report ``{"status": "ok", "assets": 0}`` on /health, so k8s marked the
    pod Ready;
  * register an EMPTY urn list with the gateway.

That last step is the damaging one. The gateway stores the list as this
broker's authoritative claim, so every lookup for an asset this
deployment really does own comes back
``404 No active domain broker found`` -- which reads as "that asset does
not exist", not "this broker never loaded". A missing dependency in a
user-deployment image surfaces to consumers as missing DATA.

Observed in the field as a compiled/pure-Python sqlglot mismatch:

    Failed to load Dagster definitions: No module named
    'sqlglot.expressions.core__mypyc'; 'sqlglot.expressions' is not a package
    Loaded 0 assets from Dagster definitions
"""
import asyncio

import pytest

pytest.importorskip("fastapi")

from dag_tools.domain_broker import main as broker


@pytest.fixture(autouse=True)
def clean_state(monkeypatch):
    """Module-level load state, reset per test."""
    monkeypatch.setattr(broker, "LOCAL_ASSETS", {}, raising=False)
    monkeypatch.setattr(broker, "DEFINITIONS_ERROR", None, raising=False)
    monkeypatch.setattr(broker, "DEFINITIONS_LOADED", False, raising=False)


def _health():
    return asyncio.run(broker.health())


def _ready():
    return asyncio.run(broker.ready())


def _status(response):
    """/ready returns a JSONResponse on failure and a plain dict on success."""
    return getattr(response, "status_code", 200)


# ---------------------------------------------------------------------------
# Still importing
# ---------------------------------------------------------------------------


def test_health_reports_loading_before_the_import_finishes():
    """A real user deployment carrying Dagster + dlt + datahub takes
    90-180s to import. That window must not read as 'ok, zero assets'."""
    assert _health()["status"] == "loading"


def test_ready_is_503_while_still_loading():
    assert _status(_ready()) == 503


# ---------------------------------------------------------------------------
# Import failed
# ---------------------------------------------------------------------------


def test_health_surfaces_the_import_error(monkeypatch):
    monkeypatch.setattr(broker, "DEFINITIONS_LOADED", True)
    monkeypatch.setattr(
        broker, "DEFINITIONS_ERROR",
        "ModuleNotFoundError: No module named 'sqlglot.expressions.core__mypyc'",
    )
    body = _health()
    assert body["status"] == "error"
    assert "sqlglot" in body["definitions_error"]
    assert body["registered"] is False


def test_ready_is_503_after_a_failed_import(monkeypatch):
    monkeypatch.setattr(broker, "DEFINITIONS_LOADED", True)
    monkeypatch.setattr(broker, "DEFINITIONS_ERROR", "boom")
    assert _status(_ready()) == 503


def test_a_failed_import_does_not_register(monkeypatch):
    """The whole point. Pushing an empty list tells the gateway this
    broker owns nothing, and that claim outlives the pod on the route
    TTL."""
    registered = []

    async def _spy(client):
        registered.append(dict(broker.LOCAL_ASSETS))

    monkeypatch.setattr(broker, "_register_once", _spy)
    monkeypatch.setattr(
        broker, "load_dagster_definitions",
        lambda: (_ for _ in ()).throw(ImportError("no sqlglot for you")),
    )

    asyncio.run(broker._startup_load_and_register())

    assert registered == [], "an empty asset list was advertised to the gateway"
    assert broker.DEFINITIONS_ERROR is not None
    assert broker.DEFINITIONS_LOADED is True


# ---------------------------------------------------------------------------
# Loaded cleanly
# ---------------------------------------------------------------------------


def test_a_clean_load_with_zero_assets_is_still_ok(monkeypatch):
    """Genuinely empty is a legitimate state -- a deployment may simply
    advertise nothing. It must stay distinguishable from a failure, and
    it must still register, or its emptiness never propagates."""
    registered = []

    async def _spy(client):
        registered.append(dict(broker.LOCAL_ASSETS))

    monkeypatch.setattr(broker, "_register_once", _spy)
    monkeypatch.setattr(broker, "load_dagster_definitions", lambda: None)

    async def _no_loop():
        return None

    monkeypatch.setattr(broker, "_re_register_loop", _no_loop)
    asyncio.run(broker._startup_load_and_register())

    assert registered == [{}], "a clean empty load must still register"
    monkeypatch.setattr(broker, "DEFINITIONS_LOADED", True)
    assert _health()["status"] == "ok"
    assert _status(_ready()) == 200


def test_a_successful_load_registers_its_assets(monkeypatch):
    registered = []

    async def _spy(client):
        registered.append(dict(broker.LOCAL_ASSETS))

    async def _no_loop():
        return None

    def _load():
        broker.LOCAL_ASSETS["urn:li:dataset:(x,y,PROD)"] = {"io_manager_type": "s3_parquet"}

    monkeypatch.setattr(broker, "_register_once", _spy)
    monkeypatch.setattr(broker, "_re_register_loop", _no_loop)
    monkeypatch.setattr(broker, "load_dagster_definitions", _load)

    asyncio.run(broker._startup_load_and_register())

    assert len(registered) == 1 and len(registered[0]) == 1
    assert broker.DEFINITIONS_ERROR is None


# ---------------------------------------------------------------------------
# Telling the operator WHICH thing about DAGSTER_DEFS_MODULE is wrong
# ---------------------------------------------------------------------------
#
# "No module named 'mfg.definitions'" is ambiguous, and expensively so: the
# package may have failed to import, or the package may be fine and simply
# have no submodule by that name. Those have completely different fixes.
# It is worst when importing the package emits hundreds of lines of its own
# output first -- that reads as success, so the error looks like it came
# from somewhere else entirely.

from dag_tools.domain_broker.main import (
    _definitions_attrs,
    _import_defs_module,
    _split_defs_module,
)


def test_a_spec_without_an_attribute_means_discover_it():
    """Deliberately NOT an error. See the package-name section below: a
    deployment started with `--package-name mfg` has no attribute name to
    quote, and requiring one sent operators hunting for a name Dagster
    never asks them to know."""
    assert _split_defs_module("mfg.definitions") == ("mfg.definitions", None)


def test_a_well_formed_spec_splits():
    assert _split_defs_module("mfg.definitions:defs") == ("mfg.definitions", "defs")


def test_a_missing_submodule_says_the_parent_was_fine():
    """The reported case: `import mfg` succeeds and builds the world, then
    mfg.definitions turns out not to exist."""
    with pytest.raises(ModuleNotFoundError) as exc:
        _import_defs_module("json.definitions")
    message = str(exc.value)
    assert "imported fine" in message
    assert "pkgutil" in message, "the message should say how to list the real submodules"


def test_a_broken_parent_package_surfaces_its_own_error():
    """When the PACKAGE is what fails, its error is the actionable one and
    must not be replaced by a tidier message about submodules."""
    with pytest.raises(ModuleNotFoundError) as exc:
        _import_defs_module("no_such_package_anywhere.definitions")
    assert "imported fine" not in str(exc.value)


def test_definitions_attrs_lists_candidates():
    """So "no attribute 'defs'" can say what IS there."""
    pytest.importorskip("dagster")
    import types

    from dagster import Definitions

    module = types.ModuleType("fake")
    module.definitions = Definitions(assets=[])
    module.not_defs = 42
    module._private = Definitions(assets=[])

    assert _definitions_attrs(module) == ["definitions"]


# ---------------------------------------------------------------------------
# Matching how the deployment is actually started
# ---------------------------------------------------------------------------
#
# A user deployment runs as `dagster api grpc --package-name mfg`. There is
# no mfg.definitions module in that world -- Dagster imports the PACKAGE and
# finds the Definitions as an attribute on it. Demanding
# "mfg.definitions:defs" from the broker meant hunting for a name Dagster
# never asks anyone to know, and reported a perfectly healthy package as
# "No module named 'mfg.definitions'".

from dag_tools.domain_broker.main import _discover_definitions


def test_a_bare_package_name_is_accepted():
    """`DAGSTER_DEFS_MODULE=mfg`, matching `--package-name mfg`."""
    assert _split_defs_module("mfg") == ("mfg", None)


def test_an_explicit_attribute_still_wins():
    assert _split_defs_module("mfg:defs") == ("mfg", "defs")


def test_whitespace_and_empty_attribute_are_tolerated():
    assert _split_defs_module("  mfg:  ") == ("mfg", None)


def test_an_empty_spec_is_refused():
    with pytest.raises(ValueError):
        _split_defs_module("")


def test_discovery_finds_the_single_definitions():
    pytest.importorskip("dagster")
    import types

    from dagster import Definitions

    module = types.ModuleType("mfg")
    module.defs = Definitions(assets=[])
    module.unrelated = 42

    assert _discover_definitions(module, "mfg") is module.defs


def test_discovery_refuses_to_guess_between_several():
    """Picking one at random would advertise half a deployment and look
    like it worked."""
    pytest.importorskip("dagster")
    import types

    from dagster import Definitions

    module = types.ModuleType("mfg")
    module.defs = Definitions(assets=[])
    module.other_defs = Definitions(assets=[])

    with pytest.raises(AttributeError, match="more than one"):
        _discover_definitions(module, "mfg")


def test_discovery_on_a_module_with_none_points_at_submodules():
    pytest.importorskip("dagster")
    import types

    module = types.ModuleType("mfg")
    with pytest.raises(AttributeError, match="<submodule>"):
        _discover_definitions(module, "mfg")
