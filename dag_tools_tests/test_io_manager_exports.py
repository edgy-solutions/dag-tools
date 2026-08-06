"""Every IO manager is reachable from the package root, and still lazily.

``dag_tools.io_managers`` re-exports its managers through a hand-written
``_LAZY_EXPORTS`` table (PEP 562), so adding a submodule and forgetting
the table entry is silent: the manager works fine via its submodule path
and simply isn't there at the root. That happened to the DuckDB manager
-- it shipped, was used in production, and was documented with a
different import path than every one of its siblings before anyone
noticed.

The drift check below discovers managers by walking the submodules, so
it fails for the NEXT one automatically rather than needing a new
assertion each time.
"""
import importlib
import pkgutil
import subprocess
import sys

import pytest

import dag_tools.io_managers as io_managers


# Submodules whose heavy third-party deps may legitimately be absent in a
# slim install -- skipped rather than failed when the import raises.
_SUBMODULES = [
    m.name for m in pkgutil.iter_modules(io_managers.__path__)
    if not m.name.startswith("_")
]


def test_there_are_submodules_to_check():
    """Guards the guard: a broken discovery walk would make every drift
    check below vacuously pass."""
    assert len(_SUBMODULES) >= 5, _SUBMODULES


@pytest.mark.parametrize("submodule", _SUBMODULES)
def test_every_configurable_io_manager_is_exported_from_the_package_root(
    submodule,
):
    """Only the ``Configurable*`` factories are required at the root.

    That's the class a user names in ``Definitions(resources=...)``, so
    it's the one whose import path they write down and the one an
    inconsistency is visible in.

    The inner managers are deliberately NOT required: ``delta`` and
    ``sql`` export theirs, ``arrow`` (``ArrowIOManager``) and ``s3``
    (``FileObjectS3IOManager``) do not. That split is pre-existing and
    harmless -- inner managers are constructed by their factory, not by
    callers -- so pinning it either way would just codify an accident.
    """
    try:
        mod = importlib.import_module(f"dag_tools.io_managers.{submodule}")
    except ImportError as e:
        pytest.skip(f"optional dependency missing for {submodule}: {e}")

    exported = set(io_managers.__all__)
    missing = sorted(
        name for name in dir(mod)
        if name.startswith("Configurable")
        and name.endswith("IOManager")
        # Defined HERE, not imported from a sibling or from dagster.
        and getattr(getattr(mod, name), "__module__", "")
        == f"dag_tools.io_managers.{submodule}"
        and name not in exported
    )
    assert not missing, (
        f"{submodule}.py defines {missing} but they are not in "
        f"_LAZY_EXPORTS, so `from dag_tools.io_managers import "
        f"{missing[0]}` fails while every sibling factory works. Add the "
        f"entry."
    )


def test_exports_actually_resolve():
    """A table entry pointing at a renamed or moved attribute raises only
    on access -- which for a lazy loader means at runtime, in whichever
    deployment happened to use that one manager."""
    unresolved = []
    for name in io_managers.__all__:
        try:
            getattr(io_managers, name)
        except ImportError as e:
            pytest.skip(f"optional dependency missing for {name}: {e}")
        except AttributeError as e:
            unresolved.append((name, str(e)))
    assert not unresolved, unresolved


def test_importing_one_manager_does_not_drag_in_its_siblings():
    """The whole point of the lazy table. Run in a subprocess because
    sys.modules in this one is already polluted by every other test."""
    code = (
        "import sys\n"
        "from dag_tools.io_managers import ConfigurableDuckDBIOManager\n"
        "loaded = [m for m in ('sql', 'delta', 'arrow', 'cortex_io_manager')\n"
        "          if f'dag_tools.io_managers.{m}' in sys.modules]\n"
        "print(','.join(loaded))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"duckdb manager not importable here: {result.stderr[-300:]}")
    assert result.stdout.strip() == "", (
        f"importing the DuckDB manager also imported: {result.stdout.strip()}"
    )
