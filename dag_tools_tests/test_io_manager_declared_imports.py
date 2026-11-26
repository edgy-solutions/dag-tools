"""Every third-party module the IO managers import at module level must be
DECLARED, not merely present.

The s3fs / redis / PyJWT story a third time. ``io_managers/arrow.py``,
``delta.py`` and ``s3.py`` import ``fsspec`` and ``upath`` at module
level, and ``pyproject.toml`` named neither:

* ``fsspec`` arrived because ``s3fs`` requires it (and pins it tightly),
  so the import stood on a package that happens to need it too.
* ``universal-pathlib`` (``from upath import UPath``) arrived ONLY
  because ``dagster`` requires it. Nothing in this project asked for it,
  so an upstream that stopped requiring it would break three IO managers
  for a reason no requirement here names.

Both worked, which is the dependency equivalent of a test passing for the
wrong reason. These tests exist so a NEW undeclared module-level import
fails here rather than in somebody's image.
"""
import ast
import pathlib
import tomllib

from packaging.requirements import Requirement

ROOT = pathlib.Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"

MODULES = [
    ROOT / "dag_tools" / "io_managers" / "arrow.py",
    ROOT / "dag_tools" / "io_managers" / "delta.py",
    ROOT / "dag_tools" / "io_managers" / "s3.py",
]

# The extra a deployment installs to get these IO managers. Core dependencies
# count too, because they are installed either way.
EXTRA = "orchestrator"

# Import name -> distribution name, written out rather than looked up: the
# installed environment is not the thing under test. ``upath`` is the one
# where the two differ, and it is exactly the one that was missing.
IMPORT_TO_DIST = {
    "fsspec": "fsspec",
    "upath": "universal-pathlib",
    "s3fs": "s3fs",
    "pandas": "pandas",
    "pyarrow": "pyarrow",
    "polars": "polars",
    "deltalake": "deltalake",
    "dagster": "dagster",
    "dagster_aws": "dagster-aws",
    "pydantic": "pydantic",
}

# Nothing is exempt. ``pydantic`` was, while it arrived only via dagster;
# it is a core dependency now (see the test below), so it is checked like
# any other name.
EXEMPT = set()


def _declared():
    with PYPROJECT.open("rb") as handle:
        project = tomllib.load(handle)["project"]
    entries = list(project["dependencies"])
    entries += list(project["optional-dependencies"][EXTRA])
    return {Requirement(entry).name.lower().replace("_", "-") for entry in entries}


def _top_level_third_party_imports(path):
    """Module-level imports only, first component only, ours dropped.

    Module level is the point: an import inside a function fails at the call
    rather than at import, so it is not what breaks a code-location load.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    import sys

    return {
        name
        for name in names
        if name != "dag_tools" and name not in sys.stdlib_module_names
    }


def test_every_module_level_import_is_declared_or_named_as_a_known_gap():
    problems = []
    declared = _declared()
    for path in MODULES:
        for name in sorted(_top_level_third_party_imports(path)):
            if name in EXEMPT:
                continue
            dist = IMPORT_TO_DIST.get(name)
            if dist is None:
                problems.append(
                    f"{path.name}: imports {name!r}, which this test has no "
                    f"distribution name for -- add it to IMPORT_TO_DIST"
                )
            elif dist.lower() not in declared:
                problems.append(
                    f"{path.name}: imports {name!r} ({dist}), declared on "
                    f"neither the core dependencies nor the {EXTRA!r} extra"
                )
    assert not problems, "\n".join(problems)


def test_pydantic_is_a_CORE_dependency_not_an_extras():
    """The pure engines import it with no extra installed.

    ``contract_mapping``, ``audit`` and ``documents.mapping`` import pydantic
    at module level and import no dagster, so an extra declaring it would
    still leave a base install unable to import them. Three extras declaring
    it and core not was the gap: it arrived only because dagster requires it.
    """
    with PYPROJECT.open("rb") as handle:
        project = tomllib.load(handle)["project"]
    core = {Requirement(entry).name.lower() for entry in project["dependencies"]}
    assert "pydantic" in core
    for engine in ("contract_mapping/mapping.py", "audit/history.py", "documents/mapping.py"):
        imports = _top_level_third_party_imports(ROOT / "dag_tools" / engine)
        assert "pydantic" in imports and "dagster" not in imports, engine


def test_the_two_names_that_were_missing_are_named_on_both_extras():
    """``fsspec`` and ``universal-pathlib`` specifically, on BOTH extras.

    ``broker`` installs these IO managers too, so a floor or a name on
    ``orchestrator`` alone is the Dockerfile-pin shape all over again.

    NO floor on either, deliberately: the gap is DECLARATION, and no
    release of either has been measured broken here. The ``s3fs`` floor
    beside them exists because 0.4.2 WAS measured failing; inventing a
    bound nothing measured is how a resolve fails against a number this
    library made up.
    """
    with PYPROJECT.open("rb") as handle:
        extras = tomllib.load(handle)["project"]["optional-dependencies"]
    for extra in ("orchestrator", "broker"):
        names = {Requirement(entry).name.lower() for entry in extras[extra]}
        assert "fsspec" in names, extra
        assert "universal-pathlib" in names, extra


def test_control_a_planted_import_is_caught():
    """Without this, the sweep above would pass on a scan that found
    nothing -- an ``ast.walk`` typo, a changed file name, a stdlib filter
    that swallowed everything."""
    tree = ast.parse("import nosuchpackage\nfrom alsonot import thing\n")
    names = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    assert names == {"nosuchpackage", "alsonot"}
    assert not (names & _declared())
    assert not (names & set(IMPORT_TO_DIST))


def test_the_scan_actually_finds_the_two_names_on_the_real_modules():
    """The other half of the control: the real files must still be the
    reason this module exists."""
    found = set()
    for path in MODULES:
        found |= _top_level_third_party_imports(path)
    assert "fsspec" in found
    assert "upath" in found
