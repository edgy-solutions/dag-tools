"""`gateway_routes.json` is the published route list, and it must not drift.

The point of publishing it is that a consumer in another repository stops keeping
its own copy. A copy that goes stale is the failure these tests exist to prevent,
so the gate reads the `ROUTE_*` constants out of the gateway's source and demands
the json say exactly the same thing.

The constants are read with `ast`, not by importing the modules. `identity.py` and
`internal_door.py` both import fastapi, so an import-based gate would SKIP wherever
fastapi is absent, and a silent skip is how this drift would ship unnoticed.
"""
from __future__ import annotations

import ast
import json
import sys
import tomllib
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parents[1]
GATEWAY_DIR = ROOT / "dag_tools" / "central_gateway"
ROUTES_JSON = GATEWAY_DIR / "gateway_routes.json"
LOADER = GATEWAY_DIR / "routes.py"
PYPROJECT = ROOT / "pyproject.toml"

#: The one route the delegate table is keyed on. Everything else is a door.
DELEGATABLE = "assets.authorize"


def _published() -> Dict[str, object]:
    return json.loads(ROUTES_JSON.read_text(encoding="utf-8"))


def _module_level_strings(path: Path) -> Dict[str, str]:
    """Every module-level `NAME = "literal"` in one source file."""
    out: Dict[str, str] = {}
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if not isinstance(node, ast.Assign):
            continue
        if not isinstance(node.value, ast.Constant) or not isinstance(node.value.value, str):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                out[target.id] = node.value.value
    return out


def _route_constants() -> Dict[str, str]:
    """Every `ROUTE_*` string constant in the gateway package, keyed by its FQN."""
    found: Dict[str, str] = {}
    for path in sorted(GATEWAY_DIR.glob("*.py")):
        for name, value in _module_level_strings(path).items():
            if name.startswith("ROUTE_"):
                found[f"dag_tools.central_gateway.{path.stem}.{name}"] = value
    return found


def test_the_published_ids_are_exactly_the_route_constants_in_the_code():
    """The drift gate: add, rename or drop a route and this fails."""
    constants = _route_constants()
    assert constants, "found no ROUTE_* constants to compare against"
    published = {route["id"] for route in _published()["routes"]}
    assert published == set(constants.values())


def test_each_route_names_the_constant_it_was_read_from():
    """So the `constant` field cannot quietly point at the wrong name."""
    constants = _route_constants()
    for route in _published()["routes"]:
        fqn = route["constant"]
        assert fqn in constants, f"{fqn} is not a ROUTE_* constant in the gateway package"
        assert constants[fqn] == route["id"]


def test_every_route_says_whether_a_delegate_row_may_name_it():
    """The field the consumer reads instead of keeping its own answer."""
    for route in _published()["routes"]:
        assert "delegate_rows_may_name" in route, route["id"]
        # A string would be truthy either way, which is the mistake worth catching.
        assert isinstance(route["delegate_rows_may_name"], bool), route["id"]


def test_the_delegate_table_route_is_the_only_one_a_delegate_row_may_name():
    named = [r["id"] for r in _published()["routes"] if r["delegate_rows_may_name"]]
    assert named == [DELEGATABLE]


def test_each_door_names_the_caller_list_it_authorizes_against():
    """A door authorizes against its own caller list, and the json says which."""
    env_names = _module_level_strings(GATEWAY_DIR / "internal_door.py")
    for route in _published()["routes"]:
        if route["delegate_rows_may_name"]:
            continue
        callers_env = route["callers_env"]
        assert env_names.get(callers_env) == callers_env, (
            f"{route['id']} names {callers_env}, which is not a setting internal_door.py reads")


def test_the_json_ships_in_the_wheel():
    """It is data in the package, so the wheel must carry the directory it sits in."""
    wheel = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    packages = wheel["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    top_level = ROUTES_JSON.relative_to(ROOT).parts[0]
    assert top_level in packages


def test_the_loader_imports_only_the_standard_library():
    """The consumer is a policy validator, not a Dagster deployment."""
    imported = set()
    for node in ast.walk(ast.parse(LOADER.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])
    assert imported
    assert imported <= sys.stdlib_module_names, sorted(imported - sys.stdlib_module_names)


def test_the_loader_reads_the_published_document():
    from dag_tools.central_gateway import routes

    assert routes.ROUTES_JSON == ROUTES_JSON
    assert routes.route_ids() == [r["id"] for r in _published()["routes"]]
    assert routes.delegatable_route_ids() == [DELEGATABLE]
