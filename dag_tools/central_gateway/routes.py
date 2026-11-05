"""The gateway's published route list, read from `gateway_routes.json`.

The json sits beside this module and ships inside the wheel, so a consumer in
another repository reads the route ids out of the installed package instead of
keeping a copy that goes stale. `delegate_rows_may_name` is the published answer
to "may a delegate row name this route?": read that field rather than deciding it
from the route's name or its module.

The json is the contract. These helpers are a convenience for Python callers; a
consumer in another language reads the file directly.

Imports nothing but the standard library, so a reader needs neither fastapi nor
dagster installed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

#: The published document, next to this module so it travels in the wheel.
ROUTES_JSON = Path(__file__).parent / "gateway_routes.json"


def load_routes() -> Dict[str, Any]:
    """The whole published document, parsed."""
    with ROUTES_JSON.open(encoding="utf-8") as handle:
        return json.load(handle)


def route_ids() -> List[str]:
    """Every published route id, in document order."""
    return [route["id"] for route in load_routes()["routes"]]


def delegatable_route_ids() -> List[str]:
    """The route ids a delegate row may name, in document order."""
    return [r["id"] for r in load_routes()["routes"] if r["delegate_rows_may_name"]]
