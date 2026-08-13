"""The call-plan wire contract, shared by both halves.

Deliberately dependency-free — no Jinja, no Dagster, no Restate. The
Restate handler imports only from here, which is what makes "the worker
has no template engine" an import-time guarantee rather than a
convention someone can quietly break. Every fallback body must therefore
be rendered before dispatch; see ``render._render_step``.
"""
from __future__ import annotations

import copy
from typing import Any

# Version of the plan wire format. The handler refuses plans it does not
# understand rather than half-executing them. Bump when the shape of a
# call, step, or fallback changes incompatibly.
#
# 2: calls carry `call_key` + `dedupe` (per-call delivery suppression),
#    and `path` is percent-encoded with any query string already folded in.
PLAN_FORMAT_VERSION = 2


def set_path(payload: Any, path: str, value: Any) -> Any:
    """Set a dotted path inside a rendered payload, returning a new copy.

    This is the aggregate-fallback mechanism: the bulk container body is
    rendered up front with its collection slot empty, and the handler
    fills that slot with the fragments of the calls that actually failed.
    Copying rather than mutating keeps the plan reusable across a retry.
    """
    result = copy.deepcopy(payload)
    if not path:
        return value
    parts = path.split(".")
    cursor = result
    for part in parts[:-1]:
        if not isinstance(cursor, dict):
            raise ValueError(f"collect_into path '{path}' does not resolve to an object")
        cursor = cursor.setdefault(part, {})
    if not isinstance(cursor, dict):
        raise ValueError(f"collect_into path '{path}' does not resolve to an object")
    cursor[parts[-1]] = value
    return result
