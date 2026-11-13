"""Query bodies for the three read routes. Pure.

A value is NEVER interpolated into a filter: the filter string holds only ``@n``
placeholders, and every value travels in ``substitutions``. ``assert_values_only_in_substitutions``
is the check the tests (and a paranoid caller) use to prove it.
"""
import re
from typing import Any, Dict, List, Optional, Sequence

ROUTES = ("query-results", "query-steps", "query-products")

_PLACEHOLDER = re.compile(r"@\d+")
_ALLOWED_FILTER_TOKENS = re.compile(
    r"^(?:\s|\(|\)|&&|\|\||==|!=|>=|<=|>|<|\bin\b|[A-Za-z_][A-Za-z0-9_.]*|@\d+)*$"
)


def _scoped(filt: str, subs: List[Any], workspace_id: Optional[str]):
    subs = list(subs)
    if workspace_id is not None:
        filt = f"({filt}) && workspace == @{len(subs)}"
        subs.append(workspace_id)
    return filt, subs


def _page(body: Dict[str, Any], take: int, token: Optional[str]) -> Dict[str, Any]:
    body["take"] = take
    if token:
        body["continuationToken"] = token
    return body


def results_query(
    part_number: str,
    *,
    order_by: str,
    take: int,
    workspace_id: Optional[str] = None,
    token: Optional[str] = None,
    updated_after: Optional[str] = None,
) -> Dict[str, Any]:
    filt, subs = "partNumber == @0", [part_number]
    if updated_after is not None:
        filt = f"({filt}) && updatedAt > @{len(subs)}"
        subs.append(updated_after)
    filt, subs = _scoped(filt, subs, workspace_id)
    body = {"filter": filt, "substitutions": subs, "orderBy": order_by, "descending": True}
    return _page(body, take, token)


def count_query(part_number: str, *, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """The sizing pass: one row requested, the server's total asked for."""
    filt, subs = _scoped("partNumber == @0", [part_number], workspace_id)
    return {"filter": filt, "substitutions": subs, "take": 1, "returnCount": True}


def steps_query(
    result_ids: Sequence[str],
    *,
    form: str,
    take: int,
    workspace_id: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    ids = [str(i) for i in result_ids]
    if not ids:
        raise ValueError("a steps query needs at least one result id")
    if form == "or_chain":
        filt, subs = " || ".join(f"resultId == @{i}" for i in range(len(ids))), list(ids)
    elif form == "in":
        filt, subs = "resultId in @0", [ids]
    else:
        raise ValueError(f"unknown step filter form {form!r}")
    filt, subs = _scoped(filt, subs, workspace_id)
    return _page({"filter": filt, "substitutions": subs}, take, token)


def products_query(
    part_numbers: Sequence[str], *, take: int, token: Optional[str] = None
) -> Dict[str, Any]:
    pns = [str(p) for p in part_numbers]
    if not pns:
        raise ValueError("a products query needs at least one part number")
    filt = " || ".join(f"partNumber == @{i}" for i in range(len(pns)))
    return _page({"filter": filt, "substitutions": pns}, take, token)


def assert_values_only_in_substitutions(body: Dict[str, Any]) -> None:
    """Raise if the filter carries anything but identifiers, operators and ``@n``."""
    filt = body.get("filter", "")
    if not _ALLOWED_FILTER_TOKENS.match(filt):
        raise AssertionError("filter carries something other than names, operators and @n")
    subs = body.get("substitutions", [])
    used = {int(p[1:]) for p in _PLACEHOLDER.findall(filt)}
    if used != set(range(len(subs))):
        raise AssertionError("placeholders and substitutions do not line up")
