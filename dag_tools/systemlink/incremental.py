"""Classification of fetched results and the newest-first scan with its stop rule. Pure.

A fetched result is exactly one of ``new_terminal``, ``changed_terminal``, ``in_progress`` or
``known_unchanged`` (spec 5.1). The scan pages newest-first and stops after ``lookback_pages``
CONSECUTIVE pages in which every result was ``known_unchanged``; a RUNNING result is stored but
fetches no steps, and is picked up on a later poll once its stored version differs.
"""
import hashlib
import hmac
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from . import events as ev
from . import queries
from .client import SystemLinkClient
from .paging import iter_pages

#: Internal: dropped by ``program_include`` / ``program_exclude``; never stored, never blocks the stop rule.
EXCLUDED = "excluded"

Known = Mapping[str, Tuple[Optional[str], Optional[str]]]  # id -> (updatedAt, status) as stored


def result_status(result: Mapping[str, Any]) -> Optional[str]:
    status = result.get("status")
    if isinstance(status, Mapping):
        value = status.get("statusType")
        return str(value) if value is not None else None
    return str(status) if status is not None else None


def result_version(result: Mapping[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    updated = result.get("updatedAt")
    return (str(updated) if updated is not None else None, result_status(result))


def classify(
    result: Mapping[str, Any], known: Known, terminal_statuses: Sequence[str]
) -> str:
    status = result_status(result)
    if status is None or status not in terminal_statuses:
        return ev.IN_PROGRESS
    stored = known.get(str(result["id"]))
    if stored is None:
        return ev.NEW_TERMINAL
    if stored == result_version(result):
        return ev.KNOWN_UNCHANGED
    return ev.CHANGED_TERMINAL


def program_allowed(result: Mapping[str, Any], include: Sequence[str], exclude: Sequence[str]) -> bool:
    program = str(result.get("programName") or "")
    if include and program not in include:
        return False
    return program not in exclude


def wants_steps(result: Mapping[str, Any], classification: str, *, step_detail_for: str,
                failed_statuses: Sequence[str]) -> bool:
    if classification not in (ev.NEW_TERMINAL, ev.CHANGED_TERMINAL):
        return False
    if step_detail_for == "failed_only":
        return result_status(result) in failed_statuses
    return True


def apply_operator(result: Dict[str, Any], store: str, key: Optional[bytes]) -> Dict[str, Any]:
    """Replace ``operator`` per policy BEFORE the row is yielded to dlt."""
    if "operator" not in result:
        return result
    if store == "drop":
        result.pop("operator")
    elif store == "pseudonym":
        if not key:
            raise ValueError("operator.store 'pseudonym' needs a key")
        value = result["operator"]
        if value is not None:
            result["operator"] = hmac.new(key, str(value).encode(), hashlib.sha256).hexdigest()
    return result


Classified = Tuple[Dict[str, Any], str, str]  # (result, classification, request_id)


def scan_pages(
    client: SystemLinkClient,
    part_number: str,
    *,
    order_by: str,
    page_size: int,
    workspace_id: Optional[str],
    known: Known,
    terminal_statuses: Sequence[str],
    lookback_pages: int,
    program_include: Sequence[str] = (),
    program_exclude: Sequence[str] = (),
    updated_after: Optional[str] = None,
    stop_on_known: bool = True,
) -> Iterator[List[Classified]]:
    """Yield one classified page at a time; stop per the lookback rule when ``stop_on_known``.

    ``stop_on_known=False`` is the backfill walk: every page of the part is read.
    """
    events = client.events
    quiet_pages = 0
    for items, rid in iter_pages(
        client,
        "query-results",
        lambda token: queries.results_query(
            part_number, order_by=order_by, take=page_size, workspace_id=workspace_id,
            token=token, updated_after=updated_after,
        ),
        "results",
        setting="incremental.order_by",
    ):
        page: List[Classified] = []
        for result in items:
            if not isinstance(result, dict) or not result.get("id"):
                events.emit(ev.SCHEMA_DRIFT, missing_field="id", route="query-results")
                continue
            if not program_allowed(result, program_include, program_exclude):
                page.append((result, EXCLUDED, rid))
                continue
            page.append((result, classify(result, known, terminal_statuses), rid))
        yield page
        if stop_on_known:
            if page and all(c in (ev.KNOWN_UNCHANGED, EXCLUDED) for _, c, _ in page):
                quiet_pages += 1
                if quiet_pages >= lookback_pages:
                    return
            else:
                quiet_pages = 0


def known_from_rows(rows: Sequence[Sequence[Any]]) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """``(id, updated_at, status)`` rows read back from the destination -> the known map."""
    return {
        str(r[0]): (None if r[1] is None else str(r[1]), None if r[2] is None else str(r[2]))
        for r in rows
    }
