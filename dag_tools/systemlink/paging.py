"""Continuation-token paging and step batching. Pure.

Step batching (spec 5.2): a :class:`StepBatcher` cannot be built able to emit more than
``step_batch_max``; a 504 / timeout splits the batch recursively in halves and the oversized
batch is never sent again; a requested id that comes back with no steps is logged ``not_visible``.
"""
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from . import events as ev
from . import queries
from .client import GatewayTimeout, RequestFailed, SystemLinkClient


def iter_pages(
    client: SystemLinkClient,
    route: str,
    make_body: Callable[[Optional[str]], Dict[str, Any]],
    items_key: str,
    *,
    setting: str,
    splittable: bool = False,
) -> Iterator[Tuple[List[Dict[str, Any]], str]]:
    """Yield ``(items, request_id)`` per page until the server stops returning a token."""
    token: Optional[str] = None
    seen = set()
    while True:
        data, request_id = client.post(route, make_body(token), setting=setting, splittable=splittable)
        items = data.get(items_key) or []
        yield list(items), request_id
        token = data.get("continuationToken")
        if not token:
            return
        if token in seen:
            raise RequestFailed(f"{route}: the server repeated a continuation token")
        seen.add(token)


class StepBatcher:
    """Chops result ids into batches that can never exceed ``maximum``."""

    def __init__(self, default: int, maximum: int) -> None:
        if default < 1 or maximum < 1:
            raise ValueError("batch sizes must be positive")
        if default > maximum:
            raise ValueError(f"step batch default {default} exceeds the maximum {maximum}")
        self.default, self.maximum = default, maximum

    def batches(self, ids: Sequence[str]) -> Iterator[List[str]]:
        ids = list(ids)
        for i in range(0, len(ids), self.default):
            yield ids[i : i + self.default]

    @staticmethod
    def halves(batch: Sequence[str]) -> Tuple[List[str], List[str]]:
        mid = (len(batch) + 1) // 2
        return list(batch[:mid]), list(batch[mid:])


def fetch_steps(
    client: SystemLinkClient,
    result_ids: Sequence[str],
    *,
    form: str,
    take: int,
    batcher: StepBatcher,
    workspace_id: Optional[str] = None,
    max_concurrency: int = 1,
) -> List[Tuple[Dict[str, Any], str]]:
    """Fetch every step of ``result_ids``. Returns ``(step, request_id)`` pairs.

    A batch is only ever delivered whole: a failure part-way through its pages discards them and
    splits, so nothing is yielded twice and the oversized batch is never re-sent.
    """
    events = client.events

    def one(batch: List[str]) -> List[Tuple[Dict[str, Any], str]]:
        if len(batch) > batcher.maximum:  # the invariant, checked at the last moment
            raise AssertionError("step batch exceeds step_batch_max")
        events.note_step_batch(len(batch))
        try:
            rows: List[Tuple[Dict[str, Any], str]] = []
            for items, rid in iter_pages(
                client,
                "query-steps",
                lambda token: queries.steps_query(
                    batch, form=form, take=take, workspace_id=workspace_id, token=token
                ),
                "steps",
                setting="limits.step_filter",
                splittable=True,
            ):
                rows.extend((s, rid) for s in items)
            return rows
        except GatewayTimeout:
            if len(batch) == 1:
                raise
            events.emit(ev.GATEWAY_TIMEOUT_SPLIT, size=len(batch))
            left, right = StepBatcher.halves(batch)
            return one(left) + one(right)

    batches = list(batcher.batches(result_ids))
    if max_concurrency > 1 and len(batches) > 1:
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            parts = list(pool.map(one, batches))
    else:
        parts = [one(b) for b in batches]
    rows = [r for part in parts for r in part]

    got = {str(s.get("resultId")) for s, _ in rows}
    for rid in result_ids:
        if str(rid) not in got:
            events.emit(ev.NOT_VISIBLE, result_id=str(rid))
    return rows
