"""Dispatch ledger — the cross-run half of exactly-once.

Restate's ``ctx.run`` deduplicates *retries within one invocation*. It
does not deduplicate a *second dispatch* of the same execution group,
which is exactly what happens when telemetry for a group is still
arriving and a later extraction run sees it again. Upsert-shaped calls
tolerate that; append-shaped calls (recording a result, appending an
event) do not — they silently duplicate.

The group-keyed VirtualObject already refuses a plan hash it has
completed, which is the authoritative guard. This ledger is the
Dagster-side complement: it stops a duplicate plan from being *sent* at
all, so the Dagster UI reports what actually happened and the ingress
does not carry redundant traffic.

Two backends, chosen by where the pipeline already keeps state:

``sql``
    A table in the staging destination. Preferred whenever the pipeline
    stages through a warehouse, because it is queryable and survives
    independently of the Dagster instance.

``dagster``
    The previous materialization's metadata on the dispatch asset.
    Needs no extra infrastructure, which is what makes direct
    (unstaged) mode viable, but it is bounded and tied to the instance's
    event log retention.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# Cap on how many (group, hash) pairs the Dagster-metadata backend
# carries forward, so asset metadata cannot grow without bound.
DAGSTER_LEDGER_LIMIT = 5000

LEDGER_METADATA_KEY = "dispatch_ledger"


def _entry(group_key: str, plan_hash: str) -> str:
    """Collision-free ledger key.

    JSON-encoding the pair means a group key containing the separator
    cannot forge another group's entry.
    """
    return json.dumps([str(group_key), str(plan_hash)])


class DispatchLedger:
    """Records which (group, plan hash) pairs have already been dispatched."""

    def __init__(self, backend: str = "dagster"):
        self.backend = backend
        self._seen: Set[str] = set()
        self._added: List[Tuple[str, str]] = []

    # --- reading -----------------------------------------------------------

    def load(self, seen: Iterable[str]) -> None:
        self._seen = set(seen)

    def contains(self, group_key: str, plan_hash: str) -> bool:
        return _entry(group_key, plan_hash) in self._seen

    def record(self, group_key: str, plan_hash: str) -> None:
        self._seen.add(_entry(group_key, plan_hash))
        self._added.append((group_key, plan_hash))

    @property
    def added(self) -> List[Tuple[str, str]]:
        return list(self._added)

    def serialize(self) -> str:
        """Serialize for the Dagster-metadata backend, newest kept."""
        entries = list(self._seen)[-DAGSTER_LEDGER_LIMIT:]
        return json.dumps(entries)


def load_from_dagster(context: Any, asset_key: Any) -> DispatchLedger:
    """Rehydrate the ledger from the dispatch asset's last materialization."""
    ledger = DispatchLedger("dagster")
    try:
        event = context.instance.get_latest_materialization_event(asset_key)
        materialization = (
            event.dagster_event.event_specific_data.materialization if event else None
        )
        raw = (materialization.metadata or {}).get(LEDGER_METADATA_KEY) if materialization else None
        if raw is not None:
            text = getattr(raw, "value", raw)
            ledger.load(json.loads(text) if isinstance(text, str) else [])
    except Exception as exc:  # never fail a run over ledger rehydration
        logger.warning("Could not load dispatch ledger from Dagster metadata: %s", exc)
    return ledger


def _qualified(schema: Optional[str], table: str) -> str:
    return f'"{schema}"."{table}"' if schema else f'"{table}"'


def load_from_sql(engine: Any, schema: Optional[str], table: str) -> DispatchLedger:
    """Rehydrate (and create if needed) the ledger table in the staging store."""
    import sqlalchemy as sa

    ledger = DispatchLedger("sql")
    qualified = _qualified(schema, table)
    with engine.begin() as conn:
        if schema:
            conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        conn.execute(
            sa.text(
                f"""
                CREATE TABLE IF NOT EXISTS {qualified} (
                    group_key TEXT NOT NULL,
                    plan_hash TEXT NOT NULL,
                    dispatched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    call_count INTEGER,
                    PRIMARY KEY (group_key, plan_hash)
                )
                """
            )
        )
        rows = conn.execute(sa.text(f"SELECT group_key, plan_hash FROM {qualified}"))
        ledger.load(_entry(str(r[0]), str(r[1])) for r in rows)
    return ledger


def flush_to_sql(
    engine: Any,
    schema: Optional[str],
    table: str,
    entries: List[Tuple[str, str]],
    call_counts: Optional[Dict[str, int]] = None,
) -> None:
    """Persist newly dispatched pairs.

    Written *after* a successful send: a crash between send and write
    re-dispatches, which the VirtualObject's completed-hash state
    absorbs. The reverse ordering would silently drop a group.
    """
    if not entries:
        return
    import sqlalchemy as sa

    qualified = _qualified(schema, table)
    counts = call_counts or {}
    with engine.begin() as conn:
        for group_key, plan_hash in entries:
            conn.execute(
                sa.text(
                    f"INSERT INTO {qualified} (group_key, plan_hash, call_count) "
                    "VALUES (:g, :h, :c) ON CONFLICT (group_key, plan_hash) DO NOTHING"
                ),
                {"g": group_key, "h": plan_hash, "c": counts.get(plan_hash)},
            )
