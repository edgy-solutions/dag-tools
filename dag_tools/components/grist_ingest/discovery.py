"""Discovery + human-friendly naming for Grist ingestion.

The sensor discovers Grist documents and their tables, then turns each
``(document, table)`` pair into a **friendly, stable identifier** used as
both the Dagster dynamic-partition key and the destination Postgres table
name. Friendly names replace the opaque ``<doc_id>__<table_id>`` scheme:
the real Grist ids are carried in run config instead of the key, so the
UI and the database show readable names like
``sales_ops__quarterly_budget__line_items``.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, NamedTuple, Optional

# Postgres identifiers are truncated to 63 bytes; keep a margin so a
# schema-qualified reference never surprises the operator.
_MAX_IDENTIFIER_LEN = 63


def normalize_identifier(value: str) -> str:
    """Lower-case, collapse anything non-alphanumeric to single ``_``.

    Produces a value safe as both a Dagster partition key and an
    unquoted Postgres table name: lowercase, ``[a-z0-9_]`` only, no
    leading digit, trimmed to the identifier length limit.
    """
    text = (value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        text = "unnamed"
    if text[0].isdigit():
        text = f"t_{text}"
    return text[:_MAX_IDENTIFIER_LEN].rstrip("_")


def friendly_table_name(
    workspace: str,
    doc_name: str,
    table_id: str,
    *,
    include_workspace: bool = True,
) -> str:
    """Build the friendly ``partition key`` / Postgres table name.

    Combines workspace, document name, and table id (each normalized)
    with ``__`` separators. Workspace inclusion is optional — turn it
    off when workspace names add noise and doc names are already unique.
    Table id is always last so identically-named tables in different
    docs stay distinct.
    """
    parts = []
    if include_workspace:
        parts.append(normalize_identifier(workspace))
    parts.append(normalize_identifier(doc_name))
    parts.append(normalize_identifier(table_id))
    combined = "__".join(p for p in parts if p)
    # Re-clamp: the join can exceed the limit even if each part fit.
    return combined[:_MAX_IDENTIFIER_LEN].rstrip("_")


class DiscoveredTable(NamedTuple):
    """One Grist table resolved to its friendly name + the ids needed to
    fetch it. ``run_key`` is unique per (doc version, table) so a given
    document revision only triggers once."""

    friendly_name: str
    doc_id: str
    table_id: str
    run_key: str
    updated_at: Any


def discover_tables(
    client: Any,
    *,
    since: Optional[Any] = None,
    docs: Optional[List[Dict[str, Any]]] = None,
    include_workspace: bool = True,
    log: Any = None,
) -> List[DiscoveredTable]:
    """Enumerate every ``(doc, table)`` newer than ``since``.

    Returns a flat, de-duplicated list of :class:`DiscoveredTable`.
    Pass ``docs`` to reuse an already-fetched document list (the sensor
    fetches once so it can advance its cursor past table-less docs);
    otherwise the docs are fetched via ``client.list_docs(since=since)``.
    Friendly-name collisions (two distinct tables normalizing to the
    same name) are resolved by appending a short suffix from the table
    id so no two entries in one sweep clobber the same Postgres table.
    """
    if docs is None:
        docs = client.list_docs(since=since)
    seen: Dict[str, int] = {}
    out: List[DiscoveredTable] = []

    for doc in sorted(docs, key=lambda d: d.get("updatedAt", "")):
        doc_id = doc.get("id")
        if not doc_id:
            continue
        updated_at = doc.get("updatedAt", "")
        workspace = doc.get("workspace", "")
        doc_name = doc.get("name", doc_id)
        for table in client.list_tables(doc_id):
            table_id = table.get("id")
            if not table_id:
                continue
            name = friendly_table_name(
                workspace, doc_name, table_id, include_workspace=include_workspace
            )
            if name in seen:
                # Collision within this sweep — disambiguate deterministically.
                seen[name] += 1
                suffix = normalize_identifier(table_id)[:6] or str(seen[name])
                name = f"{name[: _MAX_IDENTIFIER_LEN - len(suffix) - 1]}_{suffix}"
                if log:
                    log.warning("grist: friendly-name collision, using %r", name)
            else:
                seen[name] = 0
            out.append(
                DiscoveredTable(
                    friendly_name=name,
                    doc_id=doc_id,
                    table_id=table_id,
                    run_key=f"{updated_at}-{doc_id}-{table_id}",
                    updated_at=updated_at,
                )
            )
    return out
