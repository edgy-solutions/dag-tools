"""Where the history comes from, named by the operator and by nobody else.

`reads.py` decides what a read means; this module turns a warehouse's own
query-history relation into the `ReadEvent` rows it decides over. Two things
about it are deliberate and load-bearing.

**Nothing here names a relation or a column.** Not even a default. Every
warehouse spells its history differently, the operator's account may expose
it through a view of their own, and the standing rule is that no database,
schema, table or column name lives in this code -- so `HistorySource` has
required fields and no fallbacks. A default would be a name in the repo, and
the first deployment whose history is shaped differently would silently audit
the wrong relation rather than fail.

**The statement is built, not executed.** `build_history_statement` returns
SQL and bound parameters; running it is four lines at the call site. This is
the same split as `otel_api_sync._staging_url`: resolution is the part this
module decides and therefore the part worth pinning, and a test that builds an
engine dies on a driver CI does not install. So the grammar and the binding
are unit-tested, and nothing here opens a connection.

**An identifier cannot be bound as a parameter**, which is why
`validate_relation` exists and is strict: up to three dot-separated parts,
each an ordinary unquoted identifier. Anything else -- a quote, a space, a
semicolon, a comment marker, a fourth part -- is refused rather than escaped.
Values are always bound; only the relation is interpolated, and only after it
has matched that grammar.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict

from .reads import ReadEvent


_PART = r"[A-Za-z_][A-Za-z0-9_$]*"
RELATION_GRAMMAR = re.compile(rf"^{_PART}(\.{_PART}){{0,2}}$")
"""One to three unquoted identifier parts. See the module docstring for why
this is a whitelist and not an escaping routine."""

QUERY_ID = "query_id"
STARTED_AT = "started_at"
PRINCIPAL = "principal"
QUERY_TAG = "query_tag"
RELATIONS = "relations"

OUTPUT_COLUMNS = (QUERY_ID, STARTED_AT, PRINCIPAL, QUERY_TAG, RELATIONS)
"""The aliases the statement projects to. These are this module's own names
for the five things it needs, so `rows_to_events` does not need the operator's
column names a second time -- and so a change in their history is one config
edit rather than a code change."""


class UnsafeRelation(ValueError):
    """Raised instead of interpolating something that is not an identifier."""


class HistoryColumns(BaseModel):
    """Which column in the operator's history holds each of the five things.

    All required. See the module docstring: a default here would be a name in
    the repo, and a wrong default would audit the wrong column quietly.
    """

    model_config = ConfigDict(extra="forbid")

    query_id: str
    started_at: str
    principal: str
    query_tag: str
    relations: str


class HistorySource(BaseModel):
    """The operator's history relation, and how to read it."""

    model_config = ConfigDict(extra="forbid")

    relation: str
    columns: HistoryColumns
    principal_filter: Optional[str] = None
    """Restrict to one identity -- the service identity being audited. Left
    unset, every identity is read, and `reads.py` sorts pipelines from the
    rest by principal."""


def validate_relation(relation: str) -> str:
    """Return the relation unchanged, or refuse it."""
    candidate = (relation or "").strip()
    if not RELATION_GRAMMAR.match(candidate):
        raise UnsafeRelation(
            "a history relation must be one to three unquoted identifier "
            "parts, because an identifier cannot be bound as a parameter and "
            f"this one is interpolated into the statement: {relation!r}"
        )
    return candidate


def build_history_statement(
    source: HistorySource,
    since: datetime,
    until: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Build the read. Named (`:name`) parameter style; every value is bound.

    The five columns are projected to `OUTPUT_COLUMNS` so the rest of the
    module works in this package's vocabulary rather than the operator's.
    """
    relation = validate_relation(source.relation)
    columns = source.columns

    for field, value in columns.model_dump().items():
        if not RELATION_GRAMMAR.match((value or "").strip()):
            raise UnsafeRelation(
                f"the column named for {field} is not a plain identifier, and "
                f"it is interpolated into the statement: {value!r}"
            )

    projection = ", ".join(
        (
            f"{columns.query_id} AS {QUERY_ID}",
            f"{columns.started_at} AS {STARTED_AT}",
            f"{columns.principal} AS {PRINCIPAL}",
            f"{columns.query_tag} AS {QUERY_TAG}",
            f"{columns.relations} AS {RELATIONS}",
        )
    )

    where = [f"{columns.started_at} >= :since"]
    params: Dict[str, Any] = {"since": since}
    if until is not None:
        where.append(f"{columns.started_at} <= :until")
        params["until"] = until
    if source.principal_filter is not None:
        where.append(f"{columns.principal} = :principal_filter")
        params["principal_filter"] = source.principal_filter

    statement = (
        f"SELECT {projection} FROM {relation} "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY {columns.started_at}"
    )
    return statement, params


def parse_relations(cell: Any) -> List[str]:
    """Read the touched-objects cell, in any of the shapes it arrives in.

    History exposes this as a JSON array in some warehouses, a delimited
    string in others, and a view of the operator's own making in the rest. All
    three are accepted; anything else yields no objects for that row rather
    than failing the pass.
    """
    if cell is None:
        return []
    if isinstance(cell, (list, tuple, set)):
        return [str(item).strip() for item in cell if str(item).strip()]
    if isinstance(cell, dict):
        return []
    text = str(cell).strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except ValueError:
            return []
        if isinstance(parsed, list):
            return [
                str(item).strip()
                for item in parsed
                if isinstance(item, (str, int, float)) and str(item).strip()
            ]
        return []
    # Comma, semicolon or newline. A newline is the one a hand-rolled view
    # most often produces (one object per line), and no separator can be part
    # of an identifier the grammar accepts, so splitting on all three cannot
    # break a name apart.
    for separator in (";", "\n", "\r", "\t"):
        text = text.replace(separator, ",")
    return [part.strip() for part in text.split(",") if part.strip()]


def rows_to_events(
    rows: Sequence[Dict[str, Any]],
) -> Tuple[List[ReadEvent], List[str]]:
    """Map projected rows to events, soft-failing per row.

    Returns `(events, failures)`. One malformed row must never abort the pass
    -- same discipline as the inventory extractor -- and the failures are
    returned rather than logged away so the report can say how many rows it
    could not read. A pass that silently dropped rows would under-report the
    gap, which is the one direction this tool must not be wrong in.
    """
    events: List[ReadEvent] = []
    failures: List[str] = []

    for index, row in enumerate(rows):
        try:
            events.append(
                ReadEvent(
                    query_id=str(row[QUERY_ID]),
                    started_at=row[STARTED_AT],
                    principal=(
                        None
                        if row.get(PRINCIPAL) is None
                        else str(row[PRINCIPAL])
                    ),
                    query_tag=(
                        None
                        if row.get(QUERY_TAG) is None
                        else str(row[QUERY_TAG])
                    ),
                    relations=parse_relations(row.get(RELATIONS)),
                )
            )
        except Exception as exc:  # noqa: BLE001 -- one bad row, not a bad pass
            failures.append(f"row {index}: {type(exc).__name__}: {exc}")

    return events, failures
