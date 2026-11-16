"""Structured row filters for the dlt SQL source: the extraction side.

NOT the gateway's ``row_filters``. ``central_gateway`` and ``cortex_data``
carry a ticket field of the same name: a broker's per-read authorization
answer, a free-text SQL expression applied with ``pl.sql_expr``. This one
is a pipeline's extraction config, written by an operator in YAML, and it
is structured data -- never a string this library interpolates. The two are
not unified, do not import each other, and this one must not be widened to
accept an expression string.

Grammar: six operators, ``=``, ``!=``, ``in``, ``not_in``, ``is_null``,
``not_null``. ``!=`` and ``not_in`` carry an explicit ``IS NOT NULL`` guard
so the predicate reads the same here as in a mapping's ``where``.

Values are bound: the expression is built from ``table.c.<name>``, so
SQLAlchemy emits bind parameters and nothing formats a literal into SQL
text. The identifier is resolved by lookup on the reflected table.

An unknown column fails the RUN, not the load. Reflection needs a live
connection, and a code location must not fail to load because a warehouse
is down. Do not "improve" this into a load-time check. It must still never
be silent: a filter matching no column would read as a correct extraction
of an empty table.

Second copy of a grammar. The ``where`` of a contract mapping (B2b) states
the same six operators and the same literal rules, and this module carries
its own validator rather than importing it, because that module does not
exist yet. The two are therefore a duplicated grammar until B2b reconciles
them; a test comparing them was deliberately NOT shipped, since it could
only assert against a name that is a guess. The guess, recorded so B2b need
not re-derive it: ``dag_tools.contract_mapping.mapping.validate_where``,
taking one filter dict and raising ``ValueError`` / ``TypeError`` on a
refusal. The cases to reconcile: bool, ``None`` and NaN values refused; an
empty ``values`` list refused; ``value`` on a set operator and ``values`` on
a scalar one refused; either on ``is_null`` / ``not_null`` refused.
"""
import math
from typing import Any, Callable, Dict, List, Literal, Optional

import sqlalchemy as sa
from pydantic import BaseModel, ConfigDict, model_validator

Op = Literal["=", "!=", "in", "not_in", "is_null", "not_null"]
OPS = ("=", "!=", "in", "not_in", "is_null", "not_null")
_NEEDS_VALUE = ("=", "!=")
_NEEDS_VALUES = ("in", "not_in")


def _check_literal(literal: Any, where: str) -> None:
    # A bool is an int to Python and a flag to nobody; refused rather than
    # compared as 1. None is spelled is_null / not_null, never as a value.
    if isinstance(literal, bool) or literal is None or not isinstance(
        literal, (str, int, float)
    ):
        raise ValueError(
            f"{where}: {literal!r} is not a literal a filter can compare; "
            f"use a string or a number (null is spelled is_null / not_null)"
        )
    if isinstance(literal, float) and not math.isfinite(literal):
        raise ValueError(f"{where}: {literal!r} is not a finite number")


class RowFilter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    column: str
    op: Op
    value: Optional[Any] = None
    values: Optional[List[Any]] = None

    @model_validator(mode="after")
    def _shape(self) -> "RowFilter":
        who = f"filter on {self.column!r} (op {self.op!r})"
        if not self.column.strip():
            raise ValueError("a filter names no column")
        if self.op in _NEEDS_VALUE:
            if "value" not in self.model_fields_set or self.values is not None:
                raise ValueError(f"{who} takes `value` and no `values`")
            _check_literal(self.value, f"{who} value")
        elif self.op in _NEEDS_VALUES:
            if self.values is None or "value" in self.model_fields_set:
                raise ValueError(f"{who} takes `values` and no `value`")
            if not self.values:
                raise ValueError(f"{who} has an empty `values` list")
            for item in self.values:
                _check_literal(item, f"{who} values")
        else:
            if "value" in self.model_fields_set or self.values is not None:
                raise ValueError(f"{who} takes neither `value` nor `values`")
        return self


def parse_row_filters(
    pipeline: str, raw: Dict[str, Any], sources: List[str]
) -> Dict[str, List[RowFilter]]:
    """Validate the YAML block; fails the load on a malformed one."""
    unknown = sorted(set(raw) - set(sources))
    if unknown:
        raise ValueError(
            f"pipeline '{pipeline}': row_filters names tables that are not in "
            f"sources: {unknown}. Either add them to sources or drop the "
            f"filter -- a silently-unused filter reads as applied when it is "
            f"not. Sources: {sorted(sources)}"
        )
    out: Dict[str, List[RowFilter]] = {}
    for table, entries in raw.items():
        if not isinstance(entries, list) or not entries:
            raise ValueError(
                f"pipeline '{pipeline}': row_filters for '{table}' must be a "
                f"non-empty list of filters"
            )
        out[table] = [RowFilter.model_validate(e) for e in entries]
    return out


def predicate(column: Any, flt: RowFilter) -> Any:
    op = flt.op
    if op == "=":
        return column == flt.value
    if op == "!=":
        return sa.and_(column.isnot(None), column != flt.value)
    if op == "in":
        return column.in_(flt.values)
    if op == "not_in":
        return sa.and_(column.isnot(None), column.notin_(flt.values))
    if op == "is_null":
        return column.is_(None)
    return column.isnot(None)


def build_query_callback(
    pipeline: str, filters: Dict[str, List[RowFilter]]
) -> Optional[Callable]:
    """Return dlt's ``query_adapter_callback``, or ``None`` with no filters.

    Takes FOUR parameters on purpose. dlt's sql_database helper calls the
    adapter with ``(query, table, incremental, engine)`` first and falls back
    to ``(query, table)`` only by catching ``TypeError`` and asking whether
    the call itself was wrong. A two-parameter callback sends every table
    through a raised-and-caught ``TypeError``, and puts this code inside a
    ``try`` that inspects ``TypeError`` -- so a genuine ``TypeError`` from the
    filter logic would be one heuristic away from being swallowed and retried
    with a different arity. Four parameters never enter that path.
    """
    if not filters:
        return None

    def query_adapter(query: Any, table: Any, incremental: Any, engine: Any) -> Any:
        flts = filters.get(table.name)
        if not flts:
            return query
        for flt in flts:
            if flt.column not in table.c:
                raise ValueError(
                    f"pipeline '{pipeline}': row_filters for table "
                    f"'{table.name}' names column '{flt.column}', which the "
                    f"table does not have. Columns: "
                    f"{[c.name for c in table.c]}"
                )
            query = query.where(predicate(table.c[flt.column], flt))
        return query

    return query_adapter


def callback_for(
    pipeline: str,
    raw: Optional[Dict[str, Any]],
    sources: List[str],
    builds_sql_query: bool,
    source_description: str,
    declared_sources: Optional[List[str]] = None,
):
    """Parse a pipeline's ``row_filters`` and return its query callback.

    The one place both the component and the dev loader resolve filters, so
    the two paths cannot diverge. ``None`` means no filters. A pipeline that
    sets filters on a source that never builds a SQL query (a non-legacy
    source kind, or the filesystem driver) is refused: the filters would be
    ignored and the YAML would still read as filtered.

    ``declared_sources`` validates the table names when ``sources`` has been
    narrowed (``--tables``); a filter for a table outside the narrowing is
    then simply never applied.
    """
    filters = parse_row_filters(pipeline, raw or {}, declared_sources or sources)
    if not filters:
        return None
    if not builds_sql_query:
        raise ValueError(
            f"pipeline '{pipeline}': row_filters is set, but this source "
            f"({source_description}) does not build a SQL query to filter; "
            f"an ignored field is a trap. Remove it."
        )
    return build_query_callback(pipeline, filters)
