"""Observing a source without moving it.

Pure: a spec in, the statements to run out; rows back in, an `Observation`
out. Nothing here opens a connection -- the component runs the statements
through the tagged warehouse resource, so the observation itself appears in
the read history attributed to this asset, like every other read.

**Nothing is moved.** The default statements are an aggregate and an empty
projection: the row count comes back as one number, and the column names come
back from a result set with no rows in it. That is what makes this a source
*observation* rather than a copy -- and it is also why the schema is read from
the projection rather than from an information schema, which is named
differently in every warehouse this library does not want to know about.

Both statements are overridable, because a dialect this library does not own
may spell either of them differently. Neither default names a database,
schema, table or column: the relation is substituted from the operator's own
YAML.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

#: Count the rows. One number, no data.
DEFAULT_ROW_COUNT_STATEMENT = "SELECT count(*) AS row_count FROM {table}"

#: Name the columns. A projection that matches nothing, so the result carries
#: the schema and no rows.
DEFAULT_COLUMNS_STATEMENT = "SELECT * FROM {table} WHERE 1 = 0"

#: Substituted into whichever statement runs. The only placeholder.
TABLE_PLACEHOLDER = "{table}"

#: Refused inside the substituted relation. Not a sanitizer -- the relation is
#: the operator's own YAML, and an operator can always write their own
#: statement. It is here so a copy-paste accident fails at load with its
#: reason named, instead of running as two statements.
REFUSED_IN_TABLE = (";", "--", "/*", "*/", "\n", "\r")


class ObservationError(ValueError):
    """The source could not be observed, or the result makes no sense."""


@dataclass(frozen=True)
class ObservationPlan:
    """The statements one observation runs, in the order it runs them."""

    row_count_statement: str
    columns_statement: str
    as_of_statement: Optional[str] = None

    @property
    def statements(self) -> Tuple[str, ...]:
        return tuple(
            statement
            for statement in (
                self.columns_statement,
                self.row_count_statement,
                self.as_of_statement,
            )
            if statement
        )


@dataclass(frozen=True)
class Observation:
    """What one observation found. No rows of the source are in here."""

    row_count: Optional[int]
    column_names: Tuple[str, ...]
    schema_hash: str
    as_of: str
    observed_at: str

    @property
    def data_version(self) -> str:
        """Changes when the shape, the size or the as-of changes.

        Not a hash of the data: nothing read the data. A source whose row
        count and schema are unchanged still gets a new version when its
        as-of moves, which is what lets an operator-declared as-of carry a
        freshness signal the count cannot.
        """
        return _digest(
            [
                self.schema_hash,
                "-" if self.row_count is None else str(self.row_count),
                self.as_of,
            ]
        )

    def as_metadata(self) -> Dict[str, Any]:
        return {
            "row_count": self.row_count,
            "column_count": len(self.column_names),
            "schema_hash": self.schema_hash,
            "as_of": self.as_of,
            "observed_at": self.observed_at,
            "data_version": self.data_version,
        }


def plan_observation(
    *,
    table: str,
    row_count_statement: Optional[str] = None,
    columns_statement: Optional[str] = None,
    as_of_statement: Optional[str] = None,
) -> ObservationPlan:
    """Render the statements for one relation.

    An override is used verbatim apart from the one placeholder, because the
    moment this library starts rewriting an operator's SQL it has taken on a
    dialect it cannot test against.
    """
    relation = (table or "").strip()
    if not relation:
        raise ObservationError("there is no relation to observe")
    for refused in REFUSED_IN_TABLE:
        if refused in relation:
            raise ObservationError(
                f"the relation {relation!r} contains {refused!r}. It is "
                "substituted into a statement, so what would run is not the "
                "read this component describes; write your own statements "
                "instead if your relation really needs it"
            )
    return ObservationPlan(
        row_count_statement=_render(
            row_count_statement or DEFAULT_ROW_COUNT_STATEMENT, relation
        ),
        columns_statement=_render(
            columns_statement or DEFAULT_COLUMNS_STATEMENT, relation
        ),
        as_of_statement=(
            _render(as_of_statement, relation) if as_of_statement else None
        ),
    )


def _render(statement: str, relation: str) -> str:
    rendered = statement.replace(TABLE_PLACEHOLDER, relation)
    if not rendered.strip():
        raise ObservationError("an observation statement cannot be empty")
    return rendered


def read_column_names(names: Sequence[Any]) -> Tuple[str, ...]:
    """The projected column names, in the order the source projects them."""
    columns = tuple(str(name) for name in names if str(name).strip())
    if not columns:
        raise ObservationError(
            "the source projected no columns; either the relation does not "
            "exist or the columns statement does not project it"
        )
    return columns


def read_row_count(rows: Sequence[Mapping[str, Any]]) -> int:
    """The one number the count statement returned.

    Read positionally rather than by name: the default statement aliases it,
    but an operator's own statement aliases whatever their dialect prefers,
    and a component that insisted on the alias would be naming a column.
    """
    # One row, because a count is one number. Several rows means the statement
    # grouped by something, and summing them here would invent an answer the
    # operator did not ask for.
    if len(rows) != 1:
        raise ObservationError(
            f"the row count statement returned {len(rows)} rows; it must return "
            "exactly one"
        )
    values = list(rows[0].values())
    if len(values) != 1:
        raise ObservationError(
            f"the row count statement returned {len(values)} values; it must "
            "project exactly one, so that which of them is the count is never "
            "guessed"
        )
    value = values[0]
    if isinstance(value, bool) or value is None:
        raise ObservationError(f"the row count came back as {value!r}")
    try:
        # Integer text parses exactly; float() past 2**53 does not, and a row
        # count is exactly the kind of number that goes past it.
        return int(str(value).strip())
    except (TypeError, ValueError):
        raise ObservationError(
            f"the row count came back as {value!r}, which is not a whole number"
        ) from None


def read_as_of(rows: Sequence[Mapping[str, Any]]) -> str:
    """The operator's own as-of value, as text.

    Kept as the source wrote it rather than parsed into a datetime: an as-of
    is whatever the operator's statement projected -- a timestamp, a watermark,
    a load id -- and reformatting it would make two equal watermarks look
    different.
    """
    if len(rows) != 1:
        raise ObservationError(
            f"the as-of statement returned {len(rows)} rows; it must return "
            "exactly one"
        )
    values = list(rows[0].values())
    if len(values) != 1:
        raise ObservationError(
            f"the as-of statement returned {len(values)} values; it must project "
            "exactly one"
        )
    value = values[0]
    if value is None:
        raise ObservationError(
            "the as-of statement returned nothing; an absent as-of is not a "
            "fresh one, so it is refused rather than recorded as empty"
        )
    return value.isoformat() if isinstance(value, datetime) else str(value)


def schema_hash(column_names: Sequence[str]) -> str:
    """A digest of the projected schema, order included.

    Order is part of it because the default statement projects the source's
    own order: a reordered table is a changed table, and a labelling that was
    written against the old order deserves to be looked at again.
    """
    return _digest(list(column_names))


def observe(
    *,
    column_names: Sequence[str],
    row_count: Optional[int],
    as_of: Optional[str] = None,
    observed_at: Optional[str] = None,
) -> Observation:
    """Assemble what was found. The clock is injected so tests can pin it."""
    names = read_column_names(column_names)
    when = observed_at or datetime.now(timezone.utc).isoformat()
    return Observation(
        row_count=row_count,
        column_names=names,
        schema_hash=schema_hash(names),
        as_of=as_of or when,
        observed_at=when,
    )


def _digest(parts: List[str]) -> str:
    joined = "\n".join(parts).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()[:12]
