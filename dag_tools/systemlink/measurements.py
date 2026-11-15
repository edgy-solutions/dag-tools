"""Measurement text parsing: the rule table and the Python route (plan 6.5).

Pure: no Dagster, no dbt, no dlt, no IO. The same rule table is the source
the dbt package's SQL is generated from, so the two routes cannot drift
apart without a test noticing.

Nothing here names a field of any source system, and nothing here names a
comparison code: the comparator table is the deployment's, exactly as the
normalizers are, and this module ships no rows for it. The output names are
the curated contract's own, which is dag-tools' published interface.
"""

import math
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional, Tuple

# A number as text: optional sign, digits with optional fraction, optional
# exponent. Deliberately does NOT admit "inf" or "nan" -- a non-finite value
# is absent, never a number.
_NUM = r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?"

# A unit suffix: letters and the symbols units are written with. It must end
# in a letter or symbol, never a digit, so an exponent can never be mistaken
# for a unit.
_UNIT = r"[A-Za-zµμΩ°%/^\-\d]*[A-Za-zµμΩ°%]"

# Which of the contract's two limit columns a comparator bounds. These two
# tokens are the library's own, named after the columns the curated contract
# declares; they are not vocabulary from any source system.
BOUND_LOW = "low"
BOUND_HIGH = "high"

_BOUNDS = frozenset({BOUND_LOW, BOUND_HIGH})

# An operator as text. Deliberately permissive: which operators mean anything
# is the deployment's table to say, and one this library matched but the table
# does not cover is reported rather than guessed at.
_OP = r"[<>=!≤≥]+"


@dataclass(frozen=True)
class Comparator:
    """One row of the deployment's comparator table.

    `code` is what lands in the contract's `comparison_type`, and it is the
    deployment's word, never this library's: a code compiled in here would
    make a vocabulary decision a library release. **This module ships no
    comparator rows at all** -- the sample table lives in the synthetic
    example, and a caller that passes none covers nothing, which is the rule
    made true at runtime rather than asserted in a comment.
    """

    text: str
    code: str
    bound: str

    def __post_init__(self) -> None:
        if self.bound not in _BOUNDS:
            raise ValueError(
                "comparator {0!r} bounds {1!r}; expected one of {2}".format(
                    self.text, self.bound, sorted(_BOUNDS)
                )
            )
        if not self.text.strip() or not self.code.strip():
            raise ValueError("a comparator row needs both a text and a code")


#: A deployment's table, in the order it was declared. Empty here on purpose.
ComparatorTable = Tuple[Comparator, ...]

NO_COMPARATORS: ComparatorTable = ()


def find_comparator(
    text: str, comparators: ComparatorTable = NO_COMPARATORS
) -> Optional[Comparator]:
    """The table's row for an operator text, or None when it covers none.

    The match is on the **whole** operator run the grammar captured, never a
    prefix of it: a table carrying only `<` does not cover `<=`, and reading
    it as `<` would publish a bound the source did not state. So an operator
    the table does not carry is reported as uncovered -- kept as text,
    counted, and never read as either a limit or a value.
    """
    stripped = (text or "").strip()
    for row in comparators:
        if row.text.strip() == stripped:
            return row
    return None


@dataclass(frozen=True)
class Rule:
    """One row of the grammar table.

    `kind` is what the evaluator (and the SQL generator) switches on;
    `pattern` is anchored and applied to the stripped text.

    `reads_value` says whether this rule may produce a *measurement*.
    Only a plain number may: a comparator or a tolerance states a bound,
    and reading `< 50` as a measurement of 50 would put a reading in the
    column that was never taken. The plan table marks that row "as a limit
    string" for exactly this reason.
    """

    id: str
    kind: str
    pattern: str
    reads_value: bool

    @property
    def regex(self):
        return re.compile(self.pattern)


#: Ordered: the first rule that matches wins. Tolerance precedes the bare
#: number so a tolerance text is never read as its centre alone.
RULES: Tuple[Rule, ...] = (
    Rule(
        id="tolerance",
        kind="tolerance",
        reads_value=False,
        pattern=(
            r"^(?P<center>" + _NUM + r")\s*(?:±|\+/-)\s*"
            r"(?P<tol>" + _NUM + r")\s*(?P<unit>" + _UNIT + r")?$"
        ),
    ),
    Rule(
        id="comparator",
        kind="comparator",
        reads_value=False,
        pattern=(
            r"^(?P<op>" + _OP + r")\s*(?P<value>" + _NUM + r")"
            r"\s*(?P<unit>" + _UNIT + r")?$"
        ),
    ),
    Rule(
        id="number",
        kind="number",
        reads_value=True,
        pattern=r"^(?P<value>" + _NUM + r")\s*(?P<unit>" + _UNIT + r")?$",
    ),
)


@dataclass(frozen=True)
class ValueParse:
    """A measurement value read off its text."""

    num: Optional[float]
    units: Optional[str]
    rule_id: Optional[str]


@dataclass(frozen=True)
class LimitsParse:
    """A low/high pair read off a limits text."""

    low: Optional[float]
    high: Optional[float]
    comparison_type: Optional[str]
    units: Optional[str]
    rule_id: Optional[str]
    #: The operator text the deployment's table does not cover, if any. The
    #: text itself is still carried verbatim in the contract's `limits_text`;
    #: this is what lets the row be **counted** rather than merely missing.
    uncovered_comparator: Optional[str] = None

    @property
    def is_numeric(self) -> bool:
        """Whether this limits text yielded a number at all.

        `parse_failed` is defined against this, not against the text: a row
        whose limits are non-numeric is a row where a non-numeric
        measurement is not a failure.
        """
        return self.low is not None or self.high is not None


@dataclass(frozen=True)
class RowParse:
    """Everything the grammar contributes to one curated measurement row."""

    measurement_num: Optional[float]
    units: Optional[str]
    low_limit_num: Optional[float]
    high_limit_num: Optional[float]
    comparison_type: Optional[str]
    parse_failed: bool
    #: Not a contract column: the curated document declares none for it. It
    #: is what the counter counts, and the same rows are derivable from the
    #: rule and the same table -- a published-column predicate cannot tell
    #: it from a limits text that was never a bound at all.
    uncovered_comparator: Optional[str] = None


def _finite(value: float) -> Optional[float]:
    """A non-finite number is absent, never a number.

    JSON cannot carry one and a warehouse column should not either; the same
    stance as the otel and contract-mapping routes.
    """
    return value if math.isfinite(value) else None


def _to_float(text: str) -> Optional[float]:
    try:
        return _finite(float(text))
    except (TypeError, ValueError):
        return None


def _decimal(text: str) -> Optional[Decimal]:
    try:
        value = Decimal(text)
    except (InvalidOperation, TypeError, ValueError):
        return None
    return value if value.is_finite() else None


def _unit_or_none(matched: Optional[str]) -> Optional[str]:
    if matched is None:
        return None
    stripped = matched.strip()
    return stripped or None


def _match(text: Optional[str]):
    if text is None:
        return None, None
    stripped = text.strip()
    if not stripped:
        return None, None
    for rule in RULES:
        found = rule.regex.match(stripped)
        if found is not None:
            return rule, found
    return None, None


def parse_value(text: Optional[str]) -> ValueParse:
    """Read a measurement value off its text.

    A text that is not a number (a status word, an empty string, a
    non-finite token) yields no number. The text itself is the caller's to
    carry verbatim.
    """
    rule, found = _match(text)
    if rule is None:
        return ValueParse(num=None, units=None, rule_id=None)
    units = _unit_or_none(found.group("unit"))
    if not rule.reads_value:
        # A bound is not a reading. The unit it carries is still usable as a
        # fallback, but no number is published as the measurement.
        return ValueParse(num=None, units=units, rule_id=rule.id)
    return ValueParse(
        num=_to_float(found.group("value")),
        units=units,
        rule_id=rule.id,
    )


def parse_limits(
    text: Optional[str], comparators: ComparatorTable = NO_COMPARATORS
) -> LimitsParse:
    """Read a low/high pair off a limits text.

    `comparators` is the deployment's table. With none, no comparator text is
    covered -- which is the honest default for a library that owns no codes.
    """
    rule, found = _match(text)
    if rule is None:
        return LimitsParse(
            low=None, high=None, comparison_type=None, units=None, rule_id=None
        )
    units = _unit_or_none(found.group("unit"))

    if rule.kind == "tolerance":
        centre = _decimal(found.group("center"))
        tolerance = _decimal(found.group("tol"))
        if centre is None or tolerance is None:
            return LimitsParse(
                low=None,
                high=None,
                comparison_type=None,
                units=units,
                rule_id=rule.id,
            )
        # Decimal is the arithmetic; the emitted columns are floats.
        return LimitsParse(
            low=_finite(float(centre - tolerance)),
            high=_finite(float(centre + tolerance)),
            comparison_type=None,
            units=units,
            rule_id=rule.id,
        )

    if rule.kind == "comparator":
        operator = found.group("op")
        row = find_comparator(operator, comparators)
        if row is None:
            # An operator the table does not cover bounds nothing, because
            # nothing says which side it bounds. The text survives in
            # `limits_text` and the row is counted; the number is neither
            # published as a limit nor read as a value.
            return LimitsParse(
                low=None,
                high=None,
                comparison_type=None,
                units=units,
                rule_id=rule.id,
                uncovered_comparator=operator,
            )
        value = _to_float(found.group("value"))
        bounded_low = row.bound == BOUND_LOW
        return LimitsParse(
            low=value if bounded_low else None,
            high=None if bounded_low else value,
            comparison_type=row.code,
            units=units,
            rule_id=rule.id,
        )

    # A bare number as a limits text bounds nothing on its own.
    return LimitsParse(
        low=None,
        high=None,
        comparison_type=None,
        units=units,
        rule_id=rule.id,
    )


def parse_row(
    measurement_text: Optional[str],
    limits_text: Optional[str] = None,
    units: Optional[str] = None,
    comparison_type: Optional[str] = None,
    comparators: ComparatorTable = NO_COMPARATORS,
) -> RowParse:
    """Everything the grammar contributes to one curated row.

    `units` and `comparison_type` are what the source supplied: both win
    over anything parsed out of the text, which is only a fallback.
    `comparators` is the deployment's table.
    """
    value = parse_value(measurement_text)
    limits = parse_limits(limits_text, comparators)

    supplied_units = (units or "").strip() or None
    resolved_units = supplied_units or value.units or limits.units

    supplied_comparison = (comparison_type or "").strip() or None
    resolved_comparison = supplied_comparison or limits.comparison_type

    # A measurement that is not a number is only a failure where a limit is
    # a number: that is what says a number was expected here.
    parse_failed = value.num is None and limits.is_numeric

    return RowParse(
        measurement_num=value.num,
        units=resolved_units,
        low_limit_num=limits.low,
        high_limit_num=limits.high,
        comparison_type=resolved_comparison,
        parse_failed=parse_failed,
        uncovered_comparator=limits.uncovered_comparator,
    )
