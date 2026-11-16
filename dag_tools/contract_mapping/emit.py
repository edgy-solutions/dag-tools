"""Render a plan: into dbt artifacts, and into an operation over rows.

Two renderings, one plan. The warehouse-backed route emits a dbt model, a
`schema.yml` carrying the model contract and the column tests, and one
singular test per invariant dbt has no generic for. The row-backed route --
a file-backed asset has no warehouse to run SELECT in -- applies the same
transforms to dicts and checks the same invariants in Python. That both
exist is the whole reason `mapping.TRANSFORMS` is a closed set.

**Type fidelity is the point of the row route.** The same discipline as the
OTel mapper: a `numeric` column preserves its source's int-ness rather than
being forced through a float, integer text parses exactly past 2**53, and a
non-finite value is refused by name instead of travelling as `NaN`. Forcing
one amount column to float is enough to stop a mapping reproducing its
fixture byte for byte, which is exactly what the proof test asserts.

**Nothing here names a column, a table or a type of a real warehouse.** Type
names come from the mapping's `warehouse_types`, the table from its `source`,
the predicate from its `non_finite_predicate`.
"""
from __future__ import annotations

import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel, ConfigDict

from dag_tools.contract_mapping.normalize import (
    LSTRIP_ZEROS_PATTERN,
    apply_ops,
    part_index,
)
from dag_tools.contract_mapping.plan import (
    MappingPlan,
    PlannedColumn,
    PlannedFilter,
    PlannedTest,
)


class RenderError(Exception):
    """The plan cannot be rendered into the requested form."""


class CoercionError(Exception):
    """A value cannot be carried as the type its contract column declares."""


# --------------------------------------------------------------------------
# values
# --------------------------------------------------------------------------


def _parse_number(text: str) -> Any:
    """Parse numeric text without losing an integer to float precision.

    Integer text becomes an `int` -- `float()` first would round anything past
    2**53 -- and everything else becomes a `Decimal`, so a decimal literal
    keeps the digits it was written with until something asks for a float.
    """
    stripped = text.strip()
    try:
        return int(stripped)
    except ValueError:
        pass
    try:
        return Decimal(stripped)
    except InvalidOperation as exc:
        raise CoercionError(f"{text!r} is not a number") from exc


_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")

_EPOCH_AMBIGUITY = (
    "epoch seconds and epoch milliseconds cannot be told apart from the value "
    "alone, and guessing wrong is a factor-of-1000 error that still lands on a "
    "plausible date; the mapping should produce an ISO-8601 value"
)


def _coerce_timestamp(value: Any, where: str, assume_utc: bool) -> datetime:
    """Carry a timestamp as UTC; refuse anything that names no instant."""
    if isinstance(value, bool):
        raise CoercionError(f"{where}: a boolean is not a timestamp")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        raise CoercionError(
            f"{where}: {value!r} is a date, not a timestamp; widening it to "
            "midnight would invent a time of day"
        )
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip())
        except ValueError as exc:
            raise CoercionError(
                f"{where}: {value!r} is not an ISO-8601 timestamp"
            ) from exc
    elif isinstance(value, (int, float, Decimal)):
        raise CoercionError(f"{where}: {value!r} is a number; {_EPOCH_AMBIGUITY}")
    else:
        raise CoercionError(f"{where}: {value!r} is not a timestamp")

    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        if not assume_utc:
            raise CoercionError(
                f"{where}: {value!r} names no UTC offset, so it names no "
                "instant; reading it as UTC would shift every row of a "
                "non-UTC source by hours. Declare `assume_utc: true` on the "
                "column mapping if the source really is UTC"
            )
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_date(value: Any, where: str) -> date:
    """Carry a calendar date; a timestamp is not one, even at midnight."""
    if isinstance(value, bool):
        raise CoercionError(f"{where}: a boolean is not a date")
    if isinstance(value, datetime):
        raise CoercionError(
            f"{where}: {value!r} is a timestamp, not a date; accepting it at "
            "midnight would pass until the first row carrying a time"
        )
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            # 3.11's fromisoformat also takes `20240131` and week dates; the
            # shape is pinned here so the accepted spelling is one.
            if not _ISO_DATE.fullmatch(value.strip()):
                raise ValueError(value)
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise CoercionError(
                f"{where}: {value!r} is not YYYY-MM-DD, the only date spelling "
                "this library reads"
            ) from exc
    if isinstance(value, (int, float, Decimal)):
        raise CoercionError(f"{where}: {value!r} is a number; {_EPOCH_AMBIGUITY}")
    raise CoercionError(f"{where}: {value!r} is not a date")


def _coerce_string_array(value: Any, where: str) -> List[str]:
    """Carry a list of strings; a bare string is not one."""
    if isinstance(value, str):
        raise CoercionError(
            f"{where}: {value!r} is a bare string, not an array; a string is "
            "iterable and would explode into its characters. The mapping "
            "should produce a list"
        )
    if not isinstance(value, (list, tuple)):
        raise CoercionError(
            f"{where}: a {type(value).__name__} is not an array; only a list "
            "or tuple has an order that is part of its value"
        )
    for index, element in enumerate(value):
        if element is None:
            raise CoercionError(
                f"{where}: element {index} is null, and a null is not a string"
            )
        if not isinstance(element, str):
            raise CoercionError(
                f"{where}: element {index} is a {type(element).__name__}, not "
                "a string; converting it would hide a mapping mistake"
            )
    return list(value)


def _reject_non_finite(value: Any, where: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise CoercionError(
            f"{where} is {value!r}; JSON carries no non-finite number and a "
            "governed read refuses one, so it is refused here rather than "
            "written into the asset"
        )
    if isinstance(value, Decimal) and not value.is_finite():
        raise CoercionError(f"{where} is {value!r}, which is not finite")


def coerce_value(
    value: Any, contract_type: str, where: str, assume_utc: bool = False
) -> Any:
    """Carry a value as the type its contract column declares.

    `numeric` is the interesting one: it accepts an integer or a fractional
    value and keeps whichever it was handed. Widening every `numeric` to
    float would be simpler and would silently change every whole amount in
    the asset.
    """
    if value is None:
        return None

    if contract_type == "string":
        return value if isinstance(value, str) else str(value)

    if contract_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("true", "t", "yes", "y", "1"):
                return True
            if lowered in ("false", "f", "no", "n", "0"):
                return False
            raise CoercionError(f"{where}: {value!r} is not a boolean")
        if isinstance(value, int):
            return bool(value)
        raise CoercionError(f"{where}: {value!r} is not a boolean")

    if contract_type == "timestamp":
        return _coerce_timestamp(value, where, assume_utc)
    if contract_type == "date":
        return _coerce_date(value, where)
    if contract_type == "array<string>":
        return _coerce_string_array(value, where)

    # Whether the value arrived as text matters for `numeric`, below.
    from_text = isinstance(value, str)
    number = _parse_number(value) if from_text else value
    if isinstance(number, bool):
        raise CoercionError(
            f"{where}: a boolean is not a {contract_type} value; the mapping "
            "reads a column the source stores as a flag"
        )
    if not isinstance(number, (int, float, Decimal)):
        raise CoercionError(f"{where}: {value!r} is not numeric")
    _reject_non_finite(number, where)

    if contract_type == "integer":
        if isinstance(number, int):
            return number
        as_decimal = Decimal(number) if isinstance(number, float) else number
        if as_decimal != as_decimal.to_integral_value():
            raise CoercionError(
                f"{where}: {value!r} is not a whole number, and the contract "
                "declares the column integer -- rounding it here would make "
                "the asset disagree with its source without saying so"
            )
        return int(as_decimal)

    if contract_type == "float":
        return float(number)

    # `numeric` accepts a whole number or a fractional one, so there is a
    # choice to make, and both obvious answers are wrong. Widening everything
    # to float silently turns every whole amount in the asset into `1.0e6`.
    # Narrowing every whole-valued number to int turns a source's `100000.0`
    # into `100000`, which is enough on its own to stop a mapping reproducing
    # its fixture.
    #
    # So: the source's WRITTEN FORM decides. Text that was written with a
    # decimal point stays fractional, text written without one stays whole,
    # and a value that arrived already typed keeps its type. A `Decimal` only
    # ever reaches here from `scale`, where int-ness is a property of the
    # arithmetic rather than of anything the source wrote -- a whole result
    # there is a whole number.
    if isinstance(number, Decimal):
        if from_text:
            return float(number)
        if number == number.to_integral_value():
            return int(number)
        return float(number)
    return number


def _literal_equal(value: Any, literal: Any) -> bool:
    """`0 == False` in Python and not in any warehouse."""
    if isinstance(value, bool) != isinstance(literal, bool):
        return False
    return value == literal


def apply_column(column: PlannedColumn, row: Dict[str, Any]) -> Any:
    """Compute one output column's value from one source row."""
    where = f"column {column.name!r}"

    if column.transform == "constant":
        raw: Any = column.value
    else:
        for source in column.sources:
            if source not in row:
                raise RenderError(
                    f"{where} reads source column {source!r}, which this row "
                    "does not carry"
                )
        values = [row[source] for source in column.sources]

        if column.transform == "copy":
            raw = values[0]
        elif column.transform == "trim":
            raw = values[0].strip() if isinstance(values[0], str) else values[0]
        elif column.transform == "upper":
            raw = values[0].upper() if isinstance(values[0], str) else values[0]
        elif column.transform == "lower":
            raw = values[0].lower() if isinstance(values[0], str) else values[0]
        elif column.transform == "concat":
            raw = column.separator.join(
                "" if value is None else str(value) for value in values
            )
        elif column.transform == "coalesce":
            raw = next((value for value in values if value is not None), column.default)
        elif column.transform == "scale":
            base = values[0]
            if base is None:
                raw = None
            else:
                number = _parse_number(base) if isinstance(base, str) else base
                if isinstance(number, float):
                    number = Decimal(repr(number))
                raw = Decimal(number) * column.factor
        elif column.transform == "nullif":
            # Compared before coercion: what is matched is what the source
            # wrote, not what the contract widened it to.
            raw = None if _literal_equal(values[0], column.value) else values[0]
        elif column.transform == "in_set":
            # Exact match, no folding or trimming. A null answers False, as
            # the SQL's coalesce does, rather than staying null.
            raw = values[0] is not None and any(
                _literal_equal(values[0], literal) for literal in column.values or []
            )
        elif column.transform == "normalize":
            raw = apply_ops(column.ops, column.part, values[0])
        else:  # pragma: no cover - the mapping model refuses anything else
            raise RenderError(f"{where}: no row rendering for {column.transform!r}")

    return coerce_value(raw, column.contract_type, where, column.assume_utc)


def apply_to_rows(
    plan: MappingPlan, rows: Sequence[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Apply the plan to source rows, producing the contract's columns.

    Output column order follows the contract, not the source, so two mappings
    onto one contract produce the same shape.
    """
    return [
        {column.name: apply_column(column, row) for column in plan.columns}
        for row in rows
        if _row_kept(plan.row_filters, row)
    ]


def _filter_holds(flt: PlannedFilter, row: Dict[str, Any]) -> bool:
    """One `where` predicate over one source row; mirrors `_sql_predicate`.

    A null satisfies `is_null` and nothing else: `!=` and `not_in` are false
    for it, as the SQL route's `is not null` guard makes them.
    """
    if flt.column not in row:
        raise RenderError(
            f"where filter reads source column {flt.column!r}, which this "
            "row does not carry"
        )
    value = row[flt.column]
    if flt.op == "is_null":
        return value is None
    if flt.op == "not_null":
        return value is not None
    if value is None:
        return False
    if flt.op == "=":
        return _literal_equal(value, flt.value)
    if flt.op == "!=":
        return not _literal_equal(value, flt.value)
    member = any(_literal_equal(value, literal) for literal in flt.values or [])
    return member if flt.op == "in" else not member


def _row_kept(filters: Sequence[PlannedFilter], row: Dict[str, Any]) -> bool:
    return all(_filter_holds(flt, row) for flt in filters)


# --------------------------------------------------------------------------
# checks over rows
# --------------------------------------------------------------------------


class CheckFailure(BaseModel):
    """One invariant that does not hold over the produced rows."""

    model_config = ConfigDict(extra="forbid")

    rule_id: str
    kind: str
    columns: List[str]
    detail: str
    row_count: int = 0


_PY_TYPES: Dict[str, tuple] = {
    "string": (str,),
    "integer": (int,),
    "float": (float,),
    "numeric": (int, float),
    "boolean": (bool,),
}


def _key(row: Dict[str, Any], columns: Sequence[str]) -> tuple:
    return tuple(row.get(column) for column in columns)


def check_rows(
    plan: MappingPlan, rows: Sequence[Dict[str, Any]]
) -> List[CheckFailure]:
    """Run the plan's tests over produced rows, in Python.

    The row-side counterpart of the emitted dbt tests, for the asset that is
    not a dbt model. Failures are returned, not raised: an asset check wants
    every failing rule at once, and the caller decides whether a failure
    blocks.

    Only the invariants whose statement is dialect-free run here. The
    `non_finite` test's SQL predicate is a warehouse's; the row side checks
    finiteness directly instead of trying to evaluate it.
    """
    failures: List[CheckFailure] = []

    for test in plan.tests:
        if test.kind == "column_present":
            missing = sorted(
                {column for column in test.columns for row in rows if column not in row}
            )
            if missing:
                failures.append(
                    CheckFailure(
                        rule_id=test.rule_id,
                        kind=test.kind,
                        columns=missing,
                        detail=f"produced rows do not carry {', '.join(missing)}",
                    )
                )

        elif test.kind == "types_enforced":
            by_name = {column.name: column for column in plan.columns}
            offenders: Dict[str, int] = {}
            for row in rows:
                for name in test.columns:
                    value = row.get(name)
                    if value is None:
                        continue
                    wanted = _PY_TYPES.get(by_name[name].contract_type)
                    if wanted is None:
                        continue
                    # bool is an int subclass; a flag in a numeric column is
                    # the mistake this check exists to catch.
                    if isinstance(value, bool) is not (
                        by_name[name].contract_type == "boolean"
                    ) or not isinstance(value, wanted):
                        offenders[name] = offenders.get(name, 0) + 1
            if offenders:
                failures.append(
                    CheckFailure(
                        rule_id=test.rule_id,
                        kind=test.kind,
                        columns=sorted(offenders),
                        detail="; ".join(
                            f"{name}: {count} row(s) of the wrong type"
                            for name, count in sorted(offenders.items())
                        ),
                        row_count=sum(offenders.values()),
                    )
                )

        elif test.kind == "not_null":
            offenders = {}
            for row in rows:
                for name in test.columns:
                    if row.get(name) is None:
                        offenders[name] = offenders.get(name, 0) + 1
            if offenders:
                failures.append(
                    CheckFailure(
                        rule_id=test.rule_id,
                        kind=test.kind,
                        columns=sorted(offenders),
                        detail="; ".join(
                            f"{name}: {count} null row(s)"
                            for name, count in sorted(offenders.items())
                        ),
                        row_count=sum(offenders.values()),
                    )
                )

        elif test.kind == "key_unique":
            seen: Dict[tuple, int] = {}
            for row in rows:
                key = _key(row, test.columns)
                seen[key] = seen.get(key, 0) + 1
            duplicates = {key: count for key, count in seen.items() if count > 1}
            if duplicates:
                failures.append(
                    CheckFailure(
                        rule_id=test.rule_id,
                        kind=test.kind,
                        columns=list(test.columns),
                        detail=(
                            f"{len(duplicates)} key(s) appear more than once "
                            f"over ({', '.join(test.columns)})"
                        ),
                        row_count=sum(duplicates.values()),
                    )
                )

        elif test.kind == "constant_within":
            column = test.columns[0]
            within = test.within[0]
            observed: Dict[Any, set] = {}
            for row in rows:
                observed.setdefault(row.get(within), set()).add(row.get(column))
            varying = {path for path, values in observed.items() if len(values) > 1}
            if varying:
                failures.append(
                    CheckFailure(
                        rule_id=test.rule_id,
                        kind=test.kind,
                        columns=[column],
                        detail=(
                            f"{column} takes more than one value within "
                            f"{len(varying)} distinct {within}"
                        ),
                        row_count=len(varying),
                    )
                )

        elif test.kind == "period_in_calendar":
            column = test.columns[0]
            allowed = set(test.values)
            outside = sorted(
                {
                    str(row.get(column))
                    for row in rows
                    if row.get(column) is not None and row.get(column) not in allowed
                }
            )
            if outside:
                failures.append(
                    CheckFailure(
                        rule_id=test.rule_id,
                        kind=test.kind,
                        columns=[column],
                        detail=(
                            f"{len(outside)} value(s) of {column} are not in "
                            "the calendar"
                        ),
                        row_count=len(outside),
                    )
                )

        elif test.kind == "non_finite":
            offenders = {}
            for row in rows:
                for name in test.columns:
                    value = row.get(name)
                    if isinstance(value, float) and not math.isfinite(value):
                        offenders[name] = offenders.get(name, 0) + 1
            if offenders:
                failures.append(
                    CheckFailure(
                        rule_id=test.rule_id,
                        kind=test.kind,
                        columns=sorted(offenders),
                        detail="; ".join(
                            f"{name}: {count} non-finite row(s)"
                            for name, count in sorted(offenders.items())
                        ),
                        row_count=sum(offenders.values()),
                    )
                )

    return failures


# --------------------------------------------------------------------------
# dbt artifacts
# --------------------------------------------------------------------------


def _sql_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _sql_expression(column: PlannedColumn) -> str:
    """Render one column's SELECT expression, cast if the mapping says how.

    The cast wraps the whole expression rather than the source column: what
    needs correcting is the type the *transform* produced, not the type the
    source stored.
    """
    rendered = _sql_transform(column)
    if column.cast_template:
        return column.cast_template.format(expr=rendered)
    return rendered


def _sql_normalize(column: PlannedColumn) -> str:
    """Nest the normalizer's ops, left to right, around the source column.

    `trim`/`upper`/`lower` use the spellings the standalone transforms do;
    the regex ops go through the operator's `regex_templates`, with the
    pattern and replacement as quote-doubled SQL literals and the group as a
    positional integer. The extract is wrapped in `nullif(..., '')` so a
    group that did not participate is NULL whatever the dialect answers.
    """
    expr = column.sources[0]
    for op in column.ops:
        if op.op in ("trim", "upper", "lower"):
            expr = f"{op.op}({expr})"
        elif op.op in ("strip", "replace", "lstrip_zeros"):
            template = column.regex_templates.get("regex_replace")
            if template is None:
                raise RenderError(
                    f"column {column.name!r} uses normalizer op {op.op!r}, "
                    "which needs the mapping's `regex_templates.regex_replace`"
                )
            pattern = LSTRIP_ZEROS_PATTERN if op.op == "lstrip_zeros" else op.pattern
            replacement = op.replacement if op.op == "replace" else ""
            expr = template.format(
                expr=expr,
                pattern=_sql_literal(pattern),
                replacement=_sql_literal(replacement),
            )
        elif op.op == "split_revision":
            template = column.regex_templates.get("regex_extract")
            if template is None:
                raise RenderError(
                    f"column {column.name!r} uses normalizer op 'split_revision', "
                    "which needs the mapping's `regex_templates.regex_extract`"
                )
            extracted = template.format(
                expr=expr,
                pattern=_sql_literal(op.pattern),
                group=part_index(op.pattern, column.part),
            )
            expr = f"nullif({extracted}, '')"
        else:  # pragma: no cover - the op model refuses anything else
            raise RenderError(f"no SQL rendering for normalizer op {op.op!r}")
    return expr


def _sql_transform(column: PlannedColumn) -> str:
    if column.transform == "constant":
        return _sql_literal(column.value)
    first = column.sources[0] if column.sources else ""
    if column.transform == "copy":
        return first
    if column.transform == "trim":
        return f"trim({first})"
    if column.transform == "upper":
        return f"upper({first})"
    if column.transform == "lower":
        return f"lower({first})"
    if column.transform == "concat":
        joined = f", {_sql_literal(column.separator)}, ".join(column.sources)
        return f"concat({joined})" if column.separator else f"concat({', '.join(column.sources)})"
    if column.transform == "coalesce":
        arguments = list(column.sources)
        if column.default is not None:
            arguments.append(_sql_literal(column.default))
        return f"coalesce({', '.join(arguments)})"
    if column.transform == "scale":
        return f"({first} * {column.factor})"
    if column.transform == "nullif":
        return f"nullif({first}, {_sql_literal(column.value)})"
    if column.transform == "normalize":
        return _sql_normalize(column)
    if column.transform == "in_set":
        literals = ", ".join(_sql_literal(v) for v in column.values or [])
        # A bare `in` is NULL for a null column; say False instead.
        return f"coalesce({first} in ({literals}), false)"
    raise RenderError(f"no SQL rendering for transform {column.transform!r}")


def _sql_predicate(flt: PlannedFilter) -> str:
    """Three-valued logic written out. The two `is not null` guards make the
    SQL agree with the row route, which keeps a null for `!=` in Python."""
    column = flt.column
    if flt.op == "=":
        return f"{column} = {_sql_literal(flt.value)}"
    if flt.op == "!=":
        return f"({column} is not null and {column} != {_sql_literal(flt.value)})"
    literals = ", ".join(_sql_literal(v) for v in flt.values or [])
    if flt.op == "in":
        return f"{column} in ({literals})"
    if flt.op == "not_in":
        return f"({column} is not null and {column} not in ({literals}))"
    if flt.op == "is_null":
        return f"{column} is null"
    if flt.op == "not_null":
        return f"{column} is not null"
    raise RenderError(f"no SQL rendering for where operator {flt.op!r}")


def render_model_sql(plan: MappingPlan) -> str:
    """Render the dbt model.

    `source_table` is written into the FROM clause verbatim, so a mapping may
    put a `{{ source(...) }}` call there and have dbt resolve it -- which is
    the right way to get the dependency edge, and is why this function does
    no quoting of its own -- nor of the `where` columns.

    `where` filters are ANDed in the order written; with none there is no
    `where` line at all, so a mapping without filters renders as it always
    did. There is no `or` and no nesting: a disjunction belongs in a view the
    mapping reads from.
    """
    if not plan.source_table:
        raise RenderError(
            "the mapping declares no `source.table`, so there is nothing to "
            "select from; a mapping without a table is a rows-mode mapping"
        )

    lines = [
        f"-- generated from contract {plan.contract_id}"
        + (f" {plan.contract_version}" if plan.contract_version else ""),
        f"-- plan {plan.plan_hash}; edit the mapping, not this file",
        "",
        "select",
    ]
    rendered = [
        f"    {_sql_expression(column)} as {column.name}" for column in plan.columns
    ]
    lines.append(",\n".join(rendered))
    lines.append(f"from {plan.source_table}")
    for index, flt in enumerate(plan.row_filters):
        lines.append(("where " if index == 0 else "  and ") + _sql_predicate(flt))
    return "\n".join(lines) + "\n"


def _yaml_scalar(value: Optional[str]) -> str:
    if value is None:
        return '""'
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def render_schema_yml(plan: MappingPlan) -> str:
    """Render the model's `schema.yml`: the contract, and the column tests.

    Only the tests dbt has a generic for appear here (`not_null`). Everything
    else is a singular test, because a generic that needs a package would
    make this library's output depend on the deployment having installed it.
    """
    enforced = all(column.warehouse_type for column in plan.columns) and bool(
        plan.columns
    )
    not_null_rules = {
        test.rule_id: set(test.columns) for test in plan.tests if test.kind == "not_null"
    }

    lines = ["version: 2", "", "models:", f"  - name: {plan.model_name}"]
    description = f"Satisfies contract {plan.contract_id}"
    if plan.contract_version:
        description += f" {plan.contract_version}"
    if plan.dataset_urn:
        description += f". Asset of record: {plan.dataset_urn}"
    lines.append(f"    description: {_yaml_scalar(description)}")

    if enforced:
        lines += ["    config:", "      contract:", "        enforced: true"]

    lines.append("    columns:")
    for column in plan.columns:
        lines.append(f"      - name: {column.name}")
        if column.description or column.unit:
            parts = [part for part in (column.description, column.unit and f"unit: {column.unit}") if part]
            lines.append(f"        description: {_yaml_scalar('; '.join(parts))}")
        if column.warehouse_type:
            lines.append(f"        data_type: {column.warehouse_type}")
        for rule_id, names in not_null_rules.items():
            if column.name in names:
                lines += [
                    "        tests:",
                    "          - not_null:",
                    f"              name: {plan.model_name}__{rule_id}__{column.name}",
                ]
    return "\n".join(lines) + "\n"


def _test_name(plan: MappingPlan, test: PlannedTest) -> str:
    parts = [plan.model_name, test.rule_id, *test.columns, *test.within]
    return "__".join(parts)


def render_singular_tests(plan: MappingPlan) -> Dict[str, str]:
    """Render one `.sql` file per invariant dbt has no built-in generic for.

    Each selects the failing rows and carries its rule id as a column, so a
    `dbt test` failure and a governed read's refusal name the same rule
    without an operator mapping one to the other by hand.
    """
    if not plan.source_table:
        raise RenderError(
            "singular tests select from the model, which is only rendered for "
            "a mapping that declares `source.table`"
        )

    reference = "{{ ref('" + plan.model_name + "') }}"
    files: Dict[str, str] = {}

    for test in plan.tests:
        header = (
            f"-- rule: {test.rule_id}\n"
            f"-- generated from contract {plan.contract_id}; "
            "edit the mapping, not this file\n"
        )
        rule = _sql_literal(test.rule_id)

        if test.kind == "key_unique":
            keys = ", ".join(test.columns)
            body = (
                f"select {rule} as rule_id, {keys}, count(*) as rows_for_key\n"
                f"from {reference}\n"
                f"group by {keys}\n"
                "having count(*) > 1\n"
            )
        elif test.kind == "constant_within":
            column = test.columns[0]
            within = test.within[0]
            body = (
                f"select {rule} as rule_id, {within}, "
                f"count(distinct {column}) as distinct_values\n"
                f"from {reference}\n"
                f"group by {within}\n"
                f"having count(distinct {column}) > 1\n"
            )
        elif test.kind == "period_in_calendar":
            column = test.columns[0]
            allowed = ", ".join(_sql_literal(value) for value in test.values)
            body = (
                f"select {rule} as rule_id, {column}\n"
                f"from {reference}\n"
                f"where {column} is not null and {column} not in ({allowed})\n"
            )
        elif test.kind == "non_finite":
            predicate = test.predicate or ""
            clauses = " or ".join(
                f"({predicate.format(column=column)})" for column in test.columns
            )
            body = (
                f"select {rule} as rule_id, *\n"
                f"from {reference}\n"
                f"where {clauses}\n"
            )
        else:
            # not_null rides on schema.yml; the model contract covers
            # column presence and types.
            continue

        files[f"{_test_name(plan, test)}.sql"] = header + body

    return files


def render_dbt_artifacts(plan: MappingPlan) -> Dict[str, str]:
    """Every file the dbt route emits, keyed by path relative to the target.

    Returned as text rather than written, so a dry run shows the exact bytes
    the real run would write -- the same reason the OTel mapper renders
    plans Dagster-side.
    """
    files = {
        f"{plan.model_name}.sql": render_model_sql(plan),
        f"{plan.model_name}.yml": render_schema_yml(plan),
    }
    for name, text in render_singular_tests(plan).items():
        files[f"tests/{name}"] = text
    return files
