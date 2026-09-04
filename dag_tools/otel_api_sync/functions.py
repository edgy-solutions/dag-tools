"""Expression helpers exposed to mapping templates.

This module is deliberately free of Dagster, Restate and dlt imports: the
same helpers run inside the Dagster dispatch asset (where plans are
rendered) and inside unit tests, with nothing else pulled in.

Two conventions matter when reading the helpers below:

**Selectors are dual-form.** ``attr`` is callable as either
``attr(row, "ci.branch")`` (direct lookup) or ``attr("ci.branch")``
(curried selector, for passing into ``distinct`` / ``group_map`` /
``pluck``). That is a deliberate design choice, not an accident of
``*args`` — mapping YAML needs both shapes, and forcing lambdas into
templates would be worse.

**Everything coerces explicitly.** OpenTelemetry attribute maps are
``Map(String, String)`` in the ClickHouse exporter schema, so every
value read out of them is a string. APIs generally want numbers,
booleans and arrays. Nothing here guesses: use ``as_int`` / ``as_float``
/ ``as_bool`` / ``split`` when the target field is not a string.
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union

Row = Dict[str, Any]
Selector = Union[str, Callable[[Row], Any]]

# Column names the OpenTelemetry ClickHouse exporter uses for its
# Map(String, String) attribute columns, searched in this order when a key
# is not a top-level column. Overridable per pipeline via
# ``attribute_columns`` in the spec.
DEFAULT_ATTRIBUTE_COLUMNS = (
    "SpanAttributes",
    "ResourceAttributes",
    "LogAttributes",
    "Attributes",
)


def normalize_key(key: str) -> str:
    """Collapse a column name to a comparison form.

    dlt snake-cases and lower-cases identifiers on the way into a staging
    destination, so ClickHouse's ``SpanAttributes`` is read back as
    ``span_attributes``. Normalizing both sides of every lookup means one
    mapping file works unchanged whether the pipeline stages through a
    warehouse or reads ClickHouse directly — otherwise switching modes
    would silently null out every field.
    """
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _normalized_index(mapping: Dict[str, Any]) -> Dict[str, Any]:
    index: Dict[str, Any] = {}
    for existing_key, value in mapping.items():
        if isinstance(existing_key, str):
            index.setdefault(normalize_key(existing_key), value)
    return index


def _lookup(row: Any, key: str, attribute_columns: Sequence[str]) -> Any:
    """Resolve ``key`` against a row: real column first, then attribute maps.

    Each tier is tried exactly, then normalized (see :func:`normalize_key`).
    Returns None rather than raising when the key is absent anywhere —
    mapping templates run over heterogeneous telemetry and a missing
    attribute is normal, not exceptional. Use ``require`` when absence
    should fail the run instead.
    """
    if not isinstance(row, dict):
        return None

    if key in row:
        return row[key]

    bags: List[Dict[str, Any]] = []
    row_index: Optional[Dict[str, Any]] = None
    for column in attribute_columns:
        bag = row.get(column)
        if not isinstance(bag, dict):
            # The column may have been renamed by the staging load.
            if row_index is None:
                row_index = _normalized_index(row)
            bag = row_index.get(normalize_key(column))
        if isinstance(bag, dict):
            if key in bag:
                return bag[key]
            bags.append(bag)

    normalized = normalize_key(key)
    if row_index is None:
        row_index = _normalized_index(row)
    if normalized in row_index:
        return row_index[normalized]
    for bag in bags:
        bag_index = _normalized_index(bag)
        if normalized in bag_index:
            return bag_index[normalized]

    # Finally: dlt *flattens* nested objects into `parent__child` columns
    # unless the map column is explicitly hinted as JSON, so ClickHouse's
    # SpanAttributes['item.name'] is read back as the plain column
    # `span_attributes__item_name`. Normalizing the concatenation matches
    # both shapes, which is what lets one mapping file work whether the
    # staging load preserved the map or exploded it. Without this, a
    # staged pipeline silently resolves every attribute to None and
    # dispatches nothing at all.
    for column in attribute_columns:
        flattened = row_index.get(normalize_key(f"{column}{key}"))
        if flattened is not None:
            return flattened
    return None


def attribute_bags(row: Any, attribute_columns: Sequence[str]) -> List[Dict[str, Any]]:
    """Every attribute map on a row, resolved tolerantly by column name.

    Shared with :func:`_lookup` so that anything scanning attributes in
    bulk (metric extraction, prefix sweeps) sees the same maps a
    single-key lookup would — including after a staging load renamed
    ``SpanAttributes`` to ``span_attributes``.
    """
    if not isinstance(row, dict):
        return []
    bags: List[Dict[str, Any]] = []
    index = _normalized_index(row)
    for column in attribute_columns:
        bag = row.get(column)
        if not isinstance(bag, dict):
            bag = index.get(normalize_key(column))
        if isinstance(bag, dict) and bag not in bags:
            bags.append(bag)
    return bags


def row_get(row: Any, key: str) -> Any:
    """Normalization-tolerant top-level column read.

    Used by machinery outside the template layer (the readiness gate's
    timestamp column, for instance) that must survive the same
    staged-vs-direct renaming.
    """
    if not isinstance(row, dict):
        return None
    if key in row:
        return row[key]
    return _normalized_index(row).get(normalize_key(key))


def _resolve(selector: Optional[Selector], row: Any, attribute_columns: Sequence[str]) -> Any:
    """Apply a selector (callable, key string, or None) to one row."""
    if selector is None:
        return row
    if callable(selector):
        return selector(row)
    return _lookup(row, selector, attribute_columns)


def _is_empty(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def build_functions(attribute_columns: Sequence[str] = DEFAULT_ATTRIBUTE_COLUMNS) -> Dict[str, Any]:
    """Build the template global namespace, bound to this pipeline's
    attribute-column search order."""

    columns = tuple(attribute_columns or DEFAULT_ATTRIBUTE_COLUMNS)

    def attr(*args: Any) -> Any:
        """``attr(row, key)`` -> value; ``attr(key)`` -> selector callable."""
        if len(args) == 1 and isinstance(args[0], str):
            key = args[0]
            return lambda row: _lookup(row, key, columns)
        if len(args) == 2:
            return _lookup(args[0], args[1], columns)
        raise TypeError(
            "attr() takes either attr(row, key) or the curried attr(key); "
            f"got {len(args)} argument(s)"
        )

    def require(row: Any, key: str) -> Any:
        """Like ``attr`` but raises when the key is missing or blank.

        Use for fields the target API rejects as null — failing at render
        time in Dagster is far cheaper than failing per-call in Restate.
        """
        value = _lookup(row, key, columns)
        if _is_empty(value):
            raise ValueError(f"required attribute '{key}' is missing or empty")
        return value

    def pluck(rows: Iterable[Any], selector: Optional[Selector] = None) -> List[Any]:
        """Every selected value, order preserved, duplicates kept."""
        return [_resolve(selector, row, columns) for row in rows or []]

    def distinct(rows: Iterable[Any], selector: Optional[Selector] = None) -> List[Any]:
        """Unique selected values, first-seen order, empties dropped.

        Works on rows with a selector (``distinct(rows, attr('id'))``) and
        on a plain list (``distinct(some_list)``).
        """
        seen: Dict[Any, Any] = {}
        for row in rows or []:
            value = _resolve(selector, row, columns)
            if _is_empty(value):
                continue
            key = value if isinstance(value, (str, int, float, bool)) else repr(value)
            seen.setdefault(key, value)
        return list(seen.values())

    def group_map(
        rows: Iterable[Any],
        key_selector: Selector,
        value_selector: Optional[Selector] = None,
    ) -> Dict[Any, List[Any]]:
        """Bucket values by key: ``{key: [value, ...]}``, order preserved.

        This is the lever for per-item scoping — e.g. mapping each item to
        *its own* entities rather than to every entity in the group.
        Values are not de-duplicated; wrap the lookup in ``distinct`` when
        the target API wants a set.
        """
        out: Dict[Any, List[Any]] = {}
        for row in rows or []:
            key = _resolve(key_selector, row, columns)
            if _is_empty(key):
                continue
            value = _resolve(value_selector, row, columns)
            if _is_empty(value):
                continue
            out.setdefault(key, []).append(value)
        return out

    def filter_rows(rows: Iterable[Any], selector: Selector, value: Any = True) -> List[Any]:
        """Rows whose selected value equals ``value``."""
        return [row for row in rows or [] if _resolve(selector, row, columns) == value]

    def metrics_from_prefix(
        row: Any,
        prefix: str,
        name_key: str = "metricName",
        value_key: str = "resultNumeric",
        strip_prefix: bool = True,
    ) -> List[Dict[str, Any]]:
        """Turn prefixed attributes into a list of metric objects.

        ``metrics_from_prefix(row, 'metric.')`` over attributes
        ``{"metric.NUM_ERROR": "0", "metric.TOTAL": "81.2"}`` yields
        ``[{"metricName": "NUM_ERROR", "resultNumeric": 0.0},
           {"metricName": "TOTAL", "resultNumeric": 81.2}]``.

        Values are coerced to numbers (int when integral, else float);
        non-numeric values are skipped rather than shipped as strings,
        because the target field is numeric by contract.
        """
        out: List[Dict[str, Any]] = []
        bags: List[Dict[str, Any]] = []
        if isinstance(row, dict):
            bags.append({k: v for k, v in row.items() if isinstance(k, str)})
            # Resolved tolerantly: after a staging load the map column is
            # `span_attributes`, and an exact-name lookup would find
            # nothing and silently return an empty metric list.
            bags.extend(attribute_bags(row, columns))
        for bag in bags:
            for key in sorted(bag):
                if not key.startswith(prefix):
                    continue
                number = as_number(bag[key], default=None)
                if number is None:
                    continue
                name = key[len(prefix):] if strip_prefix else key
                if any(m[name_key] == name for m in out):
                    continue
                out.append({name_key: name, value_key: number})
        return out

    return {
        "attr": attr,
        "require": require,
        "pluck": pluck,
        "distinct": distinct,
        "group_map": group_map,
        "filter_rows": filter_rows,
        "metrics_from_prefix": metrics_from_prefix,
        # Type coercion — see the module docstring on why nothing is implicit.
        "as_int": as_int,
        "as_float": as_float,
        "as_bool": as_bool,
        "as_str": as_str,
        "as_number": as_number,
        # Collections / strings.
        "split": split,
        "join": join,
        "first": first,
        "default": default,
        "count": count,
        "sort": sort_values,
        "unique": unique,
        # Structured attribute values -- a JSON object serialized into a
        # single Map(String, String) value, and re-keying it.
        "from_json": from_json,
        "invert_multimap": invert_multimap,
        # Time.
        "to_iso": to_iso,
        "epoch_seconds": epoch_seconds,
    }


# --- standalone helpers (no attribute-column binding needed) ----------------


def from_json(value: Any, default: Any = None) -> Any:
    """Parse an attribute value that is itself JSON, tolerantly.

    Real telemetry carries structured planning data as a JSON-encoded
    STRING inside a ``Map(String, String)`` attribute -- a lookup table of
    ``{key: [values...]}`` serialized into a single value and emitted once
    per execution group. The map column can only hold strings, so the
    structure has nowhere else to go.

    ABSENCE IS ORDINARY, NOT EXCEPTIONAL. A group that never emitted the
    attribute is a normal group, so this returns ``default`` rather than
    raising -- on ``None``, on a blank string, and on malformed JSON
    alike. Raising would take out the whole plan render for a group whose
    only sin was not carrying an optional attribute.

    Malformed and absent deliberately collapse to the same result. The
    caller cannot act differently on them (both mean "no table here"), and
    a helper that raised on one would make an upstream producer's bug
    surface as a render failure in an unrelated pipeline.

    Already-decoded values pass through untouched, so the helper is safe
    to apply to an attribute whose encoding varies between emitters:

        from_json({"a": ["x"]})   -> {"a": ["x"]}
        from_json('{"a": ["x"]}') -> {"a": ["x"]}
        from_json(None)           -> default
        from_json("{not json")    -> default
    """
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return default
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            return default
    if not isinstance(value, str):
        return default
    if not value.strip():
        return default
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return default


def invert_multimap(mapping: Any) -> Dict[Any, List[Any]]:
    """``{k: [v, ...]}`` -> ``{v: [k, ...]}``, insertion-ordered.

    A serialized lookup table arrives keyed the way its PRODUCER thought
    about it, which is not always the way a target endpoint's fan-out
    needs it. Inverting in the mapping avoids either asking the emitter to
    change or hand-writing the flip in a template.

    ORDERING IS PART OF THE CONTRACT, not an implementation detail. The
    result feeds fan-out collections, fan-out items become step labels,
    and labels go into ``call_key`` -- so a non-deterministic order would
    change durable step identities between two renders of identical
    telemetry, and Restate would treat replays as new work. Outer keys
    appear in first-seen order of the values; each inner list in
    first-seen order of the original keys.

    Tolerances, each because the shape genuinely varies in the wild:

    * a scalar value is treated as a one-element list, so
      ``{"a": "x"}`` and ``{"a": ["x"]}`` invert identically;
    * ``None``, an empty mapping, or a non-mapping yields ``{}`` -- which
      renders dependent fan-out steps as empty no-ops rather than
      exploding a plan;
    * empty values are skipped, matching ``distinct`` and ``group_map``;
    * unhashable values (a nested list or object where a scalar was
      expected) are skipped rather than raising, since they cannot be a
      key and one malformed entry should not lose the rest of the table;
    * a repeated pair contributes its key once -- the inversion of a
      lookup table, not a bag.
    """
    out: Dict[Any, List[Any]] = {}
    if not isinstance(mapping, dict):
        return out
    for key, values in mapping.items():
        if _is_empty(key):
            continue
        if values is None:
            continue
        if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
            values = [values]
        for value in values:
            if _is_empty(value):
                continue
            try:
                bucket = out.setdefault(value, [])
            except TypeError:
                continue  # unhashable: cannot be a key
            if key not in bucket:
                bucket.append(key)
    return out


def as_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if isinstance(value, bool):
            return int(value)
        return int(float(value))
    except (TypeError, ValueError):
        return default


def as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_number(value: Any, default: Any = None) -> Any:
    """Numeric coercion that keeps integers integral.

    JSON has one number type but APIs and their schema validators often
    care whether ``0`` arrives as ``0`` or ``0.0``; this preserves the
    distinction where the input allows it.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    try:
        text = str(value).strip()
        if not text:
            return default
        number = float(text)
    except (TypeError, ValueError):
        return default
    return int(number) if number.is_integer() and "." not in text and "e" not in text.lower() else number


_TRUE = {"true", "1", "yes", "y", "on", "pass", "passed", "success"}
_FALSE = {"false", "0", "no", "n", "off", "fail", "failed"}


def as_bool(value: Any, default: Optional[bool] = None) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
    return default


def as_str(value: Any, default: str = "") -> str:
    return default if value is None else str(value)


def split(value: Any, sep: str = ",", strip: bool = True, drop_empty: bool = True) -> List[str]:
    """Explode a delimited attribute into a real JSON array.

    Telemetry attributes are strings, so a list of identifiers arrives as
    ``"a,b,c"``. Any API field typed as an array needs this.
    """
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        parts = [str(v) for v in value]
    else:
        parts = str(value).split(sep)
    if strip:
        parts = [p.strip() for p in parts]
    if drop_empty:
        parts = [p for p in parts if p]
    return parts


def join(values: Any, sep: str = ",") -> str:
    if values is None:
        return ""
    if isinstance(values, (str, bytes)):
        return values if isinstance(values, str) else values.decode()
    return sep.join(str(v) for v in values if v is not None and str(v) != "")


def first(values: Any, fallback: Any = None) -> Any:
    for value in values or []:
        return value
    return fallback


def default(value: Any, fallback: Any) -> Any:
    return fallback if _is_empty(value) else value


def count(values: Any) -> int:
    try:
        return len(values)
    except TypeError:
        return 0


def sort_values(values: Any) -> List[Any]:
    try:
        return sorted(values or [])
    except TypeError:
        return list(values or [])


def unique(values: Any) -> List[Any]:
    seen: Dict[Any, Any] = {}
    for value in values or []:
        key = value if isinstance(value, (str, int, float, bool)) else repr(value)
        seen.setdefault(key, value)
    return list(seen.values())


def to_iso(value: Any, milliseconds: bool = True, zulu: bool = True) -> Optional[str]:
    """Format a ClickHouse timestamp as ISO 8601.

    ClickHouse ``DateTime64`` comes back through clickhouse-connect as a
    naive ``datetime``; APIs that specify ISO 8601 generally want
    ``2026-08-10T20:02:36.338Z``. Naive values are treated as UTC, which
    is what the OTel exporter writes.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    elif isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
    else:
        return str(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    parsed = parsed.astimezone(dt.timezone.utc)

    text = parsed.strftime("%Y-%m-%dT%H:%M:%S")
    if milliseconds:
        text = f"{text}.{parsed.microsecond // 1000:03d}"
    return f"{text}Z" if zulu else f"{text}+00:00"


def epoch_seconds(value: Any) -> Optional[float]:
    if isinstance(value, dt.datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
        return moment.timestamp()
    return as_float(value)
