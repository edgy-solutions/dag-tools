"""Turning extracted text into typed values. Coercion never raises.

Every function here answers ``(value, failed)``. ``failed`` is true when a
value was present and could not be read as the column's type; the engine
counts those per column and stores null. A null in is a null out and is not
a failure -- absence is data, an unreadable value is a defect, and the two
go to different people.

The rules are deliberately narrow, because a lenient reading is the kind
that reads as a correct answer:

  * integers are exact (``^[+-]?\\d+$``), never through ``float()``, so a
    value past 2**53 is not rounded; ``int32`` is range-checked;
  * floats must look like a decimal number -- ``float()`` alone also accepts
    ``nan``, ``inf`` and ``1_000`` -- and a non-finite result is a failure;
  * booleans are ``true``/``false``/``1``/``0`` in any case; anything else
    goes through a ``map``;
  * timestamps are ISO-8601 with any number of fractional digits (truncated
    to microseconds) and ``Z`` or an offset, or a ``format`` for strptime. A
    value naming no offset is read in the column's ``timezone``; with none
    declared it is a failure, because a naive value names no instant and
    reading it as UTC would shift every row of a non-UTC source.
"""
from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional, Tuple

INT32_MIN, INT32_MAX = -(2**31), 2**31 - 1
INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1
_MAX_DIGITS = len(str(INT64_MAX))

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:[.,](\d+))?)?"
    r"\s*(Z|z|[+-]\d{2}(?::?\d{2})?)?$"
)
_TRUE = {"true", "1"}
_FALSE = {"false", "0"}

Result = Tuple[Any, bool]


def to_string(v: Any) -> Result:
    if v is None:
        return None, False
    if isinstance(v, bool):
        return ("true" if v else "false"), False
    return str(v), False


def _int(v: Any, lo: int, hi: int) -> Result:
    if v is None:
        return None, False
    if isinstance(v, bool):
        return None, True
    if isinstance(v, int):
        n = v
    elif isinstance(v, float):
        if not v.is_integer():
            return None, True
        n = int(v)
    else:
        s = str(v).strip()
        if not _INT_RE.match(s):
            return None, True
        # Out of range before it is parsed: no column here holds more than 19
        # digits, and int() refuses text past sys.get_int_max_str_digits.
        if len(s.lstrip("+-").lstrip("0")) > _MAX_DIGITS:
            return None, True
        n = int(s)
    if n < lo or n > hi:
        return None, True
    return n, False


def to_int32(v: Any) -> Result:
    return _int(v, INT32_MIN, INT32_MAX)


def to_int64(v: Any) -> Result:
    return _int(v, INT64_MIN, INT64_MAX)


def to_float64(v: Any) -> Result:
    if v is None:
        return None, False
    if isinstance(v, bool):
        return None, True
    try:
        if isinstance(v, (int, float)):
            f = float(v)  # an int past the double range raises OverflowError
        else:
            s = str(v).strip()
            if not _FLOAT_RE.match(s):
                return None, True
            f = float(s)
    except (OverflowError, ValueError):
        return None, True
    if not math.isfinite(f):
        return None, True
    return f, False


def to_bool(v: Any) -> Result:
    if v is None:
        return None, False
    if isinstance(v, bool):
        return v, False
    if isinstance(v, int):
        if v in (0, 1):
            return bool(v), False
        return None, True
    s = str(v).strip().lower()
    if s in _TRUE:
        return True, False
    if s in _FALSE:
        return False, False
    return None, True


def _offset(text: str) -> timezone:
    if text in ("Z", "z"):
        return timezone.utc
    sign = -1 if text[0] == "-" else 1
    digits = text[1:].replace(":", "")
    hours = int(digits[:2])
    minutes = int(digits[2:4]) if len(digits) > 2 else 0
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def make_timestamp(fmt: Optional[str], tz_name: Optional[str]) -> Callable[[Any], Result]:
    """A timestamp coercer for one column's ``format`` / ``timezone``."""
    zone = None
    if tz_name:
        from zoneinfo import ZoneInfo

        zone = ZoneInfo(tz_name)

    def _finish(dt: datetime) -> Result:
        if dt.tzinfo is None:
            if zone is None:
                return None, True
            dt = dt.replace(tzinfo=zone)
        try:
            # Year 1 east of UTC or year 9999 west of it has no UTC instant.
            return dt.astimezone(timezone.utc), False
        except (OverflowError, ValueError):
            return None, True

    def _coerce(v: Any) -> Result:
        if v is None:
            return None, False
        if isinstance(v, datetime):
            return _finish(v)
        s = str(v).strip()
        if fmt is not None:
            try:
                return _finish(datetime.strptime(s, fmt))
            except ValueError:
                return None, True
        m = _ISO_RE.match(s)
        if not m:
            return None, True
        y, mo, d, h, mi, sec, frac, off = m.groups()
        micro = int((frac or "")[:6].ljust(6, "0")) if frac else 0
        try:
            dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(sec or 0), micro)
        except ValueError:
            return None, True
        if off:
            try:
                dt = dt.replace(tzinfo=_offset(off))
            except ValueError:
                return None, True
        return _finish(dt)

    return _coerce


SCALAR_COERCERS = {
    "string": to_string,
    "int32": to_int32,
    "int64": to_int64,
    "float64": to_float64,
    "bool": to_bool,
}


def coercer_for(scalar_type: str, fmt: Optional[str] = None, tz_name: Optional[str] = None) -> Callable[[Any], Result]:
    if scalar_type == "timestamp":
        return make_timestamp(fmt, tz_name)
    return SCALAR_COERCERS[scalar_type]
