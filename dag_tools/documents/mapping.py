"""The mapping document: what a deployment writes to turn documents into tables.

Everything format-specific lives in a mapping YAML the deployment supplies.
This module knows the *grammar* of that file and nothing about any particular
document format: no element, attribute, table or column name appears here,
not as a constant, not as a default.

Two passes, both at load:

  * **Shape**, by pydantic with ``extra="forbid"``, so a misspelled key fails
    naming the key's full path (``tables.t.columns.c.typ: extra fields not
    permitted``) rather than being silently ignored.
  * **Meaning**, by :func:`validate_mapping`, which collects every problem
    rather than raising on the first: an undefined ``$var``, a lookup into an
    index that is not defined *earlier*, a lookup whose ``mode`` disagrees
    with the column's declared type, a reserved column name, an
    ``on_unknown_path`` with no ``expected_paths`` to check against.

Pure: no Dagster import, no IO beyond reading the file in :func:`load_mapping`.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

SCALAR_TYPES = ("string", "int32", "int64", "float64", "bool", "timestamp")
LIST_STRUCT = "list<struct>"
FILE = "file"
_LIST_RE = re.compile(r"^list<(\w+)>$")

#: Columns the engine adds to every table. A mapping may not declare them.
SYSTEM_COLUMNS = ("doc_id", "row_id", "source_bundle", "source_file", "publish_period", "ingested_at")
#: The table every bad document is written to. A mapping may not declare it.
REJECTS_TABLE = "_rejects"
#: Suffixes a ``file`` column emits beside itself.
FILE_SUFFIXES = ("_ref", "_member", "_resolution", "_bytes", "_sha256")
PROBE_SUFFIXES = ("_format", "_width", "_height")

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VAR_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_.\-]*)")
_TEMPLATE_FIELD_RE = re.compile(r"\{([^{}]*)\}")
RESOLVE_TEMPLATE_FIELDS = ("document_stem", "document_name")


class MappingError(ValueError):
    """A mapping that cannot be used. The message lists every problem found."""

    def __init__(self, problems: List[str]):
        self.problems = list(problems)
        super().__init__("invalid mapping:\n  - " + "\n  - ".join(self.problems))


def referenced_vars(expr: str) -> List[str]:
    """The ``$name`` variables an expression refers to, in order, once each."""
    seen: List[str] = []
    for name in _VAR_RE.findall(expr or ""):
        if name not in seen:
            seen.append(name)
    return seen


def template_fields(template: str) -> List[str]:
    return _TEMPLATE_FIELD_RE.findall(template)


def map_key(value: Any) -> str:
    """Map keys are compared as strings; YAML may hand over ints or bools."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


# ------------------------------------------------------------------ blocks


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class MapSpec(_Strict):
    """Translate a value through a table. Keys are compared as strings.

    A value missing from ``values`` takes ``default`` when one is written --
    even ``default: null`` -- and is otherwise null **and counted**, so an
    unfamiliar code is visible rather than silently becoming nothing.
    ``null`` is what an absent value becomes, when written.
    """

    values: Dict[Any, Any] = Field(default_factory=dict)
    default: Any = None
    null: Any = None

    @field_validator("values", mode="after")
    @classmethod
    def _stringify_keys(cls, v: Dict[Any, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, val in v.items():
            out[map_key(k)] = val
        return out

    @property
    def has_default(self) -> bool:
        return "default" in self.model_fields_set

    @property
    def has_null(self) -> bool:
        return "null" in self.model_fields_set


class LookupSpec(_Strict):
    """Look values up in an index built earlier from the same document."""

    index: str
    keys: str = Field(description="Expression, relative to the row, whose node-set is the keys looked up.")
    field: Optional[str] = None
    mode: Literal["exists", "first", "last", "list", "count"] = "first"
    distinct: bool = False
    sort: bool = False
    scope: Optional[str] = Field(
        default=None,
        description="Scalar, relative to the row: only index entries filed under the same scope string match. "
        "Required for a scoped index and refused for an unscoped one.",
    )
    order: Literal["document", "keys"] = Field(
        default="document",
        description="`document`: matched entries in document order. `keys`: in the order of the looked-up keys, "
        "each key's entries in document order, an entry matched twice kept at its first place.",
    )


class BasenameUnder(_Strict):
    basename_under: str


class BasenameIn(_Strict):
    """The reference's file name, looked for in exactly one folder and never below it."""

    basename_in: Literal["document_dir", "bundle_root"]


ResolveStrategy = Union[Literal["document_dir", "bundle_root"], BasenameUnder, BasenameIn]

# What a member table's column may read off the member it describes.
MEMBER_ATTRS = ("path", "name", "stem", "ext", "dir", "size", "modified", "self")
ROOT_LEVEL = "root"


class RowLevel(_Strict):
    """One named level of a ``rows`` chain: ``each`` is evaluated on every node of the level above."""

    as_: str = Field(alias="as")
    each: str


def _check_nulls(v: Optional[List[str]]) -> Optional[List[str]]:
    """``nulls`` entries are compared after trimming, so they are stored trimmed."""
    if v is None:
        return v
    out = []
    for i, s in enumerate(v):
        t = s.strip()
        if t == "":
            raise ValueError(
                f"entry {i} is blank; an empty value is already absent (see `empty_as_null`)"
            )
        out.append(t)
    return out


class ColumnSpec(_Strict):
    """One output column. Expressions are relative to the row node, or to the level named by ``from``."""

    type: Optional[str] = None
    expr: Optional[str] = None
    from_: Optional[str] = Field(
        default=None,
        alias="from",
        description="The `rows` level the column's expressions are evaluated against; `root` is the document.",
    )
    index_of: Optional[str] = Field(
        default=None, description="A `rows` level: the 0-based position of that level's node in its `each` result."
    )
    member: Optional[Literal["path", "name", "stem", "ext", "dir", "size", "modified", "self"]] = Field(
        default=None, description="Member tables only: an attribute of the member the row describes."
    )
    split: Optional[str] = Field(default=None, description="Literal separator; the value becomes the trimmed parts.")
    keep_empty: bool = False
    extract: Optional[str] = Field(
        default=None,
        description="Regex; the value becomes group `value`, else group 1, else the whole match. No match is null.",
    )
    first_of: Optional[List[str]] = None
    const: Any = Field(
        default=None,
        description="A fixed value for every row, typed by `type`. Takes no expression and no value step.",
    )
    prefer: Optional[List[Any]] = None
    when_empty: Any = None
    empty_as_null: bool = True
    nulls: Optional[List[str]] = Field(
        default=None,
        description="Strings that mean absent: a value equal to one, after trimming, is null before "
        "split, extract, map and coercion. Unset inherits `document.nulls`; `[]` turns it off.",
    )
    map: Optional[MapSpec] = None
    format: Optional[str] = None
    timezone: Optional[str] = None
    lookup: Optional[LookupSpec] = None
    # list<struct>
    rows: Optional[str] = None
    fields: Optional[Dict[str, Union[str, "ColumnSpec"]]] = None
    # file
    resolve: Optional[List[ResolveStrategy]] = None
    probe: Optional[Literal["image"]] = None
    store: Literal["blob", "reference"] = "blob"

    @property
    def has_when_empty(self) -> bool:
        return "when_empty" in self.model_fields_set

    @property
    def has_const(self) -> bool:
        """``const`` was written, even as ``null``."""
        return "const" in self.model_fields_set

    @field_validator("nulls")
    @classmethod
    def _nulls_trimmed(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        return _check_nulls(v)

    @field_validator("extract")
    @classmethod
    def _extract_compiles(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"extract regex does not compile: {e}") from e
        return v


ColumnSpec.model_rebuild()


def compile_glob(glob: str) -> "re.Pattern[str]":
    """Compile one ``document.match`` glob into a regex over a member path.

    Path-aware, because a bundle is a tree and ``fnmatch`` is not: there a
    ``*`` crosses ``/``, so ``*.xml`` matched every XML document at every
    depth and the example's second glob, ``*/*.xml``, was a no-op that read
    as a restriction. Here:

      * ``*`` matches any run of characters within ONE path segment;
      * ``?`` matches one character other than ``/``;
      * ``[abc]`` / ``[!abc]`` is a character class, never matching ``/``;
      * ``**`` as a whole segment matches zero or more segments, so
        ``**/*.xml`` is every XML document at any depth;
      * matching is case-sensitive and anchored at both ends.

    Refused rather than guessed: an empty glob, a leading ``/`` (member paths
    are relative to the bundle root, so it could never match), a backslash
    (member paths use ``/``; a Windows-style glob would silently match
    nothing), ``**`` sharing a segment with anything else, and an unclosed
    ``[``.
    """
    if not glob:
        raise ValueError("a glob may not be empty")
    if glob.startswith("/"):
        raise ValueError(f"glob {glob!r} starts with '/'; member paths are relative to the bundle root")
    if "\\" in glob:
        raise ValueError(f"glob {glob!r} contains a backslash; member paths are separated by '/'")
    segments = glob.split("/")
    out: List[str] = []
    for i, seg in enumerate(segments):
        last = i == len(segments) - 1
        if seg == "**":
            # zero or more whole segments; the trailing separator is part of it
            out.append(".*" if last else "(?:[^/]*/)*")
            continue
        if "**" in seg:
            raise ValueError(f"glob {glob!r}: '**' must be a whole path segment")
        j, n, part = 0, len(seg), []
        while j < n:
            c = seg[j]
            if c == "*":
                part.append("[^/]*")
            elif c == "?":
                part.append("[^/]")
            elif c == "[":
                k = j + 1
                if k < n and seg[k] == "!":
                    k += 1
                if k < n and seg[k] == "]":
                    k += 1
                while k < n and seg[k] != "]":
                    k += 1
                if k >= n:
                    raise ValueError(f"glob {glob!r}: unclosed '['")
                body = seg[j + 1 : k]
                neg = body.startswith("!")
                if neg:
                    body = body[1:]
                body = body.replace("^", r"\^")
                part.append(f"[^/{body}]" if neg else f"(?!/)[{body}]")
                j = k
            else:
                part.append(re.escape(c))
            j += 1
        out.append("".join(part) + ("" if last else "/"))
    return re.compile("".join(out) + r"\Z")


def glob_matcher(globs: List[str]):
    """One predicate over a member path for a whole ``document.match`` list."""
    patterns = [compile_glob(g) for g in globs]
    return lambda path: any(p.match(path) for p in patterns)


def _check_globs(v: List[str]) -> List[str]:
    problems = []
    for g in v:
        try:
            compile_glob(g)
        except ValueError as e:
            problems.append(str(e))
    if problems:
        raise ValueError("; ".join(problems))
    return v


class DocumentBlock(_Strict):
    format: Literal["xml", "json"] = Field(
        default="xml", description="`xml` (XPath expressions) or `json` (JMESPath expressions)."
    )
    match: List[str] = Field(
        min_length=1,
        description="Path-aware globs on the member path (`*` stays within a segment, `**` spans segments); "
        "a member matching none is not a document.",
    )
    strip_namespaces: bool = False
    max_bytes: int = Field(default=64 * 1024 * 1024, gt=0)
    huge_tree: bool = Field(
        default=False,
        description="Lift the XML parser's built-in size and depth limits. `max_bytes` is enforced either way.",
    )
    nulls: List[str] = Field(
        default_factory=list,
        description="The default `nulls` of every column: strings that mean absent, compared after trimming.",
    )

    @field_validator("nulls")
    @classmethod
    def _nulls_trimmed(cls, v: List[str]) -> List[str]:
        return _check_nulls(v) or []

    @field_validator("match")
    @classmethod
    def _globs_compile(cls, v: List[str]) -> List[str]:
        return _check_globs(v)

    @model_validator(mode="after")
    def _xml_options_are_xml_only(self) -> "DocumentBlock":
        if self.format == "json":
            set_ = [f for f in ("strip_namespaces", "huge_tree") if f in self.model_fields_set]
            if set_:
                raise ValueError(f"{' and '.join(set_)} apply only to format: xml")
        return self


class PublishPeriodSpec(_Strict):
    from_: Literal["bundle_name", "bundle_key"] = Field(alias="from")
    regex: str

    @field_validator("regex")
    @classmethod
    def _compiles(cls, v: str) -> str:
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"regex does not compile: {e}") from e
        return v


class LineageBlock(_Strict):
    publish_period: Optional[PublishPeriodSpec] = None


RowsSpec = Union[str, List[RowLevel]]


class IndexSpec(_Strict):
    rows: RowsSpec
    keys: str = Field(description="Expression, relative to the index row; the entry is filed under every key in its node-set.")
    scope: Optional[str] = Field(
        default=None,
        description="Scalar, relative to the index row. When set, entries are filed under (scope, key) and a lookup "
        "matches only entries whose scope equals its own.",
    )
    from_: Optional[str] = Field(
        default=None, alias="from", description="The `rows` level `keys` and `scope` are evaluated against."
    )
    values: Dict[str, Union[str, ColumnSpec]] = Field(default_factory=dict)


class TableSpec(_Strict):
    rows: Optional[RowsSpec] = None
    members: Optional[List[str]] = Field(
        default=None, description="Globs on the member path: one row per matching member of the bundle."
    )
    vars: Dict[str, str] = Field(default_factory=dict)
    row_id: Optional[str] = None
    into: Optional[str] = Field(
        default=None,
        description="The dataset the rows are written to (`<into>.lance`); default the table's name. "
        "Tables sharing one must declare the same columns with the same types.",
    )
    only_unreferenced: bool = Field(
        default=False,
        description="Member tables only: keep only members no `file` column of a document table "
        "resolved to in this bundle.",
    )
    columns: Dict[str, Union[str, ColumnSpec]] = Field(min_length=1)

    @field_validator("members")
    @classmethod
    def _member_globs_compile(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return v
        if not v:
            raise ValueError("must list at least one glob")
        return _check_globs(v)

    @model_validator(mode="after")
    def _rows_or_members(self) -> "TableSpec":
        if (self.rows is None) == (self.members is None):
            raise ValueError("set exactly one of `rows` (rows from a document) or `members` (one row per member)")
        if isinstance(self.rows, list) and not self.rows:
            raise ValueError("a `rows` chain needs at least one level")
        return self


def chain_levels(rows: Optional[RowsSpec]) -> Optional[List[str]]:
    """The level names a ``rows`` chain defines, or ``None`` for a plain-string ``rows``."""
    if isinstance(rows, list):
        return [lv.as_ for lv in rows]
    return None


class MappingDoc(_Strict):
    version: Literal[1]
    document: DocumentBlock
    lineage: LineageBlock = Field(default_factory=LineageBlock)
    expected_paths: List[str] = Field(default_factory=list)
    on_unknown_path: Literal["ignore", "warn", "fail"] = "warn"
    indexes: Dict[str, IndexSpec] = Field(default_factory=dict)
    tables: Dict[str, TableSpec] = Field(min_length=1)


# ------------------------------------------------------------------ resolved types


def as_spec(spec: Union[str, ColumnSpec]) -> ColumnSpec:
    """``name: <expr>`` is shorthand for a string column with that expression."""
    return ColumnSpec(expr=spec) if isinstance(spec, str) else spec


def lookup_type(mode: str, field_type: Optional[str]) -> Optional[str]:
    if mode == "exists":
        return "bool"
    if mode == "count":
        return "int64"
    if field_type is None:
        return None
    if mode == "list":
        return f"list<{field_type}>"
    return field_type


def parse_type(t: str) -> Tuple[str, Optional[str]]:
    """``(kind, element)``: kind is scalar/list/list_struct/file."""
    if t in SCALAR_TYPES:
        return "scalar", None
    if t == LIST_STRUCT:
        return "list_struct", None
    if t == FILE:
        return "file", None
    m = _LIST_RE.match(t)
    if m and m.group(1) in SCALAR_TYPES:
        return "list", m.group(1)
    raise ValueError(t)


# ------------------------------------------------------------------ validation


class _Problems:
    def __init__(self) -> None:
        self.items: List[str] = []

    def add(self, where: str, msg: str) -> None:
        self.items.append(f"{where}: {msg}")


# XPath 1.0 functions whose result is never a node-set. An expression that is
# nothing but a call to one of them cannot give `prefer` a list to choose from.
_SCALAR_FUNCS = frozenset(
    ["string", "concat", "count", "number", "boolean", "normalize-space", "substring", "substring-before", "substring-after", "translate", "sum", "contains", "starts-with", "string-length", "name", "local-name", "namespace-uri", "not", "true", "false", "round", "floor", "ceiling", "lang"]
)
_CALL_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9\-]*)\s*\(")


def statically_scalar(expr: str) -> bool:
    """True when ``expr`` is one call to a function that never returns nodes."""
    m = _CALL_RE.match(expr)
    if not m or m.group(1) not in _SCALAR_FUNCS:
        return False
    depth = 0
    quote = ""
    for i, ch in enumerate(expr):
        if quote:
            if ch == quote:
                quote = ""
        elif ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return expr[i + 1 :].strip() == ""
    return False


def resolved_column_type(
    spec: ColumnSpec,
    index_types: Dict[str, Dict[str, str]],
    where: str,
    problems: _Problems,
    scoped: Optional[Set[str]] = None,
) -> Optional[str]:
    """The column's type, inferred from a lookup when not declared.

    ``scoped`` names the indexes that declare a ``scope``. When it is given, a
    lookup's ``scope`` is checked against it; validation passes it, while the
    engine, which only wants the type, does not.
    """
    declared = spec.type
    if spec.lookup is not None:
        lk = spec.lookup
        fields = index_types.get(lk.index)
        if fields is None:
            problems.add(f"{where}.lookup.index", f"index {lk.index!r} is not defined before this point")
            return declared
        ftype: Optional[str] = None
        if lk.field is not None:
            if lk.field not in fields:
                problems.add(f"{where}.lookup.field", f"index {lk.index!r} has no value {lk.field!r}")
                return declared
            ftype = fields[lk.field]
        elif lk.mode in ("first", "last", "list"):
            problems.add(f"{where}.lookup.field", f"mode {lk.mode!r} needs a `field` to return")
            return declared
        if scoped is not None:
            if lk.index in scoped and lk.scope is None:
                problems.add(
                    f"{where}.lookup.scope",
                    f"index {lk.index!r} is scoped; a lookup into it must give `scope`",
                )
            elif lk.index not in scoped and lk.scope is not None:
                problems.add(
                    f"{where}.lookup.scope",
                    f"index {lk.index!r} is not scoped; a lookup into it takes no `scope`",
                )
        if lk.order == "keys" and lk.mode in ("exists", "count"):
            problems.add(f"{where}.lookup.order", f"`order` has nothing to order in mode {lk.mode!r}")
        inferred = lookup_type(lk.mode, ftype)
        picks_one = spec.prefer is not None and lk.mode == "list"
        if spec.prefer is not None and not picks_one:
            problems.add(
                f"{where}.prefer",
                f"`prefer` needs a list source; lookup mode {lk.mode!r} returns one value",
            )
        if picks_one:
            inferred = ftype  # prefer picks one element of the list
        if (lk.distinct or lk.sort) and lk.mode not in ("list", "count"):
            problems.add(f"{where}.lookup", "`distinct`/`sort` apply only to modes `list` and `count`")
        if declared is not None and inferred is not None and declared != inferred and spec.map is None:
            problems.add(
                f"{where}.type",
                f"declared {declared!r} but lookup mode {lk.mode!r}"
                + (" with `prefer`" if picks_one else "")
                + f" returns {inferred!r}",
            )
        return declared or inferred
    if spec.index_of is not None or spec.member == "size":
        return declared or "int64"
    if spec.member == "modified":
        return declared or "timestamp"
    if spec.member == "self":
        return declared or FILE
    return declared or "string"


def _check_value_column(
    spec: ColumnSpec,
    where: str,
    problems: _Problems,
    index_types: Dict[str, Dict[str, str]],
    defined_vars: List[str],
    *,
    allow_file: bool,
    allow_struct: bool,
    scoped: Optional[Set[str]] = None,
    levels: Optional[List[str]] = None,
    member_table: bool = False,
    in_struct: bool = False,
    fmt: str = "xml",
) -> Optional[str]:
    """Check one column; ``levels`` are the table's chain levels (``None``: ``rows`` is one expression)."""
    scoped = set() if scoped is None else scoped
    sources = [x for x in (spec.expr, spec.first_of, spec.lookup, spec.index_of, spec.member) if x is not None]
    if spec.has_const:
        sources.append("const")  # counted even when the constant is null
    t = resolved_column_type(spec, index_types, where, problems, scoped)
    _check_levels(spec, where, problems, levels, member_table=member_table, in_struct=in_struct)
    if fmt == "json":
        defined_vars = []  # JMESPath has no variables; a `$` in an expression is a compile error there
    try:
        kind, elem = parse_type(t or "string")
    except ValueError:
        problems.add(f"{where}.type", f"unknown type {t!r}; one of {', '.join(SCALAR_TYPES)}, list<T>, list<struct>, file")
        return t

    if kind == "list_struct":
        if not allow_struct:
            problems.add(f"{where}.type", "list<struct> is not allowed here")
        if spec.rows is None or not spec.fields:
            problems.add(where, "list<struct> needs `rows` and `fields`")
        if sources:
            problems.add(where, "list<struct> takes `rows` + `fields`, not `expr`/`first_of`/`lookup`/`const`")
        for name, f in (spec.fields or {}).items():
            fspec = as_spec(f)
            fw = f"{where}.fields.{name}"
            if not _IDENT_RE.match(name):
                problems.add(fw, "a field name must be an identifier")
            _check_value_column(
                fspec, fw, problems, index_types, defined_vars, allow_file=False, allow_struct=False, scoped=scoped,
                levels=levels, in_struct=True, fmt=fmt,
            )
        if spec.split is not None or spec.extract is not None:
            problems.add(where, "`split`/`extract` apply to values, not to a list<struct>")
        if fmt == "xml":
            _check_vars(spec.rows or "", f"{where}.rows", defined_vars, problems)
        return t
    if spec.rows is not None or spec.fields is not None:
        problems.add(where, "`rows`/`fields` apply only to list<struct>")

    self_file = spec.member == "self"
    if len(sources) != 1:
        problems.add(where, "set exactly one of `expr`, `first_of`, `lookup`, `index_of`, `member` or `const`")
    if spec.has_const:
        # A constant is the value as written: no step that reads or reshapes a
        # source value applies to it. `type`, `format` and `timezone` still do.
        steps = {"from_": "from", "split": "split", "keep_empty": "keep_empty", "extract": "extract",
                 "map": "map", "nulls": "nulls", "prefer": "prefer", "when_empty": "when_empty",
                 "empty_as_null": "empty_as_null"}
        clash = [shown for f, shown in steps.items() if f in spec.model_fields_set]
        if clash:
            problems.add(
                f"{where}.const",
                "a constant is written as it is stored; " + ", ".join(f"`{c}`" for c in clash) + " do not apply",
            )
        if kind == "file":
            problems.add(f"{where}.const", "a file column reads a member; it has no constant")
    if spec.first_of is not None and not spec.first_of:
        problems.add(f"{where}.first_of", "must list at least one expression")
    if fmt == "xml":
        for e in ([spec.expr] if spec.expr else []) + list(spec.first_of or []):
            _check_vars(e, where, defined_vars, problems)
        if spec.lookup is not None:
            _check_vars(spec.lookup.keys, f"{where}.lookup.keys", defined_vars, problems)
            if spec.lookup.scope is not None:
                _check_vars(spec.lookup.scope, f"{where}.lookup.scope", defined_vars, problems)

    if spec.split is not None:
        sw = f"{where}.split"
        if spec.split == "":
            problems.add(sw, "the separator may not be empty")
        if spec.lookup is not None:
            problems.add(sw, "`split` takes one text value; a lookup already returns a list")
        if spec.index_of is not None:
            problems.add(sw, "`index_of` is a position, not text to split")
        if kind == "file":
            problems.add(sw, "a file column cannot be split")
        elif kind == "scalar" and spec.prefer is None:
            problems.add(sw, f"`split` yields a list; declare `list<{t}>`, or add `prefer` to pick one part")
    elif "keep_empty" in spec.model_fields_set:
        problems.add(f"{where}.keep_empty", "`keep_empty` applies only with `split`")
    if spec.nulls is not None and spec.index_of is not None:
        problems.add(f"{where}.nulls", "`index_of` is a position; it is never absent")
    if spec.extract is not None and kind == "file":
        problems.add(f"{where}.extract", "a file column takes its reference as written")

    if spec.prefer is not None:
        pw = f"{where}.prefer"
        if not spec.prefer:
            problems.add(pw, "must list at least one value")
        for i, v in enumerate(spec.prefer):
            if v is None or isinstance(v, (dict, list)):
                problems.add(f"{pw}[{i}]", "a preferred value is a scalar, compared as a string")
        if spec.first_of is not None:
            problems.add(
                where,
                "`prefer` and `first_of` cannot be combined: `first_of` chooses among expressions, "
                "`prefer` among the values of one list",
            )
        if kind != "scalar":
            problems.add(pw, f"`prefer` yields one value; declare the element type, not {t!r}")
        if fmt == "xml" and spec.split is None and spec.expr is not None and statically_scalar(spec.expr):
            problems.add(pw, "`prefer` needs a list source; this expression returns one value")
        if spec.index_of is not None:
            problems.add(pw, "`prefer` needs a list source; `index_of` gives one value")
        elif spec.member is not None and spec.split is None:
            problems.add(pw, "`prefer` needs a list source; `member` gives one value unless it is `split`")

    if kind == "file":
        if not allow_file:
            problems.add(f"{where}.type", "a file column is not allowed here")
        if self_file:
            if spec.resolve is not None:
                problems.add(f"{where}.resolve", "`member: self` stores the member itself; there is nothing to resolve")
        elif spec.member is not None:
            problems.add(f"{where}.member", "a file column reads `member: self`")
        elif spec.expr is None:
            problems.add(where, "a file column needs `expr` (the reference text)")
        if not spec.resolve and not self_file:
            problems.add(f"{where}.resolve", "a file column needs at least one resolve strategy")
        for i, strat in enumerate(spec.resolve or []):
            if isinstance(strat, BasenameUnder):
                for fld in template_fields(strat.basename_under):
                    if fld not in RESOLVE_TEMPLATE_FIELDS:
                        problems.add(
                            f"{where}.resolve[{i}].basename_under",
                            f"unknown placeholder {{{fld}}}; allowed: "
                            + ", ".join("{" + f + "}" for f in RESOLVE_TEMPLATE_FIELDS),
                        )
                stripped = _TEMPLATE_FIELD_RE.sub("", strat.basename_under)
                if "{" in stripped or "}" in stripped:
                    problems.add(f"{where}.resolve[{i}].basename_under", "unbalanced brace")
        if spec.map is not None or spec.lookup is not None or spec.first_of is not None or spec.index_of is not None:
            problems.add(where, "a file column takes `expr` or `member: self` only (no map, lookup, first_of or index_of)")
    else:
        if self_file:
            problems.add(f"{where}.member", "`member: self` stores bytes; declare `type: file`")
        if spec.resolve is not None or spec.probe is not None or "store" in spec.model_fields_set:
            problems.add(where, "`resolve`/`probe`/`store` apply only to file columns")

    if spec.lookup is not None and spec.lookup.mode in ("list",) and kind != "list" and spec.map is None:
        pass  # reported by resolved_column_type
    scalar = elem if kind == "list" else (t if kind == "scalar" else None)
    if scalar == "timestamp":
        if spec.timezone is not None:
            try:
                from zoneinfo import ZoneInfo

                ZoneInfo(spec.timezone)
            except (KeyError, ValueError, OSError):  # ZoneInfoNotFoundError is a KeyError
                problems.add(f"{where}.timezone", f"unknown time zone {spec.timezone!r}")
    elif spec.format is not None or spec.timezone is not None:
        problems.add(where, "`format`/`timezone` apply only to timestamp columns")
    return t


def _check_levels(
    spec: ColumnSpec,
    where: str,
    problems: _Problems,
    levels: Optional[List[str]],
    *,
    member_table: bool,
    in_struct: bool,
) -> None:
    def known(name: str) -> str:
        return ", ".join([ROOT_LEVEL] + list(levels or []))

    if spec.from_ is not None:
        fw = f"{where}.from"
        if in_struct:
            problems.add(fw, "a list<struct> field is evaluated on its struct row; `from` applies to the column")
        elif member_table:
            problems.add(fw, "a member table has no document to evaluate against")
        elif spec.from_ == ROOT_LEVEL:
            pass
        elif levels is None:
            problems.add(fw, f"`from: {spec.from_}` needs a `rows` chain; this `rows` is one expression, so only `root` is available")
        elif spec.from_ not in levels:
            problems.add(fw, f"unknown level {spec.from_!r}; the levels here are {known(spec.from_)}")
    if spec.index_of is not None:
        iw = f"{where}.index_of"
        if in_struct:
            problems.add(iw, "a list<struct> field cannot take `index_of`")
        elif levels is None:
            problems.add(iw, "`index_of` needs a `rows` chain")
        elif spec.index_of not in levels:
            problems.add(iw, f"unknown level {spec.index_of!r}; `index_of` names a level of the chain: {', '.join(levels)}")
    if spec.member is not None and not member_table:
        problems.add(f"{where}.member", "`member` applies only to a table built from `members`")
    if member_table:
        other = [n for n, v in (("expr", spec.expr), ("first_of", spec.first_of), ("lookup", spec.lookup)) if v is not None]
        if other:
            problems.add(where, f"a member table has no document; read `member:` instead of {', '.join(other)}")


def _check_rows(rows: Any, where: str, problems: _Problems, fmt: str) -> Optional[List[str]]:
    levels = chain_levels(rows)
    if levels is None:
        if fmt == "xml" and isinstance(rows, str):
            _check_vars(rows, where, [], problems)
        return None
    seen: Set[str] = set()
    for i, lv in enumerate(rows):
        lw = f"{where}[{i}]"
        if lv.as_ == ROOT_LEVEL:
            problems.add(f"{lw}.as", "`root` is the document and is always available; name the level something else")
        elif not _IDENT_RE.match(lv.as_):
            problems.add(f"{lw}.as", "a level name must be an identifier")
        elif lv.as_ in seen:
            problems.add(f"{lw}.as", f"level {lv.as_!r} is named twice")
        seen.add(lv.as_)
        if fmt == "xml":
            _check_vars(lv.each, f"{lw}.each", [], problems)
    return levels


def _check_vars(expr: str, where: str, defined: List[str], problems: _Problems) -> None:
    for name in referenced_vars(expr):
        if name not in defined:
            problems.add(where, f"${name} is not defined in this table's `vars`")


def validate_mapping(doc: MappingDoc) -> None:
    """Raise :class:`MappingError` listing every semantic problem at once."""
    p = _Problems()

    if doc.on_unknown_path != "warn" and not doc.expected_paths:
        p.add("on_unknown_path", f"set to {doc.on_unknown_path!r} with no `expected_paths` to check against")
    fmt = doc.document.format
    lead = "$" if fmt == "json" else "/"
    for i, path in enumerate(doc.expected_paths):
        if not path.startswith(lead):
            p.add(f"expected_paths[{i}]", f"a structural path of a {fmt} document starts with {lead!r}")

    index_types: Dict[str, Dict[str, str]] = {}
    scoped: Set[str] = set()
    for iname, ix in doc.indexes.items():
        where = f"indexes.{iname}"
        if not _IDENT_RE.match(iname):
            p.add(where, "an index name must be an identifier")
        each = [ix.rows] if isinstance(ix.rows, str) else [lv.each for lv in ix.rows]
        if fmt == "xml":
            for e, w in [(e, "rows") for e in each] + [(ix.keys, "keys"), (ix.scope, "scope")]:
                for v in referenced_vars(e or ""):
                    p.add(f"{where}.{w}", f"${v}: indexes take no vars")
        ilevels = _check_rows(ix.rows, f"{where}.rows", p, "json")  # level names only; vars are refused above
        if ix.from_ is not None and ix.from_ != ROOT_LEVEL:
            if ilevels is None:
                p.add(f"{where}.from", f"`from: {ix.from_}` needs a `rows` chain; only `root` is available")
            elif ix.from_ not in ilevels:
                p.add(f"{where}.from", f"unknown level {ix.from_!r}; the levels here are {', '.join([ROOT_LEVEL] + ilevels)}")
        types: Dict[str, str] = {}
        for vname, v in ix.values.items():
            vspec = as_spec(v)
            vw = f"{where}.values.{vname}"
            t = _check_value_column(
                vspec, vw, p, index_types, [], allow_file=False, allow_struct=False, scoped=scoped,
                levels=ilevels, fmt=fmt,
            )
            types[vname] = t or "string"
        index_types[iname] = types  # defined only now: no lookup into itself or a later index
        if ix.scope is not None:
            scoped.add(iname)

    for tname, tb in doc.tables.items():
        where = f"tables.{tname}"
        if tname == REJECTS_TABLE:
            p.add(where, f"{REJECTS_TABLE!r} is reserved for documents that could not be read")
        elif not _IDENT_RE.match(tname):
            p.add(where, "a table name must be an identifier")
        member_table = tb.members is not None
        if tb.vars and (fmt == "json" or member_table):
            p.add(
                f"{where}.vars",
                "a member table has no document to read vars from" if member_table
                else "vars are XPath variables; a JSON mapping reaches other levels with `from:`",
            )
        if tb.into is not None:
            if tb.into == REJECTS_TABLE:
                p.add(f"{where}.into", f"{REJECTS_TABLE!r} is reserved for documents that could not be read")
            elif not _IDENT_RE.match(tb.into):
                p.add(f"{where}.into", "a dataset name must be an identifier")
            elif tb.into != tname and tb.into in doc.tables and (doc.tables[tb.into].into or tb.into) != tb.into:
                p.add(
                    f"{where}.into",
                    f"{tb.into!r} is a table that writes to {doc.tables[tb.into].into!r}; "
                    "a dataset named after a table is that table's own",
                )
        if tb.only_unreferenced and not member_table:
            p.add(
                f"{where}.only_unreferenced",
                "applies to a member table: it keeps the members no document's file column resolved to",
            )
        defined: List[str] = []
        for vname, vexpr in tb.vars.items():
            if not _IDENT_RE.match(vname):
                p.add(f"{where}.vars.{vname}", "a var name must be an identifier")
            if fmt == "xml":
                _check_vars(vexpr, f"{where}.vars.{vname}", defined, p)
            defined.append(vname)
        levels = _check_rows(tb.rows, f"{where}.rows", p, fmt) if not member_table else None
        emitted: List[str] = []
        for cname, c in tb.columns.items():
            cw = f"{where}.columns.{cname}"
            cspec = as_spec(c)
            if not _IDENT_RE.match(cname):
                p.add(cw, "a column name must be an identifier")
            if cname in SYSTEM_COLUMNS:
                p.add(cw, f"{cname!r} is added to every table and cannot be declared")
            t = _check_value_column(
                cspec, cw, p, index_types, defined, allow_file=True, allow_struct=not member_table, scoped=scoped,
                levels=levels, member_table=member_table, fmt=fmt,
            )
            emitted.append(cname)
            if t == FILE:
                emitted.extend(cname + s for s in FILE_SUFFIXES)
                if cspec.probe == "image":
                    emitted.extend(cname + s for s in PROBE_SUFFIXES)
        seen = set()
        for name in emitted:
            if name in seen or (name in SYSTEM_COLUMNS and name not in tb.columns):
                p.add(where, f"column {name!r} is produced twice (a file column's companions collide)")
            seen.add(name)
        if tb.row_id is not None:
            fields = template_fields(tb.row_id)
            if not fields:
                p.add(f"{where}.row_id", "a row_id template names at least one {field}")
            for f in fields:
                if f not in tb.columns and f not in ("doc_id", "row_number"):
                    p.add(f"{where}.row_id", f"{{{f}}} is not a column of this table, `doc_id` or `row_number`")
    if p.items:
        raise MappingError(p.items)


def _format_validation_error(e: ValidationError) -> List[str]:
    out = []
    for err in e.errors():
        loc = ".".join(str(x) for x in err.get("loc", ()) if not str(x).startswith(("function-", "union[")))
        out.append(f"{loc or '<root>'}: {err.get('msg')}")
    return out


def parse_mapping(data: Any) -> MappingDoc:
    """Validate an already-loaded mapping dict, shape then meaning."""
    if not isinstance(data, dict):
        raise MappingError(["<root>: a mapping is a YAML mapping (key: value), not " + type(data).__name__])
    try:
        doc = MappingDoc.model_validate(data)
    except ValidationError as e:
        raise MappingError(_format_validation_error(e)) from None
    validate_mapping(doc)
    return doc


def load_mapping(source: Union[str, Path, Dict[str, Any]]) -> MappingDoc:
    """A mapping from a path to a YAML file, or from an inline dict."""
    if isinstance(source, dict):
        return parse_mapping(source)
    path = Path(source)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise MappingError([f"<file>: cannot read mapping {path}: {e.strerror or e}"]) from None
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise MappingError([f"<file>: {path} is not valid YAML: {e}"]) from None
    return parse_mapping(data)
