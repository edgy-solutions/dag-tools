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
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

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


ResolveStrategy = Union[Literal["document_dir", "bundle_root"], BasenameUnder]


class ColumnSpec(_Strict):
    """One output column. Expressions are relative to the row node."""

    type: Optional[str] = None
    expr: Optional[str] = None
    first_of: Optional[List[str]] = None
    prefer: Optional[List[Any]] = None
    when_empty: Any = None
    empty_as_null: bool = True
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


class DocumentBlock(_Strict):
    format: Literal["xml"] = "xml"
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

    @field_validator("match")
    @classmethod
    def _globs_compile(cls, v: List[str]) -> List[str]:
        problems = []
        for g in v:
            try:
                compile_glob(g)
            except ValueError as e:
                problems.append(str(e))
        if problems:
            raise ValueError("; ".join(problems))
        return v


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


class IndexSpec(_Strict):
    rows: str
    keys: str = Field(description="Expression, relative to the index row; the entry is filed under every key in its node-set.")
    scope: Optional[str] = Field(
        default=None,
        description="Scalar, relative to the index row. When set, entries are filed under (scope, key) and a lookup "
        "matches only entries whose scope equals its own.",
    )
    values: Dict[str, Union[str, ColumnSpec]] = Field(default_factory=dict)


class TableSpec(_Strict):
    rows: str
    vars: Dict[str, str] = Field(default_factory=dict)
    row_id: Optional[str] = None
    columns: Dict[str, Union[str, ColumnSpec]] = Field(min_length=1)


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
) -> Optional[str]:
    scoped = set() if scoped is None else scoped
    sources = [x for x in (spec.expr, spec.first_of, spec.lookup) if x is not None]
    t = resolved_column_type(spec, index_types, where, problems, scoped)
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
            problems.add(where, "list<struct> takes `rows` + `fields`, not `expr`/`first_of`/`lookup`")
        for name, f in (spec.fields or {}).items():
            fspec = as_spec(f)
            fw = f"{where}.fields.{name}"
            if not _IDENT_RE.match(name):
                problems.add(fw, "a field name must be an identifier")
            _check_value_column(
                fspec, fw, problems, index_types, defined_vars, allow_file=False, allow_struct=False, scoped=scoped
            )
        _check_vars(spec.rows or "", f"{where}.rows", defined_vars, problems)
        return t
    if spec.rows is not None or spec.fields is not None:
        problems.add(where, "`rows`/`fields` apply only to list<struct>")

    if len(sources) != 1:
        problems.add(where, "set exactly one of `expr`, `first_of` or `lookup`")
    if spec.first_of is not None and not spec.first_of:
        problems.add(f"{where}.first_of", "must list at least one expression")
    for e in ([spec.expr] if spec.expr else []) + list(spec.first_of or []):
        _check_vars(e, where, defined_vars, problems)
    if spec.lookup is not None:
        _check_vars(spec.lookup.keys, f"{where}.lookup.keys", defined_vars, problems)
        if spec.lookup.scope is not None:
            _check_vars(spec.lookup.scope, f"{where}.lookup.scope", defined_vars, problems)

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
        if spec.expr is not None and statically_scalar(spec.expr):
            problems.add(pw, "`prefer` needs a list source; this expression returns one value")

    if kind == "file":
        if not allow_file:
            problems.add(f"{where}.type", "a file column is not allowed here")
        if spec.expr is None:
            problems.add(where, "a file column needs `expr` (the reference text)")
        if not spec.resolve:
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
        if spec.map is not None or spec.lookup is not None or spec.first_of is not None:
            problems.add(where, "a file column takes `expr` only (no map, lookup or first_of)")
    else:
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


def _check_vars(expr: str, where: str, defined: List[str], problems: _Problems) -> None:
    for name in referenced_vars(expr):
        if name not in defined:
            problems.add(where, f"${name} is not defined in this table's `vars`")


def validate_mapping(doc: MappingDoc) -> None:
    """Raise :class:`MappingError` listing every semantic problem at once."""
    p = _Problems()

    if doc.on_unknown_path != "warn" and not doc.expected_paths:
        p.add("on_unknown_path", f"set to {doc.on_unknown_path!r} with no `expected_paths` to check against")
    for i, path in enumerate(doc.expected_paths):
        if not path.startswith("/"):
            p.add(f"expected_paths[{i}]", "a structural path starts with '/'")

    index_types: Dict[str, Dict[str, str]] = {}
    scoped: Set[str] = set()
    for iname, ix in doc.indexes.items():
        where = f"indexes.{iname}"
        if not _IDENT_RE.match(iname):
            p.add(where, "an index name must be an identifier")
        for e, w in ((ix.rows, "rows"), (ix.keys, "keys"), (ix.scope, "scope")):
            for v in referenced_vars(e or ""):
                p.add(f"{where}.{w}", f"${v}: indexes take no vars")
        types: Dict[str, str] = {}
        for vname, v in ix.values.items():
            vspec = as_spec(v)
            vw = f"{where}.values.{vname}"
            t = _check_value_column(vspec, vw, p, index_types, [], allow_file=False, allow_struct=False, scoped=scoped)
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
        defined: List[str] = []
        for vname, vexpr in tb.vars.items():
            if not _IDENT_RE.match(vname):
                p.add(f"{where}.vars.{vname}", "a var name must be an identifier")
            _check_vars(vexpr, f"{where}.vars.{vname}", defined, p)
            defined.append(vname)
        _check_vars(tb.rows, f"{where}.rows", [], p)
        emitted: List[str] = []
        for cname, c in tb.columns.items():
            cw = f"{where}.columns.{cname}"
            cspec = as_spec(c)
            if not _IDENT_RE.match(cname):
                p.add(cw, "a column name must be an identifier")
            if cname in SYSTEM_COLUMNS:
                p.add(cw, f"{cname!r} is added to every table and cannot be declared")
            t = _check_value_column(
                cspec, cw, p, index_types, defined, allow_file=True, allow_struct=True, scoped=scoped
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
