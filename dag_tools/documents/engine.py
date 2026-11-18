"""The mapping engine: one bundle in, Arrow batches per table out.

:func:`compile_mapping` turns a validated :class:`MappingDoc` into closures
once -- expressions compiled, coercers chosen, the Arrow schema of every
table fixed -- so an empty bundle still yields every table's exact schema.
:func:`run_bundle` then walks a bundle's documents in path order:

  1. read at most ``max_bytes`` + 1 bytes and parse with external
     resolution off;
  2. check the document's structure against ``expected_paths``;
  3. evaluate each table's ``vars`` once, from the root;
  4. build the indexes in declaration order, each entry filed under every
     key in its ``keys`` node-set;
  5. select each table's rows and evaluate its columns relative to the row.

A document is committed whole or not at all: anything that goes wrong in it
-- unparseable, too large, an expression that fails, ``on_unknown_path:
fail`` -- discards that document's rows from *every* table, writes one row to
``_rejects`` and the bundle carries on. Coercion problems are not document
problems: a value that cannot be read as its column's type is stored as null
and counted per column.

Pure: no Dagster import. ``pyarrow`` is required; Pillow only for
``probe: image``. Nothing here logs a value read from a document: logs name
bundles, members, counts and column names.
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from dag_tools.documents.coerce import coercer_for
from dag_tools.documents.files import MemberIndex, probe_image, require_pillow, resolve
from dag_tools.documents.mapping import (
    REJECTS_TABLE,
    ColumnSpec,
    MappingDoc,
    MappingError,
    _Problems,
    as_spec,
    glob_matcher,
    map_key,
    parse_type,
    resolved_column_type,
    template_fields,
)
from dag_tools.documents.reader import DocumentError, reader_for
from dag_tools.documents.sources import Member, OpenBundle

log = logging.getLogger(__name__)

BLOB_METADATA = {b"lance-encoding:blob": b"true"}

Sink = Callable[[str, Any], None]


def _pa() -> Any:
    try:
        import pyarrow as pa
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "the documents engine needs pyarrow; install the `documents` extra: "
            "pip install 'edgy-dag-tools[documents]'"
        ) from e
    return pa


def doc_id_for(bundle_key: str, member_path: str) -> str:
    """Stable across runs and machines: the bundle key and the member path."""
    return hashlib.sha256(f"{bundle_key}\0{member_path}".encode()).hexdigest()[:32]


def arrow_scalar(pa: Any, t: str) -> Any:
    return {
        "string": pa.string(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "float64": pa.float64(),
        "bool": pa.bool_(),
        "timestamp": pa.timestamp("us", tz="UTC"),
    }[t]


# ------------------------------------------------------------------ stats


@dataclass
class BundleStats:
    bundle: str
    members: int = 0
    documents: int = 0
    documents_parsed: int = 0
    documents_rejected: int = 0
    rows: Dict[str, int] = field(default_factory=dict)
    #: owner (table name, or ``indexes.<name>``) -> column -> count
    coercion_failures: Dict[str, Dict[str, int]] = field(default_factory=dict)
    unmapped_values: Dict[str, Dict[str, int]] = field(default_factory=dict)
    #: owner -> column -> lookups (or, under ``indexes.<name>`` / ``scope``, index
    #: rows) whose scope was null or empty and so matched nothing
    null_scopes: Dict[str, Dict[str, int]] = field(default_factory=dict)
    #: table -> file column -> outcome -> count
    file_resolution: Dict[str, Dict[str, Dict[str, int]]] = field(default_factory=dict)
    unknown_paths: Set[str] = field(default_factory=set)
    documents_with_unknown_paths: int = 0
    attachment_bytes: int = 0
    publish_period: Optional[str] = None
    publish_period_unmatched: bool = False
    unexpanded_zips: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    def file_resolution_rate(self, table: str) -> Optional[float]:
        """Resolved / (resolved + missing + ambiguous) over the table's file columns."""
        cols = self.file_resolution.get(table) or {}
        ok = bad = 0
        for outcomes in cols.values():
            for outcome, n in outcomes.items():
                if outcome in ("missing", "ambiguous"):
                    bad += n
                else:
                    ok += n
        return None if ok + bad == 0 else ok / (ok + bad)


class _DocCounters:
    """Counts made while evaluating one document; merged only if it commits."""

    __slots__ = ("attachment_bytes", "fails", "files", "null_scopes", "unmapped")

    def __init__(self) -> None:
        self.fails: Dict[Tuple[str, str], int] = {}
        self.unmapped: Dict[Tuple[str, str], int] = {}
        self.null_scopes: Dict[Tuple[str, str], int] = {}
        self.files: Dict[Tuple[str, str, str], int] = {}
        self.attachment_bytes = 0

    def fail(self, key: Tuple[str, str]) -> None:
        self.fails[key] = self.fails.get(key, 0) + 1

    def miss(self, key: Tuple[str, str]) -> None:
        self.unmapped[key] = self.unmapped.get(key, 0) + 1

    def null_scope(self, key: Tuple[str, str]) -> None:
        self.null_scopes[key] = self.null_scopes.get(key, 0) + 1

    def merge_into(self, stats: BundleStats) -> None:
        for (owner, col), n in self.fails.items():
            d = stats.coercion_failures.setdefault(owner, {})
            d[col] = d.get(col, 0) + n
        for (owner, col), n in self.unmapped.items():
            d = stats.unmapped_values.setdefault(owner, {})
            d[col] = d.get(col, 0) + n
        for (owner, col), n in self.null_scopes.items():
            d = stats.null_scopes.setdefault(owner, {})
            d[col] = d.get(col, 0) + n
        for (owner, col, outcome), n in self.files.items():
            d = stats.file_resolution.setdefault(owner, {}).setdefault(col, {})
            d[outcome] = d.get(outcome, 0) + n
        stats.attachment_bytes += self.attachment_bytes


class _Ctx:
    """Per-document evaluation state."""

    __slots__ = ("attachment_bytes", "counters", "indexes", "member", "members", "vars")

    def __init__(self, member: Member, members: MemberIndex, counters: _DocCounters):
        self.vars: Dict[str, str] = {}
        self.indexes: Dict[str, Tuple[List[Dict[str, Any]], Dict[str, List[int]]]] = {}
        self.member = member
        self.members = members
        self.counters = counters


# ------------------------------------------------------------------ compiled columns


class _Value:
    """A scalar or list<scalar> column (or index value, or struct field)."""

    def __init__(self, owner: str, name: str, spec: ColumnSpec, ctype: str, reader: Any, index_types: Dict[str, Dict[str, str]]):
        self.owner = owner
        self.name = name
        self.key = (owner, name)
        kind, elem = parse_type(ctype)
        self.is_list = kind == "list"
        scalar = elem if self.is_list else ctype
        self.coerce = coercer_for(scalar, spec.format, spec.timezone)
        self.empty_as_null = spec.empty_as_null
        self.has_when_empty = spec.has_when_empty
        self.when_empty = spec.when_empty
        self.map = spec.map
        self.expr = reader.compile(spec.expr) if spec.expr is not None else None
        self.first_of = [reader.compile(e) for e in (spec.first_of or [])]
        self.lookup = spec.lookup
        self.lookup_keys = reader.compile(spec.lookup.keys) if spec.lookup is not None else None
        self.lookup_scope = (
            reader.compile(spec.lookup.scope) if spec.lookup is not None and spec.lookup.scope is not None else None
        )
        # `prefer` compares by string, so the candidates are held as map keys;
        # the value stored is the element itself, coerced.
        self.prefer = [map_key(v) for v in spec.prefer] if spec.prefer is not None else None
        if self.prefer is not None and self.expr is not None and not self.expr.returns_nodes():
            raise DocumentError("`prefer` needs a list source; this expression returns one value")
        # Per-value work is the hot loop, so each column binds its own path once
        # here rather than re-deciding it for every value. The general methods
        # stay the definition; the specialised ones are what they reduce to.
        self._is_string = scalar == "string"
        if self.map is None and not self.has_when_empty:
            self._finish = self._finish_plain  # type: ignore[method-assign]
        if self.prefer is not None:
            self.eval = self._eval_prefer  # type: ignore[method-assign]
        elif self.lookup is None and not self.is_list and not self.first_of and self.expr is not None:
            self.eval = self._eval_scalar  # type: ignore[method-assign]

    def _finish_plain(self, raw: Any, ctx: _Ctx) -> Any:
        """``_finish`` for a column with no ``map`` and no ``when_empty``."""
        if raw is None or (raw == "" and self.empty_as_null):
            return None  # every coercer answers (None, False) for a null
        if self._is_string and raw.__class__ is str:
            return raw  # to_string would return it unchanged
        v, failed = self.coerce(raw)
        if failed:
            ctx.counters.fail(self.key)
        return v

    def _eval_scalar(self, node: Any, ctx: _Ctx) -> Any:
        return self._finish(self.expr.scalar(node, ctx.vars), ctx)

    # -- pipeline: empty_as_null -> when_empty -> map -> coerce
    def _finish(self, raw: Any, ctx: _Ctx) -> Any:
        if raw == "" and self.empty_as_null:
            raw = None
        if raw is None and self.has_when_empty:
            raw = self.when_empty
        m = self.map
        if m is not None:
            if raw is None:
                raw = m.null if m.has_null else None
            else:
                k = map_key(raw)
                if k in m.values:
                    raw = m.values[k]
                elif m.has_default:
                    raw = m.default
                else:
                    ctx.counters.miss(self.key)
                    raw = None
        v, failed = self.coerce(raw)
        if failed:
            ctx.counters.fail(self.key)
        return v

    def _lookup(self, node: Any, ctx: _Ctx) -> Any:
        lk = self.lookup
        entries, keymap = ctx.indexes[lk.index]
        keys: List[Optional[str]] = self.lookup_keys.strings(node, ctx.vars)
        scope: Optional[str] = None
        if self.lookup_scope is not None:
            scope = self.lookup_scope.scalar(node, ctx.vars)
            if scope is None or scope == "":
                # No scope names no entry: matching it against unscoped
                # entries would be the cross-link a scope exists to prevent.
                ctx.counters.null_scope(self.key)
                keys = []
        by_keys = lk.order == "keys"
        ords: Set[int] = set()
        seq: List[int] = []
        for k in keys:
            if k is not None and k != "":
                hit = keymap.get(k if scope is None else (scope, k))
                if hit:
                    if by_keys:
                        for o in hit:
                            if o not in ords:
                                ords.add(o)
                                seq.append(o)
                    else:
                        ords.update(hit)
        mode = lk.mode
        if mode == "exists":
            return bool(ords)
        ordered = seq if by_keys else sorted(ords)
        if lk.field is None:
            return len(ordered)  # count of entries
        vals = [entries[i].get(lk.field) for i in ordered]
        if mode == "first":
            return vals[0] if vals else None
        if mode == "last":
            return vals[-1] if vals else None
        present = [v for v in vals if v is not None]
        if lk.distinct:
            seen: Set[Any] = set()
            present = [v for v in present if not (v in seen or seen.add(v))]
        if lk.sort:
            present = sorted(present)
        return len(present) if mode == "count" else present

    def _eval_prefer(self, node: Any, ctx: _Ctx) -> Any:
        """One element of a list source: the first ``prefer`` entry present.

        An empty list gives ``when_empty`` (as the column's value: it is not
        mapped) or null. A non-empty list carrying no preferred value gives
        null and is counted in ``unmapped_values``. ``map`` is applied to each
        element first; an element the map does not cover is not a candidate.
        """
        items = self._lookup(node, ctx) if self.lookup is not None else self.expr.strings(node, ctx.vars)
        present: Dict[str, Any] = {}
        empty = True
        m = self.map
        for x in items or ():
            if x is None or (x == "" and self.empty_as_null):
                continue
            empty = False
            if m is not None:
                k = map_key(x)
                if k in m.values:
                    x = m.values[k]
                elif m.has_default:
                    x = m.default
                else:
                    continue
                if x is None:
                    continue
            present.setdefault(map_key(x), x)
        if empty:
            if not self.has_when_empty:
                return None
            raw = self.when_empty
        else:
            for p in self.prefer:  # type: ignore[union-attr]
                if p in present:
                    raw = present[p]
                    break
            else:
                ctx.counters.miss(self.key)
                return None
        v, failed = self.coerce(raw)
        if failed:
            ctx.counters.fail(self.key)
        return v

    def eval(self, node: Any, ctx: _Ctx) -> Any:
        if self.lookup is not None:
            raw = self._lookup(node, ctx)
            if self.is_list:
                return [self._finish(x, ctx) for x in (raw or [])]
            return self._finish(raw, ctx)
        if self.is_list:
            return [self._finish(x, ctx) for x in self.expr.strings(node, ctx.vars)]
        if self.first_of:
            raw = None
            for e in self.first_of:
                raw = e.scalar(node, ctx.vars)
                if raw is not None and not (raw == "" and self.empty_as_null):
                    break
                raw = None
            return self._finish(raw, ctx)
        return self._finish(self.expr.scalar(node, ctx.vars), ctx)


class _Struct:
    def __init__(self, owner: str, name: str, spec: ColumnSpec, reader: Any, index_types: Dict[str, Dict[str, str]]):
        self.rows = reader.compile(spec.rows)
        self.fields: List[Tuple[str, _Value]] = []
        for fname, f in (spec.fields or {}).items():
            fspec = as_spec(f)
            ftype = resolved_column_type(fspec, index_types, "", _Problems()) or "string"
            self.fields.append((fname, _Value(owner, f"{name}.{fname}", fspec, ftype, reader, index_types)))

    def eval(self, node: Any, ctx: _Ctx) -> List[Dict[str, Any]]:
        out = []
        for sub in self.rows.nodes(node, ctx.vars):
            out.append({fname: v.eval(sub, ctx) for fname, v in self.fields})
        return out


class _File:
    def __init__(self, owner: str, name: str, spec: ColumnSpec, reader: Any):
        self.owner, self.name = owner, name
        self.expr = reader.compile(spec.expr)
        self.empty_as_null = spec.empty_as_null
        self.strategies: List[Any] = list(spec.resolve or [])
        self.probe = spec.probe == "image"
        self.blob = spec.store == "blob"
        if self.probe:
            require_pillow()

    def eval(self, node: Any, ctx: _Ctx) -> Tuple[Any, ...]:
        ref = self.expr.scalar(node, ctx.vars)
        if ref is not None and ref.strip() == "":
            ref = None
        member, outcome = resolve(ref, ctx.member, self.strategies, ctx.members)
        if outcome is not None:
            k = (self.owner, self.name, outcome)
            ctx.counters.files[k] = ctx.counters.files.get(k, 0) + 1
        data = member.read() if member is not None else None
        stored: Any = None
        size = sha = None
        fmt = width = height = None
        if data is not None:
            size = len(data)
            sha = hashlib.sha256(data).hexdigest()
            stored = data if self.blob else member.path
            if self.blob:
                ctx.counters.attachment_bytes += size
            if self.probe:
                fmt, width, height, failed = probe_image(data)
                if failed:
                    ctx.counters.fail((self.owner, f"{self.name}_format"))
        vals: Tuple[Any, ...] = (stored, ref, member.path if member else None, outcome, size, sha)
        if self.probe:
            vals += (fmt, width, height)
        return vals


# ------------------------------------------------------------------ compiled mapping


@dataclass
class _Index:
    name: str
    rows: Any
    keys: Any
    values: List[Tuple[str, _Value]]
    scope: Any = None


@dataclass
class _Table:
    name: str
    rows: Any
    vars: List[Tuple[str, Any]]
    #: (column name, evaluator, width): width>1 for a file column's companions
    columns: List[Tuple[str, Any, int]]
    row_id: Optional[str]
    row_id_fields: List[str]
    schema: Any
    blob_columns: List[str]


@dataclass
class CompiledMapping:
    doc: MappingDoc
    reader: Any
    indexes: List[_Index]
    tables: List[_Table]
    rejects_schema: Any
    period_regex: Optional[re.Pattern]
    expected_paths: Optional[Set[str]]

    @property
    def table_names(self) -> List[str]:
        return [t.name for t in self.tables]

    def schema_for(self, table: str) -> Any:
        if table == REJECTS_TABLE:
            return self.rejects_schema
        for t in self.tables:
            if t.name == table:
                return t.schema
        raise KeyError(table)

    def file_columns(self, table: str) -> List[str]:
        for t in self.tables:
            if t.name == table:
                return [name for name, ev, _w in t.columns if isinstance(ev, _File)]
        return []


def _system_fields(pa: Any, with_row_id: bool) -> List[Any]:
    out = [pa.field("doc_id", pa.string())]
    if with_row_id:
        out.append(pa.field("row_id", pa.string()))
    out += [
        pa.field("source_bundle", pa.string()),
        pa.field("source_file", pa.string()),
        pa.field("publish_period", pa.string()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
    return out


def rejects_schema() -> Any:
    pa = _pa()
    return pa.schema(
        [
            pa.field("doc_id", pa.string()),
            pa.field("source_bundle", pa.string()),
            pa.field("source_file", pa.string()),
            pa.field("publish_period", pa.string()),
            pa.field("error", pa.string()),
            pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        ]
    )


def _arrow_type(pa: Any, t: str) -> Any:
    kind, elem = parse_type(t)
    if kind == "list":
        return pa.list_(arrow_scalar(pa, elem))
    return arrow_scalar(pa, t)


def compile_mapping(doc: MappingDoc) -> CompiledMapping:
    """Compile every expression and fix every schema. Raises :class:`MappingError`."""
    pa = _pa()
    reader = reader_for(
        doc.document.format,
        strip_namespaces=doc.document.strip_namespaces,
        max_bytes=doc.document.max_bytes,
        huge_tree=doc.document.huge_tree,
    )
    problems: List[str] = []

    def _guard(where: str, fn: Callable[..., Any], *args: Any) -> Any:
        # The callable and its arguments are passed separately, never as a
        # closure over the loop variables below.
        try:
            return fn(*args)
        except DocumentError as e:
            problems.append(f"{where}: {e}")
        except ImportError as e:
            problems.append(f"{where}: {e}")
        return None

    index_types: Dict[str, Dict[str, str]] = {}
    indexes: List[_Index] = []
    for iname, ix in doc.indexes.items():
        w = f"indexes.{iname}"
        rows = _guard(f"{w}.rows", reader.compile, ix.rows)
        keys = _guard(f"{w}.keys", reader.compile, ix.keys)
        scope = _guard(f"{w}.scope", reader.compile, ix.scope) if ix.scope is not None else None
        values = []
        types: Dict[str, str] = {}
        for vname, v in ix.values.items():
            vspec = as_spec(v)
            vt = resolved_column_type(vspec, index_types, "", _Problems()) or "string"
            ev = _guard(f"{w}.values.{vname}", _Value, f"indexes.{iname}", vname, vspec, vt, reader, index_types)
            values.append((vname, ev))
            types[vname] = vt
        index_types[iname] = types
        indexes.append(_Index(iname, rows, keys, values, scope))

    tables: List[_Table] = []
    for tname, tb in doc.tables.items():
        w = f"tables.{tname}"
        rows = _guard(f"{w}.rows", reader.compile, tb.rows)
        tvars = [(vn, _guard(f"{w}.vars.{vn}", reader.compile, ve)) for vn, ve in tb.vars.items()]
        cols: List[Tuple[str, Any, int]] = []
        fields = _system_fields(pa, tb.row_id is not None)
        blob_cols: List[str] = []
        for cname, c in tb.columns.items():
            cw = f"{w}.columns.{cname}"
            cspec = as_spec(c)
            ct = resolved_column_type(cspec, index_types, "", _Problems()) or "string"
            kind, _ = parse_type(ct)
            if kind == "file":
                ev = _guard(cw, _File, tname, cname, cspec, reader)
                width = 6 + (3 if cspec.probe == "image" else 0)
                if cspec.store == "blob":
                    fields.append(pa.field(cname, pa.large_binary(), metadata=BLOB_METADATA))
                    blob_cols.append(cname)
                else:
                    fields.append(pa.field(cname, pa.string()))
                fields += [
                    pa.field(f"{cname}_ref", pa.string()),
                    pa.field(f"{cname}_member", pa.string()),
                    pa.field(f"{cname}_resolution", pa.string()),
                    pa.field(f"{cname}_bytes", pa.int64()),
                    pa.field(f"{cname}_sha256", pa.string()),
                ]
                if cspec.probe == "image":
                    fields += [
                        pa.field(f"{cname}_format", pa.string()),
                        pa.field(f"{cname}_width", pa.int32()),
                        pa.field(f"{cname}_height", pa.int32()),
                    ]
                cols.append((cname, ev, width))
            elif kind == "list_struct":
                ev = _guard(cw, _Struct, tname, cname, cspec, reader, index_types)
                if ev is not None:
                    struct = pa.struct(
                        [
                            pa.field(fname, _arrow_type(pa, resolved_column_type(as_spec(f), index_types, "", _Problems()) or "string"))
                            for fname, f in (cspec.fields or {}).items()
                        ]
                    )
                    fields.append(pa.field(cname, pa.list_(struct)))
                cols.append((cname, ev, 1))
            else:
                ev = _guard(cw, _Value, tname, cname, cspec, ct, reader, index_types)
                fields.append(pa.field(cname, _arrow_type(pa, ct)))
                cols.append((cname, ev, 1))
        tables.append(
            _Table(
                name=tname,
                rows=rows,
                vars=tvars,
                columns=cols,
                row_id=tb.row_id,
                row_id_fields=template_fields(tb.row_id) if tb.row_id else [],
                schema=pa.schema(fields),
                blob_columns=blob_cols,
            )
        )

    if problems:
        raise MappingError(problems)
    pp = doc.lineage.publish_period
    return CompiledMapping(
        doc=doc,
        reader=reader,
        indexes=indexes,
        tables=tables,
        rejects_schema=rejects_schema(),
        period_regex=re.compile(pp.regex) if pp else None,
        expected_paths=set(doc.expected_paths) if doc.expected_paths and doc.on_unknown_path != "ignore" else None,
    )


# ------------------------------------------------------------------ running


def publish_period_for(cm: CompiledMapping, bundle_key: str, bundle_name: str) -> Tuple[Optional[str], bool]:
    """``(period, unmatched)``: the named group ``period``, else group 1, else the match."""
    pp = cm.doc.lineage.publish_period
    if pp is None or cm.period_regex is None:
        return None, False
    text = bundle_name if pp.from_ == "bundle_name" else bundle_key
    m = cm.period_regex.search(text)
    if not m:
        return None, True
    if "period" in cm.period_regex.groupindex:
        return m.group("period"), False
    if m.groups():
        return m.group(1), False
    return m.group(0), False


def _template_text(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


def _error_text(e: BaseException, redact: bool) -> str:
    msg = str(e)
    if redact and not isinstance(e, DocumentError):
        return f"{type(e).__name__} [{hashlib.sha256(msg.encode('utf-8', 'replace')).hexdigest()[:12]}]"
    if isinstance(e, DocumentError):
        return msg
    return f"{type(e).__name__}: {msg}"


class _Buffers:
    """Column-wise row buffers per table, flushed to the sink as RecordBatches."""

    def __init__(self, cm: CompiledMapping, sink: Sink, flush_rows: int, flush_attachment_bytes: int):
        self.pa = _pa()
        self.cm = cm
        self.sink = sink
        self.flush_rows = flush_rows
        self.flush_bytes = flush_attachment_bytes
        self.cols: Dict[str, List[List[Any]]] = {}
        self.attach: Dict[str, int] = {}
        for t in [*cm.tables]:
            self.cols[t.name] = [[] for _ in t.schema]
            self.attach[t.name] = 0
        self.cols[REJECTS_TABLE] = [[] for _ in cm.rejects_schema]
        self.attach[REJECTS_TABLE] = 0

    def extend(self, table: str, columns: List[List[Any]], attachment_bytes: int) -> None:
        dst = self.cols[table]
        for d, s in zip(dst, columns):
            d.extend(s)
        self.attach[table] += attachment_bytes
        if len(dst[0]) >= self.flush_rows or self.attach[table] >= self.flush_bytes:
            self.flush(table)

    def flush(self, table: str) -> None:
        cols = self.cols[table]
        if not cols or not cols[0]:
            return
        schema = self.cm.schema_for(table)
        arrays = [self.pa.array(c, type=f.type) for c, f in zip(cols, schema)]
        self.sink(table, self.pa.RecordBatch.from_arrays(arrays, schema=schema))
        self.cols[table] = [[] for _ in schema]
        self.attach[table] = 0

    def flush_all(self) -> None:
        for name in list(self.cols):
            self.flush(name)


def _eval_document(
    cm: CompiledMapping,
    root: Any,
    member: Member,
    members: MemberIndex,
    counters: _DocCounters,
    system: Tuple[Any, ...],
    doc_id: str,
) -> Dict[str, Tuple[List[List[Any]], int]]:
    """Every table's rows for one document, column-wise, or raise."""
    ctx = _Ctx(member, members, counters)
    for ix in cm.indexes:
        entries: List[Dict[str, Any]] = []
        # A scoped index files each entry under (scope, key); an unscoped one
        # under the key alone. A lookup into either is validated to match.
        keymap: Dict[Any, List[int]] = {}
        scope_x = ix.scope
        scope_key = (f"indexes.{ix.name}", "scope")
        for node in ix.rows.nodes(root, {}):
            entry = {vn: ev.eval(node, ctx) for vn, ev in ix.values}
            ordinal = len(entries)
            entries.append(entry)
            scope: Optional[str] = None
            if scope_x is not None:
                scope = scope_x.scalar(node, {})
                if scope is None or scope == "":
                    counters.null_scope(scope_key)
                    continue  # filed under no scope, so no lookup can reach it
            for k in ix.keys.strings(node, {}):
                if k is None or k == "":
                    continue
                lst = keymap.setdefault(k if scope is None else (scope, k), [])
                if not lst or lst[-1] != ordinal:
                    lst.append(ordinal)
        ctx.indexes[ix.name] = (entries, keymap)

    out: Dict[str, Tuple[List[List[Any]], int]] = {}
    for t in cm.tables:
        ctx.vars = {}
        for vn, ve in t.vars:
            v = ve.scalar(root, ctx.vars)
            ctx.vars[vn] = "" if v is None else v
        before = counters.attachment_bytes
        n_sys = len(system) + 1 + (1 if t.row_id else 0)
        cols: List[List[Any]] = [[] for _ in t.schema]
        row_id = t.row_id
        # (column, evaluator, width, first output position), resolved once.
        plan = []
        i = n_sys
        for cname, ev, width in t.columns:
            plan.append((cname, ev.eval, width, i))
            i += width
        rid_col = cols[1] if row_id else None
        nrows = 0
        for rn, node in enumerate(t.rows.nodes(root, ctx.vars), start=1):
            nrows = rn
            named: Dict[str, Any] = {}
            for cname, ev_eval, width, i in plan:
                v = ev_eval(node, ctx)
                if width == 1:
                    cols[i].append(v)
                    named[cname] = v
                else:
                    for j in range(width):
                        cols[i + j].append(v[j])
                    named[cname] = v[0]
            if row_id:
                rid: Optional[str] = row_id
                for f in t.row_id_fields:
                    val = doc_id if f == "doc_id" else rn if f == "row_number" else named.get(f)
                    if val is None or isinstance(val, (bytes, list)):
                        rid = None
                        break
                    rid = rid.replace("{" + f + "}", _template_text(val))
                rid_col.append(rid)  # type: ignore[union-attr]
        # The per-row constants are the same for every row of the document.
        cols[0].extend([doc_id] * nrows)
        j = 2 if row_id else 1
        for k, sv in enumerate(system):
            cols[j + k].extend([sv] * nrows)
        out[t.name] = (cols, counters.attachment_bytes - before)
    return out


def run_bundle(
    cm: CompiledMapping,
    bundle: OpenBundle,
    sink: Sink,
    *,
    ingested_at: Optional[datetime] = None,
    flush_rows: int = 50_000,
    flush_attachment_bytes: int = 256 * 1024 * 1024,
    redact: bool = False,
) -> BundleStats:
    """Run one opened bundle through the mapping, handing batches to ``sink``.

    ``sink(table, record_batch)`` receives each table's rows in chunks, and
    ``_rejects`` the same way. Every table is flushed at the end, so a sink
    that has seen nothing for a table knows the bundle produced no row for it.
    """
    started = time.monotonic()
    ingested_at = (ingested_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    key = bundle.ref.key
    stats = BundleStats(bundle=key, members=len(bundle.members), unexpanded_zips=list(bundle.unexpanded_zips))
    stats.rows = {t.name: 0 for t in cm.tables}
    stats.rows[REJECTS_TABLE] = 0
    period, unmatched = publish_period_for(cm, key, bundle.ref.name)
    stats.publish_period, stats.publish_period_unmatched = period, unmatched
    if unmatched:
        log.warning("bundle %s: publish_period regex matched nothing; publish_period is null", key)

    is_document = glob_matcher(cm.doc.document.match)
    docs = sorted(
        (m for m in bundle.members if is_document(m.path)),
        key=lambda m: m.path,
    )
    stats.documents = len(docs)
    index = MemberIndex(bundle.members)
    buffers = _Buffers(cm, sink, max(1, flush_rows), max(1, flush_attachment_bytes))
    max_bytes = cm.doc.document.max_bytes
    policy = cm.doc.on_unknown_path

    for member in docs:
        doc_id = doc_id_for(key, member.path)
        counters = _DocCounters()
        try:
            data = member.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise DocumentError(f"document exceeds max_bytes ({max_bytes})")
            root = cm.reader.parse(data)
            unknown: Set[str] = set()
            if cm.expected_paths is not None:
                unknown = cm.reader.structural_paths(root) - cm.expected_paths
                if unknown and policy == "fail":
                    raise DocumentError(f"{len(unknown)} structural path(s) not in expected_paths")
            system = (key, member.path, period, ingested_at)
            produced = _eval_document(cm, root, member, index, counters, system, doc_id)
        except Exception as e:  # one bad document never stops the bundle
            stats.documents_rejected += 1
            log.warning("bundle %s: rejected %s (%s)", key, member.path, type(e).__name__)
            log.debug("bundle %s: rejection detail for %s", key, member.path, exc_info=not redact)
            buffers.extend(
                REJECTS_TABLE,
                [[doc_id], [key], [member.path], [period], [_error_text(e, redact)], [ingested_at]],
                0,
            )
            stats.rows[REJECTS_TABLE] += 1
            continue
        stats.documents_parsed += 1
        if unknown:
            stats.documents_with_unknown_paths += 1
            stats.unknown_paths |= unknown
        counters.merge_into(stats)
        for tname, (cols, attach) in produced.items():
            stats.rows[tname] += len(cols[0]) if cols else 0
            buffers.extend(tname, cols, attach)

    buffers.flush_all()
    if stats.unknown_paths and policy == "warn":
        log.warning(
            "bundle %s: %d document(s) carry %d structural path(s) not in expected_paths",
            key, stats.documents_with_unknown_paths, len(stats.unknown_paths),
        )
    stats.duration_seconds = time.monotonic() - started
    log.info(
        "bundle %s: %d document(s), %d parsed, %d rejected; rows %s",
        key, stats.documents, stats.documents_parsed, stats.documents_rejected, stats.rows,
    )
    return stats


def empty_tables(cm: CompiledMapping) -> Dict[str, Any]:
    """Every table's schema, rejects included -- what an empty bundle writes."""
    out = {t.name: t.schema for t in cm.tables}
    out[REJECTS_TABLE] = cm.rejects_schema
    return out


__all__ = [
    "BLOB_METADATA",
    "BundleStats",
    "CompiledMapping",
    "compile_mapping",
    "doc_id_for",
    "empty_tables",
    "publish_period_for",
    "rejects_schema",
    "run_bundle",
]
