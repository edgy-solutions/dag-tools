"""Document readers: parse bytes, select rows, evaluate expressions.

:class:`DocumentReader` is the seam a second document language plugs into
(:class:`JsonReader`, JMESPath over the standard library's parser, is the
second). The engine talks to it only through compiled expressions, so it
never sees the expression language itself. An XML view returns text; a JSON
view returns the native value (``3``, ``true``) and coercion decides whether
it fits the column.

:class:`XmlReader` is XPath 1.0 over lxml, parsed with every external
resolution off: no entity expansion, no DTD load, no network. Expressions are
compiled once per mapping. The shapes a mapping writes most often -- an
attribute of the row, an attribute of a child, a child's text -- take a fast
path that walks the tree directly; ``test_documents_reader`` asserts each
fast path answers exactly what XPath answers on the same node.
"""
from __future__ import annotations

import json
import math
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Set, Tuple

_NAME = r"[A-Za-z_][A-Za-z0-9_.\-]*"
_FP_ATTR = re.compile(rf"^@({_NAME})$")
_FP_STRING_ATTR = re.compile(rf"^string\(\s*@({_NAME})\s*\)$")
_FP_CHILD_ATTR = re.compile(rf"^({_NAME})/@({_NAME})$")
_FP_CHILD = re.compile(rf"^({_NAME})$")
_FP_PARENT_ATTR = re.compile(rf"^\.\./@({_NAME})$")
_FP_CHILD_WHERE_ATTR = re.compile(rf"^({_NAME})\[@({_NAME})\s*=\s*(?:'([^']*)'|\"([^\"]*)\")\]/@({_NAME})$")


class DocumentError(Exception):
    """A document that cannot be parsed, or an expression that cannot run on it."""


class ParserLimit(DocumentError):
    """A well-formed document the parser refused for one of its built-in limits.

    libxml2 refuses, by default, nesting deeper than 256 elements, an attribute
    value or a CDATA section over 10 MB and a name over 50,000 characters --
    and reports each under a different error code, none of which says "limit"
    in a way that holds across versions. So the reader decides it the one way
    that does: a document the default parser refused and a ``huge_tree``
    parser accepts was refused for a limit.
    """


class Compiled(Protocol):
    source: str

    def scalar(self, node: Any, variables: Dict[str, Any]) -> Any: ...

    def strings(self, node: Any, variables: Dict[str, Any]) -> List[Any]: ...

    def nodes(self, node: Any, variables: Dict[str, Any]) -> List[Any]: ...

    def returns_nodes(self) -> bool: ...


class DocumentReader(Protocol):
    """What the engine needs from a document language."""

    def parse(self, data: bytes) -> Any: ...

    def compile(self, expr: str) -> Compiled: ...

    def structural_paths(self, root: Any) -> Set[str]: ...


# ------------------------------------------------------------------ XML


def _lxml() -> Any:
    try:
        from lxml import etree
    except ImportError as e:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "reading XML documents needs lxml; install the `documents` extra: "
            "pip install 'edgy-dag-tools[documents]'"
        ) from e
    return etree


def _number_text(x: float) -> Optional[str]:
    if math.isnan(x) or math.isinf(x):
        return "NaN" if math.isnan(x) else ("Infinity" if x > 0 else "-Infinity")
    if x.is_integer():
        return str(int(x))
    return repr(x)


class _XPathExpr:
    """A compiled XPath with the scalar / string-list / node-list views."""

    __slots__ = ("_etree", "_fast_nodes", "_fast_scalar", "_fast_strings", "_string_value", "_varnames", "_xp", "source")

    def __init__(self, source: str, etree: Any, string_value: Callable[[Any], str]):
        from dag_tools.documents.mapping import referenced_vars

        self.source = source
        self._etree = etree
        self._string_value = string_value
        self._varnames = tuple(referenced_vars(source))
        try:
            self._xp = etree.XPath(source)
        except etree.XPathSyntaxError as e:
            raise DocumentError(f"expression does not compile: {source!r}: {e}") from None
        self._fast_scalar: Optional[Callable[[Any], Optional[str]]] = None
        self._fast_strings: Optional[Callable[[Any], List[Optional[str]]]] = None
        self._fast_nodes: Optional[Callable[[Any], List[Any]]] = None
        if not self._varnames:
            self._fast_paths(source)

    def returns_nodes(self) -> bool:
        """Whether the expression's result is a node-set.

        XPath 1.0 types an expression by its syntax alone, so evaluating it
        once on an empty element answers for every document. Variables are
        always strings here, so each is bound to ``""`` for the probe. An
        expression the probe cannot evaluate is given the benefit of the doubt.
        """
        try:
            r = self._xp(self._etree.Element("probe"), **{k: "" for k in self._varnames})
        except Exception:  # noqa: BLE001 -- an unprobeable expression gets the benefit of the doubt
            return True
        return isinstance(r, list)

    def _text(self, c: Any) -> str:
        if len(c) == 0:
            return c.text or ""
        return self._string_value(c)

    def _fast_paths(self, s: str) -> None:
        """Plain-Python equivalents of the few shapes a mapping is mostly made of.

        Each one must answer exactly what XPath answers on every node; the
        agreement is a test over every view (scalar, strings, nodes), not a
        claim. Anything not matched here runs through XPath unchanged.
        """
        m = _FP_ATTR.match(s)
        if m:
            name = m.group(1)

            def _attrs(n: Any) -> List[Optional[str]]:
                v = n.get(name)
                return [] if v is None else [v]

            self._fast_scalar = lambda n: n.get(name)
            self._fast_strings = _attrs
            return
        m = _FP_STRING_ATTR.match(s)
        if m:
            name = m.group(1)
            self._fast_scalar = lambda n: n.get(name, "")
            self._fast_strings = lambda n: [n.get(name, "")]
            return
        m = _FP_PARENT_ATTR.match(s)
        if m:
            name = m.group(1)

            def _parent_attr(n: Any) -> Optional[str]:
                p = n.getparent()
                return None if p is None else p.get(name)

            def _parent_attrs(n: Any) -> List[Optional[str]]:
                v = _parent_attr(n)
                return [] if v is None else [v]

            self._fast_scalar = _parent_attr
            self._fast_strings = _parent_attrs
            return
        m = _FP_CHILD_ATTR.match(s)
        if m:
            child, name = m.group(1), m.group(2)

            def _child_attr(n: Any) -> Optional[str]:
                for c in n.iterchildren(child):
                    v = c.get(name)
                    if v is not None:
                        return v
                return None

            def _child_attrs(n: Any) -> List[Optional[str]]:
                return [v for v in (c.get(name) for c in n.iterchildren(child)) if v is not None]

            self._fast_scalar = _child_attr
            self._fast_strings = _child_attrs
            return
        m = _FP_CHILD_WHERE_ATTR.match(s)
        if m:
            child, key, lit_sq, lit_dq, name = m.groups()
            lit = lit_sq if lit_sq is not None else lit_dq

            def _where_attrs(n: Any) -> List[Optional[str]]:
                out: List[Optional[str]] = []
                for c in n.iterchildren(child):
                    if c.get(key) == lit:
                        v = c.get(name)
                        if v is not None:
                            out.append(v)
                return out

            def _where_attr(n: Any) -> Optional[str]:
                for c in n.iterchildren(child):
                    if c.get(key) == lit:
                        v = c.get(name)
                        if v is not None:
                            return v
                return None

            self._fast_scalar = _where_attr
            self._fast_strings = _where_attrs
            return
        m = _FP_CHILD.match(s)
        if m:
            child = m.group(1)
            text = self._text

            def _child_text(n: Any) -> Optional[str]:
                for c in n.iterchildren(child):
                    return text(c)
                return None

            self._fast_scalar = _child_text
            self._fast_strings = lambda n: [text(c) for c in n.iterchildren(child)]
            self._fast_nodes = lambda n: list(n.iterchildren(child))
            return

    def _run(self, node: Any, variables: Dict[str, Any]) -> Any:
        kw = {k: variables[k] for k in self._varnames if k in variables}
        try:
            return self._xp(node, **kw)
        except Exception as e:  # noqa: BLE001 -- XPathEvalError and friends; re-raised as DocumentError
            raise DocumentError(f"expression failed: {self.source!r}: {type(e).__name__}") from None

    def _one(self, item: Any) -> Optional[str]:
        if isinstance(item, str):
            return str(item)
        if isinstance(item, bool):
            return "true" if item else "false"
        if isinstance(item, float):
            return _number_text(item)
        if isinstance(item, tuple):  # namespace node
            return str(item[1])
        return self._string_value(item)

    def scalar(self, node: Any, variables: Dict[str, Any]) -> Optional[str]:
        """A node-set gives its first node's string value; empty gives None."""
        if self._fast_scalar is not None:
            return self._fast_scalar(node)
        r = self._run(node, variables)
        if isinstance(r, list):
            return self._one(r[0]) if r else None
        return self._one(r)

    def strings(self, node: Any, variables: Dict[str, Any]) -> List[Optional[str]]:
        if self._fast_strings is not None:
            return self._fast_strings(node)
        r = self._run(node, variables)
        if isinstance(r, list):
            return [self._one(x) for x in r]
        return [self._one(r)]

    def nodes(self, node: Any, variables: Dict[str, Any]) -> List[Any]:
        if self._fast_nodes is not None:
            return self._fast_nodes(node)
        r = self._run(node, variables)
        if not isinstance(r, list):
            raise DocumentError(f"expression {self.source!r} must select nodes, not a {type(r).__name__}")
        return r


class XmlReader:
    """XPath 1.0 over lxml with external resolution switched off."""

    def __init__(self, *, strip_namespaces: bool = False, max_bytes: Optional[int] = None, huge_tree: bool = False):
        self._etree = _lxml()
        self.strip_namespaces = strip_namespaces
        self.max_bytes = max_bytes
        self.huge_tree = huge_tree
        self._parser = self._make_parser(huge_tree)
        self._probe: Any = None  # the huge_tree parser, built on the first refusal
        sv = self._etree.XPath("string(.)")
        self._string_value: Callable[[Any], str] = lambda n: str(sv(n))

    def _make_parser(self, huge_tree: bool) -> Any:
        return self._etree.XMLParser(
            resolve_entities=False,
            no_network=True,
            load_dtd=False,
            dtd_validation=False,
            huge_tree=huge_tree,
            remove_comments=False,
        )

    def _hit_a_limit(self, data: bytes) -> bool:
        """Whether a document the default parser refused parses with the limits lifted.

        Run only after a refusal, so a document that parses costs nothing. The
        probe keeps every other protection (no entities, no DTD, no network),
        and its tree is thrown away.
        """
        if self._probe is None:
            self._probe = self._make_parser(True)
        try:
            self._etree.fromstring(data, parser=self._probe)
        except Exception:  # noqa: BLE001 -- any failure means the probe parse did not succeed
            return False
        return True

    def parse(self, data: bytes) -> Any:
        if self.max_bytes is not None and len(data) > self.max_bytes:
            raise DocumentError(f"document exceeds max_bytes ({self.max_bytes})")
        try:
            root = self._etree.fromstring(data, parser=self._parser)
        except self._etree.XMLSyntaxError as e:
            line = getattr(e, "lineno", "?")
            # lxml's own message quotes the document at times, so none of it
            # is kept: the line number and the option are all a reject needs.
            if not self.huge_tree and self._hit_a_limit(data):
                raise ParserLimit(
                    f"document hits a built-in XML parser limit (depth, or the length of a name, "
                    f"attribute value or CDATA section) at line {line}; "
                    "set `document.huge_tree: true` in the mapping to lift it"
                ) from None
            raise DocumentError(f"not well-formed XML: line {line}") from None
        if root is None:
            raise DocumentError("empty document")
        if self.strip_namespaces:
            self._strip(root)
        return root

    def _strip(self, root: Any) -> None:
        qn = self._etree.QName
        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            if el.tag[0] == "{":
                el.tag = qn(el).localname
            if el.attrib:
                for k in [k for k in el.attrib if k[0] == "{"]:
                    v = el.attrib.pop(k)
                    el.set(qn(k).localname, v)
        self._etree.cleanup_namespaces(root)

    def compile(self, expr: str) -> _XPathExpr:
        return _XPathExpr(expr, self._etree, self._string_value)

    def structural_paths(self, root: Any) -> Set[str]:
        """Every element and attribute path, by local name: ``/A/B/@c``."""
        return xml_structural_paths(root)


def xml_structural_paths(root: Any) -> Set[str]:
    """Every element and attribute path of a parsed XML tree, by local name: ``/A/B/@c``.

    The one spelling of an XML structural path. The engine checks a document
    against ``expected_paths`` with it and the profiler writes its path lists
    with it, so a profiled list pasted into a mapping matches by construction.
    """
    if root is None:
        return set()
    from lxml import etree

    qn = etree.QName
    out: Set[str] = set()
    # A document repeats a few shapes many times, so each (parent path,
    # tag) and each (path, attribute names) is spelled out once; the set
    # of paths is the same as visiting every node, only cheaper.
    child_path: Dict[Tuple[str, str], str] = {}
    seen_attrs: Set[Tuple[str, Tuple[str, ...]]] = set()

    def local(tag: str) -> str:
        return qn(tag).localname if tag[0] == "{" else tag

    def walk(el: Any, path: str) -> None:
        keys = el.keys()
        if keys:
            sig = (path, tuple(keys))
            if sig not in seen_attrs:
                seen_attrs.add(sig)
                for a in keys:
                    out.add(f"{path}/@{local(a)}")
        for c in el:
            tag = c.tag
            if tag.__class__ is not str:  # comments, PIs, entities
                continue
            k = (path, tag)
            cp = child_path.get(k)
            if cp is None:
                cp = child_path[k] = f"{path}/{local(tag)}"
                out.add(cp)
            walk(c, cp)

    top = f"/{local(root.tag)}"
    out.add(top)
    walk(root, top)
    return out


# ------------------------------------------------------------------ JSON


def _jmespath() -> Any:
    try:
        import jmespath
    except ImportError as e:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "reading JSON documents needs jmespath; install the `documents` extra: "
            "pip install 'edgy-dag-tools[documents]'"
        ) from e
    return jmespath


_JSON_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_JSON_SEG = r'(?:[A-Za-z_][A-Za-z0-9_]*|"(?:[^"\\]|\\.)*")'
_FP_JSON_CHAIN = re.compile(rf"^\s*{_JSON_SEG}(?:\.{_JSON_SEG})*\s*$")
_FP_JSON_SEG = re.compile(_JSON_SEG)

# JMESPath functions whose result is never a list of nodes. An expression
# that is nothing but a call to one cannot give `prefer` a list to choose
# from (``sort``, ``keys``, ``values``, ``reverse`` and friends can).
JMES_SCALAR_FUNCS = frozenset(
    [
        "abs", "avg", "ceil", "contains", "ends_with", "floor", "join", "length", "max", "max_by",
        "min", "min_by", "not_null", "starts_with", "sum", "to_number", "to_string", "type",
    ]
)


def json_path_key(key: str) -> str:
    """The structural-path step for one object key: ``.k`` or ``."quoted key"``.

    A key that is not a JMESPath identifier is written as a JSON string, which
    is also how a JMESPath expression quotes it -- so a path line and an
    expression spell an awkward key the same way.
    """
    if _JSON_IDENT.match(key):
        return "." + key
    return "." + json.dumps(key, ensure_ascii=False)


def _json_fast_path(source: str) -> Optional[Tuple[str, ...]]:
    """A dotted chain of plain or quoted keys, read as a tuple of keys; ``@`` is ``()``."""
    s = source.strip()
    if s == "@":
        return ()
    if not _FP_JSON_CHAIN.match(s):
        return None
    return tuple(json.loads(seg) if seg.startswith('"') else seg for seg in _FP_JSON_SEG.findall(s))


class _JmesExpr:
    """A compiled JMESPath with the scalar / string-list / node-list views.

    Values are returned natively (a JSON ``3`` is an ``int``, ``true`` a
    ``bool``); coercion decides what fits the column. An object or a list
    where one value was expected is handed on as-is, so the column's coercer
    counts it as a failure rather than this view inventing a text for it.

    A dotted chain of keys (``a.b``, ``"Dock door".code``) and ``@`` skip
    JMESPath and walk the objects directly; ``test_documents_json`` asserts the
    walk answers what JMESPath answers on every node of the depot document.
    """

    __slots__ = ("source", "_fn", "_keys", "_parsed")

    def __init__(self, source: str, jmespath: Any) -> None:
        self.source = source
        try:
            compiled = jmespath.compile(source)
        except Exception:
            raise DocumentError(f"expression does not compile: {source!r}") from None
        self._parsed = compiled.parsed
        self._keys = _json_fast_path(source)
        self._fn = compiled.search

    def returns_nodes(self) -> bool:
        p = self._parsed
        t = p.get("type")
        if t == "literal":
            return False
        if t == "function_expression" and p.get("value") in JMES_SCALAR_FUNCS:
            return False
        return True

    def _run(self, node: Any) -> Any:
        keys = self._keys
        if keys is not None:
            cur = node
            for k in keys:
                if not isinstance(cur, dict):
                    return None
                cur = cur.get(k)
            return cur
        try:
            return self._fn(node)
        except Exception as e:
            raise DocumentError(f"expression failed: {self.source!r}: {type(e).__name__}") from None

    def scalar(self, node: Any, variables: Dict[str, Any]) -> Any:
        r = self._run(node)
        if isinstance(r, list):
            return r[0] if r else None
        return r

    def strings(self, node: Any, variables: Dict[str, Any]) -> List[Any]:
        r = self._run(node)
        if r is None:
            return []
        if isinstance(r, list):
            return [x for x in r if x is not None]
        return [r]

    def nodes(self, node: Any, variables: Dict[str, Any]) -> List[Any]:
        r = self._run(node)
        if isinstance(r, list):
            return [x for x in r if x is not None]
        if isinstance(r, dict):
            return [r]
        return []


def _refuse_constant(name: str) -> Any:
    raise DocumentError("not valid JSON: NaN and Infinity are not JSON numbers")


def _finite_float(text: str) -> Any:
    # json reads 1e999 as inf. A number that is not finite is kept as the text
    # it was written as -- which is what the XML route sees -- and coercion
    # refuses it there exactly as it would refuse the attribute.
    x = float(text)
    return x if math.isfinite(x) else text


class JsonReader:
    """JSON documents, with JMESPath expressions.

    A UTF-8 BOM is accepted. An empty or whitespace-only document has no root
    and yields no rows; so do ``{}``, ``[]`` and a bare scalar, because there
    is nothing in them for a row expression to select. Only text that is not
    JSON is a reject, and its message names a line and column, never the text.
    ``NaN`` and ``Infinity`` are not JSON and are refused.
    """

    def __init__(self, *, max_bytes: Optional[int] = None) -> None:
        self._jmespath = _jmespath()
        self.max_bytes = max_bytes

    def parse(self, data: bytes) -> Any:
        if self.max_bytes is not None and len(data) > self.max_bytes:
            raise DocumentError(f"document exceeds max_bytes ({self.max_bytes})")
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise DocumentError("document is not valid UTF-8") from None
        if not text.strip():
            return None
        try:
            return json.loads(text, parse_constant=_refuse_constant, parse_float=_finite_float)
        except json.JSONDecodeError as e:
            raise DocumentError(f"not valid JSON: {e.msg} at line {e.lineno} column {e.colno}") from None
        except DocumentError:
            raise
        except RecursionError:
            raise DocumentError("not valid JSON: nested too deeply to read") from None
        except ValueError as e:
            raise DocumentError(f"not valid JSON: {type(e).__name__}") from None

    def compile(self, expr: str) -> _JmesExpr:
        return _JmesExpr(expr, self._jmespath)

    def structural_paths(self, root: Any) -> Set[str]:
        """Every distinct container and key path: ``$``, ``$.a``, ``$.a[]``, ``$.a[].b``."""
        return json_structural_paths(root)


def json_structural_paths(root: Any) -> Set[str]:
    """Every distinct container and key path of a parsed JSON value.

    ``$``, ``$.a``, ``$.a[]``, ``$.a[].b``; a key that is not an identifier is
    written quoted (:func:`json_path_key`), ``$.a."b c"``. Each list level is
    written ``[]``, so a list nested directly in a list is ``$.a[][]``. An
    empty list still contributes its ``[]`` path. A document with no root
    (zero bytes, whitespace) has no paths at all.

    The one spelling of a JSON structural path: the engine checks documents
    with it and the profiler writes its path lists with it.
    """
    out: Set[str] = set()
    if root is None:
        return out
    key_path: Dict[Tuple[str, str], str] = {}

    def walk(v: Any, path: str) -> None:
        if isinstance(v, dict):
            for k, c in v.items():
                kk = (path, k)
                p = key_path.get(kk)
                if p is None:
                    p = key_path[kk] = path + json_path_key(k)
                    out.add(p)
                walk(c, p)
        elif isinstance(v, list):
            p = path + "[]"
            out.add(p)
            for c in v:
                walk(c, p)

    out.add("$")
    walk(root, "$")
    return out


_QUOTED_STEP = re.compile(r'\."((?:[^"\\]|\\.)*)"')


def path_spelling_key(path: str) -> str:
    """``path`` with every quoted key step unquoted: ``$.a."b c"`` -> ``$.a.b c``.

    Used only to tell a spelling difference from a structural one: two paths
    that differ here differ in structure; two that agree here and differ as
    written differ only in how a key was spelled. It is not a structural path
    itself (``$."a.b"`` and ``$.a.b`` both read ``$.a.b`` here).
    """

    def unquote(m: "re.Match[str]") -> str:
        try:
            return "." + json.loads('"' + m.group(1) + '"')
        except ValueError:
            return m.group(0)

    return _QUOTED_STEP.sub(unquote, path)


def reader_for(
    fmt: str, *, strip_namespaces: bool = False, max_bytes: Optional[int] = None, huge_tree: bool = False
) -> DocumentReader:
    if fmt == "xml":
        return XmlReader(strip_namespaces=strip_namespaces, max_bytes=max_bytes, huge_tree=huge_tree)
    if fmt == "json":
        return JsonReader(max_bytes=max_bytes)
    raise DocumentError(f"no reader for document format {fmt!r}")


def iter_strings(values: Iterable[Optional[str]]) -> List[str]:
    return [v for v in values if v is not None]
