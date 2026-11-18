"""Document readers: parse bytes, select rows, evaluate expressions.

:class:`DocumentReader` is the seam a second document language plugs into
(a JSON reader over JMESPath is the planned second one). The engine talks to
it only through compiled expressions, so it never sees the expression
language itself.

:class:`XmlReader` is XPath 1.0 over lxml, parsed with every external
resolution off: no entity expansion, no DTD load, no network. Expressions are
compiled once per mapping. The shapes a mapping writes most often -- an
attribute of the row, an attribute of a child, a child's text -- take a fast
path that walks the tree directly; ``test_documents_reader`` asserts each
fast path answers exactly what XPath answers on the same node.
"""
from __future__ import annotations

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

    def scalar(self, node: Any, variables: Dict[str, Any]) -> Optional[str]: ...

    def strings(self, node: Any, variables: Dict[str, Any]) -> List[Optional[str]]: ...

    def nodes(self, node: Any, variables: Dict[str, Any]) -> List[Any]: ...


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
        qn = self._etree.QName
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


def reader_for(
    fmt: str, *, strip_namespaces: bool = False, max_bytes: Optional[int] = None, huge_tree: bool = False
) -> DocumentReader:
    if fmt == "xml":
        return XmlReader(strip_namespaces=strip_namespaces, max_bytes=max_bytes, huge_tree=huge_tree)
    raise DocumentError(f"no reader for document format {fmt!r}")


def iter_strings(values: Iterable[Optional[str]]) -> List[str]:
    return [v for v in values if v is not None]
