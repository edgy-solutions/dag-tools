"""Mapping validation, fast-path/XPath agreement, vocabulary guard and purity."""
import copy
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("lxml")
pytest.importorskip("pyarrow")
pytest.importorskip("PIL")

from lxml import etree  # noqa: E402

from dag_tools.documents.mapping import MappingError, load_mapping  # noqa: E402
from dag_tools.documents.reader import _XPathExpr  # noqa: E402
from dag_tools_tests.documents_synthetic import MAPPING, bundle_files  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def problems(m):
    with pytest.raises(MappingError) as e:
        load_mapping(m)
    return e.value.problems


def test_the_synthetic_mapping_is_valid():
    load_mapping(copy.deepcopy(MAPPING))


def test_an_unknown_key_is_refused():
    m = copy.deepcopy(MAPPING)
    m["zzz"] = 1
    with pytest.raises(MappingError):
        load_mapping(m)
    m = copy.deepcopy(MAPPING)
    m["tables"]["loads"]["columns"]["site"] = {"expr": "Depot/@site", "bogus": 1}
    with pytest.raises(MappingError):
        load_mapping(m)


def test_a_forward_index_reference_is_refused():
    m = copy.deepcopy(MAPPING)
    m["indexes"]["reviews"]["values"]["x"] = {"lookup": {"index": "photo_refs", "keys": "@id", "mode": "exists"}}
    assert any("photo_refs" in p for p in problems(m))


def test_an_index_cannot_look_into_itself():
    m = copy.deepcopy(MAPPING)
    m["indexes"]["reviews"]["values"]["x"] = {"lookup": {"index": "reviews", "keys": "@id", "mode": "exists"}}
    assert any("reviews" in p for p in problems(m))


def test_an_undefined_var_is_refused():
    m = copy.deepcopy(MAPPING)
    m["tables"]["parcels"]["columns"]["bad"] = "$nope"
    assert any("nope" in p for p in problems(m))


def test_an_unknown_type_is_refused():
    m = copy.deepcopy(MAPPING)
    m["tables"]["loads"]["columns"]["bad"] = {"type": "decimal128", "expr": "@id"}
    assert any("decimal128" in p or "type" in p for p in problems(m))


def test_an_unknown_lookup_index_is_refused():
    m = copy.deepcopy(MAPPING)
    m["tables"]["flags"]["columns"]["bad"] = {"lookup": {"index": "ghost", "keys": "@id", "mode": "exists"}}
    assert any("ghost" in p for p in problems(m))


def test_several_problems_are_all_reported_in_one_error():
    m = copy.deepcopy(MAPPING)
    m["tables"]["parcels"]["columns"]["a"] = "$nope"
    m["tables"]["flags"]["columns"]["b"] = {"lookup": {"index": "ghost", "keys": "@id", "mode": "exists"}}
    m["tables"]["loads"]["columns"]["doc_id"] = "@id"
    m["tables"]["_rejects"] = {"rows": "/x", "columns": {"v": "@v"}}
    ps = problems(m)
    assert len(ps) >= 4
    text = "\n".join(ps)
    for needle in ("nope", "ghost", "doc_id", "_rejects"):
        assert needle in text


def test_a_row_id_naming_no_column_is_refused():
    m = copy.deepcopy(MAPPING)
    m["tables"]["loads"]["row_id"] = "{nothing}"
    assert any("nothing" in p for p in problems(m))


# ------------------------------------------------------------------ 9. fast path equals XPath


def _exprs():
    out = set()
    for t in MAPPING["tables"].values():
        items = [t["rows"], *t["columns"].values(), *t.get("vars", {}).values()]
        for c in items:
            if isinstance(c, str):
                out.add(c)
            elif isinstance(c, dict) and c.get("expr"):
                out.add(c["expr"])
    out |= {
        "@id", "@missing", "string(@id)", "string(@missing)", "Parcel", "Depot", "Missing", "Depot/@site",
        "Depot/@missing", "Missing/@x", "Parcel/@id", "Review", "Extras", "Scan", "Reading/@value",
        "@fragile", "string( @id )", "Photos",
    }
    return sorted(out)


EXPRESSIONS = _exprs()


def _nodes():
    out = []
    for name, data in bundle_files("flat").items():
        if name.endswith(".xml") and name != "broken.xml":
            out.extend(e for e in etree.fromstring(data).iter() if isinstance(e.tag, str))
    return out


def _expr(source):
    sv = etree.XPath("string(.)")
    return _XPathExpr(source, etree, lambda n: str(sv(n)))


def _pair(source):
    fast, slow = _expr(source), _expr(source)
    slow._fast_scalar = slow._fast_strings = slow._fast_nodes = None
    return fast, slow


def _agree(fast, slow, n, source):
    assert fast.scalar(n, {}) == slow.scalar(n, {}), ("scalar", source, n.tag)
    assert fast.strings(n, {}) == slow.strings(n, {}), ("strings", source, n.tag)
    if fast._fast_nodes is not None:
        assert fast.nodes(n, {}) == slow.nodes(n, {}), ("nodes", source, n.tag)


def test_fast_path_equals_xpath_for_every_expression_on_every_node():
    nodes = _nodes()
    assert len(nodes) > 20
    fast_count = 0
    for source in EXPRESSIONS:
        if "$" in source:
            continue
        fast, slow = _pair(source)
        if fast._fast_scalar is not None:
            fast_count += 1
        for n in nodes:
            _agree(fast, slow, n, source)
    assert fast_count >= 4  # anti-vacuity: the fast path really was exercised


def test_at_least_one_mapping_expression_takes_the_fast_path():
    mapped = {e for e in EXPRESSIONS if "$" not in e}
    assert any(_expr(s)._fast_scalar is not None for s in mapped)


def test_every_fast_view_is_exercised_by_the_mapping():
    # Anti-vacuity for the agreement test above: each fast shape is one the
    # synthetic mapping really uses, so the comparison runs over it.
    mapped = [_expr(s) for s in EXPRESSIONS if "$" not in s]
    assert any(e._fast_strings is not None for e in mapped)
    assert any(e._fast_nodes is not None for e in mapped)
    assert any(e.source.startswith("../") and e._fast_scalar is not None for e in mapped)
    assert any("[" in e.source and e._fast_scalar is not None for e in mapped)


@pytest.mark.parametrize(
    "xml, source",
    [
        (b"<a id=''/>", "@id"),
        (b"<a/>", "@id"),
        (b"<a id=''/>", "string(@id)"),
        (b"<a/>", "string(@id)"),
        (b"<a><b>x</b><b>y</b></a>", "b"),
        (b"<a><b/></a>", "b"),
        (b"<a><b><c>x</c>y</b></a>", "b"),
        (b"<a><b> sp </b></a>", "b"),
        (b"<a><b><!--c-->t</b></a>", "b"),
        (b"<a><b>t<c/>u</b></a>", "b"),
        (b"<a><b x='1'/><b x='2'/></a>", "b/@x"),
        (b"<a><b/><b x='2'/></a>", "b/@x"),
        (b"<a><b x=''/></a>", "b/@x"),
        (b"<a><b>&#233;</b></a>", "b"),
        (b"<a><b><![CDATA[<x>]]></b></a>", "b"),
        (b"<a><?pi b?><b>x</b><!--b--><c/><b>y</b></a>", "b"),
        (b"<a xmlns:n='urn:x'><n:b>x</n:b><b>y</b></a>", "b"),
        (b"<a xmlns='urn:x'><b>x</b></a>", "b"),
        (b"<a id='p'><b/></a>", "../@id"),
        (b"<a><b/></a>", "../@id"),
        (b"<a/>", "../@id"),
        (b"<a><b k='t' v='1'/><b k='u' v='2'/><b k='t' v='3'/></a>", "b[@k='t']/@v"),
        (b"<a><b k='t'/><b k='t' v='3'/></a>", "b[@k='t']/@v"),
        (b"<a><b k='T' v='1'/><b v='2'/></a>", 'b[@k="t"]/@v'),
        (b"<a><b k='' v='1'/></a>", "b[@k='']/@v"),
        (b"<a><b k=' t' v='1'/><b k='t' v='2'/></a>", "b[@k = 't']/@v"),
    ],
)
def test_fast_path_edge_cases_equal_xpath(xml, source):
    root = etree.fromstring(xml)
    fast, slow = _pair(source)
    assert fast._fast_scalar is not None
    for n in (e for e in root.iter() if isinstance(e.tag, str)):
        _agree(fast, slow, n, source)


def test_a_var_reference_never_takes_the_fast_path():
    assert _expr("$v")._fast_scalar is None


# ------------------------------------------------------------------ 11. vocabulary guard

GUARDED = sorted(
    list((ROOT / "dag_tools" / "documents").rglob("*.py"))
    + list((ROOT / "dag_tools" / "components" / "document_tables").rglob("*.py"))
)

# Words that would tie the library to one deployment's documents; spelled in
# pieces so this file does not trip its own scan.
FORBIDDEN = [
    "ven" + "dor", "inv" + "oice", "pur" + "chase", "sup" + "plier", "snow" + "flake",
    "system" + "link", "aero" + "space", "tail" + "_number", "part" + "_number",
]


def _hits(text):
    low = text.lower()
    return [w for w in FORBIDDEN if w in low]


def test_the_guard_covers_files():
    assert len(GUARDED) >= 8


@pytest.mark.parametrize("path", GUARDED, ids=lambda p: p.name)
def test_no_deployment_vocabulary_appears_in_the_library(path):
    assert _hits(path.read_text(encoding="utf-8")) == [], path.name


def test_control_the_guard_finds_a_planted_word():
    assert _hits("x = 'Inv" + "oice number'") == ["inv" + "oice"]
    assert _hits("nothing here") == []


# ------------------------------------------------------------------ 12. purity


@pytest.mark.parametrize("module", ["engine", "sources", "lance_writer", "mapping", "reader", "coerce", "files"])
def test_importing_the_engine_modules_imports_no_dagster(module):
    code = (
        "import sys, importlib\n"
        f"importlib.import_module('dag_tools.documents.{module}')\n"
        "bad = sorted(m for m in sys.modules if m == 'dagster' or m.startswith('dagster.'))\n"
        "print('BAD' if bad else 'OK')\n"
    )
    r = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, check=False, cwd=str(ROOT),
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    assert r.returncode == 0, r.stderr[-500:]
    assert r.stdout.strip() == "OK"


def test_control_the_purity_probe_sees_dagster_when_it_is_imported():
    pytest.importorskip("dagster")
    code = "import sys, dagster\nprint('BAD' if 'dagster' in sys.modules else 'OK')"
    r = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, check=False)
    assert r.stdout.strip() == "BAD"


# ------------------------------------------------------------------ scoped indexes


def test_a_lookup_into_a_scoped_index_must_give_a_scope_and_the_error_names_the_column():
    m = copy.deepcopy(MAPPING)
    m["tables"]["scans"]["columns"]["bad"] = {"lookup": {"index": "photo_refs", "keys": "@id", "mode": "exists"}}
    ps = problems(m)
    assert any("scans" in p and "bad" in p and "must give `scope`" in p for p in ps), ps


def test_a_lookup_into_an_unscoped_index_takes_no_scope():
    m = copy.deepcopy(MAPPING)
    m["tables"]["flags"]["columns"]["bad"] = {
        "lookup": {"index": "reviews", "scope": "../@id", "keys": "@id", "mode": "exists"}
    }
    ps = problems(m)
    assert any("flags" in p and "bad" in p and "takes no `scope`" in p for p in ps), ps


def test_an_index_scope_takes_no_var():
    m = copy.deepcopy(MAPPING)
    m["indexes"]["photo_refs"]["scope"] = "$manifest"
    ps = problems(m)
    assert any("photo_refs" in p and "scope" in p for p in ps), ps


def test_a_lookup_inside_an_index_value_follows_the_same_scope_rules():
    m = copy.deepcopy(MAPPING)
    m["indexes"]["reviews"] = {
        "rows": m["indexes"]["reviews"]["rows"],
        "keys": m["indexes"]["reviews"]["keys"],
        "values": {"seen": {"lookup": {"index": "photo_refs", "keys": "@flagRef", "mode": "exists"}}},
    }
    m["indexes"] = {"photo_refs": m["indexes"]["photo_refs"], **{k: v for k, v in m["indexes"].items() if k != "photo_refs"}}
    ps = problems(m)
    assert any("reviews" in p and "seen" in p and "must give `scope`" in p for p in ps), ps


def test_order_keys_is_refused_where_there_is_no_order_to_keep():
    m = copy.deepcopy(MAPPING)
    m["tables"]["flags"]["columns"]["bad"] = {"lookup": {"index": "reviews", "keys": "@id", "mode": "exists", "order": "keys"}}
    ps = problems(m)
    assert any("bad" in p and "order" in p for p in ps), ps


# ------------------------------------------------------------------ prefer


def _with_verdict(**changes):
    m = copy.deepcopy(MAPPING)
    m["tables"]["flags"]["columns"]["verdict"].update(changes)
    return m


def test_prefer_on_a_lookup_returning_one_value_is_refused():
    m = _with_verdict(lookup={"index": "reviews", "keys": "@id", "field": "decision", "mode": "last"})
    assert any("verdict.prefer" in p and "list source" in p for p in problems(m))


def test_prefer_on_a_scalar_expression_source_is_refused():
    m = copy.deepcopy(MAPPING)
    m["tables"]["flags"]["columns"]["verdict"] = {"expr": "string(@code)", "prefer": ["DENT"]}
    assert any("verdict.prefer" in p and "list source" in p for p in problems(m))


def test_an_empty_prefer_is_refused():
    assert any("verdict.prefer" in p for p in problems(_with_verdict(prefer=[])))


def test_prefer_with_first_of_is_refused():
    m = copy.deepcopy(MAPPING)
    m["tables"]["flags"]["columns"]["verdict"] = {"first_of": ["@a", "@b"], "prefer": ["x"]}
    assert any("verdict" in p and "first_of" in p for p in problems(m))


def test_prefer_declares_the_element_type_not_a_list():
    assert any("verdict.prefer" in p and "element type" in p for p in problems(_with_verdict(type="list<string>")))


# ---- document.match globs are compiled at load


@pytest.mark.parametrize(
    "glob, words",
    [
        ("", "may not be empty"),
        ("/d/*.xml", "starts with '/'"),
        (r"d\*.xml", "backslash"),
        ("d**/*.xml", "whole path segment"),
        ("[ab.xml", "unclosed"),
    ],
)
def test_a_glob_that_could_never_mean_what_it_says_fails_the_load(glob, words):
    m = copy.deepcopy(MAPPING)
    m["document"]["match"] = ["*.xml", glob]
    with pytest.raises(MappingError) as e:
        load_mapping(m)
    assert "document.match" in str(e.value) and words in str(e.value)


@pytest.mark.parametrize(
    "glob, path, hit",
    [
        ("m[12].xml", "m1.xml", True),
        ("m[!12].xml", "m1.xml", False),
        ("m[!12].xml", "m3.xml", True),
        ("?.xml", "a.xml", True),
        ("?/a.xml", "//a.xml", False),
        ("*/*.xml", "d/e/a.xml", False),
        ("d/**", "d/e/a.xml", True),
        ("d/**/a.xml", "d/a.xml", True),
        ("a.xml", "a_xml", False),
    ],
)
def test_glob_semantics(glob, path, hit):
    from dag_tools.documents.mapping import compile_glob

    assert bool(compile_glob(glob).match(path)) is hit
