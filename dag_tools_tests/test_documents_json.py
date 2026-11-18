"""JSON documents: the reader, the cross-format equivalence and the JSON fixtures.

Synthetic parcel-depot vocabulary only. The XML twin of every case here lives
in ``test_documents_engine.py``; ``test_the_json_depot_reproduces_the_xml_depot_row_for_row``
is the one that ties the two routes together.
"""
import copy
import json
import logging
import os
import time

import pytest

pytest.importorskip("lxml")
pytest.importorskip("pyarrow")
pytest.importorskip("PIL")
jmespath = pytest.importorskip("jmespath")

from dag_tools.documents.engine import run_bundle  # noqa: E402
from dag_tools.documents.mapping import MappingError, load_mapping  # noqa: E402
from dag_tools.documents.profiler import run_profile  # noqa: E402
from dag_tools.documents.reader import (  # noqa: E402
    JsonReader,
    json_path_key,
    json_structural_paths,
    path_spelling_key,
    xml_structural_paths,
)
from dag_tools_tests.documents_json_synthetic import (  # noqa: E402
    JSON_MAPPING,
    RICH_DOC,
    RICH_MAPPING,
    json_bulk_manifest,
    json_bundle_files,
    rich_json_files,
)
from dag_tools_tests.documents_synthetic import (  # noqa: E402
    EXPECTED_ROWS,
    MAPPING,
    TABLES,
    bundle_files,
    expected_photo_resolution,
    write_zip,
)
from dag_tools_tests.test_documents_engine import (  # noqa: E402
    INGESTED,
    Collect,
    compiled,
    mem_bundle,
    run_small,
)

SYSTEM = {"doc_id", "source_file", "row_id"}


def jm(columns, rows="items[]", **extra):
    m = {
        "version": 1,
        "document": {"format": "json", "match": ["*.json", "**/*.json"], "max_bytes": 100_000},
        "tables": {"t": {"rows": rows, "columns": columns}},
    }
    m.update(extra)
    return m


def values(sink, column, table="t"):
    return [r[column] for r in sink.rows(table)]


# ------------------------------------------------------------------ equivalence


def _normalised(rows, doc_ids):
    """Rows without ``doc_id`` / ``source_file``; ``row_id`` with its doc id spelled ``<doc>``."""
    out = []
    for r in rows:
        r = {k: v for k, v in r.items() if k not in ("doc_id", "source_file")}
        rid = r.get("row_id")
        if rid is not None:
            for d in doc_ids:
                rid = rid.replace(d, "<doc>")
            r["row_id"] = rid
        out.append(r)
    return out


@pytest.mark.parametrize("variant", ["flat", "numbered"])
def test_the_json_depot_reproduces_the_xml_depot_row_for_row(variant):
    """Compared: every column of every table except ``doc_id`` and ``source_file``
    (they name the file, so differ by the manifest's extension) and ``row_id``
    where it embeds the doc id (compared with the id masked); rows in order;
    the ``_rejects`` count and its non-name columns; coercion failures; and the
    photo file resolution outcomes. Types are compared by value, so a column that
    came back as a different Python type would fail."""
    _, xs, xk = run_small(MAPPING, bundle_files(variant))
    _, js, jk = run_small(JSON_MAPPING, json_bundle_files(variant))
    assert xs.rows == js.rows
    if variant == "flat":
        assert js.rows == EXPECTED_ROWS
    for table in TABLES:
        xr, jr = xk.rows(table), jk.rows(table)
        assert len(xr) == len(jr) > 0
        xd = {r["doc_id"] for r in xr}
        jd = {r["doc_id"] for r in jr}
        assert _normalised(xr, xd) == _normalised(jr, jd), table
        assert {r["source_file"] for r in xr} == {r["source_file"].replace(".json", ".xml") for r in jr}
    assert xs.coercion_failures == js.coercion_failures
    assert xs.file_resolution == js.file_resolution
    assert js.file_resolution == {"photos": {"image": expected_photo_resolution(variant)}}
    assert xs.documents_rejected == js.documents_rejected == 1
    (xrej,), (jrej,) = xk.rows("_rejects"), jk.rows("_rejects")
    assert xrej["source_file"] == "broken.xml" and jrej["source_file"] == "broken.json"
    for k in ("source_bundle", "publish_period", "ingested_at"):
        assert xrej[k] == jrej[k]
    assert js.unknown_paths == set()


def test_a_json_reject_message_names_a_line_and_column_and_no_text(caplog):
    caplog.set_level(logging.INFO)
    secret = "zzplantedvaluezz"
    files = {"bad.json": ('{"%s": 1, nope' % secret).encode()}
    _, st, sk = run_small(jm({"v": "v"}), files)
    (rej,) = sk.rows("_rejects")
    assert rej["error"].startswith("not valid JSON: ")
    assert "line 1 column" in rej["error"]
    assert secret not in rej["error"]
    assert secret not in caplog.text
    assert st.documents_rejected == 1
    assert sk.rows("t") == []


# ------------------------------------------------------------------ documents with nothing in them


@pytest.mark.parametrize(
    "raw",
    [b"{}", b"[]", b"3", b'"text"', b"null", b"", b"   \n ", b"\xef\xbb\xbf"],
    ids=["empty_object", "empty_array", "bare_number", "bare_string", "null", "zero_bytes", "whitespace", "bom_only"],
)
def test_a_document_with_nothing_to_select_is_no_rows_not_a_reject(raw):
    _, st, sk = run_small(jm({"v": "v"}), {"a.json": raw})
    assert sk.rows("t") == []
    assert sk.rows("_rejects") == []
    assert st.documents == st.documents_parsed == 1
    assert st.documents_rejected == 0


def test_a_document_that_is_not_utf8_is_a_reject_that_quotes_nothing():
    _, st, sk = run_small(jm({"v": "v"}), {"a.json": b'{"v": "\xff\xfe"}'})
    (rej,) = sk.rows("_rejects")
    assert rej["error"] == "document is not valid UTF-8"


def test_a_byte_order_mark_is_accepted():
    _, st, sk = run_small(jm({"v": {"type": "int32", "expr": "v"}}), {"a.json": b'\xef\xbb\xbf{"items":[{"v":1}]}'})
    assert values(sk, "v") == [1]
    assert st.documents_rejected == 0


def test_a_document_over_max_bytes_is_a_reject():
    m = jm({"v": "v"})
    m["document"]["max_bytes"] = 10
    _, st, sk = run_small(m, {"a.json": b'{"items":[{"v":1}]}'})
    (rej,) = sk.rows("_rejects")
    assert "max_bytes" in rej["error"]


def test_trailing_text_after_a_document_is_a_reject():
    _, st, sk = run_small(jm({"v": "v"}), {"a.json": b'{"items":[]} trailing'})
    (rej,) = sk.rows("_rejects")
    assert rej["error"] == "not valid JSON: Extra data at line 1 column 14"


# ------------------------------------------------------------------ native types

MIXED = [3, 3.0, 3.5, True, False, "4", "true", " 5 ", "abc", 2**40, {"a": 1}, [1], None, "1e999", -0.0, "3.0"]
MIXED_DOC = json.dumps({"items": [{"v": v} for v in MIXED]}).encode()


def test_a_json_number_fills_int32_natively_and_a_fraction_is_counted():
    _, st, sk = run_small(jm({"v": {"type": "int32", "expr": "v"}}), {"a.json": MIXED_DOC})
    got = values(sk, "v")
    assert got[0] == 3 and got[1] == 3  # 3 and 3.0
    assert got[2] is None  # 3.5
    assert got[5] == 4 and got[7] == 5  # text parsing: "4", " 5 "
    assert got[9] is None  # 2**40 does not fit
    assert got[8] is None and got[10] is None  # "abc", a dict
    assert got[12] is None  # a null is absent
    assert got[14] == 0  # -0.0
    # observed: a one-value list is read as its first value
    assert got[11] == 1
    # observed: ``true`` is not a number, and neither is the text "3.0" for an int
    assert got[3] is None and got[4] is None and got[6] is None and got[15] is None
    # counted (and only those): 3.5, true, false, "true", "abc", 2**40, {}, "1e999", "3.0"
    assert st.coercion_failures == {"t": {"v": 9}}


def test_true_fills_bool_and_the_text_true_does_too():
    _, st, sk = run_small(jm({"v": {"type": "bool", "expr": "v"}}), {"a.json": MIXED_DOC})
    got = values(sk, "v")
    assert got[3] is True and got[4] is False and got[6] is True
    assert got[0] is None  # observed: a number is not a bool
    assert st.coercion_failures["t"]["v"] == 11


def test_native_numbers_fill_float64_and_text_uses_text_parsing():
    _, st, sk = run_small(jm({"v": {"type": "float64", "expr": "v"}}), {"a.json": MIXED_DOC})
    got = values(sk, "v")
    assert got[:3] == [3.0, 3.0, 3.5]
    assert got[5] == 4.0 and got[15] == 3.0
    assert got[9] == float(2**40)
    assert got[3] is None and got[10] is None and got[13] is None
    assert st.coercion_failures["t"]["v"] == 6


def test_a_dict_into_a_string_column_is_counted_and_a_number_is_its_text():
    _, st, sk = run_small(jm({"v": "v"}), {"a.json": MIXED_DOC})
    got = values(sk, "v")
    assert got[0] == "3" and got[2] == "3.5" and got[3] == "true" and got[9] == str(2**40)
    assert got[10] is None
    assert st.coercion_failures == {"t": {"v": 1}}


@pytest.mark.parametrize("raw", [b"NaN", b"Infinity", b"-Infinity"])
def test_a_non_finite_literal_is_a_reject_never_a_float(raw):
    doc = b'{"items":[{"v":' + raw + b"}]}"
    _, st, sk = run_small(jm({"v": {"type": "float64", "expr": "v"}}), {"a.json": doc})
    assert sk.rows("t") == []
    (rej,) = sk.rows("_rejects")
    assert rej["error"] == "not valid JSON: NaN and Infinity are not JSON numbers"


def test_a_number_too_big_for_a_float_is_a_counted_failure_never_infinity():
    _, st, sk = run_small(jm({"v": {"type": "float64", "expr": "v"}}), {"a.json": b'{"items":[{"v":1e999}]}'})
    assert values(sk, "v") == [None]
    assert st.coercion_failures == {"t": {"v": 1}}
    assert st.documents_rejected == 0


def test_the_text_nan_in_a_float_column_is_a_counted_failure():
    _, st, sk = run_small(jm({"v": {"type": "float64", "expr": "v"}}), {"a.json": b'{"items":[{"v":"NaN"}]}'})
    assert values(sk, "v") == [None]
    assert st.coercion_failures == {"t": {"v": 1}}


# ------------------------------------------------------------------ awkward keys

KEYED = {
    "Depot": {"Dock door": [{"x": 1}, {"x": 2}], "Count(late)": [{"y": "a"}], "a.b": {"c": 5}},
    "quote\"d": 1,
    "héllo": {"w": 2},
}


def test_a_non_identifier_key_is_spelled_quoted_in_a_structural_path():
    assert json_path_key("plain_1") == ".plain_1"
    assert json_path_key("Dock door") == '."Dock door"'
    assert json_path_key("Count(late)") == '."Count(late)"'
    assert json_path_key("a.b") == '."a.b"'
    assert json_path_key('quote"d') == '."quote\\"d"'
    assert json_path_key("1st") == '."1st"'
    assert json_path_key("héllo") == '."héllo"'


def test_the_structural_paths_of_awkward_keys_use_the_spelling_an_expression_uses():
    paths = JsonReader().structural_paths(KEYED)
    assert '$.Depot."Dock door"' in paths
    assert '$.Depot."Dock door"[]' in paths
    assert '$.Depot."Dock door"[].x' in paths
    assert '$.Depot."Count(late)"[].y' in paths
    assert '$.Depot."a.b".c' in paths
    assert '$."quote\\"d"' in paths
    assert '$."héllo".w' in paths
    assert "$" in paths


def test_an_awkward_key_works_as_a_column_expression_and_in_rows():
    doc = json.dumps(KEYED).encode()
    m = {
        "version": 1,
        "document": {"format": "json", "match": ["*.json"], "max_bytes": 100_000},
        "tables": {
            "door": {"rows": 'Depot."Dock door"[]', "columns": {"x": {"type": "int32", "expr": "x"},
                                                                 "c": {"type": "int32", "from": "root", "expr": 'Depot."a.b".c'}}},
            "late": {"rows": 'Depot."Count(late)"[]', "columns": {"y": "y"}},
            "one": {"rows": "@", "columns": {
                "dotted": {"type": "int32", "expr": 'Depot."a.b".c'},
                "q": {"type": "int32", "expr": '"quote\\"d"'},
                "h": {"type": "int32", "expr": '"héllo".w'},
            }},
        },
    }
    _, st, sk = run_small(m, {"a.json": doc})
    assert [r["x"] for r in sk.rows("door")] == [1, 2]
    assert [r["c"] for r in sk.rows("door")] == [5, 5]
    assert [r["y"] for r in sk.rows("late")] == ["a"]
    (one,) = sk.rows("one")
    assert (one["dotted"], one["q"], one["h"]) == (5, 1, 2)
    assert st.coercion_failures == {}


def test_expected_paths_accepts_the_awkward_spelling_and_a_missing_one_is_unknown():
    doc = json.dumps(KEYED).encode()
    reader_paths = sorted(JsonReader().structural_paths(KEYED))
    base = jm({"v": "Depot.x"}, rows="@", on_unknown_path="warn")
    base["document"]["match"] = ["*.json"]
    ok = copy.deepcopy(base)
    ok["expected_paths"] = reader_paths
    _, st, _ = run_small(ok, {"a.json": doc})
    assert st.unknown_paths == set() and st.documents_with_unknown_paths == 0
    short = copy.deepcopy(base)
    short["expected_paths"] = [p for p in reader_paths if p != '$.Depot."Dock door"[].x']
    _, st, _ = run_small(short, {"a.json": doc})
    assert st.unknown_paths == {'$.Depot."Dock door"[].x'}


def test_expected_paths_must_start_with_the_json_root():
    m = jm({"v": "v"})
    m["expected_paths"] = ["/Manifest"]
    with pytest.raises(MappingError, match="expected_paths"):
        load_mapping(copy.deepcopy(m))


# ------------------------------------------------------------------ flattening


def test_a_nested_list_flattens_with_a_double_bracket_path():
    doc = {"a": [[1, 2], [3], []]}
    paths = JsonReader().structural_paths(doc)
    assert paths == {"$", "$.a", "$.a[]", "$.a[][]"}
    m = jm({"v": {"type": "int32", "expr": "@"}}, rows="a[][]")
    _, st, sk = run_small(m, {"a.json": json.dumps(doc).encode()})
    assert values(sk, "v") == [1, 2, 3]


def test_a_list_nested_three_deep_is_three_brackets():
    assert "$.a[][][]" in JsonReader().structural_paths({"a": [[[1]]]})


def test_an_empty_list_still_has_its_bracket_path():
    assert JsonReader().structural_paths({"a": []}) == {"$", "$.a", "$.a[]"}


def test_a_document_with_no_root_has_no_structural_paths():
    assert JsonReader().structural_paths(None) == set()


# ------------------------------------------------------------------ fast path vs JMESPath

PROBE = {
    "id": "R",
    "n": 4,
    "nothing": None,
    "flag": False,
    "zero": 0,
    "empty": "",
    "list": [1, None, 3],
    "objs": [{"k": "a"}, {"k": None}, {"x": 1}, 5],
    "nest": [[1, 2], [3]],
    "Dock door": {"Count(late)": {"deep": 1}, "a.b": 2},
    "quote\"d": 9,
    "o": {"p": {"q": [1, 2]}, "list": [{"r": 1}]},
}

FAST_EXPRS = [
    "id", "n", "nothing", "flag", "zero", "empty", "list", "objs", "nest", "missing", "missing.deeper",
    "o", "o.p", "o.p.q", "o.p.q.r", "o.list", "o.list.r", "id.x", "n.x", "nothing.x", "list.x",
    '"Dock door"', '"Dock door"."Count(late)"', '"Dock door"."Count(late)".deep', '"Dock door"."a.b"',
    '"quote\\"d"', '"missing key"', "@", "  id  ",
]
SLOW_EXPRS = [
    "objs[].k", "objs[*].k", "list[0]", "list[-1]", "nest[]", "nest[][]", "nest[0]", "nest[*][0]",
    "objs[?k=='a']", "o.list[].r", "length(list)", "length(missing)", "list[?@ > `1`]", "id || n", "[id, n]",
    "{a: id}", "keys(o)", "missing[]", "nothing[]", "n[]", "objs[].x", "o.p.q[]", "o.p.q[0]",
]


def _every_node(v):
    yield v
    if isinstance(v, dict):
        for c in v.values():
            yield from _every_node(c)
    elif isinstance(v, list):
        for c in v:
            yield from _every_node(c)


def _both(c, expr, node):
    """The compiled answer and jmespath's; an error on one side must be an error on the other."""
    try:
        want = ("ok", jmespath.search(expr, node))
    except Exception:
        want = ("error", None)
    try:
        got = ("ok", c._run(node))
    except Exception:
        got = ("error", None)
    return got, want


@pytest.mark.parametrize("expr", FAST_EXPRS)
def test_the_fast_path_answers_what_jmespath_answers_on_every_node(expr):
    c = JsonReader().compile(expr)
    assert c._keys is not None, "this expression was meant to take the fast path"
    for node in _every_node(PROBE):
        got, want = _both(c, expr, node)
        assert got == want


@pytest.mark.parametrize("expr", SLOW_EXPRS)
def test_an_expression_that_is_not_a_key_chain_takes_jmespath_and_agrees_with_it(expr):
    c = JsonReader().compile(expr)
    assert c._keys is None
    for node in _every_node(PROBE):
        got, want = _both(c, expr, node)
        assert got == want


def test_every_expression_the_depot_mapping_uses_agrees_with_jmespath():
    """The mapping's own expressions, on every node of the depot document it reads."""
    doc = json.loads(json_bundle_files("flat")["manifest_m1.json"])
    reader = JsonReader()
    exprs = set()

    def collect(spec):
        if isinstance(spec, str):
            exprs.add(spec)
        elif isinstance(spec, dict):
            for k, v in spec.items():
                if k in ("expr", "rows", "keys", "scope") and isinstance(v, str):
                    exprs.add(v)
                elif k == "first_of":
                    exprs.update(v)
                elif isinstance(v, (dict, list)):
                    collect(v)
        elif isinstance(spec, list):
            for v in spec:
                collect(v)

    for t in JSON_MAPPING["tables"].values():
        for col in t["columns"].values():
            collect(col)
    exprs -= {"@", "root"}
    assert len(exprs) > 20
    for expr in sorted(exprs):
        if " " in expr and "?" not in expr and "(" not in expr:
            continue
        try:
            jmespath.compile(expr)
        except Exception:
            continue
        c = reader.compile(expr)
        for node in _every_node(doc):
            got, want = _both(c, expr, node)
            assert got == want, expr


def test_an_expression_that_does_not_compile_is_a_load_failure():
    with pytest.raises(Exception, match="does not compile"):
        compiled(jm({"v": "items[?"}))


# ------------------------------------------------------------------ the rich depot


def test_the_rich_depot_reads_awkward_keys_and_mixed_numbers_and_chains():
    _, st, sk = run_small(RICH_MAPPING, rich_json_files())
    parcels = sk.rows("parcels")
    assert [(p["id"], p["door"], p["late"], p["dotted"]) for p in parcels] == [("P1", 7, 4, "dotted")] * 1 + [
        ("P2", 7, 4, "dotted"), ("P3", 7, 4, "dotted")]
    assert [p["qty"] for p in parcels] == [3, 7, 3]  # 3, "7", 3.0
    assert [p["ratio"] for p in parcels] == [2.5, 4.0, None]  # "2.5", 4, "abc"
    assert st.coercion_failures == {"parcels": {"ratio": 1}}
    assert [r["n"] for r in sk.rows("scans")] == [1, 2, 5]  # 1, "2", 5
    assert [r["ok"] for r in sk.rows("scans")] == [True, False, True]  # true, false, "true"
    assert [r["v"] for r in sk.rows("grid")] == [1, 2, 3, 4]


def test_the_rich_depot_empty_object_zero_bytes_and_bare_number_files_are_read_and_yield_nothing():
    _, st, sk = run_small(RICH_MAPPING, rich_json_files())
    assert st.documents == st.documents_parsed == 5
    assert st.documents_rejected == 0
    assert sk.rows("_rejects") == []
    assert {r["source_file"] for r in sk.rows("parcels")} == {"d/depot.json"}


def test_the_rich_depot_splits_extracts_and_resolves_a_folder_the_bundle_lacks():
    _, st, sk = run_small(RICH_MAPPING, rich_json_files())
    p1, p2, p3 = sk.rows("parcels")
    assert p1["tags"] == ["red", "blue", "green"]
    assert p2["tags"] == [] and p3["tags"] == []  # "" and null
    assert [p["code"] for p in (p1, p2, p3)] == [42, None, 7]
    assert st.unmapped_values == {"parcels": {"code": 1}}
    assert (p1["photo_resolution"], p1["photo_member"]) == ("basename_in", "d/p1.png")
    assert (p2["photo_resolution"], p2["photo_member"]) == ("basename_in", "d/p2.png")
    assert (p3["photo_resolution"], p3["photo_member"], p3["photo"]) == ("missing", None, None)
    assert st.file_resolution == {"parcels": {"photo": {"basename_in": 2, "missing": 1}}}


def test_the_rich_depot_positions_restart_within_each_parent():
    _, _, sk = run_small(RICH_MAPPING, rich_json_files())
    assert [(r["pid"], r["ppos"], r["pos"]) for r in sk.rows("scans")] == [("P1", 0, 0), ("P1", 0, 1), ("P3", 2, 0)]


def test_a_file_the_depot_never_mentions_is_not_an_error_and_not_a_row():
    _, st, sk = run_small(RICH_MAPPING, rich_json_files())
    assert "stray.png" not in {r["photo_member"] for r in sk.rows("parcels")}
    assert st.documents_rejected == 0


def test_no_document_value_reaches_the_log_at_info(caplog):
    caplog.set_level(logging.INFO)
    run_small(RICH_MAPPING, rich_json_files())
    for planted in ("ZX-0042", "outbound/p1.png", "red; blue", "dotted"):
        assert planted not in caplog.text


# ------------------------------------------------------------------ the profiler round trip


def test_the_profilers_path_files_used_as_expected_paths_leave_nothing_unknown(tmp_path):
    files = json_bundle_files("flat")
    files.pop("broken.json")
    files["d/depot.json"] = rich_json_files()["d/depot.json"]
    files["d/zero.json"] = b""
    src = tmp_path / "src"
    write_zip(src / "b_2026-03.zip", files)
    out = tmp_path / "out"
    res = run_profile(source=str(src), out=out, install_signal_handlers=False)
    assert res.errors == 0
    path_files = sorted((out / "paths").glob("*.txt"))
    assert path_files
    expected = sorted({ln for f in path_files for ln in f.read_text(encoding="utf-8").splitlines() if ln})
    assert '$.Depot."Dock door"' in expected and "$.parcels[].scans[].readings[].kind" in expected

    m = copy.deepcopy(JSON_MAPPING)
    m["document"]["match"] = ["*.json", "*/*.json", "d/*.json"]
    m["expected_paths"] = expected
    m["on_unknown_path"] = "warn"
    cm = compiled(m)
    sink = Collect()
    with_files = mem_bundle(files)
    st = run_bundle(cm, with_files, sink, ingested_at=INGESTED)
    assert st.documents_rejected == 0
    assert st.unknown_paths == set()
    assert st.documents_with_unknown_paths == 0

    # control: a listing missing one path reports exactly that path
    m["expected_paths"] = [p for p in expected if p != "$.parcels[].scans[].readings[].kind"]
    st = run_bundle(compiled(m), mem_bundle(files), Collect(), ingested_at=INGESTED)
    assert st.unknown_paths == {"$.parcels[].scans[].readings[].kind"}


# ------------------------------------------------------------------ path spelling (one function, both sides)


def _profiled_paths(tmp_path, files):
    src = tmp_path / "src"
    write_zip(src / "b_2026-03.zip", files)
    out = tmp_path / "out"
    res = run_profile(source=str(src), out=out, install_signal_handlers=False)
    assert res.errors == 0
    path_files = sorted((out / "paths").glob("*.txt"))
    assert path_files
    return sorted({ln for f in path_files for ln in f.read_text(encoding="utf-8").splitlines() if ln})


def _rich_json_bundle_files():
    files = json_bundle_files("flat")
    files.pop("broken.json")
    files["d/depot.json"] = rich_json_files()["d/depot.json"]
    return files


def _rich_json_mapping(expected, policy="warn"):
    m = copy.deepcopy(JSON_MAPPING)
    m["document"]["match"] = ["*.json", "*/*.json", "d/*.json"]
    m["expected_paths"] = expected
    m["on_unknown_path"] = policy
    return m


def test_the_profilers_json_path_list_is_exactly_the_engines_structural_paths(tmp_path):
    files = _rich_json_bundle_files()
    listed = _profiled_paths(tmp_path, files)
    engine = set()
    for name, data in files.items():
        if name.endswith(".json"):
            engine |= json_structural_paths(json.loads(data)) if data.strip() else set()
    assert set(listed) == engine
    # the keys that are not identifiers are written quoted, as the engine writes them
    for key in ("Dock door", "Count(late)", "a.b", 'quote"d'):
        assert "$.Depot" + json_path_key(key) in listed
        assert "$.Depot." + key not in listed


def test_the_profilers_xml_path_list_used_as_expected_paths_leaves_nothing_unknown(tmp_path):
    files = bundle_files("flat")
    files.pop("broken.xml")
    listed = _profiled_paths(tmp_path, files)
    from lxml import etree

    engine = set()
    for name, data in files.items():
        if name.endswith(".xml"):
            engine |= xml_structural_paths(etree.fromstring(data))
    assert set(listed) == engine
    assert "/Manifest/Parcel/Scan/Reading/@kind" in listed

    m = copy.deepcopy(MAPPING)
    m["expected_paths"] = listed
    m["on_unknown_path"] = "warn"
    st = run_bundle(compiled(m), mem_bundle(files), Collect(), ingested_at=INGESTED)
    assert st.documents_rejected == 0 and st.documents_parsed > 0
    assert st.unknown_paths == set()
    assert st.documents_with_unknown_paths == 0
    assert st.documents_with_path_spelling_mismatch == 0

    # control: a listing missing one path reports exactly that path, and it is
    # a structural difference, not a spelling one
    m["expected_paths"] = [p for p in listed if p != "/Manifest/Parcel/Scan/Reading/@kind"]
    st = run_bundle(compiled(m), mem_bundle(files), Collect(), ingested_at=INGESTED)
    assert st.unknown_paths == {"/Manifest/Parcel/Scan/Reading/@kind"}
    assert st.documents_with_path_spelling_mismatch == 0


def _misspelt(listed):
    out = [path_spelling_key(p) for p in listed]
    assert out != listed  # the list really does carry a quoted key
    return out


def test_an_unquoted_path_list_is_diagnosed_once_per_bundle_naming_no_value(tmp_path, caplog):
    files = _rich_json_bundle_files()
    listed = _profiled_paths(tmp_path, files)
    caplog.clear()
    caplog.set_level(logging.DEBUG)
    st = run_bundle(compiled(_rich_json_mapping(_misspelt(listed))), mem_bundle(files), Collect(),
                    ingested_at=INGESTED)
    assert st.documents_with_unknown_paths == 1
    assert st.documents_with_path_spelling_mismatch == 1
    hints = [r for r in caplog.records if "different key spelling" in r.getMessage()]
    assert len(hints) == 1 and hints[0].levelno == logging.WARNING
    at_warning = "\n".join(r.getMessage() for r in caplog.records if r.levelno >= logging.INFO)
    for planted in ("Dock door", "Count(late)", "dotted", 'quote"d'):
        assert planted not in at_warning


def test_a_structural_difference_is_not_reported_as_a_spelling_one(tmp_path, caplog):
    files = _rich_json_bundle_files()
    listed = _profiled_paths(tmp_path, files)
    # unquoted AND short a real path of the same document: not every unknown path
    # of that document is a spelling difference, so no hint
    short = [p for p in _misspelt(listed) if p != "$.Depot"]
    caplog.set_level(logging.DEBUG)
    st = run_bundle(compiled(_rich_json_mapping(short)), mem_bundle(files), Collect(), ingested_at=INGESTED)
    assert st.documents_with_unknown_paths >= 1
    assert st.documents_with_path_spelling_mismatch == 0
    assert "different key spelling" not in caplog.text


def test_under_fail_the_reject_carries_the_spelling_hint_and_no_path(tmp_path):
    files = _rich_json_bundle_files()
    listed = _profiled_paths(tmp_path, files)
    sink = Collect()
    st = run_bundle(compiled(_rich_json_mapping(_misspelt(listed), policy="fail")), mem_bundle(files), sink,
                    ingested_at=INGESTED)
    assert st.documents_rejected == 1
    (reject,) = sink.rows("_rejects")
    assert reject["source_file"] == "d/depot.json"
    assert "different key spelling" in reject["error"]
    assert "Dock door" not in reject["error"]


def test_path_spelling_key_unquotes_only_quoted_steps():
    assert path_spelling_key('$.depot."Dock door".x') == "$.depot.Dock door.x"
    assert path_spelling_key('$."a\\"b"') == '$.a"b'
    assert path_spelling_key("/Manifest/@id") == "/Manifest/@id"
    assert path_spelling_key("$.a[].b") == "$.a[].b"


def test_a_scrub_that_changes_a_path_line_says_so(tmp_path, caplog):
    files = _rich_json_bundle_files()
    src = tmp_path / "src"
    write_zip(src / "b_2026-03.zip", files)
    caplog.set_level(logging.WARNING)
    run_profile(source=str(src), out=tmp_path / "out", scrub=["Dock"], install_signal_handlers=False)
    assert "no longer match the engine's spelling" in caplog.text


# ------------------------------------------------------------------ benchmark


def test_json_throughput_on_a_bulk_manifest(record_property, capsys):
    n = 20_000  # the same parcels, scans and flags as the XML benchmark
    m = copy.deepcopy(JSON_MAPPING)
    m["document"]["max_bytes"] = 10**9
    cm = compiled(m)
    rows = {"n": 0}

    def sink(table, batch):
        rows["n"] += batch.num_rows

    bundle = mem_bundle({"bulk.json": json_bulk_manifest(n)}, key="bulk_2026-03")
    t0 = time.monotonic()
    stats = run_bundle(cm, bundle, sink, ingested_at=INGESTED)
    elapsed = time.monotonic() - t0
    assert stats.documents_rejected == 0
    assert rows["n"] == sum(stats.rows.values()) == 1 + n + 4 * n + n
    rate = rows["n"] / elapsed
    record_property("rows_per_second", rate)
    with capsys.disabled():
        print(f"\n[documents json bench] {rows['n']} rows in {elapsed:.2f}s = {rate:,.0f} rows/s")
    assert rate > 0
    if os.environ.get("DAGTOOLS_BENCH") == "1":
        assert rate >= 20_000
