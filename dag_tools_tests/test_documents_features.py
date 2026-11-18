"""split, extract, basename_in, the rows chain and member tables.

Each feature is tested on its own, in the formats it exists in, with its
failure cases and its empty cases beside the happy path. Synthetic parcel-depot
vocabulary only. Where a value was pinned by observation rather than stated by
the design, the test says "observed".
"""
import copy
import json
import logging

import pytest

pytest.importorskip("lxml")
pytest.importorskip("pyarrow")
pytest.importorskip("PIL")
pytest.importorskip("jmespath")

from dag_tools.documents.engine import empty_tables, run_bundle  # noqa: E402
from dag_tools.documents.lance_writer import LanceWriter  # noqa: E402
from dag_tools.documents.mapping import MappingError  # noqa: E402
from dag_tools.documents.sources import BundleRef, Member, OpenBundle, open_source  # noqa: E402
from dag_tools_tests.documents_synthetic import png_bytes, write_tree, write_zip  # noqa: E402
from dag_tools_tests.test_documents_engine import INGESTED, Collect, compiled, run_small  # noqa: E402


def jm(columns, rows="items[]"):
    return {
        "version": 1,
        "document": {"format": "json", "match": ["*.json", "**/*.json"], "max_bytes": 100_000},
        "tables": {"t": {"rows": rows, "columns": columns}},
    }


def xm(columns, rows="/m/i"):
    return {
        "version": 1,
        "document": {"match": ["*.xml"], "max_bytes": 100_000},
        "tables": {"t": {"rows": rows, "columns": columns}},
    }


def items(*vs, key="v"):
    return json.dumps({"items": [{key: v} for v in vs]}).encode()


def col(sink, name, table="t"):
    return [r[name] for r in sink.rows(table)]


def raises_naming(mapping, *needles):
    with pytest.raises(MappingError) as ei:
        compiled(mapping)
    text = str(ei.value)
    for n in needles:
        assert n in text, text
    return text


# ============================================================ split


SPLIT = {"type": "list<string>", "expr": "v", "split": ";"}


def test_split_cuts_on_a_literal_separator_and_trims_the_parts():
    _, st, sk = run_small(jm({"a": SPLIT}), {"a.json": items("red; blue ;green", "a.b;c")})
    assert col(sk, "a") == [["red", "blue", "green"], ["a.b", "c"]]  # the separator is not a pattern


def test_split_drops_empty_parts_by_default():
    _, _, sk = run_small(jm({"a": SPLIT}), {"a.json": items("a;;b", " ; a ; ", ";;;")})
    assert col(sk, "a") == [["a", "b"], ["a"], []]


def test_split_keeps_empty_parts_as_nulls_when_asked():
    # observed: a kept empty part is null, because the column treats empty as null
    c = {"type": "list<string>", "expr": "v", "split": ";", "keep_empty": True}
    _, _, sk = run_small(jm({"a": c}), {"a.json": items("a;;b", " ; a ; ")})
    assert col(sk, "a") == [["a", None, "b"], [None, "a", None]]


@pytest.mark.parametrize("raw", ["", "   ", None])
def test_split_of_an_empty_or_null_value_is_an_empty_list_even_with_keep_empty(raw):
    c = {"type": "list<string>", "expr": "v", "split": ";", "keep_empty": True}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items(raw)})
    assert col(sk, "a") == [[]]
    assert st.coercion_failures == {}


def test_split_of_an_absent_key_is_an_empty_list():
    _, _, sk = run_small(jm({"a": SPLIT}), {"a.json": items("x", key="other")})
    assert col(sk, "a") == [[]]


def test_split_maps_then_coerces_each_part_and_counts_each_part_that_fails():
    c = {"type": "list<int32>", "expr": "v", "split": ";"}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items("1;x;3", "a;;b", "7")})
    assert col(sk, "a") == [[1, None, 3], [None, None], [7]]
    assert st.coercion_failures == {"t": {"a": 3}}


def test_split_with_prefer_reduces_to_the_first_preferred_part_present():
    c = {"type": "string", "expr": "v", "split": ";", "prefer": ["x", "y"]}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items("1;x;3", "y;x", "a;b", "")})
    assert col(sk, "a") == ["x", "x", None, None]
    # observed: a non-empty value with no preferred part is counted; an empty one is not
    assert st.unmapped_values == {"t": {"a": 1}}


def test_split_works_on_an_xml_attribute_too():
    x = b'<m><i v="a; b;;c"/><i v=""/><i/></m>'
    _, _, sk = run_small(xm({"a": {"type": "list<string>", "expr": "@v", "split": ";"}}), {"a.xml": x})
    assert col(sk, "a") == [["a", "b", "c"], [], []]


@pytest.mark.parametrize(
    "spec, needle",
    [
        ({"expr": "v", "split": ";"}, "declare `list<string>`"),
        ({"type": "list<string>", "expr": "v", "split": ""}, "may not be empty"),
        ({"type": "list<string>", "expr": "v", "keep_empty": True}, "`keep_empty` applies only with `split`"),
        ({"type": "list<string>", "index_of": "root", "split": ";"}, "`index_of` is a position"),
    ],
    ids=["scalar_type", "empty_separator", "keep_empty_without_split", "split_of_a_position"],
)
def test_a_split_that_cannot_work_fails_the_load_naming_the_column(spec, needle):
    raises_naming(jm({"a": spec}), "tables.t.columns.a", needle)


# ============================================================ extract


def test_extract_takes_the_named_group_value_else_group_one_else_the_whole_match():
    cols = {
        "named": {"expr": "v", "extract": "(?P<value>[a-z]+)-\\d"},
        "first": {"expr": "v", "extract": "([a-z]+)-(\\d)"},
        "whole": {"expr": "v", "extract": "[a-z]+-\\d"},
    }
    _, _, sk = run_small(jm(cols), {"a.json": items("ab-1")})
    (r,) = sk.rows("t")
    assert (r["named"], r["first"], r["whole"]) == ("ab", "ab", "ab-1")


def test_extract_that_does_not_match_is_null_and_counted():
    c = {"type": "int32", "expr": "v", "extract": "^ZX-(?P<value>[0-9]+)$"}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items("ZX-0042", "nope", "ZX-7x")})
    assert col(sk, "a") == [42, None, None]
    assert st.unmapped_values == {"t": {"a": 2}}
    assert st.coercion_failures == {}


def test_extract_of_a_null_or_absent_value_is_null_and_not_counted():
    c = {"type": "int32", "expr": "v", "extract": "^ZX-([0-9]+)$"}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items(None, "", "ZX-1")})
    assert col(sk, "a") == [None, None, 1]
    assert st.unmapped_values == {}


def test_extract_runs_before_coercion_so_a_non_numeric_match_is_a_coercion_failure():
    c = {"type": "int32", "expr": "v", "extract": "^ZX-(?P<value>.+)$"}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items("ZX-12", "ZX-ab")})
    assert col(sk, "a") == [12, None]
    assert st.coercion_failures == {"t": {"a": 1}}
    assert st.unmapped_values == {}


def test_extract_runs_before_map_and_a_miss_skips_the_maps_default():
    # observed: the miss is null and counted as unmapped; the map's default applies
    # only to an extracted value the table does not know
    c = {"type": "string", "expr": "v", "extract": "^(\\d+)-",
         "map": {"values": {"12": "twelve"}, "default": "other"}}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items("12-a", "13-a", "zz")})
    assert col(sk, "a") == ["twelve", "other", None]
    assert st.unmapped_values == {"t": {"a": 1}}


def test_extract_then_map_without_a_default_counts_both_kinds_of_miss():
    c = {"type": "string", "expr": "v", "extract": "^(\\d+)-", "map": {"values": {"12": "twelve"}}}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items("12-a", "13-a", "zz")})
    assert col(sk, "a") == ["twelve", None, None]
    assert st.unmapped_values == {"t": {"a": 2}}


def test_extract_works_on_an_xml_attribute():
    x = b'<m><i v="ZX-07"/><i v="no"/><i/></m>'
    c = {"type": "int32", "expr": "@v", "extract": "^ZX-(\\d+)$"}
    _, st, sk = run_small(xm({"a": c}), {"a.xml": x})
    assert col(sk, "a") == [7, None, None]
    assert st.unmapped_values == {"t": {"a": 1}}


def test_extract_with_split_applies_the_pattern_to_each_part_after_the_cut():
    # observed: the value is split first and extract runs per part, so a part
    # that does not match is a null element
    c = {"type": "list<string>", "expr": "v", "extract": "^id=(.*)$", "split": ";"}
    _, _, sk = run_small(jm({"a": c}), {"a.json": items("id=a;b")})
    assert col(sk, "a") == [["a", None]]


def test_an_invalid_extract_regex_fails_the_load_naming_the_column():
    raises_naming(jm({"a": {"expr": "v", "extract": "(unclosed"}}), "tables.t.columns.a", "extract regex does not compile")


# ============================================================ basename_in


def _file_map(strategy):
    return jm({"f": {"type": "file", "expr": "f", "resolve": [{"basename_in": strategy}]}})


REFS = ["x/y/p.png", "q.png", "r.png", "nope.png", None, ""]
TREE = {
    "p.png": png_bytes(2, 2),
    "d/p.png": png_bytes(3, 3),
    "d/q.png": png_bytes(4, 4),
    "d/sub/r.png": png_bytes(5, 5),
    "d/a.json": items(*REFS, key="f"),
}


def test_basename_in_document_dir_looks_only_in_the_folder_of_the_document():
    _, st, sk = run_small(_file_map("document_dir"), copy.deepcopy(TREE))
    rows = sk.rows("t")
    assert [(r["f_ref"], r["f_member"], r["f_resolution"]) for r in rows[:4]] == [
        ("x/y/p.png", "d/p.png", "basename_in"),  # the ref's folder is ignored, its basename is used
        ("q.png", "d/q.png", "basename_in"),
        ("r.png", None, "missing"),  # d/sub/r.png is below the folder: never recursed into
        ("nope.png", None, "missing"),
    ]
    assert rows[0]["f"] == TREE["d/p.png"]  # d/p.png, not the p.png at the root
    assert rows[2]["f"] is None
    assert st.file_resolution == {"t": {"f": {"basename_in": 2, "missing": 2}}}


def test_basename_in_bundle_root_looks_only_at_the_root_of_the_bundle():
    _, st, sk = run_small(_file_map("bundle_root"), copy.deepcopy(TREE))
    rows = sk.rows("t")
    assert [(r["f_ref"], r["f_member"], r["f_resolution"]) for r in rows[:4]] == [
        ("x/y/p.png", "p.png", "basename_in"),
        ("q.png", None, "missing"),  # d/q.png is not at the root
        ("r.png", None, "missing"),
        ("nope.png", None, "missing"),
    ]
    assert rows[0]["f"] == TREE["p.png"]
    assert st.file_resolution == {"t": {"f": {"basename_in": 1, "missing": 3}}}


@pytest.mark.parametrize("strategy", ["document_dir", "bundle_root"])
def test_a_null_or_empty_ref_has_no_resolution_and_is_not_counted_missing(strategy):
    _, st, sk = run_small(_file_map(strategy), copy.deepcopy(TREE))
    for r in sk.rows("t")[4:]:
        assert (r["f_ref"], r["f_member"], r["f_resolution"], r["f"]) == (None, None, None, None)
    assert sum(st.file_resolution["t"]["f"].values()) == 4


def test_basename_in_in_a_bundle_with_no_such_file_is_all_missing():
    _, st, sk = run_small(_file_map("document_dir"), {"d/a.json": items("a.png", key="f")})
    assert st.file_resolution == {"t": {"f": {"missing": 1}}}


def test_basename_in_of_a_document_at_the_root_looks_at_the_root():
    _, st, sk = run_small(_file_map("document_dir"), {"a.json": items("z/p.png", key="f"), "p.png": png_bytes(2, 2)})
    assert st.file_resolution == {"t": {"f": {"basename_in": 1}}}


def test_basename_in_works_for_an_xml_document_too():
    x = b'<m><i f="out/p.png"/><i f="r.png"/></m>'
    c = {"f": {"type": "file", "expr": "@f", "resolve": [{"basename_in": "document_dir"}]}}
    files = {"d/a.xml": x, "d/p.png": png_bytes(2, 2), "d/sub/r.png": png_bytes(3, 3)}
    m = xm(c)
    m["document"]["match"] = ["**/*.xml"]
    _, st, sk = run_small(m, files)
    assert [r["f_resolution"] for r in sk.rows("t")] == ["basename_in", "missing"]


def test_basename_in_is_tried_in_order_after_an_earlier_strategy():
    c = {"f": {"type": "file", "expr": "f", "resolve": [{"basename_under": "sub"}, {"basename_in": "document_dir"}]}}
    _, st, sk = run_small(jm(c), {"d/a.json": items("r.png", "p.png", key="f"),
                                  "d/sub/r.png": png_bytes(2, 2), "d/p.png": png_bytes(3, 3)})
    assert [r["f_resolution"] for r in sk.rows("t")] == ["basename_under", "basename_in"]


@pytest.mark.parametrize("bad", ["somewhere", "", "Document_Dir"])
def test_basename_in_with_an_unknown_folder_name_fails_the_load(bad):
    with pytest.raises(MappingError):
        compiled(_file_map(bad))


# ============================================================ the rows chain

CHAIN_DOC = json.dumps({
    "id": "D",
    "parcels": [
        {"id": "P1", "scans": [{"n": 1}, {"n": 2}]},
        {"id": "P2", "scans": []},
        {"id": "P3", "scans": [{"n": 5}]},
        {"id": "P4"},
        {"id": "P5", "scans": None},
    ],
}).encode()


def _chain_json(extra=None):
    cols = {
        "pid": {"from": "p", "expr": "id"},
        "doc": {"from": "root", "expr": "id"},
        "n": {"type": "int32", "expr": "n"},
        "pos": {"index_of": "s"},
        "ppos": {"index_of": "p"},
    }
    cols.update(extra or {})
    return jm(cols, rows=[{"as": "p", "each": "parcels[]"}, {"as": "s", "each": "scans[]"}])


def test_a_chain_emits_one_row_per_node_of_the_last_level_with_its_parents_in_scope():
    _, st, sk = run_small(_chain_json(), {"a.json": CHAIN_DOC})
    assert [(r["pid"], r["doc"], r["n"]) for r in sk.rows("t")] == [("P1", "D", 1), ("P1", "D", 2), ("P3", "D", 5)]
    assert st.rows["t"] == 3


def test_index_of_is_zero_based_within_its_own_level_and_restarts_under_each_parent():
    _, _, sk = run_small(_chain_json(), {"a.json": CHAIN_DOC})
    assert [(r["pos"], r["ppos"]) for r in sk.rows("t")] == [(0, 0), (1, 0), (0, 2)]
    # observed: the parent's index counts the parents that had no children too
    assert [r["ppos"] for r in sk.rows("t")][-1] == 2


def test_a_parent_with_no_children_or_a_null_list_or_no_key_gives_no_rows_and_no_error():
    _, st, sk = run_small(_chain_json(), {"a.json": CHAIN_DOC})
    assert {r["pid"] for r in sk.rows("t")} == {"P1", "P3"}
    assert st.documents_rejected == 0 and st.coercion_failures == {}


def test_a_chain_over_a_document_with_no_first_level_is_no_rows():
    _, st, sk = run_small(_chain_json(), {"a.json": b"{}", "b.json": b"[]", "c.json": b""})
    assert sk.rows("t") == [] and st.documents_rejected == 0


def test_a_chain_can_have_three_levels():
    doc = json.dumps({"a": [{"i": "a1", "b": [{"i": "b1", "c": [{"i": "c1"}, {"i": "c2"}]}, {"i": "b2", "c": [{"i": "c3"}]}]}]}).encode()
    m = jm({"a": {"from": "a", "expr": "i"}, "b": {"from": "b", "expr": "i"}, "c": "i", "ci": {"index_of": "c"},
            "bi": {"index_of": "b"}},
           rows=[{"as": "a", "each": "a[]"}, {"as": "b", "each": "b[]"}, {"as": "c", "each": "c[]"}])
    _, _, sk = run_small(m, {"x.json": doc})
    assert [(r["a"], r["b"], r["c"], r["bi"], r["ci"]) for r in sk.rows("t")] == [
        ("a1", "b1", "c1", 0, 0), ("a1", "b1", "c2", 0, 1), ("a1", "b2", "c3", 1, 0)]


def test_from_root_works_on_a_plain_string_rows_and_other_levels_do_not():
    m = jm({"doc": {"from": "root", "expr": "id"}, "v": "n"}, rows="parcels[].scans[]")
    _, _, sk = run_small(m, {"a.json": CHAIN_DOC})
    assert col(sk, "doc") == ["D", "D", "D"]
    raises_naming(jm({"x": {"from": "p", "expr": "id"}}), "tables.t.columns.x.from", "`from: p` needs a `rows` chain")


def test_a_from_on_first_of_a_lookup_key_and_a_struct_rows_reads_from_that_level():
    doc = json.dumps({"id": "D", "ps": [{"k": "P1", "ss": [{"n": 1}]}, {"k": "P2", "ss": [{"n": 2}, {"n": 3}]}]}).encode()
    cols = {
        "first": {"from": "p", "first_of": ["missing", "k"]},
        "docid": {"from": "root", "first_of": ["nope", "id"]},
        "ns": {"type": "list<struct>", "from": "p", "rows": "ss[]", "fields": {"n": {"type": "int32", "expr": "n"}}},
    }
    m = jm(cols, rows=[{"as": "p", "each": "ps[]"}])
    _, _, sk = run_small(m, {"a.json": doc})
    assert col(sk, "first") == ["P1", "P2"]
    assert col(sk, "docid") == ["D", "D"]
    assert col(sk, "ns") == [[{"n": 1}], [{"n": 2}, {"n": 3}]]


def test_an_index_takes_a_chain_and_a_lookup_reads_its_keys_and_scope_from_a_level():
    doc = json.dumps({
        "ps": [{"k": "P1", "ss": [{"id": "S1", "of": "P1"}, {"id": "S2", "of": "P1"}]},
               {"k": "P2", "ss": [{"id": "S1", "of": "P2"}]}],
        "wanted": [{"p": "P1", "s": "S1"}, {"p": "P2", "s": "S1"}, {"p": "P2", "s": "S9"}],
    }).encode()
    m = jm({"p": "p", "s": "s",
            "hit": {"type": "bool", "lookup": {"index": "seen", "scope": "p", "keys": "s", "mode": "exists"}}},
           rows="wanted[]")
    m["indexes"] = {"seen": {"rows": [{"as": "pp", "each": "ps[]"}, {"as": "ss", "each": "ss[]"}],
                             "scope": "of", "keys": "id"}}
    _, _, sk = run_small(m, {"a.json": doc})
    assert col(sk, "hit") == [True, True, False]


def test_the_same_chain_works_over_xml_and_from_p_replaces_the_parent_axis():
    x = (b'<Manifest id="M"><Parcel id="P1"><Scan n="1"/><Scan n="2"/></Parcel>'
         b'<Parcel id="P2"><Scan n="3"/></Parcel><Parcel id="P3"/></Manifest>')
    cols = {
        "pid": {"from": "p", "expr": "@id"},
        "mid": {"from": "root", "expr": "/Manifest/@id"},
        "n": {"type": "int32", "expr": "@n"},
        "pos": {"index_of": "s"},
        "ppos": {"index_of": "p"},
        "up": "../@id",
    }
    m = xm(cols, rows=[{"as": "p", "each": "/Manifest/Parcel"}, {"as": "s", "each": "Scan"}])
    _, st, sk = run_small(m, {"a.xml": x, "e.xml": b"<Manifest/>"})
    assert [(r["pid"], r["mid"], r["n"], r["pos"], r["ppos"]) for r in sk.rows("t")] == [
        ("P1", "M", 1, 0, 0), ("P1", "M", 2, 1, 0), ("P2", "M", 3, 0, 1)]
    assert col(sk, "up") == col(sk, "pid")  # the old spelling and the new one agree
    assert st.rows["t"] == 3


@pytest.mark.parametrize(
    "tables, needle",
    [
        ({"x": {"from": "zz", "expr": "id"}}, "unknown level 'zz'; the levels here are root, p"),
        ({"x": {"index_of": "zz"}}, "unknown level 'zz'; `index_of` names a level of the chain: p"),
    ],
    ids=["from_unknown_level", "index_of_unknown_level"],
)
def test_a_column_naming_a_level_the_chain_lacks_fails_the_load_naming_the_levels(tables, needle):
    m = jm(tables, rows=[{"as": "p", "each": "parcels[]"}])
    raises_naming(m, "tables.t.columns.x", needle)


def test_two_levels_with_one_name_fail_the_load_naming_the_second():
    m = jm({"x": "id"}, rows=[{"as": "p", "each": "parcels[]"}, {"as": "p", "each": "scans[]"}])
    raises_naming(m, "tables.t.rows[1].as", "level 'p' is named twice")


def test_index_of_on_a_plain_string_rows_fails_the_load():
    raises_naming(jm({"x": {"index_of": "p"}}), "tables.t.columns.x.index_of", "`index_of` needs a `rows` chain")


def test_a_chain_with_no_levels_fails_the_load():
    with pytest.raises(MappingError):
        compiled(jm({"x": "id"}, rows=[]))


# ============================================================ member tables


def _members(columns, globs=("*.png", "**/*.png"), **extra):
    m = jm({"x": "v"})
    m["tables"]["mem"] = {"members": list(globs), "columns": columns}
    m["tables"]["mem"].update(extra)
    return m


ALL_ATTRS = {
    "path": {"member": "path"},
    "name": {"member": "name"},
    "stem": {"member": "stem"},
    "ext": {"member": "ext"},
    "dir": {"member": "dir"},
    "size": {"type": "int64", "member": "size"},
    "mod": {"type": "timestamp", "member": "modified"},
    "image": {"type": "file", "member": "self", "probe": "image"},
}
PNGS = {
    "a.json": items(),
    "x/one.png": png_bytes(2, 3),
    "two.png": png_bytes(4, 5),
    "x/y/three.png": b"not an image",
    "notes.txt": b"hello",
}


def test_a_member_table_is_one_row_per_matching_member_in_path_order():
    _, st, sk = run_small(_members(ALL_ATTRS), copy.deepcopy(PNGS))
    rows = sk.rows("mem")
    assert [r["path"] for r in rows] == ["two.png", "x/one.png", "x/y/three.png"]
    assert st.rows["mem"] == 3 and st.rows["t"] == 0


def test_every_member_attribute_reads_what_it_names():
    _, _, sk = run_small(_members(ALL_ATTRS), copy.deepcopy(PNGS))
    one = sk.rows("mem")[1]
    assert (one["name"], one["stem"], one["ext"], one["dir"], one["size"]) == ("one.png", "one", ".png", "x", len(PNGS["x/one.png"]))
    assert one["image"] == PNGS["x/one.png"]
    assert (one["image_ref"], one["image_member"], one["image_resolution"]) == ("x/one.png", "x/one.png", "self")
    assert (one["image_format"], one["image_width"], one["image_height"]) == ("PNG", 2, 3)


def test_a_member_at_the_root_has_a_null_dir_not_an_empty_one():
    _, _, sk = run_small(_members(ALL_ATTRS), copy.deepcopy(PNGS))
    assert sk.rows("mem")[0]["dir"] is None


def test_the_doc_id_and_source_file_of_a_member_row_come_from_the_bundle_key_and_path():
    from dag_tools.documents.engine import doc_id_for

    _, _, sk = run_small(_members(ALL_ATTRS), copy.deepcopy(PNGS))
    for r in sk.rows("mem"):
        assert r["source_file"] == r["path"]
        assert r["doc_id"] == doc_id_for("b_2026-03", r["path"])
    assert len({r["doc_id"] for r in sk.rows("mem")}) == 3


def test_a_member_that_is_not_an_image_is_kept_and_its_probe_failure_is_counted():
    _, st, sk = run_small(_members(ALL_ATTRS), copy.deepcopy(PNGS))
    bad = sk.rows("mem")[2]
    assert bad["image"] == b"not an image" and bad["image_format"] is None
    assert st.coercion_failures == {"mem": {"image_format": 1}}
    assert st.file_resolution == {"mem": {"image": {"self": 3}}}


def test_a_member_table_that_matches_nothing_is_no_rows_and_no_error():
    _, st, sk = run_small(_members(ALL_ATTRS, globs=("*.gif",)), copy.deepcopy(PNGS))
    assert sk.rows("mem") == [] and st.rows["mem"] == 0 and st.documents_rejected == 0


def test_a_member_table_over_a_bundle_with_no_members_is_no_rows():
    _, st, sk = run_small(_members(ALL_ATTRS), {})
    assert sk.rows("mem") == [] and st.rows["mem"] == 0


def test_a_member_a_document_glob_also_matched_is_described_by_both():
    m = _members({"name": {"member": "name"}}, globs=("*.json",))
    _, st, sk = run_small(m, {"a.json": items("q", "r")})
    assert st.rows["t"] == 2 and st.rows["mem"] == 1
    assert col(sk, "name", "mem") == ["a.json"]


def test_extract_and_map_apply_to_member_attributes():
    cols = {
        "side": {"member": "stem", "extract": "^overview_(?P<value>[a-z]+)$"},
        "kind": {"member": "ext", "map": {"values": {".png": "picture"}, "default": "other"}},
    }
    files = {"overview_north.png": b"1", "overview_south.png": b"2", "plain.png": b"3"}
    _, st, sk = run_small(_members(cols), files)
    assert col(sk, "side", "mem") == ["north", "south", None]
    assert col(sk, "kind", "mem") == ["picture"] * 3
    assert st.unmapped_values == {"mem": {"side": 1}}


def test_split_applies_to_a_member_attribute_rather_than_being_ignored():
    # Regression: the member branch was chosen before the split branch, so a
    # list<string> member column was the stem's characters, not its parts.
    cols = {
        "parts": {"type": "list<string>", "member": "stem", "split": "_"},
        "first": {"member": "stem", "split": "_", "prefer": ["south", "north"]},
    }
    files = {"overview_north.png": b"1", "plain.png": b"2"}
    _, st, sk = run_small(_members(cols), files)
    assert col(sk, "parts", "mem") == [["overview", "north"], ["plain"]]
    assert col(sk, "first", "mem") == ["north", None]
    assert st.coercion_failures.get("mem", {}) == {}


def test_a_member_size_is_an_integer_and_modified_is_a_timestamp_by_default():
    cm = compiled(_members({"size": {"member": "size"}, "mod": {"member": "modified"}}))
    mem = next(t for t in cm.tables if t.name == "mem")
    types = {f.name: str(f.type) for f in mem.schema}
    assert types["size"] == "int64" and types["mod"].startswith("timestamp")


def _bundle(members):
    return OpenBundle(BundleRef("b_2026-03", "tree", "", 0), members)


def _member(path, data, modified=""):
    return Member(path, len(data), modified, lambda n, _d=data: _d if n is None else _d[:n])


@pytest.mark.parametrize(
    "modified, naive_value, aware_value, failures",
    [
        ("2025-05-06T07:08:10", None, "2025-05-06T07:08:10+00:00", {"mem": {"mod": 1}}),  # zip members: naive
        ("2025-05-06T07:08:10+00:00", "2025-05-06T07:08:10+00:00", "2025-05-06T07:08:10+00:00", {}),
        ("", None, None, {}),  # unknown: null, never a failure
    ],
    ids=["naive_zip_time", "aware_time", "unknown_time"],
)
def test_a_naive_member_time_is_a_counted_failure_until_the_column_declares_a_zone(
    modified, naive_value, aware_value, failures
):
    m = _members({"mod": {"type": "timestamp", "member": "modified"},
                  "modtz": {"type": "timestamp", "member": "modified", "timezone": "UTC"}})
    sink = Collect()
    st = run_bundle(compiled(m), _bundle([_member("a.png", b"p", modified)]), sink, ingested_at=INGESTED)
    (r,) = sink.rows("mem")
    iso = lambda v: v.isoformat() if v is not None else None  # noqa: E731
    assert iso(r["mod"]) == naive_value
    assert iso(r["modtz"]) == aware_value
    assert st.coercion_failures == failures


def test_a_real_zip_member_has_a_naive_time_and_a_declared_zone_reads_it(tmp_path):
    write_zip(tmp_path / "src" / "b_2026-03.zip", {"m/a.png": png_bytes(2, 2)})
    src = open_source(str(tmp_path / "src"), 1)
    m = _members({"mod": {"type": "timestamp", "member": "modified"},
                  "modtz": {"type": "timestamp", "member": "modified", "timezone": "UTC"}})
    sink = Collect()
    with src.open_bundle(src.ref_for("b_2026-03.zip")) as b:
        st = run_bundle(compiled(m), b, sink, ingested_at=INGESTED)
    (r,) = sink.rows("mem")
    assert r["mod"] is None and r["modtz"] is not None and r["modtz"].tzinfo is not None
    assert st.coercion_failures == {"mem": {"mod": 1}}


def test_a_member_that_cannot_be_read_is_a_reject_and_the_others_still_land(caplog):
    caplog.set_level(logging.INFO)

    def boom(n):
        raise OSError("planted-detail-zz")

    sink = Collect()
    st = run_bundle(
        compiled(_members({"name": {"member": "name"}, "image": {"type": "file", "member": "self"}})),
        _bundle([_member("a.png", b"a"), Member("b.png", 5, "", boom), _member("c.png", b"c")]),
        sink, ingested_at=INGESTED)
    assert col(sink, "name", "mem") == ["a.png", "c.png"]
    (rej,) = sink.rows("_rejects")
    assert rej["source_file"] == "b.png"
    assert rej["error"] == "OSError: planted-detail-zz"
    assert st.rows["mem"] == 2 and st.rows["_rejects"] == 1
    assert "planted-detail-zz" not in caplog.text  # the warning names the class, never the message


def test_a_rejected_member_with_redaction_on_carries_a_digest_not_the_message():
    def boom(n):
        raise OSError("planted-detail-zz")

    sink = Collect()
    run_bundle(compiled(_members({"image": {"type": "file", "member": "self"}})),
               _bundle([Member("b.png", 5, "", boom)]), sink, ingested_at=INGESTED, redact=True)
    (rej,) = sink.rows("_rejects")
    assert rej["error"].startswith("OSError [") and "planted-detail-zz" not in rej["error"]


@pytest.mark.parametrize(
    "mutate, where, needle",
    [
        (lambda m: m["tables"]["mem"].update(rows="items[]"), "tables.mem", "set exactly one of `rows`"),
        (lambda m: m["tables"]["mem"].pop("members"), "tables.mem", "set exactly one of `rows`"),
        (lambda m: m["tables"]["t"]["columns"].update(x={"member": "name"}), "tables.t.columns.x.member",
         "`member` applies only to a table built from `members`"),
        (lambda m: m["tables"]["mem"]["columns"].update(name={"expr": "x"}), "tables.mem.columns.name",
         "a member table has no document"),
        (lambda m: m["tables"]["mem"]["columns"].update(name={"member": "bogus"}), "tables.mem.columns.name", "member"),
        (lambda m: m["tables"]["mem"]["columns"].update(name={"member": "stem", "prefer": ["a"]}),
         "tables.mem.columns.name.prefer", "unless it is `split`"),
    ],
    ids=["rows_and_members", "neither", "member_on_a_rows_table", "expr_in_a_member_table", "unknown_attribute",
         "prefer_on_an_unsplit_member"],
)
def test_a_member_table_that_cannot_work_fails_the_load_naming_the_field(mutate, where, needle):
    m = _members({"name": {"member": "name"}})
    mutate(m)
    raises_naming(m, where, needle)


# ------------------------------------------------------------ through the real writer


@pytest.fixture
def lance_tree(tmp_path):
    root = tmp_path / "src"
    write_tree(root / "A", PNGS)
    write_tree(root / "B", {"a.json": items(), "only.png": png_bytes(2, 2)})
    return open_source(str(root), 1)


def _ingest(cm, source, writer, key):
    writer.begin_bundle(key, empty_tables(cm))
    with source.open_bundle(source.ref_for(key)) as b:
        stats = run_bundle(cm, b, writer.sink, ingested_at=INGESTED)
    return stats, writer.finish()


def test_member_rows_land_in_lance_and_a_second_run_replaces_them_without_duplicating(lance_tree, tmp_path):
    cm = compiled(_members(ALL_ATTRS))
    w = LanceWriter(str(tmp_path / "dest"))
    stats, res = _ingest(cm, lance_tree, w, "A")
    assert stats.rows["mem"] == 3 and res.appended["mem"] == 3 and "mem" in res.created
    _ingest(cm, lance_tree, w, "B")
    assert (w.count_rows("mem", "A"), w.count_rows("mem", "B"), w.count_rows("mem")) == (3, 1, 4)

    ds = w._open("mem")
    before = sorted(ds.to_table(columns=["doc_id", "path"]).to_pylist(), key=lambda r: r["doc_id"])
    _, res2 = _ingest(cm, lance_tree, w, "A")
    assert res2.created == [] and res2.deleted["mem"] == 3 and res2.appended["mem"] == 3
    assert (w.count_rows("mem", "A"), w.count_rows("mem", "B"), w.count_rows("mem")) == (3, 1, 4)
    after = sorted(w._open("mem").to_table(columns=["doc_id", "path"]).to_pylist(), key=lambda r: r["doc_id"])
    assert after == before  # the same ids: a re-run is a replacement, not a new set


def test_a_member_file_column_reads_back_from_lance_as_the_original_bytes(lance_tree, tmp_path):
    cm = compiled(_members(ALL_ATTRS))
    w = LanceWriter(str(tmp_path / "dest"))
    _ingest(cm, lance_tree, w, "A")
    ds = w._open("mem")
    refs = ds.to_table(columns=["image_ref"]).column("image_ref").to_pylist()
    blobs = ds.take_blobs("image", indices=list(range(len(refs))))
    got = {r: f.read() for r, f in zip(refs, blobs)}
    assert got == {p: PNGS[p] for p in ("two.png", "x/one.png", "x/y/three.png")}


def test_a_bundle_with_no_matching_member_writes_zero_member_rows_and_clears_the_old_ones(lance_tree, tmp_path):
    w = LanceWriter(str(tmp_path / "dest"))
    _ingest(compiled(_members(ALL_ATTRS)), lance_tree, w, "A")
    assert w.count_rows("mem", "A") == 3
    _, res = _ingest(compiled(_members(ALL_ATTRS, globs=("*.gif",))), lance_tree, w, "A")
    assert res.appended.get("mem", 0) == 0 and w.count_rows("mem", "A") == 0


# ============================================================ nulls


def _with_doc_nulls(m, nulls):
    m["document"]["nulls"] = nulls
    return m


def test_a_value_equal_to_a_nulls_entry_after_trimming_is_absent():
    c = {"type": "string", "expr": "v", "nulls": ["-", "NIL"]}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items(" NIL ", "-", "x", "NILx", "nil")})
    # whole-value equality after trimming; case is not folded and a prefix is not a match
    assert col(sk, "a") == [None, None, "x", "NILx", "nil"]
    assert st.coercion_failures == {} and st.unmapped_values == {}


def test_the_document_list_is_the_default_and_a_column_can_override_it_even_with_an_empty_list():
    m = _with_doc_nulls(jm({
        "inherits": {"type": "string", "expr": "v"},
        "own": {"type": "string", "expr": "v", "nulls": ["x"]},
        "off": {"type": "string", "expr": "v", "nulls": []},
    }), ["-"])
    _, _, sk = run_small(m, {"a.json": items("-", "x")})
    assert col(sk, "inherits") == [None, "x"]
    assert col(sk, "own") == ["-", None]  # an override replaces the default, it does not add to it
    assert col(sk, "off") == ["-", "x"]


def test_a_nulls_value_is_not_a_coercion_failure():
    m = _with_doc_nulls(jm({"n": {"type": "int32", "expr": "v"}}), ["-"])
    _, st, sk = run_small(m, {"a.json": items("-", " - ", "7", "x")})
    assert col(sk, "n") == [None, None, 7, None]
    assert st.coercion_failures == {"t": {"n": 1}}  # only "x"


def test_a_native_json_value_is_compared_by_its_text():
    c = {"type": "int32", "expr": "v", "nulls": ["-1"]}
    _, st, sk = run_small(jm({"n": c}), {"a.json": items(-1, 1, "-1")})
    assert col(sk, "n") == [None, 1, None]
    assert st.coercion_failures == {}


def test_nulls_apply_per_part_of_a_split():
    c = {"type": "list<int32>", "expr": "v", "split": ";", "nulls": ["-"]}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items("1;-;3", "- ; 2", "-", " - ")})
    assert col(sk, "a") == [[1, 3], [2], [], []]
    assert st.coercion_failures == {}


def test_nulls_apply_per_part_and_keep_empty_keeps_an_absent_part_as_null():
    c = {"type": "list<string>", "expr": "v", "split": ";", "keep_empty": True, "nulls": ["-"]}
    _, _, sk = run_small(jm({"a": c}), {"a.json": items("a;-;b", "a;;-", "-", "")})
    # an absent part sits where it was; a value that is wholly absent is no parts, as an empty one is
    assert col(sk, "a") == [["a", None, "b"], ["a", None, None], [], []]


def test_nulls_apply_before_extract_so_an_absent_value_is_not_a_miss():
    c = {"type": "string", "expr": "v", "extract": "^(\\d+)-", "nulls": ["-"]}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items("-", "12-x", "zz")})
    assert col(sk, "a") == [None, "12", None]
    assert st.unmapped_values == {"t": {"a": 1}}  # "zz" only


def test_nulls_apply_before_map_so_an_absent_value_takes_map_null_and_is_not_unmapped():
    c = {"type": "string", "expr": "v", "nulls": ["-"], "map": {"values": {"-": "dash", "x": "X"}, "null": "none"}}
    _, st, sk = run_small(jm({"a": c}), {"a.json": items("-", "x")})
    assert col(sk, "a") == ["none", "X"]  # the map's own "-" key is never reached
    assert st.unmapped_values == {}


def test_an_absent_value_takes_when_empty():
    c = {"type": "string", "expr": "v", "nulls": ["-"], "when_empty": "unknown"}
    _, _, sk = run_small(jm({"a": c}), {"a.json": items("-", "x")})
    assert col(sk, "a") == ["unknown", "x"]


def test_first_of_skips_a_value_that_is_a_nulls_entry():
    c = {"type": "string", "first_of": ["p", "q"], "nulls": ["-"]}
    doc = json.dumps({"items": [{"p": "-", "q": "b"}, {"p": "a", "q": "b"}, {"p": "-", "q": " - "}]}).encode()
    _, _, sk = run_small(jm({"a": c}), {"a.json": doc})
    assert col(sk, "a") == ["b", "a", None]


def test_nulls_work_on_an_xml_attribute():
    m = _with_doc_nulls(xm({"a": {"type": "int32", "expr": "@v"}}), ["NIL"])
    doc = b'<m><i v="NIL"/><i v=" 4 "/></m>'
    _, st, sk = run_small(m, {"a.xml": doc})
    assert col(sk, "a") == [None, 4]
    assert st.coercion_failures == {}


def test_a_struct_columns_nulls_are_its_fields_default():
    m = _with_doc_nulls(jm({
        "s": {"type": "list<struct>", "rows": "ss[]", "nulls": ["?"], "fields": {
            "n": {"type": "int32", "expr": "n"},
            "k": {"type": "string", "expr": "n", "nulls": []},
        }},
    }), ["-"])
    doc = json.dumps({"items": [{"ss": [{"n": "?"}, {"n": "-"}, {"n": "3"}]}]}).encode()
    _, st, sk = run_small(m, {"a.json": doc})
    assert col(sk, "s") == [[{"n": None, "k": "?"}, {"n": None, "k": "-"}, {"n": 3, "k": "3"}]]
    assert st.coercion_failures == {"t": {"s.n": 1}}  # "-" is not absent for the struct: its own list wins


def test_a_file_reference_that_is_a_nulls_entry_names_no_file():
    m = _with_doc_nulls(_file_map("document_dir"), ["-"])
    _, st, sk = run_small(m, {"d/q.png": png_bytes(2, 2), "d/a.json": items("-", "q.png", key="f")})
    rows = sk.rows("t")
    assert (rows[0]["f_ref"], rows[0]["f_member"], rows[0]["f_resolution"], rows[0]["f"]) == (None, None, None, None)
    assert rows[1]["f_member"] == "d/q.png"
    assert st.file_resolution == {"t": {"f": {"basename_in": 1}}}  # the absent ref is not counted missing


@pytest.mark.parametrize("where", ["column", "document"])
def test_a_blank_nulls_entry_fails_the_load(where):
    m = jm({"a": {"type": "string", "expr": "v"}})
    if where == "column":
        m["tables"]["t"]["columns"]["a"]["nulls"] = ["-", "  "]
    else:
        m["document"]["nulls"] = [""]
    raises_naming(m, "nulls", "blank")


def test_nulls_on_an_index_of_column_fails_the_load():
    m = jm({"pos": {"index_of": "root", "nulls": ["-"]}})
    raises_naming(m, "pos", "`index_of` is a position")


# ============================================================ const


def test_a_const_is_typed_by_the_column_type_and_written_on_every_row():
    m = jm({
        "s": {"const": "depot-east"},
        "n": {"type": "int32", "const": "7"},
        "b": {"type": "bool", "const": True},
        "l": {"type": "list<int32>", "const": [1, "2"]},
        "ts": {"type": "timestamp", "const": "2026-05-01T08:00:00+02:00"},
        "none": {"type": "int64", "const": None},
    })
    _, st, sk = run_small(m, {"a.json": items("x", "y")})
    rows = sk.rows("t")
    assert len(rows) == 2
    for r in rows:
        assert (r["s"], r["n"], r["b"], r["l"], r["none"]) == ("depot-east", 7, True, [1, 2], None)
        assert r["ts"].isoformat() == "2026-05-01T06:00:00+00:00"
    assert st.coercion_failures == {}


def test_each_row_gets_its_own_copy_of_a_list_const():
    _, _, sk = run_small(jm({"l": {"type": "list<string>", "const": ["a"]}}), {"a.json": items(1, 2)})
    assert col(sk, "l") == [["a"], ["a"]]


def test_a_yaml_date_const_is_read_as_its_iso_text():
    from datetime import date, datetime, timezone

    m = jm({
        "d": {"const": date(2026, 5, 1)},
        "ts": {"type": "timestamp", "const": datetime(2026, 5, 1, 8, tzinfo=timezone.utc)},
    })
    _, _, sk = run_small(m, {"a.json": items(1)})
    r = sk.rows("t")[0]
    assert r["d"] == "2026-05-01" and r["ts"].isoformat() == "2026-05-01T08:00:00+00:00"


def test_a_const_timestamp_takes_format_and_timezone():
    c = {"type": "timestamp", "const": "01/05/2026 08:30", "format": "%d/%m/%Y %H:%M", "timezone": "Asia/Tokyo"}
    _, _, sk = run_small(jm({"ts": c}), {"a.json": items(1)})
    assert sk.rows("t")[0]["ts"].isoformat() == "2026-04-30T23:30:00+00:00"


def test_a_const_timestamp_with_no_offset_and_no_timezone_fails_the_load():
    raises_naming(jm({"ts": {"type": "timestamp", "const": "2026-05-01T08:00:00"}}), "tables.t.columns.ts", "const")


@pytest.mark.parametrize("c", [
    {"type": "int32", "const": "seven"},
    {"type": "int32", "const": [1]},
    {"type": "string", "const": {"k": "v"}},
    {"type": "list<int32>", "const": 1},
    {"type": "list<int32>", "const": [1, "x"]},
    {"type": "list<string>", "const": [["nested"]]},
    {"type": "bool", "const": "perhaps"},
])
def test_a_const_that_is_not_a_value_of_its_type_fails_the_load_naming_the_column(c):
    text = raises_naming(jm({"k": c}), "tables.t.columns.k", "const")
    assert "seven" not in text and "perhaps" not in text  # a value is never echoed


@pytest.mark.parametrize("extra, shown", [
    ({"expr": "v"}, "set exactly one of"),
    ({"map": {"values": {"a": "b"}}}, "`map`"),
    ({"nulls": ["-"]}, "`nulls`"),
    ({"split": ";"}, "`split`"),
    ({"extract": "(x)"}, "`extract`"),
    ({"when_empty": "z"}, "`when_empty`"),
    ({"from": "Z"}, "`from`"),
])
def test_a_const_refuses_every_step_that_reads_a_source_value(extra, shown):
    c = {"type": "string", "const": "a", **extra}
    raises_naming(jm({"k": c}), "tables.t.columns.k", shown)


def test_a_file_column_has_no_const():
    raises_naming(jm({"f": {"type": "file", "const": "q.png"}}), "tables.t.columns.f", "no constant")


def test_a_const_works_in_a_member_table():
    _, _, sk = run_small(_members({"path": {"member": "path"}, "kind": {"const": "image"}}), copy.deepcopy(PNGS))
    assert {r["kind"] for r in sk.rows("mem")} == {"image"}


# ============================================================ into


def _two(a_cols, b_cols, a_extra=None, b_extra=None, into="parcels"):
    a = {"rows": "items[]", "into": into, "columns": a_cols, **(a_extra or {})}
    b = {"rows": "others[]", "into": into, "columns": b_cols, **(b_extra or {})}
    return {
        "version": 1,
        "document": {"format": "json", "match": ["*.json"], "max_bytes": 100_000},
        "tables": {"a": a, "b": b},
    }


SHARED_DOC = json.dumps({
    "items": [{"v": "a1", "n": "1"}, {"v": "a2", "n": "2"}],
    "others": [{"v": "b1", "n": "10"}],
}).encode()


def test_tables_sharing_an_into_write_one_dataset_and_column_order_does_not_matter():
    m = _two({"k": "v", "n": {"type": "int32", "expr": "n"}},
             {"n": {"type": "int32", "expr": "n"}, "k": "v"})
    cm, st, sk = run_small(m, {"d.json": SHARED_DOC})
    assert cm.dataset_names == ["parcels"] and cm.tables_of("parcels") == ["a", "b"]
    assert set(sk.batches) <= {"parcels", "_rejects"}  # nothing is written under a table's own name
    assert sorted((r["k"], r["n"]) for r in sk.rows("parcels")) == [("a1", 1), ("a2", 2), ("b1", 10)]
    assert st.rows["parcels"] == 3 and "a" not in st.rows and "b" not in st.rows
    assert st.rows_by_table == {"a": 2, "b": 1}


def test_an_into_naming_a_table_joins_that_tables_own_dataset():
    m = _two({"k": "v"}, {"k": "v"}, into=None)
    m["tables"]["b"]["into"] = "a"
    del m["tables"]["a"]["into"]
    cm, st, sk = run_small(m, {"d.json": SHARED_DOC})
    assert cm.dataset_names == ["a"] and len(sk.rows("a")) == 3 and st.rows_by_table == {"a": 2, "b": 1}


def test_counts_of_a_shared_dataset_are_kept_under_the_dataset():
    m = _two({"n": {"type": "int32", "expr": "n"}}, {"n": {"type": "int32", "expr": "v"}})
    _, st, _ = run_small(m, {"d.json": SHARED_DOC})
    assert st.coercion_failures == {"parcels": {"n": 1}}


def test_an_empty_bundle_writes_one_schema_per_dataset():
    cm = compiled(_two({"k": "v"}, {"k": "v"}))
    assert set(empty_tables(cm)) == {"parcels", "_rejects"}


@pytest.mark.parametrize("b_cols, needles", [
    ({"k": "v", "extra": "v"}, ["only in b: extra"]),
    ({}, ["only in a: k"]),
    ({"k": {"type": "int32", "expr": "n"}}, ["k is string in a and int32 in b"]),
])
def test_tables_sharing_an_into_with_different_columns_fail_the_load_naming_them(b_cols, needles):
    b_cols = b_cols or {"other": "v"}
    raises_naming(_two({"k": "v"}, b_cols), "tables.b.into", "'parcels' is also written by 'a'", *needles)


def test_a_row_id_on_one_table_of_a_shared_dataset_and_not_the_other_fails_the_load():
    raises_naming(_two({"k": "v"}, {"k": "v"}, a_extra={"row_id": "{doc_id}-{row_number}"}),
                  "tables.b.into", "only in a: row_id")


@pytest.mark.parametrize("into, needle", [
    ("_rejects", "reserved"),
    ("not a name", "identifier"),
])
def test_an_into_that_is_not_a_dataset_name_fails_the_load(into, needle):
    raises_naming(_two({"k": "v"}, {"k": "v"}, into=into), "tables.a.into", needle)


def test_an_into_naming_a_table_that_writes_elsewhere_fails_the_load():
    m = _two({"k": "v"}, {"k": "v"})
    m["tables"]["b"]["into"] = "a"  # a itself writes to "parcels"
    raises_naming(m, "tables.b.into", "'a' is a table that writes to 'parcels'")


def test_a_member_table_and_a_document_table_can_share_an_into():
    m = _members({"k": {"member": "path"}}, into="things")
    m["tables"]["t"] = {"rows": "items[]", "into": "things", "columns": {"k": "v"}}
    cm, st, sk = run_small(m, {**copy.deepcopy(PNGS), "a.json": items("doc-row")})
    assert cm.dataset_names == ["things"]
    assert sorted(r["k"] for r in sk.rows("things")) == ["doc-row", "two.png", "x/one.png", "x/y/three.png"]
    assert st.rows_by_table == {"t": 1, "mem": 3}


# ============================================================ only_unreferenced


def _refs_and_members(only=True, refs=("x/one.png",), files=None):
    m = jm({"f": {"type": "file", "expr": "f", "resolve": ["bundle_root"]}})
    m["tables"]["mem"] = {"members": ["*.png", "**/*.png"], "only_unreferenced": only,
                          "columns": {"path": {"member": "path"}}}
    tree = {**copy.deepcopy(PNGS), "a.json": items(*refs, key="f")}
    tree.update(files or {})
    return m, tree


def test_only_unreferenced_keeps_the_members_no_file_column_resolved_to():
    m, tree = _refs_and_members(refs=("x/one.png", "nope.png"))
    _, st, sk = run_small(m, tree)
    assert [r["path"] for r in sk.rows("mem")] == ["two.png", "x/y/three.png"]
    assert st.members_kept == {"mem": 2} and st.members_skipped == {"mem": 1}
    assert st.rows["mem"] == 2


def test_only_unreferenced_is_decided_after_every_document_including_later_ones():
    m, tree = _refs_and_members(refs=())
    tree["z/late.json"] = items("two.png", key="f")  # sorts after a.json
    m["document"]["match"] = ["*.json", "**/*.json"]
    _, st, sk = run_small(m, tree)
    assert [r["path"] for r in sk.rows("mem")] == ["x/one.png", "x/y/three.png"]
    assert st.members_skipped == {"mem": 1}


def test_a_rejected_document_references_nothing(monkeypatch):
    from dag_tools.documents import engine

    m, tree = _refs_and_members(refs=("x/one.png",))
    real = engine._eval_document

    def resolve_then_fail(*a, **kw):
        real(*a, **kw)  # every file column has resolved, and recorded what it resolved to
        raise engine.DocumentError("planted after evaluation")

    monkeypatch.setattr(engine, "_eval_document", resolve_then_fail)
    _, st, sk = run_small(m, tree)
    assert st.documents_rejected == 1
    assert [r["path"] for r in sk.rows("mem")] == ["two.png", "x/one.png", "x/y/three.png"]
    assert st.members_skipped == {"mem": 0} and st.members_kept == {"mem": 3}


def test_without_only_unreferenced_every_member_is_kept_and_nothing_is_counted():
    m, tree = _refs_and_members(only=False)
    _, st, sk = run_small(m, tree)
    assert len(sk.rows("mem")) == 3
    assert st.members_kept == {} and st.members_skipped == {}


def test_only_unreferenced_on_a_document_table_fails_the_load():
    m = jm({"k": "v"})
    m["tables"]["t"]["only_unreferenced"] = True
    raises_naming(m, "tables.t.only_unreferenced", "member table")
