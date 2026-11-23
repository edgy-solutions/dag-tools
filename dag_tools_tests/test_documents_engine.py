"""Tests for the documents-to-tables engine (synthetic vocabulary only).

Mapping grammar, the fast-path/XPath agreement, the vocabulary guard and the
purity check live in ``test_documents_mapping.py``.
"""
import calendar
import copy
import logging
import os
import re
import time
from datetime import datetime, timezone

import pytest

pytest.importorskip("lxml")
pytest.importorskip("pyarrow")
pytest.importorskip("PIL")

from lxml import etree  # noqa: E402

from dag_tools.documents.coerce import coercer_for  # noqa: E402
from dag_tools.documents.engine import (  # noqa: E402
    compile_mapping,
    doc_id_for,
    empty_tables,
    publish_period_for,
    run_bundle,
)
from dag_tools.documents.mapping import MappingError, load_mapping  # noqa: E402
from dag_tools.documents.sources import BundleRef, Member, OpenBundle, open_source  # noqa: E402
from dag_tools_tests.documents_synthetic import (  # noqa: E402
    EXPECTED_ROWS,
    MAPPING,
    TABLES,
    bulk_manifest,
    bundle_files,
    expected_photo_resolution,
    large_manifest,
    manifest_xml,
    relay_manifest,
    write_nested_zip,
    write_tree,
    write_zip,
)

INGESTED = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
SHAPES = ["tree", "zip", "nested_zip"]
VARIANTS = ["flat", "numbered"]
BUNDLE_NAME = "b_2026-03"


# ------------------------------------------------------------------ helpers


def compiled(mapping=MAPPING):
    return compile_mapping(load_mapping(copy.deepcopy(mapping)))


class Collect:
    """A sink: ``callable(table, batch)`` keeping every batch."""

    def __init__(self):
        self.batches = {}

    def __call__(self, table, batch):
        self.batches.setdefault(table, []).append(batch)

    def rows(self, table):
        out = []
        for b in self.batches.get(table, []):
            out.extend(b.to_pylist())
        return out

    def count(self, table):
        return sum(b.num_rows for b in self.batches.get(table, []))


def open_shape(tmp_path, shape, variant, files=None):
    files = bundle_files(variant) if files is None else files
    root = tmp_path / f"src_{shape}_{variant}"
    depth = 0
    if shape == "tree":
        write_tree(root / BUNDLE_NAME, files)
        depth = 1
    elif shape == "zip":
        write_zip(root / f"{BUNDLE_NAME}.zip", files)
    else:
        write_nested_zip(root / f"{BUNDLE_NAME}.zip", files)
    src = open_source(str(root), bundle_depth=depth)
    refs = list(src.list_bundles())
    assert len(refs) == 1
    return src.open_bundle(refs[0])


def run_shape(tmp_path, shape, variant, cm=None, **kw):
    sink = Collect()
    with open_shape(tmp_path, shape, variant) as bundle:
        stats = run_bundle(cm or compiled(), bundle, sink, ingested_at=INGESTED, **kw)
    return stats, sink


def mem_bundle(files, key=BUNDLE_NAME, limits=None):
    """An in-memory bundle; ``limits`` (a list) records each read's limit."""
    members = []
    for path, data in sorted(files.items()):
        def _read(n, _d=data):
            if limits is not None:
                limits.append(n)
            return _d if n is None else _d[:n]

        members.append(Member(path, len(data), "", _read))
    return OpenBundle(BundleRef(key, "tree", "", sum(len(d) for d in files.values())), members)


def small_mapping(columns=None, rows="/a/item", **extra):
    m = {
        "version": 1,
        "document": {"match": ["*.xml"], "max_bytes": 10_000},
        "tables": {"t": {"rows": rows, "columns": columns or {"v": "@v"}}},
    }
    m.update(extra)
    return m


def run_small(mapping, files, **kw):
    cm = compiled(mapping)
    sink = Collect()
    stats = run_bundle(cm, mem_bundle(files), sink, ingested_at=INGESTED, **kw)
    return cm, stats, sink


def us(y, mo, d, h=0, mi=0, s=0, micro=0):
    return calendar.timegm((y, mo, d, h, mi, s)) * 1_000_000 + micro


def epoch_us(dt):
    return calendar.timegm(dt.utctimetuple()) * 1_000_000 + dt.microsecond


# ------------------------------------------------------------------ 1. every shape, every variant


@pytest.mark.parametrize("variant", VARIANTS)
@pytest.mark.parametrize("shape", SHAPES)
def test_every_table_yields_its_expected_rows(tmp_path, shape, variant):
    stats, sink = run_shape(tmp_path, shape, variant)
    for table, n in EXPECTED_ROWS.items():
        assert sink.count(table) == n, table
        assert stats.rows[table] == n, table
    assert stats.documents == 3 and stats.documents_parsed == 2 and stats.documents_rejected == 1


@pytest.mark.parametrize("variant", VARIANTS)
@pytest.mark.parametrize("shape", SHAPES)
def test_photo_file_resolution_counts_match_the_variant(tmp_path, shape, variant):
    stats, _ = run_shape(tmp_path, shape, variant)
    assert stats.file_resolution["photos"]["image"] == expected_photo_resolution(variant)


def test_an_inner_zip_two_levels_deep_is_reported_not_dropped(tmp_path):
    inner = tmp_path / "inner_files"
    write_zip(inner / "deep.zip", {"x.xml": b"<a/>"})
    files = dict(bundle_files("flat"))
    files["deep.zip"] = (inner / "deep.zip").read_bytes()
    root = tmp_path / "src"
    write_nested_zip(root / f"{BUNDLE_NAME}.zip", files)
    src = open_source(str(root))
    with src.open_bundle(next(iter(src.list_bundles()))) as bundle:
        stats = run_bundle(compiled(), bundle, Collect(), ingested_at=INGESTED)
    assert stats.unexpanded_zips == ["inner.zip/deep.zip"]


# ------------------------------------------------------------------ 2. system columns


SYSTEM = ["doc_id", "source_bundle", "source_file", "publish_period", "ingested_at"]


def test_every_table_carries_the_system_columns_and_row_id_only_where_declared(tmp_path):
    _, sink = run_shape(tmp_path, "zip", "flat")
    for table in TABLES:
        names = sink.batches[table][0].schema.names
        for col in SYSTEM:
            assert col in names, (table, col)
        assert ("row_id" in names) == ("row_id" in MAPPING["tables"][table]), table
    names = sink.batches["_rejects"][0].schema.names
    assert {"doc_id", "source_bundle", "source_file", "publish_period", "error", "ingested_at"} <= set(names)


def test_system_column_values_come_from_the_bundle_and_the_run(tmp_path):
    stats, sink = run_shape(tmp_path, "zip", "flat")
    key = f"{BUNDLE_NAME}.zip"
    for row in sink.rows("parcels"):
        assert row["source_bundle"] == key
        assert row["source_file"] in ("manifest_m1.xml", "manifest_m2.xml")
        assert row["doc_id"] == doc_id_for(key, row["source_file"])
        assert row["publish_period"] == "2026-03"
        assert row["ingested_at"] == INGESTED
    assert stats.publish_period == "2026-03" and stats.publish_period_unmatched is False


def test_row_ids_follow_their_templates(tmp_path):
    _, sink = run_shape(tmp_path, "zip", "flat")
    assert sorted(r["row_id"] for r in sink.rows("loads")) == ["M1", "M2"]
    assert sorted(r["row_id"] for r in sink.rows("parcels")) == ["M1/P1", "M1/P2", "M2/P3"]
    scans = sink.rows("scans")
    for r in scans:
        assert r["row_id"].startswith(r["doc_id"] + ":")
    m1 = sorted(r["row_id"].rsplit(":", 1)[1] for r in scans if r["source_file"] == "manifest_m1.xml")
    assert m1 == ["1", "2", "3", "4"]  # row_number counts rows of the table within the document


def test_doc_id_is_stable_across_two_runs_of_the_same_bundle(tmp_path):
    ids = []
    for _ in range(2):
        _, sink = run_shape(tmp_path, "zip", "flat")
        ids.append(sorted({r["doc_id"] for r in sink.rows("loads")}))
    assert ids[0] == ids[1] and len(ids[0]) == 2
    assert doc_id_for("k", "p") == doc_id_for("k", "p")
    assert doc_id_for("k", "p") != doc_id_for("k", "q")
    assert doc_id_for("k", "p") != doc_id_for("j", "p")
    assert doc_id_for("ab", "c") != doc_id_for("a", "bc")  # the separator is part of the identity


def test_publish_period_is_derived_from_the_bundle_name_by_the_regex():
    cm = compiled()
    assert publish_period_for(cm, "x/2026-03/deep/b.zip", "b_2027-11.zip") == ("2027-11", False)


def test_publish_period_from_bundle_key_reads_the_key_not_the_name():
    m = copy.deepcopy(MAPPING)
    m["lineage"]["publish_period"]["from"] = "bundle_key"
    cm = compiled(m)
    assert publish_period_for(cm, "2025-12/b.zip", "b_2027-11.zip") == ("2025-12", False)


@pytest.mark.parametrize(
    "regex, expected",
    [(r"(?P<period>\d{4})", "2026"), (r"(\d{4})-\d{2}", "2026"), (r"\d{4}-\d{2}", "2026-03")],
)
def test_publish_period_takes_named_group_then_first_group_then_whole_match(regex, expected):
    m = copy.deepcopy(MAPPING)
    m["lineage"]["publish_period"]["regex"] = regex
    assert publish_period_for(compiled(m), BUNDLE_NAME, BUNDLE_NAME) == (expected, False)


def test_a_bundle_name_the_regex_does_not_match_is_counted_not_raised():
    files = {"m.xml": manifest_xml("M1", parcels=[])}
    cm = compiled()
    sink = Collect()
    stats = run_bundle(cm, mem_bundle(files, key="no_period_here"), sink, ingested_at=INGESTED)
    assert stats.publish_period is None and stats.publish_period_unmatched is True
    assert sink.rows("loads")[0]["publish_period"] is None
    assert stats.rows["loads"] == 1


def test_a_mapping_with_no_period_rule_is_not_unmatched():
    m = copy.deepcopy(MAPPING)
    del m["lineage"]
    cm = compiled(m)
    assert publish_period_for(cm, "k", "n") == (None, False)


# ------------------------------------------------------------------ 3. coercion, table-driven


def coerce(scalar_type, value, fmt=None, tz=None):
    return coercer_for(scalar_type, fmt, tz)(value)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("0", (0, False)),
        ("007", (7, False)),
        ("+5", (5, False)),
        ("-5", (-5, False)),
        (" 12 ", (12, False)),
        ("2147483647", (2147483647, False)),
        ("-2147483648", (-2147483648, False)),
        ("2147483648", (None, True)),
        ("-2147483649", (None, True)),
        ("99999999999999999999", (None, True)),
        ("1.0", (None, True)),
        ("1e3", (None, True)),
        ("", (None, True)),
        ("  ", (None, True)),
        ("abc", (None, True)),
        ("1 2", (None, True)),
        (3, (3, False)),
        (3.0, (3, False)),
        (3.5, (None, True)),
        (float("nan"), (None, True)),
        (float("inf"), (None, True)),
        (True, (None, True)),
        (None, (None, False)),
    ],
)
def test_int32_coercion(value, expected):
    assert coerce("int32", value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("9223372036854775807", (9223372036854775807, False)),
        ("-9223372036854775808", (-9223372036854775808, False)),
        ("9223372036854775808", (None, True)),
        ("9007199254740993", (9007199254740993, False)),  # past 2**53: exact, never via float
        ("4294967296", (4294967296, False)),
        ("1e3", (None, True)),
        (None, (None, False)),
    ],
)
def test_int64_coercion_is_exact(value, expected):
    assert coerce("int64", value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1.5", (1.5, False)),
        (" 1.5 ", (1.5, False)),
        ("-0", (-0.0, False)),
        ("+1", (1.0, False)),
        (".5", (0.5, False)),
        ("5.", (5.0, False)),
        ("1e3", (1000.0, False)),
        ("1E-2", (0.01, False)),
        ("NaN", (None, True)),
        ("nan", (None, True)),
        ("inf", (None, True)),
        ("-Infinity", (None, True)),
        ("1e999", (None, True)),  # parses to infinity: not a finite number
        ("1_000", (None, True)),
        ("0x10", (None, True)),
        ("1,5", (None, True)),
        ("", (None, True)),
        (" ", (None, True)),
        ("--1", (None, True)),
        (2, (2.0, False)),
        (2.5, (2.5, False)),
        (float("nan"), (None, True)),
        (float("inf"), (None, True)),
        (float("-inf"), (None, True)),
        (True, (None, True)),
        (None, (None, False)),
    ],
)
def test_float64_coercion(value, expected):
    assert coerce("float64", value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("true", (True, False)),
        ("TRUE", (True, False)),
        ("True", (True, False)),
        ("1", (True, False)),
        (" true ", (True, False)),
        ("false", (False, False)),
        ("FALSE", (False, False)),
        ("0", (False, False)),
        ("yes", (None, True)),
        ("no", (None, True)),
        ("t", (None, True)),
        ("y", (None, True)),
        ("2", (None, True)),
        ("01", (None, True)),
        ("", (None, True)),
        (True, (True, False)),
        (False, (False, False)),
        (1, (True, False)),
        (0, (False, False)),
        (2, (None, True)),
        (None, (None, False)),
    ],
)
def test_bool_coercion(value, expected):
    assert coerce("bool", value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [("x", ("x", False)), ("", ("", False)), (5, ("5", False)), (True, ("true", False)), (None, (None, False))],
)
def test_string_coercion(value, expected):
    assert coerce("string", value) == expected


def ts(value, fmt=None, tz=None):
    v, failed = coerce("timestamp", value, fmt, tz)
    return (epoch_us(v) if v is not None else None), failed


def test_a_timestamp_is_stored_as_a_utc_instant():
    v, failed = coerce("timestamp", "2026-03-01T08:00:00+02:00")
    assert not failed and v.utcoffset().total_seconds() == 0


@pytest.mark.parametrize("digits", range(10))
def test_iso_fractional_digits_zero_to_nine_truncate_to_microseconds(digits):
    frac = "123456789"[:digits]
    text = "2026-03-01T08:00:00" + ("." + frac if frac else "") + "Z"
    micro = int(frac[:6].ljust(6, "0")) if frac else 0
    assert ts(text) == (us(2026, 3, 1, 8, 0, 0, micro), False)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("2026-03-01T08:00:00Z", us(2026, 3, 1, 8)),
        ("2026-03-01T08:00:00z", us(2026, 3, 1, 8)),
        ("2026-03-01T08:00:00+00:00", us(2026, 3, 1, 8)),
        ("2026-03-01T08:00:00+02:00", us(2026, 3, 1, 6)),
        ("2026-03-01T08:00:00-05:30", us(2026, 3, 1, 13, 30)),
        ("2026-03-01T08:00:00+0530", us(2026, 3, 1, 2, 30)),
        ("2026-03-01T08:00:00-08", us(2026, 3, 1, 16)),
        ("2026-03-01T08:00:00.123+02:00", us(2026, 3, 1, 6, 0, 0, 123000)),
        ("2026-03-01T00:30:00+01:00", us(2026, 2, 28, 23, 30)),  # crosses a day boundary
        ("2026-03-01 08:00:00Z", us(2026, 3, 1, 8)),
        ("2026-03-01T08:00Z", us(2026, 3, 1, 8)),
        ("2026-03-01T08:00:00,25Z", us(2026, 3, 1, 8, 0, 0, 250000)),
        ("  2026-03-01T08:00:00Z  ", us(2026, 3, 1, 8)),
        ("2024-02-29T23:59:59Z", us(2024, 2, 29, 23, 59, 59)),
    ],
)
def test_iso_timestamps_with_an_offset_are_exact_utc_microseconds(text, expected):
    assert ts(text) == (expected, False)


@pytest.mark.parametrize(
    "text",
    [
        "2026-03-01T08:00:00",  # names no offset and no timezone is declared
        "2026-03-01",
        "2026-13-01T08:00:00Z",
        "2026-02-30T08:00:00Z",
        "2026-03-01T25:00:00Z",
        "2026-03-01T08:00:00+99:99",
        "03/01/2026",
        "not a time",
        "",
        "2026-03-01T08:00:00.Z",
    ],
)
def test_unreadable_timestamps_fail_rather_than_guess(text):
    assert ts(text) == (None, True)


def test_a_timestamp_naming_no_offset_is_read_in_the_declared_timezone():
    assert ts("2026-01-15T12:00:00", tz="America/New_York") == (us(2026, 1, 15, 17), False)
    assert ts("2026-07-15T12:00:00", tz="America/New_York") == (us(2026, 7, 15, 16), False)  # DST


def test_an_explicit_offset_beats_the_declared_timezone():
    assert ts("2026-01-15T12:00:00Z", tz="America/New_York") == (us(2026, 1, 15, 12), False)


def test_format_with_no_offset_needs_a_timezone():
    assert ts("01/03/2026 08:30", fmt="%d/%m/%Y %H:%M") == (None, True)
    assert ts("01/03/2026 08:30", fmt="%d/%m/%Y %H:%M", tz="UTC") == (us(2026, 3, 1, 8, 30), False)
    assert ts("01/03/2026 08:30", fmt="%d/%m/%Y %H:%M", tz="Asia/Tokyo") == (us(2026, 2, 28, 23, 30), False)


def test_format_carrying_an_offset_needs_no_timezone():
    assert ts("20260301080000+0200", fmt="%Y%m%d%H%M%S%z") == (us(2026, 3, 1, 6), False)


def test_format_is_used_instead_of_iso_not_as_a_fallback():
    assert ts("2026-03-01T08:00:00Z", fmt="%d/%m/%Y", tz="UTC") == (None, True)
    assert ts("garbage", fmt="%d/%m/%Y", tz="UTC") == (None, True)


def test_a_datetime_object_is_normalised_to_utc():
    assert coerce("timestamp", datetime(2026, 3, 1, 8, tzinfo=timezone.utc))[0].utcoffset().total_seconds() == 0
    assert coerce("timestamp", datetime(2026, 3, 1, 8)) == (None, True)
    assert coerce("timestamp", datetime(2026, 3, 1, 8), tz="UTC")[1] is False


def test_a_null_timestamp_is_not_a_failure():
    assert coerce("timestamp", None) == (None, False)


# ---- the "never raises" promise

GARBAGE = [
    None, "", " ", "\t\n", "\x00", "﻿", "é", "日本語", "😀", "٣", "１２３", "-", "+", ".", "e", "1e", "e1",
    "1" * 40, "9" * 400, "-" + "9" * 400, "0." + "0" * 400 + "1", "1e400", "-1e400", "1e-400",
    "NaN", "inf", "-inf", "Infinity", "2026", "2026-", "2026-03-01T", "T08:00", "0000-00-00T00:00:00Z",
    "9999-12-31T23:59:59Z", "2026-03-01T08:00:00+24:00", "2026-03-01T08:00:00+00:60", "%", "{}", "[]",
    "<a/>", "' OR 1=1 --", "\\", "\r\n",
    0, 1, -1, 2**31, 2**63, -(2**63) - 1, 10**30, 0.0, -0.0, 1e308, -1e308, 1e-320, 5e-324,
    float("nan"), float("inf"), float("-inf"), True, False, b"bytes", object(), [], {}, (1,),
    datetime(2026, 1, 1), datetime(2026, 1, 1, tzinfo=timezone.utc),
]

COERCERS = (
    [coercer_for(t) for t in ("string", "int32", "int64", "float64", "bool", "timestamp")]
    + [coercer_for("timestamp", "%Y-%m-%d", "UTC"), coercer_for("timestamp", "%d/%m/%Y %H:%M"),
       coercer_for("timestamp", None, "Europe/Paris")]
)


@pytest.mark.parametrize("coercer", COERCERS, ids=lambda c: getattr(c, "__name__", "c"))
def test_no_coercer_raises_on_garbage(coercer):
    for value in GARBAGE:
        result = coercer(value)
        assert isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], bool), repr(value)


def test_a_failed_coercion_never_carries_a_value():
    for coercer in COERCERS:
        for value in GARBAGE:
            v, failed = coercer(value)
            if failed:
                assert v is None, repr(value)


def test_an_enormous_digit_string_is_a_failure_not_an_exception():
    assert coerce("int64", "9" * 5000) == (None, True)


def test_an_integer_too_large_for_a_double_is_a_failure_not_an_exception():
    assert coerce("float64", 10**400) == (None, True)


@pytest.mark.parametrize("text", ["0001-01-01T00:00:00+05:00", "9999-12-31T23:59:59-05:00"])
def test_a_timestamp_at_the_edge_of_the_calendar_is_a_failure_not_an_exception(text):
    assert ts(text) == (None, True)


# ------------------------------------------------------------------ 4. failures counted per column


def test_coercion_failures_are_counted_per_column_through_a_mapping():
    m = small_mapping(
        {
            "v": {"type": "int32", "expr": "@v"},
            "w": {"type": "bool", "expr": "@w"},
            "f": {"type": "float64", "expr": "@f"},
        }
    )
    doc = (
        b'<a><item v="1" w="true" f="1.5"/><item v="99999999999" w="maybe" f="NaN"/>'
        b'<item v="x"/><item v="" w="" f=""/></a>'
    )
    _, stats, sink = run_small(m, {"d.xml": doc})
    assert stats.coercion_failures == {"t": {"v": 2, "w": 1, "f": 1}}
    assert [r["v"] for r in sink.rows("t")] == [1, None, None, None]  # an unreadable value is null
    assert stats.rows["t"] == 4


def test_an_absent_value_is_null_and_not_a_failure():
    m = small_mapping({"v": {"type": "int32", "expr": "@v"}})
    _, stats, sink = run_small(m, {"d.xml": b"<a><item/></a>"})
    assert sink.rows("t")[0]["v"] is None
    assert stats.coercion_failures == {}


def test_an_empty_value_is_null_unless_empty_as_null_is_off():
    on = small_mapping({"v": {"type": "int32", "expr": "@v"}})
    off = small_mapping({"v": {"type": "int32", "expr": "@v", "empty_as_null": False}})
    doc = {"d.xml": b'<a><item v=""/></a>'}
    assert run_small(on, doc)[1].coercion_failures == {}
    assert run_small(off, doc)[1].coercion_failures == {"t": {"v": 1}}


def test_failures_in_a_struct_field_are_counted_against_the_field():
    stats, _sink = run_shape_small_synthetic()
    assert stats.coercion_failures["scans"]["readings.value"] == 1  # "NaN" in one reading
    assert stats.coercion_failures["scans"]["lane"] == 1  # lane="x"
    assert stats.coercion_failures["parcels"]["weight"] == 1  # weight="heavy"
    assert stats.coercion_failures.get("loads") is None


def run_shape_small_synthetic():
    cm = compiled()
    sink = Collect()
    stats = run_bundle(cm, mem_bundle(bundle_files("flat")), sink, ingested_at=INGESTED)
    return stats, sink


def test_the_synthetic_timestamps_and_numbers_are_exact():
    _, sink = run_shape_small_synthetic()
    # a scan id is unique only within its parcel (P2 reuses S1)
    by_id = {(r["parcel_id"], r["scan_id"]): r for r in sink.rows("scans")}
    assert epoch_us(by_id["P1", "S1"]["at"]) == us(2026, 3, 1, 8, 1, 2, 100000)
    assert epoch_us(by_id["P1", "S2"]["at"]) == us(2026, 3, 1, 6, 5, 0, 123000)  # +02:00
    assert epoch_us(by_id["P2", "S3"]["at"]) == us(2026, 3, 1, 9, 0, 0, 123456)  # nine digits truncated
    loads = {r["manifest_id"]: r for r in sink.rows("loads")}
    assert epoch_us(loads["M1"]["created"]) == us(2026, 3, 1, 8, 0, 0, 500000)
    assert epoch_us(loads["M2"]["created"]) == us(2026, 3, 2, 10)
    assert by_id["P2", "S3"]["lane"] is None and by_id["P1", "S1"]["lane"] == 1
    assert [x["value"] for x in by_id["P2", "S3"]["readings"]] == [None]
    parcels = {r["parcel_id"]: r for r in sink.rows("parcels")}
    assert parcels["P2"]["weight"] is None and parcels["P1"]["weight"] == 2.5
    assert parcels["P1"]["fragile"] is True and parcels["P3"]["fragile"] is True


# ------------------------------------------------------------------ 6. rejects and safe parsing


def test_a_malformed_document_lands_in_rejects_naming_its_member(tmp_path):
    stats, sink = run_shape(tmp_path, "zip", "flat")
    (reject,) = sink.rows("_rejects")
    assert reject["source_file"] == "broken.xml"
    assert reject["doc_id"] == doc_id_for(f"{BUNDLE_NAME}.zip", "broken.xml")
    assert reject["source_bundle"] == f"{BUNDLE_NAME}.zip"
    assert reject["publish_period"] == "2026-03"
    assert reject["error"] and "not well-formed" in reject["error"]
    assert stats.documents_rejected == 1


def test_a_rejected_document_contributes_no_row_to_any_table(tmp_path):
    _, sink = run_shape(tmp_path, "zip", "flat")
    for table in TABLES:
        assert "broken.xml" not in {r["source_file"] for r in sink.rows(table)}


def test_one_bad_document_does_not_stop_the_bundle():
    files = {"a.xml": b"<a><item v='1'", "b.xml": b"<a><item v='2'/></a>", "c.xml": b"not xml"}
    _, stats, sink = run_small(small_mapping(), files)
    assert [r["v"] for r in sink.rows("t")] == ["2"]
    assert stats.documents_rejected == 2 and stats.documents_parsed == 1
    assert sorted(r["source_file"] for r in sink.rows("_rejects")) == ["a.xml", "c.xml"]


def test_a_document_over_max_bytes_is_rejected_and_never_parsed(monkeypatch):
    m = small_mapping()
    m["document"]["max_bytes"] = 100
    big = b"<a>" + b"<item v='1'/>" * 50 + b"</a>"
    small = b"<a><item v='1'/></a>"
    assert len(big) > 100 > len(small)
    cm = compiled(m)
    parsed = []
    real = cm.reader.parse
    monkeypatch.setattr(cm.reader, "parse", lambda data: (parsed.append(len(data)), real(data))[1])
    limits = []
    sink = Collect()
    bundle = mem_bundle({"big.xml": big, "small.xml": small}, limits=limits)
    stats = run_bundle(cm, bundle, sink, ingested_at=INGESTED)
    assert parsed == [len(small)]
    assert [r["source_file"] for r in sink.rows("_rejects")] == ["big.xml"]
    assert "max_bytes" in sink.rows("_rejects")[0]["error"]
    assert sink.count("t") == 1 and stats.documents_rejected == 1
    assert limits == [101, 101]  # never reads more than max_bytes + 1 of any document


def test_a_failure_inside_one_table_discards_that_documents_rows_from_every_table():
    m = {
        "version": 1,
        "document": {"match": ["*.xml"]},
        "tables": {
            "t": {"rows": "/a/item", "columns": {"v": "@v"}},
            "u": {"rows": "/a/item", "columns": {"w": "@w"}},
        },
    }
    cm = compiled(m)
    sink = Collect()
    stats = run_bundle(cm, mem_bundle({"d.xml": b"<a><item v='1' w='2'/></a>"}), sink, ingested_at=INGESTED)
    assert stats.rows["t"] == 1 and stats.rows["u"] == 1  # control: both tables fill when nothing fails


def test_an_expression_failing_at_run_time_rejects_the_whole_document(monkeypatch):
    m = small_mapping({"v": "@v", "w": "count(//item)"})
    cm = compiled(m)
    t = cm.tables[0]
    boom = {"n": 0}
    real = t.columns[1][1].eval

    def eval_then_fail(node, ctx):
        boom["n"] += 1
        if boom["n"] == 2:
            raise RuntimeError("synthetic failure")
        return real(node, ctx)

    monkeypatch.setattr(t.columns[1][1], "eval", eval_then_fail)
    sink = Collect()
    stats = run_bundle(
        cm, mem_bundle({"d.xml": b"<a><item v='1'/><item v='2'/><item v='3'/></a>"}), sink, ingested_at=INGESTED
    )
    assert sink.count("t") == 0  # the first row's values were evaluated but the document is atomic
    assert stats.rows["t"] == 0 and stats.rows["_rejects"] == 1
    assert "RuntimeError" in sink.rows("_rejects")[0]["error"]


def _marker_file(tmp_path):
    marker = "UNIQUE-MARKER-7f3a91c4"
    p = tmp_path / "secret.txt"
    p.write_text(marker, encoding="utf-8")
    return marker, p


def _everything_written(sink):
    parts = []
    for batches in sink.batches.values():
        for b in batches:
            parts.append(repr(b.to_pylist()))
    return "\n".join(parts)


def test_an_external_entity_is_never_read(tmp_path):
    marker, p = _marker_file(tmp_path)
    uri = p.as_uri()
    docs = {
        "elem.xml": f'<!DOCTYPE a [<!ENTITY x SYSTEM "{uri}">]><a><item v="1">&x;</item></a>'.encode(),
        "attr.xml": f'<!DOCTYPE a [<!ENTITY x SYSTEM "{uri}">]><a><item v="&x;"/></a>'.encode(),
        "param.xml": f'<!DOCTYPE a [<!ENTITY % x SYSTEM "{uri}"> %x;]><a><item v="1"/></a>'.encode(),
        "bare.xml": f'<!DOCTYPE a SYSTEM "{uri}"><a><item v="1"/></a>'.encode(),
    }
    m = small_mapping({"v": "@v", "text": "item", "whole": "string(.)", "all": {"type": "list<string>", "expr": "//text()"}})
    _, stats, sink = run_small(m, docs)
    assert marker not in _everything_written(sink)
    assert stats.documents == 4


def test_the_external_entity_control_would_be_visible_if_it_were_read(tmp_path):
    """Control: the same document parsed by a resolving parser does carry the marker."""
    marker, p = _marker_file(tmp_path)
    doc = f'<!DOCTYPE a [<!ENTITY x SYSTEM "{p.as_uri()}">]><a><item>&x;</item></a>'.encode()
    unsafe = etree.fromstring(doc, parser=etree.XMLParser(resolve_entities=True, no_network=True, load_dtd=True))
    assert marker in "".join(unsafe.itertext())


def test_an_entity_expansion_bomb_is_rejected_or_bounded_never_expanded():
    levels = 10
    ents = ['<!ENTITY l0 "lolololololololololol">']
    for i in range(1, levels):
        ents.append(f'<!ENTITY l{i} "' + f"&l{i - 1};" * 10 + '">')
    doc = (f"<!DOCTYPE a [{''.join(ents)}]><a><item v=\"1\">&l{levels - 1};</item></a>").encode()
    m = small_mapping({"v": "@v", "text": "item", "whole": "string(.)"})
    t0 = time.monotonic()
    _, stats, sink = run_small(m, {"bomb.xml": doc})
    assert time.monotonic() - t0 < 5
    for batches in sink.batches.values():
        for b in batches:
            for row in b.to_pylist():
                for value in row.values():
                    assert not isinstance(value, str) or len(value) < 10_000
    assert stats.documents_rejected + stats.documents_parsed == 1


def test_a_mapping_cannot_ask_for_the_document_beyond_its_bytes():
    """A document whose only fault is size is rejected even if it is well formed."""
    m = small_mapping()
    m["document"]["max_bytes"] = 10
    _, stats, sink = run_small(m, {"d.xml": b"<a><item v='1'/></a>"})
    assert stats.documents_rejected == 1 and sink.count("t") == 0


# ------------------------------------------------------------------ 7. redaction

SECRET = "ZQX-VALUE-90210"


def _failing_parse(cm, monkeypatch):
    def parse(data):
        raise ValueError(f"cannot read {SECRET}")

    monkeypatch.setattr(cm.reader, "parse", parse)


def test_redacted_reject_text_carries_no_document_value(monkeypatch):
    cm = compiled(small_mapping())
    _failing_parse(cm, monkeypatch)
    sink = Collect()
    run_bundle(cm, mem_bundle({"d.xml": f"<a><item v='{SECRET}'/></a>".encode()}), sink, ingested_at=INGESTED, redact=True)
    (reject,) = sink.rows("_rejects")
    assert SECRET not in reject["error"]
    assert reject["error"].startswith("ValueError [") and len(reject["error"]) > len("ValueError []")


def test_control_without_redact_the_same_value_is_visible(monkeypatch):
    cm = compiled(small_mapping())
    _failing_parse(cm, monkeypatch)
    sink = Collect()
    run_bundle(cm, mem_bundle({"d.xml": b"<a/>"}), sink, ingested_at=INGESTED, redact=False)
    assert SECRET in sink.rows("_rejects")[0]["error"]


def test_a_redacted_error_is_the_same_for_the_same_failure(monkeypatch):
    cm = compiled(small_mapping())
    _failing_parse(cm, monkeypatch)
    texts = []
    for _ in range(2):
        sink = Collect()
        run_bundle(cm, mem_bundle({"d.xml": b"<a/>"}), sink, ingested_at=INGESTED, redact=True)
        texts.append(sink.rows("_rejects")[0]["error"])
    assert texts[0] == texts[1]


def test_redaction_does_not_hide_the_engines_own_document_errors():
    """A DocumentError names a line or a limit, never a value, so it stays readable."""
    sink = Collect()
    cm = compiled(small_mapping())
    run_bundle(cm, mem_bundle({"d.xml": b"<a><item></a>"}), sink, ingested_at=INGESTED, redact=True)
    assert "not well-formed" in sink.rows("_rejects")[0]["error"]


# ------------------------------------------------------------------ 8. drift


def drift_mapping(policy):
    m = copy.deepcopy(MAPPING)
    m["on_unknown_path"] = policy
    return m


def drift_bundle():
    clean = manifest_xml("M1", parcels=[])
    drifted = manifest_xml("M2", parcels=[], extras='<zz q="1"><yy/></zz>')
    return mem_bundle({"clean.xml": clean, "drifted.xml": drifted})


def test_warn_counts_unknown_paths_and_still_ingests_the_document(caplog):
    cm = compiled(drift_mapping("warn"))
    sink = Collect()
    with caplog.at_level(logging.WARNING, logger="dag_tools.documents"):
        stats = run_bundle(cm, drift_bundle(), sink, ingested_at=INGESTED)
    assert stats.unknown_paths == {"/Manifest/zz", "/Manifest/zz/@q", "/Manifest/zz/yy"}
    assert stats.documents_with_unknown_paths == 1
    assert stats.documents_parsed == 2 and stats.documents_rejected == 0
    assert sink.count("loads") == 2
    assert any("not in expected_paths" in r.getMessage() for r in caplog.records)


def test_ignore_does_not_even_look():
    cm = compiled(drift_mapping("ignore"))
    sink = Collect()
    stats = run_bundle(cm, drift_bundle(), sink, ingested_at=INGESTED)
    assert stats.unknown_paths == set() and stats.documents_with_unknown_paths == 0
    assert sink.count("loads") == 2


def test_fail_rejects_the_drifted_document_and_keeps_the_clean_one():
    cm = compiled(drift_mapping("fail"))
    sink = Collect()
    stats = run_bundle(cm, drift_bundle(), sink, ingested_at=INGESTED)
    assert stats.documents_rejected == 1 and stats.documents_parsed == 1
    assert [r["manifest_id"] for r in sink.rows("loads")] == ["M1"]
    (reject,) = sink.rows("_rejects")
    assert reject["source_file"] == "drifted.xml"
    assert "expected_paths" in reject["error"]
    assert "zz" not in reject["error"]  # the count, not the names


def test_a_clean_run_reports_no_drift_under_every_policy():
    for policy in ("warn", "fail"):
        cm = compiled(drift_mapping(policy))
        stats = run_bundle(cm, mem_bundle({"c.xml": manifest_xml("M1", parcels=[])}), Collect(), ingested_at=INGESTED)
        assert stats.unknown_paths == set() and stats.documents_with_unknown_paths == 0


def test_the_synthetic_bundle_itself_has_no_drift():
    stats, _ = run_shape_small_synthetic()
    assert stats.unknown_paths == set()


def test_an_unknown_attribute_is_drift_too():
    doc = manifest_xml("M1", parcels=[]).replace(b'<Manifest id="M1"', b'<Manifest zz="1" id="M1"')
    stats = run_bundle(compiled(), mem_bundle({"d.xml": doc}), Collect(), ingested_at=INGESTED)
    assert stats.unknown_paths == {"/Manifest/@zz"}


# ------------------------------------------------------------------ 10. logging


def _values_in_fixture():
    values = set()
    for name, data in bundle_files("flat").items():
        if not name.endswith(".xml") or name == "broken.xml":
            continue
        for el in etree.fromstring(data).iter():
            if not isinstance(el.tag, str):
                continue
            for v in el.attrib.values():
                values.add(v)
            if el.text and el.text.strip():
                values.add(el.text.strip())
    names = {BUNDLE_NAME, f"{BUNDLE_NAME}.zip", "2026-03"}
    for member in bundle_files("flat"):
        names.update(member.split("/"))
        names.add(member.rsplit(".", 1)[0])
    for table, spec in MAPPING["tables"].items():
        names.add(table)
        names.update(spec["columns"])
    names.update(["_rejects", "doc_id", "row_id"] + SYSTEM)
    return sorted(v for v in values if len(v) >= 3 and v not in names)


def _logged_values(records, values):
    hits = []
    for rec in records:
        if rec.levelno < logging.INFO:
            continue
        text = rec.getMessage()
        for v in values:
            if re.search(r"(?<![\w.\-])" + re.escape(v) + r"(?![\w.\-])", text):
                hits.append((rec.levelname, v))
    return hits


def test_the_fixture_has_values_worth_scanning_for():
    values = _values_in_fixture()
    assert len(values) >= 15
    assert {"north", "REPAIR", "temp"} <= set(values)


def test_no_log_record_at_info_or_above_carries_a_document_value(tmp_path, caplog):
    values = _values_in_fixture()
    caplog.set_level(logging.DEBUG)
    for shape in ("zip", "tree"):
        run_shape(tmp_path, shape, "numbered")
    run_shape(tmp_path, "zip", "flat", cm=compiled(drift_mapping("warn")))
    # a drifted document and a rejected one exercise the warning paths too
    run_bundle(compiled(drift_mapping("fail")), drift_bundle(), Collect(), ingested_at=INGESTED)
    assert any(r.levelno >= logging.INFO for r in caplog.records)
    assert any(r.levelno == logging.WARNING for r in caplog.records)
    assert _logged_values(caplog.records, values) == []


def test_control_the_scanner_finds_a_value_when_one_is_logged(caplog):
    values = _values_in_fixture()
    caplog.set_level(logging.DEBUG)
    logging.getLogger("dag_tools.documents.engine").warning("planted %s here", values[0])
    assert _logged_values(caplog.records, values) == [("WARNING", values[0])]


def test_control_the_scanner_ignores_debug_records(caplog):
    values = _values_in_fixture()
    caplog.set_level(logging.DEBUG)
    logging.getLogger("dag_tools.documents.engine").debug("planted %s here", values[0])
    assert _logged_values(caplog.records, values) == []


# ------------------------------------------------------------------ 13. benchmark


def test_throughput_on_a_bulk_manifest(record_property, capsys):
    n = 20_000  # 120,001 rows: 1 load + n parcels + 4n scans + n flags
    m = copy.deepcopy(MAPPING)
    m["document"]["max_bytes"] = 10**9
    cm = compiled(m)
    rows = {"n": 0}

    def sink(table, batch):
        rows["n"] += batch.num_rows

    bundle = mem_bundle({"bulk.xml": bulk_manifest(n)}, key="bulk_2026-03")
    t0 = time.monotonic()
    stats = run_bundle(cm, bundle, sink, ingested_at=INGESTED)
    elapsed = time.monotonic() - t0
    assert stats.documents_rejected == 0
    assert rows["n"] == sum(stats.rows.values()) == 1 + n + 4 * n + n
    rate = rows["n"] / elapsed
    record_property("rows_per_second", rate)
    with capsys.disabled():
        print(f"\n[documents bench] {rows['n']} rows in {elapsed:.2f}s = {rate:,.0f} rows/s")
    assert rate > 0
    if os.environ.get("DAGTOOLS_BENCH") == "1":
        assert rate >= 20_000


# ------------------------------------------------------------------ 14. empty


def test_empty_tables_has_a_schema_for_every_table_and_rejects():
    cm = compiled()
    schemas = empty_tables(cm)
    assert set(schemas) == set(TABLES) | {"_rejects"}
    for name in TABLES:
        assert schemas[name].equals(cm.schema_for(name))
        assert "doc_id" in schemas[name].names
    assert "error" in schemas["_rejects"].names


def test_an_empty_bundle_produces_zero_rows_without_error():
    cm = compiled()
    sink = Collect()
    stats = run_bundle(cm, mem_bundle({}), sink, ingested_at=INGESTED)
    assert sink.batches == {}
    assert stats.documents == 0 and stats.members == 0
    assert all(n == 0 for n in stats.rows.values()) and set(stats.rows) == set(TABLES) | {"_rejects"}


def test_a_bundle_with_no_matching_member_produces_zero_rows():
    cm = compiled()
    sink = Collect()
    stats = run_bundle(cm, mem_bundle({"readme.txt": b"x", "image.png": b"y"}), sink, ingested_at=INGESTED)
    assert sink.batches == {} and stats.documents == 0 and stats.members == 2


def test_a_document_with_no_matching_rows_yields_no_batch_for_that_table():
    sink = Collect()
    run_bundle(compiled(), mem_bundle({"d.xml": manifest_xml("M1", parcels=[])}), sink, ingested_at=INGESTED)
    assert sink.count("loads") == 1 and sink.count("parcels") == 0 and sink.count("scans") == 0


def test_flush_rows_splits_batches_without_losing_rows():
    sink = Collect()
    files = {f"d{i}.xml": b"<a>" + b"<item v='1'/>" * 5 + b"</a>" for i in range(2)}  # flush is per document
    run_bundle(compiled(small_mapping()), mem_bundle(files), sink, ingested_at=INGESTED, flush_rows=3)
    sizes = [b.num_rows for b in sink.batches["t"]]
    assert sum(sizes) == 10 and len(sizes) >= 2


# ------------------------------------------------------------------ 15. scoped index keys
#
# A scan id is unique only within its parcel: P2 of M1 reuses S1. Every index
# over flags, photos and watch entries is scoped by parcel, so a lookup reads
# only its own parcel's entries. The control removes every scope from the same
# mapping and asserts the rows come back cross-linked: without it, the scoped
# assertions could pass on a fixture that never reused an id.


def _unscoped(mapping=MAPPING):
    m = copy.deepcopy(mapping)
    for ix in m["indexes"].values():
        ix.pop("scope", None)
    for t in m["tables"].values():
        for col in t["columns"].values():
            if isinstance(col, dict) and isinstance(col.get("lookup"), dict):
                col["lookup"].pop("scope", None)
    return m


def _synthetic_rows(mapping):
    sink = Collect()
    stats = run_bundle(compiled(mapping), mem_bundle(bundle_files("flat")), sink, ingested_at=INGESTED)
    scans = {(r["parcel_id"], r["scan_id"]): r for r in sink.rows("scans")}
    flags = {r["flag_id"]: r for r in sink.rows("flags")}
    return stats, scans, flags


def test_a_scoped_lookup_reads_only_its_own_parcel_in_every_mode():
    _, scans, flags = _synthetic_rows(MAPPING)
    p1s1, p1s2, p2s1 = scans["P1", "S1"], scans["P1", "S2"], scans["P2", "S1"]
    # exists
    assert p1s1["is_flagged"] is True and p2s1["is_flagged"] is True
    assert p1s1["watched"] is False and p2s1["watched"] is True
    assert p2s1["has_photo"] is False and scans["P2", "S3"]["has_photo"] is True
    # count
    assert p1s1["flag_count"] == 1 and p2s1["flag_count"] == 1 and p1s2["flag_count"] == 2
    assert flags["F4"]["photo_count"] == 0
    # first / last
    assert (p1s1["first_flag"], p1s1["last_flag"]) == ("DENT", "DENT")
    assert (p1s2["first_flag"], p1s2["last_flag"]) == ("DENT", "WET")
    assert (p2s1["first_flag"], p2s1["last_flag"]) == ("LOST", "LOST")
    # list
    assert p1s1["flag_codes"] == ["DENT"] and p2s1["flag_codes"] == ["LOST"]
    assert p1s2["flag_codes"] == ["DENT", "WET"]


def test_control_without_scope_the_same_mapping_cross_links_the_parcels():
    _, scans, flags = _synthetic_rows(_unscoped())
    p1s1, p2s1 = scans["P1", "S1"], scans["P2", "S1"]
    assert p1s1["watched"] is True  # P2's watch entry
    assert p2s1["has_photo"] is True  # P1's photos
    assert p1s1["flag_count"] == 2 and p2s1["flag_count"] == 2
    assert p1s1["flag_codes"] == ["DENT", "LOST"] and p2s1["flag_codes"] == ["DENT", "LOST"]
    assert (p2s1["first_flag"], p2s1["last_flag"]) == ("DENT", "LOST")
    assert flags["F4"]["photo_count"] == 2


def _scoped_small(lookup_extra=None, column_extra=None):
    lookup = {"index": "ix", "scope": "@g", "keys": "r/@k", "field": "v", "mode": "list"}
    lookup.update(lookup_extra or {})
    col = {"lookup": lookup}
    col.update(column_extra or {})
    return small_mapping(
        {"v": col},
        indexes={"ix": {"rows": "/a/e", "scope": "@g", "keys": "@k", "values": {"v": "@v"}}},
    )


SCOPED_DOC = (
    b"<a>"
    b"<e g='1' k='x' v='a'/><e g='1' k='y' v='b'/><e k='x' v='c'/><e g='' k='x' v='c2'/><e g='2' k='x' v='d'/>"
    b"<item g='1'><r k='y'/><r k='x'/></item>"
    b"<item g='2'><r k='x'/></item>"
    b"<item><r k='x'/></item>"
    b"<item g=''><r k='x'/></item>"
    b"</a>"
)


def test_a_null_or_empty_scope_on_either_side_matches_nothing_and_is_counted():
    _, stats, sink = run_small(_scoped_small(), {"d.xml": SCOPED_DOC})
    assert [r["v"] for r in sink.rows("t")] == [["a", "b"], ["d"], [], []]
    assert stats.null_scopes["t"]["v"] == 2  # the two items naming no scope
    assert stats.null_scopes["indexes.ix"]["scope"] == 2  # the two entries filed under none


def test_order_keys_returns_entries_in_the_order_the_keys_name_them():
    _, _, sink = run_small(_scoped_small({"order": "keys"}), {"d.xml": SCOPED_DOC})
    assert sink.rows("t")[0]["v"] == ["b", "a"]
    _, _, sink = run_small(_scoped_small(), {"d.xml": SCOPED_DOC})
    assert sink.rows("t")[0]["v"] == ["a", "b"]  # document order stays the default


def test_order_keys_lists_an_entry_reached_by_two_keys_once():
    m = small_mapping(
        {"v": {"lookup": {"index": "ix", "keys": "r/@k", "field": "v", "mode": "list", "order": "keys"}}},
        indexes={"ix": {"rows": "/a/e", "keys": "k/@id", "values": {"v": "@v"}}},
    )
    doc = b"<a><e v='a'><k id='x'/></e><e v='b'><k id='y'/><k id='x'/></e><item><r k='y'/><r k='x'/></item></a>"
    _, _, sink = run_small(m, {"d.xml": doc})
    assert sink.rows("t")[0]["v"] == ["b", "a"]


# ------------------------------------------------------------------ 16. prefer


def test_prefer_picks_by_precedence_and_counts_a_list_with_no_preferred_value():
    stats, _, flags = _synthetic_rows(MAPPING)
    assert flags["F1"]["verdict"] == "REPAIR"  # reviews say REPAIR and ACCEPT
    assert flags["F2"]["verdict"] == "none" and flags["F3"]["verdict"] == "none"  # no review: when_empty
    assert flags["F4"]["verdict"] is None  # only HOLD, which is not preferred
    assert stats.unmapped_values["flags"]["verdict"] == 1


def _prefer_small(prefer, **col):
    return small_mapping({"v": {"expr": "d/@c", "prefer": prefer, **col}})


PREFER_DOC = b"<a><item><d c='ok'/><d c='bad'/></item><item/><item><d c='meh'/></item></a>"


def test_prefer_over_an_expression_list():
    _, stats, sink = run_small(_prefer_small(["bad", "ok"]), {"d.xml": PREFER_DOC})
    assert [r["v"] for r in sink.rows("t")] == ["bad", None, None]
    assert stats.unmapped_values["t"]["v"] == 1  # the empty list is not a miss; 'meh' is


def test_prefer_applies_map_to_each_element_first_and_compares_as_strings():
    m = _prefer_small([2, 1], type="int64", map={"values": {"ok": 1, "bad": 2}})
    _, _, sink = run_small(m, {"d.xml": PREFER_DOC})
    assert [r["v"] for r in sink.rows("t")] == [2, None, None]


def test_prefer_on_an_empty_list_gives_when_empty():
    _, stats, sink = run_small(_prefer_small(["ok"], when_empty="none"), {"d.xml": PREFER_DOC})
    assert [r["v"] for r in sink.rows("t")] == ["ok", "none", None]
    assert stats.unmapped_values["t"]["v"] == 1


def test_prefer_over_an_expression_returning_one_value_fails_the_load_naming_the_column():
    for expr in ("string(d/@c)", "concat(d/@c, '')", "count(d) > 0"):
        with pytest.raises(MappingError) as e:
            compiled(small_mapping({"pick": {"expr": expr, "prefer": ["x"]}}))
        assert any("pick" in p and "prefer" in p for p in e.value.problems), (expr, e.value.problems)


# ------------------------------------------------------------------ 17. parser limits

PLANTED = "planted-stop-7f3a"


def _with_huge_tree(on):
    m = copy.deepcopy(MAPPING)
    m["document"]["huge_tree"] = on
    return m


def test_a_document_over_a_parser_limit_is_rejected_naming_the_option_and_quoting_nothing():
    _, stats, sink = run_small(_with_huge_tree(False), {"relay.xml": relay_manifest(300, last_stop=PLANTED)})
    assert stats.documents_rejected == 1
    (reject,) = sink.rows("_rejects")
    assert "parser limit" in reject["error"] and "document.huge_tree" in reject["error"]
    assert PLANTED not in reject["error"] and "Leg" not in reject["error"]


def test_the_same_document_parses_with_huge_tree_on():
    _, stats, _sink = run_small(_with_huge_tree(True), {"relay.xml": relay_manifest(300, last_stop=PLANTED)})
    assert stats.documents_rejected == 0
    assert stats.rows["parcels"] == 1 and stats.rows["scans"] == 1


def test_control_a_relay_under_the_limit_parses_by_default():
    # Without this, the two above would pass on a reader that refused every relay.
    _, stats, _ = run_small(_with_huge_tree(False), {"relay.xml": relay_manifest(200)})
    assert stats.documents_rejected == 0 and stats.rows["parcels"] == 1


def test_a_malformed_document_is_still_called_malformed_not_a_limit():
    _, _, sink = run_small(_with_huge_tree(False), {"bad.xml": b"<Manifest><Parcel></Manifest>"})
    (reject,) = sink.rows("_rejects")
    assert "not well-formed" in reject["error"] and "huge_tree" not in reject["error"]


def test_max_bytes_is_enforced_with_huge_tree_on():
    m = _with_huge_tree(True)
    m["document"]["max_bytes"] = 1000
    _, _, sink = run_small(m, {"relay.xml": relay_manifest(300)})
    (reject,) = sink.rows("_rejects")
    assert "max_bytes" in reject["error"]


def test_the_default_parser_accepts_a_30mb_document_of_several_hundred_thousand_elements():
    """Size and element count alone are not a limit; the answer is in the docs.

    Measured with lxml 6.1.3 / libxml2 2.11.9: a 30.0 MB manifest of 587,159
    elements parses with `huge_tree: false`. What the default parser refuses
    is depth over 256 and a single name, attribute value or CDATA section
    over its length cap -- not a large document.
    """
    doc, elements = large_manifest(30_000_000)
    assert len(doc) >= 30_000_000 and elements >= 300_000
    m = _with_huge_tree(False)
    m["document"]["max_bytes"] = 10**9
    cm = compiled(m)
    stats = run_bundle(cm, mem_bundle({"large.xml": doc}), lambda table, batch: None, ingested_at=INGESTED)
    assert stats.documents_rejected == 0
    assert stats.rows["parcels"] == stats.rows["scans"] == (elements - 2) // 3


# ------------------------------------------------------------------ 18. document.match is path-aware

NESTED = {
    "top.xml": b'<a><item v="top"/></a>',
    "d/one.xml": b'<a><item v="one"/></a>',
    "d/e/two.xml": b'<a><item v="two"/></a>',
    "d/e/f/three.xml": b'<a><item v="three"/></a>',
    "d/notes.txt": b"not a document",
}


def _matched(globs):
    m = small_mapping()
    m["document"]["match"] = globs
    _, stats, sink = run_small(m, NESTED)
    return stats.documents, sorted(r["source_file"] for r in sink.rows("t"))


def test_a_star_stays_within_one_segment():
    # fnmatch's `*` crosses `/`, which made `*.xml` match every depth
    assert _matched(["*.xml"]) == (1, ["top.xml"])


def test_star_slash_star_is_exactly_one_level_down():
    assert _matched(["*/*.xml"]) == (1, ["d/one.xml"])


def test_double_star_matches_zero_or_more_segments():
    assert _matched(["**/*.xml"]) == (4, ["d/e/f/three.xml", "d/e/two.xml", "d/one.xml", "top.xml"])
    assert _matched(["d/**/*.xml"]) == (3, ["d/e/f/three.xml", "d/e/two.xml", "d/one.xml"])


def test_the_examples_two_globs_cover_the_root_and_one_level_only():
    assert _matched(MAPPING["document"]["match"]) == (2, ["d/one.xml", "top.xml"])


def test_matching_is_case_sensitive():
    assert _matched(["*.XML"]) == (0, [])
