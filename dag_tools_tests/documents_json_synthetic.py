"""The synthetic "parcel depot" as JSON documents.

Same invented depot as ``documents_synthetic`` (the only document format that
appears anywhere in dag-tools), written as JSON objects and read with
JMESPath. ``json_bundle_files("flat")`` carries the same data as
``bundle_files("flat")``, and ``JSON_MAPPING`` declares the same tables and
columns as ``documents_synthetic.MAPPING``; ``test_documents_json`` asserts
the two routes produce the same rows.

Two spellings differ from the XML document, on purpose:

* numbers a source writes as numbers are written as numbers (``"lane": 1``),
  and the values the XML route counts as coercion failures (``"heavy"``,
  ``"x"``, ``"NaN"``) stay strings;
* a scan, a flag, a photo and a watch entry carry the id of their parcel
  (``"parcel": "P1"``). A lookup's ``keys`` and ``scope`` are read from ONE
  level of the ``rows`` chain, so a scoped lookup whose scope lives on the
  parent level has to be able to read it off the row itself.

``rich_json_files()`` is a second bundle, with its own ``RICH_MAPPING``, for
what the equivalence bundle must not contain: awkward keys, numbers as both
numbers and strings, nested lists, a joined string, a composite id, file
references with a folder the bundle lacks, and files with nothing in them.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence

from dag_tools_tests.documents_synthetic import png_bytes

# ------------------------------------------------------------------ documents


def manifest_json(
    manifest_id: str,
    *,
    parcels: List[Dict[str, Any]],
    reviews: Sequence[Dict[str, Any]] = (),
    photos: Optional[Sequence[Dict[str, Any]]] = None,
    watch: Optional[Sequence[Dict[str, Any]]] = None,
    created: str = "2026-03-01T08:00:00.5Z",
    depot: Optional[Dict[str, Any]] = None,
) -> bytes:
    doc: Dict[str, Any] = {
        "id": manifest_id,
        "created": created,
        "Depot": depot if depot is not None else {"site": "north", "dock": "D4", "shift": "early"},
        "parcels": parcels,
    }
    if photos is not None:
        doc["photos"] = list(photos)
    if watch is not None:
        doc["watch"] = list(watch)
    if reviews:
        doc["reviews"] = list(reviews)
    return json.dumps(doc).encode()


def _scan(
    parcel: str, sid: str, at: str, lane: Any, readings: Sequence[Sequence[Any]],
    limits: Sequence[Dict[str, Any]] = (), items: Sequence[Sequence[Any]] = (),
) -> Dict[str, Any]:
    s: Dict[str, Any] = {
        "id": sid, "parcel": parcel, "at": at, "lane": lane,
        "readings": [{"kind": k, "value": v} for k, v in readings],
    }
    if limits:
        s["limits"] = list(limits)
    if items:
        s["items"] = [{"sku": k, "qty": q} for k, q in items]
    return s


def _flag(parcel: str, fid: str, code: str, scans: Sequence[str]) -> Dict[str, Any]:
    return {"id": fid, "parcel": parcel, "code": code, "scan_refs": list(scans)}


def json_bundle_files(variant: str = "flat") -> Dict[str, bytes]:
    """The JSON twin of ``documents_synthetic.bundle_files``: same data, same photos, same reject."""
    def photo_path(stem: str, name: str, sub: str = "01") -> str:
        return f"{stem}/{name}" if variant == "flat" else f"{stem}/{sub}/{name}"

    m1 = manifest_json(
        "M1",
        parcels=[
            {
                "id": "P1", "weight": 2.5, "fragile": True,
                "scans": [
                    _scan("P1", "S1", "2026-03-01T08:01:02.1Z", 1, [("temp", 4.5), ("humidity", 40)],
                          [{"kind": "temp", "low": 2}, {"kind": "humidity", "high": 80}],
                          [("A1", 2), ("B7", 1)]),
                    _scan("P1", "S2", "2026-03-01T08:05:00.123+02:00", 2, [("temp", 5.25)],
                          [{"kind": "temp", "high": 9}]),
                ],
                "flags": [_flag("P1", "F1", "DENT", ["S1", "S2"]), _flag("P1", "F2", "WET", ["S2"])],
            },
            {
                "id": "P2", "weight": "heavy", "fragile": False,
                "scans": [
                    _scan("P2", "S3", "2026-03-01T09:00:00.123456789Z", "x", [("temp", "NaN")]),
                    _scan("P2", "S1", "2026-03-01T09:10:00Z", 4, [("temp", 6)]),
                ],
                "flags": [_flag("P2", "F4", "LOST", ["S1"])],
            },
        ],
        photos=[
            {"parcel": "P1", "path": "capture\\S1.png", "refs": ["S1"]},
            {"parcel": "P1", "path": "capture\\S2.png", "refs": ["S2", "S1"]},
            {"parcel": "P2", "path": "capture\\gone.png", "refs": ["S3"]},
        ],
        watch=[{"parcel": "P1", "scan": "S2"}, {"parcel": "P2", "scan": "S1"}],
        reviews=[
            {"flagRef": "F1", "by": "ops", "decision": "REPAIR"},
            {"flagRef": "F1", "by": "lead", "decision": "ACCEPT"},
            {"flagRef": "F4", "by": "ops", "decision": "HOLD"},
        ],
    )
    m2 = manifest_json(
        "M2",
        parcels=[
            {
                "id": "P3", "weight": 1, "fragile": 1,
                "scans": [_scan("P3", "S4", "2026-03-02T10:00:00Z", 3, [("temp", 3)])],
                "flags": [_flag("P3", "F3", "LATE", ["S4"])],
            }
        ],
        created="2026-03-02T10:00:00.000000001Z",
        depot={"site": "south", "dock": "D1", "shift": "night"},
    )
    files: Dict[str, bytes] = {
        "manifest_m1.json": m1,
        "manifest_m2.json": m2,
        "broken.json": b'{"id": "X", "parcels": [',
        photo_path("manifest_m1", "S1.png"): png_bytes(4, 3),
        photo_path("manifest_m1", "S2.png"): png_bytes(5, 2),
        "readme.txt": b"not a document",
    }
    if variant == "numbered":
        files[photo_path("manifest_m1", "S2.png", sub="02")] = png_bytes(6, 6)
    return files


# ------------------------------------------------------------------ mapping

_TEMP = "limits[?kind=='temp']"

JSON_MAPPING: Dict[str, Any] = {
    "version": 1,
    "document": {"format": "json", "match": ["*.json", "*/*.json"], "max_bytes": 1_000_000},
    "lineage": {"publish_period": {"from": "bundle_name", "regex": r"(?P<period>\d{4}-\d{2})"}},
    "on_unknown_path": "warn",
    "indexes": {
        "reviews": {
            "rows": "reviews[]",
            "keys": "flagRef",
            "values": {"decision": "decision", "by": "by"},
        },
        "photo_refs": {
            "rows": "photos[]",
            "scope": "parcel",
            "keys": "refs[]",
            "values": {"path": "path"},
        },
        "watched": {
            "rows": "watch[]",
            "scope": "parcel",
            "keys": "scan",
            "values": {"scan": "scan"},
        },
        "flags_by_scan": {
            "rows": [{"as": "p", "each": "parcels[]"}, {"as": "f", "each": "flags[]"}],
            "from": "f",
            "scope": "parcel",
            "keys": "scan_refs[]",
            "values": {"code": {"from": "f", "expr": "code"}},
        },
    },
    "tables": {
        "loads": {
            "rows": "@",
            "row_id": "{manifest_id}",
            "columns": {
                "manifest_id": "id",
                "created": {"type": "timestamp", "expr": "created"},
                "site": "Depot.site",
                "dock": "Depot.dock",
                "shift": {"expr": "Depot.shift", "map": {"values": {"early": "E", "late": "L"}, "default": "?"}},
                "parcel_count": {"type": "int32", "expr": "length(parcels)"},
            },
        },
        "parcels": {
            "rows": [{"as": "p", "each": "parcels[]"}],
            "row_id": "{manifest_id}/{parcel_id}",
            "columns": {
                "manifest_id": {"from": "root", "expr": "id"},
                "parcel_id": {"from": "p", "expr": "id"},
                "weight": {"type": "float64", "expr": "weight"},
                "fragile": {"type": "bool", "expr": "fragile"},
                "flag_ids": {"type": "list<string>", "expr": "flags[].id"},
                "scan_count": {"type": "int64", "expr": "length(scans)"},
            },
        },
        "scans": {
            "rows": [{"as": "p", "each": "parcels[]"}, {"as": "s", "each": "scans[]"}],
            "row_id": "{doc_id}:{row_number}",
            "columns": {
                "scan_id": {"from": "s", "expr": "id"},
                "parcel_id": {"from": "p", "expr": "id"},
                "at": {"type": "timestamp", "expr": "at"},
                "lane": {"type": "int32", "expr": "lane"},
                "readings": {
                    "type": "list<struct>",
                    "rows": "readings[]",
                    "fields": {"kind": "kind", "value": {"type": "float64", "expr": "value"}},
                },
                "temp_low": {"type": "float64", "expr": _TEMP + ".low"},
                "temp_bound": {"type": "float64", "first_of": [_TEMP + ".low", _TEMP + ".high"]},
                "first_sku": {"expr": "items[].sku", "when_empty": "none"},
                "item_skus": {"type": "list<string>", "expr": "items[].sku"},
                "has_photo": {"lookup": {"index": "photo_refs", "scope": "parcel", "keys": "id", "mode": "exists"}},
                "watched": {"lookup": {"index": "watched", "scope": "parcel", "keys": "id", "mode": "exists"}},
                "is_flagged": {"lookup": {"index": "flags_by_scan", "scope": "parcel", "keys": "id", "mode": "exists"}},
                "flag_count": {"lookup": {"index": "flags_by_scan", "scope": "parcel", "keys": "id", "mode": "count"}},
                "first_flag": {
                    "lookup": {"index": "flags_by_scan", "scope": "parcel", "keys": "id", "field": "code", "mode": "first"}
                },
                "last_flag": {
                    "lookup": {"index": "flags_by_scan", "scope": "parcel", "keys": "id", "field": "code", "mode": "last"}
                },
                "flag_codes": {
                    "lookup": {"index": "flags_by_scan", "scope": "parcel", "keys": "id", "field": "code", "mode": "list"}
                },
            },
        },
        "flags": {
            "rows": [{"as": "p", "each": "parcels[]"}, {"as": "f", "each": "flags[]"}],
            "columns": {
                "flag_id": "id",
                "code": "code",
                "scan_ids": {"type": "list<string>", "expr": "scan_refs[]"},
                "review_count": {"lookup": {"index": "reviews", "keys": "id", "mode": "count"}},
                "decision": {"lookup": {"index": "reviews", "keys": "id", "field": "decision", "mode": "last"}},
                "decisions": {
                    "lookup": {"index": "reviews", "keys": "id", "field": "decision", "mode": "list", "sort": True}
                },
                "verdict": {
                    "type": "string",
                    "lookup": {"index": "reviews", "keys": "id", "field": "decision", "mode": "list"},
                    "prefer": ["REJECT", "REPAIR", "ACCEPT"],
                    "when_empty": "none",
                },
                "photo_count": {
                    "lookup": {"index": "photo_refs", "scope": "parcel", "keys": "scan_refs[]", "mode": "count"}
                },
            },
        },
        "photos": {
            "rows": "photos[]",
            "columns": {
                "parcel_id": "parcel",
                "scan_ids": {"type": "list<string>", "expr": "refs[]"},
                "image": {
                    "type": "file",
                    "expr": "path",
                    "resolve": ["document_dir", {"basename_under": "{document_stem}"}],
                    "probe": "image",
                },
            },
        },
    },
}


# ------------------------------------------------------------------ the rich depot

RICH_DOC: Dict[str, Any] = {
    "Depot": {"Dock door": 7, "Count(late)": "4", "a.b": "dotted", 'quote"d': "q"},
    "parcels": [
        {
            "id": "P1", "tags": "red; blue;; green", "code": "ZX-0042", "qty": 3, "ratio": "2.5",
            "photo": "outbound/p1.png", "grid": [[1, 2], [3]],
            "scans": [{"n": 1, "ok": True}, {"n": "2", "ok": False}],
        },
        {
            "id": "P2", "tags": "", "code": "nope", "qty": "7", "ratio": 4, "photo": "p2.png",
            "grid": [], "scans": [],
        },
        {
            "id": "P3", "tags": None, "code": "ZX-0007", "qty": 3.0, "ratio": "abc", "photo": "missing/p9.png",
            "grid": [[], [4]], "scans": [{"n": 5, "ok": "true"}],
        },
    ],
}


def rich_json_files() -> Dict[str, bytes]:
    return {
        "d/depot.json": json.dumps(RICH_DOC).encode(),
        "d/p1.png": png_bytes(2, 2, 10),
        "d/p2.png": png_bytes(3, 3, 20),
        "d/empty.json": b"",
        "d/obj.json": b"{}",
        "d/arr.json": b"[]",
        "d/num.json": b"3",
        "stray.png": png_bytes(1, 1),
        "notes.txt": b"not a document",
    }


RICH_MAPPING: Dict[str, Any] = {
    "version": 1,
    "document": {"format": "json", "match": ["**/*.json"], "max_bytes": 100_000},
    "tables": {
        "parcels": {
            "rows": "parcels[]",
            "columns": {
                "id": "id",
                "door": {"type": "int32", "from": "root", "expr": 'Depot."Dock door"'},
                "late": {"type": "int32", "from": "root", "expr": 'Depot."Count(late)"'},
                "dotted": {"from": "root", "expr": 'Depot."a.b"'},
                "qty": {"type": "int32", "expr": "qty"},
                "ratio": {"type": "float64", "expr": "ratio"},
                "tags": {"type": "list<string>", "expr": "tags", "split": ";"},
                "code": {"type": "int32", "expr": "code", "extract": "^ZX-(?P<value>[0-9]+)$"},
                "photo": {"type": "file", "expr": "photo", "resolve": [{"basename_in": "document_dir"}]},
            },
        },
        "scans": {
            "rows": [{"as": "p", "each": "parcels[]"}, {"as": "s", "each": "scans[]"}],
            "columns": {
                "pid": {"from": "p", "expr": "id"},
                "n": {"type": "int32", "expr": "n"},
                "ok": {"type": "bool", "expr": "ok"},
                "pos": {"index_of": "s"},
                "ppos": {"index_of": "p"},
            },
        },
        "grid": {"rows": "parcels[].grid[][]", "columns": {"v": {"type": "int32", "expr": "@"}}},
    },
}


# ------------------------------------------------------------------ bulk (benchmark)


def json_bulk_manifest(n_parcels: int, scans_per_parcel: int = 4) -> bytes:
    """The JSON twin of ``documents_synthetic.bulk_manifest``: same parcels, scans and flags."""
    parcels = []
    for p in range(n_parcels):
        pid = f"P{p}"
        scans = [
            _scan(
                pid, f"S{p}-{s}", f"2026-03-01T08:{s % 60:02d}:{p % 60:02d}.{p % 1000:03d}Z", str(s % 7),
                [("temp", f"{s}.5"), ("humidity", str(p % 90))],
                [{"kind": "temp", "low": str(s % 3)}],
                [(f"K{p}", "1")],
            )
            for s in range(scans_per_parcel)
        ]
        parcels.append({
            "id": pid, "weight": f"{p % 40}.25", "fragile": "true" if p % 2 else "false", "scans": scans,
            "flags": [_flag(pid, f"F{p}", f"C{p % 5}", [f"S{p}-0"])],
        })
    return manifest_json("BULK", parcels=parcels)


__all__ = [
    "JSON_MAPPING", "RICH_DOC", "RICH_MAPPING", "json_bulk_manifest", "json_bundle_files",
    "manifest_json", "rich_json_files",
]
