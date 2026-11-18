"""The synthetic "parcel depot" format: generator, mapping and expected rows.

This is the ONLY document format that appears anywhere in dag-tools -- code,
tests, fixtures and docs. It exists to exercise every mapping feature, and
every value in it is invented here.

Shape::

    Manifest(@id, @created)
      Depot(@site, @dock, @shift)
      Parcel*(@id, @weight, @fragile)
        Scan*(@id, @at, @lane)
          Reading{1,2}(@kind, @value)
          Limit{0,2}(@kind, @low | @high)     -- at most one per kind
          Item{0,2}(@sku, @qty)
        Flag*(@id, @code)
          ScanRef{1,3}(@id)
      Extras?
        Photos
          photo*(@parcel, @path)                -- backslash path
            ref*(@scan)
        Watch*(@parcel, @scan)
      Review*(@flagRef, @by)                   -- top level
        Decision(@code)

A scan id is unique only within its parcel: two parcels of one manifest
reuse ``S1``. A flag, a watch entry and a photo each belong to one parcel,
so every index over them is scoped by parcel, and the mapping's lookups say
which parcel they are asking about. ``test_documents_engine`` removes the
scopes and asserts the rows come back cross-linked.

Photos live in a folder named after the manifest file: flat
(``<stem>/<name>``) or in numbered subfolders (``<stem>/01/<name>``).
"""
from __future__ import annotations

import io
import struct
import zipfile
import zlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ------------------------------------------------------------------ mapping

MAPPING: Dict[str, Any] = {
    "version": 1,
    "document": {"format": "xml", "match": ["*.xml", "*/*.xml"], "max_bytes": 1_000_000},
    "lineage": {"publish_period": {"from": "bundle_name", "regex": r"(?P<period>\d{4}-\d{2})"}},
    "expected_paths": [
        "/Manifest", "/Manifest/@id", "/Manifest/@created",
        "/Manifest/Depot", "/Manifest/Depot/@site", "/Manifest/Depot/@dock", "/Manifest/Depot/@shift",
        "/Manifest/Parcel", "/Manifest/Parcel/@id", "/Manifest/Parcel/@weight", "/Manifest/Parcel/@fragile",
        "/Manifest/Parcel/Scan", "/Manifest/Parcel/Scan/@id", "/Manifest/Parcel/Scan/@at",
        "/Manifest/Parcel/Scan/@lane",
        "/Manifest/Parcel/Scan/Reading", "/Manifest/Parcel/Scan/Reading/@kind",
        "/Manifest/Parcel/Scan/Reading/@value",
        "/Manifest/Parcel/Scan/Limit", "/Manifest/Parcel/Scan/Limit/@kind",
        "/Manifest/Parcel/Scan/Limit/@low", "/Manifest/Parcel/Scan/Limit/@high",
        "/Manifest/Parcel/Scan/Item", "/Manifest/Parcel/Scan/Item/@sku", "/Manifest/Parcel/Scan/Item/@qty",
        "/Manifest/Parcel/Flag", "/Manifest/Parcel/Flag/@id", "/Manifest/Parcel/Flag/@code",
        "/Manifest/Parcel/Flag/ScanRef", "/Manifest/Parcel/Flag/ScanRef/@id",
        "/Manifest/Extras", "/Manifest/Extras/Photos", "/Manifest/Extras/Photos/photo",
        "/Manifest/Extras/Photos/photo/@parcel",
        "/Manifest/Extras/Photos/photo/@path", "/Manifest/Extras/Photos/photo/ref",
        "/Manifest/Extras/Photos/photo/ref/@scan",
        "/Manifest/Extras/Watch", "/Manifest/Extras/Watch/@parcel", "/Manifest/Extras/Watch/@scan",
        "/Manifest/Review", "/Manifest/Review/@flagRef", "/Manifest/Review/@by",
        "/Manifest/Review/Decision", "/Manifest/Review/Decision/@code",
    ],
    "on_unknown_path": "warn",
    "indexes": {
        "reviews": {
            "rows": "/Manifest/Review",
            "keys": "@flagRef",
            "values": {"decision": "Decision/@code", "by": "@by"},
        },
        "photo_refs": {
            "rows": "/Manifest/Extras/Photos/photo",
            "scope": "@parcel",
            "keys": "ref/@scan",
            "values": {"path": "@path"},
        },
        "watched": {
            "rows": "/Manifest/Extras/Watch",
            "scope": "@parcel",
            "keys": "@scan",
            "values": {"scan": "@scan"},
        },
        "flags_by_scan": {
            "rows": "/Manifest/Parcel/Flag",
            "scope": "../@id",
            "keys": "ScanRef/@id",
            "values": {"code": "@code"},
        },
    },
    "tables": {
        "loads": {
            "rows": "/Manifest",
            "row_id": "{manifest_id}",
            "columns": {
                "manifest_id": "@id",
                "created": {"type": "timestamp", "expr": "@created"},
                "site": "Depot/@site",
                "dock": "Depot/@dock",
                "shift": {"expr": "Depot/@shift", "map": {"values": {"early": "E", "late": "L"}, "default": "?"}},
                "parcel_count": {"type": "int32", "expr": "count(Parcel)"},
            },
        },
        "parcels": {
            "rows": "/Manifest/Parcel",
            "vars": {"manifest": "string(/Manifest/@id)"},
            "row_id": "{manifest_id}/{parcel_id}",
            "columns": {
                "manifest_id": "$manifest",
                "parcel_id": "@id",
                "weight": {"type": "float64", "expr": "@weight"},
                "fragile": {"type": "bool", "expr": "@fragile"},
                "flag_ids": {"type": "list<string>", "expr": "Flag/@id"},
                "scan_count": {"type": "int64", "expr": "count(Scan)"},
            },
        },
        "scans": {
            "rows": "/Manifest/Parcel/Scan",
            "row_id": "{doc_id}:{row_number}",
            "columns": {
                "scan_id": "@id",
                "parcel_id": "../@id",
                "at": {"type": "timestamp", "expr": "@at"},
                "lane": {"type": "int32", "expr": "@lane"},
                "readings": {
                    "type": "list<struct>",
                    "rows": "Reading",
                    "fields": {"kind": "@kind", "value": {"type": "float64", "expr": "@value"}},
                },
                "temp_low": {"type": "float64", "expr": "Limit[@kind='temp']/@low"},
                "temp_bound": {
                    "type": "float64",
                    "first_of": ["Limit[@kind='temp']/@low", "Limit[@kind='temp']/@high"],
                },
                "first_sku": {"expr": "Item/@sku", "when_empty": "none"},
                "item_skus": {"type": "list<string>", "expr": "Item/@sku"},
                "has_photo": {"lookup": {"index": "photo_refs", "scope": "../@id", "keys": "@id", "mode": "exists"}},
                "watched": {"lookup": {"index": "watched", "scope": "../@id", "keys": "@id", "mode": "exists"}},
                "is_flagged": {"lookup": {"index": "flags_by_scan", "scope": "../@id", "keys": "@id", "mode": "exists"}},
                "flag_count": {"lookup": {"index": "flags_by_scan", "scope": "../@id", "keys": "@id", "mode": "count"}},
                "first_flag": {
                    "lookup": {"index": "flags_by_scan", "scope": "../@id", "keys": "@id", "field": "code", "mode": "first"}
                },
                "last_flag": {
                    "lookup": {"index": "flags_by_scan", "scope": "../@id", "keys": "@id", "field": "code", "mode": "last"}
                },
                "flag_codes": {
                    "lookup": {"index": "flags_by_scan", "scope": "../@id", "keys": "@id", "field": "code", "mode": "list"}
                },
            },
        },
        "flags": {
            "rows": "/Manifest/Parcel/Flag",
            "columns": {
                "flag_id": "@id",
                "code": "@code",
                "scan_ids": {"type": "list<string>", "expr": "ScanRef/@id"},
                "review_count": {"lookup": {"index": "reviews", "keys": "@id", "mode": "count"}},
                "decision": {"lookup": {"index": "reviews", "keys": "@id", "field": "decision", "mode": "last"}},
                "decisions": {
                    "lookup": {"index": "reviews", "keys": "@id", "field": "decision", "mode": "list", "sort": True}
                },
                "verdict": {
                    "type": "string",
                    "lookup": {"index": "reviews", "keys": "@id", "field": "decision", "mode": "list"},
                    "prefer": ["REJECT", "REPAIR", "ACCEPT"],
                    "when_empty": "none",
                },
                "photo_count": {
                    "lookup": {"index": "photo_refs", "scope": "../@id", "keys": "ScanRef/@id", "mode": "count"}
                },
            },
        },
        "photos": {
            "rows": "/Manifest/Extras/Photos/photo",
            "columns": {
                "parcel_id": "@parcel",
                "scan_ids": {"type": "list<string>", "expr": "ref/@scan"},
                "image": {
                    "type": "file",
                    "expr": "@path",
                    "resolve": ["document_dir", {"basename_under": "{document_stem}"}],
                    "probe": "image",
                },
            },
        },
    },
}

TABLES = ["loads", "parcels", "scans", "flags", "photos"]


# ------------------------------------------------------------------ images


def png_bytes(width: int, height: int, shade: int = 128) -> bytes:
    """A grey PNG, built with the standard library so its bytes never depend
    on an imaging library's version -- the example ships these files and a
    test compares them byte for byte."""
    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    raw = b"".join(b"\x00" + bytes([shade]) * width for _ in range(height))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, 9))
        + chunk(b"IEND", b"")
    )


# ------------------------------------------------------------------ documents


def manifest_xml(
    manifest_id: str,
    *,
    parcels: List[str],
    reviews: str = "",
    extras: Optional[str] = None,
    created: str = "2026-03-01T08:00:00.5Z",
    depot: str = '<Depot site="north" dock="D4" shift="early"/>',
) -> bytes:
    body = "".join(parcels)
    ext = extras if extras is not None else ""
    return (
        f'<?xml version="1.0" encoding="utf-8"?>\n'
        f'<Manifest id="{manifest_id}" created="{created}">{depot}{body}{ext}{reviews}</Manifest>'
    ).encode()


def _scan(sid: str, at: str, lane: str, readings: str, limits: str = "", items: str = "") -> str:
    return f'<Scan id="{sid}" at="{at}" lane="{lane}">{readings}{limits}{items}</Scan>'


def bundle_files(variant: str = "flat") -> Dict[str, bytes]:
    """One bundle: two manifests, one malformed document, photos.

    ``variant`` is ``flat`` (``<stem>/<name>``) or ``numbered``
    (``<stem>/01/<name>``) for the photo layout.
    """
    def photo_path(stem: str, name: str, sub: str = "01") -> str:
        return f"{stem}/{name}" if variant == "flat" else f"{stem}/{sub}/{name}"

    m1 = manifest_xml(
        "M1",
        parcels=[
            '<Parcel id="P1" weight="2.5" fragile="true">'
            + _scan(
                "S1", "2026-03-01T08:01:02.1Z", "1",
                '<Reading kind="temp" value="4.5"/><Reading kind="humidity" value="40"/>',
                '<Limit kind="temp" low="2"/><Limit kind="humidity" high="80"/>',
                '<Item sku="A1" qty="2"/><Item sku="B7" qty="1"/>',
            )
            + _scan("S2", "2026-03-01T08:05:00.123+02:00", "2", '<Reading kind="temp" value="5.25"/>',
                    '<Limit kind="temp" high="9"/>')
            + '<Flag id="F1" code="DENT"><ScanRef id="S1"/><ScanRef id="S2"/></Flag>'
            + '<Flag id="F2" code="WET"><ScanRef id="S2"/></Flag>'
            + "</Parcel>",
            '<Parcel id="P2" weight="heavy" fragile="false">'
            + _scan("S3", "2026-03-01T09:00:00.123456789Z", "x", '<Reading kind="temp" value="NaN"/>')
            # S1 again: a scan id is unique only within its parcel
            + _scan("S1", "2026-03-01T09:10:00Z", "4", '<Reading kind="temp" value="6"/>')
            + '<Flag id="F4" code="LOST"><ScanRef id="S1"/></Flag>'
            + "</Parcel>",
        ],
        extras=(
            "<Extras><Photos>"
            '<photo parcel="P1" path="capture\\S1.png"><ref scan="S1"/></photo>'
            '<photo parcel="P1" path="capture\\S2.png"><ref scan="S2"/><ref scan="S1"/></photo>'
            '<photo parcel="P2" path="capture\\gone.png"><ref scan="S3"/></photo>'
            "</Photos>"
            '<Watch parcel="P1" scan="S2"/>'
            '<Watch parcel="P2" scan="S1"/>'
            "</Extras>"
        ),
        reviews=(
            '<Review flagRef="F1" by="ops"><Decision code="REPAIR"/></Review>'
            '<Review flagRef="F1" by="lead"><Decision code="ACCEPT"/></Review>'
            '<Review flagRef="F4" by="ops"><Decision code="HOLD"/></Review>'
        ),
    )
    m2 = manifest_xml(
        "M2",
        parcels=[
            '<Parcel id="P3" weight="1" fragile="1">'
            + _scan("S4", "2026-03-02T10:00:00Z", "3", '<Reading kind="temp" value="3"/>')
            + '<Flag id="F3" code="LATE"><ScanRef id="S4"/></Flag>'
            + "</Parcel>",
        ],
        created="2026-03-02T10:00:00.000000001Z",
        depot='<Depot site="south" dock="D1" shift="night"/>',
    )
    files: Dict[str, bytes] = {
        "manifest_m1.xml": m1,
        "manifest_m2.xml": m2,
        "broken.xml": b"<Manifest id='X'><Parcel></Manifest>",
        photo_path("manifest_m1", "S1.png"): png_bytes(4, 3),
        photo_path("manifest_m1", "S2.png"): png_bytes(5, 2),
        "readme.txt": b"not a document",
    }
    if variant == "numbered":
        # a second S2.png in another numbered subfolder: ambiguous by name
        files[photo_path("manifest_m1", "S2.png", sub="02")] = png_bytes(6, 6)
    return files


def write_tree(root: Path, files: Dict[str, bytes]) -> None:
    for rel, data in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)


def zip_bytes(files: Dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for rel, data in sorted(files.items()):
            z.writestr(rel, data)
    return buf.getvalue()


def write_zip(path: Path, files: Dict[str, bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(zip_bytes(files))


def write_nested_zip(path: Path, files: Dict[str, bytes], inner_name: str = "inner.zip") -> None:
    """A zip whose only member is another zip holding ``files``."""
    write_zip(path, {inner_name: zip_bytes(files)})


# ------------------------------------------------------------------ expected rows

EXPECTED_ROWS = {"loads": 2, "parcels": 3, "scans": 5, "flags": 4, "photos": 3, "_rejects": 1}


def expected_photo_resolution(variant: str) -> Dict[str, int]:
    if variant == "flat":
        return {"basename_under": 2, "missing": 1}
    return {"basename_under": 1, "ambiguous": 1, "missing": 1}


# ------------------------------------------------------------------ bulk (benchmark)


def bulk_manifest(n_parcels: int, scans_per_parcel: int = 4) -> bytes:
    parcels = []
    for p in range(n_parcels):
        scans = "".join(
            _scan(
                f"S{p}-{s}", f"2026-03-01T08:{s % 60:02d}:{p % 60:02d}.{p % 1000:03d}Z", str(s % 7),
                f'<Reading kind="temp" value="{s}.5"/><Reading kind="humidity" value="{p % 90}"/>',
                f'<Limit kind="temp" low="{s % 3}"/>',
                f'<Item sku="K{p}" qty="1"/>',
            )
            for s in range(scans_per_parcel)
        )
        parcels.append(f'<Parcel id="P{p}" weight="{p % 40}.25" fragile="{"true" if p % 2 else "false"}">{scans}'
                       f'<Flag id="F{p}" code="C{p % 5}"><ScanRef id="S{p}-0"/></Flag></Parcel>')
    return manifest_xml("BULK", parcels=parcels)


# ------------------------------------------------------------------ parser limits


def relay_manifest(legs: int, *, last_stop: str = "dock") -> bytes:
    """A manifest whose parcel travelled a relay of ``legs`` nested hand-offs.

    ``Extras/Relay/Leg/Leg/...``: each leg is handed off inside the one before
    it, so the document nests ``legs`` + 3 deep. libxml2 refuses more than 256
    by default; this is the generated document the default parser rejects for
    a limit and a ``huge_tree`` parser reads. ``last_stop`` lands on the
    innermost leg, so a test can plant a value and look for it in an error.
    """
    chain = "".join(f'<Leg stop="L{i}">' for i in range(legs - 1)) + f'<Leg stop="{last_stop}"/>' + "</Leg>" * (legs - 1)
    parcel = '<Parcel id="P1" weight="1.5" fragile="false">' + _scan("S1", "2026-03-01T09:00:00Z", "1", "") + "</Parcel>"
    return manifest_xml("MR", parcels=[parcel], extras=f"<Extras><Relay>{chain}</Relay></Extras>")


def large_manifest(min_bytes: int) -> Tuple[bytes, int]:
    """A well-formed manifest of at least ``min_bytes``, and its element count.

    Shallow and wide: many small parcels, each a scan and a reading, which is
    the shape a large real document usually has. It names nothing a parser
    limit is about, so it measures whether the default parser accepts size
    and element count alone.
    """
    parcels: List[str] = []
    size = 0
    p = 0
    while size < min_bytes:
        reading = f'<Reading kind="temp" value="{p % 30}"/>'
        x = (f'<Parcel id="P{p}" weight="{p % 40}.25" fragile="false">'
             + _scan(f"S{p}", "2026-03-01T09:00:00Z", str(p % 7), reading)
             + "</Parcel>")
        parcels.append(x)
        size += len(x)
        p += 1
    doc = manifest_xml("ML", parcels=parcels)
    # Manifest + Depot + three elements per parcel.
    return doc, 2 + 3 * p


__all__ = [
    "EXPECTED_ROWS", "MAPPING", "TABLES", "bulk_manifest", "bundle_files", "expected_photo_resolution",
    "large_manifest", "manifest_xml", "png_bytes", "relay_manifest", "write_nested_zip", "write_tree",
    "write_zip", "zip_bytes",
]

