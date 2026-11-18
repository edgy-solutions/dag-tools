"""Characterise document bundles (XML/JSON documents plus images) before designing
ingestion into tables.

Ported from a standalone profiler. Bundles come from
:mod:`dag_tools.documents.sources` (zips and directory trees, local or S3), which
replaces the profiler's own source code, including nested-zip expansion.

Resilience:

* every finished bundle is checkpointed under ``<out>/checkpoint`` (rows appended
  to ``*.jsonl``, aggregate state written as JSON, atomically); running again with
  the same ``out`` resumes and skips bundles already recorded;
* the first SIGINT/SIGTERM stops at the next member boundary, records the current
  bundle as partial and writes the report; a second one exits with status 130;
* ``report_only`` rebuilds the report from the checkpoint without reading a bundle;
* ``max_bundle_bytes`` skips (and records) oversized bundles, ``max_docs`` /
  ``max_images`` sample evenly inside a bundle.

Outputs in ``out``: summary.md, members.csv, path_stats.csv, docs.csv, refs.csv,
images.csv, bundles.csv, shapes.json, errors.csv, ``*.parquet`` (when pyarrow is
installed) and ``paths/<family>.txt``: one file per document family listing that
family's structural paths, sorted, one per line, in the ``expected_paths`` format
of the mapping (``/Root/Child/@attr``, local names only).

Logging names bundles, members, counts and paths only; a document value is never
logged. The report files may carry example values, which is the point of a
profile; ``redact`` replaces them with hashes.

Optional dependencies (lxml, Pillow, pyarrow) are imported where used.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import re
import shutil
import signal
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from dag_tools.documents.sources import (
    BundleRef,
    Member,
    OpenBundle,
    normalise_member_path,
    open_source,
)

logger = logging.getLogger(__name__)

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".gif", ".webp"}
XML_EXTS = {".xml"}
JSON_EXTS = {".json", ".jsonl", ".ndjson"}
JSONL_EXTS = {".jsonl", ".ndjson"}
ZIP_EXTS = {".zip"}
EXT_RE = re.compile(r"^\.[a-z0-9]{1,5}$")
DOC_EXTS = XML_EXTS | JSON_EXTS
HEADER_BYTES = 64 * 1024
STATE_VERSION = 1

# ------------------------------------------------------------------ interrupts

STOP = False


class Interrupted(Exception):
    """A stop was requested; the current bundle is kept as partial."""


def request_stop() -> None:
    """Ask the running profile to finish the current bundle and stop."""
    global STOP
    STOP = True


def _on_signal(signum: int, _frame: Any) -> None:
    global STOP
    if STOP:  # second signal: get out now
        logger.error("second interrupt: exiting without report")
        raise SystemExit(130)
    STOP = True
    logger.warning("signal %s: finishing current member, then writing report "
                   "(interrupt again to abort)", signum)


def check_stop() -> None:
    if STOP:
        raise Interrupted()


# ------------------------------------------------------------------ inference

ISO_DT = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$")
ISO_D = re.compile(r"^\d{4}-\d{2}-\d{2}$")
US_DT = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}[ T]\d{1,2}:\d{2}(:\d{2})?( ?[AP]M)?$", re.IGNORECASE)
COMPACT_DT = re.compile(r"^\d{8}[T_-]?\d{6}(\d{3})?$")
EPOCH = re.compile(r"^1[5-9]\d{8}(\d{3})?$")
INT = re.compile(r"^[+-]?\d+$")
FLOAT = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")
BOOL = {"true", "false", "yes", "no", "y", "n", "pass", "fail", "ok", "ng"}


def infer_type(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return ("epoch_like" if 1_500_000_000 <= v < 2_000_000_000
                or 1_500_000_000_000 <= v < 2_000_000_000_000 else "int")
    if isinstance(v, float):
        return "float"
    s = str(v).strip()
    if s == "":
        return "empty"
    if ISO_DT.match(s):
        return "datetime_iso"
    if ISO_D.match(s):
        return "date_iso"
    if US_DT.match(s):
        return "datetime_us"
    if COMPACT_DT.match(s):
        return "datetime_compact"
    if EPOCH.match(s):
        return "epoch_like"
    if INT.match(s):
        return "str_int"
    if FLOAT.match(s):
        return "str_float"
    if s.lower() in BOOL:
        return "bool_like"
    if PurePosixPath(s.replace("\\", "/")).suffix.lower() in IMAGE_EXTS:
        return "image_ref"
    if len(s) > 80 or "\n" in s:
        return "text_long"
    return "string"


def pattern_template(v: str, max_len: int = 40) -> str:
    s = str(v).strip()[:max_len]
    t = re.sub(r"[A-Za-z]", "A", s)
    t = re.sub(r"\d", "9", t)
    t = re.sub(r"A{3,}", "A+", t)
    t = re.sub(r"9{3,}", "9+", t)
    return t


def redact_value(v: str, redact: bool) -> str:
    v = str(v)
    if not redact:
        return v if len(v) <= 80 else v[:77] + "..."
    return "sha1:" + hashlib.sha1(v.encode("utf-8", "replace")).hexdigest()[:12]


def sample_evenly(items: list, n: int) -> list:
    if n <= 0 or len(items) <= n:
        return items
    step = len(items) / n
    return [items[int(i * step)] for i in range(n)]


# ------------------------------------------------------------------ stats


@dataclass
class PathStats:
    files_seen: int = 0
    occurrences: int = 0
    min_per_parent: Optional[int] = None
    max_per_parent: int = 0
    types: Counter = field(default_factory=Counter)
    templates: Counter = field(default_factory=Counter)
    empty: int = 0
    distinct_hashes: set = field(default_factory=set)
    examples: list = field(default_factory=list)
    container: bool = False
    namespaces: Counter = field(default_factory=Counter)

    def add_value(self, v: Any, max_examples: int, redact: bool) -> None:
        t = infer_type(v)
        self.types[t] += 1
        if t in ("empty", "null"):
            self.empty += 1
            return
        s = str(v)
        self.templates[pattern_template(s)] += 1
        if len(self.distinct_hashes) < 100_000:
            self.distinct_hashes.add(hashlib.sha1(s.strip().encode("utf-8", "replace")).digest()[:8])
        if len(self.examples) < max_examples:
            r = redact_value(s.strip(), redact)
            if r not in self.examples:
                self.examples.append(r)

    def card(self, n: int) -> None:
        self.min_per_parent = n if self.min_per_parent is None else min(self.min_per_parent, n)
        self.max_per_parent = max(self.max_per_parent, n)

    def to_json(self) -> Dict[str, Any]:
        return {
            "files_seen": self.files_seen, "occurrences": self.occurrences,
            "min_per_parent": self.min_per_parent, "max_per_parent": self.max_per_parent,
            "types": dict(self.types), "templates": dict(self.templates), "empty": self.empty,
            "distinct_hashes": sorted(h.hex() for h in self.distinct_hashes),
            "examples": list(self.examples), "container": self.container,
            "namespaces": dict(self.namespaces),
        }

    @classmethod
    def from_json(cls, d: Dict[str, Any]) -> "PathStats":
        return cls(
            files_seen=d["files_seen"], occurrences=d["occurrences"],
            min_per_parent=d["min_per_parent"], max_per_parent=d["max_per_parent"],
            types=Counter(d["types"]), templates=Counter(d["templates"]), empty=d["empty"],
            distinct_hashes={bytes.fromhex(h) for h in d["distinct_hashes"]},
            examples=list(d["examples"]), container=d["container"],
            namespaces=Counter(d["namespaces"]),
        )


def local_name(tag: Any) -> Tuple[str, str]:
    if not isinstance(tag, str):
        return "", ""
    if tag.startswith("{"):
        ns, name = tag[1:].split("}", 1)
        return ns, name
    return "", tag


@dataclass
class DocWalk:
    paths: set = field(default_factory=set)
    counts: Counter = field(default_factory=Counter)
    image_refs: list = field(default_factory=list)
    root: str = ""


# ------------------------------------------------------------------ checkpoint

ROW_TABLES = ("docs", "refs", "images", "members", "bundles")


class ProfileError(Exception):
    """The checkpoint or the request cannot be used."""


class Checkpoint:
    def __init__(self, out: Path):
        self.dir = out / "checkpoint"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.done: Dict[str, str] = {}
        f = self.dir / "done.jsonl"
        if f.exists():
            for ln in f.read_text(encoding="utf-8").splitlines():
                if ln.strip():
                    try:
                        d = json.loads(ln)
                    except json.JSONDecodeError:
                        break
                    self.done[d["bundle"]] = d["status"]

    def load_state(self) -> Optional[Dict[str, Any]]:
        f = self.dir / "state.json"
        if not f.exists():
            return None
        try:
            st = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            raise ProfileError(f"checkpoint state {f} is not readable JSON ({type(e).__name__}); "
                               "use fresh to start over") from e
        if not isinstance(st, dict) or st.get("version") != STATE_VERSION:
            raise ProfileError(f"checkpoint state {f} has an unknown version; use fresh to start over")
        return st

    def save_state(self, state: Dict[str, Any]) -> None:
        tmp = self.dir / "state.json.tmp"
        tmp.write_text(json.dumps(state), encoding="utf-8")
        os.replace(tmp, self.dir / "state.json")

    def append(self, table: str, rows: List[dict]) -> None:
        if not rows:
            return
        with (self.dir / f"{table}.jsonl").open("a", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, default=str) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def mark(self, bundle: str, status: str, **extra: Any) -> None:
        self.done[bundle] = status
        with (self.dir / "done.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"bundle": bundle, "status": status,
                                 "at": datetime.now().isoformat(), **extra}) + "\n")

    def rows(self, table: str) -> List[dict]:
        """Tolerates a truncated last line."""
        f = self.dir / f"{table}.jsonl"
        if not f.exists():
            return []
        out = []
        for ln in f.read_text(encoding="utf-8").splitlines():
            if not ln.strip():
                continue
            try:
                out.append(json.loads(ln))
            except json.JSONDecodeError:
                break
        return out


# ------------------------------------------------------------------ profiler


class Profiler:
    def __init__(self, max_examples: int, redact: bool, max_docs: int, max_images: int,
                 heartbeat: float):
        self.max_examples, self.redact = max_examples, redact
        self.max_docs, self.max_images, self.heartbeat = max_docs, max_images, heartbeat
        self.paths: Dict[str, PathStats] = defaultdict(PathStats)
        self.docs_by_kind: Counter = Counter()
        self.shapes: Dict[str, dict] = {}
        self.dir_templates: Counter = Counter()
        self.assoc: Counter = Counter()
        self.errors: List[dict] = []
        self.buf: Dict[str, List[dict]] = {t: [] for t in ROW_TABLES}
        self.path_listings: List[Tuple[str, str, int]] = []

    def state(self) -> Dict[str, Any]:
        return {
            "version": STATE_VERSION,
            "paths": {p: s.to_json() for p, s in self.paths.items()},
            "docs_by_kind": dict(self.docs_by_kind), "shapes": self.shapes,
            "dir_templates": dict(self.dir_templates), "assoc": dict(self.assoc),
            "errors": self.errors,
        }

    def load(self, st: Dict[str, Any]) -> None:
        self.paths = defaultdict(PathStats, {p: PathStats.from_json(d) for p, d in st["paths"].items()})
        self.docs_by_kind = Counter(st["docs_by_kind"])
        self.shapes = st["shapes"]
        self.dir_templates, self.assoc = Counter(st["dir_templates"]), Counter(st["assoc"])
        self.errors = st["errors"]

    def reset_buffers(self) -> None:
        self.buf = {t: [] for t in ROW_TABLES}

    # ---------------------------------------------------------------- XML walk

    def walk_xml(self, data: bytes, w: DocWalk, fam: str) -> None:
        from lxml import etree

        parser = etree.XMLParser(recover=False, huge_tree=True, remove_comments=True,
                                 resolve_entities=False, no_network=True)
        root = etree.fromstring(data, parser=parser)

        def walk(el: Any, parent: str) -> None:
            ns, name = local_name(el.tag)
            if not name:
                return
            path = f"{fam}:{parent}/{name}"
            w.paths.add(path)
            w.counts[path] += 1
            st = self.paths[path]
            if ns:
                st.namespaces[ns] += 1
            for aname, aval in el.attrib.items():
                apath = f"{path}/@{local_name(aname)[1]}"
                w.paths.add(apath)
                w.counts[apath] += 1
                self.paths[apath].add_value(aval, self.max_examples, self.redact)
                if infer_type(aval) == "image_ref":
                    w.image_refs.append((apath, aval.strip()))
            children = [c for c in el if isinstance(c.tag, str)]
            if children:
                st.container = True
                for cname, n in Counter(local_name(c.tag)[1] for c in children).items():
                    self.paths[f"{path}/{cname}"].card(n)
                for c in children:
                    walk(c, f"{parent}/{name}")
            else:
                text = el.text or ""
                if not text.strip() and el.attrib:
                    st.container = True
                else:
                    st.add_value(text, self.max_examples, self.redact)
                    if infer_type(text) == "image_ref":
                        w.image_refs.append((path, text.strip()))

        w.root = local_name(root.tag)[1]
        walk(root, "")
        for p in w.paths:
            if "/@" in p or p.split(":", 1)[1].count("/") == 1:
                self.paths[p].card(w.counts[p])

    # ---------------------------------------------------------------- JSON walk

    def walk_json(self, data: bytes, w: DocWalk, member_path: str, fam: str) -> None:
        text = data.decode("utf-8-sig", "replace")
        docs: list = []
        is_lines = PurePosixPath(member_path).suffix.lower() in JSONL_EXTS
        if not is_lines:
            try:
                docs = [json.loads(text)]
            except json.JSONDecodeError:
                is_lines = True
        if is_lines:
            docs = [json.loads(ln) for ln in text.splitlines() if ln.strip()]
            w.root = f"jsonl[{len(docs)}]"
        else:
            w.root = "object" if isinstance(docs[0], dict) else type(docs[0]).__name__

        def walk(v: Any, path: str) -> None:
            w.paths.add(path)
            w.counts[path] += 1
            st = self.paths[path]
            if isinstance(v, dict):
                st.container = True
                for k, val in v.items():
                    self.paths[f"{path}.{k}"].card(1)
                    walk(val, f"{path}.{k}")
            elif isinstance(v, list):
                st.container = True
                ip = f"{path}[]"
                self.paths[ip].card(len(v))
                if not v:
                    w.paths.add(ip)
                for item in v:
                    walk(item, ip)
            else:
                st.add_value(v, self.max_examples, self.redact)
                if isinstance(v, str) and infer_type(v) == "image_ref":
                    w.image_refs.append((path, v.strip()))

        for d in docs:
            walk(d, f"{fam}:$")

    # ---------------------------------------------------------------- documents

    @staticmethod
    def family(m: Member) -> str:
        kind = "xml" if m.ext in XML_EXTS else ("jsonl" if m.ext in JSONL_EXTS else "json")
        return f"{kind}[{pattern_template(m.stem)}]"

    def profile_doc(self, b: BundleRef, m: Member, by_path: Dict[str, Member]) -> Optional[dict]:
        kind = self.family(m)
        w = DocWalk()
        try:
            data = m.read(None)
            if kind.startswith("xml"):
                self.walk_xml(data, w, kind)
            else:
                self.walk_json(data, w, m.path, kind)
        except Exception as e:  # noqa: BLE001 -- one unparseable document is recorded, not fatal
            # A parser's message can quote the bytes it choked on; under
            # redact only the type and a digest of the message are kept.
            detail = f"{type(e).__name__} {redact_value(str(e), True)}" if self.redact else str(e)
            self.errors.append({"bundle": b.key, "member": m.path, "error": f"{kind} parse: {detail}"})
            logger.warning("document did not parse: bundle=%s member=%s family=%s error=%s",
                           b.key, m.path, kind, type(e).__name__)
            return None

        for p in w.paths:
            self.paths[p].files_seen += 1
        for p, n in w.counts.items():
            self.paths[p].occurrences += n
        self.docs_by_kind[kind] += 1

        shape_id = hashlib.sha1("\n".join(sorted(w.paths)).encode()).hexdigest()[:10]
        sh = self.shapes.setdefault(shape_id, {
            "shape_id": shape_id, "kind": kind, "files": 0, "n_paths": len(w.paths),
            "first_bundle": b.key, "last_bundle": b.key,
            "first_modified": b.modified, "last_modified": b.modified, "paths": sorted(w.paths)})
        sh["files"] += 1
        sh["last_bundle"], sh["last_modified"] = b.key, b.modified

        resolved = []
        for ref_path, ref in w.image_refs:
            hit, match = self.resolve_ref(m, ref, by_path)
            self.buf["refs"].append({"bundle": b.key, "doc": m.path, "ref_path": ref_path,
                                     "ref": redact_value(ref, self.redact),
                                     "ref_template": pattern_template(ref),
                                     "exists": int(hit is not None), "match": match,
                                     "resolved_member": hit.path if hit else None})
            if hit:
                resolved.append(hit.path)

        doc = {"bundle": b.key, "member": m.path, "kind": kind, "bytes": m.size, "root": w.root,
               "shape_id": shape_id,
               "n_nodes": sum(n for p, n in w.counts.items() if "/@" not in p),
               "image_refs": len(w.image_refs), "image_refs_resolved": len(resolved)}
        self.buf["docs"].append(doc)
        return {"resolved": resolved}

    @staticmethod
    def resolve_ref(doc: Member, ref: str,
                    by_path: Dict[str, Member]) -> Tuple[Optional[Member], Optional[str]]:
        norm = normalise_member_path(ref)
        base = PurePosixPath(norm).name
        prefix = f"{doc.dir}/" if doc.dir else ""
        # Candidates: as written, beside the document, inside a folder named after the
        # document's stem, and by bare name.
        cands = [norm, f"{prefix}{norm}", f"{prefix}{doc.stem}/{norm}", f"{prefix}{base}"]
        for c in cands:
            if c in by_path:
                return by_path[c], "path"
        by_base = [mm for p, mm in by_path.items() if PurePosixPath(p).name == base]
        if len(by_base) == 1:
            return by_base[0], "basename"
        if len(by_base) > 1:
            return by_base[0], f"basename_ambiguous({len(by_base)})"
        return None, None

    # ---------------------------------------------------------------- images

    def image_header(self, m: Member) -> dict:
        rec = {"format": None, "width": None, "height": None, "mode": None}
        try:
            from PIL import Image
        except ImportError:
            return rec
        for n in (HEADER_BYTES if m.size > HEADER_BYTES else None, None):
            try:
                im = Image.open(io.BytesIO(m.read(n)))
                rec.update(format=im.format, mode=im.mode, width=im.size[0], height=im.size[1])
                return rec
            except Exception as e:  # noqa: BLE001 -- Pillow raises many types; all mean unreadable
                rec["format"] = f"unreadable: {type(e).__name__}"
                if n is None:
                    break
        return rec

    # ---------------------------------------------------------------- bundle

    def profile_bundle(self, b: BundleRef, ob: OpenBundle) -> dict:
        """Returns the bundle's stats row; raises Interrupted if a stop was requested."""
        members = ob.members
        t0 = time.time()
        last_beat = t0
        by_path = {m.path: m for m in members}
        docs = [m for m in members if m.ext in DOC_EXTS]
        imgs = [m for m in members if m.ext in IMAGE_EXTS]
        nested = [m for m in members if m.ext in ZIP_EXTS]
        other = [m for m in members
                 if m.ext not in DOC_EXTS and m.ext not in IMAGE_EXTS and m.ext not in ZIP_EXTS]
        for zp in ob.unexpanded_zips:
            self.errors.append({"bundle": b.key, "member": zp, "error": "nested zip not expanded"})

        member_rows: Dict[str, dict] = {}
        for m in members:
            self.dir_templates[pattern_template(m.dir) if m.dir else "(root)"] += 1
            role = ("doc" if m.ext in DOC_EXTS else "image" if m.ext in IMAGE_EXTS
                    else "zip" if m.ext in ZIP_EXTS else "other")
            member_rows[m.path] = {"bundle": b.key, "member": m.path, "dir": m.dir,
                                   "ext": m.ext or "(none)",
                                   "name_template": pattern_template(m.name),
                                   "role": role, "bytes": m.size, "modified": m.modified,
                                   "association": None, "doc": None}
        self.buf["members"].extend(member_rows.values())   # inventory survives an interrupt

        doc_sample = sample_evenly(docs, self.max_docs)
        img_sample = sample_evenly(imgs, self.max_images)
        img_sampled = {m.path for m in img_sample}
        progress = {"docs_done": 0, "images_done": 0,
                    "docs_total": len(doc_sample), "images_total": len(img_sample)}

        def beat(stage: str) -> None:
            nonlocal last_beat
            if self.heartbeat and time.time() - last_beat >= self.heartbeat:
                last_beat = time.time()
                logger.info("    ... %s: docs %d/%d, images %d/%d, %.0fs", stage,
                            progress["docs_done"], progress["docs_total"],
                            progress["images_done"], progress["images_total"], last_beat - t0)

        referenced_by: Dict[str, str] = {}
        for m in doc_sample:
            check_stop()
            d = self.profile_doc(b, m, by_path)
            progress["docs_done"] += 1
            if d:
                for p in d["resolved"]:
                    referenced_by.setdefault(p, m.path)
            beat("docs")

        docs_by_dir: Dict[str, List[Member]] = defaultdict(list)
        for m in docs:
            docs_by_dir[m.dir].append(m)

        assoc_of: Dict[str, Tuple[str, Optional[str]]] = {}
        for im in imgs:
            if im.path in referenced_by:
                assoc_of[im.path] = ("content_ref", referenced_by[im.path])
                continue
            sibs = docs_by_dir.get(im.dir, [])
            same = [d for d in sibs if d.stem == im.stem]
            if same:
                assoc_of[im.path] = ("sibling_stem", same[0].path)
                continue
            pref = sorted([d for d in sibs if d.stem and im.stem.startswith(d.stem)],
                          key=lambda d: -len(d.stem))
            if pref:
                assoc_of[im.path] = ("sibling_prefix", pref[0].path)
                continue
            primary = [d for d in sibs if d.ext not in JSONL_EXTS]
            if len(sibs) == 1 or len(primary) == 1:
                assoc_of[im.path] = ("dir_sidecar", (primary or sibs)[0].path)
                continue
            assoc_of[im.path] = ("orphan", None)

        for p, (a, d) in assoc_of.items():
            member_rows[p]["association"], member_rows[p]["doc"] = a, d
        for im in imgs:
            a, d = assoc_of[im.path]
            self.assoc[a] += 1
            if im.path not in img_sampled:
                continue
            check_stop()
            rec = {"bundle": b.key, "member": im.path, "dir": im.dir, "ext": im.ext,
                   "bytes": im.size, "association": a, "doc": d,
                   "stem_template": pattern_template(im.stem)}
            rec.update(self.image_header(im))
            self.buf["images"].append(rec)
            progress["images_done"] += 1
            beat("images")

        row = {
            "bundle": b.key, "modified": ob.modified or b.modified,
            "bundle_bytes": ob.size if ob.size is not None else b.size, "status": "done",
            "members": len(members), "docs": len(docs), "docs_profiled": len(doc_sample),
            "xml": sum(m.ext in XML_EXTS for m in docs), "json": sum(m.ext == ".json" for m in docs),
            "jsonl": sum(m.ext in JSONL_EXTS for m in docs),
            "images": len(imgs), "images_profiled": len(img_sample),
            "image_bytes": sum(m.size for m in imgs),
            "nested_zips": len(nested), "other": len(other),
            "other_exts": ",".join(sorted({m.ext for m in other})),
            "orphan_images": sum(1 for im in imgs if assoc_of[im.path][0] == "orphan"),
            "dirs": len({m.dir for m in members}), "seconds": round(time.time() - t0, 1),
        }
        self.buf["bundles"].append(row)
        return row

    # ---------------------------------------------------------------- report

    def path_rows(self) -> List[dict]:
        rows = []
        for p, st in sorted(self.paths.items()):
            kind = p.split(":", 1)[0]
            n_files = self.docs_by_kind[kind] or 1
            tot = sum(st.types.values())
            rows.append({
                "path": p, "doc_kind": kind,
                "node": "attribute" if "/@" in p else ("container" if st.container else "leaf"),
                "files_seen": st.files_seen, "file_pct": round(100 * st.files_seen / n_files, 1),
                "occurrences": st.occurrences,
                "min_per_parent": st.min_per_parent if st.min_per_parent is not None else 0,
                "max_per_parent": st.max_per_parent,
                "optional": st.files_seen < n_files, "repeating": st.max_per_parent > 1,
                "dominant_type": st.types.most_common(1)[0][0] if st.types else "",
                "types": json.dumps(dict(st.types.most_common())),
                "empty_pct": round(100 * st.empty / tot, 1) if tot else 0.0,
                "distinct_values": len(st.distinct_hashes), "n_templates": len(st.templates),
                "top_templates": json.dumps(st.templates.most_common(5)),
                "examples": json.dumps(st.examples), "namespaces": ";".join(st.namespaces),
            })
        return rows

    def write_path_files(self, out: Path, scrub: Optional["Scrubber"] = None) -> None:
        """One ``paths/<family>.txt`` per document family: sorted structural paths."""
        pdir = out / "paths"
        shutil.rmtree(pdir, ignore_errors=True)
        pdir.mkdir(parents=True, exist_ok=True)
        used: Dict[str, str] = {}
        listing: List[Tuple[str, str, int]] = []
        for fam in sorted(self.docs_by_kind):
            prefix = f"{fam}:"
            lines = sorted(p[len(prefix):] for p, st in self.paths.items()
                           if p.startswith(prefix) and st.files_seen > 0)
            if scrub:
                lines = [scrub.text(ln) for ln in lines]
            fname = safe_name(fam)
            if fname in used and used[fname] != fam:
                fname = f"{fname}-{hashlib.sha1(fam.encode()).hexdigest()[:6]}"
            used[fname] = fam
            (pdir / f"{fname}.txt").write_text("".join(ln + "\n" for ln in lines), encoding="utf-8")
            listing.append((fam, f"paths/{fname}.txt", len(lines)))
        self.path_listings = listing

    def summary_md(self, rows: List[dict], T: Dict[str, List[dict]], done: Dict[str, str]) -> str:
        L: List[str] = ["# Bundle profile", ""]
        bundles, members, images, refs = T["bundles"], T["members"], T["images"], T["refs"]
        statuses = Counter(done.values())
        nref = len(refs)
        nres = sum(r["exists"] for r in refs)
        L += [f"- generated: {datetime.now().isoformat(timespec='seconds')}",
              f"- bundles: {len(bundles)} seen; status {dict(statuses)}",
              f"- members: {len(members)}",
              f"- documents by family (kind[filename template]): {dict(self.docs_by_kind)}",
              (f"- images: {sum(self.assoc.values())}  association: {dict(self.assoc)}  "
               f"(headers read for {len(images)})"),
              f"- image references in documents: {nref} ({nres} resolved, {nref - nres} dangling)",
              f"- distinct document paths: {len(self.paths)}",
              f"- distinct structural shapes: {len(self.shapes)}",
              f"- errors: {len(self.errors)}", ""]

        if self.path_listings:
            L += ["## Structural path listings (per family, mapping `expected_paths` format)", ""]
            for fam, rel, n in self.path_listings:
                L.append(f"- `{fam}`: `{rel}` ({n} paths)")
            L.append("")

        skipped = [b for b, s in done.items() if s != "done"]
        if skipped:
            L += ["## Bundles not fully profiled", ""]
            for b in skipped[:50]:
                L.append(f"- {done[b]}: {b}")
            L.append("")

        if bundles:
            slow = sorted(bundles, key=lambda r: -(r.get("seconds") or 0))[:5]
            big = sorted(bundles, key=lambda r: -(r.get("bundle_bytes") or 0))[:5]
            seen = set()
            L += ["## Largest / slowest bundles", "",
                  "| bundle | status | MB | members | images | seconds |", "|---|---|---|---|---|---|"]
            for r in big + slow:
                if r["bundle"] in seen:
                    continue
                seen.add(r["bundle"])
                L.append(f"| {PurePosixPath(r['bundle']).name} | {r.get('status', '')} | "
                         f"{(r.get('bundle_bytes') or 0) / 1e6:.0f} | {r.get('members', '')} | "
                         f"{r.get('images', '')} | {r.get('seconds', '')} |")
            L.append("")

        L += ["## Directory layout (templates of member directories)", "",
              "| dir template | files |", "|---|---|"]
        for t, c in self.dir_templates.most_common(25):
            L.append(f"| `{t}` | {c} |")
        L.append("")

        L += ["## Member roles by extension", "", "| ext | role | files | bytes |", "|---|---|---|---|"]
        ext_stats: Dict[tuple, list] = defaultdict(lambda: [0, 0])
        for m in members:
            ext_stats[(m["ext"] or "(none)", m["role"])][0] += 1
            ext_stats[(m["ext"] or "(none)", m["role"])][1] += m["bytes"]
        for (e, r), (c, byt) in sorted(ext_stats.items(), key=lambda kv: -kv[1][0]):
            L.append(f"| {e} | {r} | {c} | {byt / 1e6:.1f} MB |")
        other_names = Counter(m.get("name_template") for m in members if m["role"] == "other")
        if other_names:
            L.append("")
            L.append("Non-doc/non-image filename templates: " +
                     ", ".join(f"`{t}` x{c}" for t, c in other_names.most_common(8)))
        L.append("")

        if self.assoc:
            L += ["## Image / document association", "", "| association | images |", "|---|---|"]
            for a, c in self.assoc.most_common():
                L.append(f"| {a} | {c} |")
            if images:
                stems = Counter(i["stem_template"] for i in images)
                L += ["", "Image filename templates: " +
                      ", ".join(f"`{t}` x{c}" for t, c in stems.most_common(8)), ""]
                fmts = Counter((i["format"], i["width"], i["height"], i["mode"]) for i in images)
                L += ["| format | width | height | mode | count |", "|---|---|---|---|---|"]
                for (f, w, h, mo), c in fmts.most_common(20):
                    L.append(f"| {f} | {w} | {h} | {mo} | {c} |")
            L.append("")
            if refs:
                L.append(f"Reference resolution in documents: {dict(Counter(r['match'] for r in refs))}")
                dang = [r for r in refs if not r["exists"]]
                if dang:
                    L.append(f"Dangling references: {len(dang)}; templates "
                             f"{dict(Counter(r['ref_template'] for r in dang).most_common(5))}")
                L.append("")

        L += ["## Structural shapes (by file count)", "",
              "| shape | family | files | paths | first bundle | last bundle |",
              "|---|---|---|---|---|---|"]
        for sh in sorted(self.shapes.values(), key=lambda s: -s["files"])[:40]:
            L.append(f"| {sh['shape_id']} | {sh['kind']} | {sh['files']} | {sh['n_paths']} | "
                     f"{PurePosixPath(sh['first_bundle']).name} | {PurePosixPath(sh['last_bundle']).name} |")
        for kind in self.docs_by_kind:
            shs = [s for s in self.shapes.values() if s["kind"] == kind]
            if len(shs) > 1:
                common = set.intersection(*(set(s["paths"]) for s in shs))
                top = max(shs, key=lambda s: s["files"])
                L.append(f"\n{kind}: {len(shs)} shapes, {len(common)} paths common to all. "
                         f"Paths missing from at least one shape vs the most common ({top['shape_id']}):")
                for p in sorted(set(top["paths"]) - common)[:20]:
                    L.append(f"- `{p}`")
                extra = sorted(set().union(*(set(s["paths"]) for s in shs)) - set(top["paths"]))
                if extra:
                    L.append("Paths that only appear in less common shapes:")
                    for p in extra[:20]:
                        L.append(f"- `{p}`")
        L.append("")

        def section(title: str, rs: List[dict], limit: int = 40, show_types: bool = False) -> None:
            L.append(f"## {title}")
            L.append("")
            if not rs:
                L.append("_none_")
            for r in rs[:limit]:
                td = r["types"] if show_types else f"type {r['dominant_type'] or 'container'}"
                L.append(f"- `{r['path']}`: {r['file_pct']}% of files, "
                         f"{r['min_per_parent']}..{r['max_per_parent']} per parent, {td}, "
                         f"{r['distinct_values']} distinct")
            if len(rs) > limit:
                L.append(f"- ... {len(rs) - limit} more in path_stats.csv")
            L.append("")

        leaves = [r for r in rows if r["node"] != "container"]
        section("Repeating paths (explode candidates)", [r for r in rows if r["repeating"]])
        section("Optional paths (< 100% of files of that family)",
                sorted([r for r in rows if r["optional"]], key=lambda r: r["file_pct"]))
        section("Mixed-type leaves",
                [r for r in leaves if len({t for t in json.loads(r["types"])
                                           if t not in ("empty", "null")}) > 1], show_types=True)
        section("Image reference paths", [r for r in leaves if r["dominant_type"] == "image_ref"])
        section("Datetime-like leaves",
                [r for r in leaves if r["dominant_type"].startswith("datetime")
                 or r["dominant_type"] in ("date_iso", "epoch_like")])
        section("Low-cardinality leaves (enum candidates)",
                [r for r in leaves if 0 < r["distinct_values"] <= 20 and r["occurrences"] >= 10
                 and r["dominant_type"] != "image_ref"])
        section("High-cardinality string leaves (identifier candidates)",
                [r for r in leaves if r["dominant_type"] in ("string", "str_int")
                 and r["occurrences"] >= 10 and r["distinct_values"] / r["occurrences"] > 0.9])
        section("Long text leaves", [r for r in leaves if r["dominant_type"] == "text_long"])

        if self.errors:
            L += ["## Errors", ""] + [f"- {e['bundle']} {e['member'] or ''}: {e['error']}"
                                      for e in self.errors[:30]]
            if len(self.errors) > 30:
                L.append(f"- ... {len(self.errors) - 30} more in errors.csv")
            L.append("")
        return "\n".join(L)

    def write_report(self, out: Path, ck: Checkpoint, scrub: Optional["Scrubber"] = None) -> None:
        rows = self.path_rows()
        T = {t: ck.rows(t) for t in ROW_TABLES}
        for r in T["members"]:
            name = PurePosixPath(r["member"]).name
            e = PurePosixPath(name).suffix.lower()
            r["ext"] = e if EXT_RE.match(e) else "(none)"
            r.setdefault("name_template", pattern_template(name))
        shapes = list(self.shapes.values())
        errors = self.errors
        done = ck.done
        if scrub:
            scrub.learn_bundles(list(done) + [r["bundle"] for r in T["bundles"]])
            T = {t: scrub.rows(v) for t, v in T.items()}
            rows = scrub.rows(rows)
            shapes = scrub.rows(shapes)
            errors = scrub.rows(errors)
            done = {scrub.bundle(k): v for k, v in done.items()}
            scrub.write_crosswalk(ck.dir / "scrub_crosswalk.json")
        self.write_path_files(out, scrub)
        saved = (self.shapes, self.errors)
        self.shapes = {sh["shape_id"]: sh for sh in shapes}
        self.errors = errors
        try:
            tables = {"path_stats": rows, **T, "errors": errors}
            for name, data in tables.items():
                write_csv(out / f"{name}.csv", data)
            (out / "shapes.json").write_text(json.dumps(shapes, indent=2), encoding="utf-8")
            (out / "summary.md").write_text(self.summary_md(rows, T, done), encoding="utf-8")
        finally:
            self.shapes, self.errors = saved
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            return
        for name, data in tables.items():
            if data and name != "errors":
                pq.write_table(pa.Table.from_pylist(data), out / f"{name}.parquet")


def safe_name(name: str, limit: int = 120) -> str:
    """A file name safe on Windows: anything outside [A-Za-z0-9._-] becomes ``_``."""
    s = re.sub(r"[^A-Za-z0-9._-]", "_", name)[:limit].rstrip(". ") or "_"
    return s


_BUNDLE_FIELDS = {"bundle", "first_bundle", "last_bundle"}


class Scrubber:
    """Report-time scrubbing: anonymise bundle keys and/or regex-replace strings."""

    def __init__(self, anon_bundles: bool, patterns: Sequence[str], replacement: str = "<scrubbed>"):
        self.anon = anon_bundles
        self.res = [re.compile(p, re.IGNORECASE) for p in patterns]
        self.repl = replacement
        self.bmap: Dict[str, str] = {}

    def learn_bundles(self, keys: List[str]) -> None:
        if not self.anon:
            return
        for k in sorted(set(keys)):
            if k not in self.bmap:
                self.bmap[k] = f"bundle_{len(self.bmap) + 1:04d}"

    def bundle(self, v: Any) -> Any:
        """A bundle key, exactly: replaced by its token."""
        if isinstance(v, str) and self.anon and v in self.bmap:
            v = self.bmap[v]
        return self.text(v, keys=False)

    def text(self, v: Any, keys: bool = True) -> Any:
        if not isinstance(v, str):
            return v
        if self.anon and keys:
            # Free text (an error message) may quote a bundle key; keys of a few
            # characters would corrupt unrelated text, so only longer ones are replaced.
            for base, tok in sorted(self.bmap.items(), key=lambda kv: -len(kv[0])):
                if len(base) > 3:
                    v = v.replace(base, tok)
        for r in self.res:
            v = r.sub(self.repl, v)
        return v

    def rows(self, rows: List[dict]) -> List[dict]:
        out = []
        for r in rows:
            nr = {}
            for k, v in r.items():
                if k in _BUNDLE_FIELDS:
                    nr[k] = self.bundle(v)
                elif isinstance(v, list):
                    nr[k] = [self.text(x) for x in v]
                else:
                    nr[k] = self.text(v)
            out.append(nr)
        return out

    def write_crosswalk(self, path: Path) -> None:
        if self.bmap:
            path.write_text(json.dumps({v: k for k, v in self.bmap.items()}, indent=2),
                            encoding="utf-8")


def write_csv(path: Path, rows: List[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: List[str] = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


# ------------------------------------------------------------------ entry point


@dataclass
class ProfileResult:
    out_dir: Path
    bundles_profiled: int = 0          # new bundles finished this run
    bundles_in_checkpoint: int = 0
    statuses: Dict[str, int] = field(default_factory=dict)
    docs_by_family: Dict[str, int] = field(default_factory=dict)
    errors: int = 0
    interrupted: bool = False
    exit_code: int = 0
    summary_path: Optional[Path] = None


def run_profile(
    *,
    source: Optional[str] = None,
    out: Any = "./profile",
    endpoint_url: Optional[str] = None,
    bundle_depth: int = 0,
    sample: int = 0,
    examples: int = 3,
    redact: bool = False,
    max_bundle_bytes: float = 0,
    max_docs: int = 0,
    max_images: int = 2000,
    heartbeat: float = 30,
    anon_bundles: bool = False,
    scrub: Sequence[str] = (),
    fresh: bool = False,
    report_only: bool = False,
    client: Any = None,
    on_bundle_done: Optional[Callable[[str], None]] = None,
    install_signal_handlers: bool = True,
) -> ProfileResult:
    """Profile ``source`` into ``out`` (resuming a checkpoint there) and write the report.

    ``on_bundle_done(key)`` is called after each bundle is checkpointed; it may call
    :func:`request_stop`. Signal handlers are installed only on the main thread.
    """
    global STOP
    STOP = False
    outp = Path(out)
    outp.mkdir(parents=True, exist_ok=True)
    if fresh:
        shutil.rmtree(outp / "checkpoint", ignore_errors=True)
    ck = Checkpoint(outp)
    scrubber = Scrubber(anon_bundles, list(scrub)) if (anon_bundles or scrub) else None
    prof = Profiler(examples, redact, max_docs, max_images, heartbeat)
    st = ck.load_state()
    if st:
        prof.load(st)
        logger.info("resuming: %d bundles already in checkpoint (%s)", len(ck.done),
                    dict(Counter(ck.done.values())))

    result = ProfileResult(out_dir=outp)

    def finish() -> ProfileResult:
        result.bundles_in_checkpoint = len(ck.done)
        result.statuses = dict(Counter(ck.done.values()))
        result.docs_by_family = dict(prof.docs_by_kind)
        result.errors = len(prof.errors)
        result.interrupted = STOP
        result.exit_code = 130 if STOP else result.exit_code
        result.summary_path = outp / "summary.md"
        return result

    if report_only:
        prof.write_report(outp, ck, scrubber)
        return finish()
    if not source:
        raise ProfileError("source is required unless report_only")

    src = open_source(source, bundle_depth, endpoint_url=endpoint_url, client=client)

    previous: Dict[int, Any] = {}
    if install_signal_handlers and threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, _on_signal)

    try:
        for ref in src.list_bundles():
            if ref.key in ck.done:
                continue
            if STOP:
                break
            if max_bundle_bytes and ref.size is not None and ref.size > max_bundle_bytes:
                logger.info("skip (too big, %.1f GB): %s", ref.size / 1e9, ref.key)
                ck.append("bundles", [{"bundle": ref.key, "modified": ref.modified,
                                       "bundle_bytes": ref.size, "status": "skipped_too_big"}])
                ck.mark(ref.key, "skipped_too_big", bytes=ref.size)
                continue
            logger.info("[%d] %s (%.1f MB)", len(ck.done) + 1, ref.key,
                        ref.size / 1e6 if ref.size is not None else float("nan"))
            prof.reset_buffers()
            t0 = time.time()
            try:
                with src.open_bundle(ref) as ob:
                    if max_bundle_bytes and ref.size is None and ob.size and ob.size > max_bundle_bytes:
                        # A tree's size is only known once it is open.
                        logger.info("skip (too big, %.1f GB): %s", ob.size / 1e9, ref.key)
                        ck.append("bundles", [{"bundle": ref.key, "modified": ob.modified,
                                               "bundle_bytes": ob.size, "status": "skipped_too_big"}])
                        ck.mark(ref.key, "skipped_too_big", bytes=ob.size)
                        continue
                    row = prof.profile_bundle(ref, ob)
            except Interrupted:
                for r in prof.buf["bundles"]:
                    r["status"] = "partial"
                if not prof.buf["bundles"]:
                    prof.buf["bundles"].append({"bundle": ref.key, "modified": ref.modified,
                                                "bundle_bytes": ref.size, "status": "partial",
                                                "seconds": round(time.time() - t0, 1)})
                for t in ROW_TABLES:
                    ck.append(t, prof.buf[t])
                ck.save_state(prof.state())
                ck.mark(ref.key, "partial", seconds=round(time.time() - t0, 1),
                        docs=len(prof.buf["docs"]), images=len(prof.buf["images"]))
                logger.info("    partial: %d docs, %d images kept",
                            len(prof.buf["docs"]), len(prof.buf["images"]))
                break
            except Exception as e:  # noqa: BLE001 -- a broken bundle must not kill the run
                prof.errors.append({"bundle": ref.key, "member": None,
                                    "error": f"bundle: {type(e).__name__}: {e}"})
                ck.append("bundles", [{"bundle": ref.key, "modified": ref.modified,
                                       "bundle_bytes": ref.size, "status": "error",
                                       "seconds": round(time.time() - t0, 1)}])
                ck.save_state(prof.state())
                ck.mark(ref.key, "error", error=type(e).__name__)
                logger.error("    bundle %s failed: %s", ref.key, type(e).__name__)
                continue
            for t in ROW_TABLES:
                ck.append(t, prof.buf[t])
            ck.save_state(prof.state())
            ck.mark(ref.key, "done", seconds=row["seconds"])
            logger.info("    done in %ss: %d members, %d docs, %d images",
                        row["seconds"], row["members"], row["docs"], row["images"])
            result.bundles_profiled += 1
            if on_bundle_done:
                on_bundle_done(ref.key)
            if sample and result.bundles_profiled >= sample:
                break
    except KeyboardInterrupt:
        result.exit_code = 130
    finally:
        for sig, h in previous.items():
            signal.signal(sig, h)
        prof.write_report(outp, ck, scrubber)
        logger.info("report written to %s (%d bundles in checkpoint)",
                    outp / "summary.md", len(ck.done))
    return finish()
