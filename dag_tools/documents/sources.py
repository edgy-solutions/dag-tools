"""Bundle sources: zips and directory trees, local or S3-compatible.

A *bundle* is the unit of ingestion and the unit of idempotence: one zip, or
the directory ``bundle_depth`` levels down in a directory tree.
A bundle's *key* is its path relative to the source root, POSIX-spelled, so
it is the same string on every machine that reads the same source -- which
is what lets a writer delete exactly one bundle's rows before re-appending
them. The whole-tree bundle of a ``bundle_depth: 0`` source is keyed ``.``.
At ``bundle_depth >= 1`` discovery lists only the first ``bundle_depth``
levels and one bundle is resolved and opened without listing the rest of the
source; see :class:`BundleSource`.

Nothing is downloaded whole. A zip on S3 is read through ranged GETs
(:class:`S3File`), and a member is read only when someone asks for its
bytes. One level of nested zip inside a zip bundle is followed: the inner
zip is spooled (memory, then disk past ``NESTED_SPOOL_BYTES``) and its
members appear under ``<inner zip path>/``. A zip nested deeper than that is
left as an ordinary member and listed in :attr:`OpenBundle.unexpanded_zips`
rather than silently dropped.

Pure: no Dagster import. ``boto3`` is imported only when an ``s3://`` source
is actually opened.
"""
from __future__ import annotations

import io
import logging
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

ZIP_SUFFIX = ".zip"
S3_CHUNK = 8 * 1024 * 1024
NESTED_SPOOL_BYTES = 64 * 1024 * 1024
ROOT_BUNDLE_KEY = "."

_EXT_RE = re.compile(r"^\.[a-z0-9]{1,5}$")


class SourceError(Exception):
    """A source that cannot be listed or a bundle that cannot be opened."""


def normalise_member_path(path: str) -> str:
    """Backslash and slash are one separator; no leading slash, no ``./``."""
    p = str(path).replace("\\", "/")
    while p.startswith("./"):
        p = p[2:]
    return p.lstrip("/")


def member_label(name: str) -> str:
    """The path a member is known by inside its bundle.

    ``.`` and ``..`` segments are collapsed, and any ``..`` that would climb
    above the bundle root is dropped -- the convention ``zipfile.extract``
    follows. A zip can name a member ``../x``, and a label that leaves the
    bundle would mislead every path resolved against it. ``""`` means the
    name points at nothing inside the bundle and the member is skipped.

    Distinct from :func:`normalise_member_path`, which a reference *inside*
    a document goes through: there ``..`` is meaningful relative to the
    document's own directory and must survive until it is joined.
    """
    parts: List[str] = []
    for seg in normalise_member_path(name).split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            if parts:
                parts.pop()
            continue
        parts.append(seg)
    return "/".join(parts)


@dataclass
class Member:
    """One file inside a bundle. Bytes are read on demand."""

    path: str
    size: int
    modified: str
    reader: Callable[[Optional[int]], bytes] = field(repr=False)

    def read(self, limit: Optional[int] = None) -> bytes:
        """Read the member, or at most ``limit`` bytes of it."""
        return self.reader(limit)

    @property
    def name(self) -> str:
        return PurePosixPath(self.path).name

    @property
    def ext(self) -> str:
        e = PurePosixPath(self.path).suffix.lower()
        # "host.corp.com-1234" has no extension.
        return e if _EXT_RE.match(e) else ""

    @property
    def stem(self) -> str:
        name = self.name
        return name[: -len(self.ext)] if self.ext else name

    @property
    def dir(self) -> str:
        d = str(PurePosixPath(self.path).parent)
        return "" if d == "." else d


@dataclass(frozen=True)
class BundleRef:
    """A bundle as listed: enough to decide whether to ingest it."""

    key: str
    kind: str  # "zip" | "tree"
    modified: Optional[str] = None
    size: Optional[int] = None

    @property
    def stamp(self) -> Optional[str]:
        """Changes when the bundle's bytes plausibly changed.

        ``None`` when ``modified`` or ``size`` is unknown, which is the case
        for every tree bundle found by discovery at ``bundle_depth >= 1``
        (finding the figures would mean listing the bundle). A caller needing
        a deterministic token must substitute one.
        """
        if self.modified is None or self.size is None:
            return None
        return f"{self.modified}:{self.size}"

    @property
    def name(self) -> str:
        """The bundle's own name: the last component of its key."""
        return PurePosixPath(self.key).name if self.key != ROOT_BUNDLE_KEY else ROOT_BUNDLE_KEY


@dataclass
class OpenBundle:
    """An opened bundle. Use as a context manager so zip handles close."""

    ref: BundleRef
    members: List[Member]
    unexpanded_zips: List[str] = field(default_factory=list)
    #: Total bytes and newest modification time, computed when the bundle was opened.
    size: Optional[int] = None
    modified: Optional[str] = None
    _closers: List[Callable[[], None]] = field(default_factory=list, repr=False)

    def close(self) -> None:
        while self._closers:
            try:
                self._closers.pop()()
            except Exception:  # noqa: BLE001, S110 -- best effort; one closer failing must not leak the rest  # pragma: no cover
                pass

    def __enter__(self) -> "OpenBundle":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ------------------------------------------------------------------ S3 file


class S3File(io.RawIOBase):
    """Seekable read-only view of an S3 object: ranged GETs, read-ahead buffer."""

    def __init__(self, client: Any, bucket: str, key: str, size: int, chunk: int = S3_CHUNK):
        super().__init__()
        self._c, self._b, self._k, self._size, self._chunk = client, bucket, key, size, chunk
        self._pos = 0
        self._buf = b""
        self._buf_start = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, off: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            pos = off
        elif whence == io.SEEK_CUR:
            pos = self._pos + off
        else:
            pos = self._size + off
        self._pos = max(0, min(pos, self._size))
        return self._pos

    def _fill(self, start: int, want: int) -> None:
        end = min(self._size, start + max(want, self._chunk)) - 1
        r = self._c.get_object(Bucket=self._b, Key=self._k, Range=f"bytes={start}-{end}")
        self._buf, self._buf_start = r["Body"].read(), start

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            n = self._size - self._pos
        out = bytearray()
        while n > 0 and self._pos < self._size:
            if not (self._buf_start <= self._pos < self._buf_start + len(self._buf)):
                self._fill(self._pos, n)
                if not self._buf:
                    break
            off = self._pos - self._buf_start
            piece = self._buf[off : off + n]
            out += piece
            self._pos += len(piece)
            n -= len(piece)
        return bytes(out)

    def readinto(self, b: Any) -> int:
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)


# ------------------------------------------------------------------ zip


def _zip_members(
    fobj: Any, modified_default: str, prefix: str = ""
) -> Tuple[zipfile.ZipFile, List[Member]]:
    zf = zipfile.ZipFile(fobj)
    members: List[Member] = []
    for zi in zf.infolist():
        if zi.is_dir():
            continue

        def _read(n: Optional[int], _zi: zipfile.ZipInfo = zi) -> bytes:
            with zf.open(_zi) as fh:
                return fh.read() if n is None else fh.read(n)

        try:
            mod = datetime(*zi.date_time).isoformat()
        except (TypeError, ValueError):  # a zip may carry an impossible date
            mod = modified_default
        path = member_label(zi.filename)
        if not path:
            continue
        if prefix:
            path = f"{prefix}/{path}"
        members.append(Member(path, zi.file_size, mod, _read))
    return zf, members


def _expand_nested(bundle: OpenBundle) -> None:
    """Replace each top-level ``*.zip`` member with its own members, once."""
    out: List[Member] = []
    for m in bundle.members:
        if m.path.lower().endswith(ZIP_SUFFIX):
            # Not SpooledTemporaryFile: before Python 3.11 it has no
            # seekable(), and zipfile cannot read a member through it.
            spool: Any = (
                io.BytesIO() if m.size <= NESTED_SPOOL_BYTES else tempfile.TemporaryFile()  # noqa: SIM115 -- closed by the bundle's closers
            )
            spool.write(m.read())
            spool.seek(0)
            try:
                zf, inner = _zip_members(spool, m.modified, prefix=m.path)
            except zipfile.BadZipFile:
                spool.close()
                out.append(m)
                bundle.unexpanded_zips.append(m.path)
                continue
            bundle._closers.append(spool.close)
            bundle._closers.append(zf.close)
            for im in inner:
                if im.path.lower().endswith(ZIP_SUFFIX):
                    bundle.unexpanded_zips.append(im.path)
            out.extend(inner)
        else:
            out.append(m)
    bundle.members = out


# ------------------------------------------------------------------ grouping


def bundle_key_for(rel: str, depth: int) -> str:
    """The tree-bundle key a non-zip file at ``rel`` belongs to.

    With ``depth >= 1`` a file is in a tree bundle only when it sits *below*
    the bundle directory; a file at or above the depth belongs to none and
    answers ``.`` here only so legacy callers keep a value.
    """
    parts = PurePosixPath(rel).parts
    if depth <= 0 or len(parts) <= depth:
        return ROOT_BUNDLE_KEY
    return "/".join(parts[:depth])


def _inner_path(rel: str, key: str) -> str:
    if key == ROOT_BUNDLE_KEY:
        return rel
    return rel[len(key) + 1 :]


def _is_zip(name: str) -> bool:
    return name.lower().endswith(ZIP_SUFFIX)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


# A level entry: (name, is_dir, size, modified). size/modified are None for a directory.
_Entry = Tuple[str, bool, Optional[int], Optional[str]]


def _prune(prefix: str, start_after: Optional[str]) -> Tuple[str, Optional[str]]:
    """How ``start_after`` bears on the level whose path prefix is ``prefix``.

    ``prefix`` is ``""`` or ends in ``/``. Returns ``("none", None)`` when
    nothing is pruned, ``("all", None)`` when every key below is already past
    (<=) ``start_after``, and ``("after", name)`` when ``prefix`` is an
    ancestor of ``start_after``: entries named less than ``name`` are pruned
    and the entry named ``name`` is kept, so the ancestor directory is still
    visited. Keys under a prefix that is not an ancestor compare uniformly,
    because they first differ from ``start_after`` inside the prefix.
    """
    if not start_after:
        return "none", None
    if not prefix:
        return "after", start_after.split("/")[0]
    if start_after.startswith(prefix):
        return "after", start_after[len(prefix):].split("/")[0]
    return ("all", None) if start_after > prefix else ("none", None)


# ------------------------------------------------------------------ sources


class BundleSource:
    """List and open bundles under one root.

    ``bundle_depth`` D (a path's *depth* is its number of ``/``-separated
    components below the root):

    * ``D >= 1``: a directory at depth exactly D is a tree bundle; a ``.zip``
      at depth <= D is its own zip bundle; a non-zip file at depth <= D is not
      a bundle and is counted in :attr:`files_above_depth`. A zip inside a
      tree bundle is a member, expanded one level. Discovery lists the first D
      levels only (never a bundle's contents) and resolving or opening one
      bundle touches only that bundle.
    * ``D == 0``: one ``.`` bundle of every non-zip file, plus every zip
      anywhere as its own bundle. This lists the WHOLE source on every call
      and is unsuitable for large trees.

    After :meth:`list_bundles` is exhausted, :attr:`files_above_depth` holds
    the count of non-zip files found at or above the bundle depth (always 0 at
    D == 0). One logger line with that count is emitted per complete listing.
    """

    def __init__(self, uri: str, bundle_depth: int = 0):
        if bundle_depth < 0:
            raise SourceError("bundle_depth must be >= 0")
        self.uri = uri
        self.bundle_depth = bundle_depth
        self.files_above_depth = 0

    # -- primitives each backend supplies
    def _list_files(self) -> Iterator[Tuple[str, int, str]]:  # (rel, size, modified); the WHOLE source
        raise NotImplementedError

    def _list_level(self, rel_dir: str, after_name: Optional[str]) -> Iterator[_Entry]:
        """The direct children of ``rel_dir`` (``""`` = root), no recursion."""
        raise NotImplementedError

    def _list_tree(self, rel_dir: str) -> Iterator[Tuple[str, int, str]]:
        """Every file below ``rel_dir``: (path relative to ``rel_dir``, size, modified)."""
        raise NotImplementedError

    def _stat_file(self, rel: str) -> Optional[Tuple[int, str]]:
        """(size, modified) of one file, or None when it is absent."""
        raise NotImplementedError

    def _dir_exists(self, rel_dir: str) -> bool:
        raise NotImplementedError

    def _reader(self, rel: str) -> Callable[[Optional[int]], bytes]:
        raise NotImplementedError

    def _zip_handle(self, rel: str, size: int) -> Any:
        raise NotImplementedError

    # -- discovery
    def list_bundles(self, start_after: Optional[str] = None) -> Iterator[BundleRef]:
        """Every bundle under the root, as a stream.

        ``start_after``: bundles whose key is <= it are not yielded, and the
        listing is pruned where it can be (``StartAfter`` on S3). Tree refs
        from ``D >= 1`` carry ``size``/``modified`` of None; open the bundle
        for the real figures.
        """
        self.files_above_depth = 0
        if self.bundle_depth == 0:
            yield from self._list_flat(start_after)
        else:
            for ref in self._walk(0, "", start_after):
                if start_after is None or ref.key > start_after:
                    yield ref
        if self.files_above_depth:
            logger.info("document source: %d file(s) above the bundle depth %d are not bundles",
                        self.files_above_depth, self.bundle_depth)

    def _list_flat(self, start_after: Optional[str]) -> Iterator[BundleRef]:
        groups: List[Tuple[str, int, str]] = []
        for rel, size, mod in self._list_files():
            if _is_zip(rel):
                if start_after is None or rel > start_after:
                    yield BundleRef(rel, "zip", mod, size)
                continue
            groups.append((rel, size, mod))
        if groups and (start_after is None or ROOT_BUNDLE_KEY > start_after):
            yield BundleRef(ROOT_BUNDLE_KEY, "tree", max(r[2] for r in groups), sum(r[1] for r in groups))

    def _walk(self, level: int, rel_dir: str, start_after: Optional[str]) -> Iterator[BundleRef]:
        prefix = f"{rel_dir}/" if rel_dir else ""
        mode, after = _prune(prefix, start_after)
        if mode == "all":
            return
        depth = self.bundle_depth
        n = level + 1
        for name, is_dir, size, mod in self._list_level(rel_dir, after):
            rel = f"{prefix}{name}"
            if is_dir:
                if n == depth:
                    yield BundleRef(rel, "tree", None, None)
                elif n < depth:
                    yield from self._walk(n, rel, start_after)
            elif _is_zip(name):
                yield BundleRef(rel, "zip", mod or "", size or 0)
            else:
                self.files_above_depth += 1

    # -- resolving one bundle
    def ref_for(self, key: str) -> BundleRef:
        """The ref for ``key``. At ``D >= 1`` this does not list the source:
        a zip is one stat, a tree one existence probe of its own prefix."""
        if self.bundle_depth == 0:
            for ref in self._list_flat(None):
                if ref.key == key:
                    return ref
            raise SourceError(f"no bundle {key!r} under the source")
        depth = self.bundle_depth
        parts = key.split("/")
        if key == ROOT_BUNDLE_KEY or any(p in ("", ".", "..") for p in parts):
            raise SourceError(f"{key!r} is not a bundle key at bundle_depth {depth}")
        if _is_zip(key):
            if len(parts) > depth:
                raise SourceError(
                    f"zip {key!r} is deeper than bundle_depth {depth}: it is a member of a tree bundle, not a bundle"
                )
            st = self._stat_file(key)
            if st is None:
                raise SourceError(f"no bundle {key!r} under the source")
            return BundleRef(key, "zip", st[1], st[0])
        if len(parts) != depth:
            raise SourceError(f"bundle key {key!r} has {len(parts)} component(s); bundle_depth is {depth}")
        if not self._dir_exists(key):
            raise SourceError(f"no bundle {key!r} under the source")
        return BundleRef(key, "tree", None, None)

    # -- opening
    def open_bundle(self, ref: BundleRef) -> OpenBundle:
        if ref.kind == "zip":
            fobj = self._zip_handle(ref.key, ref.size or 0)
            try:
                zf, members = _zip_members(fobj, ref.modified or "")
            except zipfile.BadZipFile as e:
                _close_quietly(fobj)
                raise SourceError(f"bundle {ref.key!r} is not a readable zip") from e
            bundle = OpenBundle(ref, members, size=ref.size, modified=ref.modified,
                                _closers=[lambda: _close_quietly(fobj), zf.close])
            _expand_nested(bundle)
            return bundle
        members: List[Member] = []
        total = 0
        newest: Optional[str] = None
        files: Iterator[Tuple[str, int, str, str]]  # (inner path, size, modified, path to read)
        if self.bundle_depth == 0:
            files = (
                (_inner_path(rel, ref.key), size, mod, rel)
                for rel, size, mod in self._list_files()
                if not _is_zip(rel) and bundle_key_for(rel, 0) == ref.key
            )
        else:
            files = ((inner, size, mod, f"{ref.key}/{inner}") for inner, size, mod in self._list_tree(ref.key))
        for inner, size, mod, rel in files:
            total += size
            if newest is None or mod > newest:
                newest = mod
            label = member_label(inner)
            if not label:
                continue
            members.append(Member(label, size, mod, self._reader(rel)))
        bundle = OpenBundle(ref, members, size=total, modified=newest)
        if self.bundle_depth >= 1:
            _expand_nested(bundle)  # a zip inside a tree bundle is a member, expanded one level
        bundle.members.sort(key=lambda m: m.path)
        return bundle


def _close_quietly(fobj: Any) -> None:
    try:
        fobj.close()
    except Exception:  # noqa: BLE001, S110 -- closing is best effort  # pragma: no cover
        pass


class LocalSource(BundleSource):
    def __init__(self, uri: str, bundle_depth: int = 0):
        super().__init__(uri, bundle_depth)
        self.root = Path(uri.removeprefix("file://"))

    def _list_files(self) -> Iterator[Tuple[str, int, str]]:
        if not self.root.is_dir():
            raise SourceError(f"source directory does not exist: {self.root}")
        for p in sorted(self.root.rglob("*")):
            if not p.is_file():
                continue
            st = p.stat()
            yield p.relative_to(self.root).as_posix(), st.st_size, _iso(st.st_mtime)

    def _list_level(self, rel_dir: str, after_name: Optional[str]) -> Iterator[_Entry]:
        base = self.root / rel_dir if rel_dir else self.root
        if not rel_dir and not base.is_dir():
            raise SourceError(f"source directory does not exist: {self.root}")
        with os.scandir(base) as it:
            entries = sorted(it, key=lambda e: e.name)
        for e in entries:
            if after_name is not None and e.name < after_name:
                continue
            if e.is_dir():
                yield e.name, True, None, None
            elif e.is_file():
                st = e.stat()
                yield e.name, False, st.st_size, _iso(st.st_mtime)

    def _list_tree(self, rel_dir: str) -> Iterator[Tuple[str, int, str]]:
        def walk(path: Path, inner: str) -> Iterator[Tuple[str, int, str]]:
            with os.scandir(path) as it:
                entries = sorted(it, key=lambda e: e.name)
            for e in entries:
                rel = f"{inner}{e.name}"
                if e.is_dir():
                    yield from walk(path / e.name, rel + "/")
                elif e.is_file():
                    st = e.stat()
                    yield rel, st.st_size, _iso(st.st_mtime)

        yield from walk(self.root / rel_dir, "")

    def _stat_file(self, rel: str) -> Optional[Tuple[int, str]]:
        p = self.root / rel
        if not p.is_file():
            return None
        st = p.stat()
        return st.st_size, _iso(st.st_mtime)

    def _dir_exists(self, rel_dir: str) -> bool:
        return (self.root / rel_dir).is_dir()

    def _reader(self, rel: str) -> Callable[[Optional[int]], bytes]:
        path = self.root / rel

        def _read(n: Optional[int]) -> bytes:
            with path.open("rb") as fh:
                return fh.read() if n is None else fh.read(n)

        return _read

    def _zip_handle(self, rel: str, size: int) -> Any:
        return (self.root / rel).open("rb")


_S3_RE = re.compile(r"^s3://([^/]+)/?(.*)$")


class S3Source(BundleSource):
    """An S3-compatible prefix. Credentials come from boto3's own chain."""

    def __init__(self, uri: str, bundle_depth: int = 0, endpoint_url: Optional[str] = None,
                 client: Any = None):
        super().__init__(uri, bundle_depth)
        m = _S3_RE.match(uri)
        if not m:
            raise SourceError(f"not an s3:// uri: {uri!r}")
        self.bucket = m.group(1)
        self.prefix = m.group(2).rstrip("/")
        self.endpoint_url = endpoint_url
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            import boto3
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url or None,
                config=Config(retries={"max_attempts": 8, "mode": "adaptive"}),
            )
        return self._client

    def _key(self, rel: str) -> str:
        return f"{self.prefix}/{rel}" if self.prefix else rel

    @staticmethod
    def _mod(obj: Any) -> str:
        mod = obj["LastModified"]
        return mod.isoformat() if hasattr(mod, "isoformat") else str(mod)

    def _list_files(self) -> Iterator[Tuple[str, int, str]]:
        prefix = f"{self.prefix}/" if self.prefix else ""
        pages = self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                yield key[len(prefix):], int(obj["Size"]), self._mod(obj)

    def _list_level(self, rel_dir: str, after_name: Optional[str]) -> Iterator[_Entry]:
        prefix = self._key(rel_dir) + "/" if rel_dir else (f"{self.prefix}/" if self.prefix else "")
        kw: Dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix, "Delimiter": "/"}
        if after_name is not None:
            kw["StartAfter"] = f"{prefix}{after_name}"
        for page in self.client.get_paginator("list_objects_v2").paginate(**kw):
            out: List[_Entry] = []
            for cp in page.get("CommonPrefixes", []) or []:
                name = cp["Prefix"][len(prefix):].rstrip("/")
                if name:
                    out.append((name, True, None, None))
            for obj in page.get("Contents", []) or []:
                name = obj["Key"][len(prefix):]
                if not name or name.endswith("/"):
                    continue
                out.append((name, False, int(obj["Size"]), self._mod(obj)))
            yield from sorted(out, key=lambda e: e[0])

    def _list_tree(self, rel_dir: str) -> Iterator[Tuple[str, int, str]]:
        prefix = self._key(rel_dir) + "/"
        pages = self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                yield key[len(prefix):], int(obj["Size"]), self._mod(obj)

    def _stat_file(self, rel: str) -> Optional[Tuple[int, str]]:
        try:
            r = self.client.head_object(Bucket=self.bucket, Key=self._key(rel))
        except Exception as e:
            code = str(getattr(e, "response", {}).get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return int(r["ContentLength"]), self._mod(r)

    def _dir_exists(self, rel_dir: str) -> bool:
        r = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self._key(rel_dir) + "/", MaxKeys=1)
        return bool(r.get("Contents") or r.get("CommonPrefixes"))

    def _reader(self, rel: str) -> Callable[[Optional[int]], bytes]:
        key = self._key(rel)

        def _read(n: Optional[int]) -> bytes:
            kw = {"Range": f"bytes=0-{n - 1}"} if n else {}
            if n == 0:
                return b""
            return self.client.get_object(Bucket=self.bucket, Key=key, **kw)["Body"].read()

        return _read

    def _zip_handle(self, rel: str, size: int) -> Any:
        return io.BufferedReader(S3File(self.client, self.bucket, self._key(rel), size), S3_CHUNK)


def open_source(uri: str, bundle_depth: int = 0, endpoint_url: Optional[str] = None,
                client: Any = None) -> BundleSource:
    """The source for ``uri``: ``s3://bucket/prefix`` or a local directory."""
    if uri.startswith("s3://"):
        return S3Source(uri, bundle_depth, endpoint_url=endpoint_url, client=client)
    if "://" in uri and not uri.startswith("file://"):
        raise SourceError(f"unsupported source scheme in {uri!r}; use s3:// or a local path")
    return LocalSource(uri, bundle_depth)
