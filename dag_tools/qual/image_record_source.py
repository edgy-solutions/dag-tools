"""Fetch an image record's bytes from an https URL or a local path.

Not the pure module: this one does I/O (httpx). ``image_record.py`` stays
free of it. Parsing and name checks are the caller's, through that module.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Optional

import httpx

MAX_RECORD_BYTES = 65536
TIMEOUT_SECONDS = 10

_SCHEME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]*)://")
_ABSENT_TAIL = "absent at source; a record can lag its build by minutes, retry"


class ImageRecordSourceError(ValueError):
    """Base. Carries ``.source`` and ``.kind``."""

    kind = "unreadable"

    def __init__(self, source: str, message: str):
        self.source = source
        super().__init__(f"image record {source!r}: {message}")


class ImageRecordAbsent(ImageRecordSourceError):
    kind = "absent"

    def __init__(self, source: str, detail: str = ""):
        prefix = f"{detail}: " if detail else ""
        super().__init__(source, f"{prefix}{_ABSENT_TAIL}")


class ImageRecordUnreadable(ImageRecordSourceError):
    kind = "unreadable"


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def fetch_image_record(
    source: str, *, client: Optional[httpx.Client] = None,
) -> bytes:
    """Return the record's bytes. Raises only :class:`ImageRecordSourceError`."""
    m = _SCHEME_RE.match(source)
    if m is None:
        return _read_local(source)
    scheme = m.group(1).lower()
    if scheme != "https":
        raise ImageRecordUnreadable(
            source, f"scheme {scheme!r} is refused; only https or a local path"
        )
    own = client is None
    c = client or httpx.Client()
    try:
        return _get(source, c)
    except ImageRecordSourceError:
        raise
    except Exception as e:  # transport, timeout, decode
        raise ImageRecordUnreadable(source, f"fetch failed ({type(e).__name__})") from None
    finally:
        if own:
            c.close()


def _get(source: str, client: httpx.Client) -> bytes:
    with client.stream(
        "GET", source, timeout=TIMEOUT_SECONDS, follow_redirects=True,
    ) as resp:
        final = str(resp.url)
        if not final.lower().startswith("https://"):
            raise ImageRecordUnreadable(
                source, "redirected to a non-https URL; refused"
            )
        if resp.status_code == 404:
            raise ImageRecordAbsent(source, "HTTP 404")
        if resp.status_code != 200:
            raise ImageRecordUnreadable(source, f"HTTP {resp.status_code}")
        buf = bytearray()
        for chunk in resp.iter_bytes():
            buf += chunk
            if len(buf) > MAX_RECORD_BYTES:
                raise ImageRecordUnreadable(
                    source, f"body exceeds the {MAX_RECORD_BYTES}-byte cap"
                )
        return bytes(buf)


def _read_local(source: str) -> bytes:
    path = Path(source)
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        raise ImageRecordAbsent(source) from None
    except OSError as e:
        raise ImageRecordUnreadable(source, f"stat failed ({type(e).__name__})") from None
    if size > MAX_RECORD_BYTES:
        raise ImageRecordUnreadable(source, f"file exceeds the {MAX_RECORD_BYTES}-byte cap")
    try:
        return path.read_bytes()
    except FileNotFoundError:
        raise ImageRecordAbsent(source) from None
    except OSError as e:
        raise ImageRecordUnreadable(source, f"read failed ({type(e).__name__})") from None
