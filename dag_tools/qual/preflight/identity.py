"""Pure parsing of a container image reference, for preflight identity.

No IO, no Dagster. A code location reports its image as
``<repository>[:<tag>][@sha256:<digest>]``; preflight matches the digest
(or a ``sha-<short>`` tag) against what CI recorded for the pinned commit.

A reference that is half-readable is not evidence: an ``@`` with a malformed
digest makes the whole reference unparseable (``digest`` and ``tag`` both
``None``, ``error`` says why) rather than falling back to its tag.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

_DIGEST_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
_HEX_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")


@dataclass(frozen=True)
class ImageRef:
    repository: str
    tag: Optional[str] = None
    digest: Optional[str] = None
    """Lowercased ``sha256:<64 hex>``."""
    error: Optional[str] = None
    """Set when the reference is unparseable; digest and tag are then None."""


def parse_image_reference(ref: str) -> ImageRef:
    """Parse ``repository[:tag][@sha256:<hex>]``.

    Split on the LAST ``@``; the right side must be a sha256 digest. The tag
    is what follows the last ``:`` only if that ``:`` comes after the last
    ``/`` (so ``host:5000/a/b`` has no tag). Nothing is normalized.
    """
    text = (ref or "").strip()
    if not text:
        return ImageRef(repository="", error="empty image reference")
    digest: Optional[str] = None
    name = text
    if "@" in text:
        name, _, raw = text.rpartition("@")
        if not _DIGEST_RE.match(raw):
            return ImageRef(
                repository=text,
                error=f"malformed digest {raw!r}: expected sha256:<64 hex>",
            )
        digest = raw.lower()
    tag: Optional[str] = None
    colon = name.rfind(":")
    if colon > name.rfind("/") and colon != -1:
        tag = name[colon + 1:] or None
        name = name[:colon]
    if not name:
        return ImageRef(repository=text, error="no repository in image reference")
    return ImageRef(repository=name, tag=tag, digest=digest)


def sha_from_tag(tag: Optional[str], prefix: str) -> Optional[str]:
    """The sha a tag names: ``prefix`` followed by 7-40 hex characters."""
    if not tag or not prefix or not tag.startswith(prefix):
        return None
    rest = tag[len(prefix):]
    return rest if _HEX_RE.match(rest) else None


def same_repository(a: str, b: str) -> bool:
    """Case-insensitive equality. Nothing is normalized. Affects diagnosis
    only (mismatch vs unmatched pin), never pass/fail."""
    return a.strip().lower() == b.strip().lower()
