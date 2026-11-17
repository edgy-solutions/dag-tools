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
from typing import Optional

from ..image_record import (  # noqa: F401  (re-exported)
    ImageRef,
    _DIGEST_RE,
    parse_image_reference,
)

_HEX_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")


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
