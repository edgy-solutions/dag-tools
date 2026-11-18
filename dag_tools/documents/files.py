"""Resolving a document's reference to a file in the same bundle.

A document names an attachment by a path written however its producer wrote
it: relative to the document, relative to the bundle, with either separator.
The mapping lists resolution strategies in order and the **first strategy
with exactly one candidate wins**:

  * ``document_dir`` -- the reference joined to the document's directory;
  * ``bundle_root`` -- the reference taken from the bundle root;
  * ``basename_under: "<template>"`` -- any member whose file name is the
    reference's file name, anywhere under ``<document dir>/<template>``.
    The template may use ``{document_stem}`` and ``{document_name}`` and
    nothing else, replaced literally (never ``str.format``).

A strategy with no candidate passes to the next. A strategy with more than
one candidate **stops** the search as ``ambiguous`` and no bytes are stored:
picking one of two files with the same name is a guess, and a wrong guess
reads exactly like a right one. A reference that climbs out of the bundle
(``../..``) has no candidate. Matching is exact after separator
normalisation -- never case-folded.
"""
from __future__ import annotations

import io
import posixpath
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from dag_tools.documents.mapping import BasenameUnder
from dag_tools.documents.sources import Member, normalise_member_path

MISSING = "missing"
AMBIGUOUS = "ambiguous"


class MemberIndex:
    """A bundle's members by exact path and by file name."""

    def __init__(self, members: Sequence[Member]):
        self.by_path: Dict[str, Member] = {}
        self.by_name: Dict[str, List[Member]] = {}
        for m in members:
            self.by_path[m.path] = m
            self.by_name.setdefault(m.name, []).append(m)


def _clean(path: str) -> Optional[str]:
    p = posixpath.normpath(normalise_member_path(path))
    if p in (".", "") or p == ".." or p.startswith("../"):
        return None
    return p


def render_template(template: str, document: Member) -> str:
    return template.replace("{document_stem}", document.stem).replace("{document_name}", document.name)


def resolve(
    ref: Optional[str],
    document: Member,
    strategies: Sequence[Union[str, BasenameUnder]],
    index: MemberIndex,
) -> Tuple[Optional[Member], Optional[str]]:
    """``(member, outcome)``; outcome is the winning strategy, missing or ambiguous.

    A null or blank reference is no reference at all: ``(None, None)``.
    """
    if ref is None:
        return None, None
    ref_n = normalise_member_path(ref.strip())
    if not ref_n:
        return None, None
    doc_dir = document.dir
    for strat in strategies:
        if isinstance(strat, BasenameUnder):
            base = posixpath.basename(ref_n)
            root = _clean(posixpath.join(doc_dir, render_template(strat.basename_under, document)))
            if not base or root is None:
                continue
            prefix = root + "/"
            hits = [m for m in index.by_name.get(base, ()) if m.path.startswith(prefix)]
            name = "basename_under"
        else:
            target = _clean(posixpath.join(doc_dir, ref_n) if strat == "document_dir" else ref_n)
            m = index.by_path.get(target) if target is not None else None
            hits = [m] if m is not None else []
            name = strat
        if len(hits) == 1:
            return hits[0], name
        if len(hits) > 1:
            return None, AMBIGUOUS
    return None, MISSING


def probe_image(data: bytes) -> Tuple[Optional[str], Optional[int], Optional[int], bool]:
    """``(format, width, height, failed)`` from the header only; never decodes pixels."""
    try:
        from PIL import Image
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "`probe: image` needs Pillow; install the `documents` extra: "
            "pip install 'edgy-dag-tools[documents]'"
        ) from e
    try:
        with Image.open(io.BytesIO(data)) as im:
            w, h = im.size
            return (im.format or None), int(w), int(h), False
    except Exception:  # noqa: BLE001 -- Pillow raises many unrelated types for a bad image; all mean unreadable
        return None, None, None, True


def require_pillow() -> Any:
    try:
        import PIL  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "`probe: image` needs Pillow; install the `documents` extra: "
            "pip install 'edgy-dag-tools[documents]'"
        ) from e
    return PIL
