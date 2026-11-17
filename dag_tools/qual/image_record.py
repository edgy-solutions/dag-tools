"""The image-record document: one model, one validator, no storage or network.

``image.json`` names the image digests CI built for a commit. Every writer
and every reader goes through this module, so the rules live with the
document and not with one of its writers: a record fetched by URL is
untrusted input.

Pure by construction: stdlib and pydantic only. Importing it must not drag in
boto3, httpx, dagster or typer (asserted by a subprocess test).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError

KNOWN_SCHEMA_VERSIONS: Tuple[int, ...] = (1,)

_DIGEST_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
_CANONICAL_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")


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


class RecordedImage(BaseModel):
    """One image CI built for a commit: its repository and content digest."""

    repository: str
    digest: str
    """Lowercased ``sha256:<64 hex>``."""


class ImageRecord(BaseModel):
    """The content of ``inventory/<repo>/<sha>/image.json``.

    A per-commit record of what CI built. Two different digests for one
    commit is the condition it exists to expose, so a re-record with a
    different image set is refused. Same additive-only evolution rules.
    """

    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=1)
    repo: str
    git_sha: str
    images: List[RecordedImage] = Field(default_factory=list)
    recorded_at: datetime


class ImageRecordInvalid(ValueError):
    """The document cannot be used. Carries every problem, not the first."""

    def __init__(self, problems: Sequence[str]):
        self.problems: List[str] = list(problems)
        super().__init__(
            "invalid image record: " + "; ".join(self.problems)
            if self.problems else "invalid image record"
        )


class ImageRecordForOtherCommit(ValueError):
    """A valid record that names a different (repo, git_sha) than asked for."""


def _aware(dt: datetime) -> bool:
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


def _problems(record: ImageRecord) -> List[str]:
    out: List[str] = []
    if record.schema_version not in KNOWN_SCHEMA_VERSIONS:
        out.append(
            f"schema_version: found {record.schema_version!r}, "
            f"known versions are {list(KNOWN_SCHEMA_VERSIONS)}"
        )
    if not record.repo or record.repo != record.repo.strip():
        out.append(
            f"repo: {record.repo!r} must be non-empty with no leading or "
            "trailing whitespace"
        )
    if not _SHA_RE.match(record.git_sha):
        out.append(f"git_sha: {record.git_sha!r} must be 7-40 hex characters")
    if not record.images:
        out.append("images: must name at least one image")
    seen: Dict[str, int] = {}
    for n, img in enumerate(record.images):
        if not _CANONICAL_DIGEST_RE.match(img.digest):
            out.append(
                f"images[{n}].digest: {img.digest!r} must be "
                "sha256:<64 lowercase hex>"
            )
        repo = img.repository
        reason: Optional[str] = None
        if repo != repo.strip():
            reason = "has leading or trailing whitespace"
        else:
            parsed = parse_image_reference(repo)
            if parsed.error:
                reason = parsed.error
            elif parsed.tag is not None:
                reason = "carries a tag"
            elif parsed.digest is not None:
                reason = "carries a digest"
            elif parsed.repository != repo:
                reason = "is not a bare repository"
        if reason:
            out.append(f"images[{n}].repository: {repo!r} {reason}")
        key = repo.strip().lower()
        if key in seen:
            out.append(
                f"images[{n}].repository: {repo!r} is listed twice "
                f"(also images[{seen[key]}])"
            )
        else:
            seen[key] = n
    if not _aware(record.recorded_at):
        out.append("recorded_at: must be timezone-aware; a naive time names no instant")
    return out


def validate_image_record(record: ImageRecord) -> None:
    """Raise :class:`ImageRecordInvalid` listing every problem, or return."""
    problems = _problems(record)
    if problems:
        raise ImageRecordInvalid(problems)


def parse_image_record(body: Union[bytes, str]) -> ImageRecord:
    """Parse and validate a document. Only ``ImageRecordInvalid`` escapes."""
    if isinstance(body, (bytes, bytearray)):
        try:
            text = bytes(body).decode("utf-8")
        except UnicodeDecodeError as e:
            raise ImageRecordInvalid([f"document: not valid UTF-8 ({e.reason})"]) from None
    else:
        text = body
    try:
        data = json.loads(text)
    except ValueError as e:
        raise ImageRecordInvalid([f"document: not valid JSON ({e})"]) from None
    if not isinstance(data, dict):
        raise ImageRecordInvalid(
            [f"document: top level must be an object, found {type(data).__name__}"]
        )
    if "schema_version" not in data:
        raise ImageRecordInvalid(["schema_version: missing; the document must state it"])
    version = data["schema_version"]
    if isinstance(version, bool) or version not in KNOWN_SCHEMA_VERSIONS:
        raise ImageRecordInvalid([
            f"schema_version: found {version!r}, "
            f"known versions are {list(KNOWN_SCHEMA_VERSIONS)}"
        ])
    try:
        record = ImageRecord.model_validate(data)
    except ValidationError as e:
        raise ImageRecordInvalid([
            f"{'.'.join(str(p) for p in err['loc']) or 'document'}: {err['msg']}"
            for err in e.errors()
        ]) from None
    validate_image_record(record)
    return record


def check_record_names(record: ImageRecord, repo: str, git_sha: str) -> None:
    """Exact equality on both fields; a short sha is not the full one."""
    if record.repo != repo or record.git_sha != git_sha:
        raise ImageRecordForOtherCommit(
            f"record names repo={record.repo!r} git_sha={record.git_sha!r}, "
            f"not repo={repo!r} git_sha={git_sha!r}"
        )


def build_image_record(
    repo: str, git_sha: str, refs: Sequence[str], *, now: Optional[datetime] = None,
) -> ImageRecord:
    """Build a validated record from image references (tags are ignored)."""
    problems: List[str] = []
    images: List[RecordedImage] = []
    by_repo: Dict[str, str] = {}
    for ref in refs:
        parsed = parse_image_reference(ref)
        if parsed.error or not parsed.digest:
            problems.append(
                f"image {ref!r}: "
                f"{parsed.error or 'no digest (expected repository[:tag]@sha256:<64 hex>)'}"
            )
            continue
        key = parsed.repository.lower()
        if key in by_repo:
            if by_repo[key] != parsed.digest:
                problems.append(
                    f"image {ref!r}: repository {parsed.repository!r} is given "
                    f"two different digests ({by_repo[key]} and {parsed.digest})"
                )
            continue
        by_repo[key] = parsed.digest
        images.append(RecordedImage(repository=parsed.repository, digest=parsed.digest))
    when = now if now is not None else datetime.now(timezone.utc)
    if not _aware(when):
        problems.append("now: must be timezone-aware; a naive time names no instant")
        when = datetime.now(timezone.utc)  # so the rest is still checked
    record = ImageRecord(repo=repo, git_sha=git_sha, images=images, recorded_at=when)
    problems.extend(_problems(record))
    if problems:
        raise ImageRecordInvalid(problems)
    return record


def render_image_record(record: ImageRecord) -> bytes:
    """Canonical bytes: validated, sorted, LF, UTF-8, stable across runs."""
    validate_image_record(record)
    payload = record.model_dump(mode="json")
    payload["images"] = sorted(
        payload["images"], key=lambda i: (i["repository"].lower(), i["digest"])
    )
    return (
        json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
