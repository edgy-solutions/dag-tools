"""S3-compatible storage client + high-level InventoryRegistry.

Two layers:

  * :class:`S3Storage` — a thin, MinIO-aware wrapper around boto3 with
    explicit *immutable* and *mutable* put modes. The immutable mode
    refuses to overwrite (HEAD-then-PUT); the mutable mode is reserved
    for ``latest.json`` and a few qualification pointers.

  * :class:`InventoryRegistry` — composes :class:`S3Storage` with the
    layout helpers in :mod:`.layout`. Knows nothing about S3 keys
    directly; the layout module owns the strings. This is what the
    survey publishes through and the qualification CLI reads from.

Credentials come from the standard boto3 chain (env vars, ``~/.aws``,
EC2/EKS instance metadata). The only MinIO-specific knob is
``endpoint_url`` — pass it via :class:`StorageSettings`.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field

from . import layout


logger = logging.getLogger(__name__)


class StorageSettings(BaseModel):
    """Configuration for the registry client.

    ``bucket`` is the parsed bucket name (use ``layout.parse_registry_uri``
    on the raw ``s3://...`` URI from the CLI). ``endpoint_url`` is the
    MinIO endpoint; leave empty for real AWS S3.
    """

    bucket: str
    endpoint_url: Optional[str] = Field(default=None)
    region: str = Field(default="us-east-1")


class ImmutableKeyExists(Exception):
    """Raised when an immutable put would overwrite an existing key."""


class S3Storage:
    """Thin S3 wrapper with explicit immutability semantics."""

    def __init__(self, settings: StorageSettings, s3_client: Optional[Any] = None):
        self.settings = settings
        # Pull s3 client out for dependency injection in tests; moto
        # supplies a real boto3 client via mock_aws otherwise.
        self._s3 = s3_client or boto3.client(
            "s3",
            endpoint_url=settings.endpoint_url,
            region_name=settings.region,
            config=BotoConfig(signature_version="s3v4"),
        )

    # --- existence --------------------------------------------------------

    def exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self.settings.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    # --- puts --------------------------------------------------------------

    def put_immutable(self, key: str, body: bytes, content_type: str = "application/json") -> None:
        """Write an immutable key. Raises :class:`ImmutableKeyExists` if the
        key is already present. Use this for every per-build / per-qual
        artifact — only ``latest.json`` (and a few qualification pointers)
        ever overwrite.
        """
        if self.exists(key):
            raise ImmutableKeyExists(
                f"refusing to overwrite immutable key {key!r} "
                f"(pass allow_overwrite=True to put_mutable to override)"
            )
        self._put(key, body, content_type)

    def put_mutable(self, key: str, body: bytes, content_type: str = "application/json") -> None:
        """Overwrite-OK write. Reserve this for ``latest.json`` and the
        equivalent pointer keys."""
        self._put(key, body, content_type)

    def _put(self, key: str, body: bytes, content_type: str) -> None:
        self._s3.put_object(
            Bucket=self.settings.bucket, Key=key, Body=body, ContentType=content_type
        )

    # --- gets / lists -----------------------------------------------------

    def get(self, key: str) -> bytes:
        resp = self._s3.get_object(Bucket=self.settings.bucket, Key=key)
        return resp["Body"].read()

    def get_optional(self, key: str) -> Optional[bytes]:
        try:
            return self.get(key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
                return None
            raise

    def list_prefix(self, prefix: str, delimiter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Returns a list of object summaries. When ``delimiter`` is set
        (typically ``"/"``) the result also includes common-prefix entries
        (folders) under a ``CommonPrefixes`` aggregation — but for the
        common "list repos" / "list builds" cases use :meth:`list_subdirs`
        directly."""
        paginator = self._s3.get_paginator("list_objects_v2")
        kwargs: Dict[str, Any] = {"Bucket": self.settings.bucket, "Prefix": prefix}
        if delimiter:
            kwargs["Delimiter"] = delimiter
        out: List[Dict[str, Any]] = []
        for page in paginator.paginate(**kwargs):
            for entry in page.get("Contents", []) or []:
                out.append(entry)
        return out

    def list_subdirs(self, prefix: str) -> List[str]:
        """Return immediate sub-prefix names under ``prefix`` (treats S3
        like a folder tree using ``/`` as delimiter). For ``inventory/``
        this returns the list of repo names."""
        paginator = self._s3.get_paginator("list_objects_v2")
        out: List[str] = []
        for page in paginator.paginate(
            Bucket=self.settings.bucket, Prefix=prefix, Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes", []) or []:
                sub = cp["Prefix"][len(prefix):].rstrip("/")
                if sub:
                    out.append(sub)
        return out


# ---------------------------------------------------------------------------
# High-level InventoryRegistry
# ---------------------------------------------------------------------------


class LatestPointer(BaseModel):
    """The minimum content of ``inventory/<repo>/latest.json`` — just enough
    to resolve back to the per-build prefix without listing it.

    Additive evolution rules (mirroring AssetRecord): only add Optional
    fields; never rename or remove.
    """

    git_sha: str
    build_id: Optional[str] = None
    timestamp: datetime
    dagster_version: Optional[str] = None
    dagtools_version: Optional[str] = None


class BuildMeta(BaseModel):
    """The content of ``meta.json``. Same evolution rules."""

    repo: str
    git_sha: str
    build_id: Optional[str] = None
    timestamp: datetime
    dagster_version: Optional[str] = None
    dagtools_version: Optional[str] = None
    inventory_schema_version: int = Field(default=1)


class InventoryRegistry:
    """High-level facade over :class:`S3Storage` + :mod:`.layout`.

    The CI survey calls :meth:`publish_build`; the operator-side CLI
    calls :meth:`list_repos` and :meth:`read_latest_pointer`.
    """

    def __init__(self, storage: S3Storage):
        self.storage = storage

    @classmethod
    def from_settings(cls, settings: StorageSettings) -> "InventoryRegistry":
        return cls(S3Storage(settings))

    # --- write path -------------------------------------------------------

    def publish_build(
        self,
        repo: str,
        git_sha: str,
        artifacts: Dict[str, bytes],
        meta: BuildMeta,
        allow_overwrite: bool = False,
    ) -> LatestPointer:
        """Write all per-build artifacts under ``inventory/<repo>/<git_sha>/``,
        **then** update ``inventory/<repo>/latest.json`` last. Returns the
        updated ``LatestPointer``.

        ``artifacts`` maps filename → JSON-serialized bytes. ``meta`` is
        always written to ``meta.json`` (even if also present in
        ``artifacts``; the explicit param wins).

        Per the recipe, every per-SHA artifact is immutable: re-publishing
        the same SHA raises :class:`ImmutableKeyExists` unless
        ``allow_overwrite=True``. The pointer write is always mutable.
        """
        meta_key = layout.inventory_artifact_key(repo, git_sha, layout.META_FILE)
        meta_body = meta.model_dump_json().encode("utf-8")

        # 1. Write meta.json (immutable).
        self._put(meta_key, meta_body, allow_overwrite=allow_overwrite)

        # 2. Write all other artifacts (immutable).
        for filename, body in artifacts.items():
            if filename == layout.META_FILE:
                continue  # meta is owned by the explicit param above
            key = layout.inventory_artifact_key(repo, git_sha, filename)
            self._put(key, body, allow_overwrite=allow_overwrite)

        # 3. Update the latest.json pointer LAST (mutable).
        pointer = LatestPointer(
            git_sha=git_sha,
            build_id=meta.build_id,
            timestamp=meta.timestamp,
            dagster_version=meta.dagster_version,
            dagtools_version=meta.dagtools_version,
        )
        self.storage.put_mutable(
            layout.latest_pointer_key(repo),
            pointer.model_dump_json().encode("utf-8"),
        )
        return pointer

    def _put(self, key: str, body: bytes, *, allow_overwrite: bool) -> None:
        if allow_overwrite:
            self.storage.put_mutable(key, body)
        else:
            self.storage.put_immutable(key, body)

    # --- read path --------------------------------------------------------

    def list_repos(self) -> List[str]:
        """List of repo names present under ``inventory/``."""
        return sorted(self.storage.list_subdirs(layout.INVENTORY_PREFIX + "/"))

    def read_latest_pointer(self, repo: str) -> Optional[LatestPointer]:
        """Return the parsed ``latest.json`` for a repo, or None if missing."""
        body = self.storage.get_optional(layout.latest_pointer_key(repo))
        if body is None:
            return None
        return LatestPointer.model_validate_json(body)

    def read_build_artifact(
        self, repo: str, git_sha: str, filename: str
    ) -> Optional[bytes]:
        """Read one per-build artifact (e.g. ``assets.json``) as raw bytes,
        or None if absent."""
        return self.storage.get_optional(
            layout.inventory_artifact_key(repo, git_sha, filename)
        )

    def read_build_json(
        self, repo: str, git_sha: str, filename: str
    ) -> Optional[Any]:
        """Read + JSON-decode one per-build artifact, or None if absent."""
        body = self.read_build_artifact(repo, git_sha, filename)
        if body is None:
            return None
        return json.loads(body)

    # --- qualification manifest --------------------------------------------

    def put_qualification_manifest(
        self, qual_id: str, body: bytes, *, allow_overwrite: bool = False
    ) -> None:
        """Write ``qualifications/<qual_id>/manifest.yaml``.

        Immutable by default — re-running ``dagtools qual init`` with the
        same ``qual_id`` raises :class:`ImmutableKeyExists`. ``allow_overwrite``
        is for the explicit "I really do want to redo this qualification"
        path.
        """
        key = layout.qualification_manifest_key(qual_id)
        if allow_overwrite:
            self.storage.put_mutable(key, body, content_type="application/yaml")
        else:
            self.storage.put_immutable(key, body, content_type="application/yaml")

    def read_qualification_manifest(self, qual_id: str) -> Optional[bytes]:
        """Read ``qualifications/<qual_id>/manifest.yaml`` as raw bytes, or
        None if absent. Callers parse with :func:`yaml.safe_load`."""
        return self.storage.get_optional(layout.qualification_manifest_key(qual_id))

    def list_qualifications(self) -> List[str]:
        """List qual_ids present under ``qualifications/``."""
        return sorted(self.storage.list_subdirs(layout.QUALIFICATIONS_PREFIX + "/"))

    def put_qualification_classes(
        self,
        qual_id: str,
        json_body: bytes,
        markdown_body: bytes,
        *,
        allow_overwrite: bool = False,
    ) -> None:
        """Write the Q1 equivalence-class artifacts atomically.

        Both the machine-readable JSON and the human-readable Markdown go
        under ``qualifications/<qual_id>/classes/``. Immutable by default
        — re-running ``dagtools qual classes`` raises
        :class:`ImmutableKeyExists` unless ``allow_overwrite=True``. The
        markdown write goes second so a failure on the JSON write leaves
        no inconsistent state.
        """
        json_key = layout.qualification_classes_key(qual_id)
        md_key = layout.qualification_classes_md_key(qual_id)
        if allow_overwrite:
            self.storage.put_mutable(json_key, json_body, content_type="application/json")
            self.storage.put_mutable(md_key, markdown_body, content_type="text/markdown")
        else:
            self.storage.put_immutable(json_key, json_body, content_type="application/json")
            self.storage.put_immutable(md_key, markdown_body, content_type="text/markdown")

    def read_qualification_classes_json(self, qual_id: str) -> Optional[bytes]:
        """Read ``qualifications/<qual_id>/classes/equivalence_classes.json``
        as raw bytes, or None if absent."""
        return self.storage.get_optional(layout.qualification_classes_key(qual_id))

    def read_qualification_classes_md(self, qual_id: str) -> Optional[bytes]:
        return self.storage.get_optional(layout.qualification_classes_md_key(qual_id))

    # --- per-side state, run records, summary (Q2 / Q4) -------------------

    def put_side_state(self, qual_id: str, side: str, body: bytes) -> None:
        """Write ``qualifications/<qual_id>/<side>/state.json``.

        Mutable on purpose — Q2/Q4 update this in-place as representatives
        transition between pending/launched/passed/failed. The
        operator-local copy at ``~/.dagtools/quals/<qual_id>/<side>-state.json``
        mirrors it; either side can recover from the other if a desktop
        dies mid-run.
        """
        self.storage.put_mutable(
            layout.qualification_side_state_key(qual_id, side),
            body,
            content_type="application/json",
        )

    def read_side_state(self, qual_id: str, side: str) -> Optional[bytes]:
        return self.storage.get_optional(
            layout.qualification_side_state_key(qual_id, side)
        )

    def put_run_record(
        self,
        qual_id: str,
        side: str,
        class_hash: str,
        run_id: str,
        body: bytes,
        *,
        allow_overwrite: bool = False,
    ) -> None:
        """Persist one run's full record under
        ``qualifications/<qual_id>/<side>/runs/<class_hash>/<run_id>.json``.

        Immutable by default — re-runs (e.g. a re-launched failed rep)
        get a different ``run_id`` and so don't collide. ``allow_overwrite``
        exists for operator-driven cleanups."""
        key = layout.qualification_side_run_key(qual_id, side, class_hash, run_id)
        if allow_overwrite:
            self.storage.put_mutable(key, body, content_type="application/json")
        else:
            self.storage.put_immutable(key, body, content_type="application/json")

    def read_run_record(
        self, qual_id: str, side: str, class_hash: str, run_id: str
    ) -> Optional[bytes]:
        return self.storage.get_optional(
            layout.qualification_side_run_key(qual_id, side, class_hash, run_id)
        )

    def put_side_summary(
        self,
        qual_id: str,
        side: str,
        body: bytes,
        *,
        allow_overwrite: bool = False,
    ) -> None:
        """Write ``qualifications/<qual_id>/<side>/summary.json`` (immutable
        by default)."""
        key = layout.qualification_side_summary_key(qual_id, side)
        if allow_overwrite:
            self.storage.put_mutable(key, body, content_type="application/json")
        else:
            self.storage.put_immutable(key, body, content_type="application/json")

    def read_side_summary(self, qual_id: str, side: str) -> Optional[bytes]:
        return self.storage.get_optional(
            layout.qualification_side_summary_key(qual_id, side)
        )

    def put_side_preflight(
        self,
        qual_id: str,
        side: str,
        body: bytes,
        *,
        allow_overwrite: bool = False,
    ) -> None:
        """Write ``qualifications/<qual_id>/<side>/preflight.json``.

        Immutable by default — re-running preflight after fixing a failure
        needs ``--allow-overwrite``."""
        key = layout.qualification_side_preflight_key(qual_id, side)
        if allow_overwrite:
            self.storage.put_mutable(key, body, content_type="application/json")
        else:
            self.storage.put_immutable(key, body, content_type="application/json")

    def read_side_preflight(self, qual_id: str, side: str) -> Optional[bytes]:
        return self.storage.get_optional(
            layout.qualification_side_preflight_key(qual_id, side)
        )


def _utcnow() -> datetime:
    """UTC ``datetime.now`` factored out so tests can monkeypatch it."""
    return datetime.now(tz=timezone.utc)
