"""Resolve the manifest's ``staging_overrides`` pointer into run config.

``QualificationManifest.staging_overrides`` is a **pointer string**
(typically ``s3://bucket/key``), and its docstring says "Phase 2 GraphQL
launches pull this as run_config". Nothing pulled it. Both launchers
stuffed the pointer itself into the run config under a fabricated
``__staging_overrides__`` key, which Dagster rejects outright -- top-level
run config accepts only ``ops`` / ``resources`` / ``execution`` /
``loggers`` -- so EVERY launch with ``--staging-overrides`` set failed at
submission with ``DagsterInvalidConfigError``. The feature had therefore
never worked at all, which is also why no test caught it: the unit tests
asserted on the dict that was built, not on whether Dagster would accept
it.

This module does the pull. Resolution happens ONCE per side in the runner
(next to ``get_asset_launch_info``, for the same reason: it is a
whole-deployment fact, not a per-representative one) and the resolved
document is threaded down to the launcher, which stays pure and does no IO.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse

import yaml

logger = logging.getLogger(__name__)

__all__ = [
    "RUN_CONFIG_TOP_LEVEL_KEYS",
    "StagingOverridesError",
    "StagingOverridesNotResolved",
    "merge_run_config",
    "resolve_staging_overrides",
    "storage_fetch",
]

RUN_CONFIG_TOP_LEVEL_KEYS = frozenset({"ops", "resources", "execution", "loggers"})
"""The only keys Dagster accepts at the top of a run config document.

Validated here so an operator who points ``--staging-overrides`` at the
wrong file gets a message naming the bad keys, instead of a Dagster config
error from inside the submission path.
"""


class StagingOverridesError(Exception):
    """The staging_overrides pointer could not be turned into run config."""


class StagingOverridesNotResolved(StagingOverridesError):
    """A launch was attempted while the pointer was still unresolved.

    Raised by the launchers when the manifest declares ``staging_overrides``
    but the caller passed no resolved document. Deliberately loud: the
    alternative is launching against the wrong (un-overridden) resources and
    recording the result as a qualification data point.
    """


def storage_fetch(storage: Any) -> Callable[[str], bytes]:
    """Return a fetcher backed by an :class:`S3Storage`.

    Reusing the registry's storage is what makes a MinIO deployment work:
    the pointer is read through the same ``endpoint_url`` / ``region`` /
    injected client as every other registry read. The pointer may name a
    DIFFERENT bucket than the registry, which is why this goes through
    ``get_uri`` rather than any of the keyed readers.
    """
    return storage.get_uri


def resolve_staging_overrides(
    pointer: Optional[str],
    *,
    fetch: Optional[Callable[[str], bytes]] = None,
) -> Dict[str, Any]:
    """Fetch and parse ``pointer``, returning a Dagster run config dict.

    Args:
      pointer: the manifest's ``staging_overrides`` value. ``None`` or empty
        yields ``{}`` -- not an error, just "no overrides".
      fetch: reads an ``s3://`` URI and returns bytes; supply
        :func:`storage_fetch` of the registry's storage. Local paths and
        ``file://`` URIs are read from disk and need no fetcher, which is
        what lets an operator iterate on the document before uploading it.

    Raises:
      StagingOverridesError: unreachable, unparseable, not a mapping, or
        carrying keys Dagster would reject.
    """
    if not pointer:
        return {}

    raw = _read_pointer(pointer, fetch=fetch)

    try:
        # YAML, which also covers JSON -- JSON is a YAML subset, so one
        # parser serves both without sniffing the extension.
        doc = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise StagingOverridesError(
            f"staging_overrides at {pointer!r} is not valid YAML/JSON: {e}"
        ) from e

    if doc is None:
        # An empty document is a legitimate "no overrides" -- an operator
        # commenting out a block should not fail the qualification.
        logger.warning(
            "staging_overrides at %s parsed as empty; no overrides applied", pointer
        )
        return {}

    if not isinstance(doc, dict):
        raise StagingOverridesError(
            f"staging_overrides at {pointer!r} must be a mapping, "
            f"got {type(doc).__name__}"
        )

    unknown = sorted(set(doc) - RUN_CONFIG_TOP_LEVEL_KEYS)
    if unknown:
        raise StagingOverridesError(
            f"staging_overrides at {pointer!r} has top-level keys Dagster will "
            f"reject: {unknown}. Expected a run config document with keys from "
            f"{sorted(RUN_CONFIG_TOP_LEVEL_KEYS)}."
        )

    return doc


def merge_run_config(
    *,
    overrides: Optional[Dict[str, Any]],
    explicit: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Deep-merge the staging overrides under an explicit per-rep config.

    ``explicit`` wins at every leaf, preserving the layering the launcher
    always documented ("staging overrides are deliberately layered first so
    per-rep config can shadow them"). The merge must be RECURSIVE: run
    config nests as ``resources -> <key> -> config -> ...``, so a shallow
    merge of a per-rep ``resources`` block would silently drop every staging
    resource the operator did not happen to restate.
    """
    return _deep_merge(overrides or {}, explicit or {})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _read_pointer(pointer: str, *, fetch: Optional[Callable[[str], bytes]]) -> bytes:
    parsed = urlparse(pointer)

    if parsed.scheme == "s3":
        if fetch is None:
            raise StagingOverridesError(
                f"staging_overrides {pointer!r} is an s3:// URI but no fetcher "
                "was supplied; pass storage_fetch(registry.storage)"
            )
        try:
            return fetch(pointer)
        except StagingOverridesError:
            raise
        except Exception as e:  # noqa: BLE001 -- re-raised as a typed error
            raise StagingOverridesError(
                f"staging_overrides at {pointer!r} could not be read: {e}"
            ) from e

    # A one-character scheme is a Windows drive letter (C:\...), not a URL
    # scheme -- urlparse cannot tell the difference.
    if parsed.scheme in ("", "file") or len(parsed.scheme) == 1:
        path = Path(parsed.path if parsed.scheme == "file" else pointer)
        try:
            return path.read_bytes()
        except OSError as e:
            raise StagingOverridesError(
                f"staging_overrides at {pointer!r} could not be read: {e}"
            ) from e

    raise StagingOverridesError(
        f"staging_overrides {pointer!r} has unsupported scheme {parsed.scheme!r}; "
        "expected s3://, file://, or a local path"
    )


def _deep_merge(base: Dict[str, Any], top: Dict[str, Any]) -> Dict[str, Any]:
    """Return ``base`` with ``top`` layered over it; ``top`` wins at leaves."""
    merged = dict(base)
    for key, value in top.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge(existing, value)
        else:
            merged[key] = value
    return merged
