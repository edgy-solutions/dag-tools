"""Pydantic schemas for the Q5 probe generation artifacts.

Two persisted shapes:

  * :class:`ProbeModule` — one generated probe per equivalence class
    (the source plus the metadata that lets Q6 cross-reference back to
    the class matrix).
  * :class:`ProbeManifest` — the index of every probe generated for a
    given qual_id, persisted at
    ``qualifications/<qual_id>/probes/probe_manifest.json``.

Evolution rules mirror the rest of the qualification system: additive-
only, bump ``SCHEMA_VERSION_*`` on every change, readers tolerate
unknown fields.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


SCHEMA_VERSION_PROBE_MANIFEST = 1


class ProbeStatus(str, Enum):
    """Per-probe lifecycle marker.

    Q5 v1 only produces ``GENERATED`` probes — the operator deploys them
    to the ``dag-tools-probes`` user-code location manually, and the
    deployment/run/verdict integration is a follow-up. The status field
    is here so the future flow has a place to land without a schema bump.
    """

    GENERATED = "generated"
    DEPLOYED = "deployed"
    PASSED_BASELINE = "passed_baseline"
    PASSED_CANDIDATE = "passed_candidate"
    FAILED = "failed"


class ProbeModule(BaseModel):
    """One probe module's metadata + persisted source pointer."""
    model_config = ConfigDict(extra="ignore")

    class_hash: str
    """The equivalence class this probe covers."""

    module_name: str
    """Python module name (a valid identifier) used in the generated source
    and in the dag-tools-probes user-code location import graph."""

    source_key: str
    """Registry key where the source .py lives — typically
    ``qualifications/<qual_id>/probes/<class_hash>.py``. Stored here so
    operators (and Q6 later) can locate the source without re-deriving
    the layout."""

    io_manager_class: Optional[str] = None
    """FQN of the IO manager the probe targets. None when the class had no
    detected IO manager — the probe then runs against an in-memory fallback."""

    resource_classes: Dict[str, str] = Field(default_factory=dict)
    integration_libs: List[str] = Field(default_factory=list)

    runnability: Optional[str] = None
    """The class's runnability label (always ``synthetic_required`` in v1
    since only synthetic classes get probes)."""

    notes: List[str] = Field(default_factory=list)
    """Generation caveats — partitions def not synthesized, IO manager not
    importable, etc. Surfaced in the markdown report for operator review."""

    status: ProbeStatus = Field(default=ProbeStatus.GENERATED)


class ProbeManifest(BaseModel):
    """The aggregate index — what was generated, when, for which qual_id."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_PROBE_MANIFEST)
    qual_id: str
    generated_at: datetime

    synthetic_class_count: int
    """Number of SYNTHETIC_REQUIRED classes observed in the class matrix
    at generation time."""

    probes: List[ProbeModule] = Field(default_factory=list)

    skipped_class_hashes: List[str] = Field(default_factory=list)
    """Classes the generator was unable to produce a probe for (e.g. no
    representatives, no IO manager class to bind to). Surfaced so the
    operator sees the gap rather than silently missing coverage."""
