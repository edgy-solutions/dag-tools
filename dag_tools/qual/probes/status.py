"""Cross-reference the probe manifest with the test deployment's
loaded probe assets.

Operator workflow this closes the loop on:

  1. ``dagtools qual synthetic --id <q>`` writes the bundle.
  2. Operator deploys ``dag_tools.probes_location.definitions`` to the
     test deployment + sets ``DAGTOOLS_PROBES_DIR``.
  3. ``dagtools qual probes status --id <q>`` runs THIS module to verify
     the deployment actually loaded every probe the manifest expects.

What we check per probe:
  * Both ``<module_name>_upstream`` AND ``<module_name>_downstream`` are
    present in the dag-tools-probes location.
  * Partial loads (one of two assets missing) surface as ``partially_loaded``
    — typically a probe-side import error or a stale `<class_hash>.py`
    file with edits that broke one asset definition.

We also flag ``unexpected`` asset keys: probe-shaped keys
(``probe_*_(upstream|downstream)``) present in the location but absent
from the current manifest. Usually means a prior bundle's stale files
weren't cleaned up; operator decides whether that matters.

The status is computed against the **dag-tools-probes location only**,
hardcoded to match the runner's contract. Don't make it configurable
without updating the runner too.
"""
from __future__ import annotations

import logging
import re
from typing import Callable, Dict, List, Optional, Set, Tuple

import yaml
from pydantic import BaseModel, ConfigDict, Field

from ..graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    resolve_auth_token,
)
from ..qualify import QualificationManifest
from ..registry import InventoryRegistry
from ..synthetic import ProbeManifest, ProbeModule
from .runner import PROBES_LOCATION_NAME


logger = logging.getLogger(__name__)


PROBE_ASSET_KEY_RE = re.compile(r"^probe_([0-9a-f]+)_(upstream|downstream)$")


# ---------------------------------------------------------------------------
# Report shapes
# ---------------------------------------------------------------------------


class ProbeAssetCheck(BaseModel):
    """One probe's deploy state on the test deployment."""
    model_config = ConfigDict(extra="ignore")

    class_hash: str
    module_name: str
    upstream_loaded: bool
    downstream_loaded: bool

    @property
    def fully_loaded(self) -> bool:
        return self.upstream_loaded and self.downstream_loaded


class ProbesStatusReport(BaseModel):
    """Cross-reference report. Operator-facing — every field is named for
    what the operator reads, not what the code internally tracks."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=1)
    qual_id: str
    location_name: str

    location_load_status: str
    """Raw load status of the dag-tools-probes location ('LOADED',
    'LOADING', 'ERROR', or 'ABSENT' when the location isn't even
    registered in the deployment's workspace.yaml)."""
    location_error: Optional[str] = None

    expected_class_count: int
    fully_loaded_class_count: int
    fully_loaded_class_hashes: List[str] = Field(default_factory=list)
    partially_loaded: List[ProbeAssetCheck] = Field(default_factory=list)
    missing_class_hashes: List[str] = Field(default_factory=list)
    """Probes in the manifest where NEITHER asset is loaded — the
    operator hasn't redeployed yet, the DAGTOOLS_PROBES_DIR points
    elsewhere, or the location is in ERROR state."""

    unexpected_probe_asset_keys: List[List[str]] = Field(default_factory=list)
    """Asset keys in the location matching the probe naming pattern
    (``probe_<hex>_(upstream|downstream)``) but absent from the current
    manifest — usually stale files from a prior bundle."""

    @property
    def all_loaded(self) -> bool:
        return (
            self.location_load_status == "LOADED"
            and self.fully_loaded_class_count == self.expected_class_count
            and not self.partially_loaded
            and not self.missing_class_hashes
        )


# ---------------------------------------------------------------------------
# Status check
# ---------------------------------------------------------------------------


def check_probes_status(
    qual_id: str,
    *,
    registry: InventoryRegistry,
    client_factory: Optional[Callable[[QualificationManifest], DagsterGraphQLClient]] = None,
) -> ProbesStatusReport:
    """Build a :class:`ProbesStatusReport` by querying the test deployment.

    ``client_factory`` is injectable for tests so the GraphQL transport
    doesn't have to be mocked at the httpx layer.
    """
    manifest = _read_qual_manifest(registry, qual_id)
    probe_manifest = _read_probe_manifest(registry, qual_id)

    factory = client_factory or _default_client_factory
    client = factory(manifest)
    try:
        location_status, location_error = _resolve_location_status(client)
        asset_keys = _safe_get_location_asset_keys(client, location_status)
    finally:
        client.close()

    return _build_report(
        qual_id=qual_id,
        probe_manifest=probe_manifest,
        location_status=location_status,
        location_error=location_error,
        location_asset_keys=asset_keys,
    )


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------


def _read_qual_manifest(registry: InventoryRegistry, qual_id: str) -> QualificationManifest:
    body = registry.read_qualification_manifest(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no qualification manifest at qual_id={qual_id!r}; "
            f"run `dagtools qual init --id {qual_id} ...` first"
        )
    return QualificationManifest.model_validate(yaml.safe_load(body))


def _read_probe_manifest(registry: InventoryRegistry, qual_id: str) -> ProbeManifest:
    body = registry.read_probe_manifest(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no probe manifest for qual_id={qual_id!r}; "
            f"run `dagtools qual synthetic --id {qual_id}` first"
        )
    return ProbeManifest.model_validate_json(body)


def _resolve_location_status(
    client: DagsterGraphQLClient,
) -> Tuple[str, Optional[str]]:
    """Look up the dag-tools-probes location's status. Returns
    ``("ABSENT", None)`` when the location isn't in the workspace at all."""
    try:
        locations = client.get_code_locations()
    except DagsterGraphQLError as e:
        logger.warning("_resolve_location_status: workspace query failed: %s", e)
        return ("UNKNOWN", str(e))
    for loc in locations:
        if loc.name == PROBES_LOCATION_NAME:
            return (loc.load_status, loc.error)
    return ("ABSENT", None)


def _safe_get_location_asset_keys(
    client: DagsterGraphQLClient,
    location_status: str,
) -> List[List[str]]:
    """Pull the asset keys; swallow errors when the location isn't in a
    queryable state so the report still renders the deploy-state diagnosis."""
    if location_status in ("ABSENT", "ERROR", "LOADING"):
        return []
    try:
        return client.get_location_asset_keys(PROBES_LOCATION_NAME)
    except DagsterGraphQLError as e:
        logger.warning("_safe_get_location_asset_keys: %s", e)
        return []


def _build_report(
    *,
    qual_id: str,
    probe_manifest: ProbeManifest,
    location_status: str,
    location_error: Optional[str],
    location_asset_keys: List[List[str]],
) -> ProbesStatusReport:
    """Pure cross-reference — no I/O. Easy to unit-test."""
    # Build the expected upstream/downstream asset key sets per probe.
    expected_by_class: Dict[str, ProbeModule] = {
        p.class_hash: p for p in probe_manifest.probes
    }

    # Index the location's asset keys for fast membership checks.
    loaded_asset_set: Set[Tuple[str, ...]] = {
        tuple(ak) for ak in location_asset_keys
    }

    fully_loaded: List[str] = []
    partially_loaded: List[ProbeAssetCheck] = []
    missing: List[str] = []
    for class_hash, probe in expected_by_class.items():
        up_key = (f"{probe.module_name}_upstream",)
        down_key = (f"{probe.module_name}_downstream",)
        up = up_key in loaded_asset_set
        down = down_key in loaded_asset_set
        if up and down:
            fully_loaded.append(class_hash)
        elif not up and not down:
            missing.append(class_hash)
        else:
            partially_loaded.append(ProbeAssetCheck(
                class_hash=class_hash,
                module_name=probe.module_name,
                upstream_loaded=up,
                downstream_loaded=down,
            ))

    # Detect unexpected probe-shaped asset keys (stale files from a
    # prior bundle still in DAGTOOLS_PROBES_DIR).
    expected_shorts: Set[str] = {
        p.module_name.replace("probe_", "", 1)
        for p in probe_manifest.probes
    }
    unexpected: List[List[str]] = []
    for ak in location_asset_keys:
        if len(ak) != 1:
            continue
        m = PROBE_ASSET_KEY_RE.match(ak[0])
        if not m:
            continue
        if m.group(1) not in expected_shorts:
            unexpected.append(ak)

    return ProbesStatusReport(
        qual_id=qual_id,
        location_name=PROBES_LOCATION_NAME,
        location_load_status=location_status,
        location_error=location_error,
        expected_class_count=len(expected_by_class),
        fully_loaded_class_count=len(fully_loaded),
        fully_loaded_class_hashes=sorted(fully_loaded),
        partially_loaded=sorted(partially_loaded, key=lambda c: c.class_hash),
        missing_class_hashes=sorted(missing),
        unexpected_probe_asset_keys=sorted(unexpected),
    )


def _default_client_factory(manifest: QualificationManifest) -> DagsterGraphQLClient:
    if not manifest.deployment.graphql_url:
        raise RuntimeError(
            "manifest.deployment.graphql_url is not set; pass --graphql-url "
            "to `dagtools qual init` or update the manifest"
        )
    token = resolve_auth_token(manifest.deployment.auth)
    return DagsterGraphQLClient(
        endpoint_url=manifest.deployment.graphql_url,
        auth_token=token,
    )
