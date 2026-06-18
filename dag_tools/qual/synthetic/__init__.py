"""Phase 2 Q5 — synthetic probe code generation.

For each SYNTHETIC_REQUIRED equivalence class, generate a self-contained
Dagster module that imports the real IO manager FQN, defines a trivial
upstream + downstream asset pair (deterministic dict payload, identity
assertion after the round-trip), and exposes a ``defs = Definitions(...)``
the operator drops into the ``dag-tools-probes`` user-code location.

Public:
  * :func:`generate_bundle` — read class matrix, emit ProbeManifest + sources.
  * :func:`publish_bundle` — push sources + manifest to the registry,
    manifest last so partial publishes never become visible.
  * :func:`write_local_bundle` — write the operator's local copy at
    ``~/.dagtools/quals/<qual_id>/probes/``.
  * :class:`ProbeManifest` / :class:`ProbeModule` / :class:`ProbeStatus`
    — persisted schemas.
"""
from .bundle import (
    GeneratedBundle,
    default_local_probes_dir,
    generate_bundle,
    publish_bundle,
    write_local_bundle,
)
from .generator import generate_probe_module, generate_probe_source
from .schema import (
    SCHEMA_VERSION_PROBE_MANIFEST,
    ProbeManifest,
    ProbeModule,
    ProbeStatus,
)

__all__ = [
    "SCHEMA_VERSION_PROBE_MANIFEST",
    # schemas
    "ProbeManifest",
    "ProbeModule",
    "ProbeStatus",
    # generator
    "generate_probe_module",
    "generate_probe_source",
    # bundle
    "GeneratedBundle",
    "generate_bundle",
    "publish_bundle",
    "write_local_bundle",
    "default_local_probes_dir",
]
