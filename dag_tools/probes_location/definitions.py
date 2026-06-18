"""Top-level ``defs`` for the ``dag-tools-probes`` code location.

The operator's test deployment imports this module via its workspace
config:

.. code-block:: yaml

    load_from:
      - python_module:
          module_name: dag_tools.probes_location.definitions
          location_name: dag-tools-probes

Set ``DAGTOOLS_PROBES_DIR`` on the deployment to point at the bundle
directory ``dagtools qual synthetic`` produced — e.g.
``~/.dagtools/quals/<qual_id>/probes/``.

When no probes are deployed (env var unset, directory empty), the
location loads as an empty ``Definitions`` — operators can deploy the
location once and add probe bundles over time without re-deploying.
"""
from __future__ import annotations

import json
import logging

from dagster import Definitions

from .loader import load_probes_from_dir


logger = logging.getLogger(__name__)


def _build_definitions() -> Definitions:
    """Compose the merged ``Definitions``.

    Returns an empty :class:`Definitions` when no probes are deployed
    so the code location's load never fails.
    """
    report = load_probes_from_dir()

    if report.failures:
        # Surfacing in the location load is the most operator-visible
        # signal we have — Dagster's load report shows the warning log.
        logger.warning(
            "dag-tools-probes: %d probe(s) failed to load. Details: %s",
            len(report.failures),
            json.dumps(
                [{"class_hash": f.class_hash, "error": f.error[:500]}
                 for f in report.failures],
                indent=2,
            ),
        )

    loaded_defs = [outcome.defs for outcome in report.loaded]
    if not loaded_defs:
        return Definitions()

    if len(loaded_defs) == 1:
        return loaded_defs[0]

    return Definitions.merge(*loaded_defs)


defs = _build_definitions()
