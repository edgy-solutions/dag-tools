"""Names of the ``dag-tools-probes`` code location, in one dependency-free place.

Both runners launch against this location and the probe status check looks
it up, so the names live in a leaf module: ``dag_tools.qual.probes`` and
``dag_tools.qual.runs`` import each other's submodules, and a constant
defined in either would be read half-initialized from the other.
"""

PROBES_LOCATION_NAME = "dag-tools-probes"
"""Contract with :mod:`dag_tools.probes_location` — the operator must
deploy the location under this name in their test deployment's
``workspace.yaml``."""

PROBES_REPOSITORY_NAME = "__repository__"
PROBES_JOB_NAME = "__ASSET_JOB"
