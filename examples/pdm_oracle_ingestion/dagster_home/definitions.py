import sys
from pathlib import Path

repo_root = str(Path(__file__).parent.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.append(repo_root)

from dagster.components import build_component_defs

COMPONENTS_DIR = Path(__file__).parent / "components"

# The RestateDltSyncComponent generates both the dlt extraction asset and
# the *_ack_dispatch asset that fans out PKs to the Restate ingress — no
# separate trigger asset is needed for this example.
defs = build_component_defs(COMPONENTS_DIR)
