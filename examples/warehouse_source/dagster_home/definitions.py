import sys
from pathlib import Path

repo_root = str(Path(__file__).parent.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.append(repo_root)

from dagster.components import build_component_defs

COMPONENTS_DIR = Path(__file__).parent / "components"

# Two components, and again only one of them is new. The step-2
# TaggedWarehouseComponent contributes the resource under the key `warehouse`;
# WarehouseSourceComponent observes one source dataset *through* it, so the
# observation lands in query history attributed to this asset like every other
# read. A schema observation is still a read.
defs = build_component_defs(COMPONENTS_DIR)
