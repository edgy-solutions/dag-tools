import sys
from pathlib import Path

repo_root = str(Path(__file__).parent.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.append(repo_root)

from dagster.components import build_component_defs

COMPONENTS_DIR = Path(__file__).parent / "components"

# Two components, and the point of the example is that only one of them is
# new. TaggedWarehouseComponent contributes a resource under the key
# `warehouse`; ReadAuditComponent -- unchanged from step 1 -- reads its
# history through that key. The seam between them is one method,
# `fetch_rows(statement, parameters)`.
defs = build_component_defs(COMPONENTS_DIR)
