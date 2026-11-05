import sys
from pathlib import Path

repo_root = str(Path(__file__).parent.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.append(repo_root)

from dagster.components import build_component_defs

COMPONENTS_DIR = Path(__file__).parent / "components"

# ReadAuditComponent builds one read-only asset: it rolls a window of query
# history into objects-per-asset, assets-per-object, and the objects no
# asset explains. Nothing is granted, labelled or changed by it.
defs = build_component_defs(COMPONENTS_DIR)
