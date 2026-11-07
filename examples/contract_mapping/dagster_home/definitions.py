import sys
from pathlib import Path

repo_root = str(Path(__file__).parent.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.append(repo_root)

from dagster.components import build_component_defs

COMPONENTS_DIR = Path(__file__).parent / "components"

# ContractMappingComponent builds one asset: it reconciles the mapping with
# its contract and emits the dbt model and the contract tests. It moves no
# data, and in this example it writes nothing -- the rendered bytes go on the
# materialization instead.
defs = build_component_defs(COMPONENTS_DIR)
