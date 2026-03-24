import sys
from pathlib import Path

# Tell Python to look in this exact folder for trigger.py
sys.path.append(str(Path(__file__).parent))
# Also ensure the repo root is in path for dag_tools
repo_root = str(Path(__file__).parent.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.append(repo_root)

from dagster import Definitions, load_assets_from_modules
from dagster.components import build_component_defs

import trigger

# FIXED: Define the path to our components as a pathlib.Path object!
COMPONENTS_DIR = Path(__file__).parent / "components"

# Load assets from the custom trigger module
trigger_assets = load_assets_from_modules([trigger])

# Load declarative components (dlt extraction & dbt transformation) and consolidate into Definitions
defs = Definitions.merge(
    build_component_defs(COMPONENTS_DIR),
    Definitions(assets=[*trigger_assets])
)
