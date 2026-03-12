import os
from dagster import Definitions, load_assets_from_modules
from dagster.components import build_component_defs
from . import trigger

# Define the path to our declarative components
COMPONENTS_DIR = os.path.join(os.path.dirname(__file__), "components")

# Load assets from the custom trigger module
trigger_assets = load_assets_from_modules([trigger])

# Load declarative components (dlt extraction & dbt transformation)
component_defs = build_component_defs(COMPONENTS_DIR)

# Consolidate into Definitions
defs = Definitions.merge(
    component_defs,
    Definitions(assets=[*trigger_assets])
)
