from pathlib import Path
from dagster import Definitions
from dagster.components import build_component_defs

# In Dagster 1.10+, if you have registered components in pyproject.toml
# you can natively discover and load ALL YAML files inside the components/ directory.
#
# No manual yaml.load() or parsing is required anymore. The python code inside
# `dag_tools.components.dlt_pipeline.component.py` handles the translation automatically!

defs = build_component_defs(Path(__file__).parent / "components")
