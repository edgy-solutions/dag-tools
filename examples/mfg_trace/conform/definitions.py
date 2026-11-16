import sys
from pathlib import Path

# The parent of this package has to be importable: build_component_defs
# imports the components directory as a module of it.
parent = str(Path(__file__).parent.parent)
if parent not in sys.path:
    sys.path.append(parent)

from dagster.components import build_component_defs

# Two ContractMappingComponent instances, one per raw table. Each moves no
# data; each reconciles its mapping with its conformed contract at load.
defs = build_component_defs(Path(__file__).parent / "components")
