import sys
from pathlib import Path

repo_root = str(Path(__file__).parent.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.append(repo_root)

from dagster.components import build_component_defs

COMPONENTS_DIR = Path(__file__).parent / "components"

# OtelApiSyncComponent generates the dlt ClickHouse extraction asset and
# the *_dispatch asset that renders one ordered call plan per execution
# group and hands each to its group-keyed Restate object.
defs = build_component_defs(COMPONENTS_DIR)
