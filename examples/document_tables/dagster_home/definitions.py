import sys
from pathlib import Path

repo_root = str(Path(__file__).parent.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.append(repo_root)

from dagster.components import build_component_defs

COMPONENTS_DIR = Path(__file__).parent / "components"

# DocumentTablesComponent builds one multi-asset (a Lance table per mapped
# entity, plus _rejects), partitioned by bundle; an ingest job; a discovery
# sensor; and a maintenance job. Start the sensor in the UI, or add a
# partition per bundle by hand and materialize it.
defs = build_component_defs(COMPONENTS_DIR)
