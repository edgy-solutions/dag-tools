import sys
from pathlib import Path

here = Path(__file__).parent
repo_root = str(here.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.append(repo_root)
# `python_file` loading does not put the file's own directory on the path, and
# the derived asset lives next door.
if str(here) not in sys.path:
    sys.path.insert(0, str(here))
# `build_component_defs` imports the components directory as a module, so the
# package that CONTAINS this one has to be importable too -- which it is not
# when `dagster dev` runs from inside it.
if str(here.parent) not in sys.path:
    sys.path.insert(0, str(here.parent))

from dagster import Definitions
from dagster.components import build_component_defs

from derived import party_exposure

COMPONENTS_DIR = here / "components"

# Three components now, and only one of them is new.
#
#   * TaggedWarehouseComponent (step 2) contributes the resource under the key
#     `warehouse`. Everything that touches the database reads through it, so
#     every read lands in query history attributed to the asset that made it.
#   * WarehouseSourceComponent (step 4a) observes one source dataset and
#     publishes the labels a human decided on. A schema observation is still
#     a read.
#   * ClassificationPropagationComponent (step 4b/4c) builds a sensor and no
#     assets. It watches the derived asset below and labels its columns from
#     what they read -- nobody decides those labels; they are a consequence.
#
# `party_exposure` is NOT a dag-tools asset. What builds a derived table is
# the deployment's own business, and the point of the propagation component is
# that it needs nothing from the thing that built it beyond the column lineage
# Dagster already carries.
defs = Definitions.merge(
    build_component_defs(COMPONENTS_DIR),
    Definitions(assets=[party_exposure]),
)
