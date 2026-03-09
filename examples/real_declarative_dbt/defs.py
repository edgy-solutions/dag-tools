from pathlib import Path
from dagster_components import build_component_defs

# Because we used "Option A", Dagster handles the recursive directory crawl!
# This single line of python will recursively scan the folders and find BOTH:
# 1. components/dbt_project_one/component.yaml
# 2. components/dbt_project_two/component.yaml
#
# It will instantly parse them both into two distinct Dagster @multi_assets natively.

defs = build_component_defs(Path(__file__).parent / "components")
