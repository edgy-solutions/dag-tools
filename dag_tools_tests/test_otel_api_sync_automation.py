"""Scheduling the OTel -> API sync from YAML.

The example README's post_processing block is the supported way to put the
component on a cron with no Python. These tests load the real example with
that block appended (read out of the README, so the docs cannot drift from
what is tested) and run Dagster's own automation evaluator over a window of
sensor ticks.

`AutomationCondition.on_cron` looks like the obvious choice and silently
never fires here: it waits for every upstream to update since the tick, and
the extraction asset's upstream is the external ClickHouse table, which
nothing ever materializes. `test_on_cron_never_fires_for_this_component`
pins that, so nobody "simplifies" the README back to it.
"""

import datetime as dt
import re
import shutil
from pathlib import Path
from typing import List, Set, Tuple

import dagster as dg
import pytest
from dagster.components import build_component_defs

EXAMPLE = Path(__file__).parent.parent / "examples" / "otel_to_api"
EXTRACTION = "dlt/telemetry/otel_staging/execution_spans"
DISPATCH = "ci_results_dispatch"
SOURCE = "otel/otel/execution_spans"

ON_CRON_BLOCK = """
post_processing:
  assets:
    - target: "group:otel_api_sync"
      attributes:
        automation_condition: "{{ dg.AutomationCondition.on_cron('*/5 * * * *') }}"
"""


@pytest.fixture(autouse=True)
def _example_env(monkeypatch):
    for var, value in {
        "RESTATE_INGRESS_URL": "http://restate.test:8080",
        "CLICKHOUSE_HOST": "clickhouse.test",
        "CLICKHOUSE_PASSWORD": "x",
        "POSTGRES_HOST": "postgres.test",
        "POSTGRES_USER": "x",
        "POSTGRES_PASSWORD": "x",
        "TARGET_API_BASE_URL": "https://api.test",
    }.items():
        monkeypatch.setenv(var, value)


def _readme_post_processing_block() -> str:
    readme = (EXAMPLE / "README.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```yaml\n(.*?)```", readme, flags=re.DOTALL)
    matches = [b for b in blocks if b.lstrip().startswith("post_processing:")]
    assert len(matches) == 1, "README must carry exactly one post_processing block"
    return matches[0]


def _defs_with(tmp_path: Path, monkeypatch, post_processing: str) -> dg.Definitions:
    # build_component_defs imports the folder as `<tmp dir>.components`,
    # exactly as the example's definitions.py does from dagster_home.
    monkeypatch.syspath_prepend(str(tmp_path.parent))
    components = tmp_path / "components"
    shutil.copytree(
        EXAMPLE / "dagster_home" / "components",
        components,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    yaml_path = components / "otel_sync" / "component.yaml"
    yaml_path.write_text(
        yaml_path.read_text(encoding="utf-8").rstrip() + "\n" + post_processing,
        encoding="utf-8",
    )
    return build_component_defs(components)


def _requested_per_tick(defs: dg.Definitions) -> List[Tuple[str, Set[str]]]:
    """Evaluate at the sensor's 30s cadence from 12:01 to 12:16 UTC."""
    instance = dg.DagsterInstance.ephemeral()
    when = dt.datetime(2026, 11, 4, 12, 1, tzinfo=dt.timezone.utc)
    cursor = None
    requested = []
    for _ in range(32):
        result = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, cursor=cursor, evaluation_time=when
        )
        cursor = result.cursor
        keys = {r.key.to_user_string() for r in result.results if r.true_subset.size}
        if keys:
            requested.append((when.strftime("%H:%M"), keys))
        when += dt.timedelta(seconds=30)
    return requested


def test_readme_condition_requests_extraction_and_dispatch_on_every_tick(tmp_path, monkeypatch):
    defs = _defs_with(tmp_path, monkeypatch, _readme_post_processing_block())

    assert _requested_per_tick(defs) == [
        ("12:05", {EXTRACTION, DISPATCH}),
        ("12:10", {EXTRACTION, DISPATCH}),
        ("12:15", {EXTRACTION, DISPATCH}),
    ]


def test_on_cron_never_fires_for_this_component(tmp_path, monkeypatch):
    """on_cron waits on the external ClickHouse source, which never updates."""
    defs = _defs_with(tmp_path, monkeypatch, ON_CRON_BLOCK)

    graph = defs.resolve_asset_graph()
    extraction = graph.get(dg.AssetKey.from_user_string(EXTRACTION))
    assert dg.AssetKey.from_user_string(SOURCE) in extraction.parent_keys
    assert not graph.get(dg.AssetKey.from_user_string(SOURCE)).is_materializable

    assert _requested_per_tick(defs) == []


def test_yaml_automation_gets_the_default_sensor_which_starts_stopped(tmp_path, monkeypatch):
    """No Python sensor is needed, but the default one must be switched on.

    Dagster builds `default_automation_condition_sensor` from the final asset
    graph, after post_processing, so YAML-attached conditions are covered.
    It is created STOPPED, which the README tells operators to change.
    """
    defs = _defs_with(tmp_path, monkeypatch, _readme_post_processing_block())

    sensors = {s.name: s for s in defs.get_repository_def().sensor_defs}
    sensor = sensors["default_automation_condition_sensor"]
    assert isinstance(sensor, dg.AutomationConditionSensorDefinition)
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
