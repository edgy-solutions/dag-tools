"""Component-level behaviour: dry run, dispatch, and the dispatch ledger.

Exercised through the real Dagster machinery (`materialize`) so the
`Config` wiring, async asset execution and metadata contract are covered
rather than assumed.
"""
import datetime as dt
import os
from typing import Any, Dict, List

import pytest
import yaml
from dagster import DagsterInstance, materialize

# The component imports DagsterDltResource at module scope; see the note
# in test_dlt_item_maps.py for why a collection-time error is worse than
# a skip.
pytest.importorskip("dagster_dlt")

from dag_tools.components.otel_api_sync import component as component_module
from dag_tools.components.otel_api_sync import OtelApiSyncComponent

MAPPING_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "examples",
    "otel_to_api",
    "dagster_home",
    "components",
    "otel_sync",
    "mapping.yaml",
)


def _mapping() -> Dict[str, Any]:
    with open(MAPPING_PATH, "r", encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    # The example gates on a 5-minute quiet period; these tests supply a
    # terminal marker instead, which is the documented short-circuit.
    return document


def _span(span_id: str, group: str = "run-42", entity: str = "e1", item: str = "i1"):
    return {
        "Timestamp": dt.datetime.now(dt.timezone.utc),
        "TraceId": "t1",
        "SpanId": span_id,
        "SpanAttributes": {
            "execution.group_id": group,
            "execution.terminal": "true",
            "execution.outcome": "SUCCESS",
            "entity.id": entity,
            "entity.ids": entity,
            "item.name": item,
            "artifact.path": "file1.txt",
            "ci.branch": "main",
            "ci.commit": "abc1234",
            "metric.NUM_ERROR": "0",
        },
    }


class FakeIngress:
    """Stands in for httpx.AsyncClient against the Restate ingress."""

    def __init__(self, fail: bool = False):
        self.posts: List[Dict[str, Any]] = []
        self.fail = fail

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, url, json=None, headers=None):
        self.posts.append({"url": url, "json": json, "headers": headers or {}})
        if self.fail:
            raise RuntimeError("ingress unreachable")
        return _FakeResponse()


class _FakeResponse:
    status_code = 202

    def raise_for_status(self):
        return None


@pytest.fixture
def ingress(monkeypatch):
    fake = FakeIngress()
    monkeypatch.setattr(component_module.httpx, "AsyncClient", lambda **kwargs: fake)
    return fake


@pytest.fixture
def rows(monkeypatch):
    data = [_span("span-1"), _span("span-2", entity="e2", item="i2")]
    monkeypatch.setattr(
        component_module, "_read_clickhouse_rows", lambda *args, **kwargs: list(data)
    )
    return data


def _component(**pipeline_overrides) -> OtelApiSyncComponent:
    pipeline = {
        "staged": False,
        "mapping": _mapping(),
        "sources": [{"name": "spans", "table": "otel.otel_traces"}],
        "ledger": {"enabled": True, "backend": "dagster"},
    }
    pipeline.update(pipeline_overrides)
    return OtelApiSyncComponent(
        source_config={"host": "clickhouse", "database": "otel"},
        restate_endpoint="http://restate:8080",
        pipelines={"ci": pipeline},
    )


class _StubLoadContext:
    path = os.path.dirname(MAPPING_PATH)


def _defs(component: OtelApiSyncComponent):
    return component.build_defs(_StubLoadContext())


def _dispatch_asset(component: OtelApiSyncComponent):
    assets = list(_defs(component).assets or [])
    return next(a for a in assets if "ci_dispatch" in str(a.keys))


def _materialize(asset_def, config: Dict[str, Any], instance: DagsterInstance):
    return materialize(
        [asset_def],
        instance=instance,
        run_config={"ops": {"ci_dispatch": {"config": config}}},
    )


# --- definitions -----------------------------------------------------------


def test_unstaged_pipeline_generates_only_the_dispatch_asset(rows):
    assets = list(_defs(_component()).assets or [])
    assert len(assets) == 1
    assert "ci_dispatch" in str(assets[0].keys)


def test_a_malformed_mapping_fails_at_definition_load_not_at_dispatch():
    """Load-time validation: a bad mapping breaks the code location loudly."""
    broken = _component(mapping={"api": {"base_url": "https://x"}, "group_by": "{{ 1 }}"})
    with pytest.raises(Exception, match="steps"):
        _defs(broken)


def test_a_staged_pipeline_without_dest_config_is_rejected():
    with pytest.raises(ValueError, match="no dest_config"):
        _defs(_component(staged=True))


def _staged_component() -> OtelApiSyncComponent:
    return OtelApiSyncComponent(
        source_config={"host": "clickhouse", "database": "otel", "username": "default"},
        restate_endpoint="http://restate:8080",
        dest_config={
            "drivername": "postgresql",
            "credentials": "postgresql://u:p@localhost:5432/db",
            "database": "db",
            "schema": "otel_staging",
        },
        pipelines={
            "ci": {
                "staged": True,
                "dest_schema": "otel_staging",
                "mapping": _mapping(),
                "sources": [
                    {
                        "name": "execution_spans",
                        "table": "otel.otel_traces",
                        "cursor_column": "Timestamp",
                        "primary_key": ["TraceId", "SpanId"],
                    }
                ],
                "ledger": {"enabled": False},
            }
        },
    )


def test_staged_pipeline_wires_extraction_into_dispatch():
    """The dispatch asset must depend on the dlt load, not float free."""
    assets = list(_defs(_staged_component()).assets or [])
    keys = {str(list(a.keys)[0].path): a for a in assets}

    extraction = next(k for k in keys if "execution_spans" in k)
    dispatch = next(k for k in keys if "ci_dispatch" in k)
    assert "otel_staging" in extraction  # translator carried the dest context

    dispatch_deps = [
        list(dep.asset_key.path) for spec in keys[dispatch].specs for dep in spec.deps
    ]
    assert list(list(keys[extraction].keys)[0].path) in dispatch_deps


def test_staged_dispatch_reads_the_staged_table(monkeypatch, ingress):
    captured = {}

    def _fake_read(dest_config, schema, table, limit):
        captured.update({"schema": schema, "table": table})
        return [_span("s1")]

    monkeypatch.setattr(component_module, "_read_staged_rows", _fake_read)

    assets = list(_defs(_staged_component()).assets or [])
    dispatch = next(a for a in assets if "ci_dispatch" in str(a.keys))
    with DagsterInstance.ephemeral() as instance:
        result = _materialize(dispatch, {}, instance)

    assert result.success
    # dlt lower-cases table names on load; the reader must match.
    assert captured == {"schema": "otel_staging", "table": "execution_spans"}
    assert len(ingress.posts) == 1


# --- dry run ---------------------------------------------------------------


def test_dry_run_renders_everything_and_sends_nothing(rows, ingress):
    with DagsterInstance.ephemeral() as instance:
        result = _materialize(_dispatch_asset(_component()), {"dry_run": True}, instance)
        assert result.success

        metadata = result.asset_materializations_for_node("ci_dispatch")[0].metadata
        assert ingress.posts == []
        assert metadata["dry_run"].value is True
        assert metadata["groups_rendered"].value == 1
        assert metadata["groups_dispatched"].value == 0
        # 1 entity-artifact call + 1 upsert + ... over 2 entities/items/rows.
        assert metadata["calls_planned"].value == 7
        # The exact payloads are surfaced for review before they go live.
        assert metadata["plans"].value[0]["steps"][0]["calls"][0]["path"].startswith(
            "/api/EntityMaintenance/"
        )


# --- dispatch --------------------------------------------------------------


def test_dispatch_posts_one_plan_per_group_to_the_group_keyed_object(rows, ingress):
    with DagsterInstance.ephemeral() as instance:
        result = _materialize(_dispatch_asset(_component()), {}, instance)
        assert result.success

        assert len(ingress.posts) == 1
        post = ingress.posts[0]
        assert post["url"] == (
            "http://restate:8080/ApiCallPlanService/run-42/execute_plan/send"
        )
        # The plan hash rides as the ingress idempotency key so a retried
        # send collapses in Restate rather than becoming a second invocation.
        assert post["headers"]["idempotency-key"] == post["json"]["plan_id"]
        assert post["json"]["group_key"] == "run-42"


def test_group_keys_are_url_encoded(monkeypatch, ingress):
    monkeypatch.setattr(
        component_module,
        "_read_clickhouse_rows",
        lambda *a, **k: [_span("s1", group="team/alpha run 1")],
    )
    with DagsterInstance.ephemeral() as instance:
        _materialize(_dispatch_asset(_component()), {}, instance)
    assert "team%2Falpha%20run%201" in ingress.posts[0]["url"]


def test_a_failed_handoff_fails_the_asset(rows, monkeypatch):
    monkeypatch.setattr(
        component_module.httpx, "AsyncClient", lambda **kwargs: FakeIngress(fail=True)
    )
    with DagsterInstance.ephemeral() as instance:
        result = materialize(
            [_dispatch_asset(_component())],
            instance=instance,
            run_config={"ops": {"ci_dispatch": {"config": {}}}},
            raise_on_error=False,
        )
    assert not result.success


# --- run-time configuration ------------------------------------------------


def test_only_group_narrows_the_dispatch(monkeypatch, ingress):
    monkeypatch.setattr(
        component_module,
        "_read_clickhouse_rows",
        lambda *a, **k: [_span("s1", group="run-1"), _span("s2", group="run-2")],
    )
    with DagsterInstance.ephemeral() as instance:
        _materialize(_dispatch_asset(_component()), {"only_group": "run-2"}, instance)
    assert [p["json"]["group_key"] for p in ingress.posts] == ["run-2"]


def test_max_groups_caps_the_dispatch(monkeypatch, ingress):
    monkeypatch.setattr(
        component_module,
        "_read_clickhouse_rows",
        lambda *a, **k: [_span("s1", group="run-1"), _span("s2", group="run-2")],
    )
    with DagsterInstance.ephemeral() as instance:
        _materialize(_dispatch_asset(_component()), {"max_groups": 1}, instance)
    assert len(ingress.posts) == 1


# --- ledger ----------------------------------------------------------------


def test_the_ledger_stops_a_second_dispatch_of_an_unchanged_group(rows, ingress):
    """Cross-run duplicate suppression, Dagster side.

    Restate's completed-hash state is the authoritative guard; this stops
    the redundant traffic and keeps the asset's reporting honest.
    """
    asset_def = _dispatch_asset(_component())
    with DagsterInstance.ephemeral() as instance:
        first = _materialize(asset_def, {}, instance)
        assert first.success
        assert len(ingress.posts) == 1

        second = _materialize(asset_def, {}, instance)
        assert second.success
        assert len(ingress.posts) == 1  # nothing new sent

        metadata = second.asset_materializations_for_node("ci_dispatch")[0].metadata
        assert metadata["groups_skipped_duplicate"].value == 1
        assert metadata["groups_dispatched"].value == 0


def test_ignore_ledger_forces_a_re_dispatch(rows, ingress):
    asset_def = _dispatch_asset(_component())
    with DagsterInstance.ephemeral() as instance:
        _materialize(asset_def, {}, instance)
        _materialize(asset_def, {"ignore_ledger": True}, instance)
    assert len(ingress.posts) == 2


def test_a_changed_group_is_dispatched_again(monkeypatch, ingress):
    """The ledger keys on content, so new telemetry re-dispatches."""
    state = {"rows": [_span("s1")]}
    monkeypatch.setattr(
        component_module, "_read_clickhouse_rows", lambda *a, **k: list(state["rows"])
    )
    asset_def = _dispatch_asset(_component())
    with DagsterInstance.ephemeral() as instance:
        _materialize(asset_def, {}, instance)
        state["rows"] = [_span("s1"), _span("s2", entity="e2", item="i2")]
        _materialize(asset_def, {}, instance)

    assert len(ingress.posts) == 2
    assert ingress.posts[0]["json"]["plan_hash"] != ingress.posts[1]["json"]["plan_hash"]


def test_dry_run_does_not_write_to_the_ledger(rows, ingress):
    asset_def = _dispatch_asset(_component())
    with DagsterInstance.ephemeral() as instance:
        _materialize(asset_def, {"dry_run": True}, instance)
        _materialize(asset_def, {}, instance)
    assert len(ingress.posts) == 1


# --- readiness -------------------------------------------------------------


def test_a_group_that_is_still_filling_is_deferred_not_dispatched(monkeypatch, ingress):
    monkeypatch.setattr(
        component_module,
        "_read_clickhouse_rows",
        lambda *a, **k: [_span("s1") | {"SpanAttributes": {**_span("s1")["SpanAttributes"],
                                                           "execution.terminal": "false"}}],
    )
    with DagsterInstance.ephemeral() as instance:
        result = _materialize(_dispatch_asset(_component()), {}, instance)
        metadata = result.asset_materializations_for_node("ci_dispatch")[0].metadata

    assert ingress.posts == []
    assert metadata["groups_deferred_not_ready"].value == 1
    assert "still filling" in metadata["deferred_reasons"].value[0]["reason"]


def test_ignore_readiness_dispatches_a_filling_group(monkeypatch, ingress):
    monkeypatch.setattr(
        component_module,
        "_read_clickhouse_rows",
        lambda *a, **k: [_span("s1") | {"SpanAttributes": {**_span("s1")["SpanAttributes"],
                                                           "execution.terminal": "false"}}],
    )
    with DagsterInstance.ephemeral() as instance:
        _materialize(_dispatch_asset(_component()), {"ignore_readiness": True}, instance)
    assert len(ingress.posts) == 1
