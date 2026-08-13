"""End-to-end check of the shipped example mapping.

The example is the pattern people copy, so it gets exercised rather than
merely reviewed: real OTel-shaped spans in, four ordered calls out,
executed through the real handler.
"""
import asyncio
import datetime as dt
import os
from typing import Any, Dict, List

import pytest
import yaml

from dag_tools.otel_api_sync import build_plan, group_rows, load_spec
from dag_tools.restate_handlers.api_call_plan import execute_plan

from dag_tools_tests.test_otel_api_sync_executor import FakeHttp, StubContext

MAPPING_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "examples",
    "otel_to_api",
    "dagster_home",
    "components",
    "otel_sync",
    "mapping.yaml",
)


@pytest.fixture
def spec():
    with open(MAPPING_PATH, "r", encoding="utf-8") as handle:
        return load_spec(yaml.safe_load(handle))


def _span(
    span_id: str,
    seconds: int = 0,
    group: str = "run-42",
    entity: str = "entity_1",
    item: str = "item_1",
    artifact: str = "file1.txt",
    outcome: str = "SUCCESS",
    terminal: str = "true",
    **extra: Any,
) -> Dict[str, Any]:
    attributes = {
        "execution.group_id": group,
        "execution.terminal": terminal,
        "execution.outcome": outcome,
        "entity.id": entity,
        "entity.ids": entity,
        "item.name": item,
        "artifact.path": artifact,
        "ci.branch": "main",
        "ci.commit": "abc1234",
        "metric.NUM_ERROR": "0",
        "metric.TOTAL_TOGGLES": "81.2",
    }
    attributes.update({k: str(v) for k, v in extra.items()})
    return {
        # Distinct event times: rendering orders rows canonically by event
        # time, so same-timestamp spans would fall back to a content
        # digest and the artifact order would not reflect the sequence.
        "Timestamp": dt.datetime(2026, 8, 10, 20, 2, 36, 338000) + dt.timedelta(seconds=seconds),
        "TraceId": "trace-1",
        "SpanId": span_id,
        "SpanName": "execution.event",
        "SpanAttributes": attributes,
        "ResourceAttributes": {"service.name": "runner"},
    }


SPANS: List[Dict[str, Any]] = [
    _span("span-1", 0, entity="entity_1", item="item_1", artifact="file1.txt"),
    _span("span-2", 1, entity="entity_1", item="item_1", artifact="file2.txt"),
    _span("span-3", 2, entity="entity_2", item="item_2", artifact="file3.txt"),
]


def test_example_mapping_is_valid(spec):
    assert [s.id for s in spec.steps] == [
        "entity_artifacts",
        "upsert_items",
        "item_entity_map",
        "record_execution",
    ]


def test_example_renders_the_four_calls_in_order(spec):
    plan = build_plan("run-42", SPANS, spec)
    assert [s["id"] for s in plan["steps"]] == [
        "entity_artifacts",
        "upsert_items",
        "item_entity_map",
        "record_execution",
    ]
    # 2 entities, 1 bulk upsert, 2 items, 3 event records.
    assert [len(s["calls"]) for s in plan["steps"]] == [2, 1, 2, 3]


def test_example_payload_shapes_match_the_target_contract(spec):
    plan = build_plan("run-42", SPANS, spec)
    steps = {s["id"]: s for s in plan["steps"]}

    # 1. Artifacts joined per entity, de-duplicated.
    entity_calls = {c["path"]: c["body"] for c in steps["entity_artifacts"]["calls"]}
    assert entity_calls["/api/EntityMaintenance/entity_1"] == {"artifacts": "file1.txt,file2.txt"}
    assert entity_calls["/api/EntityMaintenance/entity_2"] == {"artifacts": "file3.txt"}

    # 2. Bulk upsert: boolean stays boolean, items is an array of objects.
    upsert = steps["upsert_items"]["calls"][0]["body"]
    assert upsert["deleteMissingEntries"] is False
    assert upsert["items"] == [{"itemName": "item_1"}, {"itemName": "item_2"}]

    # 3. Each item maps to ITS entities, not the group's.
    mapping = {c["path"]: c["body"] for c in steps["item_entity_map"]["calls"]}
    assert mapping["/api/ProcessItemDetails/item_1/EntityMapping"] == {
        "entityIdentifiers": ["entity_1"]
    }
    assert mapping["/api/ProcessItemDetails/item_2/EntityMapping"] == {
        "entityIdentifiers": ["entity_2"]
    }

    # 4. Per-event record: ISO timestamp, numeric metrics, array field.
    record = steps["record_execution"]["calls"][0]["body"]
    assert record["itemName"] == "item_1"
    assert record["outcome"] == "SUCCESS"
    assert record["eventDateTime"] == "2026-08-10T20:02:36.338Z"  # earliest span
    assert record["config"] == {"branch": "main", "commitId": "abc1234"}
    assert record["metrics"] == [
        {"metricName": "NUM_ERROR", "resultNumeric": 0},
        {"metricName": "TOTAL_TOGGLES", "resultNumeric": 81.2},
    ]
    assert record["relatedEntities"] == ["entity_1"]


def test_example_groups_by_execution_group(spec):
    rows = SPANS + [_span("span-9", 3, group="run-99", entity="entity_9", item="item_9")]
    groups = group_rows(rows, spec)
    assert sorted(groups) == ["run-42", "run-99"]


def test_example_terminal_marker_releases_the_group(spec):
    """complete_when short-circuits the 5-minute quiet period."""
    from dag_tools.otel_api_sync import group_readiness

    now = dt.datetime(2026, 8, 10, 20, 2, 40, tzinfo=dt.timezone.utc)
    assert group_readiness(SPANS, spec, now=now)[0] is True

    still_running = [_span("span-1", 0, terminal="false")]
    ready, reason = group_readiness(still_running, spec, now=now)
    assert ready is False
    assert "still filling" in reason


def test_example_404_fallback_creates_only_the_missing_entity(spec, monkeypatch):
    """The destructive case, end to end through the real handler.

    entity_2 does not exist and 404s; entity_1 patches successfully. The
    bulk create that follows must carry entity_2 alone — including
    entity_1 would replace data the telemetry never described.
    """
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://api.test")
    monkeypatch.setenv("TARGET_API_TOKEN", "token-value")

    fake = FakeHttp({"PATCH /api/EntityMaintenance/entity_2": 404})
    import requests

    monkeypatch.setattr(requests, "request", fake.request)

    plan = build_plan("run-42", SPANS, spec)
    result = asyncio.run(execute_plan(StubContext(key="run-42"), plan))

    bulk = [c for c in fake.calls if c["path"] == "/api/EntityMaintenance"]
    assert len(bulk) == 1
    assert [e["entityIdentifier"] for e in bulk[0]["body"]["entities"]] == ["entity_2"]
    assert bulk[0]["body"]["deleteMissingEntities"] is False

    assert result["status"] == "COMPLETED"
    # 2 patches + 1 upsert + 2 mappings + 3 records = 8, plus 1 bulk fallback.
    assert result["calls_executed"] == 8
    assert result["fallbacks_run"] == 1

    # Credentials resolved worker-side on every call.
    assert all(c["headers"]["Authorization"] == "Bearer token-value" for c in fake.calls)


def test_example_full_plan_executes_in_endpoint_order(spec, monkeypatch):
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://api.test")
    monkeypatch.setenv("TARGET_API_TOKEN", "token-value")
    fake = FakeHttp({})
    import requests

    monkeypatch.setattr(requests, "request", fake.request)

    asyncio.run(execute_plan(StubContext(key="run-42"), build_plan("run-42", SPANS, spec)))

    assert [f"{c['method']} {c['path']}" for c in fake.calls] == [
        "PATCH /api/EntityMaintenance/entity_1",
        "PATCH /api/EntityMaintenance/entity_2",
        "POST /api/ProcessItemDetails/BulkUpdate",
        "POST /api/ProcessItemDetails/item_1/EntityMapping",
        "POST /api/ProcessItemDetails/item_2/EntityMapping",
        "POST /api/RecordExecution",
        "POST /api/RecordExecution",
        "POST /api/RecordExecution",
    ]
