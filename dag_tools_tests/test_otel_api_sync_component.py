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
from dagster import AssetKey, DagsterInstance, materialize

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
        # mapping.yaml no longer carries a base URL (it is deliberately
        # portable across environments); the component supplies one, same
        # as the real component.yaml does.
        "api": {"base_url": "https://api.test"},
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
                "api": {"base_url": "https://api.test"},
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


# --- api override (component.yaml `api:` merged over mapping.yaml `api:`) --


def _minimal_mapping(api: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "api": api,
        "group_by": "{{ attr(row, 'execution.group_id') }}",
        "steps": [{"id": "s", "path": "/x", "payload": {}}],
    }


def test_component_base_url_override_wins_and_clears_base_url_env(rows, ingress):
    """base_url_env must not silently keep winning once the component sets base_url."""
    component = _component(
        mapping=_minimal_mapping({"base_url_env": "SOME_WORKER_ENV_VAR", "timeout_seconds": 5}),
        api={"base_url": "https://override.example"},
    )
    with DagsterInstance.ephemeral() as instance:
        _materialize(_dispatch_asset(component), {}, instance)
    assert ingress.posts[0]["json"]["api"]["base_url"] == "https://override.example"


def test_mapping_with_no_base_url_is_valid_when_component_supplies_one(rows, ingress):
    """mapping.yaml is portable: it need not declare api.base_url at all."""
    component = _component(
        mapping=_minimal_mapping({"timeout_seconds": 5}),
        api={"base_url": "https://override.example"},
    )
    # Would raise at definition-load time (ApiSpec requires base_url or
    # base_url_env) if the override merge were not applied before load_spec.
    assets = list(_defs(component).assets or [])
    assert len(assets) == 1


def test_plan_carried_headers_appear_in_the_rendered_plan(rows, ingress):
    component = _component(
        mapping=_minimal_mapping({"base_url": "https://api.test"}),
        api={
            "base_url": "https://api.test",
            "plan_carried_headers": {"X-Trace-Source": "dag-tools"},
        },
    )
    with DagsterInstance.ephemeral() as instance:
        _materialize(_dispatch_asset(component), {}, instance)
    assert ingress.posts[0]["json"]["api"]["headers"]["X-Trace-Source"] == "dag-tools"


def test_mapping_headers_survive_with_component_headers_layered_on_top(rows, ingress):
    component = _component(
        mapping=_minimal_mapping(
            {"base_url": "https://api.test", "headers": {"X-From-Mapping": "m"}}
        ),
        api={"base_url": "https://api.test", "headers": {"X-From-Component": "c"}},
    )
    with DagsterInstance.ephemeral() as instance:
        _materialize(_dispatch_asset(component), {}, instance)
    headers = ingress.posts[0]["json"]["api"]["headers"]
    assert headers["X-From-Mapping"] == "m"
    assert headers["X-From-Component"] == "c"


# --- completion check (`restate_plans_completed`) ---------------------------


class _FakeMaterializationEvent:
    def __init__(self, dispatched):
        self.asset_materialization = _FakeMaterialization(dispatched)


class _FakeMaterialization:
    def __init__(self, dispatched):
        self.metadata = (
            {component_module.DISPATCHED_METADATA_KEY: _FakeMetadataValue(dispatched)}
            if dispatched is not None
            else {}
        )


class _FakeMetadataValue:
    """Stands in for dagster's MetadataValue.json(...): exposes `.data`."""

    def __init__(self, data):
        self.data = data


class _FakeInstance:
    def __init__(self, dispatched):
        self._event = _FakeMaterializationEvent(dispatched) if dispatched is not None else None

    def get_latest_materialization_event(self, asset_key):
        return self._event


class _FakeCheckContext:
    def __init__(self, dispatched):
        self.instance = _FakeInstance(dispatched)


class _FakeStatusResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeStatusClient:
    """Stands in for httpx.Client: routes get_status POSTs to a status map keyed by group_key."""

    def __init__(self, status_by_group):
        self._status_by_group = status_by_group

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def post(self, url):
        # URL shape: {ingress}/ApiCallPlanService/{group_key}/get_status
        group_key = url.split("/ApiCallPlanService/", 1)[1].rsplit("/get_status", 1)[0]
        return _FakeStatusResponse(self._status_by_group[group_key])


def _run_completion_check(monkeypatch, dispatched, status_by_group, timeout_seconds=5.0):
    monkeypatch.setattr(
        component_module.httpx, "Client", lambda **kwargs: _FakeStatusClient(status_by_group)
    )
    monkeypatch.setattr(component_module.time, "sleep", lambda *_: None)
    context = _FakeCheckContext(dispatched)
    return component_module._reconcile_dispatch_completion(
        context=context,
        dispatch_key=component_module.AssetKey("ci_dispatch"),
        ingress="http://restate:8080",
        poll_interval_seconds=0.01,
        timeout_seconds=timeout_seconds,
    )


def test_completion_check_passes_when_every_group_completed(monkeypatch):
    dispatched = [{"group_key": "g1", "plan_hash": "h1"}, {"group_key": "g2", "plan_hash": "h2"}]
    status_by_group = {
        "g1": {"last_result": {"plan_hash": "h1", "status": "COMPLETED"}},
        "g2": {"last_result": {"plan_hash": "h2", "status": "COMPLETED"}},
    }
    result = _run_completion_check(monkeypatch, dispatched, status_by_group)
    assert result.passed is True
    assert result.metadata["groups_completed"].value == 2


def test_completion_check_fails_when_a_group_recorded_failed(monkeypatch):
    dispatched = [{"group_key": "g1", "plan_hash": "h1"}, {"group_key": "g2", "plan_hash": "h2"}]
    status_by_group = {
        "g1": {"last_result": {"plan_hash": "h1", "status": "COMPLETED"}},
        "g2": {"last_result": {"plan_hash": "h2", "status": "FAILED", "failures": [{"status": 400}]}},
    }
    result = _run_completion_check(monkeypatch, dispatched, status_by_group)
    assert result.passed is False
    failed = result.metadata["groups_failed"].value
    assert [f["group_key"] for f in failed] == ["g2"]


def test_completion_check_treats_completed_with_errors_as_a_failure(monkeypatch):
    """The ordering trap: COMPLETED_WITH_ERRORS still lands in completed_plan_hashes,
    so last_result must be checked BEFORE that list or this would wrongly pass."""
    dispatched = [{"group_key": "g1", "plan_hash": "h1"}]
    status_by_group = {
        "g1": {
            "last_result": {"plan_hash": "h1", "status": "COMPLETED_WITH_ERRORS"},
            "completed_plan_hashes": ["h1"],
        },
    }
    result = _run_completion_check(monkeypatch, dispatched, status_by_group)
    assert result.passed is False
    assert [f["group_key"] for f in result.metadata["groups_failed"].value] == ["g1"]


def test_completion_check_times_out_on_a_group_that_never_reports(monkeypatch):
    dispatched = [{"group_key": "g1", "plan_hash": "h1"}]
    status_by_group = {"g1": {}}  # never records last_result or completed_plan_hashes
    result = _run_completion_check(monkeypatch, dispatched, status_by_group, timeout_seconds=0)
    assert result.passed is False
    assert [g["group_key"] for g in result.metadata["groups_still_running"].value] == ["g1"]


def test_completion_check_passes_when_nothing_was_dispatched(monkeypatch):
    result = _run_completion_check(monkeypatch, dispatched=[], status_by_group={})
    assert result.passed is True
    assert "note" in result.metadata


def test_completion_check_passes_when_there_is_no_materialization(monkeypatch):
    monkeypatch.setattr(
        component_module.httpx, "Client", lambda **kwargs: _FakeStatusClient({})
    )
    context = _FakeCheckContext(dispatched=None)
    result = component_module._reconcile_dispatch_completion(
        context=context,
        dispatch_key=component_module.AssetKey("ci_dispatch"),
        ingress="http://restate:8080",
        poll_interval_seconds=0.01,
        timeout_seconds=5.0,
    )
    assert result.passed is True
    assert "note" in result.metadata


def test_completion_check_records_an_ingress_error_instead_of_crashing(monkeypatch):
    """A check that raised would fail the run with a transport traceback
    rather than the thing the operator needs to see: which group is
    unaccounted for."""

    class _ExplodingClient(_FakeStatusClient):
        def post(self, url):
            raise RuntimeError("connection refused")

    monkeypatch.setattr(component_module.httpx, "Client", lambda **kwargs: _ExplodingClient({}))
    monkeypatch.setattr(component_module.time, "sleep", lambda *_: None)
    result = component_module._reconcile_dispatch_completion(
        context=_FakeCheckContext([{"group_key": "g1", "plan_hash": "h1"}]),
        dispatch_key=component_module.AssetKey("ci_dispatch"),
        ingress="http://restate:8080",
        poll_interval_seconds=0.01,
        timeout_seconds=5.0,
    )
    assert result.passed is False
    failed = result.metadata["groups_failed"].value
    assert [f["group_key"] for f in failed] == ["g1"]
    assert "connection refused" in failed[0]["error"]


# --- wiring: the check has to actually reach Definitions -------------------


def test_build_defs_emits_the_completion_check_by_default():
    checks = list(_defs(_component()).asset_checks or [])
    assert len(checks) == 1
    spec = next(iter(checks[0].check_specs))
    assert spec.name == "restate_plans_completed"
    assert spec.asset_key == AssetKey("ci_dispatch")


def test_completion_check_can_be_disabled():
    """Opting out must remove the check, not leave a disabled one behind."""
    component = _component(completion_check={"enabled": False})
    assert list(_defs(component).asset_checks or []) == []


def test_dispatch_asset_publishes_what_it_dispatched(rows, ingress):
    """The check reads this metadata back; if the asset stops writing it,
    every check silently degrades to 'nothing to reconcile' and passes."""
    with DagsterInstance.ephemeral() as instance:
        result = _materialize(_dispatch_asset(_component()), {}, instance)

    metadata = result.asset_materializations_for_node("ci_dispatch")[0].metadata
    dispatched = metadata[component_module.DISPATCHED_METADATA_KEY].value
    assert [d["group_key"] for d in dispatched] == ["run-42"]
    assert dispatched[0]["plan_hash"] == ingress.posts[0]["json"]["plan_hash"]


# ---------------------------------------------------------------------------
# Staging destination configuration
#
# The read-back half of this component used to resolve its own credential,
# preferring a DSN string and the ambient dlt env var over the
# host/username/password parts every other component is configured with.
# It now goes through the same `config_to_credentials` as the dlt half.
# ---------------------------------------------------------------------------

_PARTS = {
    "drivername": "postgresql",
    "host": "pg.internal",
    "port": 5433,
    "username": "admin",
    "password": "password",
    "database": "telemetry",
    "schema": "otel_staging",
}


def test_staging_engine_accepts_the_same_host_parts_as_every_other_component():
    url = component_module._staging_engine(dict(_PARTS)).url
    assert url.host == "pg.internal"
    assert url.port == 5433
    assert url.username == "admin"
    assert url.password == "password"
    assert url.database == "telemetry"
    # `schema` is not part of a DSN and must not leak into the query string.
    assert "schema" not in url.query


def test_explicit_dest_config_host_beats_the_ambient_dlt_env_var(monkeypatch):
    """The trap: one DESTINATION__*__CREDENTIALS left over from another
    pipeline used to silently retarget this component's read-back."""
    monkeypatch.setenv(
        "DESTINATION__POSTGRES__CREDENTIALS", "postgresql://other:other@elsewhere:5432/wrong"
    )
    url = component_module._staging_engine(dict(_PARTS)).url
    assert url.host == "pg.internal"
    assert url.database == "telemetry"


def test_a_password_with_url_metacharacters_survives(monkeypatch):
    config = dict(_PARTS, password="p@ss/w:rd")
    assert component_module._staging_engine(config).url.password == "p@ss/w:rd"


def test_staging_engine_still_accepts_a_credentials_dsn():
    url = component_module._staging_engine(
        {"drivername": "postgresql", "credentials": "postgresql://u:p@dsnhost:5432/db"}
    ).url
    assert url.host == "dsnhost"
    assert url.database == "db"


def test_staging_engine_falls_back_to_the_dlt_env_var(monkeypatch):
    monkeypatch.setenv(
        "DESTINATION__POSTGRES__CREDENTIALS", "postgresql://u:p@envhost:5432/envdb"
    )
    url = component_module._staging_engine({"drivername": "postgresql"}).url
    assert url.host == "envhost"
    assert url.database == "envdb"


def test_staging_engine_names_the_parts_shape_when_nothing_resolves(monkeypatch):
    monkeypatch.delenv("DESTINATION__POSTGRES__CREDENTIALS", raising=False)
    monkeypatch.delenv("DESTINATION__POSTGRESQL__CREDENTIALS", raising=False)
    with pytest.raises(ValueError, match="host/username/password"):
        component_module._staging_engine({"drivername": "postgresql"})
