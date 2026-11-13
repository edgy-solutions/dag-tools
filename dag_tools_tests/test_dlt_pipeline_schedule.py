"""Per-pipeline ``schedule:`` on the dlt component.

Built in Python inside ``build_defs`` (never ``post_processing``), so it loads
on the 1.10.19 floor. Each behaviour has a case and a control.
"""
import dagster as dg
import pytest
from dagster import DagsterRunStatus, DefaultScheduleStatus
from dagster._core.instance import DagsterInstance
from dagster._core.storage.dagster_run import DagsterRun

pytest.importorskip("dlt")
pytest.importorskip("dagster_dlt")

from dag_tools_tests._dlt_component_helpers import load_component

BOTH = ("widgets", "gadgets")


def _pipe(name, table, schedule=None, **kw):
    block = {"name": name, "dest_schema": "raw", "sources": [table], **kw}
    if schedule is not None:
        block["schedule"] = schedule
    return block


def _load(tmp_path, monkeypatch, pipelines, **kw):
    return load_component(tmp_path, monkeypatch, pipelines=pipelines, tables=BOTH, **kw)


def _one(tmp_path, monkeypatch, schedule):
    return _load(tmp_path, monkeypatch, {"p": _pipe("alpha", "widgets", schedule)})


def _executable_keys(defs, job_name):
    job = defs.resolve_job_def(job_name)
    return set(job.asset_layer.executable_asset_keys)


# 6 ---------------------------------------------------------------------------
def test_no_schedule_means_no_schedule_and_no_job(tmp_path, monkeypatch):
    defs = _one(tmp_path, monkeypatch, None)
    assert not defs.jobs and not defs.schedules


def test_with_a_schedule_there_is_exactly_one_job_and_one_schedule(tmp_path, monkeypatch):
    defs = _one(tmp_path, monkeypatch, {"cron": "*/15 * * * *"})
    assert [j.name for j in defs.jobs] == ["alpha_job"]
    assert [s.name for s in defs.schedules] == ["alpha_schedule"]


# 7 ---------------------------------------------------------------------------
def test_the_schedule_is_stopped_by_default(tmp_path, monkeypatch):
    defs = _one(tmp_path, monkeypatch, {"cron": "0 * * * *"})
    assert defs.schedules[0].default_status == DefaultScheduleStatus.STOPPED


def test_enabled_true_starts_it_running(tmp_path, monkeypatch):
    defs = _one(tmp_path, monkeypatch, {"cron": "0 * * * *", "enabled": True})
    assert defs.schedules[0].default_status == DefaultScheduleStatus.RUNNING


# 8 ---------------------------------------------------------------------------
def test_each_job_selects_only_its_own_pipelines_keys(tmp_path, monkeypatch):
    defs = _load(tmp_path, monkeypatch, {
        "a": _pipe("alpha", "widgets", {"cron": "0 * * * *"}),
        "b": _pipe("beta", "gadgets", {"cron": "5 * * * *"}),
    })
    alpha = _executable_keys(defs, "alpha_job")
    beta = _executable_keys(defs, "beta_job")
    assert alpha and beta
    assert alpha.isdisjoint(beta)
    assert all("widgets" in k.to_user_string() for k in alpha)
    assert all("gadgets" in k.to_user_string() for k in beta)


def test_control_an_unscheduled_pipeline_gets_no_job(tmp_path, monkeypatch):
    defs = _load(tmp_path, monkeypatch, {
        "a": _pipe("alpha", "widgets", {"cron": "0 * * * *"}),
        "b": _pipe("beta", "gadgets"),
    })
    assert [j.name for j in defs.jobs] == ["alpha_job"]


# 9 ---------------------------------------------------------------------------
def _chain(exc):
    parts = []
    while exc is not None:
        parts.append(str(exc))
        exc = exc.__cause__ or exc.__context__
    return " | ".join(parts)


@pytest.mark.parametrize("bad", [
    {"enabled": True},
    {"cron": "0 * * * *", "cadence": "hourly"},
])
def test_a_malformed_schedule_block_fails_the_load(tmp_path, monkeypatch, bad):
    with pytest.raises(Exception) as err:
        _one(tmp_path, monkeypatch, bad)
    assert any(w in _chain(err.value) for w in ("cron", "cadence")), _chain(err.value)


def test_a_schedule_on_a_zero_asset_pipeline_fails_the_load_naming_it(tmp_path, monkeypatch):
    with pytest.raises(Exception) as err:
        _load(tmp_path, monkeypatch, {
            "p": _pipe("emptyone", "widgets", {"cron": "0 * * * *"}, sources=[]),
        })
    assert "emptyone" in _chain(err.value)


def test_control_a_zero_asset_pipeline_without_a_schedule_loads(tmp_path, monkeypatch):
    block = {"name": "emptyone", "dest_schema": "raw", "sources": []}
    defs = _load(tmp_path, monkeypatch, {"p": block})
    assert not defs.jobs


# 10 --------------------------------------------------------------------------
def _instance(tmp_path):
    # Not ephemeral(): an ephemeral instance has no InstanceRef, and a schedule
    # context reaches its instance through one.
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    return DagsterInstance.local_temp(tempdir=str(home))


def _evaluate(schedule, instance):
    ctx = dg.build_schedule_context(instance=instance)
    return schedule.evaluate_tick(ctx)


def test_a_started_run_of_the_job_skips_the_tick(tmp_path, monkeypatch):
    sched = _one(tmp_path, monkeypatch, {"cron": "0 * * * *"}).schedules[0]
    with _instance(tmp_path) as instance:
        instance.add_run(DagsterRun(job_name="alpha_job", status=DagsterRunStatus.STARTED))
        result = _evaluate(sched, instance)
    assert not result.run_requests
    assert result.skip_message and "alpha_job" in result.skip_message


def test_no_run_gives_a_run_request(tmp_path, monkeypatch):
    sched = _one(tmp_path, monkeypatch, {"cron": "0 * * * *"}).schedules[0]
    with _instance(tmp_path) as instance:
        result = _evaluate(sched, instance)
    assert len(result.run_requests) == 1


def test_a_run_of_a_different_job_does_not_skip(tmp_path, monkeypatch):
    sched = _one(tmp_path, monkeypatch, {"cron": "0 * * * *"}).schedules[0]
    with _instance(tmp_path) as instance:
        instance.add_run(DagsterRun(job_name="other_job", status=DagsterRunStatus.STARTED))
        result = _evaluate(sched, instance)
    assert len(result.run_requests) == 1


def test_a_finished_run_does_not_skip(tmp_path, monkeypatch):
    sched = _one(tmp_path, monkeypatch, {"cron": "0 * * * *"}).schedules[0]
    with _instance(tmp_path) as instance:
        instance.add_run(DagsterRun(job_name="alpha_job", status=DagsterRunStatus.SUCCESS))
        result = _evaluate(sched, instance)
    assert len(result.run_requests) == 1


# 11 --------------------------------------------------------------------------
def test_a_timezone_passes_through(tmp_path, monkeypatch):
    defs = _one(tmp_path, monkeypatch, {"cron": "0 * * * *", "timezone": "Europe/Berlin"})
    assert defs.schedules[0].execution_timezone == "Europe/Berlin"


def test_an_absent_timezone_is_none(tmp_path, monkeypatch):
    defs = _one(tmp_path, monkeypatch, {"cron": "0 * * * *"})
    assert defs.schedules[0].execution_timezone is None


# 12 --------------------------------------------------------------------------
def test_the_component_file_stays_inside_type_and_attributes(tmp_path, monkeypatch):
    """The 1.10.19 floor rejects any other top-level key and fails the whole
    load. Every test above loads through ``build_component_defs`` on a file
    of exactly this shape; this pins the shape itself."""
    import json

    _one(tmp_path, monkeypatch, {"cron": "0 * * * *"})
    written = next(tmp_path.glob("pkg_*/components/dlt/component.yaml"))
    assert set(json.loads(written.read_text())) == {"type", "attributes"}
