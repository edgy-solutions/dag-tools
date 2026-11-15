"""The ``systemlink_test_monitor`` kind through the dlt component, against the fake server.

Every case runs the shipped example exactly as a deployment would load it, over a real duckdb file.
"""
import logging
import re
import os
from pathlib import Path

import pytest

pytest.importorskip("dlt")
pytest.importorskip("dagster_dlt")
pytest.importorskip("duckdb")

from dagster import AssetKey, DagsterInstance

from dag_tools_tests._dlt_component_helpers import load_component
from dag_tools_tests._systemlink_helpers import (
    PSEUDO_ENV, TEST_KEY, TEST_PSEUDO, all_text, load_example, run_job, table,
)
from dag_tools_tests.systemlink_fake import FakeSystemLink, make_result, make_step

PREFIX = ["dlt", "warehouse", "raw"]
RESULTS, STEPS, INGEST = (AssetKey(PREFIX + [f"sl_{n}"]) for n in ("results", "steps", "ingest_runs"))


@pytest.fixture
def dlt_home(tmp_path, monkeypatch):
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    return tmp_path / "dlt"


def chain(exc):
    out, seen = [], set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        out.append(str(exc))
        exc = exc.__cause__ or exc.__context__
    return "\n".join(out)


def world(n_a=3, n_b=2, failed_b=False):
    rows = [make_result(i) for i in range(n_a)]
    rows += [make_result(i, "PN-1000-B", "FAILED" if failed_b else "PASSED") for i in range(n_b)]
    return rows, [make_step(r["id"]) for r in rows]


def materialized(result):
    return [e.asset_key for e in result.get_asset_materialization_events()]


def metadata_of(result, key):
    for e in result.get_asset_materialization_events():
        if e.asset_key == key:
            return {k: getattr(v, "value", v) for k, v in e.step_materialization_data.materialization.metadata.items()}
    raise AssertionError(f"no materialization for {key}")


def checks_of(result):
    return {e.check_name: e.passed for e in result.get_asset_check_evaluations()}


# --------------------------------------------------------------------------- the legacy kind is untouched


class _Resource:
    def __init__(self):
        self.calls = []

    def run(self, **kwargs):
        self.calls.append(kwargs)
        yield "event"


def test_the_legacy_body_still_issues_one_run_with_the_same_kwargs():
    from dag_tools.asset_wrappers.source_kinds import LEGACY

    res, ctx = _Resource(), object()
    out = list(LEGACY.execute(ctx, res, base={}, pipeline=None, config=None, run_kwargs={"a": 1, "b": 2}))
    assert out == ["event"] and res.calls == [{"context": ctx, "a": 1, "b": 2}]


def test_the_legacy_schedule_still_targets_name_job(tmp_path, monkeypatch):
    block = {"name": "alpha", "dest_schema": "raw", "sources": ["widgets"], "schedule": {"cron": "0 * * * *"}}
    defs = load_component(tmp_path, monkeypatch, pipelines={"p": block})
    assert [j.name for j in defs.jobs] == ["alpha_job"]
    assert defs.schedules[0].job_name == "alpha_job"


def test_a_legacy_kind_with_table_prefix_fails_the_load(tmp_path, monkeypatch):
    block = {"name": "alpha", "dest_schema": "raw", "sources": ["widgets"], "table_prefix": "x_"}
    with pytest.raises(Exception) as ei:
        load_component(tmp_path, monkeypatch, pipelines={"p": block})
    assert re.search("table_prefix", chain(ei.value))


def test_control_a_legacy_kind_without_table_prefix_loads(tmp_path, monkeypatch):
    block = {"name": "alpha", "dest_schema": "raw", "sources": ["widgets"]}
    assert load_component(tmp_path, monkeypatch, pipelines={"p": block}).assets


# --------------------------------------------------------------------------- definitions


def test_the_example_loads_with_no_network_and_no_key(tmp_path, monkeypatch, dlt_home):
    defs = load_example(tmp_path, monkeypatch, set_env=False)
    keys = {k for a in defs.assets if hasattr(a, "keys") for k in a.keys}
    assert keys == {RESULTS, STEPS, INGEST}
    for var in ("SYSTEMLINK_API_KEY", PSEUDO_ENV):
        assert var not in os.environ


def test_the_schedule_targets_incremental_and_no_name_job_exists(tmp_path, monkeypatch, dlt_home):
    defs = load_example(tmp_path, monkeypatch)
    assert {j.name for j in defs.jobs} == {"sl_example_incremental", "sl_example_backfill"}
    assert "sl_example_job" not in {j.name for j in defs.jobs}
    assert [s.job_name for s in defs.schedules] == ["sl_example_incremental"]


def test_pseudonym_without_a_key_env_fails_the_load(tmp_path, monkeypatch, dlt_home):
    with pytest.raises(Exception) as ei:
        load_example(tmp_path, monkeypatch, source_overrides={"operator": {"store": "pseudonym"}})
    assert re.search("pseudonym_key_env", chain(ei.value))


def test_control_the_name_policy_loads_without_a_key_env(tmp_path, monkeypatch, dlt_home):
    assert load_example(tmp_path, monkeypatch, source_overrides={"operator": {"store": "name"}}).assets


@pytest.mark.parametrize("override", [{"table_prefix": None}, {"pipeline_kwargs": {"table_name": "same"}}])
def test_a_pipeline_without_a_prefix_or_with_a_forced_table_name_fails_the_load(tmp_path, monkeypatch, dlt_home, override):
    with pytest.raises(Exception) as ei:
        load_example(tmp_path, monkeypatch, pipeline_overrides=override)
    assert re.search("table_prefix|table_name", chain(ei.value))


def test_a_bad_source_config_fails_the_load_not_the_run(tmp_path, monkeypatch, dlt_home):
    with pytest.raises(Exception):
        load_example(tmp_path, monkeypatch, source_overrides={"limits": {"step_filter": "like"}})


def test_the_seed_file_is_read_relative_to_the_component_not_the_working_dir(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)
        assert run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral()).success


# --------------------------------------------------------------------------- an incremental run


def test_the_first_run_stores_everything_and_the_second_stores_nothing_new(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        inst = DagsterInstance.ephemeral()
        first = run_job(defs, "sl_example_incremental", inst)
        assert first.success and set(materialized(first)) == {RESULTS, STEPS, INGEST}
        assert len(table(tmp_path, "sl_results")) == 5 and len(table(tmp_path, "sl_steps")) == 5
        before = len(fake.requests)
        second = run_job(defs, "sl_example_incremental", inst)
        assert second.success
        assert len(table(tmp_path, "sl_results")) == 5 and len(table(tmp_path, "sl_steps")) == 5
        later = [r["route"] for r in fake.requests[before:]]
        assert "query-steps" not in later  # nothing new, so no step was fetched again
        runs = table(tmp_path, "sl_ingest_runs", "status,new_terminal")
        assert sorted(runs) == [("SUCCESS", 0), ("SUCCESS", 5)]


def test_a_result_whose_version_moved_is_stored_once_and_its_steps_refetched(tmp_path, monkeypatch, dlt_home):
    rows, steps = world(n_a=2, n_b=0)
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        inst = DagsterInstance.ephemeral()
        run_job(defs, "sl_example_incremental", inst)
        rows[0]["updatedAt"] = "2026-06-01T00:00:00Z"
        rows[0]["status"] = {"statusType": "FAILED"}
        before = len(fake.requests)
        assert run_job(defs, "sl_example_incremental", inst).success
        assert [r["route"] for r in fake.requests[before:]].count("query-steps") == 1
        stored = table(tmp_path, "sl_results", "id,_version_status", )
        assert len(stored) == 2 and dict(stored)[rows[0]["id"]] == "FAILED"
        assert len(table(tmp_path, "sl_steps")) == 2  # merged, not duplicated


def test_a_running_result_is_stored_without_steps_and_gets_them_once_terminal(tmp_path, monkeypatch, dlt_home):
    rows, steps = world(n_a=1, n_b=0)
    rows[0]["status"] = {"statusType": "RUNNING"}
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        inst = DagsterInstance.ephemeral()
        run_job(defs, "sl_example_incremental", inst)
        assert len(table(tmp_path, "sl_results")) == 1
        with pytest.raises(Exception, match="sl_steps"):  # no table at all: nothing was stored
            table(tmp_path, "sl_steps")
        assert "query-steps" not in fake.routes_seen()
        rows[0]["status"] = {"statusType": "PASSED"}
        rows[0]["updatedAt"] = "2026-06-01T00:00:00Z"
        run_job(defs, "sl_example_incremental", inst)
        assert len(table(tmp_path, "sl_steps")) == 1


def test_the_ingest_run_row_counts_the_run(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        result = run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        (row,) = table(tmp_path, "sl_ingest_runs", "run_id,mode,status,requests,results_stored,steps_stored,max_step_batch_sent")
        assert row[1:6] == ("incremental", "SUCCESS", len(fake.requests), 5, 5) and row[6] >= 1
        meta = metadata_of(result, INGEST)
        assert meta["status"] == "SUCCESS" and meta["results_stored"] == 5


def test_the_steps_child_table_is_reported_not_made_an_asset(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        result = run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        assert "sl_steps__data__parameters" in str(metadata_of(result, STEPS)["child_tables"])
        assert {k.path[-1] for k in materialized(result)} == {"sl_results", "sl_steps", "sl_ingest_runs"}


# --------------------------------------------------------------------------- operator, key leaks


def test_the_operator_is_pseudonymised_before_it_reaches_any_table(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        ops = {r[0] for r in table(tmp_path, "sl_results", "operator")}
        assert len(ops) == 1 and "op-synthetic" not in ops.pop()
        assert "op-synthetic" not in all_text(tmp_path)


def test_control_the_name_policy_keeps_the_operator(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url, source_overrides={"operator": {"store": "name"}})
        run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        assert {r[0] for r in table(tmp_path, "sl_results", "operator")} == {"op-synthetic"}


def test_the_drop_policy_leaves_no_operator_column(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url, source_overrides={"operator": {"store": "drop"}})
        run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        assert "operator" not in all_text(tmp_path).replace("operator_", "")


def _tree_text(root: Path) -> str:
    chunks = []
    for path in root.rglob("*"):
        if path.is_file():
            chunks.append(path.name)
            chunks.append(path.read_bytes().decode("utf-8", "replace"))
    return "\n".join(chunks)


def _leaks(tmp_path, dlt_home, logs):
    return {
        secret: [where for where, text in (
            ("pipeline_dir", _tree_text(dlt_home)), ("tables", all_text(tmp_path)), ("logs", logs),
        ) if secret in text]
        for secret in (TEST_KEY, TEST_PSEUDO)
    }


def test_neither_secret_appears_in_the_pipeline_dir_any_table_or_any_log(tmp_path, monkeypatch, dlt_home, caplog):
    caplog.set_level(logging.DEBUG)
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        inst = DagsterInstance.ephemeral()
        result = run_job(defs, "sl_example_incremental", inst)
        assert result.success
        logs = caplog.text + "\n".join(str(r.dagster_event) + str(r.user_message) for r in inst.all_logs(result.run_id))
    assert dlt_home.exists() and any(dlt_home.rglob("*")), "the pipeline dir must exist or this check is vacuous"
    assert len(table(tmp_path, "sl_results")) == 5
    assert _leaks(tmp_path, dlt_home, logs) == {TEST_KEY: [], TEST_PSEUDO: []}


def test_control_the_leak_scan_finds_a_planted_key(tmp_path, monkeypatch, dlt_home, caplog):
    caplog.set_level(logging.DEBUG)
    rows, steps = world(n_a=1, n_b=0)
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
    (dlt_home / "pipelines" / "planted.txt").write_text(f"key={TEST_KEY}")
    logging.getLogger("planted").warning("pseudo=%s", TEST_PSEUDO)
    leaks = _leaks(tmp_path, dlt_home, caplog.text)
    assert leaks[TEST_KEY] == ["pipeline_dir"] and leaks[TEST_PSEUDO] == ["logs"]


def test_the_key_is_sent_in_the_header_only(tmp_path, monkeypatch, dlt_home):
    rows, steps = world(n_a=1, n_b=0)
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        assert {r["key_header"] for r in fake.requests} == {TEST_KEY}
        assert all(TEST_KEY not in str(r["body"]) for r in fake.requests)


# --------------------------------------------------------------------------- failures are recorded


def test_a_refused_key_fails_the_run_and_is_recorded_without_the_key(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps, api_key="another-key") as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        result = run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        assert not result.success
        (row,) = table(tmp_path, "sl_ingest_runs", "status,error_type,auth_failed")
        assert row == ("FAILED", "AuthFailed", 1)
        assert TEST_KEY not in all_text(tmp_path)


def test_a_missing_key_variable_fails_the_run_naming_the_variable(tmp_path, monkeypatch, dlt_home):
    defs = load_example(tmp_path, monkeypatch, set_env=False)
    result = run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
    assert not result.success
    text = " ".join(str(e) for e in result.all_events)
    assert "SYSTEMLINK_API_KEY" in text


def test_a_server_that_rejects_the_configured_spelling_fails_naming_the_setting(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps, accept_order_by="updatedAt") as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        result = run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        assert not result.success
        assert "incremental.order_by" in " ".join(str(e) for e in result.all_events)


def test_control_the_matching_spelling_settings_run_clean(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps, accept_order_by="updatedAt", accept_step_form="in") as fake:
        defs = load_example(
            tmp_path, monkeypatch, base_url=fake.base_url,
            source_overrides={"incremental": {"order_by": "updatedAt"}, "limits": {"step_filter": "in"}},
        )
        assert run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral()).success


# --------------------------------------------------------------------------- asset checks


def test_the_asset_checks_pass_on_a_clean_run(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        result = run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        assert checks_of(result) == {
            "no_auth_failed": True, "not_visible_within_threshold": True, "step_batch_within_max": True,
        }


def test_the_not_visible_check_fails_when_a_result_has_no_steps(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps[:-1]) as fake:  # the last result is not visible
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        result = run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        assert result.success and checks_of(result)["not_visible_within_threshold"] is False
        assert checks_of(result)["no_auth_failed"] is True


def test_the_batch_check_fails_when_the_configured_default_could_exceed_nothing_but_is_measured(tmp_path, monkeypatch, dlt_home):
    rows, steps = world(n_a=6, n_b=0)
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url,
                            source_overrides={"limits": {"step_batch_default": 2, "step_batch_max": 3}})
        result = run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        sizes = [len(r["body"]["substitutions"]) for r in fake.requests if r["route"] == "query-steps"]
        assert max(sizes) <= 2 and metadata_of(result, INGEST)["max_step_batch_sent"] == max(sizes)
        assert checks_of(result)["step_batch_within_max"] is True


# --------------------------------------------------------------------------- backfill


BACKFILL = "sl_example_backfill"


def test_a_multi_part_backfill_yields_one_materialization_per_key(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        result = run_job(defs, BACKFILL, DagsterInstance.ephemeral())
        assert result.success
        keys = materialized(result)
        assert sorted(map(str, keys)) == sorted(map(str, [RESULTS, STEPS, INGEST]))  # exactly once each
        assert len(table(tmp_path, "sl_results")) == 5 and len(table(tmp_path, "sl_steps")) == 5
        meta = metadata_of(result, RESULTS)
        assert meta["mode"] == "backfill" and meta["parts_total"] == 2 and meta["parts_loaded"] == 2


def test_a_backfill_first_counts_each_part_then_reads_every_page(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        run_job(defs, BACKFILL, DagsterInstance.ephemeral())
        sized = [r for r in fake.requests if r["body"].get("returnCount")]
        assert len(sized) == 2 and fake.requests[0]["body"].get("returnCount")


def test_a_repeated_backfill_resumes_past_the_parts_already_done(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        inst = DagsterInstance.ephemeral()
        run_job(defs, BACKFILL, inst)
        before = len(fake.requests)
        again = run_job(defs, BACKFILL, inst)
        assert again.success
        meta = metadata_of(again, RESULTS)
        assert meta["parts_skipped"] == 2 and meta["parts_loaded"] == 0
        assert not [r for r in fake.requests[before:] if not r["body"].get("returnCount")]


def test_control_the_restart_tag_reads_every_part_again(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        inst = DagsterInstance.ephemeral()
        run_job(defs, BACKFILL, inst)
        again = run_job(defs, BACKFILL, inst, tags={"dagtools/systemlink_restart": "true"})
        assert again.success and metadata_of(again, RESULTS)["parts_loaded"] == 2
        assert len(table(tmp_path, "sl_results")) == 5  # merged, not duplicated


def test_a_backfill_interrupted_after_one_part_resumes_with_the_other(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        inst = DagsterInstance.ephemeral()
        # Let the sizing pass and part A through, then refuse part B's page.
        fake.failures = []
        original = fake._results

        def flaky(h, body):
            subs = body.get("substitutions") or []
            if subs and subs[0] == "PN-1000-B" and not body.get("returnCount") and not getattr(fake, "healed", False):
                return fake._send(h, 401, {})
            return original(h, body)

        fake._results = flaky
        failed = run_job(defs, BACKFILL, inst)
        assert not failed.success
        assert len(table(tmp_path, "sl_results")) == 3  # part A landed
        fake.healed = True
        fixed = run_job(defs, BACKFILL, inst)
        assert fixed.success
        meta = metadata_of(fixed, RESULTS)
        assert meta["parts_skipped"] == 1 and meta["parts_loaded"] == 1
        assert len(table(tmp_path, "sl_results")) == 5


def test_the_incremental_job_is_never_a_backfill(tmp_path, monkeypatch, dlt_home):
    rows, steps = world()
    with FakeSystemLink(rows, steps) as fake:
        defs = load_example(tmp_path, monkeypatch, base_url=fake.base_url)
        run_job(defs, "sl_example_incremental", DagsterInstance.ephemeral())
        assert not [r for r in fake.requests if r["body"].get("returnCount")]
