"""Tests for build_run_record — event-log -> RunRecord flattening."""
from dag_tools.qual.classes import Representative, Runnability
from dag_tools.qual.graphql import EventLogEntry, RunStatusInfo
from dag_tools.qual.runs.launcher import build_run_record


def _rep():
    return Representative(
        repo="patriot", git_sha="abc123", asset_key=["hello"],
        runnability=Runnability.RUNNABLE,
        runnability_reason="default",
    )


def _success_status():
    return RunStatusInfo(
        run_id="r1", status="SUCCESS",
        start_time=1718712000.0, end_time=1718712100.0,
    )


def test_build_run_record_extracts_materializations_and_metadata_keys():
    events = [
        EventLogEntry(
            event_type="MaterializationEvent",
            asset_key=["hello"], step_key="step1",
            metadata_keys=["row_count", "schema_version"],
        ),
    ]
    rec = build_run_record(
        qual_id="q1", side="baseline", class_hash="h1",
        rep=_rep(), run_status=_success_status(), events=events,
    )
    assert rec.success is True
    assert rec.materialization_events[0].asset_key == ["hello"]
    assert set(rec.metadata_keys) == {"row_count", "schema_version"}


def test_build_run_record_computes_duration():
    rec = build_run_record(
        qual_id="q1", side="baseline", class_hash="h1",
        rep=_rep(), run_status=_success_status(), events=[],
    )
    assert rec.duration_seconds == 100.0


def test_build_run_record_captures_failure_step_keys_and_error():
    events = [
        EventLogEntry(
            event_type="ExecutionStepFailureEvent",
            message="step blew up",
            step_key="failing_step",
        ),
    ]
    failed_status = RunStatusInfo(run_id="r1", status="FAILURE")
    rec = build_run_record(
        qual_id="q1", side="baseline", class_hash="h1",
        rep=_rep(), run_status=failed_status, events=events,
    )
    assert rec.success is False
    assert rec.failure_step_keys == ["failing_step"]
    assert rec.error == "step blew up"


def test_build_run_record_unions_metadata_keys_across_events():
    """Two materialization events emit a union, not duplicates, of keys."""
    events = [
        EventLogEntry(
            event_type="MaterializationEvent", asset_key=["a"],
            metadata_keys=["row_count", "checksum"],
        ),
        EventLogEntry(
            event_type="MaterializationEvent", asset_key=["a"],
            metadata_keys=["checksum", "size"],
        ),
    ]
    rec = build_run_record(
        qual_id="q1", side="baseline", class_hash="h1",
        rep=_rep(), run_status=_success_status(), events=events,
    )
    assert sorted(rec.metadata_keys) == ["checksum", "row_count", "size"]


def test_build_run_record_soft_fails_on_malformed_event():
    """One bad event mustn't drop the whole record."""
    good_event = EventLogEntry(
        event_type="MaterializationEvent",
        asset_key=["hello"], step_key="step1",
        metadata_keys=["x"],
    )

    class _BadEvent:
        @property
        def event_type(self):
            raise RuntimeError("boom")
        @property
        def asset_key(self):
            return None
        message = None
        timestamp = None
        step_key = None
        metadata_keys = []
        raw = {}

    rec = build_run_record(
        qual_id="q1", side="baseline", class_hash="h1",
        rep=_rep(), run_status=_success_status(),
        events=[_BadEvent(), good_event],
    )
    assert len(rec.materialization_events) == 1
    assert rec.metadata_keys == ["x"]


def test_build_run_record_event_count_reflects_input():
    events = [
        EventLogEntry(event_type=f"E{i}") for i in range(5)
    ]
    rec = build_run_record(
        qual_id="q1", side="baseline", class_hash="h1",
        rep=_rep(), run_status=_success_status(), events=events,
    )
    assert rec.event_count == 5
