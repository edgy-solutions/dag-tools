"""The PDM request/response conversation: MEI request, control handshake.

The original example drained whatever PDM had already flagged. This adds
the two directions that make it a conversation, and both have failure
modes that only show up as a sequence:

  * we write the MEI list PDM should explode;
  * PDM writes STARTED, fills a dozen tables, writes COMPLETED;
  * we extract, ack, and append our own completion row.

The load-is-whole problem is the reason the control table exists. Polling
staging for a row count cannot tell "PDM finished" from "PDM is a third
of the way through committing", so a cycle driven off counts extracts a
partial load and acknowledges it as complete. Only the COMPLETED row is
trustworthy, and the tests below pin that the sensor waits for it.

Oracle is stood in for by sqlite, as in test_pdm_oracle_cycle: same SQL
semantics for the INSERT/DELETE/MAX these paths issue, and oracledb is
patched at the connect boundary so the real handler bodies run.
"""
import asyncio
import json
import sqlite3
from unittest.mock import patch

import pytest

pytest.importorskip("restate")
pytest.importorskip("oracledb")

# restate_dlt_sync.component imports DagsterDltResource at module scope.
pytest.importorskip("dagster_dlt")

from dag_tools.restate_handlers import oracle_control
from dag_tools.components.restate_dlt_sync.component import (
    ack_query,
    build_table_hints,
    latest_completed_query,
    latest_done_query,
    load_mei_list,
)
from dag_tools.components.restate_dlt_sync.config import (
    ControlTableSpec,
    MeiTableSpec,
    TableSpec,
)


CONTROL = ControlTableSpec(
    name="PDM_CONTROL",
    status_column="LOAD_STATUS",
    completed_value="COMPLETED",
    consumer_done_value="CONSUMED",
    timestamp_column="LOAD_TS",
    load_type_column="LOAD_TYPE",
    started_value="STARTED",
)


class FakeContext:
    """Minimal restate.Context double: runs side-effects inline."""

    async def run(self, name, fn):
        return fn()


class _CtxMgr:
    def __init__(self, target):
        self._target = target

    def __enter__(self):
        return self._target

    def __exit__(self, *_):
        return False


class FakeOracleCursor:
    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql, binds=None):
        self._cur.execute(sql, binds or {})

    def executemany(self, sql, rows):
        self._cur.executemany(sql, rows)


class FakeOracleConnection:
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return _CtxMgr(FakeOracleCursor(self._conn.cursor()))

    def commit(self):
        self._conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv("ORACLE_DSN", "fake")
    monkeypatch.setenv("ORACLE_USER", "fake")
    monkeypatch.setenv("ORACLE_PASSWORD", "fake")
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE PDM_MEI_REQUEST (MEI_NUMBER TEXT, REQUESTED_BY TEXT)")
    conn.execute(
        "CREATE TABLE PDM_CONTROL ("
        "  LOAD_STATUS TEXT, LOAD_TYPE TEXT, LOAD_TS TIMESTAMP)"
    )
    conn.commit()
    return conn


def _run(handler, payload, db):
    with patch.object(
        oracle_control.oracledb, "connect",
        return_value=FakeOracleConnection(db),
    ):
        return asyncio.run(handler(FakeContext(), payload))


# ---------------------------------------------------------------------------
# MEI request -- the write that starts a transaction
# ---------------------------------------------------------------------------


def test_mei_request_writes_the_list(db):
    result = _run(oracle_control.write_mei_request, {
        "table_name": "PDM_MEI_REQUEST",
        "mei_column": "MEI_NUMBER",
        "mei_values": ["MEI-1", "MEI-2", "MEI-3"],
    }, db)

    assert result == {"rows_written": 3}
    rows = [r[0] for r in db.execute("SELECT MEI_NUMBER FROM PDM_MEI_REQUEST")]
    assert sorted(rows) == ["MEI-1", "MEI-2", "MEI-3"]


def test_mei_request_replaces_rather_than_accumulates(db):
    """The table must state the CURRENT request. Appending would make PDM
    re-explode every MEI ever asked for, which on a delta load is the
    difference between a small update and a full rebuild."""
    _run(oracle_control.write_mei_request, {
        "table_name": "PDM_MEI_REQUEST", "mei_column": "MEI_NUMBER",
        "mei_values": ["MEI-1", "MEI-2"],
    }, db)
    _run(oracle_control.write_mei_request, {
        "table_name": "PDM_MEI_REQUEST", "mei_column": "MEI_NUMBER",
        "mei_values": ["MEI-9"],
    }, db)

    rows = [r[0] for r in db.execute("SELECT MEI_NUMBER FROM PDM_MEI_REQUEST")]
    assert rows == ["MEI-9"]


def test_mei_request_can_append_when_asked(db):
    _run(oracle_control.write_mei_request, {
        "table_name": "PDM_MEI_REQUEST", "mei_column": "MEI_NUMBER",
        "mei_values": ["MEI-1"],
    }, db)
    _run(oracle_control.write_mei_request, {
        "table_name": "PDM_MEI_REQUEST", "mei_column": "MEI_NUMBER",
        "mei_values": ["MEI-2"], "replace": False,
    }, db)

    rows = sorted(r[0] for r in db.execute("SELECT MEI_NUMBER FROM PDM_MEI_REQUEST"))
    assert rows == ["MEI-1", "MEI-2"]


def test_mei_request_dedupes(db):
    result = _run(oracle_control.write_mei_request, {
        "table_name": "PDM_MEI_REQUEST", "mei_column": "MEI_NUMBER",
        "mei_values": ["MEI-1", "MEI-2", "MEI-1"],
    }, db)
    assert result == {"rows_written": 2}


def test_mei_request_carries_extra_columns(db):
    _run(oracle_control.write_mei_request, {
        "table_name": "PDM_MEI_REQUEST", "mei_column": "MEI_NUMBER",
        "mei_values": ["MEI-1"],
        "extra_columns": {"REQUESTED_BY": "dagster"},
    }, db)
    assert db.execute(
        "SELECT MEI_NUMBER, REQUESTED_BY FROM PDM_MEI_REQUEST"
    ).fetchone() == ("MEI-1", "dagster")


def test_mei_request_rejects_an_empty_list(db):
    """An empty request would DELETE the table and leave every MEI-scoped
    table unpopulated -- a silent no-data outcome, not an error."""
    with pytest.raises(ValueError, match="Missing required payload"):
        _run(oracle_control.write_mei_request, {
            "table_name": "PDM_MEI_REQUEST", "mei_column": "MEI_NUMBER",
            "mei_values": [],
        }, db)


# ---------------------------------------------------------------------------
# Completion signal
# ---------------------------------------------------------------------------


def test_signal_load_complete_appends_our_row(db):
    _run(oracle_control.signal_load_complete, {
        "table_name": "PDM_CONTROL",
        "status_column": "LOAD_STATUS",
        "status_value": "CONSUMED",
        "timestamp_column": "LOAD_TS",
        "load_type_column": "LOAD_TYPE",
        "load_type": "DELTA",
    }, db)

    row = db.execute(
        "SELECT LOAD_STATUS, LOAD_TYPE, LOAD_TS IS NOT NULL FROM PDM_CONTROL"
    ).fetchone()
    assert row == ("CONSUMED", "DELTA", 1)


def test_signal_load_complete_requires_the_status(db):
    with pytest.raises(ValueError, match="Missing required payload"):
        _run(oracle_control.signal_load_complete, {
            "table_name": "PDM_CONTROL", "status_column": "LOAD_STATUS",
        }, db)


# ---------------------------------------------------------------------------
# The handshake -- what the cycle sensor asks the control table
# ---------------------------------------------------------------------------


def _should_fire(db, control=CONTROL):
    """Run exactly the two queries the sensor runs, and apply its rule."""
    completed = db.execute(
        latest_completed_query(control).replace(":completed_value", "?"),
        (control.completed_value, control.completed_value),
    ).fetchone()
    done_at = db.execute(
        latest_done_query(control).replace(":done_value", "?"),
        (control.consumer_done_value,),
    ).fetchone()[0]

    if not completed or completed[0] is None:
        return False, None
    completed_at, load_type = completed[0], completed[1]
    if done_at is not None and not (completed_at > done_at):
        return False, load_type
    return True, load_type


def _control_row(db, status, ts, load_type=None):
    db.execute(
        "INSERT INTO PDM_CONTROL (LOAD_STATUS, LOAD_TYPE, LOAD_TS) VALUES (?,?,?)",
        (status, load_type, ts),
    )
    db.commit()


def test_no_completed_row_means_no_cycle(db):
    """PDM has started but not finished. Firing here is the partial-load
    bug the control table exists to prevent."""
    _control_row(db, "STARTED", "2026-08-19 10:00:00", "FULL")
    fire, _ = _should_fire(db)
    assert fire is False


def test_completed_row_triggers_the_cycle_and_carries_load_type(db):
    _control_row(db, "STARTED", "2026-08-19 10:00:00", "FULL")
    _control_row(db, "COMPLETED", "2026-08-19 10:05:00", "FULL")
    fire, load_type = _should_fire(db)
    assert fire is True
    assert load_type == "FULL", "the run needs to know FULL vs DELTA"


def test_consuming_settles_the_cycle(db):
    """After our row lands the sensor must go quiet, or the same load is
    re-extracted and re-acked every interval."""
    _control_row(db, "COMPLETED", "2026-08-19 10:05:00", "FULL")
    assert _should_fire(db)[0] is True

    _run(oracle_control.signal_load_complete, {
        "table_name": "PDM_CONTROL", "status_column": "LOAD_STATUS",
        "status_value": "CONSUMED", "timestamp_column": "LOAD_TS",
    }, db)

    assert _should_fire(db)[0] is False


def test_the_next_delta_load_fires_again(db):
    """Settled is not finished: the cycle has to re-arm for the next load."""
    _control_row(db, "COMPLETED", "2026-08-19 10:05:00", "FULL")
    _control_row(db, "CONSUMED", "2026-08-19 10:09:00")
    assert _should_fire(db)[0] is False

    _control_row(db, "COMPLETED", "2026-08-19 11:00:00", "DELTA")
    fire, load_type = _should_fire(db)
    assert fire is True
    assert load_type == "DELTA"


def test_a_simultaneous_consumer_row_does_not_retrigger(db):
    """Equal timestamps mean our row closed that COMPLETED, not that a
    newer load arrived. The comparison must be strict."""
    _control_row(db, "COMPLETED", "2026-08-19 10:05:00", "FULL")
    _control_row(db, "CONSUMED", "2026-08-19 10:05:00")
    assert _should_fire(db)[0] is False


# ---------------------------------------------------------------------------
# Per-table index + cursor
# ---------------------------------------------------------------------------


def test_each_table_gets_its_own_index_and_cursor():
    """A dozen tables do not share one primary key. Before this, the
    component took a single pipeline-wide primary_key and applied it to
    every table."""
    hints = build_table_hints(
        {
            "PDM_PART": TableSpec(primary_key="PART_ID", cursor="LAST_MODIFIED"),
            "PDM_BOM": TableSpec(primary_key=["ASSY_ID", "SEQ"], cursor="CHANGED_ON"),
        },
        {},
    )
    assert hints["PDM_PART"] == {
        "primary_key": "PART_ID",
        "incremental": {"cursor_path": "LAST_MODIFIED"},
    }
    assert hints["PDM_BOM"]["primary_key"] == ["ASSY_ID", "SEQ"]
    assert hints["PDM_BOM"]["incremental"] == {"cursor_path": "CHANGED_ON"}


def test_a_table_without_a_cursor_gets_no_incremental():
    hints = build_table_hints({"T": TableSpec(primary_key="ID")}, {})
    assert "incremental" not in hints["T"]


def test_initial_value_rides_along_when_set():
    hints = build_table_hints(
        {"T": TableSpec(primary_key="ID", cursor="TS", initial_value="2026-01-01")},
        {},
    )
    assert hints["T"]["incremental"] == {
        "cursor_path": "TS", "initial_value": "2026-01-01",
    }


def test_explicit_hints_win_over_generated_ones():
    """table_config is the front door; hints stays the escape hatch for
    anything dlt supports that has no dedicated field."""
    hints = build_table_hints(
        {"T": TableSpec(primary_key="ID", cursor="TS")},
        {"T": {"write_disposition": "replace", "primary_key": "OVERRIDE"}},
    )
    assert hints["T"]["primary_key"] == "OVERRIDE"
    assert hints["T"]["write_disposition"] == "replace"
    assert hints["T"]["incremental"] == {"cursor_path": "TS"}


# ---------------------------------------------------------------------------
# Scoping the acknowledgment read-back
# ---------------------------------------------------------------------------


def test_first_ack_reads_the_whole_table():
    """No high-water mark yet means nothing has been acked, so everything
    present is fair game. This is the original behaviour, kept for the
    first cycle and for recovery after a failed one."""
    sql = ack_query("pdm_raw", "pdm_part", "PART_ID", None)
    assert "WHERE" not in sql
    assert "_dlt_load_id" in sql, "the mark still has to be readable"


def test_later_acks_are_scoped_to_new_loads():
    """Unscoped, this read returned the ENTIRE destination table every
    cycle. Under merge the destination accumulates, so the ack payload
    grew without bound and the stats row counted all-time rather than the
    batch."""
    sql = ack_query("pdm_raw", "pdm_part", "PART_ID", "1787253108.25")
    assert "WHERE _dlt_load_id > :since" in sql


def test_load_ids_sort_lexically_the_way_they_sort_numerically():
    """The scoping compares load ids as text. dlt writes them as
    str(time.time()), so this only holds while the integer part is a fixed
    width -- true until the year 2286, and both sides of the comparison
    come from that same column."""
    ids = ["1787253108.2521412", "1787253109.9780922", "1787253110.1"]
    assert sorted(ids) == ids
    assert max(ids) == "1787253110.1"


def test_a_real_dlt_load_id_column_is_what_we_scope_on():
    """Pins the column name against dlt renaming it: the scoping silently
    degrades to a full-table ack if this is wrong, which looks like
    working software."""
    from dag_tools.components.restate_dlt_sync.component import DLT_LOAD_ID_COLUMN

    assert DLT_LOAD_ID_COLUMN == "_dlt_load_id"


# ---------------------------------------------------------------------------
# The MEI overlay
# ---------------------------------------------------------------------------


def test_overlay_reads_a_yaml_list(tmp_path):
    p = tmp_path / "meis.yaml"
    p.write_text("- MEI-1\n- MEI-2\n")
    assert load_mei_list(str(p), []) == ["MEI-1", "MEI-2"]


def test_overlay_reads_a_wrapped_yaml_mapping(tmp_path):
    p = tmp_path / "meis.yaml"
    p.write_text("meis:\n  - MEI-1\n  - MEI-2\n")
    assert load_mei_list(str(p), []) == ["MEI-1", "MEI-2"]


def test_overlay_reads_json(tmp_path):
    p = tmp_path / "meis.json"
    p.write_text(json.dumps(["MEI-1", "MEI-2"]))
    assert load_mei_list(str(p), []) == ["MEI-1", "MEI-2"]


def test_overlay_reads_plain_text_with_comments(tmp_path):
    p = tmp_path / "meis.txt"
    p.write_text("# top level items\nMEI-1\n\n  MEI-2  \n")
    assert load_mei_list(str(p), []) == ["MEI-1", "MEI-2"]


def test_missing_overlay_says_what_it_is(tmp_path):
    """A silent empty list here would clear the MEI table and produce a
    successful run that loaded nothing."""
    with pytest.raises(FileNotFoundError, match="Major End Items"):
        load_mei_list(str(tmp_path / "absent.yaml"), [])


def test_inline_meis_are_the_fallback():
    assert load_mei_list(None, ["MEI-1", " MEI-2 "]) == ["MEI-1", "MEI-2"]


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def test_consumer_status_must_differ_from_completed():
    """Identical values make our own closing row look like a fresh load
    from PDM, and the cycle never settles."""
    with pytest.raises(ValueError, match="must differ from"):
        ControlTableSpec(
            name="C", status_column="S", completed_value="DONE",
            consumer_done_value="DONE", timestamp_column="TS",
        )


def test_control_table_requires_a_timestamp_column():
    """Without an ordering there is no way to tell a new COMPLETED from
    the one just consumed."""
    with pytest.raises(ValueError):
        ControlTableSpec(
            name="C", status_column="S", completed_value="DONE",
            consumer_done_value="CONSUMED",
        )


def test_unknown_config_keys_are_rejected():
    """A typo in a twelve-table config should fail at load, not read as
    configured while doing nothing."""
    with pytest.raises(ValueError):
        TableSpec(primary_key="ID", curser="TS")
    with pytest.raises(ValueError):
        MeiTableSpec(name="M", mei_column="C", sourcefile="/x")


# ---------------------------------------------------------------------------
# A request row is more than one column
# ---------------------------------------------------------------------------
#
# The request table often needs several columns per row: an identifier that
# varies, values identical on every row, and values that are usually one
# thing but occasionally stated. Only the first was expressible -- the rest
# had to be constants applied uniformly, which is not what "usually this,
# sometimes that" means.

from dag_tools.restate_handlers.oracle_control import build_request_rows


def test_bare_values_go_in_the_key_column():
    rows = build_request_rows(["A", "B"], key_column="K")
    assert rows == [{"K": "A"}, {"K": "B"}]


def test_a_mapping_entry_names_its_own_columns():
    rows = build_request_rows([{"K": "A", "R": "1"}], key_column="K")
    assert rows == [{"K": "A", "R": "1"}]


def test_defaults_fill_in_and_the_entry_wins():
    """'Usually this, occasionally stated explicitly.'"""
    rows = build_request_rows(
        [{"K": "A", "R": "9"}, {"K": "B"}], key_column="K", defaults={"R": "0"},
    )
    assert rows == [{"K": "A", "R": "9"}, {"K": "B", "R": "0"}]


def test_constants_cannot_be_overridden_by_an_entry():
    """A value that must be uniform must not be variable by editing the
    request list."""
    rows = build_request_rows(
        [{"K": "A", "C": "sneaky"}], key_column="K", constants={"C": "fixed"},
    )
    assert rows == [{"C": "fixed", "K": "A"}]


def test_every_row_carries_the_same_columns():
    """executemany binds ONE statement across the batch: a row missing a
    column another row has would bind the wrong parameters."""
    rows = build_request_rows(
        [{"K": "A", "R": "1"}, {"K": "B", "V": "2"}], key_column="K",
    )
    assert all(set(r) == {"K", "R", "V"} for r in rows), rows
    assert rows[0]["V"] is None and rows[1]["R"] is None


def test_mixed_scalars_and_mappings_are_accepted():
    rows = build_request_rows(["A", {"K": "B", "R": "1"}], key_column="K")
    assert [r["K"] for r in rows] == ["A", "B"]


def test_duplicate_keys_collapse():
    rows = build_request_rows([{"K": "A"}, {"K": "A"}, {"K": "B"}], key_column="K")
    assert len(rows) == 2


def test_a_bare_value_with_no_key_column_is_refused():
    with pytest.raises(ValueError, match="key_column"):
        build_request_rows(["A"])


def test_a_mapping_missing_the_key_is_refused():
    with pytest.raises(ValueError, match="no value for key_column"):
        build_request_rows([{"R": "1"}], key_column="K")


def test_an_empty_request_is_refused():
    with pytest.raises(ValueError, match="zero rows"):
        build_request_rows([], key_column="K")


def test_the_handler_writes_every_column(db):
    """End to end through the real handler: several columns per row, from
    all three sources at once."""
    _run(oracle_control.write_mei_request, {
        "table_name": "PDM_MEI_REQUEST",
        "key_column": "MEI_NUMBER",
        "mei_values": [{"MEI_NUMBER": "M-1"}, {"MEI_NUMBER": "M-2"}],
        "constants": {"REQUESTED_BY": "dagster"},
    }, db)

    rows = db.execute(
        "SELECT MEI_NUMBER, REQUESTED_BY FROM PDM_MEI_REQUEST ORDER BY MEI_NUMBER"
    ).fetchall()
    assert rows == [("M-1", "dagster"), ("M-2", "dagster")]


def test_the_legacy_payload_shape_still_works(db):
    """Older callers send mei_column/extra_columns."""
    result = _run(oracle_control.write_mei_request, {
        "table_name": "PDM_MEI_REQUEST",
        "mei_column": "MEI_NUMBER",
        "mei_values": ["M-1"],
        "extra_columns": {"REQUESTED_BY": "legacy"},
    }, db)
    assert result == {"rows_written": 1}
    assert db.execute(
        "SELECT MEI_NUMBER, REQUESTED_BY FROM PDM_MEI_REQUEST"
    ).fetchone() == ("M-1", "legacy")
