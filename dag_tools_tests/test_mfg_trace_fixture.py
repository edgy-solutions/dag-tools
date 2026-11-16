"""The synthetic BW-shaped fixture (`examples/mfg_trace/bw`), extracted for real.

Scope (ruling 2026-11-16): this is the RAW layer. Five things are planted;
three have an extraction-side exerciser and are proven here -- the version
filter, the faithful copy, and scd2 history. Two are planted and asserted to
SURVIVE extraction unchanged, in both directions:

  * the blank-key row: dropping it is the curated layer's `where`;
  * the open-end sentinel date: turning it into a null is the curated layer's
    `nullif`.

Both curated halves are PR D's (the as-built curated layer is not built yet)
and are proven in `contract_mapping`'s own suites. That is a named deferral,
not a silence.

Every table, column and key name is read out of the example's own `seed.py`
and `component.yaml`; this module restates none of them (see
`test_the_test_names_no_column_the_example_does_not`).
"""
import importlib.util
import re
import sqlite3
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip("dlt")
pytest.importorskip("dagster_dlt")
pytest.importorskip("duckdb")
yaml = pytest.importorskip("yaml")

import dagster as dg
import dlt
import duckdb
from dagster.components import build_component_defs

from dag_tools.asset_wrappers import dlt_assets_parsing

REPO = Path(__file__).resolve().parent.parent
EXAMPLE = REPO / "examples" / "mfg_trace" / "bw"


def _import_seed():
    spec = importlib.util.spec_from_file_location("bw_example_seed", EXAMPLE / "seed.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


seed = _import_seed()
M, A = seed.MASTER, seed.ASBUILT
PLANT_NAMES = list(seed.PLANTS)
CONTROLS = {"master": seed.CONTROL, "asbuilt": seed.CONTROL_ASBUILT}


def _names():
    """Every table and column name the fixture declares."""
    return sorted({v for d in (M, A) for v in d.values()})


def _low(name):
    return name.lower()  # dlt snake-cases identifiers into the destination


@pytest.fixture(scope="module")
def loaded(tmp_path_factory):
    """Two real extractions into duckdb; returns what the checks need."""
    tmp = tmp_path_factory.mktemp("bw")
    db = Path(seed.seed(tmp / "source.db"))
    wh = (tmp / "w.duckdb").as_posix()
    dest = dlt.destinations.duckdb(wh)
    mp = pytest.MonkeyPatch()
    mp.setattr(dlt_assets_parsing, "get_destination", lambda *a, **k: dest)
    mp.setenv("BW_EXAMPLE_SOURCE_DB", f"sqlite:///{db.as_posix()}")
    mp.setenv("BW_EXAMPLE_DB", wh)
    mp.chdir(tmp)
    mp.syspath_prepend(str(tmp))

    def run_once():
        # A fresh component per load, as a Dagster run process gets.
        pkg = "pkg_" + uuid.uuid4().hex[:8]
        comp = tmp / pkg / "components"
        (comp / "dlt").mkdir(parents=True)
        (tmp / pkg / "__init__.py").write_text("")
        (comp / "dlt" / "component.yaml").write_text(
            (EXAMPLE / "component.yaml").read_text(encoding="utf-8"), encoding="utf-8"
        )
        defs = build_component_defs(comp)
        result = dg.materialize(
            list(defs.assets), resources=defs.resources, raise_on_error=False
        )
        assert result.success

    def source(table, columns):
        c = sqlite3.connect(db)
        try:
            return c.execute(f"select {', '.join(columns)} from {table}").fetchall()
        finally:
            c.close()

    try:
        source_master = source(M["table"], [M["key"], M["version"], M["text"]])
        run_once()
        seed.apply_second_load(db)
        source_asbuilt_after = source(A["table"], [A["instance"]])
        run_once()
    finally:
        mp.undo()

    con = duckdb.connect(wh, read_only=True)
    try:
        master_cols = ", ".join(_low(c) for c in (M["key"], M["version"], M["valid_to"], M["text"]))
        state = {
            "source_master": source_master,
            "source_asbuilt_keys_after": {r[0] for r in source_asbuilt_after},
            "master": con.execute(f"select {master_cols} from raw.{_low(M['table'])}").fetchall(),
            "asbuilt": con.execute(
                f"select {_low(A['instance'])}, {_low(A['part'])}, _dlt_valid_to is null "
                f"from raw.{_low(A['table'])}"
            ).fetchall(),
        }
    finally:
        con.close()
    return state


# --------------------------------------------------------------------------
# the five symptoms; each is observable on exactly one planted key
# --------------------------------------------------------------------------
def _inactive_dropped(s, key):
    had = any(k == key and v != seed.ACTIVE for k, v, _ in s["source_master"])
    mine = [r for r in s["master"] if r[0] == key]
    return had and bool(mine) and all(r[1] == seed.ACTIVE for r in mine)


def _sentinel_kept(s, key):
    return any(r[0] == key and r[2] == seed.SENTINEL for r in s["master"])


def _blank_kept(s, key):
    return key.strip() == "" and any(r[0] == key for r in s["master"])


def _absent_stays_open(s, key):
    mine = [r for r in s["asbuilt"] if r[0] == key]
    return (
        key not in s["source_asbuilt_keys_after"]
        and bool(mine)
        and all(r[2] for r in mine)
    )


def _replaced(s, key):
    mine = sorted(r for r in s["asbuilt"] if r[0] == key)
    return (
        len(mine) == 2
        and sorted(r[2] for r in mine) == [False, True]
        and mine[0][1] != mine[1][1]
    )


CHECKS = {
    "version_mix": _inactive_dropped,
    "open_end_sentinel": _sentinel_kept,
    "blank_key": _blank_kept,
    "absent_from_second_load": _absent_stays_open,
    "replaced_child": _replaced,
}


def test_the_checks_cover_exactly_the_plants():
    assert set(CHECKS) == set(seed.PLANTS)


@pytest.mark.parametrize("check", PLANT_NAMES)
@pytest.mark.parametrize("plant", PLANT_NAMES)
def test_every_plant_trips_exactly_its_own_case(loaded, plant, check):
    key = seed.PLANTS[plant]["key"]
    assert CHECKS[check](loaded, key) is (plant == check)


@pytest.mark.parametrize("check", PLANT_NAMES)
@pytest.mark.parametrize("table", ["master", "asbuilt"])
def test_the_control_key_trips_nothing(loaded, table, check):
    assert CHECKS[check](loaded, CONTROLS[table]["key"]) is False


def test_the_control_key_is_present_in_raw(loaded):
    """Otherwise 'trips nothing' is what an empty load also says."""
    assert any(r[0] == seed.CONTROL["key"] for r in loaded["master"])
    assert any(r[0] == seed.CONTROL_ASBUILT["key"] for r in loaded["asbuilt"])


def test_the_control_is_not_a_plant():
    plant_keys = {p["key"] for p in seed.PLANTS.values()}
    assert seed.CONTROL["key"] not in plant_keys
    assert seed.CONTROL_ASBUILT["key"] not in plant_keys
    assert "control" not in seed.PLANTS


# --------------------------------------------------------------------------
# what the raw layer must hold
# --------------------------------------------------------------------------
def test_the_blank_key_row_survives_extraction_unfiltered(loaded):
    key = seed.PLANTS["blank_key"]["key"]
    assert [r for r in loaded["master"] if r[0] == key], "raw must still carry it"
    # ...and the filter did run in the same load, so this cannot pass on a
    # load where no filter was applied at all.
    assert _inactive_dropped(loaded, seed.PLANTS["version_mix"]["key"])


def test_the_sentinel_end_date_reaches_raw_as_the_sentinel(loaded):
    key = seed.PLANTS["open_end_sentinel"]["key"]
    values = [r[2] for r in loaded["master"] if r[0] == key]
    assert values == [seed.SENTINEL]
    assert None not in values


def test_the_inactive_versions_are_absent_from_raw_and_the_active_ones_are_present(loaded):
    source_active = {(k, t) for k, v, t in loaded["source_master"] if v == seed.ACTIVE}
    source_inactive = {(k, t) for k, v, t in loaded["source_master"] if v != seed.ACTIVE}
    raw = {(r[0], r[3]) for r in loaded["master"]}
    assert source_inactive, "the fixture must plant at least one inactive row"
    assert source_active
    assert raw == source_active
    assert not (raw & source_inactive)


def test_a_key_absent_from_the_second_load_keeps_its_open_version(loaded):
    key = seed.PLANTS["absent_from_second_load"]["key"]
    assert key not in loaded["source_asbuilt_keys_after"]
    assert [r for r in loaded["asbuilt"] if r[0] == key] != []
    assert _absent_stays_open(loaded, key)


def test_a_replaced_child_has_one_closed_version_and_one_open_one(loaded):
    key = seed.PLANTS["replaced_child"]["key"]
    mine = [r for r in loaded["asbuilt"] if r[0] == key]
    assert len(mine) == 2
    assert sorted(r[2] for r in mine) == [False, True]
    assert len({r[1] for r in mine}) == 2


# --------------------------------------------------------------------------
# the example is wired as the fixture says
# --------------------------------------------------------------------------
def _yaml():
    return yaml.safe_load((EXAMPLE / "component.yaml").read_text(encoding="utf-8"))


def test_the_components_names_are_the_fixtures_names():
    pipe = next(iter(_yaml()["attributes"]["pipelines"].values()))
    assert set(pipe["sources"]) == {M["table"], A["table"]}
    flt = pipe["row_filters"][M["table"]]
    assert [f["column"] for f in flt] == [M["version"]]
    assert flt[0]["value"] == seed.ACTIVE
    cfg = pipe["table_config"]
    assert set(cfg) == {A["table"]}
    assert cfg[A["table"]]["primary_key"] == [A["instance"], A["position"]]
    assert cfg[A["table"]]["history"] == "scd2"
    assert "cursor" not in cfg[A["table"]]
    assert "ingest_runs" not in pipe
    # the blank-key row is not filtered at extraction
    assert all(f["column"] != M["key"] for f in flt)


def test_the_seed_script_runs_standalone(tmp_path):
    out = subprocess.run(
        [sys.executable, str(EXAMPLE / "seed.py"), str(tmp_path / "s.db")],
        capture_output=True, text=True, cwd=tmp_path,
    )
    assert out.returncode == 0, out.stderr
    assert (tmp_path / "s.db").exists()


# --------------------------------------------------------------------------
# vocabulary: the library names nothing, the test names nothing
# --------------------------------------------------------------------------
def _word(name):
    return re.compile(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])", re.IGNORECASE)


def test_the_test_names_no_column_the_example_does_not():
    source = Path(__file__).read_text(encoding="utf-8")
    names = _names()
    assert len(names) >= 10, "the fixture must declare names for this to prove anything"
    assert [n for n in names if _word(n).search(source)] == []


def test_the_library_names_nothing_the_fixture_carries():
    names = _names()
    assert len(names) >= 10
    patterns = [(n, _word(n)) for n in names]
    hits = []
    scanned = 0
    for path in (REPO / "dag_tools").rglob("*"):
        if not path.is_file() or path.suffix in {".pyc", ".db", ".duckdb"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        scanned += 1
        hits += [(path.name, n) for n, p in patterns if p.search(text)]
    assert scanned > 100
    assert hits == []
