"""`DocumentTablesComponent` -- the YAML surface, loaded the way a deployment loads it.

Everything goes through `build_component_defs` over a copy of the shipped
example, because the failures that matter here are the ones a unit test of the
class cannot see: a key the loader rejects, an asset it drops, a pool that is
never given its limit. Every document name in these fixtures is the synthetic
vocabulary of `documents_synthetic`.

On the 1.10.19 compat floor a component file with an unknown top-level key is
refused outright; the loader helper skips (naming the field) only for that
exact error and lets anything else fail.
"""
from __future__ import annotations

import re
import shutil
from pathlib import Path

import dagster as dg
import pytest
import yaml
from dagster.components import build_component_defs

pytest.importorskip("lance")
pytest.importorskip("lxml")

import lance  # noqa: E402

from dag_tools.components.document_tables.component import (  # noqa: E402
    BUNDLE_TAG,
    partition_key_for,
    pool_for,
)
from dag_tools.documents.sources import open_source  # noqa: E402
from dag_tools_tests import documents_synthetic as syn  # noqa: E402

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "document_tables"
NAME = "document_tables_example"
PARTITIONS = {"2026-03_depot-a": "flat", "2026-04_depot-b": "numbered"}
KEYS = syn.TABLES + ["_rejects"]


def _chain_text(exc: BaseException) -> str:
    parts, seen = [], set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _skip_if_the_component_file_is_refused(exc: Exception) -> None:
    text = _chain_text(exc)
    if "extra_forbidden" in text or "Extra inputs are not permitted" in text:
        pytest.skip(f"this Dagster rejects a key in the component file: {text}")


@pytest.fixture()
def example(tmp_path, monkeypatch) -> Path:
    """A copy of the whole example (bundles included), `dagster_home` renamed
    per test so a cached module never hides a YAML edit."""
    root = tmp_path / "ex"
    shutil.copytree(EXAMPLE, root, ignore=shutil.ignore_patterns("__pycache__", "output"))
    home = root / tmp_path.name.replace("-", "_")
    (root / "dagster_home").rename(home)
    monkeypatch.syspath_prepend(str(root))
    return home / "components"


def _load(components_dir: Path) -> dg.Definitions:
    try:
        return build_component_defs(components_dir)
    except Exception as exc:
        _skip_if_the_component_file_is_refused(exc)
        raise


def _yaml(c: Path) -> Path:
    return c / "document_tables" / "component.yaml"


def _mapping(c: Path) -> Path:
    return c / "document_tables" / "mapping.yaml"


def _root(c: Path) -> Path:
    return c.parents[1]


def _output(c: Path) -> Path:
    return _root(c) / "output"


def _bundles(c: Path) -> Path:
    return _root(c) / "bundles"


def _rewrite(c: Path, fn) -> None:
    """Edit the component file's `attributes` as data (comments are dropped)."""
    doc = yaml.safe_load(_yaml(c).read_text(encoding="utf-8"))
    fn(doc["attributes"])
    _yaml(c).write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")


def _tables_asset(defs: dg.Definitions):
    return next(a for a in defs.assets if a.op.name == f"{NAME}_tables")


def _run(defs, instance, partition, job=f"{NAME}_ingest", tags=None, raise_on_error=True):
    if partition not in set(instance.get_dynamic_partitions(f"{NAME}_bundles")):
        instance.add_dynamic_partitions(f"{NAME}_bundles", [partition])
    return defs.get_job_def(job).execute_in_process(
        instance=instance, partition_key=partition, tags=tags, raise_on_error=raise_on_error
    )


def _mat_metadata(result) -> dict:
    out = {}
    for ev in result.get_asset_materialization_events():
        m = ev.step_materialization_data.materialization
        out[m.asset_key.path[-1]] = {k: v.value for k, v in m.metadata.items()}
    return out


def _checks(result) -> list:
    return list(result.get_asset_check_evaluations())


def _lance_counts(c: Path) -> dict:
    return {t: lance.dataset(str(_output(c) / f"{t}.lance")).count_rows() for t in KEYS}


def _tick(sensor, instance):
    """One evaluation; apply its partition requests as the daemon would."""
    out = sensor(dg.build_sensor_context(instance=instance))
    if isinstance(out, dg.SkipReason):
        return out
    if not isinstance(out, dg.SensorResult):
        out = list(out)
        return out
    for req in out.dynamic_partitions_requests or []:
        instance.add_dynamic_partitions(req.partitions_def_name, list(req.partition_keys))
    return out


# --- 1. load ----------------------------------------------------------------


def test_the_shipped_example_loads(example):
    defs = _load(example)
    keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert keys == {f"{NAME}/{t}" for t in KEYS}
    assert {j.name for j in defs.jobs} >= {f"{NAME}_ingest", f"{NAME}_maintenance"}
    sensor = defs.get_sensor_def(f"{NAME}_discovery")
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
    assert not defs.schedules


def test_a_cron_adds_a_stopped_maintenance_schedule(example):
    _rewrite(example, lambda a: a["maintenance"].update(cron="0 3 * * 0"))
    defs = _load(example)
    sched = defs.get_schedule_def(f"{NAME}_maintenance_schedule")
    assert sched.default_status == dg.DefaultScheduleStatus.STOPPED
    assert sched.cron_schedule == "0 3 * * 0"


def test_the_assets_are_partitioned_by_bundle(example):
    asset = _tables_asset(_load(example))
    assert isinstance(asset.partitions_def, dg.DynamicPartitionsDefinition)
    assert asset.partitions_def.name == f"{NAME}_bundles"


# --- 2. materialize ----------------------------------------------------------


@pytest.mark.parametrize("partition,variant", sorted(PARTITIONS.items()))
def test_a_partition_materializes_with_the_expected_rows_and_passing_checks(example, partition, variant):
    defs = _load(example)
    with dg.instance_for_test() as instance:
        result = _run(defs, instance, partition)
        assert result.success
        md = _mat_metadata(result)
        assert set(md) == set(KEYS)
        for t in KEYS:
            assert md[t]["dagster/row_count"] == syn.EXPECTED_ROWS[t]
            assert md[t]["dagster/column_schema"].columns, t
            assert md[t]["dagster/uri"]
            assert md[t]["documents"] == 3
            assert md[t]["documents_parsed"] == 2
            assert md[t]["documents_rejected"] == 1
            assert isinstance(md[t]["duration_seconds"], (int, float))
            assert md[t]["bundle"] == partition
        assert _lance_counts(example) == syn.EXPECTED_ROWS

        assert md["photos"]["file_resolution"] == {"image": syn.expected_photo_resolution(variant)}
        assert md["_rejects"]["unknown_paths"] == 0
        assert "documents_with_unknown_paths" in md["_rejects"]

        evals = _checks(result)
        names = {(e.asset_key.path[-1], e.check_name) for e in evals}
        assert ("_rejects", "reject_rate") in names
        assert ("photos", "file_resolution_rate") in names
        assert all(e.passed for e in evals), [(e.asset_key, e.check_name) for e in evals if not e.passed]


def test_a_run_tagged_with_its_bundle_resolves_it(example):
    defs = _load(example)
    with dg.instance_for_test() as instance:
        result = _run(defs, instance, "2026-03_depot-a", tags={BUNDLE_TAG: "2026-03_depot-a"})
        assert result.success
    assert _lance_counts(example) == syn.EXPECTED_ROWS


def test_a_tag_that_disagrees_with_the_partition_fails_the_run(example):
    defs = _load(example)
    with dg.instance_for_test() as instance:
        result = _run(
            defs, instance, "2026-03_depot-a", tags={BUNDLE_TAG: "2026-04_depot-b"}, raise_on_error=False
        )
        assert not result.success


# --- 3. idempotent re-run ----------------------------------------------------


def test_rematerializing_a_partition_leaves_every_count_unchanged(example):
    defs = _load(example)
    with dg.instance_for_test() as instance:
        for part in PARTITIONS:
            assert _run(defs, instance, part).success
        first = _lance_counts(example)
        assert first == {t: 2 * syn.EXPECTED_ROWS[t] for t in KEYS}
        assert _run(defs, instance, "2026-03_depot-a").success
        assert _lance_counts(example) == first
        assert _run(defs, instance, "2026-03_depot-a").success
        assert _lance_counts(example) == first


# --- 4. checks ---------------------------------------------------------------


def test_default_thresholds_fail_the_checks_as_warnings_and_the_run_succeeds(example):
    _rewrite(example, lambda a: a.pop("checks"))
    defs = _load(example)
    with dg.instance_for_test() as instance:
        result = _run(defs, instance, "2026-03_depot-a")
        assert result.success
        failed = {(e.asset_key.path[-1], e.check_name): e for e in _checks(result) if not e.passed}
        assert ("_rejects", "reject_rate") in failed
        assert ("photos", "file_resolution_rate") in failed
        for e in failed.values():
            assert e.severity == dg.AssetCheckSeverity.WARN


def _non_empty_tables(defs) -> set:
    return {s.asset_key.path[-1] for s in _tables_asset(defs).check_specs if s.name == "non_empty"}


def test_allow_empty_drops_the_non_empty_check_for_that_table(example):
    base = _non_empty_tables(_load(example))
    assert "flags" in base
    _rewrite(example, lambda a: a["checks"].update(allow_empty=["flags"]))
    assert _non_empty_tables(_load(example)) == base - {"flags"}


def test_allow_empty_naming_an_unknown_table_fails_the_load(example):
    _rewrite(example, lambda a: a["checks"].update(allow_empty=["no_such_table"]))
    with pytest.raises(Exception) as ei:
        build_component_defs(example)
    text = _chain_text(ei.value)
    assert "no_such_table" in text and "allow_empty" in text


# --- 5. the sensor -----------------------------------------------------------


def _stamps(c: Path) -> dict:
    """Tree refs carry no stamp; the figures live on the opened bundle."""
    src = open_source(str(_bundles(c)), bundle_depth=1)
    out = {}
    for r in src.list_bundles():
        with src.open_bundle(r) as b:
            out[partition_key_for(r.key)] = (b.size, b.modified)
    return out


def test_the_first_tick_requests_both_partitions_and_then_skips(example):
    defs = _load(example)
    sensor = defs.get_sensor_def(f"{NAME}_discovery")
    stamps = _stamps(example)
    with dg.instance_for_test() as instance:
        out = _tick(sensor, instance)
        assert isinstance(out, dg.SensorResult)
        added = [k for r in out.dynamic_partitions_requests for k in r.partition_keys]
        assert set(added) == set(PARTITIONS)
        assert {r.partition_key for r in out.run_requests} == set(PARTITIONS)
        for r in out.run_requests:
            assert stamps[r.partition_key][0]  # figures exist, but the ref has no stamp
            assert r.run_key == f"{r.partition_key}@tree"
            assert r.tags[BUNDLE_TAG] == r.partition_key  # no "/" in these keys
        assert isinstance(_tick(sensor, instance), dg.SkipReason)


def test_the_tick_is_bounded_by_max_new_partitions_per_tick(example):
    _rewrite(example, lambda a: a["discovery"].update(max_new_partitions_per_tick=1))
    defs = _load(example)
    sensor = defs.get_sensor_def(f"{NAME}_discovery")
    with dg.instance_for_test() as instance:
        seen = []
        for _ in range(2):
            out = _tick(sensor, instance)
            assert isinstance(out, dg.SensorResult)
            assert len(out.run_requests) == 1
            assert len(out.dynamic_partitions_requests[0].partition_keys) == 1
            seen.append(out.run_requests[0].partition_key)
        assert set(seen) == set(PARTITIONS)
        assert isinstance(_tick(sensor, instance), dg.SkipReason)


def test_a_changed_bundle_of_a_known_partition_is_not_requested_again_by_the_sensor(example):
    """The sensor requests only partitions it has not seen. The stamp in the run
    key tells a manual re-request apart; it does not trigger one. Pinned so a
    change of that semantics is a deliberate one."""
    defs = _load(example)
    sensor = defs.get_sensor_def(f"{NAME}_discovery")
    before = _stamps(example)
    with dg.instance_for_test() as instance:
        _tick(sensor, instance)
        readme = _bundles(example) / "2026-03_depot-a" / "readme.txt"
        readme.write_bytes(readme.read_bytes() + b"changed")
        assert _stamps(example)["2026-03_depot-a"] != before["2026-03_depot-a"]
        assert isinstance(_tick(sensor, instance), dg.SkipReason)


def test_two_bundle_keys_encoding_to_one_partition_fail_the_tick(example):
    _rewrite(example, lambda a: a["source"].update(bundle_depth=2))
    root = _bundles(example)
    shutil.rmtree(root)
    for key in ("a/b__c", "a__b/c"):
        d = root / key
        d.mkdir(parents=True)
        (d / "x.txt").write_text("x")
    defs = _load(example)
    sensor = defs.get_sensor_def(f"{NAME}_discovery")
    with dg.instance_for_test() as instance:
        with pytest.raises(Exception) as ei:
            _tick(sensor, instance)
        assert "a__b__c" in _chain_text(ei.value)
        assert not instance.get_dynamic_partitions(f"{NAME}_bundles")


def test_a_bundle_key_that_cannot_be_a_partition_key_is_refused_and_counted(example):
    bad = _bundles(example) / "x...y"
    bad.mkdir()
    (bad / "f.txt").write_text("x")
    defs = _load(example)
    sensor = defs.get_sensor_def(f"{NAME}_discovery")
    with dg.instance_for_test() as instance:
        out = _tick(sensor, instance)
        assert {r.partition_key for r in out.run_requests} == set(PARTITIONS)
        out2 = _tick(sensor, instance)
        assert isinstance(out2, dg.SkipReason)
        assert "1 refused" in out2.skip_message


# --- 6. maintenance ----------------------------------------------------------


def test_the_maintenance_job_runs_and_changes_no_count(example):
    defs = _load(example)
    with dg.instance_for_test() as instance:
        assert _run(defs, instance, "2026-03_depot-a").success
        assert _run(defs, instance, "2026-03_depot-a").success
        before = _lance_counts(example)
        assert defs.get_job_def(f"{NAME}_maintenance").execute_in_process(instance=instance).success
        assert _lance_counts(example) == before


# --- 7. pool -----------------------------------------------------------------


def test_the_pool_is_named_from_the_destination_and_limited_to_write_concurrency(example):
    _rewrite(example, lambda a: a.update(write_concurrency=3))
    defs = _load(example)
    op_def = _tables_asset(defs).op
    pool = op_def.pool
    assert pool is not None
    candidates = {
        pool_for(str(_yaml(example).parent / "../../../output")),
        pool_for(str(_output(example))),
    }
    assert re.fullmatch(r"dagtools_lance_[0-9a-f]{12}", pool)
    assert pool in candidates, "the pool is not the one for the destination the component resolved"
    with dg.instance_for_test() as instance:
        assert _run(defs, instance, "2026-03_depot-a").success
        storage = instance.event_log_storage
        if not hasattr(storage, "get_concurrency_info"):
            pytest.skip("this Dagster's event log storage does not expose get_concurrency_info")
        assert storage.get_concurrency_info(pool).slot_count == 3


# --- 8. inline mapping -------------------------------------------------------


def test_an_inline_mapping_loads_and_materializes_the_same_counts(example):
    _rewrite(example, lambda a: a.update(mapping=syn.MAPPING))
    defs = _load(example)
    with dg.instance_for_test() as instance:
        assert _run(defs, instance, "2026-04_depot-b").success
    assert _lance_counts(example) == syn.EXPECTED_ROWS


# --- 9. load-time refusals ---------------------------------------------------


def _refused(c: Path, *needles: str) -> str:
    with pytest.raises(Exception) as ei:
        build_component_defs(c)
    text = _chain_text(ei.value)
    for n in needles:
        assert n in text, text
    return text


def test_an_unknown_key_in_source_fails_the_load(example):
    _rewrite(example, lambda a: a["source"].update(bogus_key=1))
    _refused(example, "bogus_key")


def test_an_invalid_name_fails_the_load(example):
    _rewrite(example, lambda a: a.update(name="not a valid name"))
    _refused(example, "name must be an identifier")


def test_a_mapping_with_a_validation_problem_fails_the_load(example):
    _mapping(example).write_text("bogus_top_level: 1\n", encoding="utf-8")
    text = _refused(example)
    assert "bogus_top_level" in text or "tables" in text


def test_a_missing_mapping_file_fails_the_load_naming_the_file(example):
    _mapping(example).unlink()
    _refused(example, "mapping.yaml")


# --- 10. example fidelity ----------------------------------------------------


def test_the_examples_mapping_is_the_synthetic_mapping():
    p = EXAMPLE / "dagster_home" / "components" / "document_tables" / "mapping.yaml"
    assert yaml.safe_load(p.read_bytes().decode("utf-8")) == syn.MAPPING


@pytest.mark.parametrize("bundle,variant", sorted(PARTITIONS.items()))
def test_each_example_bundle_is_exactly_the_synthetic_bundle(bundle, variant):
    root = EXAMPLE / "bundles" / bundle
    on_disk = {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file() and "__pycache__" not in p.parts
    }
    expected = syn.bundle_files(variant)
    assert set(on_disk) == set(expected)
    for name, data in expected.items():
        assert on_disk[name] == data, name


def test_the_component_file_has_only_type_and_attributes():
    p = EXAMPLE / "dagster_home" / "components" / "document_tables" / "component.yaml"
    assert set(yaml.safe_load(p.read_bytes().decode("utf-8"))) == {"type", "attributes"}


def test_env_references_are_spelled_as_calls():
    """`{{ env.X }}` does not resolve on the 1.10.19 floor; `{{ env('X') }}` does."""
    calls = 0
    for p in (EXAMPLE / "dagster_home").rglob("*"):
        if not p.is_file() or p.suffix not in (".yaml", ".yml", ".py") or "__pycache__" in p.parts:
            continue
        text = p.read_bytes().decode("utf-8")
        assert not re.search(r"\benv\.[A-Za-z_]", text), p.name
        calls += len(re.findall(r"env\('", text))
    assert calls > 0  # the control: the example does carry env references, in comments


# --- 11. DataHub column schema ------------------------------------------------


def test_the_column_names_survive_to_the_datahub_schema_extractor(example):
    try:
        from dag_tools.components.datahub_lineage.component import _extract_table_schema
    except ImportError as exc:  # datahub not installed
        pytest.skip(f"datahub lineage component not importable: {exc}")
    defs = _load(example)
    with dg.instance_for_test() as instance:
        result = _run(defs, instance, "2026-03_depot-a")
        ev = next(
            e for e in result.get_asset_materialization_events()
            if e.step_materialization_data.materialization.asset_key.path[-1] == "scans"
        )
        metadata = ev.step_materialization_data.materialization.metadata
    schema = _extract_table_schema(metadata)
    assert schema is not None
    names = [c.name for c in schema.schema.columns]
    for col in syn.MAPPING["tables"]["scans"]["columns"]:
        assert col in names


def test_the_sensor_names_the_files_above_the_depth_and_orders_trees_by_key(example):
    root = _bundles(example)
    (root / "stray.txt").write_bytes(b"x")
    (root / "stray2.txt").write_bytes(b"x")
    defs = _load(example)
    sensor = defs.get_sensor_def(f"{NAME}_discovery")
    with dg.instance_for_test() as instance:
        out = _tick(sensor, instance)
        assert isinstance(out, dg.SensorResult)
        keys = [r.partition_key for r in out.run_requests]
        assert keys == sorted(keys)
        assert isinstance(_tick(sensor, instance), dg.SkipReason)
        skip = _tick(sensor, instance)
        assert "2 file(s) above the bundle depth" in skip.skip_message


# --- names with unusual characters ------------------------------------------

#: ``[``, ``]``, ``@``, ``$`` and spaces. Dagster refuses none of them in a
#: dynamic partition key (only in a multi-partition dimension), and a name
#: with no ``/`` is its own partition key.
ODD = "2026-05 depot [east] @dock $2"
ODD_SIBLINGS = (ODD + " annex", ODD + "-b")


@pytest.fixture
def aws_env(monkeypatch):
    pytest.importorskip("moto")
    pytest.importorskip("boto3")
    for k, v in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)


def _plant_odd(c: Path) -> None:
    root = _bundles(c)
    for name, variant in [(ODD, "flat")] + [(s, "numbered") for s in ODD_SIBLINGS]:
        for rel, data in syn.bundle_files(variant).items():
            p = root / name / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)


def _lance_rows_by_bundle(c: Path) -> dict:
    out = {}
    for t in KEYS:
        tbl = lance.dataset(str(_output(c) / f"{t}.lance")).to_table(columns=["source_bundle", "publish_period"])
        for b, p in zip(tbl.column("source_bundle").to_pylist(), tbl.column("publish_period").to_pylist()):
            out.setdefault((t, b, p), 0)
            out[(t, b, p)] += 1
    return out


def _exercise_odd(c: Path) -> None:
    assert partition_key_for(ODD) == ODD
    defs = _load(c)
    sensor = defs.get_sensor_def(f"{NAME}_discovery")
    with dg.instance_for_test() as instance:
        out = _tick(sensor, instance)
        reqs = {r.partition_key: r for r in out.run_requests}
        # discovery lists it, the partition key is Dagster's to hold, and the
        # request carries the bundle's own key as its tag
        assert {ODD, *ODD_SIBLINGS} <= set(reqs)
        assert ODD in set(instance.get_dynamic_partitions(f"{NAME}_bundles"))
        assert reqs[ODD].tags[BUNDLE_TAG] == ODD
        assert reqs[ODD].run_key.rsplit("@", 1)[0] == ODD  # a stamp never carries "@"

        # by tag
        assert _run(defs, instance, ODD, tags={BUNDLE_TAG: ODD}).success
        want = {(t, ODD, "2026-05"): n for t, n in syn.EXPECTED_ROWS.items()}
        assert _lance_rows_by_bundle(c) == want  # ODD's rows only; no sibling opened

        # by key match, with no tag: the same bundle, so every count is unchanged
        assert _run(defs, instance, ODD).success
        assert _lance_rows_by_bundle(c) == want


def test_a_bundle_named_with_brackets_at_dollar_and_spaces_round_trips_locally(example):
    _plant_odd(example)
    _exercise_odd(example)


def test_a_bundle_named_with_brackets_at_dollar_and_spaces_round_trips_on_s3(example, aws_env):
    import boto3
    from moto import mock_aws

    _plant_odd(example)
    root = _bundles(example)
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="bkt")
        for p in sorted(root.rglob("*")):
            if p.is_file():
                s3.put_object(Bucket="bkt", Key="bundles/" + p.relative_to(root).as_posix(), Body=p.read_bytes())
        shutil.rmtree(root)  # the run must read the bucket, not a stray local copy
        _rewrite(example, lambda a: a["source"].update(uri="s3://bkt/bundles"))
        _exercise_odd(example)


# --- shared datasets (`into`) and `only_unreferenced` -----------------------

#: Two tables feeding one dataset, and a member table keeping only what no
#: document's file column resolved to. All rows come from the example's own
#: bundles: 4 flags and 5 scans, and on the numbered variant one resolved
#: photo (S1.png) beside an ambiguous pair (S2.png) no reference resolved to.
SHARED = {"flag_events": ("/Manifest/Parcel/Flag", "flag"), "scan_events": ("/Manifest/Parcel/Scan", "scan")}
SHARED_ROWS = {"flag_events": syn.EXPECTED_ROWS["flags"], "scan_events": syn.EXPECTED_ROWS["scans"]}
KEYS_PLUS = KEYS + ["events", "loose"]


def _add_shared_and_loose(c: Path) -> None:
    doc = yaml.safe_load(_mapping(c).read_text(encoding="utf-8"))
    for i, (tname, (rows, kind)) in enumerate(SHARED.items()):
        cols = {"event_id": "@id", "kind": {"const": kind}}
        if i:
            cols = dict(reversed(list(cols.items())))  # column order is not part of the schema
        doc["tables"][tname] = {"rows": rows, "into": "events", "columns": cols}
    doc["tables"]["loose"] = {
        "members": ["**/*.png"],
        "only_unreferenced": True,
        "columns": {"path": {"member": "path"}},
    }
    _mapping(c).write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")


def _events(c: Path) -> list:
    tbl = lance.dataset(str(_output(c) / "events.lance")).to_table(columns=["kind", "event_id"])
    return sorted(zip(tbl.column("kind").to_pylist(), tbl.column("event_id").to_pylist()))


def test_a_shared_into_is_one_asset_and_its_tables_are_not(example):
    _add_shared_and_loose(example)
    defs = _load(example)
    keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert keys == {f"{NAME}/{t}" for t in KEYS_PLUS}
    desc = _tables_asset(defs).specs_by_key[dg.AssetKey([NAME, "events"])].description
    assert "`flag_events`, `scan_events`" in desc
    checked = {s.asset_key.path[-1] for s in _tables_asset(defs).check_specs}
    assert "events" in checked and not checked & set(SHARED)


def test_a_shared_dataset_counts_rows_by_table_and_members_kept_and_skipped(example):
    _add_shared_and_loose(example)
    defs = _load(example)
    with dg.instance_for_test() as instance:
        result = _run(defs, instance, "2026-04_depot-b")
        assert result.success
        md = _mat_metadata(result)
        assert set(md) == set(KEYS_PLUS)
        assert md["events"]["dagster/row_count"] == sum(SHARED_ROWS.values())
        assert md["events"]["rows_by_table"] == SHARED_ROWS
        assert {k for k, _ in _events(example)} == {"flag", "scan"}
        assert sum(1 for k, _ in _events(example) if k == "flag") == SHARED_ROWS["flag_events"]
        # S1.png was resolved by a photo; the S2.png pair was ambiguous, so neither was
        assert md["loose"]["members_kept"] == 2 and md["loose"]["members_skipped"] == 1
        assert md["loose"]["dagster/row_count"] == 2
        loose = lance.dataset(str(_output(example) / "loose.lance")).to_table(columns=["path"])
        assert sorted(p.rsplit("/", 2)[-2] for p in loose.column("path").to_pylist()) == ["01", "02"]
        # a dataset fed by one table carries neither figure
        assert "rows_by_table" not in md["flags"] and "members_kept" not in md["flags"]
        evals = {(e.asset_key.path[-1], e.check_name): e for e in _checks(result)}
        assert evals[("events", "non_empty")].passed
        assert not {k for k, _ in evals} & {(t, "non_empty") for t in SHARED}


def test_a_shared_dataset_is_replaced_per_bundle_and_kept_across_bundles(example):
    _add_shared_and_loose(example)
    defs = _load(example)
    n = sum(SHARED_ROWS.values())
    with dg.instance_for_test() as instance:
        assert _run(defs, instance, "2026-04_depot-b").success
        assert _run(defs, instance, "2026-03_depot-a").success
        assert len(_events(example)) == 2 * n
        assert _run(defs, instance, "2026-04_depot-b").success
        assert len(_events(example)) == 2 * n  # delete-then-append of both tables' rows, once


def test_allow_empty_names_the_shared_dataset_not_a_table_feeding_it(example):
    _add_shared_and_loose(example)
    _rewrite(example, lambda a: a["checks"].update(allow_empty=["events"]))
    assert "events" not in _non_empty_tables(_load(example))
    _rewrite(example, lambda a: a["checks"].update(allow_empty=["flag_events"]))
    _refused(example, "flag_events", "allow_empty", "a shared `into` is named once")
