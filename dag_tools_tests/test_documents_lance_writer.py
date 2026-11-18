"""Tests for the documents Lance writer (synthetic vocabulary only)."""
import logging
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("lxml")
lance = pytest.importorskip("lance")
pa = pytest.importorskip("pyarrow")

from dag_tools.documents.engine import compile_mapping, empty_tables, run_bundle  # noqa: E402
from dag_tools.documents.lance_writer import LanceWriter, SchemaMismatch  # noqa: E402
from dag_tools.documents.mapping import load_mapping  # noqa: E402
from dag_tools.documents.sources import open_source  # noqa: E402

from . import documents_synthetic as S  # noqa: E402

ALL_TABLES = S.TABLES + ["_rejects"]
INGESTED = datetime(2026, 3, 5, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def cm():
    return compile_mapping(load_mapping(S.MAPPING))


@pytest.fixture
def tree(tmp_path):
    """Two bundles: A (flat photos) and B (numbered photos)."""
    root = tmp_path / "src"
    S.write_tree(root / "A", S.bundle_files("flat"))
    S.write_tree(root / "B", S.bundle_files("numbered"))
    return open_source(str(root), 1)


@pytest.fixture
def dest(tmp_path):
    return str(tmp_path / "dest")


def ingest(cm, source, writer, key, **kw):
    writer.begin_bundle(key, empty_tables(cm))
    with source.open_bundle(source.ref_for(key)) as b:
        stats = run_bundle(cm, b, writer.sink, ingested_at=INGESTED, **kw)
    return stats, writer.finish()


def counts(writer, key=None):
    return {t: writer.count_rows(t, key) for t in ALL_TABLES}


def content(writer, table):
    """Rows without the blob struct, order-independent."""
    ds = writer._open(table)
    cols = [n for n in ds.schema.names if not (table == "photos" and n == "image")]
    return sorted(repr(r) for r in ds.to_table(columns=cols).to_pylist())


def blobs_by_name(writer):
    ds = writer._open("photos")
    refs = ds.to_table(columns=["image_ref"]).column("image_ref").to_pylist()
    files = ds.take_blobs("image", indices=list(range(len(refs))))
    return {r: (f.read() if f is not None else None) for r, f in zip(refs, files)}


# ------------------------------------------------------------------ 1. counts


def test_both_bundles_land_with_expected_counts(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    ingest(cm, tree, w, "B")
    assert counts(w, "A") == S.EXPECTED_ROWS
    assert counts(w, "B") == S.EXPECTED_ROWS
    assert counts(w) == {t: 2 * n for t, n in S.EXPECTED_ROWS.items()}


def test_one_dataset_per_table_under_the_destination(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    assert w.uri("scans").replace("\\", "/").endswith("/dest/scans.lance")
    import os

    assert sorted(os.listdir(dest)) == sorted(f"{t}.lance" for t in ALL_TABLES)


def test_finish_reports_what_was_written(cm, tree, dest):
    w = LanceWriter(dest)
    _stats, res = ingest(cm, tree, w, "A")
    assert res.appended == S.EXPECTED_ROWS
    assert sorted(res.created) == sorted(ALL_TABLES)
    assert all(v >= 1 for v in res.versions.values())
    _stats, res2 = ingest(cm, tree, w, "A")
    assert res2.created == []
    assert res2.deleted == S.EXPECTED_ROWS


# ------------------------------------------------------------------ 2. idempotence


def test_rerunning_a_bundle_changes_no_count_and_leaves_the_other_untouched(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    ingest(cm, tree, w, "B")
    before_total = counts(w)

    def ids(key):
        ds = w._open("scans")
        return sorted(ds.to_table(columns=["doc_id"], filter=f"source_bundle = '{key}'").column("doc_id").to_pylist())

    b_before = ids("B")
    b_content = {t: content(w, t) for t in ALL_TABLES}
    assert b_before
    ingest(cm, tree, w, "A")
    assert counts(w) == before_total
    assert counts(w, "A") == S.EXPECTED_ROWS
    assert ids("B") == b_before
    ds = w._open("scans")
    b_rows = sorted(repr(r) for r in ds.to_table(filter="source_bundle = 'B'", columns=["doc_id", "row_id"]).to_pylist())
    assert len(b_rows) == S.EXPECTED_ROWS["scans"]
    # bundle B's rows are byte-for-byte what they were
    for t in ALL_TABLES:
        full = content(w, t)
        assert all(r in full for r in [r for r in b_content[t] if "'B'" in r])


def test_the_two_bundles_have_distinct_doc_ids(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    ingest(cm, tree, w, "B")
    ds = w._open("loads")
    ids = ds.to_table(columns=["doc_id"]).column("doc_id").to_pylist()
    assert len(set(ids)) == len(ids) == 4


# ------------------------------------------------------------------ 3. quoting


@pytest.mark.parametrize("name", ["it's", "o''hara", "x' or 1=1 or 'a'='a"])
def test_a_bundle_key_with_a_quote_is_deleted_exactly(cm, tmp_path, dest, name):
    root = tmp_path / "src"
    S.write_tree(root / name, S.bundle_files("flat"))
    S.write_tree(root / "plain", S.bundle_files("flat"))
    source = open_source(str(root), 1)
    w = LanceWriter(dest)
    ingest(cm, source, w, name)
    ingest(cm, source, w, "plain")
    ingest(cm, source, w, name)  # re-run: delete must match the literal exactly
    assert counts(w, name) == S.EXPECTED_ROWS
    assert counts(w, "plain") == S.EXPECTED_ROWS
    assert counts(w) == {t: 2 * n for t, n in S.EXPECTED_ROWS.items()}


# ------------------------------------------------------------------ 4. empty


def test_begin_bundle_with_no_rows_creates_every_dataset_with_the_mapping_schema(cm, dest):
    w = LanceWriter(dest)
    schemas = empty_tables(cm)
    res = w.begin_bundle("nothing", schemas)
    w.finish()
    assert sorted(res.created) == sorted(ALL_TABLES)
    for t, schema in schemas.items():
        ds = w._open(t)
        assert ds.count_rows() == 0
        assert ds.schema.names == schema.names
    assert counts(w) == {t: 0 for t in ALL_TABLES}


def test_an_empty_bundle_run_through_the_engine_writes_zero_rows(cm, tmp_path, dest):
    root = tmp_path / "src"
    S.write_tree(root / "E", {"readme.txt": b"nothing to parse"})
    w = LanceWriter(dest)
    stats, _ = ingest(cm, open_source(str(root), 1), w, "E")
    assert counts(w, "E") == {t: 0 for t in ALL_TABLES}
    assert stats.documents == 0


def test_a_second_begin_bundle_with_no_rows_deletes_only_that_bundle(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    ingest(cm, tree, w, "B")
    w.begin_bundle("A", empty_tables(cm))
    w.finish()
    assert counts(w, "A") == {t: 0 for t in ALL_TABLES}
    assert counts(w, "B") == S.EXPECTED_ROWS


def test_append_before_begin_bundle_is_refused(dest):
    w = LanceWriter(dest)
    with pytest.raises(RuntimeError):
        w.append("loads", pa.record_batch([pa.array(["x"])], names=["doc_id"]))


def test_a_destination_is_required():
    with pytest.raises(ValueError):
        LanceWriter("")


def test_count_rows_of_an_absent_table_is_zero(dest):
    assert LanceWriter(dest).count_rows("never_written") == 0


# ------------------------------------------------------------------ 5. blobs


def test_the_blob_column_is_declared_large_binary_with_the_blob_metadata(cm):
    f = empty_tables(cm)["photos"].field("image")
    assert f.type == pa.large_binary()
    assert f.metadata[b"lance-encoding:blob"] == b"true"


def test_the_stored_blob_column_is_lances_blob_type(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    t = w._open("photos").schema.field("image").type
    assert str(getattr(t, "extension_name", "")).startswith("lance.blob")


def test_flat_variant_blobs_read_back_equal_the_fixture_bytes_and_probe_is_right(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    got = blobs_by_name(w)
    files = S.bundle_files("flat")
    resolved = {k: v for k, v in got.items() if v is not None}
    assert sorted(resolved.values(), key=len) == sorted(
        [files["manifest_m1/S1.png"], files["manifest_m1/S2.png"]], key=len
    )
    rows = {r["image_ref"]: r for r in w._open("photos").to_table().drop(["image"]).to_pylist()}
    one = next(r for k, r in rows.items() if k.endswith("S1.png"))
    two = next(r for k, r in rows.items() if k.endswith("S2.png"))
    assert (one["image_format"], one["image_width"], one["image_height"]) == ("PNG", 4, 3)
    assert (two["image_format"], two["image_width"], two["image_height"]) == ("PNG", 5, 2)
    assert one["image_bytes"] == len(S.png_bytes(4, 3))
    assert two["image_bytes"] == len(S.png_bytes(5, 2))
    assert [r["image_resolution"] for r in rows.values()].count("missing") == 1


def test_ambiguous_and_missing_files_store_no_bytes(cm, tree, dest):
    w = LanceWriter(dest)
    stats, _ = ingest(cm, tree, w, "B")
    assert stats.file_resolution["photos"]["image"] == S.expected_photo_resolution("numbered")
    ds = w._open("photos")
    rows = ds.to_table(columns=["image_ref", "image_resolution", "image_bytes", "image_format"]).to_pylist()
    got = blobs_by_name(w)
    for r in rows:
        if r["image_resolution"] in ("ambiguous", "missing"):
            assert got[r["image_ref"]] is None
            assert r["image_bytes"] is None and r["image_format"] is None
        else:
            assert got[r["image_ref"]] == S.bundle_files("numbered")["manifest_m1/01/S1.png"]
    assert sorted(r["image_resolution"] for r in rows) == ["ambiguous", "basename_under", "missing"]


# ------------------------------------------------------------------ 6. chunked flushes


def test_tiny_flushes_give_identical_contents_in_more_fragments(cm, tree, tmp_path):
    big = LanceWriter(str(tmp_path / "big"))
    small = LanceWriter(str(tmp_path / "small"))
    _s, res_big = ingest(cm, tree, big, "B")
    _s, res_small = ingest(cm, tree, small, "B", flush_rows=1, flush_attachment_bytes=1)
    for t in ALL_TABLES:
        assert content(big, t) == content(small, t), t
    assert blobs_by_name(big) == blobs_by_name(small)
    # anti-vacuity: the chunked run really appended in many pieces
    for t in ("scans", "parcels", "flags"):  # photos arrive as one document, hence one batch
        assert res_small.versions[t] > res_big.versions[t], t
        assert len(small._open(t).get_fragments()) > len(big._open(t).get_fragments()), t
    assert res_small.appended == res_big.appended


# ------------------------------------------------------------------ 7. schema mismatch


def _retyped(schema, column, new_type):
    return pa.schema([pa.field(f.name, new_type, metadata=f.metadata) if f.name == column else f for f in schema])


def test_a_changed_column_type_raises_naming_the_column_and_deletes_nothing(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    before = counts(w)
    versions = {t: w._open(t).version for t in ALL_TABLES}
    schemas = empty_tables(cm)
    schemas["scans"] = _retyped(schemas["scans"], "lane", pa.string())
    with pytest.raises(SchemaMismatch) as e:
        w.begin_bundle("A", schemas)
    assert "scans" in str(e.value) and "lane" in str(e.value)
    assert isinstance(e.value, ValueError)
    assert counts(w) == before
    assert {t: w._open(t).version for t in ALL_TABLES} == versions


def test_added_and_removed_columns_are_named_too(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    schemas = empty_tables(cm)
    schemas["flags"] = schemas["flags"].append(pa.field("extra_col", pa.string())).remove(
        schemas["flags"].get_field_index("code")
    )
    with pytest.raises(SchemaMismatch) as e:
        w.begin_bundle("A", schemas)
    msg = str(e.value)
    assert "extra_col" in msg and "code" in msg
    assert counts(w, "A") == S.EXPECTED_ROWS


def test_a_blob_column_declared_plain_binary_is_a_mismatch(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    schemas = empty_tables(cm)
    sch = schemas["photos"]
    i = sch.get_field_index("image")
    schemas["photos"] = sch.set(i, pa.field("image", pa.large_binary()))  # no blob metadata
    with pytest.raises(SchemaMismatch) as e:
        w.begin_bundle("A", schemas)
    assert "image" in str(e.value)
    assert counts(w, "A") == S.EXPECTED_ROWS


def test_an_unchanged_schema_reopens_cleanly(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    LanceWriter(dest).begin_bundle("B", empty_tables(cm))  # a fresh writer accepts the stored schema


# ------------------------------------------------------------------ 8. maintain


def test_maintain_compacts_cleans_and_keeps_counts(cm, tree, dest):
    w = LanceWriter(dest)
    for _ in range(4):
        ingest(cm, tree, w, "A", flush_rows=1)
    ingest(cm, tree, w, "B")
    before = counts(w)
    frags_before = {t: len(w._open(t).get_fragments()) for t in ("scans", "parcels", "flags")}
    assert all(n > 1 for n in frags_before.values())
    out = w.maintain(ALL_TABLES + ["never_written"], older_than=timedelta(0), retain_versions=1)
    assert out["never_written"] == {"skipped": "absent"}
    for t in ALL_TABLES:
        assert set(out[t]) == {
            "fragments_removed", "fragments_added", "bytes_removed", "old_versions_removed", "version",
            "source_bundle_index",
        }
        assert all(isinstance(v, int) for k, v in out[t].items() if k != "source_bundle_index")
        assert out[t]["source_bundle_index"] == "created"
    for t in ("scans", "parcels", "flags"):
        assert len(w._open(t).get_fragments()) < frags_before[t]
        assert out[t]["fragments_removed"] > 0
        assert out[t]["old_versions_removed"] > 0
        assert out[t]["bytes_removed"] > 0
    assert counts(w) == before
    assert blobs_by_name(w)  # blobs survive compaction
    assert sum(v is not None for v in blobs_by_name(w).values()) >= 1


def test_maintain_with_default_retention_removes_nothing_recent(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    out = w.maintain(["scans"])
    assert out["scans"]["old_versions_removed"] == 0
    assert w.count_rows("scans") == S.EXPECTED_ROWS["scans"]


# ------------------------------------------------------------------ 9. storage options


MARKER = "ZQX-STORAGE-SECRET-5530"


def test_storage_options_reach_every_lance_entry_point_and_are_never_logged(cm, tree, dest, monkeypatch, caplog):
    seen = {"dataset": [], "write_dataset": []}
    real_dataset, real_write = lance.dataset, lance.write_dataset

    def spy_dataset(*a, **kw):
        seen["dataset"].append(kw.get("storage_options"))
        return real_dataset(*a, **kw)

    def spy_write(*a, **kw):
        seen["write_dataset"].append(kw.get("storage_options"))
        return real_write(*a, **kw)

    monkeypatch.setattr(lance, "dataset", spy_dataset)
    monkeypatch.setattr(lance, "write_dataset", spy_write)
    opts = {"aws_secret_access_key": MARKER, "aws_region": "us-east-1"}
    w = LanceWriter(dest, storage_options=opts)
    assert w.storage_options == opts and w.storage_options is not opts
    with caplog.at_level(logging.DEBUG):
        ingest(cm, tree, w, "A")
        ingest(cm, tree, w, "A")
        w.count_rows("scans", "A")
        w.maintain(["scans"])
        bad = empty_tables(cm)
        bad["scans"] = _retyped(bad["scans"], "lane", pa.string())
        with pytest.raises(SchemaMismatch) as e:
            w.begin_bundle("A", bad)
    assert seen["dataset"] and seen["write_dataset"]
    assert all(o == opts for o in seen["dataset"] + seen["write_dataset"])
    assert MARKER not in str(e.value)
    for rec in caplog.records:
        assert MARKER not in rec.getMessage()
        assert MARKER not in repr(rec.args)
    assert MARKER not in caplog.text
    assert caplog.records  # the writer does log; the scan above is not vacuous


def test_no_storage_options_passes_none(cm, tree, dest, monkeypatch):
    seen = []
    real = lance.write_dataset
    monkeypatch.setattr(lance, "write_dataset", lambda *a, **kw: (seen.append(kw.get("storage_options")), real(*a, **kw))[1])
    w = LanceWriter(dest)
    assert w.storage_options is None
    ingest(cm, tree, w, "A")
    assert seen and all(o is None for o in seen)


# ---- the source_bundle index


def _bundle_indices(ds):
    return [i for i in ds.describe_indices() if list(i.field_names) == ["source_bundle"]]


def test_maintain_creates_one_source_bundle_index_and_updates_it_after(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    assert _bundle_indices(w._open("scans")) == []
    assert w.maintain(["scans"])["scans"]["source_bundle_index"] == "created"
    assert len(_bundle_indices(w._open("scans"))) == 1
    ingest(cm, tree, w, "B")
    name = _bundle_indices(w._open("scans"))[0].name
    assert w._open("scans").stats.index_stats(name)["num_unindexed_rows"] > 0
    assert w.maintain(["scans"])["scans"]["source_bundle_index"] == "updated"
    ds = w._open("scans")
    assert len(_bundle_indices(ds)) == 1
    assert ds.stats.index_stats(name)["num_unindexed_rows"] == 0


def test_the_bundle_delete_uses_the_index_once_it_exists(cm, tree, dest):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    w.maintain(["scans"])
    plan = w._open("scans").scanner(filter="source_bundle = 'A'").explain_plan()
    assert "ScalarIndexQuery" in plan
    # and re-ingesting through the index still leaves counts where they were
    before = counts(w)
    ingest(cm, tree, w, "A")
    assert counts(w) == before


def test_an_index_failure_is_reported_and_maintenance_still_cleans_up(cm, tree, dest, monkeypatch, caplog):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    ingest(cm, tree, w, "A")

    def boom(self, *a, **kw):
        raise RuntimeError("index build refused")

    monkeypatch.setattr(lance.LanceDataset, "create_scalar_index", boom)
    with caplog.at_level(logging.WARNING):
        out = w.maintain(["scans"], older_than=timedelta(0), retain_versions=1)
    assert out["scans"]["source_bundle_index"] == "failed: RuntimeError"
    assert out["scans"]["old_versions_removed"] > 0
    assert "index build refused" not in caplog.text


def test_a_pylance_without_the_index_api_is_skipped_with_a_note(cm, tree, dest, monkeypatch, caplog):
    w = LanceWriter(dest)
    ingest(cm, tree, w, "A")
    monkeypatch.delattr(lance.LanceDataset, "create_scalar_index")
    with caplog.at_level(logging.INFO):
        out = w.maintain(["scans"])
    assert out["scans"]["source_bundle_index"] == "unsupported"
    assert "source_bundle index skipped" in caplog.text
