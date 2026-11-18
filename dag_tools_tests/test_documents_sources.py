"""Tests for the document bundle sources (synthetic vocabulary only)."""
import io
import os
import shutil
import zipfile
from pathlib import Path

import pytest

pytest.importorskip("lxml")
pytest.importorskip("lance")

from dag_tools.documents import sources as src  # noqa: E402
from dag_tools.documents.sources import (  # noqa: E402
    SourceError,
    bundle_key_for,
    normalise_member_path,
    open_source,
)

from . import documents_synthetic as S  # noqa: E402


def keys(source):
    return sorted(r.key for r in source.list_bundles())


def paths(source, ref):
    with source.open_bundle(ref) as b:
        return sorted(m.path for m in b.members)


# ------------------------------------------------------------------ 1. keys


def test_bundle_keys_at_depth_0_1_2(tmp_path):
    S.write_tree(tmp_path, {"a/x/one.xml": b"1", "a/y/two.xml": b"2", "b/three.xml": b"3", "top.xml": b"4"})
    assert keys(open_source(str(tmp_path), 0)) == ["."]
    s1 = open_source(str(tmp_path), 1)
    assert keys(s1) == ["a", "b"]
    assert s1.files_above_depth == 1  # top.xml is not a bundle
    # files at or above the depth belong to no bundle and are only counted
    s2 = open_source(str(tmp_path), 2)
    assert keys(s2) == ["a/x", "a/y"]
    assert s2.files_above_depth == 2  # top.xml and b/three.xml


def test_the_count_of_files_above_the_depth_is_logged_once_without_names(tmp_path, caplog):
    import logging

    S.write_tree(tmp_path, {"a/x.xml": b"1", "top.xml": b"2", "other.txt": b"3"})
    S.write_zip(tmp_path / "z.zip", {"q.xml": b"1"})
    source = open_source(str(tmp_path), 1)
    with caplog.at_level(logging.INFO, logger=src.logger.name):
        assert keys(source) == ["a", "z.zip"]
    lines = [r.getMessage() for r in caplog.records if r.name == src.logger.name]
    assert len(lines) == 1 and "2 file(s)" in lines[0]
    assert "top.xml" not in lines[0] and "other.txt" not in lines[0]


def test_a_zip_at_or_above_the_depth_is_a_bundle_and_a_deeper_one_is_a_member(tmp_path):
    S.write_zip(tmp_path / "top.zip", {"q.xml": b"1"})
    S.write_zip(tmp_path / "a" / "b" / "deep.zip", {"r.xml": b"2"})
    source = open_source(str(tmp_path), 2)
    assert keys(source) == ["a/b", "top.zip"]
    assert paths(source, source.ref_for("a/b")) == ["deep.zip/r.xml"]
    with pytest.raises(SourceError, match="deeper"):
        source.ref_for("a/b/deep.zip")


def test_depth_zero_is_unchanged_one_root_bundle_and_every_zip(tmp_path):
    S.write_tree(tmp_path, {"a/x.xml": b"1", "top.xml": b"22"})
    S.write_zip(tmp_path / "a" / "z.zip", {"q.xml": b"3"})
    source = open_source(str(tmp_path), 0)
    refs = {r.key: r for r in source.list_bundles()}
    assert set(refs) == {".", "a/z.zip"}
    assert refs["."].size == 3 and refs["."].modified and refs["."].stamp
    assert paths(source, refs["."]) == ["a/x.xml", "top.xml"]
    assert source.files_above_depth == 0
    assert source.ref_for(".") == refs["."]


def test_bundle_key_for_is_posix_and_depth_bounded():
    assert bundle_key_for("a/b/c.xml", 0) == "."
    assert bundle_key_for("a/b/c.xml", 1) == "a"
    assert bundle_key_for("a/b/c.xml", 2) == "a/b"
    assert bundle_key_for("a/c.xml", 2) == "."


def test_negative_depth_is_refused(tmp_path):
    with pytest.raises(SourceError):
        open_source(str(tmp_path), -1)


def test_a_zip_at_the_bundle_level_is_a_bundle_and_members_are_inner_paths(tmp_path):
    S.write_zip(tmp_path / "pack.zip", S.bundle_files("flat"))
    S.write_tree(tmp_path / "2026-04", S.bundle_files("numbered"))
    source = open_source(str(tmp_path), 1)
    refs = {r.key: r for r in source.list_bundles()}
    assert set(refs) == {"pack.zip", "2026-04"}
    assert refs["pack.zip"].kind == "zip"
    assert refs["2026-04"].kind == "tree"
    assert refs["pack.zip"].name == "pack.zip"
    assert paths(source, refs["pack.zip"]) == sorted(S.bundle_files("flat"))
    # tree members are relative to the bundle directory
    assert paths(source, refs["2026-04"]) == sorted(S.bundle_files("numbered"))


def test_a_zip_inside_a_tree_bundle_is_a_member_expanded_one_level(tmp_path):
    S.write_tree(tmp_path, {"a/x.xml": b"1", "b/y.xml": b"2"})
    S.write_zip(tmp_path / "a" / "z.zip", {"q.xml": b"3"})
    S.write_zip(tmp_path / "a" / "outer.zip", {"in.zip": S.zip_bytes({"deep.xml": b"4"})})
    source = open_source(str(tmp_path), 1)
    assert keys(source) == ["a", "b"]
    with source.open_bundle(source.ref_for("a")) as b:
        assert sorted(m.path for m in b.members) == ["outer.zip/in.zip", "x.xml", "z.zip/q.xml"]
        assert b.unexpanded_zips == ["outer.zip/in.zip"]
    assert paths(source, source.ref_for("b")) == ["y.xml"]


def test_member_paths_are_normalised():
    assert normalise_member_path("a\\b\\c.png") == "a/b/c.png"
    assert normalise_member_path("./a/b") == "a/b"
    assert normalise_member_path("/abs/x") == "abs/x"


def test_backslash_zip_member_names_are_normalised(tmp_path):
    with zipfile.ZipFile(tmp_path / "w.zip", "w") as z:
        z.writestr("dir\\file.xml", b"<a/>")
    source = open_source(str(tmp_path))
    assert paths(source, source.ref_for("w.zip")) == ["dir/file.xml"]


def test_a_zip_nested_in_a_zip_bundle_is_expanded_under_its_own_path(tmp_path):
    S.write_nested_zip(tmp_path / "outer.zip", S.bundle_files("flat"), inner_name="inner.zip")
    source = open_source(str(tmp_path))
    with source.open_bundle(source.ref_for("outer.zip")) as b:
        got = {m.path: m.read() for m in b.members}
        assert b.unexpanded_zips == []
    assert set(got) == {f"inner.zip/{p}" for p in S.bundle_files("flat")}
    assert got["inner.zip/readme.txt"] == b"not a document"


def test_a_zip_nested_two_deep_is_listed_unexpanded(tmp_path):
    inner = S.zip_bytes({"deep.xml": b"<a/>"})
    mid = S.zip_bytes({"mid.zip": inner, "ok.xml": b"<a/>"})
    S.write_zip(tmp_path / "outer.zip", {"mid.zip": mid})
    source = open_source(str(tmp_path))
    with source.open_bundle(source.ref_for("outer.zip")) as b:
        assert "mid.zip/mid.zip" in b.unexpanded_zips
        assert "mid.zip/ok.xml" in {m.path for m in b.members}


def test_a_corrupt_zip_bundle_raises_source_error(tmp_path):
    (tmp_path / "bad.zip").write_bytes(b"not a zip at all")
    source = open_source(str(tmp_path))
    with pytest.raises(SourceError):
        source.open_bundle(source.ref_for("bad.zip"))


def test_missing_directory_and_unknown_scheme_are_refused(tmp_path):
    with pytest.raises(SourceError):
        list(open_source(str(tmp_path / "nope")).list_bundles())
    with pytest.raises(SourceError):
        open_source("ftp://host/x")


def test_file_scheme_is_a_local_source(tmp_path):
    S.write_tree(tmp_path, {"a.xml": b"1"})
    assert keys(open_source("file://" + str(tmp_path))) == ["."]


def test_member_read_limit(tmp_path):
    S.write_tree(tmp_path, {"a.bin": b"0123456789"})
    source = open_source(str(tmp_path))
    with source.open_bundle(source.ref_for(".")) as b:
        assert b.members[0].read(4) == b"0123"
        assert b.members[0].read() == b"0123456789"


# ------------------------------------------------------------------ 2. stamp


def _figures(source, key):
    with source.open_bundle(source.ref_for(key)) as b:
        return b.size, b.modified


def test_a_tree_ref_has_no_stamp_and_the_open_bundle_carries_the_figures(tmp_path):
    S.write_tree(tmp_path / "k", {"a.xml": b"one"})
    source = open_source(str(tmp_path), 1)
    ref = source.ref_for("k")
    assert (ref.size, ref.modified, ref.stamp) == (None, None, None)
    assert [r.stamp for r in source.list_bundles()] == [None]
    size, mod = _figures(source, "k")
    assert size == 3 and mod


def test_open_bundle_figures_change_with_content(tmp_path):
    S.write_tree(tmp_path / "k", {"a.xml": b"one"})
    source = open_source(str(tmp_path), 1)
    first = _figures(source, "k")
    assert _figures(source, "k") == first
    p = tmp_path / "k" / "a.xml"
    p.write_bytes(b"one-and-more")
    st = p.stat()
    os.utime(p, (st.st_atime, st.st_mtime + 10))
    assert _figures(source, "k") != first


def test_open_bundle_size_changes_on_size_alone(tmp_path):
    S.write_tree(tmp_path / "k", {"a.xml": b"one"})
    source = open_source(str(tmp_path), 1)
    before = _figures(source, "k")
    p = tmp_path / "k" / "a.xml"
    st = p.stat()
    p.write_bytes(b"three")
    os.utime(p, (st.st_atime, st.st_mtime))  # same mtime, new size
    after = _figures(source, "k")
    assert after[1] == before[1] and after[0] != before[0]


def test_a_zip_stamp_changes_when_the_zip_is_rewritten(tmp_path):
    S.write_zip(tmp_path / "z.zip", {"a.xml": b"1"})
    source = open_source(str(tmp_path))
    first = source.ref_for("z.zip").stamp
    S.write_zip(tmp_path / "z.zip", {"a.xml": b"1", "b.xml": b"2" * 100})
    assert source.ref_for("z.zip").stamp != first


def test_ref_for_round_trips_list_bundles(tmp_path):
    S.write_zip(tmp_path / "a.zip", S.bundle_files("flat"))
    S.write_tree(tmp_path / "t", S.bundle_files("flat"))
    source = open_source(str(tmp_path), 1)
    for ref in source.list_bundles():
        assert source.ref_for(ref.key) == ref
    for bad in ("absent", "absent.zip", ".", "t/x", "../t"):
        with pytest.raises(SourceError):
            source.ref_for(bad)


# ------------------------------------------------------------------ 3. traversal


def test_absolute_member_names_are_normalised_to_relative(tmp_path):
    with zipfile.ZipFile(tmp_path / "evil.zip", "w") as z:
        z.writestr("/abs/evil.xml", b"<a/>")
        z.writestr("ok/fine.xml", b"<a/>")
    source = open_source(str(tmp_path))
    got = paths(source, source.ref_for("evil.zip"))
    assert all(not p.startswith("/") for p in got)
    assert got == ["abs/evil.xml", "ok/fine.xml"]


@pytest.mark.parametrize("name", ["../escape.xml", "ok/../../escape.xml", "..\\escape.xml"])
def test_parent_traversal_never_survives_in_a_member_path(tmp_path, name):
    with zipfile.ZipFile(tmp_path / "t.zip", "w") as z:
        z.writestr(name, b"<a/>")
    source = open_source(str(tmp_path))
    try:
        got = paths(source, source.ref_for("t.zip"))
    except SourceError:
        return  # refusing is acceptable
    assert all(".." not in p.split("/") for p in got), got


@pytest.mark.parametrize(
    "name, label",
    [("../escape.xml", "escape.xml"), ("ok/../../escape.xml", "escape.xml"), ("a/./b/../c.xml", "a/c.xml")],
)
def test_a_climbing_member_name_is_clamped_to_the_bundle_root(tmp_path, name, label):
    # The zipfile.extract convention: collapse, and drop what climbs above the root.
    with zipfile.ZipFile(tmp_path / "t.zip", "w") as z:
        z.writestr(name, b"<a/>")
    source = open_source(str(tmp_path))
    assert paths(source, source.ref_for("t.zip")) == [label]


def test_a_reference_inside_a_document_keeps_its_parent_segments():
    # References resolve against the document's directory, so '..' there is meaningful.
    assert normalise_member_path("../photos/x.png") == "../photos/x.png"


def test_opening_a_hostile_zip_writes_nothing_outside_the_source(tmp_path):
    root = tmp_path / "src"
    root.mkdir()
    with zipfile.ZipFile(root / "t.zip", "w") as z:
        z.writestr("../../escape.xml", b"<a/>")
    source = open_source(str(root))
    with source.open_bundle(source.ref_for("t.zip")) as b:
        for m in b.members:
            m.read()
    assert not (tmp_path / "escape.xml").exists()
    assert not (tmp_path.parent / "escape.xml").exists()


# ------------------------------------------------------------------ 5. handles


def test_open_read_close_leaves_no_handle_so_rmtree_succeeds(tmp_path):
    root = tmp_path / "tree"
    S.write_zip(root / "a.zip", S.bundle_files("flat"))
    S.write_nested_zip(root / "n.zip", S.bundle_files("flat"))
    S.write_tree(root / "t", S.bundle_files("flat"))
    source = open_source(str(root), 1)
    for ref in list(source.list_bundles()):
        b = source.open_bundle(ref)
        for m in b.members:
            m.read()
        b.close()
    shutil.rmtree(root)
    assert not root.exists()


def test_context_manager_closes_and_members_are_lazy(tmp_path):
    root = tmp_path / "tree"
    S.write_zip(root / "a.zip", S.bundle_files("flat"))
    source = open_source(str(root))
    with source.open_bundle(source.ref_for("a.zip")) as b:
        assert b.members  # listing alone read nothing; bytes come on request
        assert all(callable(m.reader) for m in b.members)
    assert b._closers == []
    shutil.rmtree(root)
    assert not root.exists()


def test_closing_twice_is_harmless(tmp_path):
    S.write_zip(tmp_path / "a.zip", {"x.xml": b"1"})
    source = open_source(str(tmp_path))
    b = source.open_bundle(source.ref_for("a.zip"))
    b.close()
    b.close()


# ------------------------------------------------------------------ 4. S3


@pytest.fixture
def s3_env(monkeypatch):
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


def _bucket(files_zip, files_tree):
    import boto3

    c = boto3.client("s3", region_name="us-east-1")
    c.create_bucket(Bucket="bkt")
    c.put_object(Bucket="bkt", Key="pre/fix/pack.zip", Body=S.zip_bytes(files_zip))
    for rel, data in files_tree.items():
        c.put_object(Bucket="bkt", Key=f"pre/fix/2026-04/{rel}", Body=data)
    c.put_object(Bucket="bkt", Key="other/ignored.xml", Body=b"x")
    c.put_object(Bucket="bkt", Key="pre/fix/folder/", Body=b"")
    return c


def test_s3_lists_and_opens_a_zip_bundle_and_a_directory_bundle(s3_env):
    from moto import mock_aws

    with mock_aws():
        _bucket(S.bundle_files("flat"), S.bundle_files("numbered"))
        source = open_source("s3://bkt/pre/fix", bundle_depth=1)
        refs = {r.key: r for r in source.list_bundles()}
        assert set(refs) == {"pack.zip", "2026-04", "folder"}
        assert refs["pack.zip"].kind == "zip"
        with source.open_bundle(refs["pack.zip"]) as b:
            got = {m.path: m.read() for m in b.members}
        assert got == S.bundle_files("flat")
        with source.open_bundle(refs["2026-04"]) as b:
            got = {m.path: m.read() for m in b.members}
            first = b.members[0]
            assert first.read(3) == got[first.path][:3]
        assert got == S.bundle_files("numbered")
        assert source.ref_for("2026-04") == refs["2026-04"]
        total = sum(len(v) for v in S.bundle_files("numbered").values())
        assert refs["2026-04"].stamp is None
        with source.open_bundle(refs["2026-04"]) as b:
            assert b.size == total and b.modified
        assert refs["pack.zip"].stamp


def test_s3_nested_zip_expands(s3_env):
    import boto3
    from moto import mock_aws

    with mock_aws():
        c = boto3.client("s3", region_name="us-east-1")
        c.create_bucket(Bucket="bkt")
        c.put_object(Bucket="bkt", Key="n.zip", Body=S.zip_bytes({"inner.zip": S.zip_bytes(S.bundle_files("flat"))}))
        source = open_source("s3://bkt")
        with source.open_bundle(source.ref_for("n.zip")) as b:
            assert {m.path for m in b.members} == {f"inner.zip/{p}" for p in S.bundle_files("flat")}


def test_s3_endpoint_url_is_accepted(s3_env):
    from moto import mock_aws

    with mock_aws():
        _bucket({"a.xml": b"1"}, {"b.xml": b"2"})
        source = open_source("s3://bkt/pre/fix", 1, endpoint_url="https://s3.us-east-1.amazonaws.com")
        assert source.endpoint_url == "https://s3.us-east-1.amazonaws.com"
        assert set(keys(source)) == {"pack.zip", "2026-04", "folder"}


def test_s3_credentials_are_never_passed_by_the_source(s3_env, monkeypatch):
    import boto3

    seen = {}
    real = boto3.client

    def spy(*a, **kw):
        seen.update(kw)
        return real(*a, **kw)

    monkeypatch.setattr(boto3, "client", spy)
    from moto import mock_aws

    with mock_aws():
        _bucket({"a.xml": b"1"}, {})
        list(open_source("s3://bkt/pre/fix", 1, endpoint_url="https://s3.us-east-1.amazonaws.com").list_bundles())
    assert not {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"} & set(seen)
    assert seen["endpoint_url"] == "https://s3.us-east-1.amazonaws.com"


def test_s3_bucket_only_uri_and_bad_uri():
    pytest.importorskip("boto3")
    s = src.S3Source("s3://bkt")
    assert (s.bucket, s.prefix) == ("bkt", "")
    with pytest.raises(SourceError):
        src.S3Source("bkt/prefix")


def test_s3_ranged_reads_serve_a_zip_member(s3_env):
    import boto3
    from moto import mock_aws

    with mock_aws():
        c = boto3.client("s3", region_name="us-east-1")
        c.create_bucket(Bucket="bkt")
        big = {f"f{i}.bin": os.urandom(2000) for i in range(5)}
        body = S.zip_bytes(big)
        c.put_object(Bucket="bkt", Key="z.zip", Body=body)
        f = src.S3File(c, "bkt", "z.zip", len(body), chunk=512)
        zf = zipfile.ZipFile(io.BufferedReader(f, 512))
        assert zf.read("f3.bin") == big["f3.bin"]


# ---------------------------------------------------------------- listing cost


def _count_s3_calls(client):
    calls = []

    def rec(name):
        def hook(params, **kw):
            q = params.get("query_string") or {}
            flat = {"Prefix": q.get("prefix"), "Delimiter": q.get("delimiter"),
                    "StartAfter": q.get("start-after"), "MaxKeys": q.get("max-keys")}
            calls.append((name, flat))
        return hook

    client.meta.events.register("before-call.s3.ListObjectsV2", rec("list"))
    client.meta.events.register("before-call.s3.HeadObject", rec("head"))
    return calls


def _wide_bucket(others, members):
    import boto3

    c = boto3.client("s3", region_name="us-east-1")
    c.create_bucket(Bucket="bkt")
    for i in range(others):
        for j in range(members):
            c.put_object(Bucket="bkt", Key=f"r/b{i:03d}/m{j:03d}.xml", Body=b"x")
    c.put_object(Bucket="bkt", Key="r/k/x.xml", Body=b"x")
    c.put_object(Bucket="bkt", Key="r/pack.zip", Body=S.zip_bytes({"q.xml": b"1"}))
    c.put_object(Bucket="bkt", Key="r/top.xml", Body=b"x")
    return c


@pytest.mark.parametrize("others", [2, 30])
def test_s3_opening_one_bundle_lists_only_its_own_prefix(s3_env, others):
    from moto import mock_aws

    with mock_aws():
        _wide_bucket(others, 3)
        source = open_source("s3://bkt/r", 1)
        calls = _count_s3_calls(source.client)
        ref = source.ref_for("k")
        with source.open_bundle(ref) as b:
            assert [m.path for m in b.members] == ["x.xml"]
        lists = [p for n, p in calls if n == "list"]
        assert len(lists) == 2  # one existence probe, one listing of the bundle
        assert all(p["Prefix"] == "r/k/" for p in lists)
        assert lists[1]["Delimiter"] is None  # the bundle's own listing is recursive


@pytest.mark.parametrize("members", [1, 50])
def test_s3_discovery_lists_one_level_whatever_the_bundle_size(s3_env, members):
    from moto import mock_aws

    with mock_aws():
        _wide_bucket(3, members)
        source = open_source("s3://bkt/r", 1)
        calls = _count_s3_calls(source.client)
        got = keys(source)
        assert got == ["b000", "b001", "b002", "k", "pack.zip"]
        assert source.files_above_depth == 1
        lists = [p for n, p in calls if n == "list"]
        assert len(lists) == 1 and lists[0]["Delimiter"] == "/"


def test_s3_zip_ref_for_is_one_head_and_no_listing(s3_env):
    from moto import mock_aws

    with mock_aws():
        _wide_bucket(5, 5)
        source = open_source("s3://bkt/r", 1)
        calls = _count_s3_calls(source.client)
        ref = source.ref_for("pack.zip")
        assert ref.kind == "zip" and ref.stamp
        assert [n for n, _ in calls] == ["head"]


def test_s3_start_after_prunes_with_start_after_and_skips_earlier_bundles(s3_env):
    from moto import mock_aws

    with mock_aws():
        _wide_bucket(3, 2)
        source = open_source("s3://bkt/r", 1)
        calls = _count_s3_calls(source.client)
        assert keys_after(source, "b001") == ["b002", "k", "pack.zip"]
        lists = [p for n, p in calls if n == "list"]
        assert lists[0].get("StartAfter") == "r/b001"


def test_s3_start_after_at_depth_two_visits_the_ancestor(s3_env):
    import boto3
    from moto import mock_aws

    with mock_aws():
        c = boto3.client("s3", region_name="us-east-1")
        c.create_bucket(Bucket="bkt")
        for k in ("a/x", "a/y", "b/x", "b/y", "c/x"):
            c.put_object(Bucket="bkt", Key=f"{k}/f.xml", Body=b"1")
        source = open_source("s3://bkt", 2)
        assert keys_after(source, "a/x") == ["a/y", "b/x", "b/y", "c/x"]
        assert keys_after(source, "b/y") == ["c/x"]
        assert keys_after(source, "c/x") == []


def keys_after(source, start_after):
    return [r.key for r in source.list_bundles(start_after=start_after)]


def test_local_discovery_scans_only_to_the_bundle_depth(tmp_path, monkeypatch):
    S.write_tree(tmp_path, {"a/x/1.xml": b"1", "a/y/2.xml": b"1", "b/x/3.xml": b"1", "t.xml": b"1"})
    scanned = []
    real = os.scandir

    def spy(path):
        scanned.append(Path(path).relative_to(tmp_path).as_posix())
        return real(path)

    monkeypatch.setattr(src.os, "scandir", spy)
    source = open_source(str(tmp_path), 2)
    assert keys(source) == ["a/x", "a/y", "b/x"]
    assert sorted(scanned) == [".", "a", "b"]  # never a bundle's own directory


def test_local_start_after_nested(tmp_path):
    S.write_tree(tmp_path, {"a/x/1.xml": b"1", "a/y/2.xml": b"1", "b/x/3.xml": b"1", "c/z/4.xml": b"1"})
    source = open_source(str(tmp_path), 2)
    assert keys_after(source, "a/x") == ["a/y", "b/x", "c/z"]
    assert keys_after(source, "b/x") == ["c/z"]
    assert keys_after(source, "a") == ["a/x", "a/y", "b/x", "c/z"]
