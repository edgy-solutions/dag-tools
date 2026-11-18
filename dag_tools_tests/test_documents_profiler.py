"""Tests for the document bundle profiler (synthetic vocabulary only)."""
import io
import json
import re
import zipfile
from pathlib import Path

import pytest

pytest.importorskip("lxml")
pytest.importorskip("pyarrow")
PIL_Image = pytest.importorskip("PIL.Image")

from typer.testing import CliRunner  # noqa: E402

from dag_tools.documents import profiler  # noqa: E402
from dag_tools.documents.profiler import run_profile  # noqa: E402

PLANTED = "ZQX-PLANTED-4471"


def manifest_xml(n: int) -> bytes:
    return (
        f'<Manifest><Depot site="S{n}" dock="D1" shift="A"/>'
        f'<Parcel id="P{n}"><Scan id="C{n}" at="2026-01-02T03:04:05.123Z" lane="1">'
        '<Reading>1.5</Reading><Limit kind="low" value="1"/>'
        f'<Item sku="{PLANTED}" qty="2"/></Scan></Parcel>'
        f'<Flag id="F{n}" code="x"><ScanRef ref="C{n}"/></Flag>'
        '<Extras><Photos><photo path="sub\\a.png" scan="C1"/></Photos></Extras>'
        '<Review flagRef="F1" by="u"><Decision code="ok"/></Review></Manifest>'
    ).encode()


def png() -> bytes:
    buf = io.BytesIO()
    PIL_Image.new("RGB", (4, 3), (200, 10, 10)).save(buf, "PNG")
    return buf.getvalue()


def members(n: int) -> dict:
    stem = f"manifest_{n:04d}"
    return {f"{stem}.xml": manifest_xml(n), f"{stem}/sub/a.png": png()}


@pytest.fixture
def zip_src(tmp_path):
    d = tmp_path / "src"
    d.mkdir()
    for n in (1, 2, 3):
        with zipfile.ZipFile(d / f"bundle_{n}.zip", "w") as z:
            for name, data in members(n).items():
                z.writestr(name, data)
    return d


@pytest.fixture
def tree_src(tmp_path):
    d = tmp_path / "tree"
    for n in (1, 2):
        for name, data in members(n).items():
            p = d / f"b{n}" / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
    return d


def all_bytes(out: Path) -> bytes:
    return b"".join(p.read_bytes() for p in sorted(out.rglob("*"))
                    if p.is_file() and "checkpoint" not in p.parts)


def test_zip_bundles_profile(zip_src, tmp_path):
    out = tmp_path / "out"
    res = run_profile(source=str(zip_src), out=out)
    assert res.bundles_profiled == 3
    assert res.statuses == {"done": 3}
    for f in ("summary.md", "path_stats.csv", "shapes.json", "docs.parquet"):
        assert (out / f).exists()
    assert res.docs_by_family
    imgs = (out / "images.csv").read_text(encoding="utf-8")
    assert "content_ref" in imgs


def test_tree_source(tree_src, tmp_path):
    res = run_profile(source=str(tree_src), out=tmp_path / "out", bundle_depth=1)
    assert res.bundles_profiled == 2
    assert res.errors == 0


def test_resume_skips_done_bundles(zip_src, tmp_path):
    out = tmp_path / "out"
    first = run_profile(source=str(zip_src), out=out, sample=1)
    assert first.bundles_profiled == 1
    seen = []
    second = run_profile(source=str(zip_src), out=out, on_bundle_done=seen.append)
    assert second.bundles_profiled == 2 and len(seen) == 2
    assert second.statuses == {"done": 3}
    third = run_profile(source=str(zip_src), out=out)
    assert third.bundles_profiled == 0


def test_stop_flag_then_rerun(zip_src, tmp_path):
    out = tmp_path / "out"
    res = run_profile(source=str(zip_src), out=out, on_bundle_done=lambda k: profiler.request_stop())
    assert res.interrupted and res.exit_code == 130
    assert res.bundles_profiled == 1
    res2 = run_profile(source=str(zip_src), out=out)
    assert res2.bundles_profiled == 2 and res2.statuses == {"done": 3}


def test_report_only_reads_no_bundle(zip_src, tmp_path):
    out = tmp_path / "out"
    run_profile(source=str(zip_src), out=out)
    (out / "summary.md").unlink()
    for z in zip_src.glob("*.zip"):
        z.unlink()
    res = run_profile(out=out, report_only=True)
    assert (out / "summary.md").exists()
    assert res.statuses == {"done": 3}


def test_redact_hides_value_and_control_sees_it(zip_src, tmp_path):
    plain = tmp_path / "plain"
    run_profile(source=str(zip_src), out=plain)
    assert PLANTED.encode() in all_bytes(plain)
    red = tmp_path / "red"
    run_profile(source=str(zip_src), out=red, redact=True)
    assert PLANTED.encode() not in all_bytes(red)
    assert PLANTED.encode() not in (red / "checkpoint" / "state.json").read_bytes()


def test_path_files(zip_src, tmp_path):
    out = tmp_path / "out"
    run_profile(source=str(zip_src), out=out)
    files = list((out / "paths").glob("*.txt"))
    assert files
    assert all(all(c.isalnum() or c in "._-" for c in f.name) for f in files)
    lines = files[0].read_text(encoding="utf-8").splitlines()
    assert "/Manifest/Parcel/Scan/@at" in lines
    assert lines == sorted(lines)
    assert not any(":" in ln for ln in lines)
    assert "paths/" in (out / "summary.md").read_text(encoding="utf-8")


def test_state_is_json_and_not_pickle(zip_src, tmp_path):
    out = tmp_path / "out"
    run_profile(source=str(zip_src), out=out)
    state = json.loads((out / "checkpoint" / "state.json").read_text(encoding="utf-8"))
    assert state["version"] == profiler.STATE_VERSION
    assert not list((out / "checkpoint").glob("*.pkl"))
    assert not list((out / "checkpoint").glob("*.tmp"))


def test_cli_help_and_profile(zip_src, tmp_path):
    from dag_tools.qual.cli import app

    # Rich styles option names with ANSI escapes wherever it thinks a terminal
    # is attached (CI runners included), which splits "--report-only" in the
    # raw output. Ask for plain text and strip any styling that survives.
    r = CliRunner().invoke(app, ["documents", "profile", "--help"],
                           env={"NO_COLOR": "1", "TERM": "dumb", "COLUMNS": "200"})
    plain = re.sub(r"\x1b\[[0-9;]*m", "", r.output)
    assert r.exit_code == 0 and "--report-only" in plain, plain
    r = CliRunner().invoke(app, ["documents", "profile", "--source", str(zip_src),
                                 "--out", str(tmp_path / "o")])
    assert r.exit_code == 0, r.output
    assert (tmp_path / "o" / "summary.md").exists()


@pytest.fixture
def broken_src(tmp_path):
    # lxml's mismatch message quotes the tag it choked on, so the plant
    # reaches the error text unless redact keeps only a digest of it.
    d = tmp_path / "broken"
    d.mkdir()
    with zipfile.ZipFile(d / "bundle_1.zip", "w") as z:
        z.writestr("manifest_0001.xml", f"<Manifest><{PLANTED}></Wrong></Manifest>".encode())
    return d


def test_control_a_parse_error_quotes_the_document_without_redact(broken_src, tmp_path):
    out = tmp_path / "plain"
    run_profile(source=str(broken_src), out=out)
    assert PLANTED.encode() in all_bytes(out)


def test_a_parse_error_under_redact_keeps_no_text_from_the_document(broken_src, tmp_path, caplog):
    out = tmp_path / "red"
    with caplog.at_level("DEBUG"):
        run_profile(source=str(broken_src), out=out, redact=True)
    assert b"parse:" in all_bytes(out)  # the error is still recorded
    assert PLANTED.encode() not in all_bytes(out)
    assert PLANTED.encode() not in (out / "checkpoint" / "state.json").read_bytes()
    assert PLANTED not in caplog.text
