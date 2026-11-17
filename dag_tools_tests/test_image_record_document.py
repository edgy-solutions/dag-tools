"""The image-record document: one validator, shared by every writer and reader."""
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

import dag_tools.qual.image_record as mod
from dag_tools.qual.image_record import (
    ImageRecord,
    ImageRecordForOtherCommit,
    ImageRecordInvalid,
    RecordedImage,
    build_image_record,
    check_record_names,
    parse_image_record,
    render_image_record,
    validate_image_record,
)

SHA = "9f2c1ab4d5e6f708192a3b4c5d6e7f8091a2b3c4"
D1 = "sha256:" + "a" * 64
D2 = "sha256:" + "b" * 64
REPO_IMG = "registry.example.com:5000/team/app"
NOW = datetime(2026, 11, 16, 12, 0, 0, tzinfo=timezone.utc)


def _doc(**over):
    d = {
        "schema_version": 1, "repo": "domain-a", "git_sha": SHA,
        "images": [{"repository": REPO_IMG, "digest": D1}],
        "recorded_at": "2026-11-16T12:00:00Z",
    }
    d.update(over)
    return d


def _parse(**over):
    return parse_image_record(json.dumps(_doc(**over)))


def _problems(**over):
    with pytest.raises(ImageRecordInvalid) as e:
        _parse(**over)
    return e.value.problems


def _record(**over):
    kw = dict(repo="domain-a", git_sha=SHA, images=[RecordedImage(repository=REPO_IMG, digest=D1)],
              recorded_at=NOW)
    kw.update(over)
    return ImageRecord(**kw)


# --- the document ---------------------------------------------------------


def test_a_rendered_record_parses_back_to_itself():
    r = build_image_record("domain-a", SHA, [f"{REPO_IMG}@{D1}", f"other/x@{D2}"], now=NOW)
    back = parse_image_record(render_image_record(r))
    assert back.recorded_at == r.recorded_at and back.repo == r.repo
    key = lambda i: (i.repository, i.digest)
    assert sorted(back.images, key=key) == sorted(r.images, key=key)


def test_an_unknown_schema_version_is_refused_by_name():
    p = _problems(schema_version=7)
    assert len(p) == 1 and "schema_version" in p[0] and "7" in p[0]


def test_control_schema_version_1_parses():
    assert _parse(schema_version=1).schema_version == 1


def test_a_v2_file_is_refused_by_its_version_not_a_missing_field():
    body = json.dumps({"schema_version": 2, "repo": "x"})
    with pytest.raises(ImageRecordInvalid) as e:
        parse_image_record(body)
    assert len(e.value.problems) == 1
    assert "schema_version" in e.value.problems[0]
    assert not any("images" in p for p in e.value.problems)


def test_a_missing_schema_version_is_refused_not_defaulted():
    d = _doc()
    del d["schema_version"]
    with pytest.raises(ImageRecordInvalid) as e:
        parse_image_record(json.dumps(d))
    assert "schema_version" in e.value.problems[0]


def test_an_empty_image_list_is_refused():
    assert any(p.startswith("images:") for p in _problems(images=[]))


def test_a_repository_listed_twice_is_refused_case_insensitively():
    p = _problems(images=[{"repository": "Reg/App", "digest": D1},
                          {"repository": "reg/app", "digest": D2}])
    assert any("listed twice" in x and "images[1].repository" in x for x in p)


def test_control_two_distinct_repositories_are_fine():
    assert len(_parse(images=[{"repository": "reg/app", "digest": D1},
                              {"repository": "reg/app2", "digest": D2}]).images) == 2


def test_a_repository_carrying_a_tag_is_refused():
    p = _problems(images=[{"repository": "reg/app:v1", "digest": D1}])
    assert any("images[0].repository" in x and "tag" in x for x in p)


def test_a_repository_carrying_a_digest_is_refused():
    p = _problems(images=[{"repository": f"reg/app@{D1}", "digest": D1}])
    assert any("images[0].repository" in x for x in p)


def test_control_a_registry_port_is_not_a_tag():
    assert _parse(images=[{"repository": "host:5000/a/b", "digest": D1}]).images[0].repository == "host:5000/a/b"


@pytest.mark.parametrize("bad", ["sha256:abc", "md5:" + "a" * 32, "sha256:" + "g" * 64, ""])
def test_a_malformed_digest_is_refused(bad):
    p = _problems(images=[{"repository": "reg/app", "digest": bad}])
    assert any("images[0].digest" in x for x in p)


def test_an_uppercase_digest_is_refused():
    p = _problems(images=[{"repository": "reg/app", "digest": "sha256:" + "A" * 64}])
    assert any("images[0].digest" in x for x in p)


def test_a_naive_recorded_at_is_refused():
    assert any("recorded_at" in x for x in _problems(recorded_at="2026-11-16T12:00:00"))


@pytest.mark.parametrize("bad", ["xyz1234", "abc", "a" * 41, ""])
def test_a_sha_that_is_not_hex_is_refused(bad):
    assert any(x.startswith("git_sha") for x in _problems(git_sha=bad))


def test_every_problem_is_reported_at_once():
    p = _problems(
        git_sha="nothex",
        images=[{"repository": "reg/app:v1", "digest": "sha256:zz"}],
        recorded_at="2026-11-16T12:00:00",
    )
    assert len(p) == 4, p
    for field in ("git_sha", "images[0].repository", "images[0].digest", "recorded_at"):
        assert sum(field in x for x in p) == 1, (field, p)


def test_control_a_clean_record_reports_no_problem():
    validate_image_record(_parse())


@pytest.mark.parametrize("body", [b"\xff\xfe\x00bad", b"{not json", b"[1, 2]", b'"str"', b"", "null"])
def test_not_json_not_utf8_and_not_an_object_are_each_refused_as_invalid(body):
    with pytest.raises(ImageRecordInvalid) as e:
        parse_image_record(body)
    assert isinstance(e.value, ValueError) and e.value.problems


def test_a_pydantic_type_error_is_folded_into_problems_with_its_path():
    p = _problems(images=[{"repository": "reg/app"}])
    assert any("images.0.digest" in x for x in p)


def test_extra_fields_are_ignored():
    assert _parse(future_field={"a": 1}).repo == "domain-a"


# --- the name check -------------------------------------------------------


def test_a_record_for_another_repo_is_refused():
    with pytest.raises(ImageRecordForOtherCommit):
        check_record_names(_record(), "domain-b", SHA)


def test_a_record_for_another_sha_is_refused():
    with pytest.raises(ImageRecordForOtherCommit):
        check_record_names(_record(), "domain-a", "1" * 40)


def test_a_short_sha_is_not_the_pins_full_sha():
    with pytest.raises(ImageRecordForOtherCommit):
        check_record_names(_record(git_sha=SHA[:7]), "domain-a", SHA)


def test_control_the_exact_pair_passes():
    check_record_names(_record(), "domain-a", SHA)


# --- the builder ----------------------------------------------------------


def test_a_ref_without_a_digest_is_refused():
    with pytest.raises(ImageRecordInvalid) as e:
        build_image_record("domain-a", SHA, [f"{REPO_IMG}:v1"], now=NOW)
    assert any(REPO_IMG in p for p in e.value.problems)


def test_two_tags_of_one_image_collapse():
    r = build_image_record("domain-a", SHA, [f"{REPO_IMG}:v1@{D1}", f"{REPO_IMG}:latest@{D1}"], now=NOW)
    assert len(r.images) == 1


def test_one_repository_at_two_digests_is_refused():
    with pytest.raises(ImageRecordInvalid) as e:
        build_image_record("domain-a", SHA, [f"{REPO_IMG}@{D1}", f"{REPO_IMG}@{D2}"], now=NOW)
    assert any("two different digests" in p for p in e.value.problems)


def test_the_tag_is_not_recorded():
    r = build_image_record("domain-a", SHA, [f"{REPO_IMG}:v1@{D1}"], now=NOW)
    assert r.images[0].repository == REPO_IMG
    assert "v1" not in render_image_record(r).decode()


def test_a_naive_now_is_refused():
    with pytest.raises(ImageRecordInvalid) as e:
        build_image_record("domain-a", SHA, [f"{REPO_IMG}@{D1}"], now=datetime(2026, 1, 1))
    assert any(p.startswith("now:") for p in e.value.problems)


def test_the_bytes_do_not_depend_on_ref_order():
    refs = [f"b/x@{D2}", f"A/y@{D1}", f"{REPO_IMG}@{D1}"]
    a = render_image_record(build_image_record("domain-a", SHA, refs, now=NOW))
    b = render_image_record(build_image_record("domain-a", SHA, list(reversed(refs)), now=NOW))
    assert a == b and a.endswith(b"\n") and b"\r" not in a


def test_render_refuses_an_invalid_record():
    with pytest.raises(ImageRecordInvalid):
        render_image_record(_record(images=[]))
    with pytest.raises(ImageRecordInvalid):
        validate_image_record(_record(recorded_at=datetime(2026, 1, 1)))


def test_pydantic_renders_an_aware_utc_time_as_z():
    # Pinned so a pydantic change in datetime rendering shows up here.
    out = json.loads(render_image_record(_record()))
    assert out["recorded_at"] == "2026-11-16T12:00:00Z"


def test_an_offset_time_round_trips_to_the_same_instant():
    t = datetime(2026, 11, 16, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    back = parse_image_record(render_image_record(_record(recorded_at=t)))
    assert back.recorded_at == t


# --- purity ---------------------------------------------------------------

_SCAN = (
    "import sys\n{pre}"
    "import dag_tools.qual.image_record\n"
    "bad = [m for m in ('boto3','botocore','httpx','requests','dagster','typer') if m in sys.modules]\n"
    "print(','.join(bad))\n"
)


def _scan(pre=""):
    r = subprocess.run([sys.executable, "-c", _SCAN.format(pre=pre)],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


def test_the_document_module_imports_no_storage_or_network():
    assert _scan() == ""


def test_control_the_purity_scan_sees_a_planted_import():
    pytest.importorskip("boto3")
    assert "boto3" in _scan("import boto3\n")


# --- re-exports (section 6) ----------------------------------------------


def test_the_old_import_paths_still_work():
    pytest.importorskip("boto3")
    pytest.importorskip("httpx")
    from dag_tools.qual import registry
    from dag_tools.qual.registry import client
    from dag_tools.qual.preflight import identity

    assert identity.parse_image_reference is mod.parse_image_reference
    assert identity.ImageRef is mod.ImageRef
    assert registry.ImageRecord is mod.ImageRecord
    assert registry.RecordedImage is mod.RecordedImage
    assert client.ImageRecord is mod.ImageRecord
