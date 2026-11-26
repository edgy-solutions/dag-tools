"""CI writes one image record per commit, or it writes nothing.

A deployment pins an image by DIGEST, and a qualification's preflight compares
what a code location is running against the digests recorded for the commit it
is pinned to. Nothing wrote those digests down, so that comparison had nothing
to match and every location read as unverified. The digest is also the one fact
only CI knows: it is minted by the push and cannot be recovered downstream for
a commit whose tags have since moved.

The document is the ``ImageRecord`` the qual registry already reads, written by
the one command that writes it -- one document, one parser, one writer. What
this module pins is the part on CI's side of that seam, and every invariant
here is a REFUSAL rather than a field, because the document has nowhere to
record a gap:

  * completeness is measured against the DECLARED fleet, never against the
    fragments that arrived. A record listing only the services that ran is a
    claim that what it lists is everything;
  * a record is never rewritten, so a partial one could never be corrected --
    therefore nothing is published at all unless every declared service
    produced a usable ref. "The file is absent" and "the file is short a
    service" are one state rather than two readings of one, which is the
    guarantee the reader side is specced against;
  * an absent fragment and an empty one are told apart, because they send an
    operator to different places: never ran, versus ran and pushed nothing;
  * a pull request records nothing;
  * the release job builds no document of its own. ``recorded_at`` makes two
    renders of one commit different bytes, so it fetches the commit's record,
    checks every digest against what the release names now resolve to, and
    attaches those exact bytes. That is what makes "retag, not rebuild means a
    release's digests equal its commit's" a claim this workflow checks rather
    than a sentence in AGENTS.md;
  * no credential is added to this repository's CI.

The fleet rule lives in a committed script rather than a YAML heredoc,
precisely so these tests can run it instead of string-matching a shell body
nobody here executes.
"""
import importlib.util
import json
import pathlib
import re
import subprocess
import sys

import pytest

yaml = pytest.importorskip("yaml")

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "build-containers.yml"
SCRIPT = ROOT / "scripts" / "image_refs.py"

REGISTRY = "ghcr.io/edgy-solutions/dag-tools"
FLEET = "central-gateway restate-worker user-deployment"
COMMIT = "4fbd272d97eadb45b64b4624b6e04a883950898c"

A = "sha256:" + "a" * 64
B = "sha256:" + "b" * 64
C = "sha256:" + "c" * 64
D = "sha256:" + "d" * 64


@pytest.fixture(scope="module")
def workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def image_refs():
    """The committed fleet script, imported as a module.

    It is a file rather than a heredoc so this is possible at all: a rule
    inside a YAML ``run:`` body can only ever be string-matched, never run.
    """
    spec = importlib.util.spec_from_file_location("_image_refs_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fragments(tmp_path, **services):
    """One file per service, named after it, holding its one line.

    ``None`` writes no file at all (the service never ran); ``""`` writes an
    empty one (it ran and pushed nothing). A bare digest is expanded into the
    full ref that service's own job would have written.
    """
    directory = tmp_path / "fragments"
    directory.mkdir(exist_ok=True)
    for service, value in services.items():
        service = service.replace("_", "-")
        if value is None:
            continue
        if value.startswith("sha256:"):
            value = f"{REGISTRY}/{service}@{value}"
        directory.joinpath(service).write_text(
            value + ("\n" if value else ""), encoding="utf-8"
        )
    return directory


def _record(path, images, *, repo="dag-tools", sha=COMMIT):
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "repo": repo,
                "git_sha": sha,
                "recorded_at": "2026-11-16T12:00:00Z",
                "images": [
                    {"repository": f"{REGISTRY}/{service}", "digest": digest}
                    for service, digest in images.items()
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


# --- the fleet rule: collect ------------------------------------------------


def test_completeness_is_measured_against_the_declared_fleet(image_refs, tmp_path):
    """Two of three services produced a ref; the record is refused.

    Derived from the successes instead, this would have been a complete record
    of two images -- indistinguishable from a two-service fleet.
    """
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker=B, user_deployment=None
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.collect(FLEET.split(), REGISTRY, fragments)
    assert "user-deployment" in str(exc.value)
    assert "1 of 3" in str(exc.value)


def test_control_the_same_fragments_under_their_own_fleet_collect_clean(
    image_refs, tmp_path
):
    """Without this, the test above would pass under any rule at all."""
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker=B, user_deployment=None
    )
    refs = image_refs.collect(["central-gateway", "restate-worker"], REGISTRY, fragments)
    assert refs == [f"{REGISTRY}/central-gateway@{A}", f"{REGISTRY}/restate-worker@{B}"]


def test_an_absent_fragment_and_an_empty_one_are_told_apart(image_refs, tmp_path):
    """Never ran, versus ran and pushed nothing. Different places to look."""
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker="", user_deployment=None
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.collect(FLEET.split(), REGISTRY, fragments)
    message = str(exc.value)
    assert "restate-worker: its fragment is empty" in message
    assert "user-deployment: no fragment at" in message


def test_a_fragment_naming_another_service_is_refused_not_recorded(
    image_refs, tmp_path
):
    """A ref that landed under the wrong name would record the wrong bytes for
    a service, which reads as an answer rather than as a gap."""
    fragments = _fragments(
        tmp_path,
        central_gateway=f"{REGISTRY}/restate-worker@{A}",
        restate_worker=B,
        user_deployment=C,
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.collect(FLEET.split(), REGISTRY, fragments)
    assert "central-gateway: its fragment reads" in str(exc.value)


def test_a_malformed_digest_is_refused_rather_than_recorded(image_refs, tmp_path):
    """A pin built from a malformed digest names bytes that do not exist,
    which is worse than a gap because it reads as an answer."""
    fragments = _fragments(
        tmp_path,
        central_gateway=f"{REGISTRY}/central-gateway@sha256:nothex",
        restate_worker=B,
        user_deployment=C,
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.collect(FLEET.split(), REGISTRY, fragments)
    assert "central-gateway" in str(exc.value)


def test_every_problem_is_named_at_once(image_refs, tmp_path):
    """Whoever is fixing CI wants the list, not the first entry of it."""
    fragments = _fragments(
        tmp_path, central_gateway=None, restate_worker="", user_deployment=None
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.collect(FLEET.split(), REGISTRY, fragments)
    message = str(exc.value)
    assert "3 of 3" in message
    for service in FLEET.split():
        assert service in message


def test_a_fleet_of_zero_services_is_refused(image_refs):
    """An empty fleet would make every record vacuously complete."""
    with pytest.raises(SystemExit) as exc:
        image_refs.parse_fleet("   ")
    assert "no services were declared" in str(exc.value)


def test_a_duplicated_service_is_refused(image_refs):
    with pytest.raises(SystemExit) as exc:
        image_refs.parse_fleet("a b a")
    assert "more than once" in str(exc.value)


def test_a_complete_fleet_collects_one_full_ref_per_service(image_refs, tmp_path):
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker=B, user_deployment=C
    )
    assert image_refs.collect(FLEET.split(), REGISTRY, fragments) == [
        f"{REGISTRY}/central-gateway@{A}",
        f"{REGISTRY}/restate-worker@{B}",
        f"{REGISTRY}/user-deployment@{C}",
    ]


def test_a_short_fleet_prints_no_ref_at_all(image_refs, tmp_path, capsys):
    """Through the entry point, because the job splats what this prints into
    ``--image`` arguments: a refusal that still printed two refs would leave
    the job holding a partial argument list to write a record from."""
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker=B, user_deployment=None
    )
    with pytest.raises(SystemExit):
        image_refs.main(
            [
                "collect",
                "--services", FLEET,
                "--registry", REGISTRY,
                "--fragments", str(fragments),
            ]
        )
    assert capsys.readouterr().out == ""


# --- the fleet rule: verify -------------------------------------------------


def test_a_release_whose_digests_all_agree_passes(image_refs, tmp_path):
    record = _record(
        tmp_path / "image.json",
        {"central-gateway": A, "restate-worker": B, "user-deployment": C},
    )
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker=B, user_deployment=C
    )
    image_refs.verify(FLEET.split(), REGISTRY, record, fragments)


def test_a_moved_digest_fails_the_release_naming_both(image_refs, tmp_path):
    """Retag, not rebuild, means these cannot differ. When they do, the
    release fails and the record is NOT rewritten -- which is what makes that
    sentence in AGENTS.md a checkable claim rather than an assertion."""
    record = _record(
        tmp_path / "image.json",
        {"central-gateway": A, "restate-worker": B, "user-deployment": C},
    )
    fragments = _fragments(
        tmp_path, central_gateway=D, restate_worker=B, user_deployment=C
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.verify(FLEET.split(), REGISTRY, record, fragments)
    message = str(exc.value)
    assert D in message and A in message
    assert "is NOT rewritten" in message


def test_a_record_naming_a_service_outside_the_fleet_is_refused(image_refs, tmp_path):
    record = _record(
        tmp_path / "image.json",
        {"central-gateway": A, "restate-worker": B, "user-deployment": C, "ghost": D},
    )
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker=B, user_deployment=C
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.verify(FLEET.split(), REGISTRY, record, fragments)
    assert "ghost" in str(exc.value)


def test_a_record_short_a_service_is_refused(image_refs, tmp_path):
    """The writer refuses before producing this, so it cannot arrive from
    there -- but a hand-edited or truncated record can, and the release says
    so rather than attaching it."""
    record = _record(
        tmp_path / "image.json", {"central-gateway": A, "restate-worker": B}
    )
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker=B, user_deployment=C
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.verify(FLEET.split(), REGISTRY, record, fragments)
    assert "user-deployment: the record names no digest for it." in str(exc.value)


def test_an_absent_record_is_refused_and_says_who_writes_it(image_refs, tmp_path):
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker=B, user_deployment=C
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.verify(FLEET.split(), REGISTRY, tmp_path / "nothing.json", fragments)
    message = str(exc.value)
    assert "no record at" in message
    assert "a release does not write one of" in message


def test_a_record_that_is_not_json_is_refused(image_refs, tmp_path):
    record = tmp_path / "image.json"
    record.write_text("not json at all", encoding="utf-8")
    fragments = _fragments(
        tmp_path, central_gateway=A, restate_worker=B, user_deployment=C
    )
    with pytest.raises(SystemExit) as exc:
        image_refs.verify(FLEET.split(), REGISTRY, record, fragments)
    assert "not readable as JSON" in str(exc.value)


def test_verifying_rewrites_nothing(image_refs, tmp_path):
    """The record belongs to the commit. A release checks it and attaches it;
    a disagreement fails the release rather than changing the record."""
    record = _record(
        tmp_path / "image.json",
        {"central-gateway": A, "restate-worker": B, "user-deployment": C},
    )
    before = record.read_bytes()
    fragments = _fragments(
        tmp_path, central_gateway=D, restate_worker=B, user_deployment=C
    )
    with pytest.raises(SystemExit):
        image_refs.verify(FLEET.split(), REGISTRY, record, fragments)
    assert record.read_bytes() == before


# --- the writing command ----------------------------------------------------


@pytest.fixture
def runner():
    return pytest.importorskip("typer.testing").CliRunner()


def _write(runner, tmp_path, *args, out="image.json"):
    from dag_tools.qual.cli import app

    target = tmp_path / out
    result = runner.invoke(
        app,
        ["registry", "write-image-record", "--repo", "dag-tools", "--sha", COMMIT]
        + list(args)
        + ["--out", str(target)],
    )
    return result, target


def test_the_writer_needs_no_registry_and_opens_no_storage(
    runner, tmp_path, monkeypatch
):
    """CI has no bucket credential and is not getting one.

    So this command must not go through ``require_registry`` -- every other
    command in the group does -- and must touch no storage. Doubles that raise
    on any call are what prove the second half; asserting the exit code alone
    would pass on a command that connected successfully.
    """
    import boto3

    import dag_tools.qual.cli as cli

    monkeypatch.delenv("DAGTOOLS_REGISTRY", raising=False)
    monkeypatch.delenv("DAGTOOLS_S3_ENDPOINT", raising=False)

    def refuse(*a, **k):
        raise AssertionError("this command must not touch storage")

    monkeypatch.setattr(cli, "InventoryRegistry", refuse)
    monkeypatch.setattr(cli, "StorageSettings", refuse)
    monkeypatch.setattr(boto3, "client", refuse)
    monkeypatch.setattr(boto3, "resource", refuse)

    result, target = _write(
        runner, tmp_path, "--image", f"{REGISTRY}/central-gateway@{A}"
    )
    assert result.exit_code == 0, result.output
    assert json.loads(target.read_text(encoding="utf-8"))["images"] == [
        {"repository": f"{REGISTRY}/central-gateway", "digest": A}
    ]


def test_the_writer_writes_nothing_when_it_refuses(runner, tmp_path):
    """A partial document on disk is a document some later step would read."""
    result, target = _write(runner, tmp_path, "--image", f"{REGISTRY}/central-gateway")
    assert result.exit_code != 0
    assert not target.exists()


def test_the_writer_names_every_problem_at_once(runner, tmp_path):
    result, _ = _write(
        runner,
        tmp_path,
        "--image", f"{REGISTRY}/central-gateway",
        "--image", f"{REGISTRY}/restate-worker",
    )
    assert result.exit_code != 0
    assert result.output.count("digest") >= 2


def test_the_sha_is_carried_exactly_as_given(runner, tmp_path):
    """The reader compares it to the pin by exact string equality, so
    lowercasing or shortening it here would silently stop a pin matching."""
    result, target = _write(
        runner, tmp_path, "--image", f"{REGISTRY}/central-gateway@{A}"
    )
    assert result.exit_code == 0, result.output
    assert json.loads(target.read_text(encoding="utf-8"))["git_sha"] == COMMIT


def test_what_the_writer_writes_is_what_the_reader_parses(runner, tmp_path):
    """One document, one parser. A file this command writes cannot fail the
    reader's parse unless somebody edits it in between."""
    from dag_tools.qual.image_record import parse_image_record

    result, target = _write(
        runner,
        tmp_path,
        "--image", f"{REGISTRY}/central-gateway@{A}",
        "--image", f"{REGISTRY}/restate-worker@{B}",
    )
    assert result.exit_code == 0, result.output
    record = parse_image_record(target.read_bytes())
    assert record.repo == "dag-tools"
    assert record.git_sha == COMMIT
    assert sorted(i.digest for i in record.images) == sorted([A, B])


# --- the workflow -----------------------------------------------------------


def test_the_declared_fleet_is_both_matrices(workflow):
    """The fleet is declared once and two matrices must agree with it. A
    service that builds and is not retagged has no release tag at all; one
    that is neither would silently drop out of every record."""
    declared = set(workflow["env"]["SERVICES"].split())
    built = {
        entry["service"]
        for entry in workflow["jobs"]["build"]["strategy"]["matrix"]["include"]
    }
    retagged = set(workflow["jobs"]["retag"]["strategy"]["matrix"]["service"])
    assert declared == built, (declared, built)
    assert declared == retagged, (declared, retagged)


def test_the_build_step_captures_the_digest_it_mints(workflow):
    """Without an ``id:`` the push's digest is unreadable, and it is the one
    fact nothing downstream can recover.

    The count is of the steps that PUSH, not of every build step. The job also
    builds the gateway image locally for the signing check, which pushes
    nothing and so mints no digest for anything to record. A step counts as
    non-pushing only if it says ``push: false`` unconditionally -- the pushing
    step's own value is an expression (``github.event_name == 'push'``), and
    the action pushes by default when the key is absent -- so a second pushing
    step still trips the count whether it pushes always or only sometimes.
    That is the property worth keeping: with two of them, WHICH digest the
    record names would be an accident of ordering.
    """
    steps = workflow["jobs"]["build"]["steps"]
    builds = [
        s for s in steps if str(s.get("uses", "")).startswith("docker/build-push-action")
    ]
    push = [s for s in builds if s.get("with", {}).get("push", True) is not False]
    assert len(push) == 1, [s.get("name") for s in push]
    assert push[0]["id"] == "push"
    for other in builds:
        if other is push[0]:
            continue
        assert other.get("id") != "push", other.get("name")
    record = [s for s in steps if s.get("name") == "Record this image's ref"]
    assert len(record) == 1
    assert "steps.push.outputs.digest" in record[0]["env"]["DIGEST"]


def test_the_fragment_is_written_whether_or_not_a_digest_was_minted(workflow):
    """A step that skipped itself would leave "ran and pushed nothing" looking
    exactly like "never ran", and those two send an operator to different
    places. So the fragment is written empty rather than not written."""
    record = [
        s
        for s in workflow["jobs"]["build"]["steps"]
        if s.get("name") == "Record this image's ref"
    ][0]
    assert "${DIGEST:+" in record["run"]
    assert "github.event_name" in record["if"]


def test_a_pull_request_records_nothing(workflow):
    """A PR pushes no image, so a record of one would name bytes nobody can
    pull."""
    assert "github.event_name" in workflow["jobs"]["image-record"]["if"]
    for job in ("image-record", "release-record"):
        assert "pull_request" not in str(workflow["jobs"][job])


def test_the_record_job_runs_even_when_one_build_failed(workflow):
    """``success()`` would skip quietly; ``!cancelled()`` reaches the
    collector, whose refusal names the services it is short and where their
    fragments should have been."""
    assert "!cancelled()" in workflow["jobs"]["image-record"]["if"]
    download = [
        s
        for s in workflow["jobs"]["image-record"]["steps"]
        if str(s.get("uses", "")).startswith("actions/download-artifact")
    ]
    assert len(download) == 1
    assert download[0]["continue-on-error"] is True


def test_the_record_of_truth_is_a_branch_and_the_artifact_is_only_a_convenience(
    workflow,
):
    """An Actions artifact expires; a pin outlives it. So the record of truth
    is a commit on a branch of its own, and the artifact says what it is."""
    job = workflow["jobs"]["image-record"]
    assert workflow["env"]["RECORDS_BRANCH"]
    commit = [s for s in job["steps"] if s.get("name") == "Commit the record"][0]
    assert "RECORDS_BRANCH" in commit["run"]
    artifacts = [
        s
        for s in job["steps"]
        if str(s.get("uses", "")).startswith("actions/upload-artifact")
    ]
    assert len(artifacts) == 1
    assert "convenience, never the record" in WORKFLOW.read_text(encoding="utf-8")


def test_the_record_path_is_derivable_from_the_pin(workflow):
    """A reader derives the URL from a pin's ``repo`` and ``sha``, so the path
    mirrors the registry's own layout and nothing else. Spelled here by
    calling that layout, so a change to it fails this test rather than leaving
    two derivations that agree only today."""
    from dag_tools.qual.registry import layout

    expected = (
        layout.inventory_build_prefix("${INVENTORY_REPO}", "${GITHUB_SHA}")
        + layout.IMAGE_FILE
    )
    for job in ("image-record", "release-record"):
        bodies = "\n".join(s.get("run", "") for s in workflow["jobs"][job]["steps"])
        assert 'RECORD_PATH="' + expected + '"' in bodies


def test_an_existing_record_is_never_rewritten(workflow):
    """Identical bytes are a no-op; differing bytes fail. A commit's images do
    not change, so a disagreement means something rebuilt them, and that is
    not a thing to paper over by overwriting."""
    commit = [
        s
        for s in workflow["jobs"]["image-record"]["steps"]
        if s.get("name") == "Commit the record"
    ][0]["run"]
    assert "cmp -s" in commit
    assert "A record is never rewritten" in commit
    assert "DIFFERENT bytes" in commit


def test_the_record_job_does_not_install_the_whole_library(workflow):
    """It writes a ten-line JSON document. A full install would drag in
    pyodbc, oracledb, deltalake, connectorx and clickhouse-connect, and
    couple the record path to whether those wheels build today."""
    install = [
        s
        for s in workflow["jobs"]["image-record"]["steps"]
        if s.get("name") == "Install the writer"
    ][0]["run"]
    assert "--no-deps -e ." in install
    assert not re.search(r"pip install (?!.*--no-deps)[^\n]*\s\.\s*$", install, re.M)


def test_the_release_job_builds_no_document_of_its_own(workflow):
    """``recorded_at`` is the time the record was written, so two renders of
    one commit could never be the same bytes. The release fetches the commit's
    record, verifies it, and attaches those exact bytes."""
    bodies = "\n".join(
        s.get("run", "") for s in workflow["jobs"]["release-record"]["steps"]
    )
    assert "write-image-record" not in bodies
    assert "image_refs.py verify" in bodies


def test_the_release_record_is_attached_not_created(workflow):
    """A second creator makes a second release rather than colliding.

    The record is uploaded into the release `build-package.yml` creates, and
    this workflow is called from a job there that `needs` the creator, so the
    release exists already: the attach does not poll, and a failed upload fails.
    """
    job = workflow["jobs"]["release-record"]
    assert not any("action-gh-release" in str(s.get("uses", "")) for s in job["steps"])
    bodies = "\n".join(s.get("run", "") for s in job["steps"])
    assert "gh release upload" in bodies
    # Only the ATTACH is asserted not to wait. Fetching the record still retries,
    # because a record can land on the records branch minutes after its build.
    (attach,) = [s for s in job["steps"] if "gh release upload" in s.get("run", "")]
    assert "gh release view" not in attach["run"] and "sleep" not in attach["run"]
    assert "::error::" in attach["run"]


def test_no_credential_is_added_to_this_workflow(workflow):
    """The whole mechanism rides the runner's own token."""
    used = set(
        re.findall(r"secrets\.([A-Za-z0-9_]+)", WORKFLOW.read_text(encoding="utf-8"))
    )
    assert used == {"GITHUB_TOKEN"}, used


def test_only_the_record_jobs_may_write_contents(workflow):
    """Writing is what a record needs and nothing else here needs it."""
    assert workflow["permissions"]["contents"] == "read"
    writers = {
        name
        for name, job in workflow["jobs"].items()
        if (job.get("permissions") or {}).get("contents") == "write"
    }
    assert writers == {"image-record", "release-record"}


def test_the_release_fragment_is_written_inside_the_retag_script(workflow):
    """It must hold what the registry was RESOLVED to answer with, not what
    the job intended to write -- otherwise the release verifies its own
    intention and the check is vacuous."""
    scripts = [s for s in workflow["jobs"]["retag"]["steps"] if "run" in s]
    assert len(scripts) == 1, "the retag job should be a single scripted step"
    body = scripts[0]["run"]
    assert "digest-fragment/${SERVICE}" in body
    assert "@${SOURCE_DIGEST}" in body


def test_both_record_jobs_run_the_committed_script(workflow):
    """A rule in a heredoc can only ever be string-matched, never run."""
    assert SCRIPT.is_file()
    for job, subcommand in (("image-record", "collect"), ("release-record", "verify")):
        bodies = "\n".join(s.get("run", "") for s in workflow["jobs"][job]["steps"])
        assert "scripts/image_refs.py " + subcommand in bodies


def test_the_script_runs_under_a_bare_interpreter(workflow):
    """Both jobs call it as ``python3 scripts/image_refs.py``, and the release
    job installs nothing at all, so it must not need this library importable."""
    assert (
        subprocess.run(
            [sys.executable, str(SCRIPT), "--help"], capture_output=True, cwd=ROOT
        ).returncode
        == 0
    )
    assert [
        s
        for s in workflow["jobs"]["release-record"]["steps"]
        if "pip install" in s.get("run", "")
    ] == []
