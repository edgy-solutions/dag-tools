# Spec: one image-record document, one validator

Status: Part A ready to build. Part B waits on the owner's answer packet.
Branch: `feat/qual-image-record-validation`, off `master` at `e5db176` (#52 merged).

## Why

The architect ruled (2026-11-16) that the published file is the `ImageRecord`
document `record-image` writes. One document, one parser. A single dag-tools
command writes it in CI, and the owner builds that command using the qual
session's validation. Preflight reads the file by path or URL, pins its hash at
`qual init`, and fails if the file is unreadable or has changed.

Today nothing validates the document. `ImageRecord` is a bare pydantic model:

- it accepts an empty `images` list;
- it accepts any string as a digest;
- it accepts a repository listed twice;
- it accepts a repository carrying a tag or a digest;
- it accepts any `schema_version`;
- it accepts a naive `recorded_at`.

The only checks live in the `record-image` CLI's argument loop, which is the one
place a record is *not* read. A file fetched by URL is untrusted input, so the
rules belong to the document, not to one of its writers.

The model also lives in `registry/client.py`, which imports boto3 at module
level. The reference grammar lives in `preflight/identity.py`, whose package
`__init__` imports the GraphQL client (httpx) and boto3. So anything that
validates a record today drags in storage and network. The owner's writer and
my reader must share code that needs neither.

## Part A: the shared document module (build now)

### A1. New module `dag_tools/qual/image_record.py`

It is pure: no boto3, botocore, httpx, requests, dagster or typer import, at
module level or anywhere. Only stdlib and pydantic. `dag_tools/qual/__init__.py`
imports nothing, so importing this module stays light. Assert it (A5).

Move these here unchanged in behaviour, and re-export them from their old homes
so every existing import keeps working (§6):

- From `preflight/identity.py`: `ImageRef`, `parse_image_reference`,
  `_DIGEST_RE`. `identity.py` then does `from ..image_record import ImageRef,
  parse_image_reference, _DIGEST_RE` (or keeps its own `_HEX_RE`); the
  `sha_from_tag` and `same_repository` functions stay where they are.
- From `registry/client.py`: `RecordedImage`, `ImageRecord`. `client.py`
  re-imports them, and `registry/__init__.py` keeps exporting both names.
  `ImageRecordMismatch` stays in `client.py`, since it is a storage outcome.

New public names:

```python
KNOWN_SCHEMA_VERSIONS: tuple[int, ...] = (1,)

class ImageRecordInvalid(ValueError):
    """The document cannot be used. Carries every problem, not the first."""
    problems: list[str]     # one sentence each, naming the field

class ImageRecordForOtherCommit(ValueError):
    """A valid record that names a different (repo, git_sha) than asked for."""

def build_image_record(repo: str, git_sha: str, refs: Sequence[str], *,
                       now: datetime | None = None) -> ImageRecord: ...
def validate_image_record(record: ImageRecord) -> None: ...
def parse_image_record(body: bytes | str) -> ImageRecord: ...
def check_record_names(record: ImageRecord, repo: str, git_sha: str) -> None: ...
def render_image_record(record: ImageRecord) -> bytes: ...
```

### A2. The rules (`validate_image_record`)

Collect every problem, then raise `ImageRecordInvalid` once with all of them.
Someone fixing a producer wants the full list, which is the repo's standing
rule.

1. `schema_version` is in `KNOWN_SCHEMA_VERSIONS`. If not, the message names
   the version found and the versions known. Never guess.
2. `repo` is non-empty and has no leading or trailing whitespace.
3. `git_sha` matches `^[0-9a-fA-F]{7,40}$`. Do not normalise case. The name
   check (A4) is exact equality with the pin, as `_pinned_images` already does.
4. `images` is non-empty. A record naming no image would pin nothing, and the
   preflight would read it as a commit with nothing to verify.
5. For each image:
   - `digest` matches `^sha256:[0-9a-f]{64}$`. It must be lowercase, because
     every writer lowercases (`parse_image_reference` does), so an uppercase
     digest means a hand-edited file.
   - `repository` equals itself stripped, and `parse_image_reference(repository)`
     returns no error, `tag is None`, `digest is None`, and
     `.repository == repository`. In other words, no tag and no digest.
     `host:5000/a/b` is valid: a port is not a tag, and the existing grammar
     already knows that.
6. No repository appears twice, compared case-insensitively (the same comparison
   as `identity.same_repository`). The message names the repository.
7. `recorded_at` is timezone-aware. A naive time names no instant.

Messages carry field paths (`images[1].digest`) and may quote the offending
value. Nothing in the document is secret: a digest and a repository are public.

### A3. Parse (`parse_image_record`)

Accepts `bytes` or `str`. It refuses each of the following with
`ImageRecordInvalid`, never a raw `json`/pydantic error:

- bytes that are not UTF-8;
- text that is not JSON;
- JSON whose top level is not an object;
- a missing `schema_version`. The model default of 1 must not stand in for a
  file that never said it. Every record `record-image` has written includes the
  field, so this breaks no stored record;
- an unknown `schema_version`. This is checked **before** model validation, so a
  v2 file is refused by its version and not by whichever v1 field it lacks;
- any pydantic validation error. Fold it into `problems`, with one problem per
  pydantic error and the location path in each.

It then runs `validate_image_record`. Unknown extra fields are ignored
(`extra="ignore"`, unchanged).

### A4. Name check (`check_record_names`)

The check uses exact string equality on `repo` and `git_sha`. On a mismatch it
raises `ImageRecordForOtherCommit`, naming both pairs. A short sha in the file
against a full sha in the pin is a mismatch, because the record must name the
commit the pin names.

### A5. Build and render

`build_image_record(repo, git_sha, refs, *, now=None)` works as follows:

- Each ref goes through `parse_image_reference`, and the tag is ignored, as
  `record-image` does today.
- A ref with a parse error or no digest is a problem naming the ref.
- Two refs naming one repository (case-insensitively) with the same digest
  collapse to one entry, keeping the first spelling. Two tags of one image are
  one image.
- The same repository with two different digests is a problem. That is the
  condition the record exists to expose.
- `now` defaults to `datetime.now(timezone.utc)`. A naive `now` is a problem,
  not converted.
- It then runs `validate_image_record` on the result, so builder and reader
  enforce one rule set. All problems are raised together.

`render_image_record(record)` produces the canonical bytes:

- It validates first and refuses to render an invalid record.
- It renders `json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n"`,
  encoded UTF-8 with LF line endings. `payload` is `record.model_dump(mode="json")`
  with `images` sorted by `(repository.lower(), digest)`.
- `recorded_at` is rendered as pydantic's JSON mode renders an aware datetime.
  A UTC time must render with a `Z` or `+00:00` suffix. Pin whichever pydantic
  gives in a test, so a pydantic change shows up.
- `parse_image_record(render_image_record(r))` must equal `r` up to image order.
- The same inputs, including `now`, must give the same bytes whatever the order
  of `refs`.

### A6. Adopt it everywhere a record is made or read

- **`InventoryRegistry.record_image`:**
  - build the `ImageRecord` as now;
  - call `validate_image_record` before anything is read or written;
  - write `render_image_record(record)` instead of `model_dump_json()`;
  - keep idempotence and `ImageRecordMismatch` exactly as they are;
  - an invalid offer raises `ImageRecordInvalid` and touches storage not at all.
    That includes the existence read, so test it with a storage double that
    fails on any call.
- **`InventoryRegistry.read_image_record`:** parse through `parse_image_record`.
  Its docstring changes from "pydantic `ValidationError`" to `ImageRecordInvalid`
  (a `ValueError`). `_pinned_images` already catches `Exception`, so its
  behaviour is unchanged. Check that no test asserts `ValidationError` from this
  path. If one does, update it; that is the intended change.
- **`qual/qualify/init.py::_pinned_images`:** replace the inline repo/sha
  comparison with `check_record_names`. Keep the `ValueError` message naming the
  pinned repo and the key; wrap the `ImageRecordForOtherCommit`.
- **CLI `registry record-image`:**
  - Replace the per-ref loop with `build_image_record`. On `ImageRecordInvalid`,
    exit 2 and print every problem on stderr, one per line.
  - Add `--from-file PATH`, mutually exclusive with `--image`. Supplying both, or
    neither, exits 2 naming the two options.
  - With `--from-file`, read the bytes, `parse_image_record` them, then
    `check_record_names(record, repo, sha)`. `--repo` and `--sha` stay required:
    the operator says which commit they mean and the file must agree.
  - Then call `record_image(record.repo, record.git_sha, record.images, now=record.recorded_at)`,
    so the bucket and the published file carry one `recorded_at`.
  - Output format is unchanged.

  This is the "optional path, run only where storage access exists" from the
  ruling: it ingests the very file CI published.

The **writer command is the owner's** (`write-image-record` or whatever the
owner names it). Do not build it here. It will call `build_image_record` and then
`render_image_record`. Part A must make that a two-line command.

### A7. Tests: new `dag_tools_tests/test_image_record_document.py`

Every refusal gets a control. A refusal test without a control passes on a
validator that refuses everything.

**The document:**

- `test_a_rendered_record_parses_back_to_itself`
- `test_an_unknown_schema_version_is_refused_by_name`, with control
  `test_control_schema_version_1_parses`
- `test_a_v2_file_is_refused_by_its_version_not_a_missing_field`: a body with
  `schema_version: 2` and no `images`, where the problems name the version only.
- `test_a_missing_schema_version_is_refused_not_defaulted`
- `test_an_empty_image_list_is_refused`
- `test_a_repository_listed_twice_is_refused_case_insensitively`, with control
  `test_control_two_distinct_repositories_are_fine`
- `test_a_repository_carrying_a_tag_is_refused` and
  `test_a_repository_carrying_a_digest_is_refused`, with control
  `test_control_a_registry_port_is_not_a_tag`
- `test_a_malformed_digest_is_refused` and `test_an_uppercase_digest_is_refused`
- `test_a_naive_recorded_at_is_refused`
- `test_a_sha_that_is_not_hex_is_refused`
- `test_every_problem_is_reported_at_once`: plant 4 distinct defects and assert 4
  problems, each naming its field.
- `test_not_json_not_utf8_and_not_an_object_are_each_refused_as_invalid`:
  parametrised; the exception is `ImageRecordInvalid` and also `ValueError`.
- `test_extra_fields_are_ignored`

**The name check:**

- `test_a_record_for_another_repo_is_refused` and
  `test_a_record_for_another_sha_is_refused`
- `test_a_short_sha_is_not_the_pins_full_sha`
- control `test_control_the_exact_pair_passes`

**The builder:**

- `test_a_ref_without_a_digest_is_refused`
- `test_two_tags_of_one_image_collapse`
- `test_one_repository_at_two_digests_is_refused`
- `test_the_tag_is_not_recorded`
- `test_a_naive_now_is_refused`
- `test_the_bytes_do_not_depend_on_ref_order`
- `test_render_refuses_an_invalid_record`

**Purity:**

- `test_the_document_module_imports_no_storage_or_network`: in a subprocess,
  `import dag_tools.qual.image_record`, then assert none of boto3, botocore,
  httpx, requests, dagster or typer is in `sys.modules`.
- control `test_control_the_purity_scan_sees_a_planted_import`: in the same
  subprocess shape, import `boto3` first and assert the scan reports it. Without
  this, a broken scan passes.

**Re-exports (§6):**

- `test_the_old_import_paths_still_work`: `identity.parse_image_reference`,
  `identity.ImageRef`, `registry.ImageRecord`, `registry.RecordedImage`,
  `registry.client.ImageRecord`. Each is the same object as the new module's.

In `test_registry_record_image.py`, add:

- `test_an_invalid_offer_touches_no_storage` (storage double that raises on
  any call);
- `test_record_image_writes_the_canonical_bytes`;
- `test_read_image_record_refuses_a_hand_edited_uppercase_digest`;
- `test_cli_from_file_records_what_the_file_says`, with
  `test_cli_from_file_for_another_sha_is_refused` and
  `test_cli_from_file_and_image_together_is_refused`;
- `test_cli_reports_every_bad_image_at_once`.

Every existing test must pass unchanged, except one asserting
`ValidationError` from `read_image_record` (see A6).

### A8. Docs

- **`AGENTS.md`:** in §4g's pin-driven bullet, add one sentence: the record
  document has one validator, `dag_tools.qual.image_record`, read by every
  writer and every reader, and is pure. Name the regressions
  `test_every_problem_is_reported_at_once`, `test_a_v2_file_is_refused_by_its_version_not_a_missing_field`,
  `test_the_document_module_imports_no_storage_or_network` and its control.
  Keep it short.
- **`CHANGELOG.md` Unreleased:** a `registry read-image-record` / `qual init`
  now refuses a malformed record it previously accepted (an empty image list, a
  bad digest, a tagged repository, an unknown version), and `record-image` gains
  `--from-file`. This is a behaviour change, so say so.
- **README:** add the `--from-file` line where `record-image` is documented,
  if it is.

### A9. Acceptance

- The new and touched modules pass on the main venv.
- The whole `dag_tools_tests/test_*record*` and `test_preflight_image_identity*`
  set passes on the main venv and the floor venv.
- The full suite on the main venv shows no new failures.
- The floor venv's 6 `contract_mapping` failures are a known baseline (no `dlt`)
  and are not this branch's.

## Part B: the reader (after the owner answers)

The owner answered on 2026-11-16, in
`2026-11-16-packet-from-dag-tools-owner-image-record-answers.md`. The answers
that shape this part:

- **The record of truth.** It lives on branch `image-records` at
  `inventory/<repo>/<git_sha>/image.json`, the same path as `layout.py`.
  - Its credential-free raw URL follows
    `https://raw.githubusercontent.com/<owner>/<name>/image-records/inventory/<repo>/<sha>/image.json`.
  - That URL is derivable from `(repo, sha)`. This holds for a public repo only.
  - CI enforces immutability: identical bytes are a no-op, and different bytes
    fail the job. The URL itself is mutable by force-push, which is why the
    pinned sha256 below is load-bearing.
  - Raw content is cached for a few minutes, so a fresh record can 404 briefly
    after its build.
- **The release copy.** The same bytes are at
  `https://github.com/<owner>/<name>/releases/download/<tag>/image.json`. That URL
  is derivable from the tag, not from `(repo, sha)`. A pin therefore uses the
  records-branch URL.
- **The `repo` value.** dag-tools' own CI writes `dag-tools`. The owner declared
  this; it was not measured, because the survey runs in Jenkins. A wrong value
  must be caught by the repo-mismatch refusal, which stays a refusal.
- **The digest.** `outputs.digest` is the top-level image index digest, and
  every tag resolves to it. The owner measured this on 3 services × 3 tags.
- **All or nothing.** CI writes no record at all unless every declared service
  produced a valid digest. A record that is short a service cannot occur, so
  the reader models no such state.

- `InventoryPin.image_record: Optional[ImageRecordSource]`, where
  `ImageRecordSource` is `{source: str, sha256: str}`. It is additive, so
  manifest `SCHEMA_VERSION` goes from 5 to 6.
- `qual init --image-record-template REPO=TEMPLATE` (repeatable, keyed by
  inventory repo). TEMPLATE contains `{repo}` and `{sha}`, filled from the
  **pin's** own repo and git_sha.
  - `qual init --image-record REPO=PATH_OR_URL` is an explicit per-pin override.
    It is for a private repo, where the raw URL needs a credential.
  - The template is operator configuration. No host, owner or branch is written
    into the library.
- At init and at preflight, a 404 is reported apart from other read failures:
  "absent at source; a record can lag its build by minutes, retry". Both still
  fail. Only the message differs.
- Init fetches the source, `parse_image_record`s it, `check_record_names`
  against the pin, and records the sha256 of the bytes and the images
  (`InventoryPin.images`, unchanged).
  - If a bucket record also exists, the two image sets must agree or init fails.
- Preflight re-fetches the source at run time and compares the sha256.
  - If the source is unreadable, has changed, or no longer parses, that pin's
    identity fails, naming the source and which of the three it was.
  - There is no fallback to tag matching: a declared record that cannot be read
    is a failure, not an absence.
  - Pins with no `image_record` keep today's behaviour.
- The source is an `https://` URL or a local path. Plain `http://` is refused.
  The body is capped (64 KiB). The fetch goes through an injectable seam over
  httpx, so tests need no network.
