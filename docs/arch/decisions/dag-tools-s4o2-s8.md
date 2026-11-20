# dag-tools decisions: §4o-2 and §8

This record covers the decisions in `AGENTS.md` sections 4o-2 and 8 at the component-records pin (`add5bc4`): the gateway image's signature support, and release images. [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §4o-2 The gateway image's signature support

The gateway is described in the [central gateway record](../components/central-gateway.md).

### §4o-2.1 The token library is installed with its cryptography extra

- **Decision.** The gateway image installs the token library with its cryptography extra, never without it. The image's Dockerfile is its dependency manifest.
- **Why.** Without the extra, the library still imports and the container still starts and passes its health check. It registers only the shared-secret algorithms. A gateway configured for an RSA algorithm, which is the default, then refuses every token as invalid. That looks like a fault at the token issuer when it is a fault in the image.
- **Where else.** The library's broker extra in `pyproject.toml` names the same extra.
- **Status.** `built`.

### §4o-2.2 The check runs against the built image, before the push

- **Decision.** A script checks signature support inside the built gateway image, before anything is pushed. It mints a key in memory, signs a token, verifies it on both paths the gateway can take, and checks every algorithm that needs the cryptography package. It reports every problem at once and exits non-zero.
- **Why.** A check in a development environment says nothing about an image. The pushed build is multi-architecture and cannot be loaded locally, and a pull request pushes nothing. So the workflow makes a separate single-architecture build that it can load and run.
- **Where else.** The check needs no network, no key on disk and no credential. The workflow runs it with networking disabled, so that is a property of how CI runs it.
- **Status.** `built`.

### §4o-2.3 A log line names the gap; the refusal reason stays the same

- **Decision.** The gateway logs a configured algorithm that the installed library cannot verify:
  - at error level when nothing configured can verify anything;
  - at warning level when only some of it cannot.

  The line is said once per distinct state, so an image that is fixed and then broken again says so again. A failure inside the check costs a log line and never a refusal. The refusal reason stays the invalid-token reason.
- **Why.** The token really cannot be verified by this process. Each reason is also a number on the health endpoint and an HTTP status (census §4o-1). Moving this case to a deployment-fault reason is a decision about that vocabulary.
- **Status.** The log line is `built`. Moving the reason is `proposed`: the census flags it and does not take it.

## §8 Release images

These decisions govern the release workflow, `.github/workflows/build-containers.yml`. Its service list names the chart's three images: the central gateway, the Restate worker and the user deployment.

### §8.1 A commit is built once, and a release tag renames that build

- **Decision.** The build job does not run for a tag. A separate job, run only for a tag, reads the commit build's manifest and writes the same bytes under the release version. It also writes the major and minor version, but only for a plain three-part version.
- **Why.** A tag push used to run the build a second time, so one commit had two different images. The build is not reproducible: dependencies resolve fresh each run and layer metadata carries the build time. A second build of one commit can never be the same image.
- **Status.** `built`. The census cites a release whose image differed from its commit's image. That comparison is the census's; no run is recorded by this lane.

### §8.2 The rename copies the manifest; it does not rebuild an index

- **Decision.** The rename is not done with the buildx tool that assembles a new index. The manifest bytes are copied verbatim. The request accepts all four manifest media types.
- **Why.** These images carry attestation manifests, so a newly assembled index is not guaranteed to keep the digest, which is the one thing the rename exists to keep. A registry asked for fewer media types may answer with a converted manifest whose digest differs.
- **Status.** `built`.

### §8.3 The latest tag follows the main branch only

- **Decision.** Only the branch build pushes the latest tag. A tag push never moves it.
- **Why.** The metadata action treats a tag as being on the default branch. A mistaken tag once published the latest tag from the wrong tree.
- **Status.** `built`.

### §8.4 A tag on a commit that was never built fails

- **Decision.** The rename waits for the commit build's tag for a bounded time and then fails. It never falls back to building.
- **Why.** The wait covers the normal gap between merge and tag. The source tag is spelled to match the metadata action's short form, so a change in that action fails the job instead of rebuilding silently.
- **Status.** `built`.

### §8.5 Regressions for §8.1 to §8.4

This bullet lists the tests that pin the four decisions above. It states no decision of its own. The tests are in the Sources table.

### §8.6 CI records each main-branch build's image digests as one document

- **Decision.** For every main-branch build, CI writes one image record per commit. It is the same document, parsed by the same module, that the qualification preflight reads. The writing command takes no registry context, so it cannot open storage.
- **Why.** A deployment pins an image by digest, and preflight compares a running location with the digests recorded for its pinned commit. Nothing recorded them, so every location read as unverified. Only CI knows the digest, because the push mints it.
- **Where else.** The preflight reader is census §4g.
- **Status.** `built`.

### §8.7 A record is complete or it is not written

- **Decision.** Nothing is published unless every service in the workflow's declared list produced a usable reference. A committed script collects the references and refuses, naming every problem at once. It tells a service that never ran from one that ran and pushed nothing. The record job runs even when a build failed, so a short list is named rather than skipped.
- **Why.** A record has no field for a missing service, and a record is never rewritten, so a partial one could never be corrected. "No record" and "a short record" must be one state.
- **Status.** `built`.

### §8.8 The record of truth is a commit on a records branch

- **Decision.** Each record is committed to a dedicated branch, at a fixed path the workflow spells itself. A test derives the path from the layout functions the reader uses and asserts that the workflow's path equals it, so a disagreement is caught when the tests run, not prevented by construction. An existing record is never rewritten: identical bytes are a no-op and different bytes are an error. A pull request records nothing. The workflow artifact is a convenience that expires.
- **Why.** A pin outlives an artifact. A commit's images do not change, so two different records for one commit mean something rebuilt them.
- **Where else.** The census notes that reading the record over the raw URL without a credential holds because this repository is public, and would not hold for a private one.
- **Status.** `built`.

### §8.9 The release reads and checks the record; it writes no record of its own

- **Decision.** The rename job resolves every release name against the registry and writes the resolved reference inside its scripted step, from what the registry answered. The release job reads the commit's record from the records branch, refuses unless each resolved digest equals the recorded one, and attaches the record's exact bytes to the existing release.
- **Why.** A record carries the time it was written, so two renders of one commit never match. A release that rendered its own would publish a second document for one commit. Another release-creating action would make a second release rather than wait for the first.
- **Status.** `built`.

### §8.10 No credential is added to the workflow

- **Decision.** The workflow names only the built-in workflow token. Its default permission on repository contents is read. Only the two record jobs may write.
- **Status.** `built`.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Test files are under `dag_tools_tests/`. The workflow is named `workflow` in short: `.github/workflows/build-containers.yml`. `signing` is `test_gateway_signing.py`, `retag` is `test_image_release_retag.py` and `ci` is `test_image_record_ci.py`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §4o-2.1 extra installed | `dag_tools/central_gateway/Dockerfile:32`; `pyproject.toml:224` | AGENTS.md §4o-2, bullet 1 | signing: `test_a_signed_token_verifies_in_this_environment`, `test_control_an_image_without_signature_support_fails_the_check`, `test_the_stub_reproduces_the_real_broken_state_rather_than_a_crash`, `test_the_extra_is_what_carries_signature_support` | `built` |
| §4o-2.2 check on the built image before the push | `scripts/check_gateway_signing.py` (algorithm set at `:52`); workflow `:116-127` (load build), `:136-141` (run with no network) | bullet 2 | signing: `test_the_check_exercises_both_paths_the_gateway_can_take`, `test_the_check_reports_every_problem_at_once`, `test_the_workflow_runs_the_check_against_the_built_gateway_image`, `test_the_check_runs_before_the_push`, `test_no_credential_is_added_to_this_workflow` | `built` |
| §4o-2.3 log line, once per state | `dag_tools/central_gateway/identity.py:356`, called at `:419`; algorithms at `:264`; latch modelled on `:335` | bullet 3 | signing: `test_a_configured_algorithm_the_library_cannot_verify_is_named`, `test_an_image_that_can_verify_says_nothing`, `test_one_configured_algorithm_of_several_is_a_warning_not_an_error`, `test_it_is_said_once_per_state_not_once_per_token`, `test_control_an_image_fixed_and_then_regressed_says_so_again`, `test_the_line_carries_no_token_because_it_is_not_about_one`, `test_an_instrument_may_never_break_the_thing_it_measures` | `built` |
| §4o-2.3 reason unchanged | `identity.py:95` | bullet 3 | signing: `test_the_refusal_reason_is_deliberately_unchanged` | `built` |
| §4o-2.3 moving the reason | `identity.py:95` | bullet 3 | signing: `test_the_refusal_reason_is_deliberately_unchanged` | `proposed` |
| §8.1 build once, rename on a tag | workflow `:56-60` (build skips tags), `:147-158` (no semver pattern), `:390-399` (rename on tags), `:445` (read), `:451-457` (version names), `:470` (digest checked) | AGENTS.md §8, bullet 1 | retag: `test_a_tag_ref_does_not_build`, `test_the_build_job_emits_no_semver_tag_pattern`, `test_the_retag_job_builds_nothing`, `test_the_retag_verifies_the_release_resolves_to_the_source_digest`, `test_every_built_service_is_retagged` | `built` |
| §8.2 manifest copied, four media types | workflow `:386`, `:417`, `:445`, `:463` | bullet 2 | retag: `test_the_release_name_is_not_minted_by_imagetools_create` | `built` |
| §8.3 latest from the branch only | workflow `:60`, `:157` | bullet 3 | retag: `test_the_release_does_not_move_latest` | `built` |
| §8.4 bounded wait, then fail | workflow `:410`, `:429-439` | bullet 4 | retag: `test_a_missing_commit_build_fails_instead_of_building` | `built` |
| §8.6 one document, one writer with no storage | workflow `:236-245`, `:303`; `dag_tools/qual/cli.py:235`; `dag_tools/qual/image_record.py:76`, `:171`, `:216` | bullet 6 | ci: `test_the_writer_needs_no_registry_and_opens_no_storage`, `test_the_writer_writes_nothing_when_it_refuses`, `test_what_the_writer_writes_is_what_the_reader_parses`, `test_the_sha_is_carried_exactly_as_given` | `built` |
| §8.7 completeness is a refusal | workflow `:20`, `:192-212`, `:236`, `:292`; `scripts/image_refs.py:85`, `:138` | bullet 7 | ci: `test_completeness_is_measured_against_the_declared_fleet`, `test_control_the_same_fragments_under_their_own_fleet_collect_clean`, `test_an_absent_fragment_and_an_empty_one_are_told_apart`, `test_a_fragment_naming_another_service_is_refused_not_recorded`, `test_every_problem_is_named_at_once`, `test_a_fleet_of_zero_services_is_refused`, `test_a_short_fleet_prints_no_ref_at_all`, `test_the_declared_fleet_is_both_matrices`, `test_the_fragment_is_written_whether_or_not_a_digest_was_minted`, `test_the_record_job_runs_even_when_one_build_failed` | `built` |
| §8.8 records branch; never rewritten | workflow `:32`, `:251`, `:324`, `:342`, `:374-375`, `:553`; `dag_tools/qual/registry/layout.py:66`, `:91` | bullet 8 | ci: `test_the_record_of_truth_is_a_branch_and_the_artifact_is_only_a_convenience`, `test_the_record_path_is_derivable_from_the_pin`, `test_an_existing_record_is_never_rewritten`, `test_a_pull_request_records_nothing` | `built` |
| §8.9 release verifies and attaches | workflow `:466-479`, `:490-492`, `:514-529`, `:575`, `:601`; `scripts/image_refs.py:165` | bullet 9 | ci: `test_a_moved_digest_fails_the_release_naming_both`, `test_an_absent_record_is_refused_and_says_who_writes_it`, `test_verifying_rewrites_nothing`, `test_the_release_job_builds_no_document_of_its_own`, `test_the_release_record_is_attached_not_created`, `test_the_release_fragment_is_written_inside_the_retag_script`, `test_both_record_jobs_run_the_committed_script` | `built` |
| §8.10 no added credential | workflow `:34-35`, `:236-245`, `:514-529` | bullet 10 | ci: `test_no_credential_is_added_to_this_workflow`, `test_only_the_record_jobs_may_write_contents` | `built` |

### Rows that carry no status

These rows count or point at records, or name a decision that is superseded or decides nothing here. None is a claim about the mesh, so none carries a status.

| Claim | Source | Decision | Evidence |
|---|---|---|---|
| §8.5 regression list | `dag_tools_tests/test_image_release_retag.py` | bullet 5 | the eight tests above |
