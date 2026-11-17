# Preflight: identify a code location by its image, in four outcomes

Status: approved by the architect, 2026-11-16. Built on `feat/qual-preflight-image-identity`.

## Why

`deployed_shas_match_pins` (AGENTS §4g) reads a repo and sha stamp from
`workspace.yaml`'s `grpc_server.additional_metadata`. The fleet installs Dagster
only through the official Helm chart, and that chart builds `workspace.yaml`
itself from a template that emits only host, port, ssl and location name.
Measured on chart 1.10.19 and 1.13.1. As shipped, then, the check can't pass on a
chart install, and candidate preflight is the one hard gate with no override (§4h).

There is a source that does reach preflight with no operator work:
- The chart sets `DAGSTER_CURRENT_IMAGE=<image.repository>:<image.tag>` on every
  user-deployment pod.
- The code server reports that value as `container_image`.
- `CodeLocation.get_display_metadata()` merges it in as `{"image": ...}`.
- `WorkspaceLocationEntry.displayMetadata` is exactly the field preflight already
  queries.

So the image is in the dict preflight receives today, and no new GraphQL is
needed.

## Ruling (architect)

1. **Identity is matched in this order:**
   1. The image **digest** against the digest the manifest records for the pinned
      commit, which CI publishes.
   2. A **`sha-<short>` tag** against the pinned sha.

   A release tag and its sha tag share one digest, because releases retag and
   never rebuild (§8). So the digest comparison covers a release tag without a
   registry lookup, provided the location is deployed by digest.
2. **Four outcomes, kept distinct:**
   - match;
   - mismatch, which fails;
   - a pin no location matches, which fails;
   - unverified: a location that reports no digest and no sha.

   Unverified is not a pass. It fails the check unless a manifest row waives that
   location with a reason.
3. Route (a) from the packet (repository metadata or an env var read by the
   location) is dropped: neither has a GraphQL surface. Defect 5.1 (a null
   metadata value read as the string `"None"`) is dropped too: `RepositoryMetadata.value`
   is `String!` and the resolvers drop nulls, so it is unreachable against a real
   server.

## Deploying by digest through the chart: measured

Measured with `helm template` against the pulled charts:
- Values used: `image.repository: ghcr.io/example/user-code`, `image.tag: "sha-abc1234@sha256:<64 hex>"`.
- Both 1.10.19 and 1.13.1 render the container `image:` field and `DAGSTER_CURRENT_IMAGE`
  identically, as `ghcr.io/example/user-code:sha-abc1234@sha256:<hex>`.
- A release tag rendered the same way: `0.12.0@sha256:<hex>`.
- The user-deployments value schema types `tag` as a free string with no pattern.
- 1.13.1 rendered with schema validation on.
- 1.10.19 needed its schema files removed, but only because its schema `$ref`s a
  remote URL that would not resolve offline. That has nothing to do with the tag.

A kubelet given `name:tag@digest` pulls by digest. So the operator contract is one
line in `values.yaml`: append `@sha256:<digest>` to the tag, or deploy by a
`sha-<short>` tag.

## Design

### A. CI records the digest: `dagtools registry record-image`

```
dagtools registry record-image --repo R --sha S --image REF [--image REF ...]
```

- `REF` must carry a digest: `repository@sha256:<64 hex>` or
  `repository:tag@sha256:<64 hex>`. Any tag is accepted and ignored. A reference
  with no digest is refused with a non-zero exit.
- Writes `inventory/<R>/<S>/image.json` through
  `layout.inventory_artifact_key(R, S, layout.IMAGE_FILE)`.
  - `IMAGE_FILE = "image.json"` is a new constant in `layout.py`.
  - No key string is built anywhere else.
- Body: `ImageRecord`, a pydantic model with `extra="ignore"`:
  - `schema_version: int = 1`
  - `repo`
  - `git_sha`
  - `images: List[RecordedImage]`, where `RecordedImage` is `repository: str` plus
    `digest: str` (lowercased `sha256:<hex>`)
  - `recorded_at: datetime`
- Put the models next to `LatestPointer` in `registry/client.py`. Add
  `InventoryRegistry.record_image(repo, git_sha, images)` and
  `InventoryRegistry.read_image_record(repo, git_sha) -> Optional[ImageRecord]`.
- **Idempotent.** If the key exists and its image set (as a set of
  `(repository, digest)`) equals the new one, return the existing record and exit 0.
  If the sets differ, raise and exit non-zero naming the key, and write nothing.
  This is a per-commit record of what CI built, and two different digests for one
  commit is exactly the condition it exists to expose.
- Never touches `latest.json`. It is independent of `publish_build`, so the order
  between the survey and the image build is CI's choice.
- Output is JSON by default (`--format json|table`), like every `dagtools` command.

Wiring this into any repo's CI (this repo's `build-containers.yml` included) is
release and image work and is **out of scope** here. It is flagged to the owner.

### B. Manifest (bump `qualify/manifest.py` `SCHEMA_VERSION` 4 → 5)

All changes are additive with defaults.

- `InventoryPin.images: Optional[List[PinnedImage]] = None`, where `PinnedImage`
  has `repository` and `digest`.
  - `None` means no record was found at init.
  - Init copies the list from `image.json`.
- `Deployment.image_metadata_key: str = "image"`: the `displayMetadata` key that
  carries the image reference. Dagster's own name.
- `Deployment.sha_tag_prefix: str = "sha-"`: what docker/metadata-action's
  `type=sha` emits.
- `Deployment.unverified_waivers: List[LocationWaiver] = []`, where
  `LocationWaiver` has `location: str` and `reason: str`.
  - Both are required.
  - Both are refused when empty or whitespace.
  - Two rows for one location are refused.
  - The waiver lives in the immutable manifest, set at `qual init`. There is
    deliberately no Q6 flag for it: candidate-preflight failure has no
    `GapAcceptance` override (§4h).

`qual init` (`init.py` `_pin_inventories`, `cli.py`):
- For each pin it reads `read_image_record(repo, git_sha)`.
  - Absent: `images=None`.
  - Valid: copy the images.
  - Malformed, or naming a different repo or sha than the pin: init fails naming
    the repo and the key, and writes no manifest.
- New options:
  - `--waive-unverified-location NAME=REASON`, repeatable. The first `=` splits.
    A missing `=` or an empty side is refused.
  - `--image-metadata-key`
  - `--sha-tag-prefix`

### C. Parsing an image reference (new pure module `dag_tools/qual/preflight/identity.py`)

`parse_image_reference(s) -> ImageRef(repository, tag: Optional, digest: Optional)`
- Split on the **last** `@`. The right side must match `sha256:[0-9a-fA-F]{64}`,
  and it is lowercased.
  - If an `@` is present but the digest is malformed, the reference is
    **unparseable**: digest and tag are both `None` and `ImageRef.error` names
    why. A reference half-read is not evidence.
- The tag is what follows the last `:` **if** that `:` comes after the last `/`.
  - So `host:5000/a/b` has no tag.
  - `host:5000/a/b:sha-abc1234` has tag `sha-abc1234`.
- `sha_from_tag(tag, prefix)`: the sha if the tag is `prefix` followed by 7–40 hex
  characters, else `None`.
- Repository comparison is case-insensitive equality. Nothing is normalized:
  `docker.io/` and `library/` are not added or removed.
  - This only affects the diagnosis (mismatch versus unmatched pin), never
    pass/fail. Digest equality alone produces a match, and both
    repository-dependent outcomes fail.

### D. Classifying a location: sources in order, first attribution wins

For each location in check 2's `code_locations` (reuse them; **no second workspace
query**):

1. **digest**, if the image carries digest D:
   - pins whose `images` contain D → **matched** to each of them;
   - otherwise, pins whose `images` contain the location's repository →
     **mismatch** against each of them.
2. **sha_tag**, if the tag yields sha X:
   - pins with `_shas_match(pin.git_sha, X)` → **matched**;
   - otherwise, pins whose `images` contain the location's repository →
     **mismatch**.
   - A step-1 mismatch is final. The digest is what the kubelet pulled, and a tag
     claiming the pin does not overrule it.
3. **stamp**: the existing `additional_metadata` route, kept because
   `workspace.yaml` deployments use it today (§6).
   - It applies only if both the repo key and the sha key are present and
     non-empty.
   - Pinned repo → matched or mismatch by `_shas_match`.
   - Unpinned repo → no attribution.

If no source attributes the location:
- **unpinned** if it reported a digest, a sha tag or a stamp sha. This is
  informational: it's another repo's location, e.g. `dag-tools-probes` when
  dag-tools is not pinned.
- Otherwise **unverified**.
  - If a waiver row names exactly this location, it is recorded as **waived**
    with the reason instead.

Per pin:
- **matched**: at least one location matched it and none mismatched it;
- **mismatch**: any location mismatched it;
- **unmatched**: nothing attributed to it.

The check passes iff every pin is matched **and** no location is unverified
(waived locations don't count).
- With **zero pins**, the check passes with the existing "no inventory pins to
  verify" wording, **whatever the locations report**. Each location's outcome is
  still computed and published in `location_identities`, unverified ones
  included; the detail adds "N location(s) unverified; not gated because nothing
  is pinned". Decided during the build:
  - The unverified rule exists so that a location cannot stand in for a pin
    without being identified. With no pins, no location stands in for anything
    and there is nothing to qualify its identity against.
  - Gating there would fail every version and rendering test that has no
    inventory, for a reason unrelated to what those tests check.
  - A qualification with zero pins is degenerate already; Q1 has no classes.
  - Sealed by `test_with_no_pins_an_unverified_location_is_reported_not_gated`
    and its control `test_control_with_one_pin_the_same_location_fails`.
- Waiver rows that name no workspace location, or a location that wasn't
  unverified, go to `unused_waivers`. This is informational.

Failure details tell the operator what to do:
- Unmatched or mismatched pin: deploy by digest (`tag: <tag>@sha256:...`) or by a
  `sha-<short>` tag, and record digests in CI with `dagtools registry record-image`.
- Unverified location: the same, or waive it with
  `qual init --waive-unverified-location NAME=REASON`.

### E. Report (bump `preflight.py` `SCHEMA_VERSION` 2 → 3, additive)

- New `LocationIdentity`:
  - `location_name`
  - `outcome: Literal["matched","mismatch","unpinned","unverified","waived"]`
  - `source: Optional[Literal["digest","sha_tag","stamp"]]`
  - `pins: List[str]` (the repos)
  - `image`, `deployed_digest`, `deployed_sha` (all optional)
  - `waiver_reason`
  - `detail`
- `PreflightReport.location_identities: List[LocationIdentity] = []` and
  `unused_waivers: List[str] = []`.
- `RepoShaCheck` gains:
  - `outcome: Optional[Literal["matched","mismatch","unmatched"]] = None`
  - `source`
  - `deployed_digest`

  `matched`, `deployed_sha` and `location_name` keep their meaning.
- `unstamped_locations` is kept. It now lists the locations whose outcome is
  unverified or waived. Its docstring must stop calling it informational.

### F. Defect 5.2 (rides along)

In `graphql/client.py` `get_code_locations`, move the `try` inside the
`displayMetadata` loop.
- A failing entry is skipped, and a WARNING names the location.
- The other entries survive.

Defect 5.1 is not changed (section "Ruling", item 3).

## Seals: every outcome with its control

Each case below sits beside its control. A case without one passes vacuously on a
check that always answers the same.

| Outcome | Test | Control |
|---|---|---|
| match by digest | `test_a_location_deployed_by_digest_matches_its_pin` | `test_control_one_changed_digest_digit_is_a_mismatch` |
| release tag by digest | `test_a_release_tag_deployed_by_digest_matches_without_a_registry_lookup` | `test_control_the_same_release_tag_without_a_digest_is_unverified` |
| match by sha tag | `test_a_sha_tag_matches_its_pin` | `test_control_a_sha_tag_of_another_commit_is_a_mismatch_when_the_repository_is_recorded`, and `..._leaves_the_pin_unmatched_when_it_is_not` |
| digest outranks tag | `test_the_digest_decides_over_a_tag_that_claims_the_pin` | `test_control_the_same_tag_with_the_pins_digest_matches` |
| mismatch fails | `test_a_mismatch_fails_the_check` | the matched case passes |
| unmatched pin fails | `test_a_pin_no_location_matches_fails` | `test_control_adding_the_location_passes` |
| unverified fails | `test_an_unverified_location_fails_the_check` | `test_control_a_waiver_row_passes_it_and_records_the_reason`, `test_a_waiver_for_another_location_does_not_pass_it`, `test_a_waiver_without_a_reason_is_refused_by_the_manifest` |
| unpinned is informational | `test_an_unpinned_location_is_informational` | `test_control_the_same_location_without_a_sha_is_unverified` |
| distinct | `test_the_four_outcomes_land_in_four_buckets` (one workspace producing all four; each in its own bucket, none in two) | — |
| §6 stamp still works | `test_the_workspace_yaml_stamp_still_matches` | `test_control_a_stamp_naming_another_sha_is_a_mismatch` |
| no second query | the existing `test_sha_check_does_not_make_a_second_workspace_call` still passes | — |

Plus:
- **Parser:** registry with a port, digest only, tag plus digest, no tag,
  malformed digest (unparseable, not half-read), uppercase hex lowercased.
- **Registry:**
  - `test_record_image_writes_under_the_layout_key_and_leaves_latest_alone`
  - `test_record_image_is_idempotent_for_the_same_digests`
  - `test_record_image_refuses_a_different_digest_for_a_recorded_commit`
  - `test_record_image_refuses_a_reference_without_a_digest`
- **Init:**
  - `test_init_copies_the_recorded_images_into_the_pin`
  - `test_init_without_a_record_leaves_images_none`
  - `test_init_fails_on_a_record_naming_another_commit`
  - `test_init_parses_waivers_and_refuses_one_without_a_reason`
- **Client:** `test_one_malformed_display_metadata_entry_drops_only_itself_and_warns`.
- **The Dagster half, end to end:**
  `test_dagster_reports_the_container_image_through_the_workspace_query`.
  - Start a real `dagster api grpc` subprocess with `DAGSTER_CURRENT_IMAGE` set.
  - Load a workspace pointing at it, run the client's own workspace query
    through `dagster_graphql`, and assert the `image` entry carries the value.
  - Run it on both 1.10.19 and 1.13.1.
  - It proves the code path the chart relies on, but not the chart itself.

Existing test that changes, because the behaviour changes:
`test_an_unstamped_location_is_informational_not_a_failure`. A location reporting
nothing now fails unless waived. Replace it with the unverified pair above, and say
so in the CHANGELOG.

## Docs

- AGENTS §4g: rewrite the `deployed_shas_match_pins` bullet. Cover the order, the
  four outcomes, the waiver living in the manifest, the regressions, and the
  measured chart fact.
- RECIPE: rewrite the sha-stamp section (~L342) and the line at ~L1143. Cover
  deploying by digest or sha tag, `record-image` in CI, and waivers.
- README: the preflight line (L38) and the new registry command.
- CHANGELOG Unreleased:
  - **Behaviour change**: a location reporting no digest and no sha now fails
    preflight unless waived.
  - New `record-image` command.
  - Manifest schema 5, report schema 3.

## What has been run, and what has not

- **Run:**
  - `helm template` of both chart versions (above).
  - Reading the Dagster code path at both versions.
  - The suites and the end-to-end gRPC test, once built.
- **Not run:** preflight against a live, chart-installed deployment, and a kubelet
  actually pulling by `tag@digest`. Nothing here has touched a cluster.
