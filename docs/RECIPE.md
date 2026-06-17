# Recipe: Dagster Upgrade Regression & Qualification System

**Repo:** `edgy-solutions/dag-tools`

**Audience:** An autonomous coding agent (or engineer) building and operating
this system for a managed fleet of Dagster user-deployment repos.

**Operating context (assumptions this recipe relies on):**

- Operator manages and has code access to all user deployment repos in the fleet.
- Repos build via **Jenkins**; `dag-tools` is installed as a build dependency.
- A full **k8s test deployment** of the platform exists (webserver, daemon,
  run launcher, user code deployments) and can be upgraded independently of prod.
- **MinIO (S3-compatible)** is available as the inventory registry and results
  store.
- Qualification of a new Dagster version is **manually driven** from an
  operator desktop; the continuous part is only the per-repo survey in CI.

**Goal:** (1) Every Jenkins build publishes a structural inventory of its
repo's Dagster assets to the registry. (2) When qualifying a new Dagster
version, an operator runs a desktop tool that builds a fleet-wide
equivalence-class matrix from the registry, launches representative
materializations **through the k8s test deployment** (exercising webserver →
daemon → run launcher → k8s jobs → IO managers), runs local orchestration
snapshot tests, and produces a baseline-vs-candidate verdict with resumable
state.

**Non-goals:** Business-logic validation of pipelines; load/perf testing;
supporting arbitrary third-party repos without fleet conventions.

---

## System overview

```
┌─────────────────┐   publish inventory    ┌──────────────────────┐
│ Jenkins build    │ ─────────────────────▶ │ MinIO registry        │
│ (per repo, per   │   survey + load-check  │ s3://dag-tools/...    │
│  commit)         │                        └─────────┬────────────┘
└─────────────────┘                                   │ pull latest per repo
                                                      ▼
                                         ┌──────────────────────────┐
                                         │ Desktop qualification CLI │
                                         │  - class matrix           │
                                         │  - manifest + run state   │
                                         └──────┬──────────┬────────┘
                                 GraphQL launch │          │ in-process
                                                ▼          ▼
                                  ┌──────────────────┐  ┌─────────────────────┐
                                  │ k8s test          │  │ Local version envs   │
                                  │ deployment        │  │ (baseline/candidate) │
                                  │ baseline→candidate│  │ orchestration snaps  │
                                  └──────────────────┘  └─────────────────────┘
```

**Hard guardrails (apply to every phase):**

- All materializations launch against the **k8s test deployment** with
  resources pointed at staging targets. Never against production.
- `dagster instance migrate` runs only on the test deployment's instance DB.
- Every command is idempotent and emits machine-readable JSON.
- All registry writes are immutable (new keys per build/qualification; never
  overwrite). The only exceptions are `latest.json` and equivalent
  qualification pointers, which are written **last**.

---

## MinIO registry contract

Bucket: `dag-tools` (operator-configurable). Layout:

```
s3://dag-tools/
  inventory/
    <repo>/
      <git_sha>/
        meta.json
        assets.json
        automation.json      (sensors, schedules, asset checks)
        io_managers.json
        dbt_projects.json
        load_validation.json
        canary.json          (optional next-version load check result)
      latest.json            (pointer; written LAST)
  qualifications/
    <qual_id>/
      manifest.yaml
      classes/equivalence_classes.json
      baseline/
        runs/<class_hash>/<run_id>.json
        orchestration/*.json
        summary.json
      candidate/
        runs/...
        orchestration/*.json
        summary.json
      UPGRADE_VERDICT.md
      verdict.json
```

Client implementation lives in `dag_tools.qual.registry`:

- `S3Storage` — boto3 wrapper with explicit *immutable* and *mutable* put
  modes. The immutable mode HEADs first and refuses to overwrite
  (`ImmutableKeyExists`).
- `InventoryRegistry` — composes `S3Storage` with the layout helpers in
  `registry/layout.py`. Knows nothing about S3 keys directly; the layout
  module owns the strings.

All `dagtools` commands accept `--registry s3://dag-tools` and
`--endpoint-url https://minio...` or read `DAGTOOLS_REGISTRY` /
`DAGTOOLS_S3_ENDPOINT`. AWS credentials come from the standard boto3 chain.

`latest.json` is the **only mutable key per repo** and is written **last**
(after all artifacts), so readers never observe a partial inventory. This is
enforced by `InventoryRegistry.publish_build` and regression-tested by
`dag_tools_tests/test_registry_client.py::test_pointer_not_updated_when_artifact_write_fails`.

---

## Part 1 — Continuous: survey stage in Jenkins (per repo)

**Objective:** Every build of every fleet repo publishes a fresh structural
inventory and proves its definitions load under the repo's pinned Dagster
version.

### 1.1 Jenkins integration

The drop-in shared-library snippet lives at
[`templates/Jenkinsfile.survey`](../templates/Jenkinsfile.survey):

```groovy
stage('dagster-survey') {
  steps {
    sh '''
      python -m pip install --upgrade pip
      python -m pip install "dag_tools[qual]"
      dagtools \
        --registry "${DAGTOOLS_REGISTRY}" \
        --endpoint-url "${DAGTOOLS_S3_ENDPOINT}" \
        survey \
          --introspect \
          --locations workspace.yaml \
          --repo "${REPO_NAME}" \
          --sha "${GIT_COMMIT}" \
          --build "${BUILD_ID}"
    '''
  }
}
```

The optional `dagster-canary` stage (continuous early-warning for the next
Dagster version) lands alongside the `dagtools canary` subcommand in a
follow-up; the template will be updated then.

### 1.2 What `dagtools survey --introspect` does

Runs inside the repo's build environment (deps already installed), per code
location:

1. **Load validation (gating).** Loads every code location with `-W all`
   (via `simplefilter("always")` in `loader._capture_warnings`), capturing
   all deprecation / experimental warnings. **A load failure fails the Jenkins
   stage and nothing is published — the registry never contains an inventory
   for code that doesn't load.** Enforced by `publisher.run_survey` and
   regression-tested by
   `dag_tools_tests/test_survey_publisher.py::test_run_survey_refuses_to_publish_when_load_fails`.

2. **Asset introspection.** Via the shared
   [`dag_tools.inventory.extract_records`](../dag_tools/inventory/extractors.py)
   — also used by the runtime Domain Broker. Each record carries:
   `asset_key, location, group, compute_kind, io_manager_key,
   io_manager_class (FQN), io_manager_family (classifier registry),
   partitions_def_class, partition_mapping_classes[], resource_keys,
   resource_classes (FQN per key), integration_libs (derived from module
   paths: dagster_dbt, dagster_aws, custom packages), automation_condition,
   freshness/backfill policy, code_version, tags, has_asset_checks,
   job_names, urn` (DataHub sidecar).

3. **Automation inventory.** Every sensor (type, target job, asset selection,
   interval), schedule (cron, tz), asset check (target asset key); every dbt
   project (resource_key, project_dir, manifest path when present,
   translator_class FQN). **Custom translator subclasses are flagged** via
   `is_custom_translator` — high-risk because custom code is exactly what
   stock-shaped tests miss.

4. **Publish** all JSON artifacts under `inventory/<repo>/<git_sha>/` via
   `InventoryRegistry.publish_build`, then write `latest.json`.

Implementation notes:

- Prefer public Dagster accessors; internal attribute names drift between
  versions. Version-gate any `dagster._core` access and fail soft per-field
  (record `None` and log WARNING — never abort).
- IO manager classes resolve from `defs.get_assets_def(key)` and its
  `get_io_manager_key_for_asset_key()` (Dagster 1.13+ surface). Record fully
  qualified class names — equivalence classing depends on them.

### 1.3 Optional canary stage (planned)

`dagtools canary --candidate <version>`: in a throwaway venv inside the
build, install the repo with `dagster==<candidate>` (and matching `dagster-*`
pins), run load validation only, publish `canary.json` with
`{loads, errors[], new_warnings_vs_pinned[]}`. Non-blocking (`UNSTABLE`, not
`FAILURE`). Gives continuous early signal on the next upgrade between formal
qualifications, fleet-wide, for free.

**Pass criteria for Part 1:** Every fleet repo's Jenkins pipeline includes
the survey stage; `inventory/<repo>/latest.json` exists and is < N hours old
for all repos. Checked by `dagtools registry status`.

---

## Part 2 — Qualification: manual, desktop-driven

Run by an operator when qualifying `TARGET_DAGSTER_VERSION`. All state lives
in a **qualification manifest** so runs are reproducible and resumable.

> Status: Phase 2 implementation begins after Phase 1 sign-off. The shapes
> below match the recipe; small adjustments may surface during build.

### Phase Q0 — Create the qualification manifest

```
dagtools qual init --id 2026-06-15-dagster-1.12 \
  --baseline-version 1.10.x --candidate-version 1.12.x \
  --registry s3://dag-tools --endpoint-url https://minio...
```

This:

1. Pulls `latest.json` for every repo under `inventory/`; **pins** the exact
   `(repo, git_sha)` set into the manifest. The qualification is now immune
   to builds that land mid-qualification.
2. Records the version pair, the test deployment's GraphQL URL(s), staging
   resource override config reference, and the integration-library pin sets
   for both versions.
3. Diffs resolved transitive pins (notably `dbt-core`, dbt adapters, warehouse
   clients) between baseline and candidate and records `co_upgrade_risks[]`
   — a dbt-core bump hidden inside a Dagster bump must be called out, not
   discovered later as a false Dagster regression.
4. Writes `manifest.yaml` locally and to
   `s3://dag-tools/qualifications/<qual_id>/manifest.yaml`.

`manifest.yaml` shape:

```yaml
qual_id: 2026-06-15-dagster-1.12
baseline: {dagster: 1.10.6, pins: {dagster-dbt: ..., dagster-k8s: ...}}
candidate: {dagster: 1.12.1, pins: {...}}
co_upgrade_risks: [{lib: dbt-core, from: 1.8.x, to: 1.9.x}]
inventory_pins: [{repo: domain-a, sha: abc123}, ...]
deployment:
  graphql_url: https://dagster-test.internal/graphql
  auth: env:DAGSTER_TEST_TOKEN
staging_overrides: s3://dag-tools/config/staging_resources.yaml
selection:
  prefer_tag: "regression"
  reps_per_class: 2
```

### Phase Q1 — Build the fleet equivalence-class matrix

```
dagtools qual classes --id <qual_id>
```

1. Load all pinned inventories; merge into one fleet asset table.
2. Class key:

   ```
   (compute_kind, io_manager_class, partitions_def_class,
    frozenset(partition_mapping_classes), frozenset(resource_classes),
    frozenset(integration_libs), has_asset_checks, automation_condition_type)
   ```

3. Custom IO manager classes and custom dbt translator classes always form
   their own classes — custom code is exactly what stock-shaped tests miss.
4. Representative selection per class: prefer assets tagged
   `regression: "true"`, else smallest known runtime/output, else
   deterministic first-by-name; select `reps_per_class` where possible,
   ideally spanning ≥2 repos per class.
5. Runnability per representative:
   - `RUNNABLE` — resources redirect cleanly to staging targets via the
     override layer.
   - `SYNTHETIC_REQUIRED` — touches prod-only/expensive systems.
   - `OBSERVE_ONLY` — external/observable assets.
6. Emit `classes/equivalence_classes.json` + human-readable
   `equivalence_classes.md` (expect hundreds of assets → ~15–40 classes),
   publish to the registry, and print the coverage table for operator review
   **before** anything runs.

### Phase Q2 — Baseline pass (test deployment on current version)

Precondition: test deployment runs `baseline` versions and the pinned repo
SHAs are deployed to it (`dagtools qual preflight` checks deployed
code-location versions via GraphQL against `inventory_pins` and fails on
mismatch).

```
dagtools qual run --id <qual_id> --side baseline
```

For each `RUNNABLE` representative (one concrete partition key per partitioned
rep, chosen deterministically and recorded in run state):

1. Launch via GraphQL against the test deployment — asset-job launch /
   `launchPipelineExecution` with the staging resource overrides as run
   config and a tag `dagtools/qual: <qual_id>`. **Launching through the
   deployment is the point:** it exercises webserver, daemon, run launcher,
   k8s job spin-up, executor, and user code deployment images — all of which
   are upgrade surface (dagster-k8s changes bite regularly).
2. Poll run status; on completion pull the event log via GraphQL.
3. Persist per run: `{class_hash, asset_key, run_id, success, duration,
   materialization_events[], asset_check_results[], metadata_keys[],
   error (if any)}` → `baseline/runs/<class_hash>/<run_id>.json`.
4. **IO round-trip probe:** for each class, also launch the class's probe
   asset (a tiny downstream defined in a dag-tools probe code location, or a
   designated real downstream) that loads the representative's output via
   the same IO manager and asserts non-null/shape. This validates
   `load_input`, not just `handle_output`.

dbt representatives: ensure manifests were generated with the side's dbt
version; build against the staging schema with seed/`--empty`-style minimal
data.

**Local orchestration snapshots, baseline side**:

```
dagtools qual orchestration --id <qual_id> --side baseline
```

In-process under baseline pins; evaluates every sensor/schedule with
`build_sensor_context()` / `build_schedule_context()` (frozen eval time,
seeded cursors); snapshots run requests, run config, tags, cursor advancement,
skip reasons. For every distinct `(upstream_partitions_def, mapping_class,
downstream_partitions_def)` triple in the fleet inventory: resolves a fixed
sample of partition keys both directions and snapshots the resolved sets.
Construct/serialize each automation-condition type in use; where the version
pair's public testing APIs allow, evaluate minimal scenarios (parent updated,
missing, cron tick) and snapshot results.

### Phase Q3 — Upgrade the test deployment

Operator-driven, but dag-tools assists:

1. Bump images/Helm chart to candidate versions (including user code
   deployment images rebuilt against candidate pins — Jenkins can produce
   these from the pinned SHAs with a candidate-constraints build parameter).
2. Run `dagster instance migrate` on the test deployment's instance DB.
   **This is the real migration test** — record schema versions
   before/after.
3. `dagtools qual preflight --side candidate`: verify via GraphQL that the
   webserver is up, reports candidate version, all code locations load
   (fleet-wide load validation under candidate, on real infrastructure),
   and historical runs/materializations from the baseline pass still render
   (event-log back-compat spot check).

A candidate-side code-location load failure here is a **hard gate**; fix or
document before proceeding.

### Phase Q4 — Candidate pass

```
dagtools qual run --id <qual_id> --side candidate
dagtools qual orchestration --id <qual_id> --side candidate
```

Identical manifest, identical representatives, identical partition keys
(read from baseline run state — never re-randomized).

**Resumability (required behavior):** `qual run` maintains
`~/.dagtools/quals/<qual_id>/state.json` mirroring to the registry: per-
representative status `pending | launched(run_id) | passed | failed |
skipped`. Re-invocation processes only non-passed entries. `--only-class
<hash>`, `--only-failed`, and `--retry <asset_key>` are supported so the
operator can iterate on individual failures without re-running the matrix.
If the desktop dies mid-run, re-invoking reconciles `launched` entries by
querying run status via GraphQL rather than relaunching.

### Phase Q5 — Synthetic coverage for `SYNTHETIC_REQUIRED` classes

```
dagtools qual synthetic --id <qual_id> --side <side>
```

For each such class, generate a module under a dedicated **probe code
location** deployed to the test deployment (`dag-tools-probes` user
deployment):

- Import the **real** IO manager and resource classes (FQNs from the
  inventory — this is why the survey records them), instantiated with staging
  config.
- Trivial upstream asset producing a small deterministic payload matching the
  class's broad shape (DataFrame/dict/file — infer from IO manager type;
  record the assumption), plus a downstream that loads and asserts
  equality/shape.
- Mirror the class's partitions def and partition mappings.
- Launch through the deployment like any other representative.

Mark in reports that synthetic coverage is weaker: it validates Dagster
plumbing through real custom classes, not prod-only credentials/paths/data
scale.

### Phase Q6 — Diff and verdict

```
dagtools qual report --id <qual_id>
```

Comparisons:

- Per representative: success parity, materialization event count & asset
  keys, metadata key set (values may differ), asset-check status parity, IO
  round-trip probe parity. Duration deltas reported but non-gating.
- Orchestration: snapshot diffs for sensors/schedules, partition-mapping
  resolution, automation conditions — empty, or each entry annotated with an
  upstream changelog citation and operator sign-off.
- Migration/preflight results from Q3.

**Verdict logic — GO when:**

1. Candidate preflight: all code locations load on the test deployment
   (hard gate).
2. All `RUNNABLE` classes green; all `SYNTHETIC_REQUIRED` classes green via
   probes.
3. All orchestration diffs empty or signed off with citations.
4. All `co_upgrade_risks` separately validated or pinned back.

Emit `UPGRADE_VERDICT.md` + `verdict.json` to the registry: versions,
coverage (% of fleet assets covered by a green class), failures, accepted
risks, untested gaps (synthetic-only classes, prod-only behaviors). The
manifest + verdict are the permanent qualification evidence for fleet
rollout approval.

---

## Repository layout

Currently built (Phase 1):

```
dag_tools/
  inventory/                # shared structural-inventory contract
    schema.py               # versioned AssetRecord
    classifier.py           # FQN -> family + MRO walking + substring fallback
    extractors.py           # version-tolerant Definitions walker
  qual/
    registry/               # MinIO/S3 client + layout contract
      layout.py             # every S3 key constructed here
      client.py             # S3Storage + InventoryRegistry
      status.py             # compute_staleness + StatusReport
    survey/                 # per-build inventory publisher
      schemas.py            # pydantic types per artifact
      loader.py             # workspace.yaml / module-spec loading
      introspector.py       # automation + dbt + io_managers
      publisher.py          # run_survey orchestrator
    cli.py                  # the `dagtools` Typer app
templates/
  Jenkinsfile.survey
docs/
  RECIPE.md
```

Phase 2 in progress:

```
dag_tools/qual/
  qualify/                  # Q0 manifest, plus Q3 preflight + Q4 state (to come)
    manifest.py             # pydantic schemas: QualificationManifest etc.
    risks.py                # co_upgrade_risks diff
    init.py                 # create_qualification orchestrator
```

To be built (rest of Phase 2):

```
dag_tools/qual/
  classes/      # fleet merge + equivalence-class builder + rep selection (Q1)
  graphql/      # version-tolerant Dagster GraphQL layer (Q2/Q3/Q4)
  runs/         # resumable run state machine (Q4)
  synthetic/    # synthetic probe generator (Q5)
  verdict/      # diff + verdict (Q6)
probes/         # the dag-tools-probes code location (Q5 target)
```

---

## CLI surface

Implemented today:

```
dagtools registry status [--max-age-hours N] [--format json|table]
                         [--exit-nonzero-on-stale]
dagtools survey --locations <workspace.yaml | *.py | pkg.mod[:attr]>
                --repo <name> --sha <git_sha> [--build <id>]
                [--allow-overwrite] [--skip-publish]
                [--format json|table]
dagtools qual init --id <qual_id>
                --baseline <version> [--baseline-pins <yaml>]
                --candidate <version> [--candidate-pins <yaml>]
                [--graphql-url URL] [--graphql-auth-env VAR_NAME]
                [--staging-overrides s3://...]
                [--prefer-tag TAG] [--reps-per-class N]
                [--local-path PATH] [--allow-overwrite]
                [--format json|table]
```

Planned:

```
dagtools canary --candidate <version> --publish
dagtools qual classes   --id <qual_id>
dagtools qual preflight --id <qual_id> --side baseline|candidate
dagtools qual run       --id <qual_id> --side baseline|candidate
                          [--only-class <hash>] [--only-failed] [--retry <asset_key>]
dagtools qual orchestration --id <qual_id> --side baseline|candidate
dagtools qual synthetic --id <qual_id> --side baseline|candidate
dagtools qual report    --id <qual_id>
```

---

## Architectural decisions

These were made during Phase 1 and inform Phase 2 work.

### ADR-1: Share code, not service — separate registry from gateway/broker

The runtime data-mesh Central Gateway / Domain Broker and the qualification
registry both touch "Dagster asset metadata," but their purposes, lifetimes,
and storage shapes are fundamentally different:

| | Gateway/Broker (Redis K/V) | Qualification registry (S3) |
|---|---|---|
| Purpose | Where is asset X *right now*? | What did repo R look like at SHA S? |
| Timescale | 5-minute TTL | Permanent build history |
| Storage | Redis routing index | Immutable S3 archive + 1 mutable pointer |
| Read pattern | Sub-ms per asset | Operator-desktop, batched |
| Failure mode | User-facing AuthZ outage | Qualification blocked |

We **share the introspection code** (`dag_tools.inventory.extractors`) but
not the service. Storage, AuthZ, and failure surfaces stay decoupled. This
is the right shape because the **only** real overlap is "how you walk a
Dagster `Definitions` object" — that's a library boundary, not a service
boundary.

### ADR-2: Schema discipline is the decoupling lever, not package boundary

Both the long-lived Broker and the per-commit CI survey will read records
written by the other across version skew. The instinctive fix is to extract
`inventory/` into a separate `dag-inventory` package so the runtime mesh
isn't dependent on the OSS tool's release cadence — but the Broker
**already** imports from `dag_tools` (see `domain_broker/main.py`'s
`asset_keys_to_dataset_urn_converter` import). Splitting only renames the
coupling.

The actual decoupling mechanism is the **`schema_version` field on
`AssetRecord` with strict additive-only evolution and tolerant readers**
(`extra="ignore"`). That mechanism works identically whether the code lives
in one package or two. We get the same decoupling benefit without the
overhead of a separate pyproject, wheel, and version bump in lockstep.

Practical rules (also in `AGENTS.md` §4a):

- Only add `Optional[...]` fields with defaults. Never rename or remove.
- Bump `SCHEMA_VERSION` by 1 in the same commit that adds a field.
- Readers use `extra="ignore"` and tolerate unknown fields silently.

### ADR-3: FQN-first classification with MRO walking + logged substring fallback

The Broker originally substring-matched lowercased class names
(`"postgres" in class_name.lower()` etc.). That silently misclassified custom
forks (a `PostgresVectorIOManager` that drops "postgres" from its name; a
generic `VectorStore` that has "store" in it).

The classifier in `dag_tools.inventory.classifier` replaces it with three-tier
resolution:

1. Explicit FQN registry — exact-match wins.
2. MRO walking — custom subclasses of registered ancestors classify
   correctly without extra config.
3. Substring fallback — last resort, logged at WARNING so unknown classes
   surface and get added to the registry.

Both the survey and the Broker call `classify(target)`; survey persists the
FQN *and* the family; Broker reads just the family for routing. The
Broker's `extract_io_manager_info` is now FQN-based as a side effect of
the migration in commit `28a0212`.

### ADR-4: DataHub URN sidecar field on `AssetRecord`

Adds `urn: Optional[str]` populated via the existing
`asset_keys_to_dataset_urn_converter` (in
`dag_tools/components/datahub_lineage/component.py`). Lets a later
qualification preflight cross-check: "registry says repo R at SHA S has
asset X" vs. "live Brokers are heartbeating URN(X)" — catching deployed-code
≠ surveyed-code drift before wasting a baseline pass. Read-only on
heartbeat data; no new coupling.

### ADR-5: `dagtools` binary, `dag_tools` package; everything in this repo

The recipe originally proposed `dagtools/` as a sibling Python package
distinct from the existing `dag_tools/`. We collapsed that: everything lives
under `dag_tools/qual/` and a single console script `dagtools = "dag_tools.qual.cli:app"`
is registered via `[project.scripts]` in `pyproject.toml`. Two reasons:

1. The Broker is already in `dag_tools/domain_broker/`; the shared
   inventory module is in `dag_tools/inventory/`. A separate `dagtools/`
   sibling would have created cross-package import paths for what is one
   logical project.
2. The qualification CLI ships with the same wheel as the runtime mesh
   code, on the same release cadence, with the same review surface. That
   matches the actual operational shape.

### ADR-6: Typer for the CLI; machine-readable JSON by default

Per the recipe rule: *"Every command is idempotent and emits
machine-readable JSON."* Every `dagtools` sub-command takes
`--format` defaulting to `json` with `table` available for humans. The
contract is enforced by tests; new sub-commands must follow.

Typer over Click/argparse because type hints become the schema, sub-command
trees compose cleanly, and rich text output is built-in for `--help`.

### ADR-7: moto-backed S3 tests; no real cloud dependency

Registry tests use `moto[s3]` with `mock_aws()` — no real S3 / MinIO is
required in CI. This gives portable, fast tests of the actual put/get/list
semantics (including immutability refusal and the write-last invariant)
without provisioning credentials. The S3Storage class accepts a
dependency-injected boto3 client for the same reason.

### ADR-8: Discovery — Dagster 1.13 `AssetChecksDefinition.specs` vs `.check_specs`

Surfaced during step 3 development:
`AssetChecksDefinition.specs` is the **target asset specs** (often empty
for stock checks); the actual check specs live under `.check_specs`. The
introspector tries `check_specs` first then falls back to `specs` for
older API shapes. Worth flagging because it's exactly the kind of API
drift the qualification system is built to surface — and the survey
caught it on itself.

---

## Implementation status

| Phase | Step | Status |
|---|---|---|
| 1 | 1 Shared inventory contract (`dag_tools.inventory`) + Broker migration | ✅ done — commit `28a0212` |
| 1 | 2 Registry + `dagtools registry status` | ✅ done — commit `f00f1be` |
| 1 | 3 Survey CLI + `Jenkinsfile.survey` | ✅ done — commit `77b2833` |
| 1 | 4 Documentation Recipe + ADRs | ✅ done — this file |
| 1 | (opt) Canary stage | Not yet started |
| 2 | Q0 Manifest + `dagtools qual init` | ✅ done — pins inventories, version pair, co_upgrade_risks |
| 2 | Q1 Equivalence-class matrix + `dagtools qual classes` | Not yet started |
| 2 | Q2/Q3/Q4 GraphQL launcher + baseline/candidate runs + preflight | Not yet started |
| 2 | Q5 Synthetic probes | Not yet started |
| 2 | Q6 Diff + verdict | Not yet started |

### Key regression tests enforcing recipe invariants

These tests double as guardrails: if you find yourself needing to change
them, you're probably violating a recipe rule. Read carefully first.

| Recipe invariant | Test |
|---|---|
| `latest.json` is written LAST | `test_registry_client.py::test_pointer_not_updated_when_artifact_write_fails` |
| Immutable per-build keys | `test_registry_client.py::test_publish_build_is_immutable_for_same_sha` |
| Load failure means publish NOTHING | `test_survey_publisher.py::test_run_survey_refuses_to_publish_when_load_fails` |
| Partial load failure also refuses publish | `test_survey_publisher.py::test_run_survey_partial_load_failure_also_refuses_publish` |
| JSON output by default | `test_dagtools_cli.py::test_registry_status_json_default` |
| Schema tolerates forward-compat fields | `test_inventory.py::test_schema_tolerates_unknown_fields` |
| Qualification manifest is immutable per qual_id | `test_qualify_init.py::test_re_init_same_qual_id_raises` |
| Dagster-family pins filtered from co_upgrade_risks | `test_qualify_risks.py::test_dagster_family_is_filtered_out` |
| Inventory pins freeze the registry snapshot at init time | `test_qualify_init.py::test_create_qualification_pins_inventories` |

---

## Known limitations

- **GraphQL drift.** Dagster GraphQL fields and internals shift across
  versions; the survey and launcher must be defensive and version-gated
  (schema introspection, soft per-field failure). The inventory extractor
  already follows this pattern; the Phase 2 GraphQL layer must too.
- **Synthetic probes are weaker coverage.** They validate framework plumbing
  through real custom classes, not prod-only systems or data-scale behavior.
  Report this explicitly in `UPGRADE_VERDICT.md`.
- **Orchestration snapshots detect *changes*, not *incorrectness*.** Diffs
  may be desirable behavior changes — hence citation + sign-off, not
  auto-fail.
- **One test deployment is sequential.** Baseline → upgrade → candidate on
  one deployment means baseline cannot be re-established cheaply after the
  upgrade. If iteration on candidate failures is expected to be heavy,
  run two namespaces side by side instead.
- **Business logic is out of scope.** This system validates behavior
  preservation at the framework boundary; pipeline correctness vs. business
  intent is the user's job.

---

## Where to look next

| For… | Read… |
|---|---|
| Schema rules for `AssetRecord` and every survey artifact | `dag_tools/inventory/schema.py` and `dag_tools/qual/survey/schemas.py` |
| Adding a new IO manager class to the classifier | `dag_tools/inventory/classifier.py::FAMILY_REGISTRY` |
| S3 key layout / adding a new artifact | `dag_tools/qual/registry/layout.py` |
| The load-gate enforcement | `dag_tools/qual/survey/publisher.py::run_survey` |
| Agent-facing operational rules | `AGENTS.md` §4a, §4b, §4c |
| Cursor agent rules | `.cursorrules` §4a, §4b, §4c |
| High-level architecture context for LLMs | `llms.txt` points 7a, 7b |
