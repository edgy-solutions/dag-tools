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
dagtools qual synthetic --id <qual_id>
                        [--skip-publish] [--skip-local]
                        [--local-path PATH] [--allow-overwrite]
                        [--format json|table]
```

For each `SYNTHETIC_REQUIRED` class, the generator emits a self-contained
Dagster module the operator drops into the `dag-tools-probes` user
deployment. v1 (shipped) does **generation + persistence**; "deploy these
through Q2 and feed coverage into Q6" is the next slice — until then,
`qual report --accept-synthetic-coverage-missing` is how operators GO past
synthetic gaps.

What the generator produces per class:

- **Import the real IO manager** by FQN (from the inventory), wrapped in a
  try/except with an `InMemoryIOManager` fallback so the
  `dag-tools-probes` code location always loads. A failed import becomes a
  runtime error at materialization time (visible in the run output), not a
  silent code-location load failure.
- **Deterministic dict payload** + an upstream/downstream asset pair. The
  downstream loads the upstream through the same IO manager and asserts
  the payload survived the round-trip intact.
- **Notes** captured when the class has features v1 doesn't yet
  synthesize: partitions defs (operator extends the probe), partition
  mappings, custom `DagsterDbtTranslator` subclasses (a dbt-aware probe is
  the operator's job).

Outputs:

- Registry: `qualifications/<qual_id>/probes/<class_hash>.py` (one per
  class) and `qualifications/<qual_id>/probes/probe_manifest.json` —
  manifest written **last**, so a reader observing it is guaranteed every
  referenced source is present.
- Local: `~/.dagtools/quals/<qual_id>/probes/` (overridable via
  `--local-path` or `DAGTOOLS_HOME`) — same file layout as the registry.
  `--skip-publish` and `--skip-local` let the operator pick one side.

**Deploy target — the `dag-tools-probes` code location.** The repo ships
a deployable Dagster code location at
`dag_tools.probes_location.definitions` that the operator points the
test deployment at (separate user-code location, separate deploy
cadence from the regular fleet). The location dynamically loads every
`<class_hash>.py` under `DAGTOOLS_PROBES_DIR`, merges their
`Definitions` (each probe uses a class-unique `io_manager_<short>`
resource key so merge never collides), and soft-fails per file so one
broken probe doesn't block the location load.

```yaml
# operator's test-deployment workspace.yaml
load_from:
  - python_module:
      module_name: dag_tools.probes_location.definitions
      location_name: dag-tools-probes
```

```bash
# operator sets the env var on the test deployment to point at the bundle
DAGTOOLS_PROBES_DIR=/path/to/~/.dagtools/quals/<qual_id>/probes/
```

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
  classes/                  # Q1 fleet equivalence-class matrix
    key.py                  # ClassKeyComponents, class_hash, EquivalenceClass, ClassMatrix
    selection.py            # pick_representatives + classify_runnability
    builder.py              # build_class_matrix + publish + render_markdown
  graphql/                  # Q2/Q3/Q4 version-tolerant Dagster GraphQL client
    client.py               # DagsterGraphQLClient (launch, poll, event log)
  runs/                     # Q2/Q4 resumable run execution
    state.py                # RepStatus + RepState + QualRunState (local + registry-mirrored)
    records.py              # RunRecord persisted to <side>/runs/<class_hash>/<run_id>.json
    launcher.py             # launch_representative + build_run_record
    runner.py               # run_side orchestrator (reconciliation, retries, summary)
  preflight/                # Q3 deployment-readiness gate via GraphQL
    preflight.py            # PreflightReport schema + run_preflight orchestrator
  verdict/                  # Q6 diff + GO/NO-GO verdict
    diff.py                 # RepDiff per-rep parity + ClassVerdict roll-up
    verdict.py              # build_verdict + render_markdown + GapAcceptance
  synthetic/                # Q5 synthetic probe code generation
    schema.py               # ProbeManifest + ProbeModule + ProbeStatus
    generator.py            # generate_probe_module + generate_probe_source
    bundle.py               # generate_bundle + publish_bundle + write_local_bundle
dag_tools/probes_location/  # the deployable `dag-tools-probes` Dagster code location
  loader.py                 # load_probes_from_dir + ProbeLoadReport (per-file soft-fail)
  definitions.py            # top-level `defs` the test deployment imports
dag_tools/qual/probes/      # Q5c probe runner (parallel to runs/)
  state.py                  # ProbeRunState + ProbeRepState + ProbeRepStatus
  runner.py                 # run_probes_side launcher + reconciler + summary
```

Q5 phase complete — every recipe item under "Synthetic coverage for
SYNTHETIC_REQUIRED classes" is implemented end-to-end (generation +
deploy target + runner + deploy-state visibility + Q6 coverage + Q6
record diff).

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
                [--location-name NAME] [--job-name NAME]
                [--staging-overrides s3://...]
                [--prefer-tag TAG] [--reps-per-class N]
                [--local-path PATH] [--allow-overwrite]
                [--format json|table]
dagtools qual classes --id <qual_id>
                [--allow-overwrite] [--format json|table]
dagtools qual run --id <qual_id> --side baseline|candidate
                [--retry-failed] [--only-class <hash>]
                [--poll-interval N] [--poll-timeout N]
                [--format json|table]
dagtools qual preflight --id <qual_id> --side baseline|candidate
                [--sample-size N] [--allow-overwrite]
                [--format json|table]
dagtools qual synthetic --id <qual_id>
                [--skip-publish] [--skip-local]
                [--local-path PATH] [--allow-overwrite]
                [--format json|table]
dagtools qual probes run --id <qual_id> --side baseline|candidate
                [--retry-failed] [--only-class <hash>]
                [--poll-interval N] [--poll-timeout N]
                [--format json|table]
dagtools qual probes status --id <qual_id>
                [--exit-nonzero-on-gap]
                [--format json|table]
dagtools qual report --id <qual_id>
                [--accept-co-upgrade-risks]
                [--accept-synthetic-coverage-missing]
                [--accept-orchestration-deferred]
                [--allow-overwrite] [--format json|table]
```

Planned:

```
dagtools canary --candidate <version> --publish
dagtools qual orchestration --id <qual_id> --side baseline|candidate
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
| 2 | Q1 Equivalence-class matrix + `dagtools qual classes` | ✅ done — class key + hash, representatives with runnability, JSON + Markdown |
| 2 | Q2 Baseline pass + `dagtools qual run --side baseline` | ✅ done — GraphQL launcher, resumable state, per-rep records, side summary |
| 2 | Q3 Preflight + `dagtools qual preflight --side <side>` | ✅ done — version + locations + (candidate-only) historical-run-rendering checks |
| 2 | Q4 Candidate pass | ✅ done by Q2 — operator runs `dagtools qual run --side candidate` after Q3 passes |
| 2 | Q5 Synthetic probe **generation** + `dagtools qual synthetic` | ✅ done — per-class self-contained module emitted with real IO manager FQN + fallback, persisted to registry + `~/.dagtools/quals/<id>/probes/` |
| 2 | Q5 Synthetic probe **deploy target** — `dag-tools-probes` code location | ✅ done — `dag_tools.probes_location.definitions` dynamically loads every `<class_hash>.py` from `DAGTOOLS_PROBES_DIR`, merges via `Definitions.merge`, soft-fails per probe |
| 2 | Q5c Synthetic probe **runner** + `dagtools qual probes run` | ✅ done — launches each probe's downstream asset against the dag-tools-probes location, resumable per-side state, immutable run records under `<side>/probes/runs/<class_hash>/<run_id>.json` |
| 2 | Q5d Probe deploy-state visibility — `dagtools qual probes status` | ✅ done — GraphQL cross-reference of probe manifest vs the dag-tools-probes location: fully-loaded vs partially-loaded vs missing classes + stale-asset detection |
| 2 | Q5e Probe RunRecord diff in Q6 | ✅ done — divergent-but-passing probes (both PASSED but materialization / metadata / asset-check parity broke) populate `synthetic_classes_red` and block GO regardless of acceptance flags; `ClassVerdict.probe_diff` carries the per-class diff for `UPGRADE_VERDICT.md` |
| 2 | Q6 synthetic-coverage integration | ✅ done — `synthetic_classes_with_probe_coverage` counts classes whose probes passed on BOTH sides AND (when records exist) diff cleanly; graceful degradation to status-only when records are missing |
| 2 | Q6 Diff + verdict + `dagtools qual report` | ✅ done — per-rep parity diff, class roll-up, GO/NO-GO with strict-by-default known-gap acceptance |
| 2 | Q2/Q4 IO round-trip probes + local orchestration snapshots | Deferred — see Known limitations |

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
| Q1 reads the manifest's inventory_pins, NOT the registry's latest | `test_classes_builder.py::test_q1_reads_pinned_sha_not_latest` |
| Custom dbt translators force their own equivalence class | `test_classes_builder.py::test_build_class_matrix_segregates_custom_dbt_translator` |
| Class hash is deterministic over its components | `test_classes_key.py::test_class_hash_is_deterministic` |
| Q2 re-invocation skips PASSED reps (resumability) | `test_runs_runner.py::test_run_side_skips_passed_reps_on_re_invocation` |
| Q2 reconciles LAUNCHED reps via poll, not relaunch | `test_runs_runner.py::test_run_side_reconciles_launched_via_poll_not_relaunch` |
| Q2 side state mirrors to registry after every transition | `test_runs_runner.py::test_run_side_state_is_mirrored_to_registry_after_each_transition` |
| Q2 non-runnable representatives are SKIPPED, not launched | `test_runs_runner.py::test_run_side_skips_synthetic_required_reps` |
| GraphQL launch uses the typed-union failure shape | `test_graphql_client.py::test_launch_asset_run_raises_on_typed_failure_shape` |
| Launch mutation never selects `message` on fieldless error types (InvalidStepError/InvalidOutputError) — else it 400s every launch | `test_graphql_client.py::test_launch_mutation_does_not_select_message_on_fieldless_error_types` |
| Q2 launcher targets the manifest's deployment.location_name / job_name (NOT hardcoded 'default') | `test_runs_runner.py::test_run_side_threads_manifest_location_and_job_into_launch` |
| Q5c probe launch selects BOTH upstream + downstream (single-asset launch fails to load the input) | `test_probes_runner.py::test_run_probes_side_launches_against_dag_tools_probes_location` |
| Q3 preflight fails on version mismatch | `test_preflight.py::test_run_preflight_fails_on_version_mismatch` |
| Q3 preflight fails when any code location doesn't load | `test_preflight.py::test_run_preflight_fails_when_a_code_location_does_not_load` |
| Q3 candidate preflight samples baseline runs for back-compat | `test_preflight.py::test_candidate_preflight_samples_baseline_runs` |
| Q3 baseline preflight skips the run-rendering check | `test_preflight.py::test_baseline_preflight_does_not_sample_baseline_runs` |
| Q6 metadata parity uses key SET only (values may differ) | `test_verdict_diff.py::test_diff_compares_metadata_KEY_set_not_values` |
| Q6 duration deltas are informational, never gating | `test_verdict_diff.py::test_diff_duration_is_informational_not_gating` |
| Q6 candidate-preflight failure is a hard gate | `test_verdict.py::test_verdict_no_go_when_candidate_preflight_failed` |
| Q6 strict-by-default: deferred gaps block GO until accepted | `test_verdict.py::test_verdict_no_go_by_default_when_orchestration_not_accepted` |
| Q6 co_upgrade_risks block GO unless --accept-co-upgrade-risks | `test_verdict.py::test_verdict_no_go_when_co_upgrade_risks_unaccepted` |
| Q5 only generates probes for SYNTHETIC_REQUIRED classes | `test_synthetic_generator.py::test_generate_probe_module_skips_non_synthetic` |
| Q5 generated source always parses as Python (code location loads) | `test_synthetic_generator.py::test_generated_source_parses_as_python` |
| Q5 source imports IO manager by FQN with InMemoryIOManager fallback | `test_synthetic_generator.py::test_generated_source_has_inmemory_fallback_for_missing_io_manager` |
| Q5 generated probes do NOT import `dag_tools.qual.*` (decoupled deploy cycles) | `test_synthetic_generator.py::test_generate_probe_source_is_self_contained` |
| Q5 manifest written LAST in publish (sources visible-then-manifest invariant) | `test_synthetic_bundle.py::test_publish_bundle_writes_sources_then_manifest` |
| Q5 probe artifacts are immutable per qual_id | `test_synthetic_bundle.py::test_publish_bundle_is_immutable_by_default` |
| Q5 each probe uses class-unique `io_manager_<short>` resource key (no merge collision) | `test_synthetic_generator.py::test_each_probe_uses_class_unique_io_manager_resource_key` |
| Q5 dag-tools-probes location loads cleanly when no probes are deployed | `test_probes_location_loader.py::test_loader_returns_empty_report_when_env_unset` |
| Q5 dag-tools-probes location soft-fails per-probe (one broken probe does NOT block the location) | `test_probes_location_loader.py::test_loader_soft_fails_a_malformed_probe` |
| Q5 N generated probes merge into one Definitions without conflict | `test_probes_location_loader.py::test_loaded_probes_merge_into_one_definitions_without_resource_collision` |
| Q5c probe runner launches against `dag-tools-probes` location with downstream asset key only | `test_probes_runner.py::test_run_probes_side_launches_against_dag_tools_probes_location` |
| Q5c PASSED probe is sacred (no relaunch on re-invocation) | `test_probes_runner.py::test_run_probes_side_skips_passed_probes_on_re_invocation` |
| Q5c LAUNCHED probes reconcile via GraphQL poll, not relaunch | `test_probes_runner.py::test_run_probes_side_reconciles_launched_via_poll_not_relaunch` |
| Q5c state mirrors to registry after every transition | `test_probes_runner.py::test_run_probes_side_mirrors_state_to_registry_after_each_transition` |
| Q6 counts synthetic classes with PASSED probes on BOTH sides as covered | `test_verdict.py::test_verdict_go_when_probes_pass_on_both_sides` |
| Q6 probe FAILED blocks GO regardless of `--accept-synthetic-coverage-missing` | `test_verdict.py::test_verdict_no_go_when_probe_failed_even_with_synthetic_accept` |
| Q6 partial probe coverage still requires `--accept-synthetic-coverage-missing` | `test_verdict.py::test_verdict_partial_probe_coverage_still_blocks` |
| Q6 divergent-but-passing probes block GO regardless of acceptance flags | `test_verdict.py::test_verdict_diverged_probes_blocks_go_even_with_passing_status` |
| Q6 matching probe records (both sides PASSED + clean diff) count as covered | `test_verdict.py::test_verdict_matching_probe_records_count_as_covered` |

---

## Known limitations

- **GraphQL drift.** Dagster GraphQL fields and internals shift across
  versions; the survey and launcher must be defensive and version-gated
  (schema introspection, soft per-field failure). The inventory extractor
  already follows this pattern; the Phase 2 GraphQL layer must too. When
  editing any mutation/query, select ONLY fields that exist on the target
  type — introspect the live schema, don't guess. Selecting a nonexistent
  field (e.g. `message` on `InvalidStepError`) 400s the entire request,
  not just that field. This was a real shipped bug that broke every
  `qual run` / `qual probes run` launch until caught by live testing.
- **Survey IO-manager FQN fidelity.** When an IO manager class is defined
  *inside a `.py` file that the survey loads via importlib* (rather than
  imported from an installed package), its captured FQN is a synthetic
  module name (`__dagtools_loaded_<file>__.<Class>`) that isn't importable
  later — so a generated probe falls back to `InMemoryIOManager`. Real
  deployments define IO managers in installed packages with genuinely
  importable FQNs, so this only bites synthetic/demo setups. If it matters,
  move the IO manager into an importable module.
- **`qual run` needs the deployment's real location name.** Pass
  `dagtools qual init --location-name <name>` (the name the test
  deployment's workspace surfaces, e.g. the user-deployment / gRPC server
  name). The launcher falls back to `"default"` otherwise, which no real
  deployment uses. Discover the name via the workspace GraphQL query or
  the Dagster UI.
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
- **Q2 v1 defers two recipe items.** The IO round-trip probe (per-class
  probe asset that loads representative output via the same IO manager)
  needs the Q5 probes code location infrastructure to land first. The
  local in-process orchestration snapshot (`dagtools qual orchestration`)
  is a separate command and ships separately. Both are tracked in the
  implementation-status table.
- **Q5 v1 is generation + deploy target + runner + Q6 coverage.**
  `dagtools qual synthetic` generates the modules,
  `dag_tools.probes_location.definitions` is the deployable code
  location, and `dagtools qual probes run --side <s>` exercises each
  probe through the test deployment. Q6 now counts synthetic classes
  with PASSED probes on both sides as covered (no
  `--accept-synthetic-coverage-missing` needed) and flags ran-and-failed
  probes as a hard block (NOT opt-out-able with the synthetic accept
  flag — that flag excuses *missing* coverage, not actively-failing
  probes). The visibility convenience commands (`qual probes status`)
  and deeper probe-record parity in Q6's per-rep diff are operator
  follow-ups, not blockers.

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
