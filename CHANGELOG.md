# Changelog

Notable changes to `edgy-dag-tools`, newest first. The import name is `dag_tools`.

This file starts at 0.11.0. **0.10.0 and earlier are not recorded here** — for those, read the
git history between the `v*` tags. Each release is a `chore(release): <version>` commit tagged
`v<version>`.

Downstream repositories depend on this library, so every entry says whether it changes an
existing signature, export or default. Where a default changed, the old behaviour and how to get
it back are named.

## Unreleased

### Added

- **A `systemlink_test_monitor` source kind** for `DltPipelineComponent`: a read-only, incremental
  (known-ids) and resumable-backfill load of SystemLink Test Monitor results and steps, with an
  `ingest_runs` table and three asset checks. Selected only by `source_config.type`; no existing
  YAML, signature, export or default changes. Its schedule targets `<pipeline>_incremental`.
  `operator` and `table_prefix` are required for this kind. Settings the server decides
  (`limits.page_size`, `incremental.order_by`, `limits.step_filter`, ...) ship as conservative
  defaults pending confirmation. `resolve_platform` gains a `VENDOR_PLATFORMS` table consulted
  after `SOURCE_TYPE_PLATFORMS`; `PRODUCER_ALIASES` is unchanged.
- **A source-kind seam in `DltPipelineComponent`** (`dag_tools.asset_wrappers.source_kinds`).
  A registered `source_config.type` selects a kind that owns validation, unit preparation,
  source building and any extra definitions. **No existing YAML changes meaning**: a missing
  `type`, `sql_database`, `postgres` or `duckdb` resolves silently to the legacy path, which
  delegates to the unchanged `_prepare_units` / `build_dlt_source`. Any other unregistered value
  also resolves to legacy, with one WARNING per value. The one new refusal is a near miss of a
  registered non-legacy kind, and none ships yet. `instantiate_assets` gains a trailing optional
  `kind=None`; no other signature, export or default changes.
- **A per-pipeline `schedule:` block** (`cron`, `enabled`, `timezone`). It builds
  `<pipeline>_job` over exactly that pipeline's asset keys and `<pipeline>_schedule`, which
  starts **STOPPED** unless `enabled: true` and skips a tick while a run of its job is queued or
  in progress. A malformed block, or a schedule on a pipeline that produced no assets, fails the
  load. The skip guard filters by job name, so two code locations defining a pipeline with the
  same name hold each other's ticks.

## 0.14.0 — 2026-11-10

### Added

- **An instrument on the gateway that counts reads recorded against nobody.**
  `dag_tools.central_gateway.principal_gauge` measures, per issued ticket, whether the subject
  the read was *recorded* for is the subject the authorization decision was *made* for. **It
  refuses nothing and changes no request**: no signature, export or default changes, no header
  is read differently, and a defect in the gauge degrades to a warning while the request
  proceeds exactly as before.

  It exists because the two subjects *were* resolved separately and, under the observe posture,
  disagreed. The gate resolved the originator header and fell back to the token's own claim;
  the recorded subject — the one in the broker payload and in `TICKET_ISSUED`, which agree with
  each other by construction — resolved the header with no fallback. So a request carrying a
  subject-bearing token and no `X-Originator-Email` / `on_behalf_of` header was authorized for
  that principal and recorded as nobody: the broker was handed a null subject and the issue line
  printed `subject=-`. Under `enforce` both read the verified decision and the question did not
  arise.

  That shape is not an edge case. It is the subject-source gauge's `token-claim` bucket — the
  one that gauge documents as the *healthy* shape — so the healthiest way for identity to
  arrive is the one way the record of the read names nobody, and every other signal on such a
  request is green. Nothing flagged it.

  **The divergence it measured is closed in this same release** — see the one-resolution entry
  under *Changed* — and the gauge stays, because its counts are the only standing evidence that
  the closure holds. `unnamed` and `divergent` are now states nothing on the request path should
  be able to produce, and an instrument retired the moment its finding is fixed is how the next
  one arrives unseen.

  The fix was not free to apply unobserved, which is why the measurement shipped first: the
  recorded subject is encoded by a `mint-role` backend into a role's GRANTs and ROW POLICY, so
  sending a subject where `null` went before changes what a credential grants, for callers nobody
  had enumerated; and the subject available to fall back on was read from an *unverified* token,
  which is a defensible thing to log and a different thing to authorize a credential against.
  Both are answered in the same release: the fallback is gone rather than widened, so no
  credential is minted for a subject nothing established.

  Buckets are logged as `principal-record: record=<bucket>`: `named`, `unnamed` (the finding),
  `divergent` (should be unreachable, and worse than `unnamed` if it ever appears),
  `undecided`, and `unreadable` — the last being a request the upstream gauge could not read,
  **never counted as clean**, because zero-because-silent and zero-because-clean are the two
  states an instrument exists to separate. The gauge announces itself at startup for the same
  reason. `unnamed`, `divergent` and `unreadable` each also emit a WARNING naming the urn and
  no principal and no credential; the ticket id is on the `TICKET_ISSUED` line.

  The counts are also served on `/health` as `principal_record`, the way `telemetry_misses` is in
  the agent. Every bucket is always present even at zero, and `observed` rides beside them as the
  discriminator: a dict of zeros from a gateway nothing has asked to read and one from a gateway
  serving clean reads are otherwise the same bytes. `/health` still answers if the counter
  misbehaves — `principal_record` comes back `null`, the status stays `ok`, and a warning names
  the exception type. **No existing `/health` key changes.**

- **The gateway's `enforce` posture now verifies the issuer against an accepted *list* and the
  audience against one.** Two new env vars, read by `dag_tools.central_gateway.identity`:

  - `GATEWAY_JWT_ISSUERS` — comma-separated issuers this gateway accepts.
  - `GATEWAY_JWT_AUDIENCES` — comma-separated audiences this gateway accepts.

  **Neither has a default and an empty list refuses every token**, reported as
  `verification_unconfigured` (503). That state is reachable on purpose: a built-in value would
  be worse than a wrong one, because it would make the rule unobservable. The singular
  `GATEWAY_JWT_ISSUER` / `GATEWAY_JWT_AUDIENCE` still work and are read as **one entry of the
  same list**, so a deployment that set them keeps working and there is never a second
  resolution to disagree with the first.

  The issuer is a list because one provider is commonly reachable under more than one name. A
  token carries the issuer it was minted with, so a provider's in-cluster service name is a
  second accepted **entry** rather than a rewrite of a proven claim. A signature-valid token
  naming an unlisted issuer is refused as `issuer_not_accepted` (401) — a distinct reason, with
  a WARNING naming the rejected issuer and the var to list it in, because `token_invalid` would
  send an operator looking at keys and clocks when the token is fine.

  `identity.announce()` now reports `issuers=<n> audiences=<n> enforce_ready=yes|no` and, when
  it is `no`, the names of the unset vars. Counts and env var names only, never values:
  `issuers=0` is a gateway that refuses every token, and that is the one state which otherwise
  looks exactly like a quiet one.

### Changed

- **One function now resolves the principal, and both the authorization decision and the record
  of the read use its answer.** `identity.resolve_principal` is the single answer to "who does
  this request read as": `check_topaz_authz` is handed that answer and decodes nothing itself,
  and the broker payload and the `TICKET_ISSUED` line name the same principal by construction.
  There were two resolutions before, and they could only agree by coincidence — which they
  mostly did, which is why the divergence ran unnoticed and had to be measured before it could
  be believed (the principal-record gauge, above, is what measured it).

  **A token's claim is a principal only if this gateway verified the token** — signature, then
  the issuer against the accepted list, then the audience, from served values, with an empty list
  refusing everything. An unverified token names nobody, and **a request that names nobody is now
  refused `no_principal` (401) before the authorizer and before the broker**: no ticket is minted
  and no role is minted for a subject nothing established. Previously such a request was
  authorized for the claim the token asserted and recorded against nobody.

  **This changes `observe`, the default posture, in exactly that one way, and there is no flag
  back** — a deployment wanting the old reading of those requests does not have one, because the
  old reading was the defect. The `X-Originator-Email` / `on_behalf_of` header path is untouched:
  it still wins, is still claim-agnostic, and still does not care whether the token verified,
  which is the path every reader rides today. A request that names a principal is unaffected
  under either posture.

  **A defect in the resolution now refuses under `observe` too** (`verification_error`, 503);
  observe previously warned and served. Failing closed is safe here precisely because the
  resolution reports its refusals as *reasons* rather than as exceptions — an unconfigured
  verifier and an unreachable JWKS both come back as a decision an operator can read — so an
  exception out of it is a defect in the resolution itself, and a resolution that cannot say who
  a request reads as must not let the read proceed under a guess. The evidence line is the other
  way round and stays that way: a failure emitting it is swallowed and the request proceeds,
  because an instrument may not break what it measures.

  The two pins written deliberately red — the gauge module's own, and the matrix arm that pinned
  `"subject": null` as the behaviour of a token-only request — are flipped in the same change,
  and a new structural pin asserts the shape rather than the outcome: the gate reads nothing off
  the token, the route resolves once and emits no evidence line of its own, and one request
  verifies the token once. The behavioural seal can be satisfied by accident, since two
  resolutions agree most of the time.

- **Under `enforce`, a token with no `aud` claim, or an `aud` outside the accepted list, is now
  refused** (`token_invalid`, 401), and a gateway with no accepted audience configured refuses
  every token (`verification_unconfigured`, 503). Previously the audience was not verified at
  all. **This also reaches the internal doors** (`INTERNAL_DOOR_POSTURE=enforce`), which share
  the one verification: the mesh service token needs an audience the list accepts — in
  Keycloak, an audience mapper on the `client_credentials` client. There is deliberately no
  per-caller exemption, because a second verification is a second answer and two answers that
  can disagree is the defect being closed. **`observe` (the default posture) is unchanged** and
  refuses nothing; a deployment not running `enforce` is unaffected.

- A GitHub Release now attaches the wheel and the sdist by name rather than globbing `dist/*`.
  `uv build` also writes `dist/.gitignore`, a release asset's name may not begin with a dot, so
  the API renamed it and every release from v0.10.0 to v0.13.0 carries a stray 1-byte
  `default.gitignore`. Nothing broke; the only reader it misled was a human deciding what to
  download. Naming the two patterns also makes `fail_on_unmatched_files` mean what it says —
  against `dist/*` the `.gitignore` alone satisfied it, so a build that produced no sdist would
  have published a wheel-only release.

- `test_the_gateway_does_not_BRANCH_on_the_gauge` now measures what it always stood for.
  It forbade *assigning* the subject-source gauge's reading, as a proxy for not branching on
  it; the principal-record gauge is built out of that reading, so the proxy stopped fitting.
  It now asserts no name bound from the gauge appears in a condition, comparison, `return` or
  `raise` — which also catches a branch written through an alias, something the old form could
  never have seen.

## 0.13.0 — 2026-11-09

### Added

- **The deployment now carries rows a governed read is supposed to REJECT.** The demo surface
  gains `dag_tools.user_deployment.demo_ev_refusal` — three assets holding one planted error
  per rule the shipped EV contract declares, each addressed by its own synthetic program, plus
  a clean control program in the same asset. **No existing signature, export or default
  changes**; `mesh_demo_assets.build_mesh_demo_io_manager()` is a new export that holds the
  IO-manager construction `build_demo_defs()` was already doing inline, and the new surface is
  merged only under `DAG_TOOLS_DEMO_MODE`, so a deployment with the toggle off is byte-for-byte
  what it was.

  The existing `ev_fixture` is what a read should *accept*. Nothing deployed was what it should
  *refuse*, which meant every refusal path a governed read has existed only in the reader's own
  unit tests, over frames that reader had built itself — a reader could agree with itself about
  a defect no producer is able to emit, and a refusal that fires for the wrong rule looks
  exactly like one that fires for the right one. The planted rows travel the real
  CSV -> polars -> parquet -> IO-manager path, so a planted null arrives as a null and a planted
  NaN as a NaN, and each case is isolated to one rule: a frame carrying two violations proves
  nothing, because a reader refusing on the first never reaches the second.

  Two of the rules cannot be planted in a program's rows at all — a column's absence and a
  column's type are properties of the frame, not of a row — so those get an asset each. One is
  not a producer defect in the first place: the contract says the dataset may hold other
  programs' rows and that a read which kept them forgot its filter, so that case is the asset
  holding every program at once. The checks live in `dag_tools_tests/test_demo_ev_refusal.py`
  and are driven off the contract document, so a rule added there fails the fixture until a
  case for it is planted.

- **A routing ticket now says which read it is, and over what version of the data.**
  Every ticket the broker issues carries `ticket_id`, `issued_at`, `as_of` and
  `as_of_source`; `dag_tools.cortex_data.client.CortexDataClient` gains
  `get_dataframe_with_ticket(urn)`, which returns the frame **and** a receipt for the read;
  and the gateway logs a `TICKET_ISSUED` line naming the ticket id against the principal it
  authorized for. **No existing signature, export or default changes** — `get_dataframe(urn)`
  still returns a bare `LazyFrame` and now delegates to the new method, and a consumer that
  ignores the new keys is unaffected.

  Two things were impossible before. A read could not be **referred to**: the gateway logged
  its decision and the broker logged its minting, and nothing joined either to the frame a
  consumer ended up holding, so "which read was this?" — the question an audit, a complaint
  and a bill all begin with — had no answer. And a frame could not be **pinned to a version**:
  two agents reading a dataset that is being rewritten underneath them legitimately disagree,
  and neither could say which version it read.

  **`as_of` is the data's version; `issued_at` is the time we looked.** They are never the same
  field, and an absent `as_of` is never filled in with the issue time — the tempting
  substitution, because it makes every ticket look complete, and the one that would make the
  field worthless: a reader could no longer tell a version from a clock reading, and would have
  to assume the worst of every ticket including the ones carrying a real version. An absent
  `as_of` instead carries `as_of_unavailable` (a reason) and is **counted per backend on
  `/health`**, beside the ADR-0044 exposure numbers and for the same reason. The same house
  rule as `warehouse_source.observe.read_as_of`: a version is carried exactly as the source
  wrote it, because reformatting makes two equal watermarks look different.

  `as_of_source` distinguishes a version read **at authorize** from one the producer
  **advertised at load**. The distinction is load-bearing rather than decorative: advertised
  coordinates are read once at broker startup and live in `LOCAL_ASSETS` for the process
  lifetime, so an advertised version describes the dataset as it was when that pod booted.
  `ticket_identity.AS_OF_READERS` is the per-backend table that reads a current version, and it
  **ships empty** — reading a Delta or Iceberg version means real IO against a real store,
  which nothing here can exercise yet — so until a backend gets a row, `as_of` comes only from
  what a producer advertised, labelled as such and counted when absent. Registering a reader is
  `ticket_identity.register_as_of_reader(source_type, reader)`; a reader that raises degrades to
  an absent `as_of` and never fails a ticket, because an as-of is metadata about the read while
  a credential **is** the read.

  The consumer's receipt is an **allowlist** (`client.TICKET_RECEIPT_FIELDS`), not "the ticket
  minus credentials": it is meant to be kept, printed beside an answer and pasted into a ticket,
  so a denylist would start publishing the day a ticket gains a secret-shaped field.
  `physical_uri` is deliberately left off it — a presigned URI is a credential wearing a path.

  Both halves are additive across a rollout: the identity is the broker's to mint, so a gateway
  talking to a broker that has not been upgraded logs `ticket_id=-` and serves the read, and the
  client's receipt keeps one shape (the keys present, as `None`) whatever it is talking to.

## 0.12.0 — 2026-11-07

### Added

- **Classification propagation (ADR-0064 steps 4b/4c), as a pure engine.**
  `dag_tools.warehouse_source.propagate` composes a derived column's label
  from the source columns it read: column lineage in, one decision per output
  column out, with no Dagster, no catalog and no warehouse. A column with no
  declared lineage falls back to the table max rather than to nothing.
- **`ClassificationPropagationComponent`** — the YAML surface over that
  engine, and a new export `dag_tools.ClassificationPropagationComponent`. It
  builds **one sensor and no assets**: for each derived dataset it watches,
  every new materialization is read for its `dagster/column_lineage`, each
  output column's label is composed from the source columns that column came
  from, and the result is published to the catalog against the dataset's own
  physical URN (a `dagster` one is refused) and recorded against the asset as
  an observation. What *builds* the derived table is the deployment's own
  business; this component neither owns nor materializes it. Published as two
  **patch** aspects — `datasetProperties` and `editableSchemaMetadata`, the
  latter removing every vocabulary term it does not set, since add-only would
  leave a stricter term behind and void a downgrade. The sensor ships
  **stopped**, re-reads the dimensions document every tick, isolates one
  dataset's failure from the others, and retries a failing dataset on later
  ticks without re-materializing it. `dry_run` composes and records everything
  and emits nothing. Chained propagation — a derived dataset as another's
  source — is not supported. Which upstreams carry labels is declared per
  dataset, not inferred from the asset graph.
- **Source-side label lookups read the observation log as well as the
  materialization log.** A step-4a source is an observable source asset, so
  observing it emits an `AssetObservation` and never a materialization: a
  materializations-only lookup refused every real 4a source with "publishes no
  label set", a wiring failure wearing a labelling failure's words. Found by
  building the example, not by review.
- **`examples/warehouse_source` now runs both halves end to end** against
  sqlite with no catalog and no credentials: the labelled source, plus a
  derived table whose three columns show composition from column lineage,
  composition followed by a declared downgrade, and the table-max fallback.
- **The classification dimensions document declares a `type:` per dimension**
  — `hierarchical`, `permissive_set` (a reader holds at least one of the
  label's values), `restrictive_set` (holds all of them) or `exclusionary`
  (holds none) — following ADR-0064's 2026-11-06 amendment aligning it to
  OpenDDIL ADR-0055. Composition is each type's own: `max` on the ladder,
  union on a restrictive or exclusionary set, and **intersection** on a
  permissive one, since a wider permissive set admits more readers. A
  dimension that declares no type is a restrictive set, which is exactly what
  every unordered dimension already meant, so a document already mounted
  means today what it meant yesterday.
- **Downgrade declarations.** No downgrade is automatic: a standing one is a
  declared rule carrying a named owner and a reason, and each application
  records the rule, the owner, the reason and both the derived and the
  published label. A rule that names no owner, gives no reason, aims at a
  column the asset does not produce, tightens any dimension or changes
  nothing is refused rather than recorded.

- **OTel -> API sync: a concurrency cap and a bounded retry.** New optional `api:` fields
  `max_concurrency`, `max_attempts` and `max_retry_seconds` (mapping.yaml or component.yaml;
  the component wins). `max_concurrency` limits in-flight requests per worker process, per
  `base_url`, so the effective cap is `max_concurrency x worker replicas`. The wait for a permit
  is inside the durable step, so a replay never waits. The retry bound turns an exhausted call
  into a recorded `FAILED` plan (plan hash withheld, delivered calls banked) rather than an
  invocation that retries forever. Each worker thread now keeps one `requests.Session`, so
  connections are reused. **The cap and the bound need the new worker image**; an older worker
  ignores the fields and behaves as before. **No default changed** for the library: with none of
  the fields set, requests and retries behave exactly as they did. **One chart default
  changed:** `restateWorker.apiCallPlan.abortTimeoutSeconds` is now `3600` (Restate's own
  default is 600), exported as `API_CALL_PLAN_ABORT_TIMEOUT_SECONDS`. To get the old behaviour
  back, set it to `null`. `restateWorker.apiCallPlan.inactivityTimeoutSeconds` defaults to
  `null` (Restate's default). Setting either needs Restate 1.6 or later (discovery v3).

- **Contract mapping — `ContractMappingComponent`.** A data contract plus a source schema in,
  the dbt model that satisfies the contract and the tests that prove it still does out. New pure
  module `dag_tools.contract_mapping` (no Dagster, no dbt, no IO: documents in as dicts, a
  `MappingPlan` out), new export `dag_tools.ContractMappingComponent`. The same plan also runs
  over Python rows (`apply_to_rows` / `check_rows`), which is how a mapping is proven with no
  warehouse. Runnable example with no warehouse, no dbt project and no credentials:
  `examples/contract_mapping`.

  **With a file contract the whole plan is built at the code location load.** A mapping that
  reads a column the source does not have, drops a column a governed read selects (required,
  grain or scope), or disagrees with the contract about write cadence fails there — not in a run
  an hour later. A **served** contract (iagent's JSON) is fetched at materialization instead,
  because a code location must not fail to load because a service is down; set
  `expect_contract_id` there, since what arrives is not under the operator's control and a
  service that starts serving a different document would otherwise silently re-point the
  mapping.

  The line between a refusal and a note is whether the emitted artifact would be **wrong**. A
  test that could not be written — the contract names its period calendar rather than listing
  it, no `warehouse_types`, no `non_finite_predicate` — is a note on the materialization, and
  **nothing is guessed to fill the gap**: no default warehouse type, no default cast, no default
  non-finite predicate, no inferred calendar. `config.contract.enforced` is emitted only when
  every column has a declared type.

  The **asset of record is the physical platform's dataset URN**, from the contract, never a
  `dagster` platform URN; the source table's URN and the Dagster asset attach as lineage.

  One hazard worth knowing before shipping a scaled integer column: the row route **refuses** a
  scaled value that is not whole, while a SQL `cast` to an integer type **rounds** in most
  dialects. The cast is your own SQL from `cast_templates` and this library owns no dialect, so
  this is documented rather than papered over — if silent rounding is unacceptable, the cast is
  where the check goes.

  `prune` is off by default: the component writes into a directory it does not own, and deleting
  a test someone wrote by hand is worse than leaving a stale one. With `prune: true` only files
  named `<model>__...` are ever removed.

  Verified on `dagster==1.10.19` as well as current — the whole test module set runs identically
  on both, same plan hash, and the example loads and materializes on both.

- **Q2 IO round-trip probe.** `dagtools qual synthetic` generates a probe per RUNNABLE
  class; `dagtools qual run` runs it after the Q2 pass, loading the representative's output
  through its real IO manager and recording type and columns. It writes nothing. New
  `RunRecord.partition_key` / `probe_metadata` and `ProbeManifest.roundtrip_probes`
  (additive; schema bumps). `qual probes status` counts missing round-trip assets.

- **`dagtools qual stage` materializes the upstreams Q2 reads, in runs of its own.** (#18)
  Q2 launches a representative alone, and Dagster treats an unmaterialized upstream as a
  *loaded input* whose output must already exist in the IO manager — so a representative whose
  upstreams are computed rather than external had nothing to read. `dag_tools.qual.stage` plans
  those upstreams, launches them in dependency order (one run per partition group, since a run
  cannot mix partition definitions), and records the result in a slot of its own at
  `<side>/stage/`. A failed group skips only the groups that depend on it; a cross-group cycle
  is reported rather than launched. Staging is per side, so re-run it after the candidate
  upgrade.

  **Q2 itself is unchanged.** Its selection stays the representative alone, and `StageRecord` is
  not a `RunRecord`, so Q6's diff has nothing to key it on — Q6 records `stage_by_side` and
  never compares it. `qual run` warns when required representatives exist and stage has not
  passed for the current selection hash. Unlike Q2, an empty `launch_info` or a declared but
  unresolved `staging_overrides` pointer refuses to launch rather than degrading: staged outputs
  landing in the wrong place would bank a qualification of the wrong deployment.

- **Probe IO managers take constructor config from the manifest.** (#4) A new `probes` block,
  written by `dagtools qual init --probe-config`, supplies kwargs per IO manager FQN — so a
  probe for an IO manager that takes arguments can now PASS. Previously only a no-arg IO manager
  could, which is why synthetic coverage needed `--accept-synthetic-coverage-missing`
  indefinitely. Keyed by FQN rather than `class_hash`, because class hashes change on every
  re-survey; `io_manager_config_by_class` is the narrow override and wins. A `{env: VAR}` value
  renders as `os.environ["VAR"]` in the generated probe and is **never resolved at generation
  time** — probe source is published to the registry and written under `~/.dagtools/quals/`, so
  a resolved value would commit a credential to both. The manifest records kwarg names, never
  values. With no config the generated source is byte-identical to before.

- **A class's other resources can be bound into its probe, opt-in per resource FQN.** (#5)
  Resource keys now travel key -> FQN on `ClassMember.resource_classes` ->
  `Representative.resource_classes` (class matrix schema 3); the class key still discards them,
  so a hash stays stable across repos that name the same resource differently.
  `probes.resource_config` (keyed by resource FQN, `{}` for a no-arg construct) is the opt-in: a
  resource with no entry is not bound, and is named in the bundle's notes. Binding everything
  automatically could regress a probe that passes today — a resource that connects in
  `setup_for_execution` really connects — and a self-inflicted coverage regression is
  indistinguishable from a real Q6 finding. Each resource is constructed in its own
  `try`/`except`, so one that fails is left out of `Definitions` instead of taking the probe
  location down. The probe proves import, construct and availability on the context, not backend
  behaviour. With no entries the generated source is byte-identical to before.

- **Warehouse source — `WarehouseSourceComponent`.** One labelled source dataset per
  component instance, published as a Dagster **observable source asset**: no data is moved,
  and the observation records a row count, an order-sensitive schema hash and an optional
  as-of watermark. The catalog payload carries a classification label per column, the owner,
  the access mode (`brokered` / `passthrough`) and a `classification_origin` property. New
  pure module `dag_tools.warehouse_source` (the dimensions document, the label algebra, the
  observation plan and the catalog payload — no Dagster, no DataHub, no SQLAlchemy, no dlt),
  new export `dag_tools.WarehouseSourceComponent`. Runnable example against sqlite with no
  credentials and no catalog: `examples/warehouse_source`. **Nothing is removed or renamed;
  no existing signature, export or default changes.**

  **The asset of record is the physical platform's dataset URN.** A `dagster` platform URN is
  refused at the code-location load, and the Dagster asset is attached to the physical dataset
  as lineage in the fabric read off that URN — so a broker authorizing a read and a catalog
  describing it name one object.

  Column labels are `{dimension: values}` and are validated against the **deployment's**
  mounted classification document — env `DAGTOOLS_CLASSIFICATION_LEVELS`, default
  `/etc/dagtools/classification/levels.yaml`, top-level key `classification_dimensions`. This
  library ships no such file and names no dimension and no value, not even as a default; the
  hierarchical dimension is discovered from the document's `hierarchical` flag, takes exactly
  one value, and is required on every label that exists. A column with **no** label is
  published most restrictive in every dimension and counted as `defaulted_columns` — deny by
  default, so an incomplete list under-serves rather than over-serves — while a labelled
  column the source no longer has fails loudly instead of being defaulted. The document is
  read again **at materialization**: if it is absent then, the run fails and names the path,
  but the code location still loads, so a missing ConfigMap cannot take the location's other
  assets down with it.

  Reads go through the tagged warehouse resource named by `resource_key`, via a new
  `fetch_columns(statement)` seam on `TaggedWarehouseResource` / `BoundWarehouse` —
  **additive**, alongside the unchanged `fetch_rows`. It exists because an empty table yields
  no rows to read column names off, so names taken from a row would report a source with zero
  columns. A schema read is refused at the same chokepoint if it cannot carry its tag.

  Two deliberate differences from `CustomDbtProjectComponent`'s DataHub publishing: **a
  catalog failure fails the materialization** (here the publish is the deliverable, not a
  side effect of a build worth keeping), and **no `schemaMetadata` aspect is emitted**,
  because only column names were read. Use `dry_run: true`, or omit `catalog.server`, to see
  the exact payload with no server.

  Two Dagster facts worth knowing if you build on this: an observe function is called with
  resources only, so an observable source asset can take **no run config** (a `config:`
  parameter raises `TypeError` at run time, not at definition time); and source assets are
  **excluded from `AssetSelection.all()`**, so a job built that way runs zero steps and still
  reports success — name the keys. Both measured on `dagster==1.10.19` and current; the
  example loads and materializes on both.

  Downstream label propagation (`max` on the hierarchical dimension, union on the rest) is
  not in this release.

- **Tagged warehouse resource — `TaggedWarehouseComponent`.** A warehouse resource that
  sets a query tag of `{principal, asset_key, run_id}` on every query it sends, so the read
  audit below can explain the read. `principal` is `pipeline:<name>` or the authz_id the
  gateway verified for a read made on someone's behalf. **A query it cannot attribute is not
  sent** — it raises rather than issuing an untagged read, and the refusal happens before
  the connection is resolved. New module `dag_tools.resources.warehouse`, new pure module
  `dag_tools.audit.warehouse` (how the tag travels, decided as data), new export
  `dag_tools.TaggedWarehouseComponent`. Runnable example with no warehouse and no credentials:
  `examples/tagged_warehouse`.

  This library ships **no tag dialect and guesses none**. `placement: comment` puts the tag in
  the statement text; `placement: session` runs your own statement first with the tag bound as
  `:tag`, and a statement that does not bind it fails the code-location load rather than
  attributing every later read to nothing. The connection has **no environment fallback**:
  name a `dsn`, or a `drivername` with host/username/password/database. It is resolved to a URL
  during validation and never to an engine, so a typo fails the load and no driver is imported
  while a code location loads.

  A resource is not told which asset is reading it, so `fetch_rows` tags the run; call
  `for_asset(context)` for asset-key attribution or `for_user(authz_id)` for a brokered read.
  It drops in under `ReadAuditComponent` unchanged — same `fetch_rows(statement,
  parameters)` seam, named by `resource_key`.

  Verified on `dagster==1.10.19` as well as current — the example loads and materializes
  identically on both. Two things that only the floor run showed: the connection resolves
  through SQLAlchemy rather than `config_to_credentials`, because the latter imports `dlt` and
  `dlt` is an `orchestrator` extra (a base install could otherwise not resolve a connection at
  all); and environment references in a component file are spelled `{{ env('X') }}`, since the
  `{{ env.X }}` attribute form does not resolve on 1.10.19 and fails the whole code-location
  load.

  **No breaking change**: additive only. Nothing existing changes signature, export or default.
  `sqlalchemy` was already a declared dependency and every import of it is function-local.

- **Read audit — `ReadAuditComponent` and `dagtools audit`.** Reports which assets read which
  warehouse objects from the warehouse's own query history, and which reads no asset explains.
  Read-only: it issues one `SELECT` (or reads an export you already have) and writes a report;
  it grants nothing and changes nothing. New package `dag_tools.audit` (pure — imports neither
  Dagster nor `dag_tools.qual`), new export `dag_tools.ReadAuditComponent`, new CLI group
  `dagtools audit` with `statement` and `reads`. Runnable example that needs no warehouse and no
  credentials: `examples/read_audit`.

  Nothing in it names a relation, a column, a level or an identity — not even as a default. The
  five history column names and the relation are required fields on the resource topology,
  because a wrong default would audit the wrong column and report an empty window as a clean
  audit. Your names arrive through YAML and leave in the report.

  The component builds the statement and hands it to a resource as SQL plus bound parameters
  (`fetch_rows(statement, parameters)`); this library opens no connection and carries no driver
  dependency. Reads are attributed by query tag, then by correlation against a single-asset run
  window, and anything else is reported as `ambiguous` / `unattributed` / `not_a_pipeline` —
  stated in the headline gap *and* broken out by cause, since those three have different fixes.
  `coverage` is omitted rather than reported as `1.0` when nothing was read.

  **No breaking change**: additive only. Nothing existing changes signature, export or default.

  Verified on `dagster==1.10.19` as well as current — the example loads and materializes
  identically on both, and the component file deliberately stays inside `type:` + `attributes:`
  to keep it that way.

- **The earned-value fixture carries `status_period` and `eac_reported`.** (contract #275)
  `status_period` is a program figure like `program_bac`: the period the program is reported
  through, repeated identically on every row, so a reader takes it once per program. It is the
  column an as-of read selects on, and it is one of the periods the rows actually carry — a
  status period outside that calendar would make every as-of computation select nothing and the
  read would look empty rather than wrong.

  `eac_reported` is the estimate as it was stated at each period's status, and it is the one
  money column that belongs to the `(work_package, period)` row rather than to the work package.
  **It is a stock, not an amount**: it adds across work packages at one period and never across
  periods. Summed over the work packages at one period it is the program's estimate as reported
  then; summed over every row it comes to nothing meaningful while still reading as a plausible
  total. It is a column of its own rather than a time-phased `eac` precisely because `eac` must
  stay constant per work package for the readers that take it once per path — so `eac_reported`
  **varies down a work package's rows where `eac` cannot**, and a reader that takes it once per
  path is wrong. No invariant is written over it (none can be), so the fixture's own tests are
  what would catch a source holding it flat; at the status period it equals that work package's
  `eac`, and no reported estimate is below the cost booked through its period. `Int64`, like the
  other two completion figures.

  `bcws`, `bcwp` and `acwp` remain the only per-row amounts and the only ones a reader may sum.
  The fixture's other columns and types are unchanged. **An existing materialization does not
  gain the columns** — re-materialize `ev_fixture` to pick them up.

  `examples/contract_mapping` maps both (the contract declared them forward with
  `produced_since: null`; that is now set), so the mapping example again produces every column
  the fixture ships and the plan writes a constant-within test over `status_period`.

### Changed

- **A release tag now RENAMES the commit's image build instead of rebuilding it.** (#16)
  `v<X.Y.Z>` no longer runs the container build a second time: the tag job GETs the
  `:sha-<short>` manifest the branch build already pushed and PUTs **its own bytes** under
  `X.Y.Z` (and `X.Y`), so the release tag and the commit tag resolve to **one digest**.
  Before this, one commit had two digests and a deployment pinning the release pinned bytes no
  commit tag named — measured on `v0.11.0` against `sha-4924235`: different config blob and
  most layer blobs different. Nothing in this build is reproducible (every run resolves
  dependencies fresh), so a second build of one commit could never be the same image.
  **`:latest` is a master pointer and a tag no longer moves it.** A tag on a commit master
  never built now fails loudly rather than falling back to a build. This is the first release
  cut through that path.

### Fixed

- **A failed or timed-out poll no longer marks a run FAILED and launches a second one.** (#19)
  `poll_to_completion` raising — a timeout, a transport error, a non-terminal event-log read —
  says nothing about the run, which may still be executing. The representative now stays
  LAUNCHED with the error noted and the next invocation reconciles it. Marking it FAILED, as the
  runner did, let the next pass launch a second, concurrent run of the same representative. The
  one permanent case is a run id Dagster no longer has (`DagsterRunNotFoundError`, matched by
  GraphQL typename): that run will never finish, so it is FAILED and relaunched. Q2, the probe
  runner and `qual stage` all follow the rule. A probe left LAUNCHED counts as uncovered in Q6,
  not red, because nothing failed.

### Behaviour change

- **A RUNNABLE class with no round-trip coverage now blocks GO in `dagtools qual report`.**
  Previously Q6 gated only on the Q2 run and synthetic probes. It now also requires the Q2
  IO round-trip probe (above) to have PASSED on both sides with the same loaded type and
  columns. Existing qualifications that were GO will report NO_GO until the probes are
  generated and run. To get the old verdict back, pass `--accept-roundtrip-missing`, which
  excuses missing coverage only; a probe that failed on one side or loaded a different shape
  blocks regardless. To clear it: `dagtools qual synthetic`, deploy `dag-tools-probes`, then
  re-run `dagtools qual run` per side. Classes with no IO manager class, and dbt classes, are
  listed as not applicable and never block. Verdict schema 2 -> 3 (additive).

## 0.11.0 — 2026-11-04

**Tag repaired 2026-11-05 (UTC).** `v0.11.0` was first pushed at `cc915a5`, which is
0.10.0's tree, by operator error; the tag was deleted and re-pushed at `4924235`, the merge
that carries everything below. **Nothing consumed the first tag.** PyPI refused the upload
(`0.10.0` was already there under that content), so `0.11.0` on PyPI has only ever been built
from `4924235`. The container images are the part that did go out: the cancellation landed
after their push steps had finished, so `:0.11.0`, `:0.11` and `:latest` carried 0.10.0's tree
for about four minutes — 02:01:21 to 02:06:38 UTC — before the corrected tag's run replaced
every one of them. The replacement was confirmed by resolving each tag's digest against the
registry, not from the run's own status. **No deployment resolves those tag names**: the
sandbox pins these images by digest, so nothing running there could have pulled the wrong tree.
An install that leaves a dag-tools image's `tag` unset falls through to `:latest`, which the
first tag's run also pushed and the corrected run also replaced — if you pulled a dag-tools
image inside that window, re-pull.

### Added

- **The central gateway verifies the token before deciding who a request reads as.** (#6)
  A new identity module takes the subject of an authorize request from a verified token —
  signature, expiry and exact issuer — rather than from an unchecked header.
  `GATEWAY_IDENTITY_POSTURE` is **`observe` by default**, which changes no outcome and logs
  `IDENTITY_WOULD`; `enforce` refuses a header that disagrees with a user token's claim, and
  refuses a service naming a user unless the delegate seam lists it for the route. Both names are
  audited either way.
  The on-behalf-of header is spelled `on_behalf_of` on the wire. `X-Originator-Email` stays
  honoured everywhere it already was, and the client sends both and accepts `originator_email` as
  an alias, so **no caller has to move**. Note the delegate seam's default answers `False` until
  the policy-rail reader is wired: an *enforcing* gateway therefore refuses every delegation.

- **The two internal doors require a service token.** (#8)
  `POST /api/v1/internal/resolve` (domain broker) mints credentials and
  `POST /api/v1/internal/register` (central gateway) points a URN's route at a broker. Neither
  checked its caller. Each now wants a verified `svc:` token whose service is in that door's own
  list — `BROKER_RESOLVE_CALLERS` and `GATEWAY_REGISTER_CALLERS`, each comma-separated, **empty
  admitting nobody under enforce**. `INTERNAL_DOOR_POSTURE` is `observe` by default (a refusal
  logs `would_refuse` and the request is served) and is separate from
  `GATEWAY_IDENTITY_POSTURE`, so the doors and the read path flip independently. Callers obtain
  their own token through `internal_door.service_headers()`, which never raises.

- **The gateway's route ids are published as package data.** (#10)
  `dag_tools/central_gateway/gateway_routes.json` ships in the wheel, so a consumer no longer
  keeps its own copy of the list. Read it through `dag_tools.central_gateway.routes`
  (`load_routes()`, `route_ids()`, `delegatable_route_ids()`) — a loader that imports nothing but
  the standard library, so neither `fastapi` nor `dagster` has to be installed to read it.
  Each route carries **`delegate_rows_may_name`**: read that field rather than deciding the
  answer from the route's id, its module or a list kept elsewhere. A test parses the route
  constants out of the source and fails if the published ids and the code ever disagree.

- **A synthetic earned-value fixture in the demo code location.** (#7)
  An `ev_fixture` asset beside `mesh_demo_customers`, so a governed read of tabular period
  figures has something to read on a demo cluster: same group, same producer IO manager, same
  `DAG_TOOLS_DEMO_MODE` switch. The rows live in a CSV inside the package. **Nothing in code
  names a program**, and with the switch off the module is never imported, so a production
  deployment gains nothing.

### Changed

- **The earned-value fixture carries `bac` and `eac` per work package.** (#11)
  Both are work-package figures at a status date, repeated identically on every one of that work
  package's period rows — exactly as `program_bac` repeats on every row of the program. **A
  reader must take such a figure once per work package and must not sum it over the periods**:
  summed over rows it comes out at one value per period, and the total still conserves, so a
  wrong answer looks right. `bcws`, `bcwp` and `acwp` remain the only per-row amounts and the
  only ones a reader may sum. Both new columns are `Int64`, so no difference taken between them
  can be non-finite.
  The fixture's other columns and types are unchanged. **An existing materialization does not
  gain the columns** — re-materialize `ev_fixture` to pick them up.

- **`OtelApiSyncComponent` schedules from YAML with `cron_tick_passed(...)`, not `on_cron`.**
  Documentation and tests only; no code change. `AutomationCondition.on_cron` can never fire for
  this component — its `all_deps_updated_since_cron` waits on the extraction asset's upstream,
  an external ClickHouse source spec that nothing materializes, so extraction is never requested
  and dispatch waits behind it **silently**. The example README now carries a working
  `post_processing` block and says that the auto-created `default_automation_condition_sensor`
  **starts stopped**. If you hand-wrote a `sensor.py` to work around this, the README block
  replaces it.
  The block requires a Dagster whose component YAML accepts `post_processing`;
  **1.10.19 rejects it and fails the whole component load**, so deployments on that
  version have no YAML schedule and must set the condition from Python. The tests for
  the block skip there rather than fail, naming the missing field.

### Fixed

- **Numbers in a rendered otel→API plan are exact or absent — never `NaN`, `Infinity`, or a
  rounded integer.**
  JSON carries none of those, so a plan could pass review and then fail at dispatch. Two paths
  produced them silently: native template rendering literal-parses string results, so an
  attribute value shaped like an exponent (`"8e01234"`) rendered as a non-finite float — such a
  parse now keeps the original string, and every other literal parse is unchanged, because
  mappings rely on `"0"` arriving as `0`. And the numeric coercion helpers went through
  `float()`: integer text now parses exactly with no rounding past 2\*\*53, non-finite input
  returns the helper's default, and `as_int` refuses a float-parsed value it cannot represent
  exactly. `build_plan` also rejects any non-finite number that still reaches a plan, naming the
  group and field path, so a dry run fails the same way the send would.

- **The user-deployment image's `s3fs` floor is declared on the extra the image installs.** (#9)
  `dag_tools.io_managers.delta` and `.arrow` construct `s3fs.S3FileSystem(endpoint_url=...)` at
  IO-manager construction time. s3fs 0.4.2 predates aiobotocore and passes those kwargs straight
  to botocore, raising `TypeError: Session.__init__() got an unexpected keyword argument
  'endpoint_url'` — so an image that resolved that release could not talk to MinIO at all. The
  `>=2023.1.0` floor existed, but on the `orchestrator` extra, which that image deliberately does
  not install; the image named a bare `s3fs` on its own install line, so the floor never bound.
  The floor now sits on **`broker`**, the extra the image does install, and the Dockerfile's bare
  pin is gone so the declaration is the single source. A Dockerfile-only pin is invisible to
  `uv.lock` and to the test suite, which is why this survived.
