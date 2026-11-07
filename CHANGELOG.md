# Changelog

Notable changes to `edgy-dag-tools`, newest first. The import name is `dag_tools`.

This file starts at 0.11.0. **0.10.0 and earlier are not recorded here** — for those, read the
git history between the `v*` tags. Each release is a `chore(release): <version>` commit tagged
`v<version>`.

Downstream repositories depend on this library, so every entry says whether it changes an
existing signature, export or default. Where a default changed, the old behaviour and how to get
it back are named.

## Unreleased

### Behaviour change

- **A RUNNABLE class with no round-trip coverage now blocks GO in `dagtools qual report`.**
  Previously Q6 gated only on the Q2 run and synthetic probes. It now also requires the Q2
  IO round-trip probe (below) to have PASSED on both sides with the same loaded type and
  columns. Existing qualifications that were GO will report NO_GO until the probes are
  generated and run. To get the old verdict back, pass `--accept-roundtrip-missing`, which
  excuses missing coverage only; a probe that failed on one side or loaded a different shape
  blocks regardless. To clear it: `dagtools qual synthetic`, deploy `dag-tools-probes`, then
  re-run `dagtools qual run` per side. Classes with no IO manager class, and dbt classes, are
  listed as not applicable and never block. Verdict schema 2 -> 3 (additive).

### Added

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
