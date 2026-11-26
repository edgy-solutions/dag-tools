# Changelog

Notable changes to `edgy-dag-tools`, newest first. The import name is `dag_tools`.

This file starts at 0.11.0. **0.10.0 and earlier are not recorded here** — for those, read the
git history between the `v*` tags. Each release is a `chore(release): <version>` commit tagged
`v<version>`.

Downstream repositories depend on this library, so every entry says whether it changes an
existing signature, export or default. Where a default changed, the old behaviour and how to get
it back are named.

## 0.20.0 — 2026-11-25

Cut at `f7ac596`. The release that makes the dlt component refuse what it used to get silently
wrong: a misspelt YAML key (#161), a destination that was never built (#162), two unnamed
pipelines sharing one op name (#163), and two source databases landing in one destination
database (#165). Alongside them: the release assets in a fixed order (#164), DuckDB
`configure`, `pydantic` declared and `staging_use_https` sent to ClickHouse only (#166), and
a docstring (#160). Both entries under Changed refuse something that used to be accepted, and
each says what was wrong with it and what to do.

### Changed

- **`get_destination` now raises when a destination it recognises cannot be constructed, and
  warns when it falls back.** The old body printed the error and fell through to
  `return "filesystem"`, so a snowflake, postgres, clickhouse or databricks destination that
  failed to build **became a filesystem destination**: the pipeline ran, wrote somewhere nobody
  had configured, and reported success. dlt mentions the swap in one line of a log of thousands
  — *"The destination dlt.destinations.snowflake:None in state differs from destination
  dlt.destinations.filesystem:filesystem in pipeline and will be ignored"* — and that line says
  "will be ignored" about the destination the operator actually asked for.

  **This is a behaviour change: a call that used to return a destination can now raise
  `RuntimeError`.** It can only do so where the destination was already wrong, and the exception
  names the drivername and chains the original as `__cause__`. No signature, export or default
  changed.

  **There is no flag to restore the old behaviour, deliberately.** Restoring it means writing a
  load to a destination nobody configured and calling the run green, so a caller that really
  wants that has to say so at its own call site by catching the `RuntimeError` — where the
  decision is visible to whoever reads that code, rather than being this library's default.

  The fallback itself is **kept** for an *unrecognised* drivername — that is documented
  behaviour (duckdb reaches it, which is why six test modules monkeypatch this function) — but
  it now logs a WARNING naming the drivername. Until now a load sent somewhere the
  configuration never named announced itself only by the data's absence from the destination the
  operator had written down. Whether an unrecognised drivername should be refused outright is a
  separate question and is not answered here.

- **`DltAssetGroupConfig` now refuses an unknown key (`extra="forbid"`).** This model is built
  from an operator's YAML with `**pipeline_attrs`, so until now a misspelt setting was dropped
  on the floor and the pipeline loaded with that setting simply absent — no error, no warning,
  and a code-location load that reads as a success. The failures that shape takes are not
  cosmetic: a mistyped `cursor` is a full reload every run, a mistyped `row_filters` extracts
  every row of every table while the YAML reads as filtered, and a mistyped `primary_key`
  changes what a merge writes. Each one looks exactly like a correct extraction until someone
  reads the destination. Refusing at the load is the only moment the operator still has the YAML
  in front of them.

  **This is a behaviour change and it can fail a load that used to succeed.** Every shipped
  example was swept and none of them carries a key the model would refuse (the sweep is a test,
  `test_no_shipped_example_carries_a_key_this_model_would_refuse`). Who breaks, and the fix:

  - **A `pipelines:` block with a typo in any setting** — on `DltPipelineComponent`,
    `RestateDltSyncComponent` or `RestateApiSyncComponent`, or in `dagtools dlt dev`. The load
    now fails naming the key. **Fix: correct the spelling.** The failure is the feature; the key
    was doing nothing before.
  - **`RestateApiSyncComponent` YAML carrying `table_config`, `row_filters`, `schedule` or
    `hints`.** That component pops only `primary_key`, `api_path`, `sources` and `name`, so
    those four keys now reach a strict model and are refused. They have never done anything on
    that component, so the previous behaviour was a block of YAML that read as configuration and
    was not. **Fix: delete them.** If you need table-level configuration on an API sync, say so
    — that is a feature request, not a regression.
  - **Anyone constructing `DltAssetGroupConfig` directly in Python with a keyword it does not
    declare.** The field set is unchanged, so only a keyword that was already being ignored can
    break. **Fix: drop the keyword, or check `DltAssetGroupConfig.model_fields` for the real
    name.**

  No field was added, removed or renamed, no signature changed, and no default changed. The
  `*_dev` loader is unaffected: it already filtered an operator's attrs by
  `set(DltAssetGroupConfig.model_fields)`.

  One consequence worth stating for anyone extending a component: the "pop the component-only
  keys" step in each component's `build_defs` is now **load-bearing** rather than defensive. A
  component that grows a new YAML key must pop it in the same commit, or the load fails — which
  is the point, since the omission used to ship as a feature that silently did nothing.
  `test_dlt_config_forbids_unknown_keys.py` reads each component's pop list out of its own
  source and holds it to an independently written one, in both directions, so that list cannot
  go stale behind the compatibility sweep that depends on it.

### Fixed

- **Each dlt unit now gets its own destination credentials, so a second source database no
  longer lands in the first one's.** `_prepare_units` built one destination credentials object
  for the whole call and then mutated it per unit: when the destination named no `database`, the
  first unit filled it with its own and every later unit found it filled and kept it. A call
  carrying two units from two source databases (AssetKey sources resolved through credential
  providers) therefore sent both to the first database, named both pipelines after it and
  reported it as both units' `dest_database` -- no error, no warning, and a run that read as a
  success. Credentials are now built per unit. A destination that names its `database` keeps it
  for every unit, as before, and a single-unit call is unchanged. No signature, export or default
  changed. Regressions: `test_dlt_per_unit_credentials.py` --
  `test_two_units_with_different_databases_each_write_to_their_own` (rows read back out of each
  unit's own destination), `test_each_unit_names_its_own_destination_database_and_pipeline`, the
  planted-defect control `test_control_one_shared_credentials_object_sends_the_second_unit_to_the_first`,
  and `test_a_destination_that_names_its_database_keeps_it_for_every_unit`.

- **A release's assets are attached in a fixed order, and PyPI waits for all of them.** One job
  creates the release -- `build-package.yml`'s `build`, which generates the notes -- and the chart
  and image-record workflows are no longer started by the tag: each is called from a tag-gated job
  that `needs` the creator, so it uploads into a release that already exists instead of polling
  for one, and a failed upload fails visibly. `publish-pypi` now needs the creator and both
  callers, so a release missing its chart or its image record is never published to PyPI. The
  PyPI trusted publisher is unchanged (it is bound to `build-package.yml`, where the publish stays)
  and no credential is added.

- **`ConfigurableDuckDBIOManager.configure` exists.** It mixed in nothing and defined no
  `configure`, so a deployment that configures its IO managers from a config file -- the way the
  Arrow, Delta, S3 file and SQL managers and `DuckDBResource` itself are configured -- failed at
  the first line of its code location with `AttributeError: ... has no attribute 'configure'`. It
  now mixes in `ConfigureFromDict` and takes the INNER block (its fields are flat, as on the Arrow
  manager); `duckdb` takes a dict of `DuckDBResource` fields or a pre-built resource, and handing
  it the outer `config:` level fails on the missing fields rather than building from defaults.
  Regressions: `test_duckdb_configure.py` -- `test_the_inner_block_configures_the_manager_and_its_nested_resource`,
  `test_the_outer_block_is_refused_rather_than_read`, the control
  `test_the_method_is_the_classs_own_not_the_mixins_stub` (the mixin's own `configure` raises, so a
  class that mixed it in and stopped would pass a `hasattr` check), and
  `test_a_configured_manager_writes_and_reads_back`.

- **`pydantic` is declared as a core dependency.** The pure engines (`contract_mapping`, `audit`,
  `documents.mapping`, `otel_api_sync.spec`, `row_filters`) and every IO manager import it at module
  level, and core did not declare it: it arrived only because dagster requires it, and three extras
  declared it on their own. No floor is set, because none was measured. The exemption
  `test_io_manager_declared_imports.py` carried for it is closed, and
  `test_pydantic_is_a_CORE_dependency_not_an_extras` asserts it is core specifically, since an
  extra would still leave a base install unable to import the engines.

- **`staging_use_https` is sent to ClickHouse only.** `get_destination` set it for every destination,
  and it is a field of dlt's ClickHouse configuration alone, so dlt dropped it everywhere else --
  with a WARNING on filesystem and databricks naming a setting the operator never wrote. The
  default `False` is now set in the clickhouse arm; an explicit value still wins there, and an
  operator who writes the key for another destination still has it passed through. Regressions:
  `test_dlt_get_destination.py` -- `test_clickhouse_gets_the_librarys_https_default`,
  `test_an_explicit_clickhouse_value_wins_over_the_default`,
  `test_no_other_destination_is_sent_the_https_key` with its control
  `test_control_the_recorder_does_see_keys_this_function_adds`, and
  `test_an_operators_own_https_key_is_still_passed_through`.

- **A `DltAssetGroupConfig` with no `name` no longer builds a colliding Dagster op name.**
  `name` is `Optional[str]` with a `None` default, so leaving it unset is inside the contract --
  but it was interpolated raw into the op name, which rendered the literal text `"None"` there.
  Two pipelines over one source schema then built the **same** op name and Dagster failed the
  whole code location with `DagsterInvalidDefinitionError: Detected conflicting node definitions
  with the same name "dlt_None_<schema>_asset"`. The fallback is now the **source database**,
  because `(database, schema)` is the unit mapping's own key and so is unique per unit by
  construction. When `name` is set the expression is byte-identical to before, so every shipped
  component is unaffected -- all three set a name -- and no asset key moves: the string is the
  **op** name, not a key. No signature, export or default changed.

- **`get_destination_distinctions` no longer raises on an absent optional credential field, so
  an asset group no longer vanishes for a correctly configured destination.** It read four
  `Optional[str]` fields unguarded. `AwsCredentials.endpoint_url` is `None` for plain AWS S3 --
  the *ordinary* case; it is set only for an S3-compatible store -- so the ordinary case raised
  `TypeError: expected string or bytes-like object, got 'NoneType'`, and the caller's `except`
  swallowed it into a `print`: the whole group disappeared from the code location, reported only
  as `DLT <name> assets could not be instantiated for <db>.<schema>: expected string or
  bytes-like object, got 'NoneType'`. The AWS arm now **recovers** to `[bucket]`, which is what
  that arm already returns when the endpoint pattern does not match, so nothing that resolves
  today moves. The azure, databricks and connection-string arms **refuse by name** instead:
  what this function returns is spliced into asset keys, `AssetKey` rejects a `None` element
  with a `CheckError` raised far from the field that was empty (measured), and a stand-in would
  name something the destination does not have. The message names the field. No signature,
  export or default changed.


- **`config_to_credentials` no longer raises `NameError` on a snowflake destination.**
  `SnowflakeCredentials` was the one class in that function's seven-way dispatch that was never
  imported, so the snowflake arm raised `NameError: name 'SnowflakeCredentials' is not defined`
  at its first line, before any of the operator's config was read. Nothing in the suite drove a
  snowflake config through this function, which is why a missing import survived every release.
  The fix is one eager import matching the other five. Additive. The new
  `test_every_credentials_class_the_dispatch_names_is_actually_imported` walks the function's own
  AST, so the same gap in a future arm fails here rather than in somebody's deployment.

- **`ConfigurableArrowIOManager` and `ArrowResource` now implement `configure`.** Neither mixed
  in `ConfigureFromDict` and neither defined the method, so a deployment configuring its IO
  managers from a config file — the way every other manager in this package is configured — got
  `AttributeError: type object 'ConfigurableArrowIOManager' has no attribute 'configure'` at the
  first line of its code location. Both now accept the inner config block
  (`ConfigurableArrowIOManager.configure({"fs": {"s3": {"common": {...}}}, "uri_base": ...})`);
  the fields are flat, unlike `ConfigurableSQLIOManager`'s single `config:` wrapper, so the two
  take different levels of the same document. Purely additive: a new classmethod and a new base
  class, no existing field, signature or default touched. **`ConfigurableDuckDBIOManager` has
  the same gap and is not fixed here** — it is reported rather than swept up, because a blanket
  "every `Configurable*` is configurable from a dict" rule would also catch
  `TaggedWarehouseResource`, which legitimately has no such seam.

- **`fsspec` and `universal-pathlib` are now declared on the `orchestrator` and `broker`
  extras.** `io_managers/arrow.py`, `delta.py` and `s3.py` import them at module level and
  `pyproject.toml` named neither: `fsspec` arrived because `s3fs` requires it, and
  `universal-pathlib` (`from upath import UPath`) arrived **only** because `dagster` does. Both
  worked, which is the dependency equivalent of a test passing for the wrong reason — an
  upstream that stopped requiring either would break three IO managers for a reason no
  requirement here names. Additive; no floor is declared on either, deliberately, because no
  release of either has been measured broken here (the `s3fs` floor beside them exists because
  0.4.2 *was*). `test_io_manager_declared_imports.py` reads the module-level imports out of
  those three files, so a new undeclared one fails there instead. Note this does **not** make
  `.[broker]` alone sufficient for those modules: `dagster` and `pandas` are undeclared on that
  extra too, and the user-deployment image supplies them on its own install line.

- `_unadvertisable_reason` no longer promises a `/health` field that has never existed. Its
  docstring said its return values were "stable keys for `/health.adr0044.unadvertised`", and
  `docs/publishing-io-managers-to-the-mesh.md` repeated it. The broker's `/health` has never
  carried that key: the withheld-asset reasons go to a log — one INFO line per asset, then one
  WARNING carrying the whole tally once the registration pass finishes. An operator diagnosing a
  deployment that advertises nothing was sent to the payload, found no such field, and could not
  tell "nothing was withheld" from "I am looking in the wrong place", which is the exact
  confusion that function exists to end. Both texts now say where the tally really goes, and
  `test_broker_unadvertised_tally.py` holds them to the code in both directions: the payload may
  not grow the key behind the prose's back (asserted with the tally non-empty, so it cannot pass
  vacuously), and no prose may claim it again. Whether `/health` *should* serve the tally beside
  the other posture numbers is a live question and deliberately left open — the block's
  membership is not pinned, so a new posture number does not fail this module. Documentation and
  tests only; no signature, export or default changes.

## 0.19.0 — 2026-11-24

Cut at `202987f`. The release that gives a caller a way to ask what it may read, and gives a
deployment as-of readers to select (#153, #154, #156).

### Added

- A deployment names the as-of readers it wants, and a name nothing ships refuses at import
  (`DAG_TOOLS_AS_OF_READERS`). Three things read one table, so there is one table:
  `AVAILABLE_AS_OF_READERS` is what the **build** ships, static and known at import, which is what
  lets the chart's render check answer with no pod; `AS_OF_READERS` is what this **process**
  registered, which only `/health` can know; and the variable names a subset of the first. If any
  of the three grew its own list the three would disagree silently and each one individually would
  look correct.
  - The check sits at **module scope**, beside the env reads, and that placement is the change
    rather than an implementation detail. The broker's `lifespan` creates a background task and
    yields immediately — deliberately, because the Dagster/dlt/datahub imports would blow past
    hypercorn's 60s lifespan timeout — so the pod is Ready before anything has loaded, and a
    refusal raised in that task is swallowed into the task object while the pod is already
    serving. At module scope the module simply never finishes importing. A failed Dagster load
    deliberately degrades instead, because an import failure has a competing cost; a reader name
    nobody ships has none.
  - `/health` reports both sets separately (`as_of_readers_available`, `as_of_readers_registered`)
    and carries **no** `as_of_readers_unknown` key: an unknown name refuses at import, so at
    runtime there is no such state, and a key that is always empty reads as reassurance rather
    than as nothing to say.
  - Every unknown name is reported at once, a refused selection registers nothing so no
    half-applied state exists, and `UnknownAsOfReader` subclasses `ValueError` as
    `MissingCredentials` does. An unset or empty value selects nothing, so the setting ships
    inert. Additive: no existing signature, export or default changes.
- Two as-of readers, one file-backed and one table-backed
  (`dag_tools.domain_broker.as_of_readers`): `s3_parquet` and `postgres`. Neither is registered
  until a deployment selects it through the variable above. Delta and Iceberg still wait, under
  the gate this work was accepted with: no reader ships until it can be exercised against a real
  store of that kind.
  - `s3_parquet` answers a **content identity** — `listing-sha256:<hex>` over the dataset's
    sorted `(key, ETag, size)` listing — and `postgres` answers `pg_current_wal_lsn()` carried
    verbatim. Both are an identity, **not an order**: measured against a live server, `'0/9' <
    '0/10'` is true as `pg_lsn` and false as text, which is why the digest carries a label that
    cannot be parsed as a number.
  - A reader may not **hang** the read either, the half the contract does not cover: a reader runs
    inside the authorize call, so an unreachable store with no bound parks every authorize for
    that backend. Bounds are read once at import (a malformed one warns and falls back rather
    than refusing to import) and the S3 client sets `max_attempts=1`, because a retry inside an
    authorize call multiplies the bound by the attempts.
  - **No ClickHouse reader**, on two independent measurements: the minted user cannot read
    `system.parts` at all (code 497), and a reader holds exactly the consumer's credential by
    design; and `max_block_number` provably misses a mutation (`data_version` 1 → 3 while
    `max_block_number` stayed 2). An as-of that stays equal across a real change is the failure
    that reads as correct.
  - This repository's CI names no secret, so the gate has two halves:
    `dag_tools_tests/test_as_of_readers.py` proves construction and semantics in CI, and
    `scripts/verify_as_of_readers.py` runs the same functions — not a copy — against a real
    postgres and a real S3 server behind an env gate, with every prediction pre-registered at a
    confidence level and every refusal paired with a positive control. P7 was predicted wrong and
    is recorded **wrong** in the file rather than quietly corrected.
  - `test_no_as_of_reader_ships_by_default` is renamed
    `test_no_as_of_reader_is_REGISTERED_until_a_deployment_selects_one`, because shipping a reader
    and registering one are two different sets and the old name described the state this build
    left behind. Test-only rename; additive otherwise.
- `GET /api/v1/assets` lists the assets a caller may read (central gateway). A caller that wanted
  to know has had one way to find out: try a read and see whether it is refused — fine for one
  asset, and for a client building a picker a 403 storm that looks exactly like an attack.
  - A listing may not read a **silence** as a refusal. On the read door an authorizer that refuses
    and one that never answers both deny, correctly, because authz is a gate; here both produce
    the identical empty list, and an empty list reads as a sentence about the caller — *you may
    read nothing* — when the fact is that this gateway could not find out. So the gate body moves
    to `topaz_decision`, returning an `AuthzOutcome` that carries `undecided` and a cause token.
    **`check_topaz_authz` keeps its exact signature and three-tuple** because other repositories
    import it, and the allow computation is byte-identical.
  - `assets.list` is its own route id. The audit line names the route, so sharing
    `assets.authorize` would record a listing as a read authorization, and the delegate seam is
    keyed per route, so sharing would widen every existing delegate row to a route nobody
    granted. The consequence is stated rather than smoothed over: **under `enforce`, a service
    delegated only for `assets.authorize` is refused `not_a_delegate` here until the policy rail
    carries a row for `assets.list`** — a policy fact to flag to governance, never a grant to
    infer in code. `_principal` now takes its route as a required parameter, so the next door
    added here cannot silently record its decisions as the read door's.
  - The body carries no counts and no masking rules: with the authorizer down the undecided count
    equals the size of the routing table, so publishing it would tell every authenticated caller
    how much the mesh holds. The counts go in the `ASSET_LIST` log line. The authorizer also
    volunteers `allowed_columns` and `row_filters`, which are the shape of a read, and their
    absence from the body is measured rather than assumed.
  - Candidates come from the routing table by `SCAN`, never `KEYS`, which makes the answer *what
    may you read right now* and agrees with the read door's 404 on the same absence. The candidate
    set counts **distinct** assets: `SCAN` misses nothing present throughout the iteration but
    does not promise each key once, and a keyspace resize during the walk is exactly when it
    repeats — under the load the cost bounds exist for. The repeat check runs before the cap
    check, so `candidate_limit_reached` still means what it says; measured against the pre-fix
    route, a repeat at a full cap returned **only** the repeated urn and never asked about the
    second, readable asset at all.
  - An unreadable routing table is a 503, because an unreadable table is not an empty one. Three
    cost bounds warn and default rather than refuse to boot, since every value still produces a
    correct listing and crash-looping over a typo trades a slow listing for no gateway. A
    deny-listed caller gets the read door's 403 through a now-shared `_denied_sub`, not an empty
    list.

### Documentation

- `docs/file-backed-asset-to-published-dataset.md` walks the producer path for a **file-backed**
  asset at `4fbd272` (v0.16.1), for the engineer whose deployment is pinned below 0.17: write the
  bytes where another process can reach them, advertise the location from the IO manager, let the
  broker register it, check what it actually advertised, read it back. It names the shipped files
  rather than examples, because this path has no runnable example at either pin, and it says so.
  Its version section states what 0.17 and 0.18 changed on the path, starting with the gateway
  image that could not verify an RS256 token — the default — and reported that as
  `token_invalid`, plus the two ways out of it that need no library upgrade.
  - It also records that `_unadvertisable_reason`'s docstring names
    `/health.adr0044.unadvertised`, a key the payload carries at **neither** pin: the per-reason
    withholding counts are logged and summarised at WARNING, never served. The page tells an
    operator to read the log; `test_file_backed_path_doc.py` fails if the key is ever added, so
    the page is corrected rather than left behind.
  - Linked from the README and held to the files it names, the hook a producer writes and the
    `KNOWN_SOURCE_TYPES == set(_MINTERS)` identity its one reassurance rests on. Documentation
    and tests only: no code, signature, export or default changes.

## 0.18.0 — 2026-11-23

Cut at `0ac0cc3`. The release the fleet pins to get the write-credential fix (#143).

### Added

- The broker refuses, or counts, a ticket it did not mint (`dag_tools.domain_broker.unminted_posture`). ADR-0044's minting is per backend, so a ticket for a backend with no minter was served carrying the **producer's own** credential — a write credential, unscoped to the asset, not expiring with the access window. It was counted and labelled, which is how it was found and also why it sat there.
  - New environment variable `BROKER_UNMINTED_POSTURE`: `observe` (**the default — today's behaviour exactly, plus the numbers**) or `enforce`. Read through the shared `parse_posture`, so an unrecognised value fails closed and is logged carrying only the length of the value.
  - The guard sits at the one seam every resolve path returns through, and **before** the ticket is stamped, so a refused read is not counted as a ticket that was issued. Each path declares whether the broker minted; nothing is inferred from `mode` (the producer's word) or from backend membership (the legacy coordinate path's placeholder token is for backends that *have* minters).
  - An unminted ticket carrying **no** credential is not an exposure and is served unchanged. A minted ticket is never refused.
  - `/health.adr0044.unminted_credentials` publishes the posture plus `would_refuse` and `refused` — two per-backend counts that are never summed, because under `observe` a would-refusal is served anyway. A ticket naming no backend counts under `(absent)`, which reads as a backend nobody can name rather than as a rendering artefact. The counts and the log line sit in separate `try` blocks and `/health` answers nulls rather than raising.
  - **Changed default, consumer-visible:** the gateway turned every non-200, non-404 broker response into a 502, which reached a reader as `MeshUnavailable` — telling them to retry and an operator to look at broker health, both the wrong place. A 403 from the broker now forwards as a refusal and classifies as `NotEntitled`. Only the `error` key travels, and only from a dict. No existing signature or export changes.
- Each pod announces at startup the internal door it serves and the one it calls (`dag_tools.central_gateway.internal_door`). `INTERNAL_DOOR_POSTURE=enforce` with an empty callers list refuses every internal call — `callers=0` is a door that admits **nobody**, and it otherwise looks exactly like a door nobody has called yet. The broker's lifespan announced nothing at all, so `BROKER_RESOLVE_CALLERS`, a list only the broker reads, was invisible on the one pod that reads it until a caller was refused.
  - Two lines per pod, because both pods serve one door and call the other. The served side reports the posture, the caller count, `enforce_ready` and the names of the unset variables; the calling side reports `token_ready` and the unset `MESH_SERVICE_*` names. A pod with none of those set sends its own internal calls bare, which `observe` serves and `enforce` refuses as `no_token` — and the one-shot error in `service_headers` fires only once a call is attempted, which is after the thing it would have warned about.
  - `enforce_blockers` extends `identity.enforce_blockers()`, because the door verifies through the identity module: a full callers list with no accepted issuer admits nobody either, and on the broker this line is the only place those names appear. Counts and variable names only, never values, since these lines go wherever logs go.
  - `helm/dag-tools/values.yaml` carries the block **commented out**, so nothing renders by default. It documents that `BROKER_RESOLVE_CALLERS` belongs on the broker — which this chart does not deploy, so setting it on the gateway configures nothing — and that `MESH_SERVICE_CLIENT_SECRET` is delivered through `envFrom`, never `env:`, which would render its value into the Deployment.
  - Additive: no signature, export or default changes, and `GATEWAY_GAUGE_LOG_AUTOCONFIG=0` still disables the log visibility the announcement establishes for itself.

### Changed

- Four statements in docstrings and comments now say which claims are **intended rather than built**: nothing in this library consults a classification to answer an access question. Comment-only — `components/classification_propagation/component.py`, `components/warehouse_source/component.py`, `io_managers/mesh_publishing.py`. No code, signature, export or default changes.

### Documentation

- `docs/warehouse-table-to-published-dataset.md` walks one warehouse table from an unmeasured read to a curated, contracted, classified, published dataset using only examples this repository ships, and states where those examples stop short. Linked from the README and held to the shipped example directories by `test_operator_path_doc.py`.
- `docs/arch/` records are repinned to the then-current master and the roadmap's rows carry their line citations at that pin.
- `docs/arch/ROADMAP.md` row 3 records the write-credential fix as built in code and names the commit and lines it is built at, and states separately what a `proposed` row means. That row says the fix is in no release, which is true of the tree it cites and goes false with this tag; repinning it is a follow-up, not part of this release.

### Repository

- The lane-trailer check no longer exempts a branch inside `lane/` that matches no row of `.githooks/lanes`, and reads a stacked branch's trailer against its lane. CI only; nothing shipped in the wheel changes.

## 0.17.0 — 2026-11-18


### Added

- `DocumentTablesComponent` (`dag_tools.DocumentTablesComponent`) turns bundles of documents into Lance tables. A bundle is a zip or a directory tree, local or on S3, and each one is a dynamic partition.
  - A private mapping YAML supplies every format-specific detail. The library names no document format.
  - Per bundle, the writer deletes and then appends, so a re-run leaves counts unchanged. `write_concurrency` defaults to 1, through a Dagster pool.
  - Also defined: a discovery sensor and, when `maintenance.cron` is set, a maintenance schedule (both stopped by default), four checks at WARN severity, and a maintenance job.
  - The engine lives in `dag_tools.documents` and does not import Dagster.
  - New command `dagtools documents profile` surveys a source before a mapping is written.
  - New optional extra `documents` (`lxml`, `pylance`, `Pillow`, `boto3`, `pyarrow`, plus `tzdata` on Windows).
  - Additive only: no existing signature, export or default changes.
  - Mapping features: scoped indexes (`scope` on an index and on every lookup into it; a null or empty scope matches nothing and is counted in `null_scopes` / `index_null_scopes`), `prefer` to pick one value out of a list by precedence, and `order: keys` on lookups.
  - `document.huge_tree: true` lifts libxml2's built-in parser limits. A document that hits one is rejected naming that option, told apart from malformed XML by a probe re-parse. `max_bytes` still applies.
  - `document.match` globs are path globs: `*` does not cross `/`, and `**` matches whole segments.
  - Opening a bundle lists only that bundle's prefix; maintenance keeps a scalar index on `source_bundle` and reports it as `source_bundle_index`.
  - See `docs/DOCUMENT_TABLES.md` and `examples/document_tables`.
- Document tables read JSON and gain new mapping features. All are additive: a mapping that worked before loads and produces the same rows.
  - `document.format: json`, with JMESPath expressions and every XML guarantee (compiled once, a fast path held equal to JMESPath, coercion never raises, whole-document commit or reject, `expected_paths`). Values arrive typed; non-identifier keys are quoted; a zero-byte file, `{}`, `[]` or a bare scalar gives no rows and no reject; `NaN` / `Infinity` are rejects; a UTF-8 BOM is accepted. `jmespath` is added to the `documents` extra. The profiler now quotes a JSON key that is not an identifier in the paths it writes (`$.Depot."Dock door"`, where it wrote `$.Depot.Dock door`), so its output pastes into `expected_paths`; identifier keys are written as before.
  - `rows` as a chain (`[{as, each}, ...]`), with `from: <level>` (and `from: root` on any table) on columns and indexes, and `index_of: <level>`.
  - `split` (with `keep_empty`) and `extract` on columns.
  - `basename_in: document_dir | bundle_root` file resolution, and a non-text file reference counted as a `<column>_ref` coercion failure.
  - Member tables: `members: [globs]` with `member:` columns.
  - `nulls` (per column, defaulting to `document.nulls`): strings that mean absent, matched whole after trimming, before split, extract, map and coercion.
  - `const`: a constant column, typed by the column's `type` when the mapping loads.
  - `into`: several tables write one dataset and one asset. Their columns and types must be identical; the dataset's metadata carries `rows_by_table`, and `checks.allow_empty` names the dataset.
  - `only_unreferenced` on a member table keeps only members no file column resolved to, counted as `members_kept` / `members_skipped`.
  - The profiler writes structural paths with the engine's own spelling, and a run whose `expected_paths` differ only in key quoting says so once per bundle.
  - Bundle names containing `[`, `]`, `@`, `$` or spaces work for local and S3 sources.
  - Known limits at this release: **XML and JSON are the only formats**, and **throughput is below the 20,000 rows/s target** -- XML runs at 13k to 16k rows/s and JSON at roughly 70-80% of that rate on the same manifest, so both benches fail under `DAGTOOLS_BENCH=1`. The coercion rules were not loosened to reach the target; whether to build a compiled evaluator is a later decision. A changed bundle that already has a partition is not rediscovered and has to be re-materialised by hand, S3 as a source is tested against a simulated bucket only, and Lance on S3 is unmeasured (keep `write_concurrency` at 1 there).
- `qual init --image-record-template REPO=TEMPLATE` and `--image-record REPO=PATH_OR_URL` (both repeatable) declare where a pin's image record is read from; the manifest moves to schema 6 (`InventoryPin.image_record`, additive; a v5 manifest still loads) and the preflight report to schema 4 (`RepoShaCheck.outcome` gains `record_failed`, plus `image_record_source` and `image_record_problem`). A pin with a declared record fails preflight if the record is absent, unreadable, changed, unparseable or names another commit, with no fallback to tag matching. A pin that declares a record must carry a full 40-character lowercase `git_sha`, or `qual init` refuses it and names the survey stage; pins with no record declared are unaffected. `create_qualification` and `run_preflight` gain keyword-only parameters with defaults; nothing existing changes.

### Changed

- **Behaviour change: preflight identifies a code location by image digest, then `sha-<short>`
  tag, then the legacy stamp, and a location that reports none of the three now FAILS
  `deployed_shas_match_pins` when anything is pinned.** Before, a location with no
  `workspace.yaml` stamp was informational, so a Helm-deployed fleet (the chart cannot emit the
  stamp) could never pass and a deployment with no stamps was indistinguishable from a verified
  one. To keep the old leniency for a location that genuinely has no image of its own, waive it
  in the manifest: `qual init --waive-unverified-location NAME=REASON` (a reason is required;
  there is no Q6 flag, so the waiver is part of the immutable record). With **no** pins the check
  still passes, and its detail now says how many locations it could not verify. The report
  schema is 3 (`location_identities`, `unused_waivers`, per-pin `outcome` / `source` /
  `deployed_digest`) and the manifest schema is 5 (`images`, `unverified_waivers`,
  `image_metadata_key`, `sha_tag_prefix`); both are additive and older readers ignore them. The
  test `test_an_unstamped_location_is_informational_not_a_failure` was replaced by
  `test_an_unverified_location_fails_while_an_unpinned_one_stays_informational`.
- `DagsterGraphQLClient.get_code_locations` skips one malformed `displayMetadata` entry with a
  WARNING naming the location instead of discarding that location's metadata.

- **Behaviour change: `registry read-image-record` and `qual init` now refuse a malformed image record they previously accepted** (an empty image list, a malformed or uppercase digest, a repository carrying a tag or digest, a repository listed twice, a naive `recorded_at`, a missing or unknown `schema_version`), raising `ImageRecordInvalid` (a `ValueError`) listing every problem instead of a pydantic `ValidationError`. `record-image` applies the same rules and writes canonical bytes. The model and `parse_image_reference` moved to `dag_tools.qual.image_record`; the old import paths still work.

- **`limits.page_size` on the SystemLink Test Monitor source loses the ceiling this library invented.** It carried `le=1000` while the server declares no maximum and accepted a far larger `take`, so a deployment that had measured its own server could not configure what it measured: the load failed against a bound naming nothing the server said. Only positivity is validated now (`ge=1`), which is a property of the request rather than a reading of any server. The conservative default is unchanged, so a deployment that configured nothing sees no difference -- this only widens what is accepted.

### Added

- **New `dagtools registry record-image --repo R --sha S --image REF@sha256:<hex>`** (repeatable).
  Writes `image.json` immutably beside the commit's inventory; idempotent for an identical set,
  refuses a different one, refuses a reference with no digest, and never touches `latest.json`.
  `qual init` copies the record into the pin. Also `qual init --image-metadata-key`,
  `--sha-tag-prefix` and `--waive-unverified-location`.
- `dagtools registry record-image --from-file PATH` ingests an image-record document CI already published (exclusive with `--image`; it must name `--repo` and `--sha` exactly).
- **New `dagtools registry write-image-record --repo R --sha S --image REF@sha256:<hex> --out FILE`**
  (`--image` repeatable). Renders an image-record document to a file and touches no storage, so it
  needs no `--registry` and no credential. Refuses, naming every problem at once, a reference with
  no digest, one repository given two different digests, or an empty list, and writes nothing when
  it refuses. This is what CI calls; `record-image --from-file` is what ingests the result.
- **CI records every build's image digests, so the preflight check above has something to match.**
  `.github/workflows/build-containers.yml` now writes one image record per master commit — the
  same `ImageRecord` document `record-image` reads, one document and one parser — and commits it
  to a branch of its own, `image-records`, at `inventory/<repo>/<git_sha>/image.json`. That path
  mirrors the registry's own layout, is derivable from a pin's `repo` and `sha`, and on a public
  repository is readable with no credential:
  `https://raw.githubusercontent.com/<owner>/<name>/image-records/inventory/<repo>/<sha>/image.json`.
  A release additionally carries the record as an `image.json` asset on the GitHub release. A
  record is never rewritten: identical bytes are a no-op, differing bytes fail the job. An Actions
  artifact is kept beside it as a convenience, never as the record. A pull request records nothing.

  Because the document has no field for a service that produced no digest, and a record is never
  rewritten, **nothing is published at all unless every service in the workflow-declared fleet
  produced a usable ref** — so a record naming fewer images than the fleet cannot occur, and "the
  file is absent" and "the file is short a service" are one state rather than two readings of one.
  The fleet rule lives in the new `scripts/image_refs.py`, a committed file rather than a YAML
  heredoc, because a rule in a heredoc can only ever be string-matched and never run. The release
  job builds no document of its own — `recorded_at` makes two renders of one commit different
  bytes — it fetches the commit's record, refuses unless the digest it names for each service is
  what that service's release names now resolve to against the registry, and attaches those exact
  bytes. That is what makes "retag, not rebuild means a release's digests equal its commit's" a
  claim this workflow checks rather than a sentence.

  No library change: no signature, export or default. **No credential was added to CI** — the
  mechanism rides the runner's own `GITHUB_TOKEN`, with `contents: write` scoped to the two jobs
  that write a record.

- **The central gateway measures its own signature support at startup, and a process that cannot verify a signed token is not ready.** The other half of the fault recorded under Fixed below: that entry repairs the image and checks it at build time, this makes a running process say so about itself. At startup the gateway signs and verifies a token with each algorithm in `GATEWAY_JWT_ALGORITHMS` -- a round trip, not a registry lookup, because an algorithm can be registered and broken -- and sorts each into one of four buckets. `unsupported` (the library does not register it) and `failed` (registered, a key minted, the round trip did not come back) make `/ready` refuse, before Redis, because Redis is usually fine at the same moment and comes back by itself where an image does not. `verified` and `unexercised` (registered, and this check knows no key family for it) do not refuse, so a future algorithm cannot make a working gateway unready over this check's own ignorance; neither does a check that threw, which is logged at ERROR instead, because an instrument may never break the thing it measures. A **partial** answer is not ready: a gateway able to verify one of two configured algorithms refuses every token its issuer actually mints. `/health` stays 200 and carries the verdict under `signature_support`, since a pod held out of service answers no probe an operator reads and restarting fixes nothing here. The question is asked **once**, in the lifespan: the library is baked into the image and the algorithm list arrives as an environment variable, populated once at process start, so the answer cannot change under a probe and a fix takes a restart, which re-runs it. Keys are minted in memory for the check alone and are never logged, published or sent, and the check reaches no network -- one that needed the issuer reachable would report an outage somewhere else as an image fault.

  **The refusal vocabulary is unchanged.** `identity.REASONS`, its status mapping and every HTTP status are exactly as before: a token this process cannot verify *is* unverified, and a process-level problem does not belong in a per-token answer. No signature, export or default changes, and a deployment already serving verifiable tokens sees no difference.

- **CI fails when `uv.lock` does not record the version in `pyproject.toml`.** The test
  workflow runs `uv lock --check`, so a version bump that leaves the lockfile behind is
  caught on the commit that makes it rather than on the next, unrelated one. The lockfile
  records this project's own version alongside every dependency's, and `uv lock --check`
  exits 1 when the two disagree. No library change: no signature, export or default, and
  **no credential was added to CI**.

### Fixed

- **The central gateway image rejected every RS256 token.** Its Dockerfile installed bare
  `pyjwt`, and PyJWT's signature algorithms live in the optional `crypto` extra
  (`cryptography>=3.4.0; extra == "crypto"`). Without it `import jwt` still succeeds, the
  container still starts and still passes its healthcheck, and only `HS256/384/512` register —
  so a gateway configured for RS256, which is the default, came up clean and refused every
  token as `token_invalid` whatever its signature, while PyJWT's own explanation
  (`RS256 requires 'cryptography' to be installed`) was swallowed by the verifier's broad
  `except`. A deployment fault in a token fault's words. The image now installs
  `pyjwt[crypto]`, and the `broker` extra declares `PyJWT[crypto]>=2.8` for anyone installing
  the library rather than running the image — note that only the Dockerfile governs the image,
  since the gateway's build context never reads the project file.

  New `scripts/check_gateway_signing.py` verifies a freshly minted, in-memory RS256 token on
  both paths the gateway can take (a key resolved through `jwt.PyJWK` and a static PEM, which
  fail differently), and CI runs it **inside the built gateway image, before the push**. It
  needs no network, writes no key material and adds **no credential to CI**.

  `dag_tools.central_gateway.identity` now also logs, once per distinct state, when a
  configured algorithm cannot be verified by the installed library. The refusal reason is
  unchanged: it is still `token_invalid`, so `/health`'s refusal counts and the HTTP statuses
  are exactly as before. No signature, export or default changes.

- **The SystemLink backfill sizing pass pulled and discarded one row of real data per part.** It asked for `take: 1` with `returnCount`; it asks for `take: 0`, which makes moving nothing a property of the request. It also named `limits.page_size` when the server refused it, while the body carried its own hardcoded `take` -- a refusal sending an operator to change a setting that never appears in the request. It names `returnCount`, the one thing the body sends that a server can refuse.

- **A 400 on a multi-id steps batch named `limits.step_filter` whatever the cause**, so an over-long or-chain sent the operator to change the *form* -- whose only other value a server refusing the form refuses too, a dead end while the right diagnosis was available. The batch is re-issued **once** with a single id, the simplest filter this kind can build: accepted means the length was the cause and the message names `limits.step_batch_default` / `limits.step_batch_max` and the size that failed, refused means the form was and the message is today's. Both arms raise. A 400 is never split the way a 504 is, a single-id batch is not probed because nothing is left to tell apart, and the probe does not count as a batch sent. New setting `limits.step_batch_max` (default 20, the longest or-chain a server has answered); the spike gains an opt-in `--chain-ladder` so a deployment can measure its own and raise it in YAML.

- **Two workflows each attached a file to a tag's release by creating one, so every tag also got a second, empty release.** `generate_release_notes` is set on one of the two writers, so which release those notes reached was an accident of ordering: v0.16.0's went to a draft nobody sees and left the published release with no body at all, and v0.16.1 came out the other way round. A release's body is the one thing on a release that cannot be reconstructed afterwards from its assets. The wheel workflow is now the one creator, because it is the one that generates those notes; the chart workflow waits for that release and uploads into it with `gh release upload`, which writes to the release `gh release view` resolves and therefore cannot mint a second one however the race goes. It waits and then **fails** rather than skipping, because a chart silently absent from a release is what a downstream `oci://` dependency discovers later. The two empty drafts from v0.16.0 and v0.16.1 have been deleted and v0.16.0's generated notes moved onto its published release. No library change: no signature, export or default, and **no credential was added to CI**.

## 0.16.1 — 2026-11-16

### Fixed

- **`GenericOracleAckService` no longer retries a malformed payload for the life of the worker.**
  The handler read its payload and raised a plain `ValueError` *before* `ctx.run`, so the retry
  bound added in 0.16.0 never applied to it: the bound bounds the step, and out at the handler
  Restate retries an invocation that failed on a non-terminal error with no `max_attempts`
  anywhere. A payload with `table_name`, `pk_column` or `record_ids` missing was retried with
  backoff indefinitely, and the journal never said what was wrong. Nothing was damaged, which is
  why it went unnoticed — the invocation simply never ended.

  Payload reading now happens inside the bounded step, and a payload that cannot be acted on is
  `restate.TerminalError` on the first failure. The refusal names every problem at once and
  carries **no value** from the payload, because a `TerminalError`'s message travels into the
  Restate journal and back to the caller, and a payload names a table and a column.

  **One malformed payload used to commit rather than loop.** `record_ids` given as a *string* is
  truthy, sliceable and iterable, so the presence check accepted it and the chunker bound it one
  character at a time — an UPDATE that ran, committed, and flipped whatever rows carried those
  single-character keys. That shape is now refused with its own message.

- **`GenericApiSyncService` and `GenericOracleControlService` are converted the same way, so
  nothing in the library still validates a payload outside its step.** Both carried the identical
  defect and both were a loop, not damage.

  `api_sync.process_record` has one payload and **two** steps, and `_validated` is the first
  statement of *both* closures rather than one call before them: the two steps are separate journal
  entries, so a replay can execute either without the other. It refuses a non-mapping payload, a
  missing or non-text `api_path` / `source_table` / `pk_column`, a `pk_value` that is not a single
  identifier, and an `api_path` that does not begin with `/` — that last one joins the base URL by
  *extending its host*, which fails like a name that does not resolve, on the one step here that is
  deliberately unbounded. **The sub-case that sends rather than loops is `row_data` as a string**: a
  string is valid JSON, so the request would be built, sent and quite possibly accepted — a row
  posted to the API as a bare JSON string.

  `oracle_control`'s `_require` now raises `MalformedPayload` from **inside** the step, and both its
  handlers (`write_mei_request`, `signal_load_complete`) read nothing outside one. **The sub-case
  that commits rather than loops is `mei_values` as a string**: the presence check accepted one and
  `build_request_rows` bound it one character at a time — an INSERT that ran, committed, and filled
  the request table with a row per character, which PDM then reads. `build_request_rows`' three
  refusals are `MalformedPayload` too and no longer quote the payload value they refused.

  **No signature, export or default changed, and the raised type is compatible.** `MalformedPayload`
  is a `ValueError` subclass, so anything that caught the old `ValueError` still catches this one
  (AGENTS §6) — which is why the existing `build_request_rows` tests assert `ValueError` unchanged
  and still pass. What a *handler* raises to its caller is now `restate.TerminalError`, because the
  refusal happens inside the step; that is the whole point of the move.

- **`db_retry` gains `terminal_on_refusal`** (additive), which makes only *our own* refusals
  (`MissingCredentials`, `MalformedPayload`) terminal and classifies nothing else. The HTTP step in
  `api_sync` takes it, and keeps no retry bound. `terminal_on_damage` cannot be used there: it reads
  the driver's codes out of the exception text, matching them as standalone numbers anywhere in it,
  and an HTTP failure quotes the URL it failed on — so `404 Client Error: Not Found for url:
  .../orders/18456` would be read as a SQL Server login failure and the call abandoned on the first
  attempt, which is exactly the retry that step exists for. The connecting step beside it keeps
  `terminal_on_damage` and its bound.

- **A structural seal stops the defect returning in a fourth handler.**
  `test_every_payload_handler_refuses_inside_its_step` walks the handler package, finds every
  `@...handler()` whose last parameter is a payload, and fails if any of them touches it outside a
  step — with a planted-defect control, because a check that sees nothing passes otherwise.

- **`dag_tools/restate_handlers/db_retry.py` gains `MalformedPayload`** (additive; a `ValueError`
  subclass, like `MissingCredentials`), the `PAYLOAD_MALFORMED` reason token, and a `classify`
  verdict of `("payload_malformed", "permanent")`. Permanent, never damaging: retrying a payload
  spends attempts, not the account's failed-login budget. No signature, export or default changed.

### Documentation

Nothing in this section changes code. Each item corrects something the docs claimed and the
library does not have, or the reverse.

- **There is no sensor library and no schedule library here, and `llms.txt` said otherwise.**
  `dag_tools/sensors/__init__.py` is empty, there is no `dag_tools/schedules/` package, and every
  sensor and schedule in this repository is built by a component from that component's YAML. The
  old entry also described the S3 sensor as partitioning chronologically from metadata flags; it
  registers one dynamic partition per object key with the slashes replaced, and *excludes*
  `metadata.json` rather than reading it. Corrected in `llms.txt`, `README.md` and `.cursorrules`.
- **`llms.txt` listed 8 of 13 components and 4 of 5 Restate services.** All are now named;
  `GenericOracleControlService` was the missing one, and `ApiCallPlanService` is marked as the
  VirtualObject it is.
- **`README.md` named no IO manager for Snowflake or Databricks, while its YAML examples use an
  `io_manager_key` of `snowflake_io_manager`.** That key is one a *deployment* binds; this library
  ships `dlt` destination support for both and no IO manager for either, which the README now says.
- **The five decisions that existed only in conversation are written down**, in `docs/rulings.md`,
  each dated with its source and who ruled it.
- **`docs/adr/README.md` says which decisions are recorded here.** Data-infrastructure decisions
  are owned and recorded in this repository; governance decisions are owned by `invincible-agent`,
  stay there, and are cited by repo and commit. No decision is recorded twice. The file previously
  said the opposite. It also states the citation rule: a bare `ADR-NNNN` means this repository's
  series, and every citation of another repository's series names that repository.
- **`AGENTS.md` §4r said the mfg-trace conform stage was deferred; it has landed.** Unbuilt stated
  as unbuilt is the rule, and built stated as unbuilt is the same error pointing the other way.
  The transforms it was waiting on (`normalize` against a normalizer document, `in_set`,
  `coalesce` with a `default`, `nullif`) are shipped, and the two mappings are in
  `examples/mfg_trace/conform/components/`.
- **A number announced before its record existed is not a citation, and one had moved.** The
  provisional `ADR-0066` for this work was never allocated, so the three references to it in
  `dag_tools/contract_mapping/` are reworded rather than renumbered.

## 0.16.0 — 2026-11-16

### Added

- **A curated slice for manufacturing test data: two conformed contract documents, a pure
  measurement grammar, and a shipped dbt package that derives three curated models from them.**
  Nothing here is a new signature or a changed default; it is all additive.

  - `dag_tools/systemlink/measurements.py` — a **pure** grammar (no Dagster, dbt, dlt, duckdb,
    sqlalchemy or jinja2 import) that reads a measurement's text and its limits text and answers a
    number, a low or high limit, a comparison code and two flags. One rule table, `RULES`.
  - `dag_tools/systemlink/sql.py` — a generator that reads the same `RULES` and emits the package's
    SQL. `PACKAGE_DIR` names the shipped directory, `REQUIRED_VARS` the four variables a deployment
    must set, `GRAINS` and `ORPHAN_EDGES` the contract tests it generates.
  - `dag_tools/dbt_project_packages/mfg_trace_curated/` — the package itself: three models
    (`test_result`, `test_measurement`, `test_catalog`), a grain-uniqueness test per model, one
    reference test, an uncovered-comparator count, and one hand-written macro. Installed with a
    `local:` entry in a deployment's `packages.yml`; ask the library for the path
    (`python -c "from dag_tools.systemlink import sql; print(sql.PACKAGE_DIR)"`).
  - `examples/mfg_trace/` — the two conformed contract documents, a worked `curated/` dbt project
    with a **sample comparator table**, and a README covering both stages.

  **The comparator table is the deployment's, as the normalizers are.** The library ships no
  comparison codes; it ships the two words `low` and `high`, named after the curated contract's own
  limit columns. A row is `{text, code, bound}` and matching is on the **whole** operator, never a
  prefix: a table carrying `<` does not cover `<=`, and reading it as `<` would publish the right
  number on the right side with the wrong strictness. An operator the table does not carry is
  **kept as text and counted, never dropped and never read as a value** — it publishes no limit,
  the received text is still there to audit against, and the package's `uncovered_comparator` test
  reports it at **warning** severity, because a build that refuses to publish over an unfamiliar
  operator puts the pressure on deleting the row rather than on adding one to the table. It is not
  a parse failure; that flag says a number was expected and not found.

  **`is_latest` is terminal-only.** A non-terminal attempt is never the latest result, whatever its
  sequence number, and a unit with no terminal attempt has no latest row. Tolerance arithmetic runs
  in the warehouse's own exact numeric type, which is one of the four required variables: through
  binary floats a `1.005 ± 0.005` tolerance gives limits that are both wrong and both display as
  right.

  **The package declares a default for none of the four variables.** Two relations, the exact
  numeric type and the comparator table are each a thing this library must not guess, so one left
  unset fails `dbt parse` rather than resolving to something.

  **Stage 1 — the mapping from a source's raw tables onto the two conformed contracts — now
  ships too**, in `examples/mfg_trace/conform/`: one `contract_mapping` component instance per raw
  table. It needed three transforms this library did not have, and they are below. Stage 1 is the
  one place anything from below is named, and it is a file an operator edits when a source renames
  a field, not a library release.

- **Three more contract column types: `timestamp`, `date` and `array<string>`.** The three a
  conformed vocabulary needs, and the three where a lenient reading is indistinguishable from a
  correct one. `timestamp` is carried as UTC and **refuses a value naming no offset**: a naive
  value names no instant, so reading it as UTC shifts every row of a non-UTC source by hours and
  the result still looks like a timestamp. `assume_utc: true` on a column mapping is how a
  deployment declares its source really is UTC, and it is refused on any other type, because a
  knob that says nothing must not look wired up. `date` reads `YYYY-MM-DD` only — a datetime is
  refused rather than silently accepted at midnight, which would pass until the first row carrying
  a time, and the shape is pinned before parsing because `date.fromisoformat` also takes `20240131`
  and week dates. `array<string>` refuses a bare string (iterable, and it would explode into its
  characters) and a set (whose order is not part of its value). A number is refused for both date
  types: epoch seconds and epoch milliseconds are indistinguishable in range for recent dates.
  Purely additive — the five existing types are unchanged.

- **Four more transforms, and a source-block row filter, on `contract_mapping`.** Each exists
  because a source disagrees with a contract about exactly one thing.

  - `nullif` — the value that means absent becomes a real null. A source writing `''` or a
    far-future sentinel instead of a null is the ordinary case, and a reader cannot tell a
    sentinel from data.
  - `in_set` — a membership test over the source's own enum, which is how a boolean flag gets
    derived from a status vocabulary this library may not name. Type-changing, so the mapping
    declares a cast template for the result.
  - `normalize` — a composed, declared rule: `trim`, `upper`, `lower`, `strip`, `replace`,
    `lstrip_zeros` and `split_revision`, applied in order. The single-step case folds could not be
    composed, and a real normalizer is a rule, not a case fold. The rules live in a **normalizer
    document** (`normalizers.path` or `normalizers.url`, the contract's own topology), so the
    identifier grammar is a file an operator edits, not a library release. A `*_raw` companion
    column is the mapping author's own `copy` — nothing generates one, because what to keep
    verbatim is the author's decision.
  - `run_timestamp` — one instant for a whole load, taken from the caller
    (`apply_to_rows(..., run_timestamp=...)`, keyword with a default, so no existing caller
    changes) and from the mapping's declared `run_timestamp_sql` on the SQL route. Every row of
    one load carries the same value; a per-row clock reading would make rows of one load
    distinguishable by when they were written rather than by what they are.
  - `source.where` — a row filter on the source block, on the same shared grammar the dlt
    component's `row_filters` uses, so a filter means one thing in this repo.

- **`table_config`, `row_filters`, a row-count check and `ingest_runs` on `DltPipelineComponent`.**
  `table_config` carries a table's `primary_key` and `history: scd2`; the key is emitted as a
  `merge_key`, so a key absent from a whole load is **not** retired. `history` together with
  `cursor` fails the load naming both. `row_filters` expresses an extraction-side filter in six
  operators. The row-count check asserts a table was not loaded empty. `ingest_runs: true` writes
  one row per run, failed or not, carrying the **root** error's class name and a digest of its
  message, never the message — an exception string routinely carries a DSN, a row of source data
  or a token, and that table is as readable as any other in the warehouse. All four are opt-in;
  a pipeline that declares none behaves exactly as before.

- **A contract `path` may now hold a BUNDLE — several documents in one file.** A deployment
  governs its contracts together, which is how this repo's own example ships them: one file, one
  document per table, separated by `---`. The component read a contract with `yaml.safe_load`,
  which refuses such a stream outright, so it could point only at a single-document file — or at a
  **copy** of one document, which is a second governed text needing a drift test to hold it to the
  first. `expect_contract_id` now **selects** the document from a bundle, and a bundle with no id
  named **fails the load listing the ids it holds**: taking the first document would map against
  whichever happens to be first, and every column name might still line up, which is the failure
  that field exists to catch on a mis-edited path. One field rather than two, deliberately — a
  separate selector would say the same fact twice and would make `expect_contract_id` vacuous on
  the bundle route, since the selection would guarantee the match. A single-document file behaves
  exactly as before, and there the id stays a check.

- **A synthetic source fixture for the example, built by a script rather than committed as a
  binary** (`examples/mfg_trace/bw/`): a second `DltPipelineComponent` instance over a sqlite
  stand-in for a versioned, time-dependent source. Five things the extraction side has to get
  right are planted on their **own keys**, beside a control key that trips nothing — without the
  control, an accidentally-broken fixture filters or refuses every row and reads as a full set of
  successful cases. Runs with no warehouse and no credentials.

### Changed

- **BREAKING (default removed): `filters.terminal_statuses` is now required on the
  `systemlink_test_monitor` source kind, and the library ships no default for it.** It was
  `["PASSED", "FAILED", "ERRORED", "TERMINATED", "TIMED_OUT", "DONE"]` in
  `dag_tools/systemlink/spec.py`, which made one server's vocabulary every deployment's answer to
  the question that decides when a result is complete and its steps are fetched. A server that
  spells any of those differently was reading as a clean extraction that silently collected no
  step. The `filters:` block is therefore required too, since it carries the field; the refusal
  names `terminal_statuses` rather than leaving an operator to find out which field inside the
  block has no default.

  An **empty** list gets its own refusal, because it satisfies a required-field check and means
  nothing is ever terminal: no step would ever be fetched, every result would be re-read on every
  run, and nothing would report any of it.

  `filters.failed_statuses` keeps its list but is now required where it is actually read — under
  `filters.step_detail_for: failed_only`, non-empty. Requiring it unconditionally would force
  every operator to write a list nothing reads.

  To get the old behaviour back, state the old default in your YAML:

  ```yaml
  filters:
    terminal_statuses: [PASSED, FAILED, ERRORED, TERMINATED, TIMED_OUT, DONE]
  ```

  Read it off your own server first; it is a sample, not a recommendation. Nothing in this
  repository was deployed on the default. `examples/mfg_trace/systemlink/component.yaml` carries
  the sample list, and `examples/mfg_trace/README.md` no longer claims the library holds one. No
  signature or export changed.

- **`coalesce` now counts ARGUMENTS, not source columns, so one source plus a declared `default`
  is accepted.** Such a mapping was refused by the arity table while **both** routes already
  implemented it and agreed on it — the row route falls back to the column's `default` when every
  value is null, the SQL route appends it as the last `coalesce` argument. Nobody writes two
  matching route bodies for a shape they meant to refuse, so the arity rule was the mistake rather
  than the feature, and the old message sent an operator to add a second source they do not have.
  A bare one-argument `coalesce` with no fallback is still a no-op and is still refused. `concat`
  is deliberately **not** relaxed alongside it: it has no fallback to count, so a one-source
  `concat` really is a no-op, and a later edit symmetrizing the two because they share the `2+`
  spelling would be wrong. This only accepts documents that previously failed the load.

  The same change closes a two-route divergence of the same class, and moves it to the **load**:
  a `default` the contract's column type cannot take was refused by the row route and emitted as a
  bare literal on the SQL route, where the warehouse either errors or silently coerces — and which
  it does is not this library's to decide. The check calls the row route's own acceptance test
  rather than copying its rules, since a second copy would be the divergence it exists to prevent.
  **Default change**, in the sense that such a mapping now fails the code-location load instead of
  producing SQL whose behaviour depended on the warehouse.

- **The two conformed `mfg_trace` contract documents now declare the grammar rule ids their own
  emitted tests cite**: `null_value`, `wrong_type`, and `non_finite` on the one document carrying a
  float column. `contract_mapping` emits tests under seven ids of its own and the template declared
  two, so `tests_under_undeclared_rules` counted the rest — which is exactly the gap that number
  exists to report: a failure naming an id outside the contract's `rules:` cannot be acted on by
  anyone reading the contract. `non_finite` is declared on the result document only, because the
  step-parameter document's one numeric column is an integer and declaring a rule nothing can trip
  is the mirror vacuity. **This template is dag-tools' published interface**, so a deployment's
  registered superset of it needs the same three ids — the same class of coordination as
  `produced_since`.

- **Key uniqueness now resolves to one rule id, `duplicate_key`, instead of a spelling derived
  from the contract's grain.** The derived id (`duplicate_<grain columns>`, falling back to
  `duplicate_grain`) reproduced whatever a contract happened to call its own key rule, so the id
  a failing test named changed from table to table: two contracts over the same data could not be
  read against each other, and a reader who learned the rule once had to learn it again next door.
  `constant_within` keeps its per-column derivation — there the column is what the rule is about.
  **Default change.** A contract that declares the derived spelling and not `duplicate_key` now
  has its key test counted among the tests under undeclared rules. To keep the old id, set it in
  the mapping: `rule_ids: {key_unique: <the contract's spelling>}`. The shipped
  `examples/contract_mapping` mapping does exactly that, because its contract is vendored
  byte-identical to upstream and is not edited here. No signature or export changes.

- **A contract whose `scope` is the word `none` now reads as having no scope, rather than as a
  column called `none`.** YAML reads that word as a string — only `null`, `~` and an empty value
  are nulls — so such a document arrived as a column name and the load refused because nothing
  produces a column called that: a refusal naming something the operator never wrote. The word is
  read case-insensitively, and a blank scope now reads as absent too, so `None` is the one
  representation of absence. Deliberately **only** `scope`: a `none` in `period.column` or
  `status_period` still means what the document said and still fails loudly, because silently
  dropping a period a reader filters on is worse than a refusal. No existing signature, export or
  default changes; the only documents whose behaviour changes are ones that previously failed the
  load. A contract whose scope column is genuinely called `none` can no longer say so.

### Fixed

- **`<prefix>ingest_runs.error_digest` on the SystemLink kind digested the wrong exception, so two
  runs of one identical failure never matched.** The wrapper digested `str(failure)` — the
  exception dlt hands over, whose message embeds a per-run load id — so the column was different
  on every run and could not do the one job it exists for: letting two rows recording the same
  failure be recognised as the same failure. The shared `ingest_runs.error_digest` had already
  been corrected for this; the SystemLink wrapper kept a local copy of the old form.

  It now digests the exception the row **names**. `error_type` here deliberately reports the
  engine's own error from the cause chain rather than the innermost one, so the two are derived
  from one function (`_recorded_error`); deriving them separately is how a row ends up describing
  two different exceptions. New helper `ingest_runs.message_digest(exc)` digests one chosen
  exception — additive, and `error_digest(failure)` is unchanged, now expressed through it.

  **Boundary, deliberately accepted:** digests written before this release are not comparable
  with digests written after it, for the same failure. The table is young and no deployment is
  grouping on this column yet; the alternative was keeping a column that could never match.

- **A Restate retry of a failed database login no longer locks the account out.**
  `ctx.run` retries whatever raises, and a failed login is not a free operation: every
  attempt spends one of the account's failed-login attempts, so an unbounded retry of a
  wrong password is itself what locks the account — and the lockout then takes down every
  other consumer of that account, including the ones whose password was right all along.
  Oracle's `FAILED_LOGIN_ATTEMPTS` defaults to 10, and sites that have tightened it are
  common.

  New module `dag_tools/restate_handlers/db_retry.py`, applied at the five `ctx.run` steps
  that open a database connection: `oracle_ack.mark_as_processed`,
  `oracle_control.write_mei_request` and `.signal_load_complete`, `api_sync.process_record`
  and `sap_induction`'s local-database update. The HTTP steps beside them are untouched —
  they connect to nothing and retrying them costs nothing but time.

  Two mechanisms, both needed. **Classification** makes a recognised failure terminal on its
  *first* attempt, so a wrong password causes one login attempt rather than a loop. **A retry
  bound** (`max_attempts=3`, under the default threshold with room to spare) is the backstop
  a code list cannot be: an authentication error nothing recognises would still loop forever
  without it. The classifier reads `str(exc)` and so imports no driver — `oracledb` and
  `pyodbc` both put the vendor's code in the exception text, and SQLAlchemy repeats it
  because it wraps and quotes the DBAPI error.

  **Changed behaviour at the call site:** a classified failure now surfaces as
  `restate.TerminalError` rather than the driver's own exception, because that is what stops
  Restate retrying. Its message carries the code and a fixed sentence and **nothing** out of
  `str(exc)` — a connection error quotes the connect identifier and sometimes the account
  name, and a `TerminalError`'s message travels into the Restate journal and back to the
  caller. The original is kept as `__cause__`, where the worker's log already had it. A
  failure the classifier does not recognise is re-raised unchanged and merely bounded.

  Absent credentials are terminal too, and the new `db_retry.MissingCredentials` replaces the
  bare `ValueError` the handlers raised for them. It is a **`ValueError` subclass**, so
  anything catching `ValueError` keeps working. The reason it is terminal is the same reason
  the `_connect` docstring needed correcting: that docstring claimed reading the environment
  per call meant "a rotated secret should take effect on the next invocation, not the next
  pod restart", which is not true of a secret delivered as an environment variable —
  `os.environ` is populated once, when the process starts. That is how a worker ends up still
  presenting the old password while the rest of the deployment has the new one, and then
  retrying it until the account locks. A secret that really updates in place has to arrive as
  a *mounted file* and be read from that file.

  Tunable without a release: `DAGTOOLS_DB_MAX_ATTEMPTS` and `DAGTOOLS_DB_MAX_RETRY_SECONDS`.
  A value of zero, a negative number or anything unreadable falls back to the default rather
  than removing the bound — unbounded is the state this change exists to leave.

- **`oracle_control.build_request_rows` de-duplicates on the whole row, not on the
  identifier.** **This changes a default.** Two rows sharing an identifier — the same item at
  two versions — were collapsed to one, and the collapse was silent: no error, no warning and
  no count, so the first place the loss could be noticed was in the output. Two such rows are
  two different requests, and dropping the second is not a saving but a wrong answer. The
  comparison is now over the whole row, and it runs *after* the rows are squared up to a
  common column set, so a row that merely omits a column the others carry is compared as the
  row it becomes rather than as a distinct one.

  Uniform input is unaffected: an exact repeat still collapses, which is the saving the old
  behaviour was reaching for. The old behaviour is not restorable by configuration and
  deliberately so — a caller that wants one row per identifier should pick which one it wants
  before calling, rather than have the library drop whichever happened to arrive second.

- **A `duckdb:///absolute/path` dev destination resolves to an absolute path again.**
  `dag_tools.dlt_dev`'s `--destination` resolution stripped every leading slash, so on
  POSIX the absolute spelling the refusal message itself offers came back relative: a dev
  run landed a DuckDB file beside the current directory and reported success against the
  wrong one. The two meanings of that slash are now told apart -- before a drive letter it
  is the URL's separator and goes, otherwise it is the first character of the path and
  stays -- which is why Windows never saw the bug. The existing coverage asserted only
  that a spec resolved to something, so the path itself is now pinned.
- **The `mfg_trace` contract template spells its key-uniqueness rule `duplicate_key`**, the word
  the plan already uses for that refusal kind, instead of `key_not_unique`. Template and planning
  documents only — no governed contract and no library vocabulary changes. Note that neither
  spelling is an id this library's key-uniqueness test resolves to (it derives
  `duplicate_<grain>` and falls back to `duplicate_grain`), so those tests still land under an
  undeclared rule until a mapping overrides `rule_ids.key_unique` or the document also declares
  the generic id.

## 0.15.0 — 2026-11-14

### Fixed

- **An `observe`-posture refusal is now logged by the name of the reason it was actually
  refused for.** `IDENTITY_WOULD` reports what `enforce` *would* have done, which is
  `_decided`'s answer — and `_decided` cannot produce `no_principal`. That reason is minted by
  the observe arm of `resolve_principal`, after the line has already been emitted, and the route
  logs nothing when it refuses (it raises). So a request refused for naming no principal reached
  the caller as a 401 saying `no_principal` while its one log line named a *different* reason,
  usually `no_subject`. A log describing a decision that was not made is worse than one with a
  gap in it: the gap is visible.

  `observe` now emits an additional `IDENTITY_REFUSED` line, at WARNING, naming the refusal that
  was actually made. `IDENTITY_WOULD`'s contract is **unchanged** — it still answers "what
  happens when the posture flips", and the two lines naming different reasons for one request is
  correct, not a disagreement. `enforce` is unaffected: there the refusal is one decision and
  `IDENTITY_AUDIT` already carries it, so no second line is emitted. No signature, export,
  default, status code or response body changes.

### Added

- **A `systemlink_test_monitor` source kind** for `DltPipelineComponent`: a read-only, incremental
  (known-ids) and resumable-backfill load of SystemLink Test Monitor results and steps, with an
  `ingest_runs` table and three asset checks. Selected only by `source_config.type`; no existing
  YAML, signature, export or default changes. Its schedule targets `<pipeline>_incremental`.
  `operator` and `table_prefix` are required for this kind. Settings the server decides
  (`limits.page_size`, `incremental.order_by`, `limits.step_filter`, ...) ship as conservative
  defaults a deployment overrides in its own YAML. `resolve_platform` gains a `VENDOR_PLATFORMS` table consulted
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
- **Refusals are counted by reason name, in two separate populations**
  (`dag_tools.central_gateway.refusal_gauge`), served on `/health` under a new
  `refusal_reasons` key. `would_refuse` is what `enforce` would have refused with the request
  **served anyway** — the migration-readiness number, broken out by what breaks. `did_refuse` is
  what the gateway actually refused, where the caller got a 4xx. Under `observe` only
  `no_principal` can appear in `did_refuse`; under `enforce` the two are one decision, so a
  refusal counts in both. They are never summed: one number would report refusals for requests
  that were served, or the reverse.

  Every reason is present at 0 from start-up, and the vocabulary is installed by `identity` from
  the mapping that already gives each reason its status code (`identity.REASONS`), so a reason
  added there is counted the day it is added rather than the day it is first hit. `observed`
  rides beside the counts as the discriminator between a quiet gateway and a dark gauge.
  Counts are per process and since start-up; nothing persists or aggregates.

  **It refuses nothing.** The counter runs in its own `try/except`, separate from the log line's,
  so neither instrument can silence the other, and `/health` answers `null` for a misbehaving
  counter rather than 500ing and taking the pod out. No signature, export or default changes;
  `/health` gains one key and changes none.


### Changed

- **The vendored EV contract is re-vendored from its governed copy**
  (`examples/contract_mapping/contracts/ev_fixture.yaml`), which has gained one rule id:
  `varies_within_scope`, the scope-level case of a broken `constant_within` that
  `varies_within_path` states at path level. The refusal fixture
  (`dag_tools.user_deployment.demo_ev_refusal`) gains the matching planted case and scope, so
  every rule in the document still has exactly one case. A reader that enumerates the contract's
  rule ids sees one more id; no signature, export or default changes.

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
