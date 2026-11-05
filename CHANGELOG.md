# Changelog

Notable changes to `edgy-dag-tools`, newest first. The import name is `dag_tools`.

This file starts at 0.11.0. **0.10.0 and earlier are not recorded here** — for those, read the
git history between the `v*` tags. Each release is a `chore(release): <version>` commit tagged
`v<version>`.

Downstream repositories depend on this library, so every entry says whether it changes an
existing signature, export or default. Where a default changed, the old behaviour and how to get
it back are named.

## 0.11.0 — 2026-11-04

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
