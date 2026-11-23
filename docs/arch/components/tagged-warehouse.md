# Tagged warehouse (CO-TAGGED-WAREHOUSE)

The tagged warehouse component contributes one Dagster resource and no assets. The resource runs SQL against a warehouse and attaches an attribution tag to every query. A query that cannot carry its tag is not sent. The [read audit](read-audit.md) reads those tags back out of the warehouse's query history. The [warehouse source](warehouse-source.md) component reads through a resource with the same two read methods this one offers, under a key the deployment names.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the dag-tools pin listed there (`503607d`). The record shape is provisional.

## Findings

1. **It contributes a resource and nothing else.** The component returns definitions that hold only the resource, under its resource key. That key has a default, and a deployment may set its own. `built`
2. **A query without a tag is refused before any connection is made.** Building a plan refuses an empty tag. Running a plan checks again that it carries one. That second check is in the function that receives an open connection, at both places that open one, so it stops an untagged statement being sent rather than the connection being made. The resource resolves the principal before it resolves the connection, so a bad connection cannot hide a missing principal. The reasons are in AGENTS.md §4j. `built`
3. **The tag names a principal, a run and, when known, an asset.** On its own the resource tags a pipeline principal and the run id. `for_asset` adds the asset key. `for_user` sets a user principal from the identifier it is handed. The tag is sorted JSON with a length cap and a schema version. It carries no signature. `built`
4. **`for_user` changes the tag, not the credential, and does not check the identifier.** A read made for a person runs on the same warehouse connection as a pipeline read. The resource writes whatever identifier it is given into the tag. Whether that identifier was verified is the caller's responsibility. `built`
5. **The tag travels in a comment or through a session statement the operator writes.** In comment placement, the tag leads the statement, and its colons are escaped so SQLAlchemy does not read them as parameters. In session placement, the operator's own statement runs first with the tag as a bound parameter. That statement must bind the tag, must be one statement and must carry no comment marker; a statement that fails this fails the load. A tag that itself contains a comment marker is refused in comment placement, so it cannot close the comment. The library ships no vendor statement. `built`
6. **The connection is named in config and checked at load, with no environment fallback.** `connection` must give a DSN, or a driver name with parts. It is resolved to a URL during validation, which builds no engine and imports no driver. No environment variable stands in for a missing connection. `built`
7. **Nothing limits what a statement does.** The resource runs any statement it is given inside a transaction that commits. It adds no read-only setting, no row limit and no timeout. `built`
8. **Each query builds a new engine, and none is disposed.** Every plan creates an engine from the URL and does not dispose of it, so connection pools are not reused or closed by the resource. `built`
9. **The chart and the shipped code location do not deploy it.** No chart template and nothing in the user code location wires this component. A deployment adds it in its own code location. `built`

## Where it exists and what its example proves

Commits are named in full. A release is a version tag whose release page carries the built wheel. The wheel holds the component and not its example, which stays in the repository under `examples/`. Each `measured` row cites the Tests workflow run 44451597250 at the records pin. Every test such a row names ran in process and passed in both test jobs: the current Dagster and the 1.10.19 compatibility floor. A row saying what the example does not prove, or what a deployment must still configure, describes a condition of the code and carries that condition's status. A value a deployment must configure is required or has no default, and neither the chart nor the shipped code location supplies it. Supplying it is the deployment's job, not a remedy, so no roadmap row follows from it.

### Commit and release

| # | Claim | Evidence | Status |
|---|---|---|---|
| 1 | The component and its example were first committed at `cee7df91373e437941f3760f52a4cd7ee9735f04`. | the commit that adds `dag_tools/components/tagged_warehouse/` and `examples/tagged_warehouse/` | `built` |
| 2 | Merged to master by PR #20, merge commit `5995b5d09c5dd65ef18cbe6833bc594568c314ed`, on 2026-11-06 (UTC). | the pull request's merge commit; master contains it | `built` |
| 3 | First released in v0.12.0, tag at `1b149e2135aef9b8349d44c09442a6f734dd3873`, published 2026-11-09. Also in v0.17.0, tag at `a85c5f99233962a8c34e6eb0f399c23030865ee4`, the most recent release, and at the records pin. | each tag contains the merge commit; the component's module is in each release's wheel | `built` |
| 4 | A component file names it as `type: dag_tools.TaggedWarehouseComponent`; the package root exports the class. | `dag_tools/__init__.py:12, :32` | `built` |

### What the example proves, and what it does not

| # | Claim | Evidence | Status |
|---|---|---|---|
| 1 | The example loads as a code location whose one resource is this component's and whose one asset is the read audit, under the key its component file names. | Tests run 44451597250, both test jobs; `test_tagged_warehouse_component.py`: `test_the_shipped_example_loads`, `test_the_component_contributes_a_resource_and_no_assets`, `test_the_resource_key_in_the_yaml_is_the_key_the_resource_lands_under` | `measured` |
| 2 | The read audit runs unchanged over this resource, by its key, against a local sqlite file with no credential. It explains one read by asset key, reports one untagged read, and reports one read carrying another tool's tag as not a pipeline. | Tests run 44451597250, both test jobs; `test_tagged_warehouse_component.py`: `test_the_resource_drops_in_under_an_unchanged_read_audit` | `measured` |
| 3 | The tag in the example's history rows is the tag the shipped resource builds, so the writer and the reader of the tag cannot drift apart. | Tests run 44451597250, both test jobs; `test_tagged_warehouse_component.py`: `test_the_tag_in_the_fixture_is_the_tag_the_resource_sends` | `measured` |
| 4 | A session statement that does not bind the tag, and a connection that names nothing, each fail the code location's load rather than a run. | Tests run 44451597250, both test jobs; `test_tagged_warehouse_component.py`: `test_a_tag_statement_without_the_bound_tag_fails_the_load_not_the_run`, `test_a_connection_naming_nothing_fails_the_load` | `measured` |
| 5 | The example writes its environment references in the form the 1.10.19 floor resolves. | Tests run 44451597250, both test jobs; `test_tagged_warehouse_component.py`: `test_the_example_names_env_vars_the_way_the_floor_resolves_them` | `measured` |
| 6 | Not proved: that a warehouse records the tag in its own query history. sqlite keeps no query history, so the history the example's audit reads is a fixture, and no test runs the resource against a warehouse that keeps one. | `examples/tagged_warehouse/seed.py`; no such test in `test_tagged_warehouse_component.py` or `test_tagged_warehouse_resource.py` | `built` |

### What a deployment must still configure

| # | A deployment must still configure | What the example uses instead | Evidence | Status |
|---|---|---|---|---|
| 1 | The tag placement its warehouse supports; for session placement, the warehouse's own statement that binds the tag. The library ships no statement for any warehouse. | comment placement | Settings `tag.placement`, `tag.statement`; `examples/tagged_warehouse/dagster_home/components/warehouse/component.yaml:22` | `built` |
| 2 | A connection to the warehouse, as a URL or in parts, and the warehouse account it signs in as. | a local sqlite file | Settings `connection.*`; `examples/tagged_warehouse/dagster_home/components/warehouse/component.yaml:49` | `built` |
| 3 | The pipeline name that becomes the tag's principal, or a `for_user` call on every read. | a pipeline name of its own | Settings `principal.pipeline_name`; `examples/tagged_warehouse/dagster_home/components/warehouse/component.yaml:33` | `built` |
| 4 | A `for_asset` call in each asset whose reads should be attributed to it, and a `for_user` call with the verified identity on a read made for a person. Without `for_asset` a read is tagged with its run only, which the audit resolves only when the run selected one asset. | nothing; the example's only asset is the read audit, and the README shows the call | `examples/tagged_warehouse/README.md:60`, `:66`; Findings | `built` |
| 5 | Its own code location wiring the component. | the example's own code location | Findings, item 9 | `built` |

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `tag.placement` | required | `comment` or `session` |
| `tag.statement` | unset; required for `session`, refused for `comment` | the operator's statement that sets the tag; must bind `:tag` |
| `principal.pipeline_name` | unset | the pipeline principal; without it each read must be made through `for_user` |
| `connection.dsn` | unset | a full connection URL |
| `connection.drivername`, `host`, `port`, `username`, `password`, `database` | unset; a driver name is required with parts | the connection in parts |
| `resource_key` | has a default | the key the resource is registered under |

## Gaps

- **A read made for a person uses the pipeline's credential** (finding 4). The warehouse sees one account for every reader, and the tag is the only record of who asked. Remedy: [roadmap row 1](../ROADMAP.md) (not filed) covers verifying the principal. Nothing on the roadmap gives a person's read its own credential here. Status: `built`.
- **The tag is unsigned** (finding 3). Anything with a connection can write a tag that names a pipeline. Remedy: [roadmap row 17](../ROADMAP.md) (not filed). Status: `built`.
- **No read-only enforcement, row limit or timeout** (finding 7). Remedy: [roadmap row 4](../ROADMAP.md) (not filed) covers bounded reads. Status: `built`.
- **Engines are created per query and never disposed** (finding 8). Remedy: not on the roadmap. Status: `built`.

## Not established from code

- Whether a given warehouse's query history keeps a comment-placed tag intact.
- What a deployment's session statement does on its warehouse.
- Which warehouse account a deployment connects as, and what that account may do.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| XE-SQL-WAREHOUSE | calls | SQL through a database driver | the connection's own warehouse credential, from config | tagged statements, run in a committing transaction | `dag_tools/resources/warehouse.py:77-132`, `:447-457`; `test_tagged_warehouse_resource.py`: `test_a_connection_naming_nothing_is_refused_with_no_env_fallback` | `built` |
| CO-DAGSTER | is called by | in-process | none | resource definition under its key | `dag_tools/components/tagged_warehouse/component.py:54-86`; `test_tagged_warehouse_component.py`: `test_the_component_contributes_a_resource_and_no_assets` | `built` |
| CO-READ-AUDIT | is called by | in-process resource call | none | a history statement and parameters | `dag_tools/components/read_audit/component.py:144-167`; `test_tagged_warehouse_component.py`: `test_the_resource_drops_in_under_an_unchanged_read_audit` | `built` |
| CO-WAREHOUSE-SOURCE | is called by | in-process resource call | none | count and column statements | `dag_tools/components/warehouse_source/component.py:403-409`; no test | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | runs warehouse queries that each carry an attribution tag, and refuses to send one that cannot | — |
| How the mesh wires it | a Dagster resource from a component a deployment adds; the warehouse source and read audit components can read through it by key | — |
| Sign-in | the connection's own warehouse credential, from config | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct from Dagster to the warehouse; the gateway and broker are not on it | — |
| Plane | `data-management`: attributed access to stored data | [NATO L3](../alignment/nato-dcra.md) |
| Security role | marks each read with a principal; enforces nothing on what the read does | [NATO R5](../alignment/nato-dcra.md) |
| Services served | the attribution the read audit measures | [DoD S10](../alignment/dod-dmra.md) |

## Sources

All paths are at `503607dd321cd6627509bfc93955357b8566597f`. Component paths are under `dag_tools/components/tagged_warehouse/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| A resource and no assets | `component.py:54-86` | AGENTS.md §4j | `test_tagged_warehouse_component.py`: `test_the_component_contributes_a_resource_and_no_assets`, `test_the_resource_key_in_the_yaml_is_the_key_the_resource_lands_under` | `built` |
| Untagged query refused before a connection; second check at both connection points | `dag_tools/audit/warehouse.py:190-237`; `dag_tools/resources/warehouse.py:228-257`, `:352-381`, `:447-457` | AGENTS.md §4j | `test_tagged_warehouse_resource.py`: `test_a_query_with_no_principal_is_refused_and_nothing_is_sent`, `test_an_untagged_plan_is_refused_at_the_point_of_execution`, `test_the_resource_refuses_before_resolving_a_connection` | `built` |
| Principal, run and asset in the tag; no signature | `dag_tools/resources/warehouse.py:352-414`; `dag_tools/audit/tag.py:123-146` | AGENTS.md §4j | `test_tagged_warehouse_resource.py`: `test_the_tag_carries_the_principal_the_asset_and_the_run`, `test_for_asset_sharpens_a_run_scoped_tag_to_the_asset_key`; `test_audit_tag.py`: `test_an_over_long_tag_raises_rather_than_truncating` | `built` |
| `for_user` keeps the credential; identifier unchecked | `dag_tools/resources/warehouse.py:393-414`, `:459-462` | AGENTS.md §4j | `test_tagged_warehouse_resource.py`: `test_for_user_attributes_the_read_to_the_verified_caller` (tags only) | `built` |
| Comment or session placement; operator's statement checked | `dag_tools/audit/warehouse.py:71-120`, `:171-187`; `dag_tools/resources/warehouse.py:156-182` | AGENTS.md §4j | `test_audit_warehouse.py`: `test_a_session_statement_must_bind_the_tag_as_a_parameter`, `test_a_session_statement_must_be_one_statement`, `test_session_placement_demands_a_statement_and_guesses_no_dialect`; `test_a_tag_carrying_a_comment_marker_is_refused`; `test_tagged_warehouse_resource.py`: `test_a_comment_placed_tag_survives_sqlalchemy_text`; `test_tagged_warehouse_component.py`: `test_a_tag_statement_without_the_bound_tag_fails_the_load_not_the_run` | `built` |
| Connection checked at load; no environment fallback | `dag_tools/resources/warehouse.py:77-132`; `schema.py:78-105` | AGENTS.md §4j | `test_tagged_warehouse_resource.py`: `test_a_connection_naming_nothing_is_refused_with_no_env_fallback`, `test_resolving_a_connection_does_not_import_dlt`; `test_tagged_warehouse_component.py`: `test_a_connection_naming_nothing_fails_the_load` | `built` |
| Committing transaction; no read-only, limit or timeout | `dag_tools/resources/warehouse.py:447-457` | not stated | no test | `built` |
| Engine per query, not disposed | `dag_tools/resources/warehouse.py:459-462` | not stated | no test | `built` |
| Not wired by the chart or the shipped code location | no match for the component under `helm/` or `dag_tools/user_deployment/` | not stated | — | `built` |
