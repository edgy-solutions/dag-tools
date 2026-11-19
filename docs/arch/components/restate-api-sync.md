# Restate API sync (CO-RESTATE-API-SYNC)

The Restate API sync component is the Dagster side of the generic API sync. For each configured source table it builds the extraction assets, then one extra asset that reads the staged rows back and posts each row to a Restate ingress address. The Restate server then runs the `api_sync` handler for each row. The handler calls the target API and writes an acknowledgement back to a SQL Server database. See [the Restate worker record](restate-worker.md) for the handler side.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It builds the extraction assets plus one dispatch asset per source table.** For each pipeline it calls the shared extraction factory, which makes the dlt assets. For each table named in the pipeline's `sources`, it adds an asset called `<pipeline>_<table>_api_dispatch` that depends on that table's extraction asset. It defines no sensor, schedule, job or asset check, so nothing runs it unless the deployment attaches something. `built`
2. **Three settings are required, two have defaults, and two per-pipeline keys are required.** `source_config`, `dest_config` and `restate_endpoint` have no default. `staging_config` and `pipelines` default to empty. Inside each pipeline, `primary_key` and `api_path` are popped without a default, so a missing one fails the load with a key error. The rest of the pipeline's keys go to the extraction group's own config. The component never imports `schema.py`, so the models there are not what the component validates against. `built`
3. **Each run re-reads the whole staged table and posts every row.** The dispatch asset selects every row of the staged table, with no filter. It posts them one at a time, in order. The component records nothing about which rows were sent, so a second materialization posts them all again. `built`
4. **The staging read uses a credential from the Dagster process's environment.** The dispatch asset takes a connection URL from a `DESTINATION__<DRIVER>__CREDENTIALS` variable, named from the `drivername` in `dest_config`. If that is unset, it tries the Postgres variable. If neither is set, the run fails with a message naming the variable. The `dest_config` parts are not used for this read, though the extraction step uses `dest_config`, so a deployment must keep the two in agreement. `built`
5. **It posts to the Restate ingress with no credential and no identity.** The asset posts JSON to the `restate_endpoint` value as given. It appends no path and sends no header of its own. In a container (a Docker marker file or a Kubernetes service variable), a value that starts with one local development form is rewritten to one in-cluster form. Any other value is used unchanged. The deployment must set a full ingress URL that reaches the `api_sync` handler. `built`
6. **It goes directly to the staging database and to Restate; the central gateway and domain broker are not on the path.** The component reads the staging database through SQLAlchemy and posts to Restate over HTTP. It issues no ticket and calls neither mesh service. It never reads the target API's credential. The handler reads `API_BASE_URL` and `API_KEY` from its own pod. `built`
7. **A failed post is logged and skipped, and the asset still succeeds.** Any exception around a post is caught and logged as a warning, and the loop moves on. The response status is never read, so a rejection by the ingress is not seen. `built`
8. **The payload names the table, key and route, and carries the whole row.** Each message has `api_path`, `source_table`, `pk_column`, `pk_value` and `row_data`. Decimal values are converted to floats. The key value is looked up by the lower-cased column name first, then as written. The handler reads exactly these five fields. `built`
9. **The handler refuses a payload it cannot act on, inside each of its two steps.** Both steps call the same check as their first statement, because a replay can run the second step without the first. It refuses `row_data` given as a string, since a string is valid JSON and would be posted as a bare value. It refuses an `api_path` that does not start with `/`, since the joined URL would extend the host name. It also refuses a missing field or a key value that is not one identifier. It reports every problem at once, naming fields and no values. The reasons are in AGENTS.md §4. `built`
10. **The handler's HTTP step is unbounded and refuses only its own errors; its database step is bounded.** The post to the target API takes `terminal_on_refusal`, not `terminal_on_damage`, because an HTTP failure quotes its URL and a number in it would be read as a database login code. The acknowledgement step keeps the retry bound. An absent API base address or SQL Server DSN is refused at once. This component supplies neither value. The post to the target API sets no timeout, so a target that never answers holds the unbounded step open. `built`
11. **It publishes nothing to the catalog.** The dispatch asset returns nothing and logs the row count. It applies and checks no classification label on the rows it sends. `built`
12. **The handler's acknowledgement writes the payload's table and key column into the statement text.** It updates a fixed status column and an external-id column in the table the payload names, where the key column the payload names equals the key value. The key value and the external id are bound as parameters. The table and column names are placed in the statement as given; the check confirms only that each is non-empty text. Because the ingress takes a post with no credential (finding 5), whoever can reach it chooses those names. `built`
13. **The component has no test of its own; the handler does.** The only test found that touches this component's module checks the shared dlt resource it registers. The handler's refusals and bounds are tested by the named tests below. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `source_config` | required | the source database configuration, passed to the extraction factory; its `schema` is a fallback for the staging read |
| `dest_config` | required | the staging destination configuration; its `drivername` names the environment variable for the read-back, and its `schema` is the first fallback for the staging schema |
| `restate_endpoint` | required | the full ingress address rows are posted to |
| `staging_config` | empty | the optional staging bucket or filesystem, passed to the extraction factory |
| `pipelines` | empty | a map of pipeline name to its settings |
| `pipelines.<name>.primary_key` | required | the key column sent with each row |
| `pipelines.<name>.api_path` | required | the route the handler appends to the API's base address; must begin with `/` |
| `pipelines.<name>.sources` | empty | the tables to extract and dispatch |
| `pipelines.<name>.name` | the pipeline key | the extraction group's name |
| other `pipelines.<name>` keys | the extraction group's own defaults | passed to it as is, for example `dest_schema` and `hints` |

## Gaps

- **Rows are posted to the Restate ingress with no credential** (finding 5). Anything that can reach the ingress can start the handler with a payload of its choosing. Remedy: [roadmap row 13](../ROADMAP.md) (not filed).
- **A rejected or failed post is invisible to Dagster** (finding 7). A run in which every post failed still shows as successful, with warnings in its log. Remedy: not on the roadmap.
- **Every materialization re-sends every staged row** (finding 3). The API receives the same rows again each time. Remedy: not on the roadmap.
- **The acknowledgement's table and column names come from the payload unchecked** (finding 12). Only their presence is checked, and the sender is not identified. Remedy: not on the roadmap; who may send is [roadmap row 13](../ROADMAP.md) (not filed).
- **The post to the target API has no timeout** (finding 10). Remedy: not on the roadmap.
- **The component has no test of its own** (finding 13). Remedy: not on the roadmap.

## Not established from code

- Which ingress address a deployment sets, and the exact path form the Restate ingress needs for the handler. The component sends to whatever it is given.
- Whether the address a deployment sets is a one-way send or a call that waits for the handler. The component does not choose.
- What the target API does with a repeated row.
- Whether a deployment attaches the dispatch asset to a schedule or sensor.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | sends staged rows, one message each, to the Restate handler that delivers them to an external API and acknowledges them at the source | — |
| How the mesh wires it | a Dagster component with an extraction step and a dispatch asset per table; the address and the staging credential are set by the deployment | — |
| Sign-in | none to Restate; a database URL from the process environment for the staging read | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct from Dagster to the staging database and to the Restate ingress; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | operational, where producers' rows are pushed out to a consumer's API | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none; the handler's refusals are input validation | — |
| Services served | delivery of records to an API as a registered, retried exchange, started from Dagster | [DoD S7](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/restate_api_sync/`; handler paths under `dag_tools/restate_handlers/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Extraction assets plus a dispatch asset per table; no sensor, schedule or check | `component.py:43-63`, `:75-90`, `:166-173` | AGENTS.md §2 | no test | `built` |
| Required and defaulted settings; `schema.py` not imported | `component.py:23-36`, `:47-48`; `schema.py:5-50` | AGENTS.md §5 | no test | `built` |
| Whole table re-read and posted in order | `component.py:104-118`, `:142-160` | not stated | no test | `built` |
| Staging read credential from the environment | `component.py:92-101` | not stated | no test | `built` |
| Posts to the given address, no header; container rewrite | `component.py:133-142`, `:160` | not stated | no test | `built` |
| Direct path, no gateway or broker | `component.py:101`, `:142`; `api_sync.py:115-116` | not stated | — | `built` |
| Failed post logged and skipped | `component.py:159-162` | not stated | no test | `built` |
| Payload fields; decimals to floats | `component.py:122-131`, `:146-154`; `api_sync.py:55-57`, `:71`, `:83` | not stated | no test of the component | `built` |
| Refusal inside each step | `api_sync.py:14-104`, `:113`, `:160` | AGENTS.md §4 | `test_restate_api_sync_refusal.py`: `test_each_step_refuses_for_itself_because_a_replay_can_skip_the_first`, `test_a_string_of_row_data_is_refused_rather_than_posted_as_a_bare_string`, `test_a_path_that_would_extend_the_host_is_refused`, `test_every_problem_is_reported_at_once` | `built` |
| Unbounded HTTP step with `terminal_on_refusal`; bounded database step; absent settings refused; no timeout on the post | `api_sync.py:118-126`, `:137`, `:144-156`, `:165-171`, `:184-191` | AGENTS.md §4 | `test_restate_api_sync_refusal.py`: `test_the_acking_step_carries_a_bound_and_the_http_step_does_not`, `test_an_http_failure_quoting_a_five_digit_id_is_retried_not_refused`, `test_an_absent_base_url_is_terminal_rather_than_retried_forever`, `test_an_absent_dsn_is_terminal_on_the_acking_step_too` | `built` |
| Acknowledgement statement: names from the payload, values bound | `api_sync.py:45-57`, `:173-177` | not stated | no test of the statement text | `built` |
| Nothing published to the catalog | `component.py:91-164` | not stated | no test | `built` |
| No component test; handler registered as `api_sync` | `serve.py:38`; `test_components_share_the_dlt_resource.py`: `test_every_dlt_component_registers_the_same_instance` (shared resource only) | not stated | no test of the component | `built` |
