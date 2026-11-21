# Contract mapping (CO-CONTRACT-MAPPING)

The contract mapping component checks a mapping against a data contract and writes the dbt model and tests that follow from it. A contract states a table's columns, grain, scope and rules. A mapping says which source column feeds each contract column. The component builds one Dagster asset per instance. It moves no data and runs nothing against a warehouse. It writes files into a dbt project, or in a dry run writes nothing and puts the rendered text on the materialization.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **It builds one asset and nothing else.** The component returns a single asset, named by `asset_name`, and no check, sensor, schedule or job. The asset's description says it moves no data. It returns a dictionary describing the plan and the files, and puts headline numbers on the materialization. `built`
2. **The checking engine is pure.** The engine in `dag_tools/contract_mapping/` imports pydantic and a YAML reader, and does no file or network access. It takes parsed documents and returns a plan. The component is the only part that knows about Dagster. A test imports the engine in a fresh process and fails if Dagster, dbt, dlt, SQLAlchemy, Jinja or DataHub is loaded. `built`
3. **A file contract is checked when the code location loads.** When the contract is a file and the mapping lists its own source columns, the whole plan is built at load. A mapping that reads a column the source lacks, or leaves out a column a governed read selects, fails the load. A malformed emit block, a missing contract file and a wrong contract id also fail at load. `built`
4. **A served contract is fetched at materialization, with only the headers the deployment sets.** With `contract.url`, nothing is fetched at load, and the asset's notes say that nothing was reconciled at load. The run does a plain GET with the configured headers and timeout. No other credential is added, and nothing identifies the caller beyond those headers. The component does not use the central gateway or the domain broker. `built`
5. **One field both selects from a bundle and checks the id.** A contract file may hold several documents separated by `---`. With more than one, `expect_contract_id` picks the document, and a bundle with no id named fails the load and lists the ids it holds. With one document, the id is only a check. A served contract is checked against the id after the fetch. `built`
6. **A refusal means the emitted files would be wrong. Anything less is a note.** The plan collects every problem before raising. It refuses a mapping paired with the wrong contract, a column outside the contract, a required column left unproduced, and a source column the schema lacks. A test that cannot be written (no calendar values, no non-finite predicate, no warehouse types) is a note on the plan and is published on the materialization. `built`
7. **Nothing is guessed to fill a gap.** There is no default warehouse type, cast template, regex template, load-instant expression or non-finite predicate; each is an empty setting in the mapping. A model contract is enforced only when every column has a warehouse type; with one missing, none is emitted. `built`
8. **Everything is rendered into memory, then written.** If rendering fails, the dbt project is left as it was. The component writes the model file, a schema file and one test file per invariant that dbt has no built-in test for. It refuses a project directory that does not exist. Line endings are written as newline. `built`
9. **Pruning is off by default and limited by name.** With `emit.prune` on, it deletes generated test files that start with the model name and that the current plan no longer emits. Other files in the test directory are never considered. `built`
10. **The asset of record is the contract's physical dataset URN.** The materialization carries it as `asset_of_record` and carries the mapping's source URN as `source_dataset`. The contract field is documented as never a Dagster-platform URN, but the component does not check which platform the URN names. The schema file records the asset of record. `built`
11. **It can read the source schema from the catalog, using a token from config or the environment.** With `source_schema.datahub`, the run reads the source's schema aspect from the catalog server named in config, keyed by the mapping's source URN. The token is `datahub_config.token`, else the `DATAHUB_TOKEN` environment variable; no token key is sent when neither is set. The catalog's schema wins over the mapping's, and a disagreement is reported as a note. It publishes nothing to the catalog. `built`
12. **Tests pin the load/run split and the write rules, but not the catalog read or a served normalizer fetch.** `test_contract_mapping_component.py` covers load-time refusal, the served fetch at run time and not at load, bundle selection, where files land, dry run, prune, and no writes after a failed render. No test reads a schema from the catalog or fetches a served normalizer document. `built`

## Settings and defaults

Component attributes in the component's YAML file. The mapping document's own settings (warehouse types, cast templates and the rest) are in the mapping file, not here.

| Setting | Default | What it does |
|---|---|---|
| `mapping_file` | required | the mapping document; relative paths resolve against the component directory |
| `contract.path` | one of `path` or `url` is required | a contract file, which may hold several documents; read at load and again at run |
| `contract.url` | one of `path` or `url` is required | a served contract; fetched at run, never at load |
| `contract.headers` | empty | headers sent with the `url` fetch |
| `contract.timeout_seconds` | `30` | timeout for the `url` fetch |
| `contract.expect_contract_id` | none | refuses a contract with another id; selects the document in a bundle, and is required for one |
| `normalizers.path` / `normalizers.url` | none; exactly one when the block is set | the normalizer document; needed only when a mapping uses `normalize`; `url` is fetched at run |
| `normalizers.headers` / `.timeout_seconds` | empty / `30` | as for the contract |
| `source_schema.from_mapping` | `true` | use the source columns the mapping lists |
| `source_schema.datahub` | `false` | read the source schema from the catalog at run |
| `source_schema.datahub_config` | empty | `server` (required with `datahub`) and optional `token` |
| `emit.dbt_project_dir` | required unless `dry_run` | the dbt project written into; must already exist |
| `emit.models_subdir` / `emit.tests_subdir` | `models` / `tests` | where under the project the model and the tests go |
| `emit.dry_run` | `false` | render and write nothing; the text goes on the materialization |
| `emit.prune` | `false` | delete stale generated tests named for the model |
| `asset_name` | `contract_mapping` | name of the asset |
| `group_name` / `io_manager_key` | none | passed to the asset |
| `owners` / `tags` | empty | passed to the asset |
| run config `dry_run` | none | overrides `emit.dry_run` for one run |
| run config `max_listed` | `20` | how many notes and file names are listed in metadata |

## Gaps

- **The catalog schema read has no test** (finding 12). The code path that builds the catalog client, takes the token and compares schemas is not run by any test. Remedy: not on the roadmap.
- **A served normalizer document has no test** (finding 12). Its fetch and its note about unverified references are not exercised. Remedy: not on the roadmap.

## Not established from code

- Which contracts, mappings or dbt projects any deployment points it at.
- What is behind a deployment's contract URL, and what credential it expects in the headers.
- Whether the output directory is part of a project that a deployment's builds then run. The component does not run dbt.
- Whether the example's contract is current with its upstream copy. The README says it is vendored from the agent repository, and a test compares them only when the upstream copy is on the machine.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | turns a source table into the shape a contract states, by emitting a dbt model and tests, and reports what it could not check | [DoD P2](../alignment/dod-dmra.md) |
| How the mesh wires it | a Dagster component configured in YAML; the asset it builds writes files into a dbt project that a deployment builds separately | — |
| Sign-in | none to the mesh; a served contract gets only the headers the deployment configures; the catalog read uses a configured or environment token | [NATO R7](../alignment/nato-dcra.md) |
| Data path | none; it moves no data, and the gateway and broker are not on its path | — |
| Plane | data management, where shared lifecycle components run | [NATO L3](../alignment/nato-dcra.md) |
| Security role | none for mesh reads; it refuses mappings that disagree with their contract, which is input validation | — |
| Services served | translation into the contract's form once, at the producer; quality tests that fail under the contract's own rule ids | [DoD S14](../alignment/dod-dmra.md), [S13](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/contract_mapping/`; engine paths under `dag_tools/contract_mapping/`; tests under `dag_tools_tests/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| One asset, no check, sensor, schedule or job | `component.py:438-448`, `:570-619` | AGENTS.md §4k | `test_contract_mapping_component.py`: `test_the_shipped_example_loads`, `test_the_example_materializes_and_publishes_the_bytes_it_would_write` | `built` |
| Engine is pure | `contract.py:28-32`; `plan.py:25-34`; `emit.py:21-42`; `normalize.py:42-47`; `component.py:55-59` | AGENTS.md §4k | `test_contract_mapping_normalize.py`: `test_the_engine_imports_none_of_the_orchestration_stack` | `built` |
| File contract checked at load | `component.py:401-429` | AGENTS.md §4k | `test_contract_mapping_component.py`: `test_an_unknown_source_column_fails_the_code_location_load`, `test_a_mapping_that_drops_a_required_column_fails_the_load`, `test_a_malformed_emit_block_fails_the_load_not_the_run`, `test_a_contract_file_that_is_not_there_fails_the_load` | `built` |
| Served contract fetched at run, headers only | `component.py:165-187`, `:430-436`, `:454-462`; `schema.py:49-60` | AGENTS.md §4k | `test_contract_mapping_component.py`: `test_a_served_contract_is_not_fetched_at_load`, `test_a_served_contract_is_fetched_at_materialization`, `test_a_served_contract_with_the_wrong_id_fails_the_run`; no test of the headers sent | `built` |
| Bundle selected by `expect_contract_id` | `component.py:100-162`, `:212-219`; `schema.py:62-74` | AGENTS.md §4k | `test_contract_mapping_component.py`: `test_a_contract_bundle_is_selected_from_by_the_expected_id`, `test_a_bundle_with_no_expected_id_fails_the_load_listing_the_ids`, `test_a_single_document_contract_still_checks_the_id_rather_than_selecting`, `test_the_expected_contract_id_is_enforced` | `built` |
| Refusals versus notes | `plan.py:489-560`, `:719-761`, `:822-861` | AGENTS.md §4k | `test_contract_mapping_plan.py`: `test_every_problem_is_reported_at_once`, `test_a_column_outside_the_contract_is_refused`, `test_an_unproduced_required_column_is_refused`, `test_a_named_but_unlisted_calendar_gets_no_test_and_says_why`, `test_no_non_finite_predicate_means_no_non_finite_test_and_says_so` | `built` |
| No guessed defaults | `mapping.py:426-474`; `plan.py:690-691`, `:719-761` | AGENTS.md §4k | `test_contract_mapping_plan.py`: `test_no_warehouse_types_means_no_enforced_contract_and_says_so`, `test_one_missing_warehouse_type_drops_the_contract_rather_than_half_of_it`; `test_contract_mapping_emit.py`: `test_the_contract_is_enforced_only_when_every_column_has_a_warehouse_type` | `built` |
| Render in memory, then write | `component.py:322-351`, `:492-502`; `emit.py:903-916` | AGENTS.md §4k | `test_contract_mapping_component.py`: `test_nothing_is_written_when_the_render_fails`, `test_the_files_land_where_the_emit_block_says`, `test_a_dbt_project_directory_that_is_not_there_is_refused`, `test_a_run_config_dry_run_renders_without_writing` | `built` |
| Prune off by default, name-limited | `component.py:353-369`; `schema.py:202-211` | AGENTS.md §4k | `test_contract_mapping_component.py`: `test_prune_is_off_by_default_so_a_hand_written_test_survives`, `test_prune_removes_only_files_this_component_would_have_written` | `built` |
| Asset of record is the contract's dataset URN | `contract.py:135-148`; `plan.py:879-880`; `component.py:559-562` | AGENTS.md §4k, §4l | `test_contract_mapping_component.py`: `test_the_materialization_records_the_physical_platform_as_the_asset_of_record`; the code does not check the URN's platform | `built` |
| Catalog schema read, token from config or environment | `component.py:222-279`, `:288-319`, `:464-476`; `schema.py:122-169` | AGENTS.md §4k | no test | `built` |
| Tests cover load/run split and writes; not the catalog read or a served normalizer | `dag_tools_tests/test_contract_mapping_component.py`; grep of that file and `test_contract_mapping_normalize.py` for the catalog and served normalizers found none | not stated | — | `built` |
