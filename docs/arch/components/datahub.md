# DataHub (CO-DATAHUB)

DataHub is the catalog the mesh's components write datasets, lineage and labels to. Four components write to it and one reads from it. The gateway and the broker never call it. Neither chart deploys DataHub: the dag-tools chart has no reference to it, and the invincible-agent chart passes its address and an optional token to the workloads that use it. This record collects how the five components reach the catalog and where they differ. Each component's own record has the detail.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **Neither chart deploys DataHub.** The dag-tools chart does not mention it. The invincible-agent chart's infrastructure template deploys a relational database, a graph database, a vector store and an RDF store, and no catalog. That chart ships an image of its own that wraps the catalog's API, and it expects the catalog to be supplied separately. `not built`
2. **The invincible-agent chart wires an address and an optional token.** Its values give the dag-tools code location `DATAHUB_SERVER` and give other workloads their own address keys. One chart secret, empty by default, becomes `DATAHUB_TOKEN`. The secret reaches the code-location pods as an optional reference, so a pod starts without it. `built`
3. **Four components write, and they differ on what a failed write does.** The [lineage sensor](datahub-lineage.md) logs a warning and carries on (finding 8). The [dbt project](dbt-project.md) logs the failure and never fails the build (finding 6). The [warehouse source](warehouse-source.md) fails the materialization (finding 10). [Classification propagation](classification-propagation.md) fails that one dataset and carries on with the others (findings 10 and 11). `built`
4. **Two writers send whole aspects and one sends patches.** The lineage sensor and the warehouse source send each aspect whole, which replaces whatever the catalog held for that aspect. The warehouse source's per-column labels go in the editable schema aspect, sent whole; what that does to an earlier edit is not established (that record, Not established). Classification propagation sends patches that add each label term and remove every other term in the vocabulary (that record, finding 8). The dbt project hands its artifacts to the catalog's own ingestion command. `built`
5. **Every writer and the reader take the token the same way.** Each reads it from its own config first and then from `DATAHUB_TOKEN`, and none sends an empty token as a credential. No component has a token default. Only the dbt project has an address default; the others need the address set. `built`
6. **One component reads from the catalog.** [Contract mapping](contract-mapping.md) can read a source's schema aspect from the catalog when its config asks it to (finding 11). It needs the address set, takes the token as above, and treats an absent aspect as an empty schema with a note. No test covers the read (that record, finding 12). `built`
7. **The gateway, the broker and the data client make no catalog call.** The [domain broker](domain-broker.md) derives each dataset's URN locally, through the lineage component's converter. It prefers a URN tag on the asset, then the derived physical URN. An asset with neither is not advertised, and without the catalog plugin no physical URN is derived. `built`
8. **Two components refuse a `dagster` platform URN as the asset of record, and contract mapping does not check.** The warehouse source and classification propagation refuse one at load (warehouse source, finding 3). Contract mapping takes the contract's physical URN as the asset of record (finding 10) and has no check that refuses a `dagster` platform URN. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `DATAHUB_SERVER` (dag-tools code location) | unset in code; set in the invincible-agent chart | the catalog address for the lineage sensor, and the gate that adds it |
| `DATAHUB_TOKEN` | unset; from an empty chart secret in the invincible-agent chart | the token any component uses when its config has none |
| each component's `datahub_config` or `catalog` block | unset | the component's own address and token, which win over the environment |
| `DATAHUB_SENSOR_STATUS` | `RUNNING` | the lineage sensor's starting state in the shipped code location |
| `secrets.datahubToken` (invincible-agent chart) | empty | the chart secret that becomes `DATAHUB_TOKEN` |

## Gaps

- **Writers disagree on whether a failed write fails anything** (finding 3). An operator reading a green run cannot tell whether the catalog received it without knowing which component wrote. Remedy: not on the roadmap.
- **Whole-aspect writes replace what another writer or a person set** (finding 4). Remedy: not on the roadmap.
- **Contract mapping does not refuse a `dagster` platform URN** (finding 8). Remedy: not on the roadmap.
- **No test covers the contract-mapping catalog read** (finding 6). Remedy: not on the roadmap.

## Not established from code

- Which catalog version a deployment runs, and where.
- Whether two writers address the same dataset in one deployment, and so whether finding 4 arises.
- What the invincible-agent chart's catalog seed job and its policy-sync job write or read; both run scripts outside this repo.
- Whether a deployment sets the catalog token, and how long it lives.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | holds the datasets, lineage, labels and schemas the components publish | — |
| How the mesh wires it | an external service; the invincible-agent chart passes its address and an optional token | — |
| Sign-in | a token from each component's config or `DATAHUB_TOKEN`, or none | [NATO R7](../alignment/nato-dcra.md) |
| Data path | metadata only; no data passes through it | — |
| Plane | data management: the catalog of what exists and where it came from | [NATO L3](../alignment/nato-dcra.md) |
| Security role | holds the labels a policy information point would read; dag-tools makes no decision from it | [NATO R3](../alignment/nato-dcra.md) |
| Services served | identifiers, the catalog, lineage and labels | [DoD S1](../alignment/dod-dmra.md), [DoD S3](../alignment/dod-dmra.md), [DoD S4](../alignment/dod-dmra.md), [DoD S12](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. Invincible-agent chart paths are under `helm/invincible-agent/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No catalog in either chart; a wrapper image only | `helm/dag-tools/` (no match); invincible-agent `templates/infrastructure.yaml`; `values.yaml:942-946`; `templates/NOTES.txt:47` | not stated | — | `not built` |
| Address and optional token wired | invincible-agent `values.yaml:1316`, `:2961`; `templates/secrets.yaml:18`; `templates/user-deployments.yaml:132-141` | not stated | — | `built` |
| Writers differ on failure | `dag_tools/components/datahub_lineage/component.py:170-250`; `dag_tools/components/dbt_project/component.py:220-224`; `dag_tools/components/warehouse_source/component.py:277-293`; `dag_tools/components/classification_propagation/component.py:395-414` | AGENTS.md §4i, §4m, §4n | `test_warehouse_source_component.py`: `test_a_catalog_failure_FAILS_the_materialization` | `built` |
| Whole aspects and patches | `dag_tools/components/datahub_lineage/component.py:170-250`; `dag_tools/components/warehouse_source/component.py:191-274`; `dag_tools/components/classification_propagation/component.py:348-369`; `dag_tools/components/dbt_project/component.py:189-214` | AGENTS.md §4m, §4n | `test_warehouse_source_component.py`: `test_no_schema_aspect_is_emitted_because_only_names_were_read`; `test_dbt_datahub_artifacts.py`: `test_every_artifact_the_datahub_recipe_names_is_in_the_ingest_cwd` | `built` |
| Token from config then environment; no empty token | `dag_tools/components/datahub_lineage/component.py:718-728`; `dag_tools/components/dbt_project/component.py:181-187`; `dag_tools/components/warehouse_source/component.py:169-188`; `dag_tools/components/classification_propagation/component.py:373-392`; `dag_tools/components/contract_mapping/component.py:242-258` | AGENTS.md §4i | `test_warehouse_source_component.py`: `test_an_empty_token_is_never_sent_as_a_credential` | `built` |
| One reader | `dag_tools/components/contract_mapping/component.py:222-279` | AGENTS.md §4k | no test | `built` |
| No catalog call from gateway, broker or data client; local URNs | `dag_tools/domain_broker/main.py:506-560`, `:901-912`; no catalog call in `dag_tools/central_gateway/main.py` or `dag_tools/cortex_data/client.py` | not stated | — | `built` |
| `dagster` platform refusal in two components | `dag_tools/warehouse_source/spec.py:38-41`, `:170-180`; `dag_tools/components/classification_propagation/schema.py:168-172`; no check in `dag_tools/contract_mapping/` | AGENTS.md §4l, §4m | `test_warehouse_source_component.py`: `test_a_dagster_platform_urn_fails_the_load`; `test_warehouse_source_engine.py`: `test_the_asset_of_record_may_not_be_a_dagster_urn` | `built` |
