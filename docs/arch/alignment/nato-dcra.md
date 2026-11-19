# Alignment to the NATO data-centric reference (AL-DCRA)

This record sets the mesh against the security roles and the data planes of the NATO data-centric reference architecture, one row per element. It uses the alignment shape in [the README](../README.md), and statuses and pins are as listed there. It cites a second reference for the plane rows, because the DoD reference names the same services from the other side. The shape is provisional.

**This record describes; it proposes nothing.** Where it names something unbuilt, the row's status says so.

## The references

| | Primary | Also cited |
|---|---|---|
| Name | NATO, *Data Centric Reference Architecture for the Alliance* | Chief Digital and Artificial Intelligence Office (CDAO), *Data Mesh Reference Architecture* |
| Edition | version 2, AC/322-D(2025)0056, publicly disclosed | version 2.6, March 2024 |
| Terms | Public. Cited by element name and appendix paragraph only. No figure or passage is copied, and each element's meaning is given here in this record's own words. The document is not committed. | As in [the DoD record](dod-dmra.md). Cited only by that record's row numbers. |
| Elements used | Appendix 9 (data-centric security): ¶3, the four policy roles; ¶6 to ¶10, the identity, credential and access management services. Appendix 14 (data planes): ¶1 to ¶10, the four planes. | — |

**Out of scope.** Appendix 14 ¶11 to ¶15 cover APIs, consistency, ontologies and discoverability across the planes. They are not rows here. The DoD record's rows S2 (semantic), S3 (catalog) and S8 (search) cover the same ground.

## Summary

- **Roles:** every Appendix 9 role has a component, except certificate services. The access model is a relationship on one dataset, not an evaluation of attributes, and it is recorded as that. The evidence for each role is in [need to know, "Roles in the NATO frame"](../flows/need-to-know.md#roles-in-the-nato-frame); this record repeats the main lines and adds how closely each role corresponds.
- **Planes:** each plane has a built part, and each is `partial`. The main missing parts are:
  - no standard metadata form for the catalog (DCAT) or for labels;
  - no retirement in data management;
  - SDK vector and graph clients not built for the analytical plane.

## Security roles (Appendix 9)

| # | Element | What corresponds | How closely | Status | Evidence |
|---|---|---|---|---|---|
| R1 | Policy enforcement point (¶3): where a request is stopped or let through | **The central gateway** refuses before any data path is resolved, checking identity, then the deny list, then Topaz. **The domain broker** mints a credential scoped to one dataset and fails the ticket if minting fails. **The client** applies narrowing to the frame for backends that cannot. **Agent services** make their own checks before a request reaches the gateway. | `partial`: the gateway is the enforcement point. The gaps are the posture default (`observe`), object stores narrowed by prefix only, and unminted backends on the producer's credential. See [need to know, findings 4 and 5](../flows/need-to-know.md). | `built` | dag-tools `dag_tools/central_gateway/identity.py:107-120`; `dag_tools/central_gateway/main.py:438-470`; `dag_tools/domain_broker/main.py:226-308`, `:1219-1223`, `:1242-1268`; `dag_tools/cortex_data/client.py:318-366`; invincible-agent `tests/sandbox_e2e/_seal_asset_read_denied_at_the_gate.py:1-13` |
| R2 | Policy decision point (¶3): where the answer is computed | The Topaz authorizer, which denies by default and allows only on an explicit `reader` or `owner` relation. | `partial`: it decides on relations, not attributes. No narrowing or label policy exists. | `built` | invincible-agent `helm/invincible-agent/templates/topaz-configmap.yaml:159-164`, `:352-392` |
| R3 | Policy information point (¶3): where the facts a decision uses come from | The Topaz directory holds datasets and their owners (synced from the catalog), readers (synced from grants), and users and groups (synced from policy files). At request time, the gateway passes in the verified subject and checks the deny list. | `partial`: catalog tags, domains and labels do not reach the directory, and token claims do not reach the decision. | `built` | invincible-agent `policy/sync/datahub_topaz_sync.py:53-57`; `helm/invincible-agent/templates/topaz-seed-cronjob.yaml:252-293`; dag-tools `dag_tools/central_gateway/identity.py:398`; `dag_tools/central_gateway/main.py:78-82` |
| R4 | Policy administration point (¶3): where policy is written and managed | Grants are reviewed files that record who granted them and why. The seed job validates every policy file before any write, then syncs to Topaz. Deleting a row revokes the grant. | `partial`: git history is the only grant ledger, and the validator's CI is in another repository. | `built` | invincible-agent `policy/asset_grants.yaml:1-26`; `policy/sync/validate_policy.py:1-12`; `helm/invincible-agent/templates/topaz-seed-cronjob.yaml:48`, `:252-293` |
| R5 | ICAM (¶6): identity, credential and access management as one service family | Keycloak issues identities, the gateway verifies them, and Topaz decides access, as in R6 to R9. | `partial`, as rows R6 to R9. | `built` | rows R6 to R9 |
| R6 | Authorization and access control (¶7): deciding and enforcing what an identity may do | The decision and information points above. | `different`: access control is an explicit grant of a relation on a dataset, not a rule evaluated over the subject's and the data's attributes. Labels exist but are not inputs to the decision. | `built` (relation); `proposed` (labels as decision input) | rows R2, R3; invincible-agent `policy/classification/dimensions.yaml:1-12` |
| R7 | Authentication (¶8): proving who is asking | The Keycloak realm issues tokens. The gateway verifies them against JWKS or a static key, checking issuer and audience. | `close` | `built` | invincible-agent `helm/invincible-agent/templates/keycloak.yaml:38`; dag-tools `dag_tools/central_gateway/identity.py:398-444` |
| R8 | Digital certificates (¶9): issuing and managing certificates | No certificate service was found in the chart. Trust in the realm comes from a mounted truststore. | `none` | `not built` | invincible-agent `helm/invincible-agent/values.yaml:2268-2283` |
| R9 | Digital identity (¶10): what an identity is and carries | The Keycloak realm gives one authorization-id claim and two realm roles. Service clients carry a fixed authorization id. Group, persona and domain come from the directory, not the token. | `partial`: the identity carries no attributes. | `built` | invincible-agent `helm/invincible-agent/templates/keycloak-configmap.yaml:2`, `:240-255`, `:279-286` |

## Data planes (Appendix 14)

| # | Element | What corresponds | How closely | Status | Evidence |
|---|---|---|---|---|---|
| L1 | Infrastructure plane (¶2 to ¶4): the shared foundation, compute and storage that the other planes run on | The platform chart deploys the graph, vector and RDF stores as StatefulSets, each switched on in values. Dagster runs the pipelines. One shared Restate worker image runs the durable handlers, chosen by environment variable. | `partial`: the chart is one platform, deployed per site, not a federated foundation. Object storage and the catalog are external to the chart. | `built` | invincible-agent `helm/invincible-agent/templates/infrastructure.yaml:124-128`, `:197-201`, `:287-291`; `helm/invincible-agent/values.yaml:2077`, `:2096`, `:2129`; dag-tools `Dockerfile.restate-worker`; `dag_tools/restate_handlers/serve.py:35` |
| L2 | Operational data plane (¶5 to ¶7): where communities produce data, with metadata standardized as it is produced | Producers run their own pipelines, as dlt source kinds and dbt projects, and their own broker. Each source is labelled where it is declared and published to the catalog under its physical URN. | `partial`: metadata is standardized in the mesh's own form, as DataHub aspects and glossary terms. **Gap: not in DCAT form, and labels not in a standard label form.** | `built` (pipelines, own-form metadata); `not built` (standard forms) | dag-tools `dag_tools/asset_wrappers/source_kinds.py:23`; `dag_tools/asset_wrappers/dlt_assets_parsing.py:184`; `dag_tools/components/warehouse_source/component.py:191-243`; invincible-agent `helm/invincible-agent/values.yaml:1176-1186` |
| L3 | Data management plane (¶8 to ¶9): the shared lifecycle services, standardized through APIs and metadata | dag-tools components: contract mapping, label propagation, lineage, the read audit and ingest run records. They write to an external catalog. Access goes through one gateway API. | `partial`: there is no retirement, recall or notice in the lifecycle (DoD record, row S11). | `built` (components); `not built` (retire, recall) | dag-tools `dag_tools/contract_mapping/plan.py:472`; `dag_tools/components/classification_propagation/component.py:529-541`; `dag_tools/components/datahub_lineage/component.py:761`; `dag_tools/audit/reads.py:216`; `dag_tools/asset_wrappers/ingest_runs.py:129`; `dag_tools/central_gateway/main.py:374` |
| L4 | Analytical data plane (¶10): turning data into insight for decisions | Curated dbt models are shipped as a package. The agent fleet reads through the SDK, which resolves a URN to a data reference and reads it with the `cortex_data` client. The fleet also has an ontology service and catalog search. | `partial`: big-data reads are built. The SDK's vector and graph clients are not: vector is an interface only, and the graph client has no gateway handler. | `built` (reads, curated models); `proposed` (SDK graph); `not built` (SDK vector client) | dag-tools `dag_tools/dbt_project_packages/mfg_trace_curated/models/`; `dag_tools/cortex_data/client.py:161`; SDK `interfaces.py:724`, `:1013`; SDK `remote_clients.py:36-41`, `:58` (supplied); invincible-agent `agent_fleet/ontology_service/main.py:1-8` |

## Could not establish

- The posture any deployment runs (`GATEWAY_IDENTITY_POSTURE`). Rows record the code default.
- Whether a certificate service exists outside these charts. None was looked for.
- The SDK lines were supplied by the SDK's session and not read here.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Roles and their evidence | [need to know](../flows/need-to-know.md), reviewed by the dag-tools owner at the pin | as cited there | as cited there | `built` |
| Topaz `can_read = reader \| owner` | invincible-agent `helm/invincible-agent/templates/topaz-configmap.yaml:159-164` | cited in code: invincible-agent ADR-0025 (not read) | — | `built` |
| Policy validated before any write | invincible-agent `helm/invincible-agent/templates/topaz-seed-cronjob.yaml:48`, `:252-255` | none read | — | `built` |
| Labels not read by the decision; rails validator-only | invincible-agent `policy/classification/dimensions.yaml:1-12`; `topaz-configmap.yaml:352-392` | cited in file: gov ruling 2026-11-06 | — | `built` (labels); `proposed` (as input) |
| No certificate service | invincible-agent `helm/invincible-agent/templates/` searched; `values.yaml:2268-2283` | none | — | `not built` |
| Graph, vector and RDF stores in the chart | invincible-agent `helm/invincible-agent/templates/infrastructure.yaml:124-128`, `:197-201`, `:287-291` | none read | — | `built` |
| SDK vector interface only; graph client without a handler | SDK `interfaces.py:724`; `remote_clients.py:36-41`, `:58` | none | supplied by the SDK session | `not built` / `proposed` |
| No DCAT, no standard label form | both repositories searched | lane rule | — | `not built` |
