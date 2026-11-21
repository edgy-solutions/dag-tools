# The mesh: an overview

This page says what the mesh is, how to read the statuses in these records, and how the mesh's services depend on each other. It introduces the other records and adds no claim of its own: every line below points to the record that carries the evidence. Where this page and a record disagree, the record holds.

**This page describes; it decides nothing.** The record shape is provisional, as [the README](README.md) explains.

## What the mesh is

The mesh lets data stay with the team that produces it, while other teams read it under a decision made for each read.

- **Producers.** A producing team runs its own Dagster code location and its own domain broker. Its pipelines are dag-tools components configured in YAML: sources, contract mappings, dbt projects and label propagation. See [Dagster](components/dagster.md), [the domain broker](components/domain-broker.md) and [the dlt pipeline](components/dlt-pipeline.md).
- **The read path.** A consumer asks [the central gateway](components/central-gateway.md) for a dataset by its catalog URN. The gateway verifies the caller's token and asks [Topaz](components/topaz.md) whether the person may read that dataset. It then asks the producer's broker for a ticket, and the broker mints a credential for that dataset alone. The consumer reads with [the mesh data client](components/mesh-data-client.md). [The need-to-know flow](flows/need-to-know.md) walks through the nine steps.
- **The catalog.** [DataHub](components/datahub.md) holds datasets, lineage, labels and dbt metadata. It runs outside the platform chart and is reached by URL.
- **Durable work.** [The Restate server](components/restate-server.md) and [its worker](components/restate-worker.md) run steps that must finish even when a call fails part-way.
- **Who holds what.** dag-tools holds the component library and the gateway and broker services. invincible-agent holds the platform chart, the policy files and the agent fleet. The SDK is the agents' interface to the mesh ([its decisions](decisions/sdk.md)). pub-tools records its own decisions ([the pointer](decisions/pub-tools.md)).

## How to read the statuses

Every claim in these records carries one status: `measured`, `built`, `accepted` or `proposed`. [The README](README.md#status-vocabulary) defines each one and the evidence it needs. In brief:

- **`built`** means code at the pinned commit does it. The record gives a file and line, and a test when one exists. When none exists, the record says "no test".
- **`measured`** needs a recorded run of what the claim is about. An in-process run in continuous integration measures the code's own behaviour, and is cited as "in CI, in process". A claim about a deployed service or a real backend needs a run against that service or backend. A test that could measure a claim is not enough. [The built-to-measured list](built-to-measured.md) names eight claims `measured` in CI, in process, at least in part, and the claims a further run could change. **No other record carries `measured` yet**; each is updated when it is revisited.
- **`accepted`** means a decision record accepts it and no code does it yet. **`proposed`** means it is written down as intent only.
- **`not built`** marks a gap with nothing built.
- **`—`** marks a row that carries no claim of its own, such as a pointer, a reference or a superseded entry.
- **Mixed findings** are written in parts, as "X: `built`. Y: `not built`". A component can be built while one of its uses is only proposed.
- **Alignment rows carry three fields of their own.** How closely the mesh corresponds to an outside element (`close`, `partial`, `different` or `none`) is separate from the status of what corresponds. A row can be `close` and `proposed`. The third, Capability, names the NATO DCRA capability the element falls under, as `dcra:A1` to `dcra:A13` or `none`.
- **Pins.** Every file and line is read at a pinned commit, listed in [the README](README.md#pins). A line number is valid only at its pin.
- **Not established.** Where code, charts and tests do not settle a fact, the record says "not established" rather than guessing.

## Services and the components that serve them

The services are the fifteen of the DoD data-mesh reference, set against the mesh in [the DoD alignment record](alignment/dod-dmra.md). The status column is that record's Status field; its separate "How closely" field is not carried here. The last column lists the components whose records name the service in their "Services served" row.

| Service | Status | Components that name it |
|---|---|---|
| S1 Unique identifiers | `built` | [DataHub](components/datahub.md), [DataHub lineage](components/datahub-lineage.md), [domain broker](components/domain-broker.md), [mesh data client](components/mesh-data-client.md) |
| S2 Semantic services | `built` | none; the alignment record cites an invincible-agent ontology service and the identifier rules of contract mapping |
| S3 Federated catalog | catalog: `built`. A standard catalog form: `not built`. | [DataHub](components/datahub.md), [dbt project](components/dbt-project.md), [Grist ingest](components/grist-ingest.md), [warehouse source](components/warehouse-source.md) |
| S4 Profiles and lineage | `built` | [Dagster](components/dagster.md), [DataHub](components/datahub.md), [dbt project](components/dbt-project.md), [dbt](components/dbt.md), [document tables](components/document-tables.md), [warehouse source](components/warehouse-source.md) |
| S5 Policy access control | `built` | [central gateway](components/central-gateway.md), which enforces the policy engine's answer |
| S6 Policy administration | `built` | none; the alignment record cites invincible-agent's policy validator and seed job |
| S7 Data exchange | `built` | [central gateway](components/central-gateway.md), [domain broker](components/domain-broker.md), [mesh data client](components/mesh-data-client.md), [Keycloak](components/keycloak.md), [Topaz](components/topaz.md), [MinIO](components/minio.md), [dlt pipeline](components/dlt-pipeline.md), [S3 sensor](components/s3-sensor.md), [Grist](components/grist.md), [JupyterHub](components/jupyterhub.md), and the Restate [server](components/restate-server.md), [worker](components/restate-worker.md), [API sync](components/restate-api-sync.md) and [dlt sync](components/restate-dlt-sync.md) |
| S8 Data product search | `built` | none; the alignment record cites invincible-agent's catalog wrapper |
| S9 Pub/sub | `not built`; the pub/sub phase is `proposed` | [Redpanda](components/redpanda.md), itself `proposed` |
| S10 Performance analytics | `built` | [read audit](components/read-audit.md), [tagged warehouse](components/tagged-warehouse.md), [Dagster](components/dagster.md), [central gateway](components/central-gateway.md), [domain broker](components/domain-broker.md) |
| S11 Life cycle | origin and history: `built`. Retire and recall: `not built`. | [DataHub lineage](components/datahub-lineage.md) |
| S12 Classification | as published metadata: `built`. As a decision input: `proposed`. A standard label form: `not built`. | [classification propagation](components/classification-propagation.md), [warehouse source](components/warehouse-source.md), [DataHub](components/datahub.md), [Topaz](components/topaz.md) |
| S13 Quality | `built` | [contract mapping](components/contract-mapping.md), [Dagster](components/dagster.md), [dbt](components/dbt.md), [dlt pipeline](components/dlt-pipeline.md), [document tables](components/document-tables.md), [the SystemLink source](components/systemlink-source.md) |
| S14 Mediation | `built` | [contract mapping](components/contract-mapping.md), [dbt](components/dbt.md), [document tables](components/document-tables.md), [OTel API sync](components/otel-api-sync.md), [Restate worker](components/restate-worker.md) |
| S15 Instrumentation | probes: `built`. Metrics and tracing: `not built`. | [central gateway](components/central-gateway.md), [domain broker](components/domain-broker.md) |

[Hasura](components/hasura.md), [Superset](components/superset.md) and [Vault](components/vault.md) name none of the fifteen at the pin.

## How the services depend on each other

One service depends on another when it cannot do its job without what the other provides. Each edge below is stated in the record it cites, and carries that record's status.

| Service | Depends on | Why | Status | Records |
|---|---|---|---|---|
| S7 Data exchange | S5 Policy access control | The gateway asks Topaz before it asks any broker for a ticket. A refusal stops the read. | `built` | [need to know](flows/need-to-know.md), steps 5 to 7; [DoD S5, S7](alignment/dod-dmra.md) |
| S7 Data exchange | S1 Identifiers | A read names its dataset by catalog URN, and each ticket gets its own id. | `built` | [DoD S1](alignment/dod-dmra.md); [domain broker](components/domain-broker.md) |
| S5 Policy access control | S6 Policy administration | Grants are reviewed files; a seed job syncs them to Topaz only after the validator passes. | `built` | [DoD S6](alignment/dod-dmra.md) |
| S5 Policy access control | S3 Catalog | The Topaz directory's dataset objects and owner relations are synced from the catalog. | `built` | [need to know](flows/need-to-know.md), roles in the NATO frame |
| S5 Policy access control | S12 Classification | Labels as an input to the read decision. Today the decision reads relations only. | `proposed` | [need to know](flows/need-to-know.md), findings; [DoD S5, S12](alignment/dod-dmra.md) |
| S12 Classification | S4 Lineage | A derived column's label is composed from the labels of the columns its lineage names. | `built` | [classification propagation](components/classification-propagation.md); [DoD S4, S12](alignment/dod-dmra.md) |
| S12 Classification | S3 Catalog | Labels are published to the catalog, for sources and for derived datasets. | `built` | [warehouse source](components/warehouse-source.md); [classification propagation](components/classification-propagation.md) |
| S4 Lineage | S3 Catalog | Lineage from Dagster and dbt is published to the catalog. | `built` | [DataHub lineage](components/datahub-lineage.md); [DoD S4](alignment/dod-dmra.md) |
| S1 Identifiers | S3 Catalog | A dataset's identifier is its catalog URN on the physical platform. | `built` | [DoD S1](alignment/dod-dmra.md); [warehouse source](components/warehouse-source.md) |
| S8 Search | S3 Catalog | The catalog wrapper searches the catalog. | `built` | [DoD S8](alignment/dod-dmra.md) |
| S8 Search | S5 Policy access control | A per-domain `can_view` check when a deployment turns it on; otherwise a gate in code. | `built` | [DoD S8](alignment/dod-dmra.md) |
| S11 Life cycle | S4 Lineage | Origin is read from lineage. | `built` | [DoD S11](alignment/dod-dmra.md) |
| S13 Quality | S14 Mediation | Contract tests are generated from the contract mapping. | `built` | [contract mapping](components/contract-mapping.md); [DoD S13, S14](alignment/dod-dmra.md) |
| S14 Mediation | S2 Semantic services | Local identifier spellings are normalized by a declared rule document, per contract rather than as a service. | `built` | [DoD S2, S14](alignment/dod-dmra.md) |
| S10 Analytics | S15 Instrumentation | The gateway's and broker's counts are read from their `/health` routes. | `built` | [central gateway](components/central-gateway.md); [DoD S10, S15](alignment/dod-dmra.md) |
| S9 Pub/sub | S5 Policy access control | A subscription would need an access decision of its own. Nothing subscribes today. | `not built`; pub/sub is `proposed` | [DoD S9](alignment/dod-dmra.md); [Redpanda](components/redpanda.md) |

Read together, the edges show that the catalog (S3) and policy (S5, S6) carry most of the weight. A read depends on policy, and policy depends on the catalog being right about owners. How that ownership data is kept correct is listed as not established in [the need-to-know flow](flows/need-to-know.md).

## Where to read next

| To find | Read |
|---|---|
| How a read is decided, step by step | [need to know](flows/need-to-know.md) |
| How large data moves | [big data](flows/big-data.md) |
| What agents can and cannot do through the mesh | [agent capabilities](flows/agent-capabilities.md) |
| What one component is, how it is wired, how it signs in, and its plane (the planes are listed in [the README](README.md#planes)) | the component records, listed in [the README](README.md#records) |
| Why the code is the way it is | [the decisions index](decisions/README.md) |
| How the mesh lines up with the two references | [DoD](alignment/dod-dmra.md) and [NATO](alignment/nato-dcra.md) |
| The remedies proposed for gaps | [the roadmap note](ROADMAP.md) |
| What a decision-maker can be shown, and what it rests on | [the evidence walkthrough](walkthroughs/evidence.md) |
| How a Dagster upgrade is qualified | [the sustainment walkthrough](walkthroughs/sustainment.md) |
| Which built claims a recorded run could make measured | [the built-to-measured list](built-to-measured.md) |
| What the records do not establish, in one list | [the not-established list](not-established.md) |

Two walkthroughs follow a question through the records: [a decision-maker asking for evidence](walkthroughs/evidence.md) and [a sustainment case](walkthroughs/sustainment.md), the qualification of a Dagster upgrade.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Services and their statuses | [DoD alignment](alignment/dod-dmra.md), services S1 to S15 | — | the evidence in each row there | as each row there |
| Components per service | the "Services served" row of each component record | — | as each record | — |
| Dependency edges | the records named in each row above | — | as each record | as each row above |
| No record but the built-to-measured list carries `measured` | the status columns of the records in this directory | — | none found outside that list | — |
