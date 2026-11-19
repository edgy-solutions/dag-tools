# Mesh architecture records

These records **describe the mesh as it is**. They do not propose. Where a record names something that is not built, it gives that thing a status of `accepted` or `proposed` and says so. Nothing unbuilt is called built. The one exception is [the roadmap note](ROADMAP.md): it holds the remedies proposed for gaps the records found. Every row in it is `proposed` except the one the architect has accepted, which the note marks.

The records are written for arch-kit, pinned at tag `v0.1.0` (`e0b9b3c40520440a09a4e0e6050ea5017de05848`). arch-kit v0.1.0 has no record type for a *flow* or for *alignment to an outside reference*. Until the kit or its loader declares one, these files follow the kit's view form: a title line, findings first, and a Sources table at the end. **That shape is provisional.**

## Status vocabulary

Every claim carries exactly one status.

| Status | Means | Evidence it needs |
|---|---|---|
| `measured` | A recorded run showed it. A run measures what it exercises: an in-process run in continuous integration measures the code's own behaviour, and is cited as "in CI, in process". A claim about a deployed service or a real backend needs a run against that service or backend, and stays `built` until one is recorded. | The record of the run. A continuous-integration run id is accepted today. Having a test that could measure it is not enough. |
| `built` | Code at the pinned commit does it. | A file and line. A test that pins it, when one exists; when none does, the row says so. |
| `accepted` | A decision record accepts it, and no code does it yet. | The decision record. |
| `proposed` | It is written down as intent, but not accepted or built. | Where it is written. |

A gap is a claim the reference expects and the mesh does not meet. Its status is the status of whatever partial thing exists. A gap with nothing built is written `not built`. So is an absence: a claim that a chart or the code does not provide something is `not built`, never `built`.

Three cases are settled by one rule. An absence (nothing at the pin does it) is `not built`. A test behind an environment gate still counts as a test: the evidence cell names the suite it runs in, the claim stays `built`, and the claim is listed among the built claims a recorded run could make measured, until a recorded run of it exists. A run the census reports with no recorded result is `built`, and the record writes "the census reports"; it becomes `measured` only when the run's record is cited.

## Pins

All evidence is read at these commits. A line number is valid only at its pin.

| Source | Pin | Read status |
|---|---|---|
| dag-tools | `9d89568a70a54482ef9ee0b3d6502555f6b0c4d1` | read directly |
| dag-tools (component records) | `add5bc48021fe6d2c79c24c31072142fc93c8179` | read directly |
| invincible-agent | `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb` | read directly, file by file, for the claims cited |
| iagent-mesh-sdk | `d6a737837c3d0609ace95017c6b4b45b500ea755` | facts supplied by the owning session; not read here |
| iagent-mesh-sdk (decision entries 5 to 8) | `f1a941030bd68f1218ee6122750619ea08ee7276` | facts supplied by the owning session; not read here |
| pub-tools | `90fba4a` | facts supplied by the owning session; not read here |
| pub-tools (decision records) | `ad981b119dc00dc34ab2d187456bf6f4b4205bbb` | file list and status counts read through the hosting service's API; contents not checked |

## References

- **NATO, *Data Centric Reference Architecture for the Alliance*, version 2 (AC/322-D(2025)0056), publicly disclosed.** Its Appendix 9 (Data-Centric Security) frames the need-to-know flow.
- **Chief Digital and Artificial Intelligence Office (CDAO), *Data Mesh Reference Architecture*, version 2.6, March 2024.** It is the primary alignment for the service and principle records.

Both are public. The records cite them by element name and section or paragraph number only. They copy no figure and no passage, and they state each definition in their own words. Neither document is committed here.

## Alignment records

An alignment record sets the mesh against an outside reference. It follows the shape the OpenDDIL lane gives:

1. **The reference:** name, edition, and the terms under which it is cited. A record may cite more than one reference.
2. **One row per reference element**, with four fields:
   - the element's name, with its section or paragraph number;
   - what corresponds in the mesh;
   - how closely it corresponds;
   - the evidence.

These records add a fifth field to each row: the status of what corresponds, in the vocabulary above. The shape gives none, and this README requires one for every claim.

They add a sixth, `Capability`, after the element: the NATO DCRA capability the row falls under. The value is `dcra:A1` to `dcra:A13`, where the number is the DCRA appendix that describes the capability. A row under more than one lists each, separated by commas. A row under none says `none`, so that a blank never reads as an answer. The list of thirteen is declared once, in the mesh composition.

How closely is one of:

| How closely | Means |
|---|---|
| `close` | The mesh does what the element describes, in the role the element gives it. |
| `partial` | The mesh does part of it. The row names the missing part. |
| `different` | The mesh meets the same need another way. The row says how. |
| `none` | Nothing in the mesh corresponds. |

`how closely` describes the correspondence; `status` describes the thing that corresponds. A row can be `close` and `proposed`, if the intent matches and nothing is built.

## Planes

The mesh places each component in one of four planes. The four are the ones the NATO DCRA names in Appendix 14 (its technical architecture), ¶1. The definitions below are this README's own words; the alignment rows are in [the DCRA alignment](alignment/nato-dcra.md#data-planes-appendix-14).

| Plane | Holds | DCRA | Alignment row |
|---|---|---|---|
| `infrastructure` | the compute, storage, network and foundation services that every other plane runs on | Appendix 14, ¶2–4 | [L1](alignment/nato-dcra.md) |
| `operational-data` | the systems that produce and use data in day-to-day work, and the parts that read data out of them or write back to them | Appendix 14, ¶5–7 | [L2](alignment/nato-dcra.md) |
| `data-management` | the shared services that govern data across its life: catalog, lineage, contracts, access decisions and transformation | Appendix 14, ¶8–9 | [L3](alignment/nato-dcra.md) |
| `analytical-data` | where data is explored and analysed, and turned into insight for decisions | Appendix 14, ¶10 | [L4](alignment/nato-dcra.md) |

Each component record's Plane row names exactly one plane, in backticks, and then says why. A component that also carries work of a second plane says so in that explanation; it is still placed in one. A component that nothing wires into the mesh has no plane, and its row says `not established`.

## Ids and connections

Each component record carries an id beginning `CO-` in its title line, and each flow record an id beginning `FL-`. Other records refer to a component or a flow by that id. An id does not change once it is pushed.

A component record's "Connections" table has one row per connection, read from that record's side. The first cell is the other party's id, and it holds that id and nothing else. A party with no record of its own has an id beginning `XE-` (an outside system or element), and a person has one beginning `RL-` (a role). Both are defined once, in the table below. An `XE-` id names a kind of party, not one instance of it: two rows naming `XE-POSTGRES` may mean two different databases. Anything that qualifies the party goes in What passes. The direction cell holds one of four words: `calls` (this side sends the request), `is called by` (the other side sends it), and `reads` or `writes` (this side moves data out of, or into, a party that keeps it). A party that keeps data draws each such row from its own side as `is called by`. When a caller can reach any store of a kind and the records do not say which one a deployment runs, the caller draws the store by its `XE-` id, and no store record lists that caller.

A flow record's step table numbers its steps and names the parties of each step in the same way. The Actor and Target cells each hold one id.

| Id | Names |
|---|---|
| `XE-AGENT-ENGINE` | an agent engine in the invincible-agent chart |
| `XE-AGENT-READER` | an agent engine's in-process graph or vector reader |
| `XE-CALLBACK-API` | a callback endpoint that a durable handler notifies |
| `XE-CLASSIFICATION-DOC` | the classification document a deployment mounts |
| `XE-CLICKHOUSE` | a ClickHouse database |
| `XE-CODE-LOCATION` | a Dagster code-location server |
| `XE-CONSUMER` | a consumer of a dataset: a notebook, an agent, an asset or SDK code |
| `XE-CONTRACT-FILE` | a contract file |
| `XE-CONTRACT-SERVICE` | an endpoint that serves contracts |
| `XE-DAGSTER-STORAGE` | the Postgres database Dagster keeps runs and events in |
| `XE-DATASET-BACKEND` | the store that holds a dataset, any one of `XE-OBJECT-STORE`, `XE-POSTGRES` or `XE-CLICKHOUSE`; used where a step does not depend on which |
| `XE-DBT-PROJECT-DIR` | a dbt project directory |
| `XE-DEFINITIONS-MODULE` | a deployment's Dagster definitions module |
| `XE-DLT-DESTINATION` | the destination a dlt pipeline loads into |
| `XE-DUCKDB-RESOURCE` | a DuckDB resource in a code location |
| `XE-HISTORY-EXPORT` | an exported file of warehouse query history |
| `XE-HISTORY-RESOURCE` | a resource that reads warehouse query history |
| `XE-IO-MANAGER` | an IO manager in a code location |
| `XE-KNOWLEDGE-STORE` | a graph or vector store |
| `XE-LANCE-STORE` | a Lance destination store |
| `XE-OBJECT-STORE` | an S3-compatible object store a deployment names; `CO-MINIO` is one |
| `XE-ODATA-SERVICE` | a source system's OData service |
| `XE-POSTGRES` | a Postgres database that holds a dataset |
| `XE-PROJECTOR` | the invincible-agent projector |
| `XE-QUAL-TOOLING` | the qualification tooling, including its registry client |
| `XE-ROUTE-CACHE` | the gateway's in-process table of broker routes |
| `XE-SDK` | the SDK |
| `XE-SOURCE-DB` | a source database a pipeline reads |
| `XE-SQL-DATABASE` | a SQL database |
| `XE-SQL-WAREHOUSE` | a deployment's SQL warehouse |
| `XE-STAGING-DB` | a staging SQL database |
| `XE-SYSTEMLINK-API` | a test-results source's HTTP API |
| `XE-TARGET-API` | the API a durable handler sends rows to |
| `XE-TOKEN-DELIVERY` | a mechanism that delivers a user's token to a notebook |
| `XE-TOKEN-SERVICE` | an object store's token service |
| `XE-VOLUME` | a persistent volume |
| `RL-OPERATOR` | a person operating a deployment |
| `RL-PERSON` | a person on whose behalf data is read |

## Records

| Record | Kind | State |
|---|---|---|
| [overview.md](overview.md) | overview: what the mesh is, how to read the statuses, how services depend on each other | draft |
| [flows/need-to-know.md](flows/need-to-know.md) | flow | draft |
| [flows/big-data.md](flows/big-data.md) | flow | draft |
| [flows/agent-capabilities.md](flows/agent-capabilities.md) | flow | draft |
| [walkthroughs/evidence.md](walkthroughs/evidence.md) | walkthrough: a decision-maker asking for evidence | draft |
| [walkthroughs/sustainment.md](walkthroughs/sustainment.md) | walkthrough: qualifying a Dagster upgrade | draft |
| [built-to-measured.md](built-to-measured.md) | list: built claims and the test or run that could make each measured | draft |
| [not-established.md](not-established.md) | list: every fact the records do not establish, merged and grouped by what would settle it | draft |
| [components/central-gateway.md](components/central-gateway.md) | component | draft |
| [components/classification-propagation.md](components/classification-propagation.md) | component | draft |
| [components/contract-mapping.md](components/contract-mapping.md) | component | draft |
| [components/dagster.md](components/dagster.md) | component | draft |
| [components/datahub-lineage.md](components/datahub-lineage.md) | component | draft |
| [components/datahub.md](components/datahub.md) | component | draft |
| [components/dbt-project.md](components/dbt-project.md) | component | draft |
| [components/dbt.md](components/dbt.md) | component | draft |
| [components/dlt-pipeline.md](components/dlt-pipeline.md) | component | draft |
| [components/document-tables.md](components/document-tables.md) | component | draft |
| [components/domain-broker.md](components/domain-broker.md) | component | draft |
| [components/grist-ingest.md](components/grist-ingest.md) | component | draft |
| [components/grist.md](components/grist.md) | component | draft |
| [components/hasura.md](components/hasura.md) | component | draft |
| [components/jupyterhub.md](components/jupyterhub.md) | component | draft |
| [components/keycloak.md](components/keycloak.md) | component | draft |
| [components/mesh-data-client.md](components/mesh-data-client.md) | component | draft |
| [components/minio.md](components/minio.md) | component | draft |
| [components/otel-api-sync.md](components/otel-api-sync.md) | component | draft |
| [components/read-audit.md](components/read-audit.md) | component | draft |
| [components/redpanda.md](components/redpanda.md) | component | draft |
| [components/restate-api-sync.md](components/restate-api-sync.md) | component | draft |
| [components/restate-dlt-sync.md](components/restate-dlt-sync.md) | component | draft |
| [components/restate-server.md](components/restate-server.md) | component | draft |
| [components/restate-worker.md](components/restate-worker.md) | component | draft |
| [components/s3-sensor.md](components/s3-sensor.md) | component | draft |
| [components/superset.md](components/superset.md) | component | draft |
| [components/systemlink-source.md](components/systemlink-source.md) | component | draft |
| [components/tagged-warehouse.md](components/tagged-warehouse.md) | component | draft |
| [components/topaz.md](components/topaz.md) | component | draft |
| [components/vault.md](components/vault.md) | component | draft |
| [components/warehouse-source.md](components/warehouse-source.md) | component | draft |
| [decisions/README.md](decisions/README.md) | decisions index: how census decisions are addressed, and every decision record | draft |
| [ROADMAP.md](ROADMAP.md) | roadmap note (the kit's format) | draft |
| [alignment/dod-dmra.md](alignment/dod-dmra.md) | alignment: the DoD reference's principles and services | draft |
| [alignment/nato-dcra.md](alignment/nato-dcra.md) | alignment: the NATO reference's security roles and planes | draft |
