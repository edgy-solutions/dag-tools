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

A row carries one status. A claim whose parts differ in status is split into one row per part, and each row names its part. An alignment row is one outside element and stays one row: its status is that of what corresponds, and the parts that differ are described, without backticks, in its "How closely" cell. A row that counts or points at records is bookkeeping, not a claim about the mesh, and sits in a table with no Status column.

A gap is a claim the reference expects and the mesh does not meet. A gap carries the status of the condition it describes, because that is what its evidence supports: a gap that says what the code does, or that nothing at the pin does a thing, and cites the lines that show it, is `built`. That status says the condition is established from code; it never says the remedy is built. The remedy is a separate row in [the roadmap](ROADMAP.md), with its own status, usually `proposed`. Each gap carries exactly one status, written once at its end after "Status:". Any other status word inside a gap's text is written without backticks.

Only the four statuses above are statuses. An absence established from code (nothing at the pin does it, and the lines that show it are cited) is a condition of the code, and is `built`. Something intended and not built is `proposed`. Something that could not be established is an entry in the record's not-established list, not a claim with a status. Two cases about tests and runs are settled the same way. A test behind an environment gate still counts as a test: the evidence cell names the suite it runs in, the claim stays `built`, and the claim is listed among the built claims a recorded run could make measured, until a recorded run of it exists. A run the census reports with no recorded result is `built`, and the record writes "the census reports"; it becomes `measured` only when the run's record is cited.

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

They add a sixth, `Capability`, after the element: the NATO DCRA capability the row falls under. The value is `dcra:A1` to `dcra:A13`, where the number is the DCRA appendix that describes the capability. A row under more than one lists each, separated by commas. A row under none says `none`, so that a blank never reads as an answer. The thirteen are the titles of the DCRA's Appendices 1 to 13, and each alignment record lists them among the elements it uses. Declaring the list once for the whole mesh is `proposed`; no file in this directory declares it.

How closely is one of:

| How closely | Means |
|---|---|
| `close` | The mesh does what the element describes, in the role the element gives it. |
| `partial` | The mesh does part of it. The row names the missing part. |
| `different` | The mesh meets the same need another way. The row says how. |
| `none` | Nothing in the mesh corresponds. |

`how closely` describes the correspondence; `status` describes the thing that corresponds. A row can be `close` and `proposed`, if the intent matches and nothing is built.

## Planes

The mesh places each component in one of four planes. The plane list is with the alignment records, in [the DCRA alignment](alignment/nato-dcra.md#plane-list). It gives each plane's id, its name as the DCRA prints it, its paragraphs in Appendix 14 and its alignment row.

Each component record's Plane row names exactly one plane, in backticks, and then says why. A component that also carries work of a second plane says so in that explanation; it is still placed in one. A component that nothing wires into the mesh is placed by the role its record establishes for it. Where the record establishes no role, the component has no plane, and its row says not established.

## Ids and connections

Each component record carries an id beginning `CO-` in its title line, and each flow record an id beginning `FL-`. Other records refer to a component or a flow by that id. An id does not change once it is pushed.

A component record's "Connections" table has one row per connection, read from that record's side. The first cell is the other party's id, and it holds that id and nothing else. A party with no record of its own has an id beginning `XE-` (an outside system or element), and a person has one beginning `RL-` (a role). Both are declared once, under [Element ids](#element-ids). An `XE-` id names a kind of party, not one instance of it: two rows naming `XE-POSTGRES` may mean two different databases. Anything that qualifies the party goes in What passes. The direction cell holds one of four words: `calls` (this side sends the request), `is called by` (the other side sends it), and `reads` or `writes` (this side moves data out of, or into, a party that keeps it). A party that keeps data draws each such row from its own side as `is called by`. When a caller can reach any store of a kind and the records do not say which one a deployment runs, the caller draws the store by its `XE-` id, and no store record lists that caller.

A flow record's step table numbers its steps and names the parties of each step in the same way. The Actor and Target cells each hold one id.

## Element ids

Each id that names a party with no record of its own is declared here once. Kind is `outside element` for an `XE-` id and `role` for an `RL-` id.

| Id | Kind | What it is |
|---|---|---|
| `XE-AGENT-ENGINE` | outside element | an agent engine in the invincible-agent chart |
| `XE-AGENT-READER` | outside element | an agent engine's in-process graph or vector reader |
| `XE-CALLBACK-API` | outside element | a callback endpoint that a durable handler notifies |
| `XE-CI` | outside element | the dag-tools repository's continuous integration workflows |
| `XE-CLASSIFICATION-DOC` | outside element | the classification document a deployment mounts |
| `XE-CLICKHOUSE` | outside element | a ClickHouse database |
| `XE-CODE-LOCATION` | outside element | a Dagster code-location server |
| `XE-CONSUMER` | outside element | a consumer of a dataset: a notebook, an agent, an asset or SDK code |
| `XE-CONTRACT-FILE` | outside element | a contract file |
| `XE-CONTRACT-SERVICE` | outside element | an endpoint that serves contracts |
| `XE-DAGSTER-STORAGE` | outside element | the Postgres database Dagster keeps runs and events in |
| `XE-DATASET-BACKEND` | outside element | the store that holds a dataset, any one of `XE-OBJECT-STORE`, `XE-POSTGRES` or `XE-CLICKHOUSE`; used where a step does not depend on which |
| `XE-DBT-PROJECT-DIR` | outside element | a dbt project directory |
| `XE-DEFINITIONS-MODULE` | outside element | a deployment's Dagster definitions module |
| `XE-DLT-DESTINATION` | outside element | the destination a dlt pipeline loads into |
| `XE-DUCKDB-RESOURCE` | outside element | a DuckDB resource in a code location |
| `XE-HISTORY-EXPORT` | outside element | an exported file of warehouse query history |
| `XE-HISTORY-RESOURCE` | outside element | a resource that reads warehouse query history |
| `XE-IO-MANAGER` | outside element | an IO manager in a code location |
| `XE-KNOWLEDGE-STORE` | outside element | a graph or vector store |
| `XE-LANCE-STORE` | outside element | a Lance destination store |
| `XE-OBJECT-STORE` | outside element | an S3-compatible object store a deployment names; `CO-MINIO` is one |
| `XE-ODATA-SERVICE` | outside element | a source system's OData service |
| `XE-POSTGRES` | outside element | a Postgres database that holds a dataset |
| `XE-PROJECTOR` | outside element | the invincible-agent projector |
| `XE-QUAL-TOOLING` | outside element | the qualification tooling, including its registry client |
| `XE-ROUTE-CACHE` | outside element | the gateway's in-process table of broker routes |
| `XE-RUN-WINDOW-EXPORT` | outside element | an exported file of run windows |
| `XE-SDK` | outside element | the SDK |
| `XE-SOURCE-DB` | outside element | a source database a pipeline reads |
| `XE-SQL-DATABASE` | outside element | a SQL database |
| `XE-SQL-WAREHOUSE` | outside element | a deployment's SQL warehouse |
| `XE-STAGING-DB` | outside element | a staging SQL database |
| `XE-SYSTEMLINK-API` | outside element | a test-results source's HTTP API |
| `XE-TARGET-API` | outside element | the API a durable handler sends rows to |
| `XE-TOKEN-DELIVERY` | outside element | a mechanism that delivers a user's token to a notebook |
| `XE-TOKEN-SERVICE` | outside element | an object store's token service |
| `XE-VOLUME` | outside element | a persistent volume |

### Roles

A role is what a person does in the mesh, not who the person is, and one person may act in several. The roles are named after the actors the DoD reference lists in §3, "DMRA Use Cases and Actors/Roles". Each definition is in these records' own words. A walkthrough step's Actor cell holds one of these ids, and so does a flow step's Actor or Target cell when a person acts. The reference's Domain Team, Data Engineer and Enabling Team have no id. A domain team's product work is written under `RL-OWNER`, and an id for any of the three is added when a record has a step that one performs.

| Id | Kind | What it is | Actor in the DoD reference |
|---|---|---|---|
| `RL-CONSUMER` | role | a person who reads a data product, or a figure made from one, and may ask what stands behind it. The decision-maker of WT-EVIDENCE acts in this role | Data Consumer (§3) |
| `RL-OPERATOR` | role | the platform operator: a person who builds or operates the mesh platform, including a deployment of it. Sets what the chart and code leave to a deployment, maintains the builds of its images, and runs the qualification tooling | Data Platform Engineer (§3) |
| `RL-OWNER` | role | the data product owner: accountable for one data product, and writes the configuration it is built from, such as its source declaration, its contract mapping and its pipeline settings | Data Product Owner, within the Domain Team (§3) |
| `RL-PERSON` | role | a consumer as the read path sees one: a person on whose behalf data is read, under that person's verified identity. A flow step that turns on the identity names this id rather than `RL-CONSUMER` | Data Consumer (§3) |
| `RL-STEWARD` | role | accountable for a governance decision about data, such as widening a column's label or ratifying a classification value | Data Governance Group (§3); the reference also names a data steward under quality management (§4, item 13) |

## Records

| Record | Kind | State |
|---|---|---|
| [overview.md](overview.md) | overview: what the mesh is, how to read the statuses, how services depend on each other | draft |
| [flows/need-to-know.md](flows/need-to-know.md) | flow | draft |
| [flows/big-data.md](flows/big-data.md) | flow | draft |
| [flows/agent-capabilities.md](flows/agent-capabilities.md) | flow | draft |
| [flows/source-to-use.md](flows/source-to-use.md) | flow | draft |
| [flows/label-propagation.md](flows/label-propagation.md) | flow | draft |
| [walkthroughs/evidence.md](walkthroughs/evidence.md) | walkthrough: a decision-maker asking for evidence | draft |
| [walkthroughs/sustainment.md](walkthroughs/sustainment.md) | walkthrough: qualifying a Dagster upgrade | draft |
| [built-to-measured.md](built-to-measured.md) | list: built claims and the test or run that could make each measured | draft |
| [not-established.md](not-established.md) | list: every fact the records do not establish, merged and grouped by what would settle it | draft |
| [glossary.md](glossary.md) | glossary: one line per mesh term a reader meets in these records | draft |
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
