# Glossary

One line per mesh term a reader meets in these records. Each definition is in these records' own words and says what the term means where the records use it. A line adds no claim and carries no status, so the table has no Status column; the claims, and their statuses, are in the records the last column names. That column names records by id, or names the README section that defines a term of the record system itself. Status words are defined in [the README](README.md#status-vocabulary), and the four planes are listed in [the DCRA alignment](alignment/nato-dcra.md#plane-list).

| Term | What it means | Where a reader meets it |
|---|---|---|
| agent | A program in the invincible-agent agent fleet that reaches mesh stores through the SDK | FL-AGENT; AL-DCRA |
| alignment record | A record that sets the mesh against an outside reference, one row per element of that reference | AL-DMRA; AL-DCRA; [README, Alignment records](README.md#alignment-records) |
| allowed columns | The part of a narrowing that lists the columns a principal may read; the ticket and the receipt carry it | CO-GATEWAY; CO-TOPAZ; CO-BROKER; CO-CLIENT |
| as-of | The version of the data a read saw, as the source itself records it. It is not the time the ticket was issued, and where it cannot be read it is left absent rather than guessed | CO-WAREHOUSE-SOURCE; CO-CLIENT; FL-BIG |
| asset of record | The physical dataset, in its own store, that the catalog registers and a read is authorized for. The Dagster asset that builds it is attached to it as lineage | CO-CONTRACT-MAPPING; CO-DATAHUB-LINEAGE; AL-DMRA; WT-EVIDENCE |
| backfill | A source run that reads the whole history part by part, as a job of its own, and can resume past the parts already done | CO-SYSTEMLINK-SOURCE |
| capability | A function the NATO reference describes in one of its appendices; each alignment row names the capabilities it falls under | AL-DCRA; AL-DMRA |
| catalog | The shared register of datasets, their lineage and their labels. In the mesh it is DataHub | CO-DATAHUB; CO-WAREHOUSE-SOURCE; CO-CLASSIFICATION-PROPAGATION; AL-DMRA |
| central gateway | The mesh's single entry point for a dataset read. It settles who the read is for, asks the policy engine, finds the broker that serves the dataset and hands back that broker's ticket. The data itself does not pass through it | CO-GATEWAY; FL-NTK; FL-BIG |
| chart | A Helm package that deploys a set of services. A component record describes what a chart provides and what a deployment may set, never one deployment | CO-DAGSTER; CO-RESTATE; CO-RESTATE-WORKER; CO-SUPERSET |
| classification | The labels a column carries, one per dimension, read against a single document of dimensions that policy owns | CO-WAREHOUSE-SOURCE; CO-CLASSIFICATION-PROPAGATION; AL-DMRA; FL-LABEL-PROPAGATION |
| clearance | What a person may read on one dimension. On the ordered dimension it is a ceiling: the cleared value and every value below it | FL-NTK |
| code location | A unit of Dagster definitions that Dagster loads and serves on its own. A producer runs its pipelines in one | CO-DAGSTER; CO-DATAHUB-LINEAGE; CO-GRIST-INGEST |
| component | A part of the mesh that has a record of its own and an id beginning CO-. The dag-tools library's building blocks configured in YAML are among them | [README, Ids and connections](README.md#ids-and-connections) |
| connection | One row of a component record's Connections table: the other party by id, and one of four directions | [README, Ids and connections](README.md#ids-and-connections) |
| consumer | A person or program that reads a data product | RL-CONSUMER; CO-CLIENT; FL-BIG; WT-EVIDENCE |
| contract | A document that states a table's columns, its grain, its scope and the rules its rows must meet, each rule under an id of its own | CO-CONTRACT-MAPPING; CO-DBT; AL-DMRA |
| contract mapping | A statement of which source column feeds each contract column and how it is transformed. A dbt model and its tests are written from it, and no data moves | CO-CONTRACT-MAPPING |
| credential | What opens a store for a read. The broker mints one for a single read where the backend allows it; other backends are read with the producer's own | CO-BROKER; CO-CLIENT; CO-MINIO; FL-NTK |
| data product | A dataset a domain publishes for others to read, with an owner accountable for it | RL-OWNER; RL-CONSUMER; AL-DMRA |
| data product owner | The role accountable for one data product | RL-OWNER |
| decision record | A record of decisions that shape the mesh, kept in the repository that owns each one and cited by repository and commit. Decision records carry no id | [decisions](decisions/README.md) |
| deny by default | The rule that a column with no label is read as the most restricted in every dimension, so that nobody reads it rather than everybody | CO-WAREHOUSE-SOURCE; FL-NTK; FL-AGENT; AL-DMRA |
| deny list | A list of subjects the gateway refuses before it asks the policy engine | CO-GATEWAY; FL-NTK |
| digest | A hash that names an image's exact bytes. A tag can be moved to other bytes; a digest cannot | WT-SUSTAIN |
| dimension | One axis of classification. Exactly one dimension is ordered from least to most restricted; each other one is a set of values | CO-CLASSIFICATION-PROPAGATION; CO-WAREHOUSE-SOURCE; AL-DMRA |
| document table | A table built from a bundle of XML or JSON documents through a mapping file | CO-DOCUMENT-TABLES; CO-MINIO |
| domain | The part of the organization that answers for its own data and its own pipelines | AL-DMRA |
| domain broker | The producer-side service that registers the producer's datasets with the gateway and, for one read, returns a ticket. It holds and serves no data | CO-BROKER; FL-NTK; FL-BIG |
| downgrade | A declared rule, with a named owner and a reason, that widens a derived column's label. It is the only way a label is loosened | CO-CLASSIFICATION-PROPAGATION; WT-EVIDENCE; RL-STEWARD; FL-LABEL-PROPAGATION |
| durable step | A step whose result is written to a journal, so that a retry or a restart does not do it again | CO-RESTATE; CO-RESTATE-WORKER |
| enforcement point | A place where an access decision is applied: the gateway before any data path, the broker and the store on the data path, and the agent services outside the data read | FL-NTK; CO-GATEWAY; CO-CLIENT; AL-DCRA |
| execution group | The telemetry rows that belong to one execution, gathered so that their calls are rendered and sent together | CO-OTEL-API-SYNC |
| fallback | In a rendered plan, a call made only when a named call fails, and scoped so that it cannot touch items that succeeded | CO-OTEL-API-SYNC |
| finding | A numbered observation in a record about what the code at the pin does, often a limit or a defect | CO-BROKER; CO-DATAHUB; FL-BIG |
| fleet | The set of agent engines invincible-agent runs. In WT-SUSTAIN, the set of images one build produces | FL-AGENT; WT-SUSTAIN |
| gap | A claim an outside reference expects that the mesh does not meet. It carries one status, written once at its end | [README, Status vocabulary](README.md#status-vocabulary) |
| glossary term | A catalog term attached to a column. Labels reach the catalog as glossary terms; the word does not mean an entry of this record | CO-WAREHOUSE-SOURCE; CO-CLASSIFICATION-PROPAGATION; AL-DMRA |
| grain | The set of columns that makes one row of a table unique | CO-CONTRACT-MAPPING; CO-DBT; AL-DMRA |
| identity | Who a request is for, as the gateway settles it | CO-GATEWAY; CO-KEYCLOAK; FL-NTK |
| identity provider | The service that signs the tokens a request carries. In the mesh it is Keycloak | CO-KEYCLOAK |
| image record | The record of one commit's image digests, written once by continuous integration and never rewritten | WT-SUSTAIN; XE-CI |
| incremental | A source run that reads only what is new or changed since the last run | CO-SYSTEMLINK-SOURCE |
| ingest run | One row per source run, failed or not, holding its counts and the kind of error, never the error's message | CO-SYSTEMLINK-SOURCE; WT-EVIDENCE |
| inventory | The list of a repository's assets and their properties at one commit, made by a survey | WT-SUSTAIN |
| journal | The durable engine's log of completed steps, replayed after a restart | CO-RESTATE; CO-OTEL-API-SYNC |
| label | The value a column carries on one dimension of classification, published to the catalog | CO-WAREHOUSE-SOURCE; CO-CLASSIFICATION-PROPAGATION; FL-NTK; AL-DMRA; FL-LABEL-PROPAGATION |
| lake | No record describes a lake. The word appears only inside a catalog platform name. The stores the records describe are object stores and warehouses | CO-DATAHUB-LINEAGE |
| lineage | The record of which datasets, and which columns, another was computed from | CO-DATAHUB-LINEAGE; CO-DATAHUB; FL-SOURCE-TO-USE; WT-EVIDENCE |
| materialization | A Dagster run's production of an asset's data, and the event that records it | CO-CONTRACT-MAPPING; CO-CLASSIFICATION-PROPAGATION; CO-DATAHUB-LINEAGE |
| mesh | The arrangement in which data stays with the team that produces it and other teams read it under a decision made for each read | [overview](overview.md) |
| mesh data client | The consumer-side library that asks the gateway, takes the ticket and reads straight from the store | CO-CLIENT |
| narrowing | The part of the policy engine's answer that limits a permitted read to some columns or some rows | CO-TOPAZ; CO-GATEWAY; CO-BROKER; FL-NTK |
| normalizer | A declared rule that puts an identifier in one canonical form, so that two tables can be joined on it | CO-CONTRACT-MAPPING; CO-DBT-PROJECT |
| not established | Written where the evidence the records hold cannot settle a fact. Such facts are gathered in one list | [not established](not-established.md) |
| object store | A store that keeps files by key in buckets and is reached through the S3 interface | CO-MINIO; CO-CLIENT; CO-BROKER; CO-S3-SENSOR |
| observation | A Dagster event that records what was seen of an asset without producing it | CO-WAREHOUSE-SOURCE; CO-CLASSIFICATION-PROPAGATION |
| on behalf of | Said of a read a program or an agent makes for a named person, under that person's identity | FL-AGENT; CO-CLIENT; RL-PERSON |
| ontology | A model of the kinds of things the agents reason about and how they relate, served by an ontology service | FL-AGENT; AL-DMRA; AL-DCRA |
| outside element | A party with no record of its own, named by an id beginning XE-. The id names a kind of party, not one instance | [README, Element ids](README.md#element-ids) |
| pin | A full commit sha at which evidence is read; a line number holds only at its pin. In a qualification, the commit a manifest fixes each repository at | [README, Pins](README.md#pins); WT-SUSTAIN |
| plan | A set of calls rendered in full before anything is sent, so that a dry run shows its exact bytes. A contract mapping also builds a plan, of the columns it writes | CO-OTEL-API-SYNC; CO-RESTATE-WORKER; CO-CONTRACT-MAPPING |
| plane | One of four layers the mesh places each component in. Each component record names exactly one, or says not established where the record establishes no role for the component | AL-DCRA; [README, Planes](README.md#planes) |
| plane `analytical-data` | Where data is explored and analysed and turned into insight | AL-DCRA |
| plane `data-management` | The shared services that govern data through its life: catalog, lineage, contracts, access decisions and transformation | AL-DCRA |
| plane `infrastructure` | Compute, storage, network and the foundation services every other plane runs on | AL-DCRA |
| plane `operational-data` | The systems that produce and use data in day-to-day work | AL-DCRA |
| platform operator | The role that builds or operates the mesh platform | RL-OPERATOR |
| policy engine | The service the gateway asks whether a principal may read a dataset. In the mesh it is Topaz | CO-TOPAZ; CO-GATEWAY |
| posture | The gateway's mode for settling identity. Under observe, the identity comes from a header and what enforce would refuse is counted; under enforce, it is read from a verified token | CO-GATEWAY; CO-KEYCLOAK; FL-NTK |
| preflight | The checks, on each side of a qualification, that the deployment is the one pinned. Each check becomes an entry in a report | WT-SUSTAIN |
| presigned location | A store location with the authority to read it built in: a credential in the shape of an address, which is why the receipt leaves it out | CO-CLIENT |
| principal | Whoever a read is decided for: a person, a pipeline or a service | CO-GATEWAY; CO-KEYCLOAK; CO-TAGGED-WAREHOUSE; CO-READ-AUDIT |
| probe | Generated code that exercises a class of assets that cannot run as it stands, by writing and reading back through the real input/output manager | WT-SUSTAIN; CO-DAGSTER |
| producer | The team that makes a dataset and runs its own code location and broker | CO-BROKER; FL-NTK; FL-BIG |
| propagation | Deriving a downstream column's label from the labels of the source columns it read. It only ever tightens | CO-CLASSIFICATION-PROPAGATION; CO-DATAHUB |
| pub/sub | Publish-and-subscribe delivery between producers and consumers. AL-DMRA gives its status | AL-DMRA; CO-REDPANDA |
| qualification | The evidence gathered before a Dagster upgrade is allowed, from survey to verdict | WT-SUSTAIN; CO-DAGSTER |
| query tag | An attribution carried with each warehouse query, naming who it was made for | CO-TAGGED-WAREHOUSE; CO-READ-AUDIT |
| read audit | A comparison of the warehouse's query history with Dagster runs. The reads nothing explains are its result | CO-READ-AUDIT; CO-TAGGED-WAREHOUSE; FL-SOURCE-TO-USE; WT-EVIDENCE |
| readiness | The test that all of an execution group's telemetry has arrived before its plan is sent | CO-OTEL-API-SYNC |
| receipt | The part of a ticket a consumer keeps beside an answer: a fixed list of the URN and identifying fields, with no credential and no physical location | CO-CLIENT; FL-BIG; FL-AGENT; FL-SOURCE-TO-USE; WT-EVIDENCE |
| refusal | A read the gateway or the broker declines, returned with a status code and a named reason | CO-GATEWAY; CO-BROKER |
| registry | In a qualification, the store a survey publishes to: each build's artifacts written once, and the pointer to the latest written last. In a dbt project, the shared table that turns a name into an asset key | WT-SUSTAIN; CO-MINIO; CO-DBT-PROJECT |
| representative | The one asset per class that a qualification launches | WT-SUSTAIN |
| role | What a person does in the mesh, named by an id beginning RL-. One person may act in several | [README, Roles](README.md#roles) |
| row filter | The part of a narrowing that limits which rows a principal may read. A dlt pipeline's extraction filters share the name and are a different thing | CO-GATEWAY; CO-TOPAZ; CO-DLT-PIPELINE |
| scope | The column a read filters on to take one part of a contract's table | CO-CONTRACT-MAPPING |
| SDK | The agents' interface to the mesh, owned outside dag-tools | FL-AGENT; AL-DCRA |
| seal | The invincible-agent decisions' word for a test that holds a stated rule | FL-NTK; FL-AGENT |
| service | One of the fifteen services of the DoD reference, numbered S1 to S15 | AL-DMRA |
| source asset | A Dagster asset whose data something else produces, so Dagster observes it rather than builds it | CO-WAREHOUSE-SOURCE |
| source kind | A kind of source the dlt pipeline can read, chosen by the type its configuration names | CO-DLT-PIPELINE; CO-SYSTEMLINK-SOURCE |
| staging | In a qualification, preparing the upstream data a run will read. For OTel API sync, the destination extracted telemetry is written to | WT-SUSTAIN; CO-OTEL-API-SYNC |
| status words | `measured`, `built`, `accepted` and `proposed`; every claim carries exactly one. A gap carries the status of the condition it describes | [README, Status vocabulary](README.md#status-vocabulary) |
| steward | The role accountable for a governance decision about data | RL-STEWARD |
| survey | Loading each repository's code locations at a commit and recording an inventory. If any location fails to load, nothing is published | WT-SUSTAIN |
| table max | The label a derived column takes when no lineage says which source columns it read: the composition of every labelled column of every declared source | CO-CLASSIFICATION-PROPAGATION; FL-LABEL-PROPAGATION |
| ticket | What the broker returns for one read of one dataset: the source type, the physical location and, where the backend allows it, a credential minted for that read alone. It also carries an id and the time it was issued | CO-BROKER; CO-CLIENT; FL-BIG; WT-EVIDENCE |
| token | A signed statement of identity, issued by the identity provider and carried with a request | CO-KEYCLOAK; CO-GATEWAY; CO-CLIENT; CO-JUPYTERHUB |
| URN | A dataset's identifier in the catalog. A consumer names the dataset it wants by its URN | CO-GATEWAY; CO-DATAHUB; CO-CONTRACT-MAPPING; FL-BIG |
| verdict | The outcome of a qualification, go or no-go, from comparing the two sides | WT-SUSTAIN |
| warehouse | A SQL database that holds analytical tables | CO-TAGGED-WAREHOUSE; CO-WAREHOUSE-SOURCE; CO-READ-AUDIT |
| warehouse source | The component that declares one classified source table and observes it, moving no data | CO-WAREHOUSE-SOURCE |
