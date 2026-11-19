# What is not established: the merged list

This page gathers, in one place, every fact these records leave open. Each record keeps its own "Not established" section. This page merges them, removes duplicates, and groups each fact by what would settle it. Every entry links to each record that carries it.

**This page settles nothing and carries no claim of its own.** An entry here has no status. The claim beside it in the linked record keeps that record's status, and the record holds where the two differ. Statuses are those of [the README](README.md), and line numbers are at the pins listed there. The record shape is provisional.

## How the list was built

- **Sources.**
  - Every "Not established" section in this directory: 186 items in 43 records.
  - Thirteen lines in nine files that say "not established" outside such a section. The invincible-agent decision files carry more of these, in their status lines, and groups C8 to C12 point to those whole.
  - Items 1 to 35 of the lane's twice-daily reports, each of which already comes from a record.
- **Merged.** One fact carried by several records is one entry, and it links to each of them.
- **Grouped by what would settle it:**
  1. a deployment's settings, which these records do not describe by rule;
  2. a recorded run against an outside system, or that system's own documentation;
  3. a recorded run against the mesh itself;
  4. a decision nobody has made yet;
  5. another repository or lane;
  6. a further read of code.
- **Result.** 79 entries. Two decisions in group 4 were closed by the architect's ruling of 2026-11-19, and one entry was added in group 5.

## Findings

1. **Most of what is open is a deployment fact.** Group 1 holds 35 entries. A component record describes what the chart and code provide by default and what a deployment may set. It never describes a particular deployment, so these entries stay open by design. A deployment's own record could close them.
2. **Few claims are `measured`, and only in CI, in process.** The architect ruled on 2026-11-19 that an in-process run in continuous integration measures what it exercises, and that a run id is accepted today. [The built-to-measured list](built-to-measured.md) names the claims that ruling makes `measured`. Group 3 lists the runs that would change the rest.
3. **The read path's open facts sit at policy and the catalog.** These are which policy Topaz runs, what its answer carries, and how the catalog's ownership data is kept correct. The read decision depends on all three, as [the overview](overview.md#how-the-services-depend-on-each-other) shows.
4. **The invincible-agent decision records carry most of the partial reads.** Group 6 lists them. Ten of those ADRs carry a header that disagrees with the code at the pin, in one direction or the other.

## 1. Facts of a deployment

These are settled by a deployment's settings or its own record.

| # | Not established | Records |
|---|---|---|
| D1 | Which identity provider a deployment runs, and which issuers, audiences and key source it sets. How long its service tokens live. Which claim carries the entitlement key. | [Keycloak](components/keycloak.md) |
| D2 | Which policy a deployment loads into Topaz, and so whether any decision narrows. Where the engine runs, and how the gateway reaches it. | [Topaz](components/topaz.md); [central gateway](components/central-gateway.md); [need to know](flows/need-to-know.md), finding 2 |
| D3 | How many gateway replicas run, and whether they share one cache. Whether any deployment sets a posture other than the default. | [central gateway](components/central-gateway.md) |
| D4 | Which chart deploys the broker in practice, and with what settings. How many minted SQL accounts a database accumulates. | [domain broker](components/domain-broker.md) |
| D5 | Whether a deployment's client-authentication rules let a minted Postgres role sign in other than by password. Postgres applies the expiry to password sign-in only. | [roadmap note](ROADMAP.md) |
| D6 | Which object store a deployment runs, and whether it is MinIO. Which credential names each client is given, and whether they are one key. Whether the store refuses plain HTTP. Whether document tables uses an object store at all, and with which credentials for the S3 client and Lance. | [MinIO](components/minio.md); [document tables](components/document-tables.md) |
| D7 | Whether a deployment fills its Kubernetes Secrets from Vault, and how. Which secrets go into each `envFrom` source. | [Vault](components/vault.md); [Grist](components/grist.md) |
| D8 | Which handlers a worker selects, and with which credentials. Whether credentials are passed as plain values in `env` or as secret references. Which callers can reach the worker's port. How many worker replicas run; the concurrency cap multiplies by replicas. | [Restate worker](components/restate-worker.md); [OTel API sync](components/otel-api-sync.md) |
| D9 | How an external Restate server, the default layout, is configured: version, authentication and reachability. Whether a network policy limits the ingress and admin ports. How the journal volume is backed up, and how long the journal is kept. | [Restate server](components/restate-server.md) |
| D10 | Which ingress address a deployment sets, and the exact path form the ingress needs. Whether that address is a one-way send or a call that waits for the handler. Which Restate services the three dlt-sync endpoints reach, and whether one worker hosts them. | [Restate API sync](components/restate-api-sync.md); [Restate dlt sync](components/restate-dlt-sync.md) |
| D11 | The default status of sensors in a running instance. Whether run monitoring is on; the abort sensor does not fire for a run that never reports a failure. | [Restate dlt sync](components/restate-dlt-sync.md) |
| D12 | Whether a schedule, sensor or automation condition is attached to each component that ships none or ships one stopped, and whether it is started. This covers OTel API sync, the Restate API sync dispatch asset, the SystemLink jobs and the warehouse source observation. | [OTel API sync](components/otel-api-sync.md); [Restate API sync](components/restate-api-sync.md); [SystemLink source](components/systemlink-source.md); [warehouse source](components/warehouse-source.md) |
| D13 | Which Dagster version a deployment runs. Whether a sign-in is in front of the webserver, and which. Whether anything besides the qualification tooling calls the GraphQL API. | [Dagster](components/dagster.md) |
| D14 | Whether a deployment runs the probe code location, and with which probe configuration. | [Dagster](components/dagster.md); [sustainment walkthrough](walkthroughs/sustainment.md) |
| D15 | Which catalog version a deployment runs, and where. Whether a catalog server address and token are set, how the token is delivered, and how long it lives. Whether the catalog runs with token authentication on. | [DataHub](components/datahub.md); [DataHub lineage](components/datahub-lineage.md); [classification propagation](components/classification-propagation.md); [warehouse source](components/warehouse-source.md) |
| D16 | Whether two writers address the same dataset in one deployment. | [DataHub](components/datahub.md), finding 4 |
| D17 | Whether a deployment runs the lineage sensor, the propagation sensor or the read audit. Each is off or absent by default. When the audit runs, and over which history. | [evidence walkthrough](walkthroughs/evidence.md); [document tables](components/document-tables.md); [read audit](components/read-audit.md) |
| D18 | Which dbt projects run, against which warehouse, with which profile credential. Whether the curated package is installed, and with which variables. Whether the contract-mapping SQL route runs. | [dbt](components/dbt.md); [dbt project](components/dbt-project.md) |
| D19 | Whether the dbt target directory outlives a run or a pod restart. It holds the ingest recipe that carries the catalog token, and the demo project's target sits under the Dagster home. | [dbt project](components/dbt-project.md); [dbt](components/dbt.md) |
| D20 | Which contracts, mappings and dbt projects contract mapping is pointed at. What is behind a contract URL, and what credential its headers expect. Whether the output directory belongs to a project that later builds run. | [contract mapping](components/contract-mapping.md) |
| D21 | Which resource the warehouse source binds, which warehouse it reaches, and with which credential. Which account the tagged warehouse connects as, and what it may do. What the session tag statement does on the warehouse. | [warehouse source](components/warehouse-source.md); [tagged warehouse](components/tagged-warehouse.md) |
| D22 | Which destinations dlt uses, and with which credentials. Whether source accounts are read-only. The components set no such limit. The dlt-sync handlers also write back to the source with the worker's credentials. | [dlt pipeline](components/dlt-pipeline.md); [Restate dlt sync](components/restate-dlt-sync.md) |
| D23 | Which credential `S3Resource` is given, and whether it is read-only. Which component is wired to the S3 sensor, and which job it names as its target. How the arrow resource's four required fields would be supplied once that component builds. | [S3 sensor](components/s3-sensor.md), finding 8 |
| D24 | Whether a deployment mounts the dimensions document. Which sources, derived assets and downgrade rules it declares. | [warehouse source](components/warehouse-source.md); [classification propagation](components/classification-propagation.md) |
| D25 | Which SystemLink server versions, status vocabularies and limits a deployment has. The code ships conservative defaults and no measured values. Which destination it loads into, and with which credentials. | [SystemLink source](components/systemlink-source.md) |
| D26 | Which telemetry store, staging database and Restate ingress OTel API sync points at, and with which credentials. | [OTel API sync](components/otel-api-sync.md) |
| D27 | Whether a deployment runs a Grist server, and where. Which instance, organisation and database the ingest reads. How its config file is mounted; no chart template or value carries one. Which account the token stands for, what it may read, and how long it lives. | [Grist](components/grist.md); [Grist ingest](components/grist-ingest.md) |
| D28 | Whether a deployment runs Superset, and where. Which role its token stands for, and how long it lives. Which databases it connects to, and with which credentials. | [Superset](components/superset.md) |
| D29 | Whether a deployment runs JupyterHub, and how its users sign in. How a user's token would reach a notebook, and how long it would live. Whether a notebook reads as the user or as a service. | [JupyterHub](components/jupyterhub.md) |
| D30 | Whether any deployment runs Hasura alongside the mesh. | [Hasura](components/hasura.md) |
| D31 | Whether a deployment's run coordinator honours pools, so that the write-concurrency setting limits concurrent writes. | [document tables](components/document-tables.md) |
| D32 | Whether any deployment turns agentic authorization on. | [agent capabilities](flows/agent-capabilities.md) |
| D33 | The largest dataset any deployment reads through the client, and the memory its consumers are given. | [big data](flows/big-data.md) |
| D34 | Which processes outside dag-tools use the client, and with which identity. Whether any consumer other than the data-analyst agent bounds its reads. | [mesh data client](components/mesh-data-client.md); [big data](flows/big-data.md) |
| D35 | How many minted credentials, sensors or schedules a deployment runs at once is not recorded anywhere in these records. Each component record states only its defaults. | the "Not established" section of each component record |

## 2. Behaviour of systems outside this code

These are settled by a recorded run against that system, or by its own documentation.

| # | Not established | Records |
|---|---|---|
| O1 | What a Restate server does when a plain service's handler sets state, and whether the invocation then fails after its two steps have run. What the admin port lets a caller read or change beyond registration. | [Restate API sync](components/restate-api-sync.md), finding 14; [Restate server](components/restate-server.md) |
| O2 | Whether the engine returns the narrowing fields at the top level of its answer, where the gateway reads them. Whether an answer carries allowed columns or row filters when only `allowed` is asked for. Whether the engine checks the bearer token or tenant header it is sent. | [Topaz](components/topaz.md); [need to know](flows/need-to-know.md) |
| O3 | Whether a whole aspect sent to the catalog replaces what the catalog held. Whether the schema aspect overwrites one a source crawler wrote. Whether a publish carrying only current terms drops an earlier, stricter term or a person's edit. | [DataHub lineage](components/datahub-lineage.md); [warehouse source](components/warehouse-source.md) |
| O4 | What a catalog with authentication on returns to a request with no token or a wrong one. Whether the lineage plugin's sensor is triggered by run status or by materialization events, and what it does when its sends fail. | [DataHub lineage](components/datahub-lineage.md) |
| O5 | Whether a warehouse's query history keeps a comment-placed tag intact, so that the audit can read it back. | [read audit](components/read-audit.md); [tagged warehouse](components/tagged-warehouse.md) |
| O6 | Whether a whole-table read succeeds on a minted SQL account narrowed to some columns. Whether any Postgres deployment applies the decision in the database on the older path, where the client skips its own narrowing. | [mesh data client](components/mesh-data-client.md); [domain broker](components/domain-broker.md), finding 6 |
| O7 | Whether a minted object-store credential is narrower than the prefix in practice. That is the token service's and the role's behaviour. | [domain broker](components/domain-broker.md) |
| O8 | Whether object-store scans push column selection and row filters into the files, and how much they read. | [big data](flows/big-data.md) |
| O9 | Whether an object store is safe as a Lance destination; the documentation says this was not measured. Whether Lance reads environment variables of its own when its storage options are empty. Whether the targeted ingest rate is met; the documentation says the measured rate falls short. | [document tables](components/document-tables.md) |
| O10 | What a target API does with a repeated or replayed call or row, and whether it honours an idempotency header a mapping names. | [OTel API sync](components/otel-api-sync.md); [Restate API sync](components/restate-api-sync.md) |
| O11 | Whether the operator probe for the SystemLink source matches any server. No run against a real server was read. | [SystemLink source](components/systemlink-source.md) |
| O12 | Whether a Grist server's download redirect is followed with the token in the header, to the address the redirect names. The code does that; no server was checked. | [Grist ingest](components/grist-ingest.md) |
| O13 | What a failed dbt source-freshness command does to the build that follows. The dagster-dbt library's defaults decide it. | [dbt project](components/dbt-project.md); [dbt](components/dbt.md) |
| O14 | Where dlt writes when the destination resolves to the bare string `filesystem`. | [dlt pipeline](components/dlt-pipeline.md), gap 2; [Restate dlt sync](components/restate-dlt-sync.md) |
| O15 | The retry policy of the callback push, which carries no retry options of its own. | [dag-tools §3.3](decisions/dag-tools-s1-s3.md) |

## 3. Recorded runs against the mesh

These are settled by a recorded run. Until one is cited, the claims stay `built`.

| # | Not established | Records |
|---|---|---|
| R1 | Whether the live seals for the read path have a recorded run. They exist as scripts, and a result has been asked of the owner. | [need to know](flows/need-to-know.md) |
| R2 | Whether any figure has been traced end to end through the evidence steps on a running system. | [evidence walkthrough](walkthroughs/evidence.md) |
| R3 | Whether any qualification has been run on a deployment, and with what verdict. | [sustainment walkthrough](walkthroughs/sustainment.md) |
| R4 | Which further built claims a recorded run would make `measured`. Eight are `measured` in CI, in process, at least in part; the rest are listed and have no recorded run. | [built-to-measured list](built-to-measured.md) |

## 4. Decisions nobody has made yet

These are settled by a decision, not by reading code.

| # | Not established | Records |
|---|---|---|
| X3 | What role Hasura is meant to play in the mesh, which plane it would sit in, and which services it would serve. | [Hasura](components/hasura.md) |
| X4 | What role Redpanda would play, and which datasets or events it would carry. How a subscription would be authorized, and by which decision point. Whether a published object would be registered in the catalog. Pub/sub is `proposed`. | [Redpanda](components/redpanda.md); [roadmap note](ROADMAP.md) |

## 5. Owned by another repository or lane

These are settled by the session that owns the fact.

| # | Not established | Owner | Records |
|---|---|---|---|
| L1 | What the dimensions file declares, and how a chart mounts it. | the policy repo | [dag-tools §4l.1](decisions/dag-tools-s4r-s4l-s4m.md); [warehouse source](components/warehouse-source.md) |
| L2 | Whether anything uses the published labels to decide a read, and how a clearance is checked at read time. Labels are `built` as metadata and `proposed` as a decision input. | the policy repo | [dag-tools §4l.3](decisions/dag-tools-s4r-s4l-s4m.md); [classification propagation](components/classification-propagation.md) |
| L3 | How the catalog's ownership data, from which the policy directory is synced, is kept correct. | invincible-agent | [need to know](flows/need-to-know.md); [evidence walkthrough](walkthroughs/evidence.md); [overview](overview.md#how-the-services-depend-on-each-other) |
| L4 | What the invincible-agent chart's catalog seed job and policy-sync job write or read. Both run scripts outside this repo. | invincible-agent | [DataHub](components/datahub.md) |
| L5 | What the decision records cited by the policy comments say, and what the ADR-0025 flip plan lists as unbuilt. | invincible-agent | [need to know](flows/need-to-know.md); [agent capabilities](flows/agent-capabilities.md) |
| L6 | What provenance-telemetry emits, where it lands, and whether a trace reaches from an agent's answer to the store read. | the perf lane | [agent capabilities](flows/agent-capabilities.md) |
| L7 | The SDK's lines, which were supplied and not read here. The reasons for entries 1, 2, 4 and 5. Whether the line numbers of entries 1 to 4 still hold at the later pin. Tests for entries 1 and 4. | the SDK | [SDK decisions](decisions/sdk.md); [agent capabilities](flows/agent-capabilities.md); [big data](flows/big-data.md) |
| L8 | Whether pub-tools' records match its code line by line. | pub-tools | [pub-tools pointer](decisions/pub-tools.md) |
| L9 | Whether the downstream repositories surveyed tag their assets for runnability as the class matrix expects. | each downstream repository | [sustainment walkthrough](walkthroughs/sustainment.md) |
| L10 | Whether the contract-mapping example's contract is current with its upstream copy. A test compares them only when the upstream copy is on the machine. | invincible-agent | [contract mapping](components/contract-mapping.md) |
| L11 | Whether invincible-agent's own continuous integration runs the tests the built-to-measured list cites from that repository, and with what result. | invincible-agent | [built-to-measured list](built-to-measured.md) |

## 6. Code not read, or read only in part

These are settled by a further read at the pin.

| # | Not established | Records |
|---|---|---|
| C1 | Whether a chained propagation declaration is refused, or reads no label set. No check or test for a chain was found. | [classification propagation](components/classification-propagation.md); [dag-tools §4n.8](decisions/dag-tools-s4n-s4o-s4k1.md) |
| C2 | How a derived asset's builder attaches column lineage and a column schema. The sensor reads both, and fails a dataset that has neither. | [classification propagation](components/classification-propagation.md) |
| C3 | Whether the broker reads inventory records the survey wrote, across versions. The broker builds its own in-process, and no such read was found. | [dag-tools §4a.1 (inventory)](decisions/dag-tools-s4e-s4a-s7.md) |
| C4 | What the lineage plugin's source does. It was not read. | [DataHub lineage](components/datahub-lineage.md); [dlt pipeline](components/dlt-pipeline.md) |
| C5 | Whether anything publishes the SystemLink tables, or the Grist destination tables, to the catalog. No catalog call was found in the SystemLink kind or engine. Whether a catalog sensor registers the Grist tables was not read. | [SystemLink source](components/systemlink-source.md); [Grist ingest](components/grist-ingest.md) |
| C6 | What the worker image contains. Its build file was not read. The worker-side behaviour of OTel API sync was read only through the plan it receives. | [Restate worker](components/restate-worker.md); [OTel API sync](components/otel-api-sync.md) |
| C7 | For each "no test" line, which candidate test would cover it. The lines are counted per record and not yet sorted. | [built-to-measured list](built-to-measured.md) |
| C8 | invincible-agent ADRs whose header disagrees with the code at the pin. The headers of 0040, 0041, 0042, 0045, 0046 and 0047 understate the code. The headers of 0056, 0062, 0064 and 0065 lag it: they say proposed, design only or no code, and code exists. | [ADR-0025 to 0048](decisions/invincible-agent-adr-0025-0048.md); [ADR-0049 to 0072](decisions/invincible-agent-adr-0049-0072.md) |
| C9 | invincible-agent ADRs checked only in part, from greps, comments, headers or file names rather than a full read: 0004, 0005, 0008, 0009, 0014, 0017, 0018, 0019 and 0023. | [ADR-0001 to 0024](decisions/invincible-agent-adr-0001-0024.md) |
| C10 | invincible-agent ADRs checked only in part: 0027, 0031, 0035, 0037, 0039, 0044, 0045, 0047 and 0048. Two of the findings are worth naming. ADR-0031's matching functions have a second copy in the ontology service. The ADR-0044 status of settings-profile pinning on the telemetry store is open. | [ADR-0025 to 0048](decisions/invincible-agent-adr-0025-0048.md) |
| C11 | invincible-agent ADRs checked only in part: 0049, 0050, 0051, 0052, 0053, 0054, 0055, 0060, 0061, 0063, 0064, 0067, 0070, 0071 and 0072. Among the open items are ADR-0049's status itself, which of ADR-0064's six steps are complete, ADR-0052's open items as the ADR states them, and the SDK types ADR-0060 names, which are in another repository. ADR-0072 also leaves open how the live view behaves with more than one replica. | [ADR-0049 to 0072](decisions/invincible-agent-adr-0049-0072.md) |
| C12 | invincible-agent ADR parts not located at the pin. These are ADR-0004 steps A, B, D, E and F; ADR-0005, 0006, 0008, 0009, 0015, 0016, 0019, 0021 and 0022; ADR-0025, 0026, 0028, 0030, 0033, 0034, 0036, 0038, 0040, 0041, 0042, 0045, 0046, 0047 and 0048; and ADR-0062 and 0065. One test that ADR-0006 names does not exist at the pin. ADR-0010's tracing does not match its plan, so it stays `proposed`. | the three invincible-agent decision files: [0001 to 0024](decisions/invincible-agent-adr-0001-0024.md), [0025 to 0048](decisions/invincible-agent-adr-0025-0048.md), [0049 to 0072](decisions/invincible-agent-adr-0049-0072.md) |

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Each entry above | the "Not established" section, or the line, of each record linked in its row | — | as each record | none; an entry is not established |
| 186 section items in 43 records | the "Not established" sections of the records in this directory at the #104 head | — | a count of the items under those headings | — |
| Thirteen further lines in nine files | [dag-tools §3.3](decisions/dag-tools-s1-s3.md); [§4a.1](decisions/dag-tools-s4e-s4a-s7.md); [§4n.8](decisions/dag-tools-s4n-s4o-s4k1.md); [§4l.1 and §4l.3](decisions/dag-tools-s4r-s4l-s4m.md); [roadmap note](ROADMAP.md); [DataHub](components/datahub.md), finding 4; [Hasura](components/hasura.md), purpose and plane; [Restate API sync](components/restate-api-sync.md), finding 14; [agent capabilities](flows/agent-capabilities.md), finding 9 | — | the lines that say "not established" outside a section | — |
| No record but the built-to-measured list carries `measured` | the status columns of the records in this directory | — | none found outside that list | — |
| X1 and X2 closed | the architect's ruling of 2026-11-19: an in-process CI run measures what it exercises, and a run id is accepted today | — | — | — |
