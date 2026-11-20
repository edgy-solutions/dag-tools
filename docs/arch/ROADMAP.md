# Roadmap note

Status: A note, not a decision.

The remedies proposed for gaps the flow and component records found, in the order they would run, with no dates. Every row is `proposed` except row 8: it is written here and nowhere else, no decision record accepts it, and no code does it. Each row would be decided in its own record by the repository that owns the code. Row 8 is `accepted`: the architect ruled on it on 2026-11-18, and no code does it yet. Rows 6 and 7 have owners named by the architect the same day and stay `proposed`. Line numbers are at the pins in [the README](README.md): rows 1 to 8 at the flows' pins, rows 9 to 24 at the component-records pin. Where an issue tracks a row, it is named beside the row; "not filed" means no issue exists yet.

## Order

| # | increment | what isolates | home | depends on |
|---|---|---|---|---|
| 1 | Verified principal by default (dag-tools #70) | the gateway reads who a request is for from the verified token, not from a header | — (proposed; no decision yet) | the policy delegate rail and its reader |
| 2 | Refuse what the object store cannot narrow (dag-tools #71) | an object-store ticket is never wider than the decision that allowed it | — (proposed; no decision yet) | — |
| 3 | No ticket on the producer's credential (dag-tools #72) | every backend a ticket names is read on a credential minted for that ticket | — (proposed; no decision yet) | — |
| 4 | Bounded reads from SQL backends (dag-tools #73) | a read from a SQL backend is no larger than the caller asked for and can stop before the whole table moves | — (proposed; no decision yet) | — |
| 5 | A version on every read (dag-tools #74) | a receipt says which version of the dataset was read | — (proposed; no decision yet) | — |
| 6 | Engine read decisions on by default (not filed) | a graph or vector read returns only what the person may read | — (proposed; owner named: invincible-agent) | the preconditions invincible-agent's chart names for the switch |
| 7 | A receipt on graph and vector reads (not filed) | a graph or vector answer says what was read, for whom, and which version | — (proposed; owners named: the SDK and invincible-agent; no work now) | row 5, for the version |
| 8 | Retire the SDK's remote graph client (not filed) | no client posts to a route nothing serves; in-process readers inside engines are the supported shape | the architect's ruling, 2026-11-18 (accepted: retire) | — |
| 9 | No placeholder on a served path (not filed) | a registered dataset and an issued ticket are real or refused | — (proposed; no decision yet) | — |
| 10 | Remove expired minted SQL accounts (not filed) | a database holds no minted account past its lifetime | — (proposed; no decision yet) | — |
| 11 | The gateway says what it does (not filed) | the gateway's code and warnings describe the gateway that runs | — (proposed; no decision yet) | — |
| 12 | No mesh token sent to a backend (dag-tools #76) | the token meant for the gateway reaches only the gateway | — (proposed; no decision yet) | row 9 |
| 13 | Restate's ports establish their caller (not filed) | only the mesh's own senders start a durable job, and only its own workers register | — (proposed; no decision yet) | — |
| 14 | A pinned Restate server, and no setting read by nothing (not filed) | two installs of one chart run one Restate version, and every external Restate setting is used | — (proposed; no decision yet) | — |
| 15 | The worker reports that it is registered (not filed) | a worker the server cannot route to is not reported ready | — (proposed; no decision yet) | — |
| 16 | The internal doors admit only listed services by default (not filed) | only a listed service registers a route at the gateway or calls the broker's resolve route | — (proposed; no decision yet) | — |
| 17 | One signed attribution for every read (not filed) | a read the audit counts as explained was made by the principal its tag names, and a read made under a ticket joins that ticket | — (proposed; no decision yet) | row 1 |
| 18 | Labels as a decision input (not filed) | a person reads a labelled column only where the label and the person's clearance allow it | — (proposed; no decision yet) | — |
| 19 | The probes code location in the chart (not filed) | a qualification can cover the classes that need a probe without a hand-written deployment entry | — (proposed; no decision yet) | — |
| 20 | A joined source-to-use report (not filed) | a read of a source can be followed through lineage to each read of a dataset built from it, and each of those reads names the decision it was made under | — (proposed; no decision yet) | row 17 |
| 21 | Dashboards authorized through the decision point (not filed) | a dashboard tool's read of mesh data is decided at the gateway and leaves a gateway receipt | — (proposed; no decision yet) | row 24 |
| 22 | An API layer authorized through the decision point (not filed) | an API layer's read of mesh data is decided at the gateway and leaves a gateway receipt | — (proposed; no decision yet) | — |
| 23 | Propagation across more than one step (not filed) | a dataset built from a derived dataset carries a label composed from the label that dataset carries | — (proposed; no decision yet) | — |
| 24 | The documented pattern for a dashboard tool to authorize through the mesh (not filed) | an operator can wire a dashboard tool to the decision point from the client documentation alone | — (proposed; no decision yet) | — |

## Where later slices fold in

| row | slice | folds into | why it costs the increment nothing |
|---|---|---|---|

## The rows

### 1. Verified principal by default

- **Gap:** [need to know, finding 4](flows/need-to-know.md). Under `observe`, the code default, the gateway uses the named principal as given (`dag_tools/central_gateway/identity.py:178-197`, `:588-608`). `built`
- **What already exists:** the gateway counts what `enforce` would have refused, separately from what it did refuse, and reports both on `/health` (`dag_tools/central_gateway/refusal_gauge.py:20-53`; `main.py:142-170`). `built`
- **Remedy:** a deployment moves `GATEWAY_IDENTITY_POSTURE` to `enforce` once the would-refuse counts read zero over a period it chooses. Before that, the delegation seam (`identity.py:270-285`) gets a reader of the policy rail that lists which services may act for a person, so those services are not refused. `proposed`
- **Depends on:** the delegate rail, which policy owns in invincible-agent. Its reader in dag-tools is not built.
- **Issue:** dag-tools #70.

### 2. Refuse what the object store cannot narrow

- **Gap:** [need to know, finding 5](flows/need-to-know.md). The object-store minter scopes a credential to the dataset's prefix and ignores the decision's column and row narrowing (`dag_tools/domain_broker/main.py:226-256`). `built`
- **Remedy:** when a decision narrows and the backend cannot enforce the narrowing, the broker refuses the ticket rather than issuing a prefix credential. This holds until a narrowed read path exists on the server side. Minting already fails closed with a 503 when it errors (`main.py:1242-1268`), so the refusal has a path to use. `proposed`
- **Note:** no deployed policy narrows today (finding 2), so this row changes nothing until one does. It keeps the gap from opening when one does.
- **Issue:** dag-tools #71.

### 3. No ticket on the producer's credential

- **Gap:** [need to know, finding 5](flows/need-to-know.md). A backend with no entry in `_MINTERS` (`dag_tools/domain_broker/main.py:298-308`) is returned on the producer's own credential, and the code says so rather than calling it protected (`:1219-1223`). `built`
- **Remedy:** add a minter for each backend in use. Until each has one, a deployment switch makes the broker refuse a ticket for an unminted backend instead of returning the producer's credential. `proposed`
- **Issue:** dag-tools #72.

### 4. Bounded reads from SQL backends

- **Gap:** [big data, findings 2, 3 and 5](flows/big-data.md). The Postgres and ClickHouse paths read the whole table eagerly into client memory (`dag_tools/cortex_data/client.py:318`, `:323`, `:352-353`), so a consumer's own size gate runs after the transfer. `built`
- **Remedy:** the client passes the caller's column list, predicate and a row limit into the SQL it sends, and reads the result in batches rather than all at once. The data read gets a timeout of its own, as the ticket request already has (`client.py:205`). `proposed`
- **Note:** the object-store paths are already lazy scans (`client.py:260-264`, `:295-297`) and need nothing here.
- **Issue:** dag-tools #73.

### 5. A version on every read

- **Gap:** [big data, finding 8](flows/big-data.md). The receipt has an `as_of` field, and the table of version readers ships empty (`dag_tools/domain_broker/ticket_identity.py:110`). `built`
- **Who fills it:** the domain broker (`ticket_identity.py:149-194`). The client only copies the field onto the receipt (`dag_tools/cortex_data/client.py:45-56`). **Owner: dag-tools.**
- **Waiting on it:** invincible-agent #375 cites the receipt's as-of in a finance agent's answer, and claims none when the receipt has none.
- **Remedy:** add a version reader for each backend in use, starting with the table formats that keep one (Delta, Iceberg). `proposed`
- **Issue:** dag-tools #74.

### 6. Engine read decisions on by default

- **Gap:** [agent capabilities, finding 6](flows/agent-capabilities.md). The engines' per-document and per-class checks run only when `ENABLE_AGENTIC_AUTH` is on, and the code and chart both default it off (invincible-agent `agent_fleet/weaviate_expert/service.py:92-96`; `helm/invincible-agent/values.yaml:2927-2944`). With it off, every result is kept. `built`
- **Owner:** invincible-agent, with review by a second lane (named by the architect, 2026-11-18). The chart ties the switch to the last step of its ADR-0025 and to a plan for the switch, which lists preconditions that are not built. Neither was read here.
- **First deliverable (read-only):** what exactly is unprotected with the switch off, meaning which reads return results the caller is not entitled to and which other checks still apply; what ADR-0025's remaining step is; and what breaks when the switch is on. Then a plan to turn it on in a test deployment and then to make it the chart default. The row leads the planning of invincible-agent's next release; it is not deferred. `proposed`
- **Remedy:** the switch defaults on once those preconditions are met, so a graph or vector read is decided per object unless a deployment opts out. `proposed`
- **Note:** the chart says the switch does nothing for the data-analyst read path, which the gateway decides (`templates/configmap.yaml:388-397`). That path is [need to know](flows/need-to-know.md), not this row.
- **Issue:** not filed.

### 7. A receipt on graph and vector reads

- **Gap:** [agent capabilities, findings 3 and 7](flows/agent-capabilities.md). Graph and vector reads carry no ticket, receipt or version, and the store sees the engine's credential, not the person's. `built`
- **Owner:** invincible-agent, whose engines hold the readers, and the SDK, whose result shape would have to carry the receipt.
- **Remedy:** each reader returns, with its result, the person it read for, the decision it read under and a version of what it read. The version would use the dataset receipt's own fields (`as_of`, `as_of_source`, `as_of_unavailable`; dag-tools `dag_tools/cortex_data/client.py:23-32`). The person and the decision would need fields the dataset receipt does not have: it carries no principal, and its `allowed_columns` and `row_filters` are the narrowing a decision produced, not the decision. `proposed`
- **Note:** the person is already on every read as provenance, but not on any receipt. The version would not appear for free: the table of version readers ships empty (row 5), so a receipt reusing those fields would say the version is unavailable until a reader exists for that store.
- **Placement (2026-11-18):** the architect placed this row jointly with the SDK and invincible-agent, to be recorded as an invincible-agent decision that these records cite, and confirmed the placement the same day. No work starts on it now. `proposed`
- **Issue:** not filed.

### 8. Retire the SDK's remote graph client

- **Gap:** [agent capabilities, finding 1](flows/agent-capabilities.md). The SDK ships a remote graph client that posts to a route nothing serves (supplied by the SDK's session). `built`
- **What already exists:** in-process readers inside engines are what ships and is used today; the remote client was built after them, and has no production caller (supplied by the SDK's session). `built`
- **Decision:** retire the client (the architect's ruling, 2026-11-18). It has no route and no production caller, and in-process readers are what ships. Before the ruling, the SDK had declined to build a server end without a scheduled consumer or to retire the client on its own (supplied by the SDK's session). `accepted`
- **Owner:** the SDK and invincible-agent, each for its own part.
- **Remedy:** the SDK marks the client deprecated in its next patch release and removes it in its next minor release, with a note that in-process readers are the supported shape. invincible-agent closes its open item for generic mesh gateway routes as not planned; the item reopens when a named consumer needs a remote route, and that route is then designed behind a decision point. `accepted`; no code does it yet.
- **Issue:** not filed.

### 9. No placeholder on a served path

- **Gap:** [domain broker, findings 3 and 8](components/domain-broker.md). With no definitions module named, the broker registers two placeholder datasets (`dag_tools/domain_broker/main.py:42`, `:802-818`). On the older path, a SQL ticket carries a fixed placeholder token, and an object-store credential has a fixed one-hour lifetime rather than the configured one (`main.py:1273-1360`). No test covers the SQL branch. `built`
- **Owner:** dag-tools.
- **Remedy:** with no module named, the broker registers nothing and says why on `/ready`. On the older path, a SQL ticket is refused, and an object-store credential takes the configured lifetime. `proposed`
- **Issue:** not filed.

### 10. Remove expired minted SQL accounts

- **Gap:** [domain broker, finding 10](components/domain-broker.md). The ClickHouse users and Postgres roles the broker mints are refused once they expire, and are never dropped (`dag_tools/domain_broker/sql_minting.py:22-33`). `built`
- **Owner:** dag-tools.
- **Remedy:** a sweep drops minted accounts whose lifetime has passed, using the identity that created them. A Postgres policy names its role, so the sweep drops the policy before the role. `proposed`
- **Issue:** not filed.
- **Note:** each engine sets the expiry in the statement that creates the account (`dag_tools/domain_broker/sql_minting.py:200-201`, `:324-325`), so the server refuses a login after it. An expired account stays in the engine's catalog of users or roles. The broker mints one account per SQL ticket, with a default lifetime of 900 seconds (`sql_minting.py:113`), plus one row policy when the decision carries a row filter (`sql_minting.py:212-218`, `:338-341`). `built`
- **Not established:** Postgres applies an expiry only to password sign-in. Whether a deployment's client-authentication rules let a minted role sign in some other way, such as trust, peer or certificate, is a deployment fact this repo cannot settle.

### 11. The gateway says what it does

- **Gap:** [central gateway, findings 9 and 13](components/central-gateway.md). A relationship-check payload is built and never sent (`dag_tools/central_gateway/main.py:325`). A startup warning says no verification is enforced when a retired flag is set, though the identity posture can enforce it (`subject_gauge.py:310-316`). `built`
- **Owner:** dag-tools.
- **Remedy:** remove the unsent payload, or send it under a recorded decision; reword the warning to name the identity posture. `proposed`
- **Issue:** not filed.

### 12. No mesh token sent to a backend

- **Gap:** [mesh data client, finding 9](components/mesh-data-client.md). The client uses its own mesh token as the database password when the ticket carries no password (Postgres, `dag_tools/cortex_data/client.py:315`) or neither a password nor a token (ClickHouse, `:339`). It is the same bearer token the client presents to the gateway (`:189-191`). On the broker's older path the ticket carries a placeholder token and no password, so it is Postgres that receives the mesh token; no path in this repo issues a ClickHouse ticket with neither. No test reaches either branch. `built`
- **Owner:** dag-tools.
- **Remedy:** the client refuses a SQL ticket that carries no password, rather than sending a token meant for the gateway; and a test reaches each SQL branch. `proposed`
- **Note:** the older path is where a Postgres ticket carries no password (row 9), so the two rows close the same opening from both ends. The owner reads this row as a present exposure, not tidiness: once that path is taken, the send is unconditional in code. Whether a database receives it there depends on name resolution of the placeholder host, which the code cannot settle.
- **Issue:** dag-tools #76.

### 13. Restate's ports establish their caller

- **Gap:** [Restate server, finding 5](components/restate-server.md) and [Restate worker, findings 5 and 6](components/restate-worker.md). Neither the server's ingress and admin ports nor the worker's port checks who is calling. The worker registers itself with no credential, and the Dagster components that send work send no authorization header (`dag_tools/restate_handlers/serve.py:72`, `:75-112`; `helm/dag-tools/templates/restate-server.yaml:47-54`). `built`
- **Owner:** dag-tools.
- **Remedy:** the server's ports accept only callers that prove they are the mesh's senders or workers, and the worker accepts calls only from its server. Senders and the worker present that proof. Which mechanism to use is for the decision record, which would read what Restate itself offers first. `proposed`
- **Issue:** not filed.

### 14. A pinned Restate server, and no setting read by nothing

- **Gap:** [Restate server, findings 6 and 7](components/restate-server.md). The in-chart server's image is tag `latest`, by value and by the helper's fallback (`helm/dag-tools/values.yaml:34-38`; `templates/_helpers.tpl:43-46`). `externalRestate.ingressUrl` resolves in a helper that no template uses, while the senders take their own address setting (`values.yaml:22-24`; `templates/_helpers.tpl:66-74`). `built`
- **Owner:** dag-tools.
- **Remedy:** the chart names a Restate version, or a digest, that its tests ran against; and the ingress setting is either passed to the senders or removed. `proposed`
- **Issue:** not filed.

### 15. The worker reports that it is registered

- **Gap:** [Restate worker, findings 3 and 5](components/restate-worker.md). The worker declares no probe, and a registration that gave up is only an error log line (`helm/dag-tools/templates/restate-worker.yaml:1-98`; `dag_tools/restate_handlers/serve.py:75-112`). `built`
- **Owner:** dag-tools.
- **Remedy:** the worker declares a readiness probe that fails until its registration has succeeded, and a test covers registration. `proposed`
- **Issue:** not filed.

### 16. The internal doors admit only listed services by default

- **Gap:** [central gateway, finding 12](components/central-gateway.md). The door on the gateway's register route and on the broker's resolve route defaults to `observe`. Under it, a caller the door would refuse, one with no token included, is logged as `would_refuse` and served. The chart sets no door posture (`dag_tools/central_gateway/internal_door.py:17-21`, `:101-131`; `helm/dag-tools/values.yaml:173-215`). `built`
- **What already exists:** the door, its service lists and its `enforce` posture, and the `would_refuse` line a deployment can read before it switches (`internal_door.py:50-72`, `:101-131`). `built`
- **Owner:** dag-tools.
- **Remedy:** the chart sets the door to `enforce`, with each route's list of services, once the `would_refuse` lines read zero over a period the deployment chooses; or the code default moves to `enforce`. Which of the two is for the decision record. It could be filed together with row 1, which moves the identity posture the same way. `proposed`
- **Issue:** not filed.

### 17. One signed attribution for every read

- **Gap:** [read audit, finding 5](components/read-audit.md); [tagged warehouse, finding 3](components/tagged-warehouse.md); [evidence walkthrough](walkthroughs/evidence.md), Gaps. The query tag is compact JSON carrying a principal, an optional asset key, an optional run id and a schema version, with no signature (`dag_tools/audit/tag.py:71-79`, `:123-146`). A read that carries a forged pipeline tag counts as explained. The gateway logs each ticket with its id (`dag_tools/central_gateway/main.py:591-597`). The client that reads a ticket's backend sets no query tag, so the ticket log and the read audit share no key. `built`
- **What already exists:** the tag and its encoder, and a ticket id minted per request (`dag_tools/domain_broker/ticket_identity.py:218`). `built`
- **Owner:** dag-tools.
- **Remedy:** the tag carries a signature that the read audit checks, and a read made under a ticket carries the ticket id in its tag. A tag that fails the check is reported as its own cause, not counted as explained. How the key is held, and which component sets the tag on a ticket read, are for the decision record. `proposed`
- **Depends on:** row 1 for a person's reads, because a tag set from an unverified principal would sign a name nobody checked.
- **Issue:** not filed.

### 18. Labels as a decision input

- **Gap:** [need to know, finding 3](flows/need-to-know.md); [evidence walkthrough](walkthroughs/evidence.md), Gaps. Labels are published to the catalog as metadata. The decision path does not read them, and the policy rails that would carry a clearance are empty. `built` as metadata; `proposed` as a decision input
- **What already exists:** label publishing for sources and for derived datasets, each label set carrying the digest of the document that ratified it. `built`
- **Owner:** not named. The rails are policy's, in invincible-agent; their reader would be a later dag-tools step, which invincible-agent ADR-0064 describes (cited in code, not read here).
- **Remedy:** a reader of the rails, and a decision that consults a column's label and the person's clearance, reading each dimension by its declared kind. `proposed`
- **Issue:** not filed.

### 19. The probes code location in the chart

- **Gap:** [Dagster, finding 10](components/dagster.md); [sustainment walkthrough](walkthroughs/sustainment.md), Gaps. The probes code location is built and loads whatever probe modules its directory holds (`dag_tools/probes_location/definitions.py:40`, `:55-62`, `:65`). Neither chart has an entry for it. `built`
- **What already exists:** the location, which loads empty when the probes directory variable is unset (`definitions.py:56-57`), and the status check that reports it absent from a workspace. `built`
- **Owner:** dag-tools.
- **Remedy:** the dag-tools chart offers the probes code location as an entry a deployment turns on, off by default. `proposed`
- **Issue:** not filed.

### 20. A joined source-to-use report

- **Gap:** [source to use, finding 8](flows/source-to-use.md). The read audit reads query history and run windows, the lineage sensor reads materializations and the asset graph, and the ticket line goes to the gateway's log. No code reads any two of them together, and the ticket line does not carry the narrowing the decision set (`dag_tools/central_gateway/main.py:558-563`, `:590-596`). `built`
- **What already exists:** each of the three records on its own: the read audit's attribution of source reads (`dag_tools/audit/reads.py:154-213`), lineage in the asset graph and the catalog (`dag_tools/components/datahub_lineage/component.py:294-344`), and the ticket line with the client's receipt. `built`
- **Owner:** dag-tools.
- **Remedy:** a report that follows a source read through lineage to each gateway read of a dataset built from it, naming the person and the decision of each read. It needs the decision's columns and row filters on the receipt, and a key shared by the read tag and the ticket. `proposed`
- **Depends on:** row 17, for the shared key.
- **Issue:** not filed.

### 21. Dashboards authorized through the decision point

- **Gap:** [Superset, finding 5](components/superset.md); [source to use, finding 7](flows/source-to-use.md). An agent engine calls a dashboard tool's API directly, with the chart's bearer token, which is empty by default (invincible-agent `agent_fleet/restate_analyst/main.py:1009-1075`). The gateway does not decide the read, and no gateway receipt is written. `built`
- **What already exists:** the gateway's authorize route and the client that calls it (`dag_tools/central_gateway/main.py:404-405`). What the dashboard tool's own roles permit is not established from code. `built`
- **Owner:** not named. The caller is invincible-agent's; the decision point is dag-tools'.
- **Remedy:** a dashboard tool's reads of mesh data are decided at the gateway, and each leaves a gateway receipt. `proposed`
- **Depends on:** row 24, for the documented pattern.
- **Issue:** not filed.

### 22. An API layer authorized through the decision point

- **Gap:** [Hasura, finding 1](components/hasura.md); [source to use, finding 7](flows/source-to-use.md). Nothing at either pin wires an API layer, so a deployment that adds one adds a read path the gateway does not decide. `built`
- **What already exists:** the gateway's authorize route and the client that calls it (`dag_tools/central_gateway/main.py:404-405`). `built`
- **Owner:** not named.
- **Remedy:** an API layer's reads of mesh data are decided at the gateway, and each leaves a gateway receipt. `proposed`
- **Issue:** not filed.

### 23. Propagation across more than one step

- **Gap:** [classification propagation](components/classification-propagation.md), Not established from code. AGENTS.md §4n says a derived dataset cannot be another's source. The schema refuses only a dataset that lists itself as a source (`dag_tools/components/classification_propagation/schema.py:184-190`; no test), and no check refusing a chain was found. `built`
- **What already exists:** propagation one step from declared, labelled sources, recorded as an observation against the derived asset (`component.py:495-517`, `:594-605`). `built`
- **Owner:** dag-tools.
- **Remedy:** a derived dataset may name another derived dataset as a source, whose published decision is read as that source's label set, and a cycle is refused. `proposed`
- **Issue:** not filed.

### 24. The documented pattern for a dashboard tool to authorize through the mesh

- **Gap:** [Superset, finding 5](components/superset.md); [source to use, finding 7](flows/source-to-use.md). No document at the dag-tools pin outside these records mentions a dashboard tool. `built`
- **What already exists:** the client's authorize call and its receipt (`dag_tools/cortex_data/client.py:23-56`, `:161-171`). `built`
- **Owner:** the SDK, in its client documentation.
- **Remedy:** the SDK's client documentation describes how a dashboard tool authorizes each read through the decision point and keeps the receipt. `proposed`
- **Issue:** not filed.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Posture default `observe`, set per deployment | dag-tools `dag_tools/central_gateway/identity.py:178-197` | not stated | `test_posture_parse` | `built` |
| Would-refuse and did-refuse counts on `/health` | dag-tools `dag_tools/central_gateway/refusal_gauge.py:20-53`, `main.py:142-170` | not stated | `test_gateway_refusal_reasons.py` | `built` |
| Delegation seam answers no | dag-tools `identity.py:270-285` | not stated | `test_the_default_seam_is_false_and_says_once_that_the_rail_is_not_wired` | `built` |
| Object-store minter ignores narrowing | dag-tools `dag_tools/domain_broker/main.py:226-256` | cited in code: invincible-agent ADR-0044 (not read) | `test_minting_failure_fails_closed` | `built` |
| No-minter backends get the producer credential | dag-tools `dag_tools/domain_broker/main.py:298-308`, `:1219-1223` | not stated | — | `built` |
| SQL reads are the whole table, eagerly | dag-tools `dag_tools/cortex_data/client.py:318`, `:323`, `:352-353` | not stated | no test reaches either call | `built` |
| No version reader ships | dag-tools `dag_tools/domain_broker/ticket_identity.py:110` | dag-tools AGENTS.md §4o | `test_no_as_of_reader_ships_by_default` | `built` |
| Engine read checks default off in code and chart | invincible-agent `agent_fleet/weaviate_expert/service.py:92-96`; `helm/invincible-agent/values.yaml:2927-2944` | cited in chart: invincible-agent ADR-0025 (not read) | `test_the_control_with_auth_OFF_both_paths_keep_EVERY_chunk` | `built` |
| Graph and vector reads carry no ticket, receipt or version | invincible-agent `agent_fleet/ontology_service/mesh_graph.py`, `agent_fleet/utils/mesh_vectors.py`: no match at the pin | not stated | — | `built` |
| The remote graph client has no server end | SDK `remote_clients.py:40-41`, `:58` (supplied) | not stated | tested against a stub (supplied) | `built` (no server end) |
| Placeholder datasets; the older path's fixed token and lifetime | dag-tools `dag_tools/domain_broker/main.py:42`, `:802-818`, `:1273-1360` | not stated | no test of the SQL branch | `built` |
| Minted SQL accounts are never removed | dag-tools `dag_tools/domain_broker/sql_minting.py:22-33` | not stated | — | `built` |
| An unsent check payload; a stale warning | dag-tools `dag_tools/central_gateway/main.py:325`, `subject_gauge.py:310-316` | not stated | — | `built` |
| The client's mesh token as a SQL password | dag-tools `dag_tools/cortex_data/client.py:315`, `:339` | not stated | no test | `built` |
| Restate ports and registration check no caller | dag-tools `dag_tools/restate_handlers/serve.py:72`, `:75-112`; `helm/dag-tools/templates/restate-server.yaml:47-54` | not stated | no test | `built` |
| Restate server image at `latest`; an ingress setting read by nothing | dag-tools `helm/dag-tools/values.yaml:22-24`, `:34-38`; `templates/_helpers.tpl:43-46`, `:66-74` | not stated | — | `built` |
| Internal doors serve a refused caller under `observe`, the default; chart sets no posture | dag-tools `dag_tools/central_gateway/internal_door.py:17-21`, `:50-72`, `:101-131`; `helm/dag-tools/values.yaml:173-215` | not stated | `test_internal_doors.py`: `test_observe_serves_and_logs_would_refuse` | `built` |
| No worker probe; registration failure only logged | dag-tools `helm/dag-tools/templates/restate-worker.yaml:1-98`; `dag_tools/restate_handlers/serve.py:75-112` | not stated | no test | `built` |
| Query tag unsigned; ticket id minted per request and logged | dag-tools `dag_tools/audit/tag.py:71-79`, `:123-146`; `dag_tools/domain_broker/ticket_identity.py:218`; `dag_tools/central_gateway/main.py:591-597` | dag-tools AGENTS.md §4j, §4o | `test_the_tag_carries_the_principal_the_asset_and_the_run`, `test_two_resolves_of_one_urn_get_different_ticket_ids` | `built` |
| Labels published as metadata | [need to know, finding 3](flows/need-to-know.md) | cited in code: invincible-agent ADR-0064 (not read) | `test_the_published_label_set_says_which_document_ratified_it` | `built` |
| Labels as a decision input | [need to know, finding 3](flows/need-to-know.md) | cited in code: invincible-agent ADR-0064 (not read) | `test_the_published_label_set_says_which_document_ratified_it` | `proposed` |
| Probes code location built | dag-tools `dag_tools/probes_location/definitions.py:40`, `:55-62`, `:65` | dag-tools AGENTS.md §4g1 | `test_probes_location_loader.py`: `test_loader_returns_empty_report_when_env_unset` (empty when the variable is unset); `test_probes_location_definitions.py`: `test_definitions_module_yields_merged_defs_for_two_probes` (loads what the directory holds) | `built` |
| Probes code location in neither chart | dag-tools `dag_tools/probes_location/definitions.py:40`, `:55-62`, `:65` | dag-tools AGENTS.md §4g1 | `test_probes_location_loader.py`: `test_loader_returns_empty_report_when_env_unset` (empty when the variable is unset); `test_probes_location_definitions.py`: `test_definitions_module_yields_merged_defs_for_two_probes` (loads what the directory holds) | `built` |
| No joined source-to-use report | dag-tools `dag_tools/audit/`, `dag_tools/components/datahub_lineage/`, `dag_tools/central_gateway/` searched | not stated | — | `built` |
| A dashboard tool is called directly | invincible-agent `agent_fleet/restate_analyst/main.py:1009-1075` | not stated | no test | `built` |
| Nothing wires an API layer | [Hasura, finding 1](components/hasura.md) | not stated | — | `built` |
| Only a self-listed source is refused | dag-tools `dag_tools/components/classification_propagation/schema.py:184-190` | dag-tools AGENTS.md §4n | no test | `built` |
| No dashboard pattern documented | dag-tools repository searched outside `docs/arch/` | not stated | — | `built` |
| The twenty-four remedies, except row 8 | this note | none | — | `proposed` |
| Row 8: retire the remote graph client | this note | the architect's ruling, 2026-11-18 | — | `accepted` |
