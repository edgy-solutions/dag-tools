# Need to know (FL-NTK)

This flow follows a governed data read: a person's token, the policy decision in Topaz, and what data is then exposed. It is framed by elements of Appendix 9 (Data-Centric Security) of the NATO *Data Centric Reference Architecture for the Alliance*, version 2:

- the policy enforcement, decision, information and administration points (Appendix 9, paragraph 3);
- the identity, credentials and access management (ICAM) services (Appendix 9, paragraphs 6 to 10).

Each element names the mesh component that plays it and the code that shows it.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **Need to know is an explicit grant on one dataset, and nothing else is consulted.** The decision is one Topaz check: `can_read` on `dataset:<urn>` for `user:<subject>`, where `can_read = reader | owner`. Persona, domain and classification are not read. The policy's own comment calls persona and domain "eligibility, never sufficient". `built`
2. **Access is to the whole dataset.** Narrowing is built at every hop:
   - the gateway reads `allowed_columns` and `row_filters`;
   - the broker turns them into database grants and row policies;
   - the client filters the frame.

   But no deployed policy produces either value:
   - the policy's narrowing rules are labelled example mocks, and they key on `input.user`, which the same policy says is empty;
   - the gateway asks Topaz for the `allowed` decision only.

   A minted SQL role is therefore granted `SELECT` on the whole table. An object-store credential is scoped to the dataset's prefix.

   Narrowing: `built`. No deployed policy produces it, so today every grant reaches the whole dataset.
3. **Classification labels are published but not read at decision time.**
   - As published metadata, labels are `built`. dag-tools publishes them to the catalog for sources and derived datasets (`warehouse_source`, `classification_propagation`).
   - As an input to the decision, labels are `proposed`. The decision path does not read them. The policy rails that would (`cleared_for:<level>` and `holds:<dimension>:<value>`) are written down in two policy files, which the policy validator checks. Both files are empty, and each says no reader applies it until a later dag-tools step. The step is described in invincible-agent ADR-0064, which is cited in code and not read here.
4. **Who the request is for is verified only under `enforce`, and `observe` is the code default.**
   - Under `enforce`, the gateway refuses a request whose named principal differs from the verified token claim.
   - Under `observe`, the named principal is used as given and the gateway logs what `enforce` would have done.
   - A service acting for a person is refused under `enforce`, because the delegation seam always answers no.
   - `observe` is the default in dag-tools code. A deployment sets the posture through `GATEWAY_IDENTITY_POSTURE`; which value it sets is a fact of its own values, not of dag-tools, and this record names none.

   `built`
5. **How the data plane enforces the decision depends on the backend.**

   | Backend | Credential | Where narrowing is enforced |
   |---|---|---|
   | ClickHouse and Postgres | expiring role | in the database |
   | Object store (Parquet, Delta, Iceberg) | expiring credential, scoped by prefix | nowhere; client side and advisory only |
   | Any other backend | the producer's own credential | nowhere |

   `built`
6. **Policy is administered as reviewed files.**
   - Grants are YAML in the policy directory, and every grant names who granted it and why.
   - A scheduled job validates the files, then syncs them into the Topaz directory.
   - Git history is the stated audit trail. No separate grant ledger was found.

   `built`

## The flow, step by step

Each actor and target cell holds one id. An `RL-` id names a role, and an `XE-` id a party with no record of its own; both are declared in [the README](../README.md#element-ids).

| # | Actor | Action | Target | What passes | Decision or check | Evidence | Status |
|---|---|---|---|---|---|---|---|
| 1 | RL-PERSON | signs in and is issued a token whose authorization id is a single claim, `email` by default | CO-KEYCLOAK | sign-in; a token back | — | invincible-agent `helm/invincible-agent/templates/keycloak.yaml:38`, `keycloak-configmap.yaml:2`, `values.yaml:2199` | `built` |
| 2 | CO-CLIENT | asks for a dataset | CO-GATEWAY | the bearer token and the principal it acts for | — | dag-tools `dag_tools/cortex_data/client.py:161-205` | `built` |
| 3 | CO-GATEWAY | verifies the token, then resolves the principal | CO-GATEWAY | — | signature, `exp`, `iss`, `aud`; the configured posture | `dag_tools/central_gateway/identity.py:178-201`, `:270`, `:398` | `built` |
| 4 | CO-GATEWAY | checks the request against the deny list | CO-GATEWAY | — | the configured deny list | `dag_tools/central_gateway/main.py:78-82`, `:438-465` | `built` |
| 5 | CO-GATEWAY | asks for one decision, `allowed` | CO-TOPAZ | the dataset URN and the subject, in the resource context | — | `main.py:208-335`, `:468` | `built` |
| 6 | CO-TOPAZ | evaluates the request against its directory | CO-TOPAZ | — | `ds.check(dataset, can_read, user)` under the `data_mesh…authorize` policy | invincible-agent `topaz-configmap.yaml:159-165` (model), `:352-392` (policy) | `built` |
| 7 | CO-GATEWAY | looks up the producer's broker and asks it for a ticket | CO-BROKER | the dataset | proceeds only when step 6 allowed | `main.py:476-544` | `built` |
| 8 | CO-BROKER | mints a credential for that dataset | XE-DATASET-BACKEND | — | the minter for the backend type | `dag_tools/domain_broker/main.py:226-308`, `dag_tools/domain_broker/sql_minting.py:164-221`, `:264-344` | `built` |
| 9 | CO-GATEWAY | logs `TICKET_ISSUED` and returns the ticket; the client reads with it | CO-CLIENT | the ticket | — | `main.py:549-571`; `client.py` read paths | `built` |

## Roles in the NATO frame

| Element (Appendix 9) | Mesh component | What it does here | Status | Evidence | Gap |
|---|---|---|---|---|---|
| Policy enforcement point (¶3) | Central gateway | Refuses before any data path is resolved: identity, then the deny list, then Topaz. A refusal returns 401, 403 or 503; a Topaz deny returns 403. | `built` | `identity.py:107-120`; `main.py:438-470` | Defaults to `observe` (finding 4). |
| Policy enforcement point (¶3), data plane | Domain broker and the backend | Mints an expiring credential scoped to one dataset. A minting failure fails the ticket (503). | `built` | `domain_broker/main.py:226-308`, `:1219-1223`, `:1242-1268` | The object store narrows by prefix only. Backends with no minter get the producer's credential (finding 5). |
| Policy enforcement point (¶3), client | `cortex_data` client | Applies column and row narrowing to the frame for non-Postgres backends. For Postgres it skips this and relies on the minted role. | `built` | `client.py:318-366` | Advisory only. Today nothing arrives to apply (finding 2). |
| Policy enforcement point (¶3), outside the data read | Agent backend and agent services | Make their own Topaz checks for persona and domain cells, documents, ontology, capabilities and tasks. A person refused a cell never reaches the dataset gate. | `built` | invincible-agent `_seal_asset_read_denied_at_the_gate.py:1-13` | Out of scope for this flow; listed so no one reads the gateway as the only enforcement point. |
| Policy decision point (¶3) | Topaz authorizer, `data_mesh.GET.api.v1.assets.__asset_key.authorize` | Deny by default. Allows only on an explicit `reader` or `owner` relation. | `built` | `topaz-configmap.yaml:352-392` | No narrowing policy (finding 2). No label policy (finding 3). |
| Policy information point (¶3) | Topaz directory | Holds `dataset` objects and `owner` relations synced from the catalog, `reader` relations synced from grants, and users and groups synced from policy files. | `built` | invincible-agent `policy/sync/datahub_topaz_sync.py:53-57`, `policy/sync/grant_sync.py`, `topaz-seed-cronjob.yaml:255-293` | Catalog tags and domains are parsed but not written to the directory. Token claims do not reach it; the subject is passed in by the gateway. |
| Policy information point (¶3), request | Token claims, deny list | The verified subject, plus the configured deny list. | `built` | `identity.py:398`; `main.py:78-82` | No directory, catalog or label lookup at decision time. |
| Policy administration point (¶3) | Policy YAML plus the seed CronJob | Grants are reviewed files carrying `granted_by` and `reason`. The CronJob validates them, then syncs. Removing a row revokes the grant. | `built` | invincible-agent `policy/asset_grants.yaml:1-26`; `topaz-seed-cronjob.yaml:255-293` | No grant ledger beyond git history. |
| ICAM: authentication services (¶8) | Keycloak; gateway token verification | Tokens are issued by the realm and verified at the gateway against JWKS or a static key, with the issuer and audience checked. | `built` | `keycloak.yaml:38`; `identity.py:398-444` | — |
| ICAM: digital identity services (¶10) | Keycloak realm | One authorization-id claim. Realm roles are `user` and `admin`. Service clients carry a hardcoded authorization id. | `built` | `keycloak-configmap.yaml:2`, `:240-255`, `:279-286` | No group, persona or domain claim; those come from the directory. |
| ICAM: authorization and access control services (¶7) | Topaz directory and policy | The decision and information points above. | `built` | as above | Access control is relationship-based on explicit grants. It is not attribute-based over security labels. |
| ICAM: digital certificate services (¶9) | — | No certificate service was found in the chart. Trust in the realm comes from a mounted truststore. | `not built` | invincible-agent `values.yaml:2268-2283` | Recorded as a gap. |

## Gaps

- **Security labels in the standard metadata form:** not built. Labels are published in the mesh's own form (finding 3); no standard metadata form was found. Status: `built`.
- **Labels as a decision input:** proposed. The decision reads no label. The rails that would carry clearances are written down and empty (finding 3). The access model is an explicit per-dataset relation, and it is recorded as such and not relabelled. Status: `proposed`.
- **Narrowing produced by policy:** narrowing is built at every hop, and no deployed policy produces it (finding 2). Status: `built`.
- **Object-store narrowing:** limited to a prefix. The minter's own docstring says it takes the decision's narrowing and ignores it. Remedy: [roadmap row 2](../ROADMAP.md). Status: `built`.
- **Backends with no minter:** they return the producer's credential. Remedy: [roadmap row 3](../ROADMAP.md). Status: `built`.
- **Default posture `observe`:** trusts the named principal without checking it against the token. Remedy: [roadmap row 1](../ROADMAP.md). Status: `built`.
- **Delegation:** a service may not act for a person; the seam always answers no. Roadmap row 1 depends on it. Status: `built`.
- **Gateway code hygiene:** A second, relation-style request is built and never sent (`main.py:295-307`). If Topaz returns an empty `decisions` list, the allow flag is read before it is assigned. The resulting exception is caught and the result is a deny, so it fails closed. Status: `built`.
- **No test pins the decision request:** neither the policy path nor `can_read` is asserted by a dag-tools test. Status: `built`.

## Not established from code

- Whether the Topaz `authz/is` response carries `allowed_columns` or `row_filters` when only `allowed` is requested. The code shows nothing would produce them; no response was recorded.
- Whether the live seals have a recorded run. They exist as scripts. A recorded result has been asked of the dag-tools owner; until one is cited, every row above says `built` and none says `measured`.
- The decision records the policy comments cite were not read, so the Decision column below names them as cited in code only.
- How the catalog's ownership data is kept correct. The directory trusts what the catalog sync writes.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Decision is `can_read` on one dataset | invincible-agent `helm/invincible-agent/templates/topaz-configmap.yaml:159-165`, `:352-392` | cited in code: invincible-agent ADR-0025 (not read) | invincible-agent `_seal_asset_read_denied_at_the_gate.py:1-13` | `built` |
| Gateway asks for `allowed` only, with subject in resource context | dag-tools `dag_tools/central_gateway/main.py:278-292`, `:309-335` | not stated | no test pins the request | `built` |
| Narrowing rules are mocks on an empty input | invincible-agent `topaz-configmap.yaml:359-403` | not stated | — | `built` (mocks) |
| SQL minters grant the whole table when nothing narrows | dag-tools `dag_tools/domain_broker/sql_minting.py:204-209`, `:331-336` | cited in code: invincible-agent ADR-0044 (not read) | `test_pg_columns_narrow_when_topaz_narrows`, `test_ch_row_policy_is_created_server_side` | `built` |
| Object-store minter ignores narrowing | dag-tools `dag_tools/domain_broker/main.py:226-256` | cited in code: invincible-agent ADR-0044 (not read) | `test_minting_failure_fails_closed` | `built` |
| No-minter backends get the producer credential | dag-tools `dag_tools/domain_broker/main.py:1219-1223` | not stated | — | `built` |
| Postgres client skips client-side narrowing | dag-tools `dag_tools/cortex_data/client.py:318-325` | not stated | `test_the_frame_is_still_narrowed_when_the_receipt_is_asked_for` | `built` |
| Posture default `observe`; unknown value → `enforce` | dag-tools `dag_tools/central_gateway/identity.py:178-197` | not stated | `test_posture_parse` | `built` |
| Observe trusts the named principal | dag-tools `identity.py:588-608` | not stated | `test_observe_logs_what_enforce_would_do_and_a_forged_token_WITH_A_HEADER_still_passes` | `built` |
| Delegation seam answers no | dag-tools `identity.py:270-285` | not stated | `test_the_default_seam_is_false_and_says_once_that_the_rail_is_not_wired` | `built` |
| Labels published as catalog metadata | dag-tools `dag_tools/warehouse_source/`, `dag_tools/components/classification_propagation/` | cited in code: invincible-agent ADR-0064 (not read) | `test_the_physical_dataset_is_the_entity_every_label_is_attached_to`, `test_the_shipped_example_propagates_from_a_source_it_only_OBSERVED` | `built` |
| Labels as a decision input: rails written, empty, unread | invincible-agent `policy/level_readers.yaml:17-20`, `policy/dimension_holders.yaml:23-26` | cited in code: invincible-agent ADR-0064 (not read) | `tests/security/test_level_readers_rail.py`, `tests/security/test_dimension_holders_rail.py` (validate the files; no reader) | `proposed` |
| Posture is set per deployment | dag-tools `dag_tools/central_gateway/identity.py:197` | not stated | — | `built` |
| Grants are reviewed files with grantor and reason | invincible-agent `policy/asset_grants.yaml:1-26` | not stated | `tests/test_grant_sync.py` | `built` |
| Validate, then sync, on a schedule | invincible-agent `helm/invincible-agent/templates/topaz-seed-cronjob.yaml:109`, `:255-293` | not stated | `tests/test_topaz_manifest_load_job.py` | `built` |
| Catalog sync writes datasets and owners only | invincible-agent `policy/sync/datahub_topaz_sync.py:53-65` | not stated | `tests/test_datahub_topaz_sync.py` | `built` |
| One authorization-id claim, default `email` | invincible-agent `keycloak-configmap.yaml:2`; `values.yaml:2199` | not stated | — | `built` |
| Producer brokers run as a second pod from the producer's image | invincible-agent `helm/invincible-agent/values.yaml:1176-1186` | not stated | pub-tools tests, as reported by its owner | `built` |
