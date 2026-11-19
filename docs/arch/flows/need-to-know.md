# Need to know (FL-NTK)

This flow follows a governed data read: a person's token, the policy decision in Topaz, and what data is then exposed. It is framed by the elements of the data-centric security appendix of the NATO *Data Centric Reference Architecture for the Alliance*:

- the policy enforcement, decision, information and administration points;
- the identity, credential and access management (ICAM) services.

Each element names the mesh component that plays it and the code that shows it.

**This record describes; it proposes nothing.** Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **Need to know is an explicit grant on one dataset, and nothing else is consulted.** The decision is one Topaz check: `can_read` on `dataset:<urn>` for `user:<subject>`, where `can_read = reader | owner`. Persona, domain and classification are not read. The policy's own comment calls persona and domain "eligibility, never sufficient". `built`
2. **Access is to the whole dataset.** Narrowing is built at every hop:
   - the gateway reads `allowed_columns` and `row_filters`;
   - the broker turns them into database grants and row policies;
   - the client filters the frame.

   But no deployed policy produces either value:
   - the policy's narrowing rules are labelled example mocks, and they key on `input.user`, which the same policy says is empty;
   - the gateway asks Topaz for the `allowed` decision only.

   A minted SQL role is therefore granted `SELECT` on the whole table. An object-store credential is scoped to the dataset's prefix. Narrowing machinery: `built`. Narrowing policy: `not built`.
3. **Classification labels are published but not read at decision time.** dag-tools publishes classification labels to the catalog for sources and derived datasets (`warehouse_source`, `classification_propagation`). The decision path does not read them. The policy rails that would (`cleared_for:<level>` and `holds:<dimension>:<value>`) exist only in the policy validator. Their two files are empty, and each says no reader applies it yet. Label-based access: `not built`.
4. **Who the request is for is verified only under `enforce`, and `observe` is the default.**
   - Under `enforce`, the gateway refuses a request whose named principal differs from the verified token claim.
   - Under `observe`, the named principal is used as given and the gateway logs what `enforce` would have done.
   - A service acting for a person is refused under `enforce`, because the delegation seam always answers no.

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

| # | Step | Component | Status | Evidence |
|---|---|---|---|---|
| 1 | The person signs in and gets a token. Its authorization id is a single claim, `email` by default. | Keycloak realm, imported from the chart | `built` | invincible-agent `helm/invincible-agent/templates/keycloak.yaml:38`, `keycloak-configmap.yaml:2`, `values.yaml:2199` |
| 2 | The client asks the gateway for a dataset, sending the bearer token and the principal it acts for. | `cortex_data` client | `built` | dag-tools `dag_tools/cortex_data/client.py:161-205` |
| 3 | The gateway verifies the token: signature, `exp`, `iss`, `aud`. It then resolves the principal under the configured posture. | central gateway, identity module | `built` | `dag_tools/central_gateway/identity.py:178-201`, `:270`, `:398` |
| 4 | A configured deny list is checked. | central gateway | `built` | `dag_tools/central_gateway/main.py:78-82`, `:438-465` |
| 5 | The gateway asks Topaz for one decision: `allowed`, with the dataset URN and the subject passed in the resource context. | central gateway → Topaz authorizer | `built` | `main.py:208-335`, `:468` |
| 6 | Topaz evaluates `ds.check(dataset, can_read, user)` against its directory. | Topaz, `data_mesh…authorize` policy | `built` | invincible-agent `topaz-configmap.yaml:159-165` (model), `:352-392` (policy) |
| 7 | When allowed, the gateway looks up the producer's broker and asks it for a ticket. | central gateway → domain broker | `built` | `main.py:476-544` |
| 8 | The broker mints a credential for that dataset. | domain broker minters | `built` | `dag_tools/domain_broker/main.py:226-308`, `dag_tools/domain_broker/sql_minting.py:164-221`, `:264-344` |
| 9 | The gateway logs `TICKET_ISSUED` and returns the ticket. The client reads with it. | central gateway, client | `built` | `main.py:549-571`; `client.py` read paths |

## Roles in the NATO frame

| Element | Mesh component | What it does here | Status | Evidence | Gap |
|---|---|---|---|---|---|
| Policy enforcement point | Central gateway | Refuses before any data path is resolved: identity, then the deny list, then Topaz. A refusal returns 401, 403 or 503; a Topaz deny returns 403. | `built` | `identity.py:107-120`; `main.py:438-470` | Defaults to `observe` (finding 4). |
| Policy enforcement point (data plane) | Domain broker and the backend | Mints an expiring credential scoped to one dataset. A minting failure fails the ticket (503). | `built` | `domain_broker/main.py:226-308`, `:1212-1265` | The object store narrows by prefix only. Backends with no minter get the producer's credential (finding 5). |
| Policy enforcement point (client) | `cortex_data` client | Applies column and row narrowing to the frame for non-Postgres backends. For Postgres it skips this and relies on the minted role. | `built` | `client.py:318-366` | Advisory only. Today nothing arrives to apply (finding 2). |
| Policy enforcement point (outside the data read) | Agent backend and agent services | Make their own Topaz checks for persona and domain cells, documents, ontology, capabilities and tasks. A person refused a cell never reaches the dataset gate. | `built` | invincible-agent `tests/sandbox_e2e/_seal_asset_read_denied_at_the_gate.py:1-13` | Out of scope for this flow; listed so no one reads the gateway as the only enforcement point. |
| Policy decision point | Topaz authorizer, `data_mesh.GET.api.v1.assets.__asset_key.authorize` | Deny by default. Allows only on an explicit `reader` or `owner` relation. | `built` | `topaz-configmap.yaml:352-392` | No narrowing policy (finding 2). No label policy (finding 3). |
| Policy information point | Topaz directory | Holds `dataset` objects and `owner` relations synced from the catalog, `reader` relations synced from grants, and users and groups synced from policy files. | `built` | invincible-agent `policy/sync/datahub_topaz_sync.py:53-57`, `policy/sync/grant_sync.py`, `topaz-seed-cronjob.yaml:255-293` | Catalog tags and domains are parsed but not written to the directory. Token claims do not reach it; the subject is passed in by the gateway. |
| Policy information point (request) | Token claims, deny list | The verified subject, plus the configured deny list. | `built` | `identity.py:398`; `main.py:78-82` | No directory, catalog or label lookup at decision time. |
| Policy administration point | Policy YAML plus the seed CronJob | Grants are reviewed files carrying `granted_by` and `reason`. The CronJob validates them, then syncs. Removing a row revokes the grant. | `built` | invincible-agent `policy/asset_grants.yaml:1-26`; `topaz-seed-cronjob.yaml:255-293` | No grant ledger beyond git history. |
| ICAM: authentication | Keycloak; gateway token verification | Tokens are issued by the realm and verified at the gateway against JWKS or a static key, with the issuer and audience checked. | `built` | `keycloak.yaml:38`; `identity.py:398-444` | — |
| ICAM: digital identity | Keycloak realm | One authorization-id claim. Realm roles are `user` and `admin`. Service clients carry a hardcoded authorization id. | `built` | `keycloak-configmap.yaml:2`, `:240-255`, `:279-286` | No group, persona or domain claim; those come from the directory. |
| ICAM: authorization and access control | Topaz directory and policy | The decision and information points above. | `built` | as above | Access control is relationship-based on explicit grants. It is not attribute-based over security labels. |
| ICAM: digital certificate | — | No certificate service was found in the chart. Trust in the realm comes from a mounted truststore. | `not built` | invincible-agent `values.yaml:2268-2283` | Recorded as a gap. |

## Gaps

- **Security labels in the standard metadata form:** `not built`. The decision reads no label. Label rails exist only in the validator (finding 3).
- **Attribute-based access over labels:** `not built`. The access model is an explicit per-dataset relation. It is recorded as such and not relabelled.
- **Column and row narrowing policy:** `not built`. The mechanism is `built` (finding 2).
- **Object-store narrowing:** limited to a prefix, by design. The minter's own docstring says it takes the decision's narrowing and ignores it.
- **Backends with no minter:** they return the producer's credential.
- **Default posture `observe`:** trusts the named principal without checking it against the token.
- **Delegation:** a service may not act for a person; the seam always answers no.
- **Gateway code hygiene:**
  - A second, relation-style request is built and never sent (`main.py:295-307`).
  - If Topaz returns an empty `decisions` list, the allow flag is read before it is assigned. The resulting exception is caught and the result is a deny, so it fails closed.
- **No test pins the decision request:** neither the policy path nor `can_read` is asserted by a dag-tools test.

## Not established from code

- Whether the Topaz `authz/is` response carries `allowed_columns` or `row_filters` when only `allowed` is requested. The code shows nothing would produce them; no response was recorded.
- Which posture any deployment runs. The default is `observe`; no deployed value was read.
- Whether the live seals have run and passed. They exist as scripts, which is why every row above says `built` and none says `measured`.
- The decision records the policy comments cite were not read, so the Decision column below names them as cited in code only.
- How the catalog's ownership data is kept correct. The directory trusts what the catalog sync writes.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Decision is `can_read` on one dataset | invincible-agent `helm/invincible-agent/templates/topaz-configmap.yaml:159-165`, `:352-392` | cited in code: invincible-agent ADR-0025 (not read) | invincible-agent `tests/sandbox_e2e/_seal_asset_read_denied_at_the_gate.py` | `built` |
| Gateway asks for `allowed` only, with subject in resource context | dag-tools `dag_tools/central_gateway/main.py:278-292`, `:309-335` | not stated | no test pins the request | `built` |
| Narrowing rules are mocks on an empty input | invincible-agent `topaz-configmap.yaml:359-403` | not stated | — | `built` (mocks) |
| SQL minters grant the whole table when nothing narrows | dag-tools `dag_tools/domain_broker/sql_minting.py:204-209`, `:331-336` | cited in code: invincible-agent ADR-0044 (not read) | `test_pg_columns_narrow_when_topaz_narrows`, `test_ch_row_policy_is_created_server_side` | `built` |
| Object-store minter ignores narrowing | dag-tools `dag_tools/domain_broker/main.py:226-256` | cited in code: invincible-agent ADR-0044 (not read) | `test_minting_failure_fails_closed` | `built` |
| No-minter backends get the producer credential | dag-tools `dag_tools/domain_broker/main.py:1212-1223` | not stated | — | `built` |
| Postgres client skips client-side narrowing | dag-tools `dag_tools/cortex_data/client.py:318-325` | not stated | `test_the_frame_is_still_narrowed_when_the_receipt_is_asked_for` | `built` |
| Posture default `observe`; unknown value → `enforce` | dag-tools `dag_tools/central_gateway/identity.py:178-197` | not stated | `test_posture_parse` | `built` |
| Observe trusts the named principal | dag-tools `identity.py:588-608` | not stated | `test_observe_logs_what_enforce_would_do_and_a_forged_token_WITH_A_HEADER_still_passes` | `built` |
| Delegation seam answers no | dag-tools `identity.py:270-285` | not stated | `test_the_default_seam_is_false_and_says_once_that_the_rail_is_not_wired` | `built` |
| Label rails unread | invincible-agent `policy/level_readers.yaml:17-20`, `policy/dimension_holders.yaml:23-26` | not stated | `tests/security/test_level_readers_rail.py`, `tests/security/test_dimension_holders_rail.py` | `not built` (reader) |
| Grants are reviewed files with grantor and reason | invincible-agent `policy/asset_grants.yaml:1-26` | not stated | `tests/test_grant_sync.py` | `built` |
| Validate, then sync, on a schedule | invincible-agent `helm/invincible-agent/templates/topaz-seed-cronjob.yaml:109`, `:255-293` | not stated | `tests/test_topaz_manifest_load_job.py` | `built` |
| Catalog sync writes datasets and owners only | invincible-agent `policy/sync/datahub_topaz_sync.py:53-65` | not stated | `tests/test_datahub_topaz_sync.py` | `built` |
| One authorization-id claim, default `email` | invincible-agent `keycloak-configmap.yaml:2`; `values.yaml:2199` | not stated | — | `built` |
| Producer brokers run as a second pod from the producer's image | invincible-agent `helm/invincible-agent/values.yaml:1176-1186` | not stated | pub-tools tests, as reported by its owner | `built` |
