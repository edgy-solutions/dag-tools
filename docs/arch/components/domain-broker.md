# Domain broker (CO-BROKER)

The domain broker turns an authorized read into a ticket. It loads a deployment's Dagster definitions, works out which datasets they publish and where those live, and registers their URNs with the central gateway. When the gateway calls it with a decision, it returns a ticket: the source type, the physical location and, for the backends it can, a credential minted for that one read. It holds no data and serves none ([big data, finding 1](../flows/big-data.md)).

This record covers what the code and the dag-tools image provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`), not at the flows' pin. The record shape is provisional.

## Findings

1. **It ships in the user-deployment image, and no dag-tools chart template deploys it.** The image carries the broker's dependencies through the `broker` extra. Its own comments say a chart overrides the container command to start the broker. No template under the dag-tools chart does so. `built`
2. **It finds its datasets by importing the definitions module a deployment names.** The module is named as `module` or `module:attribute`. The import runs in the background after startup. Until it finishes, `/ready` answers 503 and `/health` reports `loading`. If the import fails, `/ready` stays 503, `/health` reports the error, and the broker does not register with the gateway. `/health` answers 200 in every state. `built`
3. **With no module named, it serves two placeholder datasets.** It logs a warning and populates one SQL and one object-store entry with made-up locations, then registers them like any others. No test covers this default. `built`
4. **It registers what it loaded, and repeats.** It posts its address and its URNs to the gateway's register route with a service credential. The first post follows the load, and later ones run every two minutes by default. A failed post is logged and retried on the next round. The gateway keeps each route for five minutes ([gateway, finding 10](central-gateway.md)). `built`
5. **The resolve route is behind the gateway's service door.** It admits the services its caller list names, under the door's own posture, which defaults to `observe` ([gateway, finding 12](central-gateway.md)). `built`
6. **A dataset whose IO manager advertises its coordinates gets a minted credential, or no ticket.** This is the path every dag-tools IO manager uses. The producer's credentials are stripped from the coordinates at load and counted. At resolve time, the broker mints:

   | Source type | Credential | How it is narrowed | How it expires |
   |---|---|---|---|
   | object store: Parquet, Delta, Iceberg | a temporary credential from the token service, allowing get and a prefix-scoped list | to the dataset's prefix only; the decision's columns and rows are accepted and ignored | after the credential lifetime |
   | ClickHouse | a new user, read-only, created in one statement | column grants when the decision names columns; a row policy built from the decision's filter | the database refuses it after the lifetime |
   | Postgres | a new login role, with ambient grants revoked | column or table grants; a row policy, and a refusal if a filter is required and the table has row security off | the database refuses it after the lifetime |

   If minting fails, the broker refuses with 503. For the object store, a missing identity adds the token-service variables to set to the refusal. For the two SQL backends the refusal is a bare sentence; the variables to set appear only in the error log line. It never falls back to the producer's credential. A ticket with no scope and none derivable is refused with 500. `built`
7. **A source type with no minter is handed the producer's credential.** The ticket goes out as the producer advertised it, and the source type is counted on `/health` as unprotected. [Roadmap row 3](../ROADMAP.md) (dag-tools #72) is the remedy. `built`
8. **A dataset with no advertised coordinates takes an older path, which does not mint for SQL.** For a SQL source type it returns a ticket carrying a fixed placeholder token. Neither branch reads the decision's allowed columns or row filters, so the policy engine's narrowing does not reach this path. For an object store it asks the token service for a credential with a fixed one-hour lifetime, not the configured one, and refuses with 503 on failure. Any other type is refused with 400. No test covers the SQL branch. The two placeholder datasets of finding 3 take this path. `built`
9. **The minting identity is read from the environment, in a fixed order.** For the token service: a broker-specific pair first, then the platform's standard pair, then two older pairs, then the ambient credential chain. The region falls back to a fixed public region. For each SQL backend: a broker-specific admin pair, then the backend's ordinary pair. With none set, minting fails closed (finding 6). `built`
10. **Minted SQL accounts are never removed.** The database refuses them once they expire, and they stay listed. The code states this as a gap. `built` (expiry); `not built` (removal)
11. **Every ticket gets an id, an issue time and a version field.** The version comes from a reader registered for the backend type, else from a version the producer advertised at load. No reader ships, so an unadvertised version is absent and counted. `built` (the field); `not built` (a reader)
12. **`/health` reports four counts.** These are: producer credentials dropped at load; datasets loaded on a producer credential, by backend type, counted once per dataset at load; tickets with no version, by backend type; and hosts that are not fully qualified, reported rather than refused. A fifth count, datasets not advertised by reason, is kept in the process but appears only in a warning logged at load. A count on `/health` can be watched; a warning at load cannot. `built`
13. **Identifiers are refused, not cleaned.** A table, schema or column name outside a plain identifier grammar fails the mint. Passwords are long random alphanumeric strings, and account names carry a fixed prefix and a random suffix. `built`

## Settings and defaults

No dag-tools chart template deploys the broker, so there is no chart column.

| Setting | Code default | What it does |
|---|---|---|
| `DAGSTER_DEFS_MODULE` | empty: the placeholder datasets of finding 3 | the definitions module to load, as `module` or `module:attribute` |
| `CENTRAL_GATEWAY_URL` | an in-cluster service name | where registrations go |
| `BROKER_URL` | an in-cluster service name | the address the broker registers as its own |
| `BROKER_REGISTER_INTERVAL_SEC` | 120 | seconds between registrations |
| `BROKER_CREDENTIAL_TTL_SEC` | 900 | lifetime of a minted credential, on the minted path only |
| `BROKER_STS_ACCESS_KEY_ID`, `BROKER_STS_SECRET_ACCESS_KEY` | none | the object-store minting identity, preferred over the pairs after it |
| `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, then `S3_ACCESS_KEY`/`S3_SECRET_KEY`, then `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` | none | fallbacks for the same identity; with none, the ambient chain |
| `AWS_ASSUME_ROLE_ARN` | an example value | the role assumed for an object-store credential |
| `BROKER_CH_ADMIN_USER`/`_PASSWORD`, then `CH_USER`/`CH_PASSWORD` | none | the ClickHouse identity that creates read users |
| `BROKER_PG_ADMIN_USER`/`_PASSWORD`, then `PG_USER`/`PG_PASSWORD` | none | the Postgres identity that creates read roles |
| `INTERNAL_DOOR_POSTURE`, `BROKER_RESOLVE_CALLERS` | `observe`; empty | the resolve route's service door |
| `MESH_SERVICE_TOKEN_URL`, `_CLIENT_ID`, `_CLIENT_SECRET` | none | the credential the broker presents when it registers |

## Gaps

- **The object-store credential ignores the decision's columns and rows** (finding 6). Remedy: [roadmap row 2](../ROADMAP.md) (dag-tools #71).
- **Backends with no minter get the producer's credential** (finding 7). Remedy: [roadmap row 3](../ROADMAP.md) (dag-tools #72).
- **No version reader ships** (finding 11). Remedy: [roadmap row 5](../ROADMAP.md) (dag-tools #74).
- **The broker serves placeholders by default, and the older path hands out a credential it did not mint** (findings 3, 8). With no module named, it registers made-up datasets. A SQL dataset on the older path gets a fixed placeholder token. The older path's object-store credential ignores the configured lifetime. None of these is tested. Remedy: [roadmap row 9](../ROADMAP.md) (not filed).
- **Minted SQL accounts are never removed** (finding 10). Remedy: [roadmap row 10](../ROADMAP.md) (not filed).

## Not established from code

- Which chart deploys the broker in practice, and with what settings. The image's comments say a chart overrides the command; no dag-tools template does.
- Whether a minted object-store credential is narrower than the prefix in practice. That is the token service's and the role's behaviour; no run was recorded.
- How many minted SQL accounts a database accumulates. That is a fact of deployments.

## Place in the mesh

| Aspect | What the code shows | Alignment row |
|---|---|---|
| Purpose | turn an authorized read into a ticket for one dataset | — |
| How the mesh wires it | ships in the dag-tools user-deployment image and loads the definitions module a deployment names; registers its URNs with the gateway; the gateway calls its resolve route | — |
| Sign-in | no user signs in to it; the gateway calls it through the service door; it presents a client-credentials token when it registers; it mints with admin identities read from its environment | [NATO R7](../alignment/nato-dcra.md) |
| Data path | none: it hands out a location and a credential, and the client reads | [DoD S7](../alignment/dod-dmra.md) |
| Plane | operational: producers run their own broker | [NATO L2](../alignment/nato-dcra.md) |
| Security role | enforcement point: it mints a credential scoped to one dataset, or refuses | [NATO R1](../alignment/nato-dcra.md) |
| Services served | data exchange; a unique id on every ticket; performance analytics, by its exposure counts; instrumentation, by `/health` and `/ready` | [DoD S7, S1, S10, S15](../alignment/dod-dmra.md) |

## Sources

All dag-tools paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Broker source paths are under `dag_tools/domain_broker/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Ships in the user-deployment image; command overridden by a chart; no dag-tools template | `dag_tools/user_deployment/Dockerfile:13-17`, `:102-118`; `helm/dag-tools/templates/`: no broker template | not stated | — | `built` |
| Background import; `/ready` 503 until loaded or after a failure; no registration after a failure | `main.py:693-723`, `:1013-1075`, `:1097-1165` | not stated | `test_broker_load_failure.py`: `test_ready_is_503_while_still_loading`, `test_health_surfaces_the_import_error`, `test_ready_is_503_after_a_failed_import`, `test_a_failed_import_does_not_register`, `test_a_successful_load_registers_its_assets` | `built` |
| Placeholder datasets when no module is named | `main.py:42`, `:802-818` | not stated | no test | `built` |
| Registers, then every two minutes by default | `main.py:963-1010` | not stated | `test_internal_doors.py`: `test_the_broker_register_call_sends_the_service_token` | `built` |
| Resolve route behind the service door; every ticket stamped | `main.py:1168-1189`; `ticket_identity.py:226` | dag-tools AGENTS.md §4o | `test_internal_doors.py`: `test_enforce_with_no_list_refuses_everyone`; `test_ticket_identity.py`: `test_the_unprotected_path_is_identified_TOO` | `built` |
| Producer credentials stripped at load | `main.py:68`, `:323` | cited in code: invincible-agent ADR-0044 (not read) | — | `built` |
| Minted path; fail closed with 503; 500 with no scope; the variables named in the refusal for the object store only | `main.py:298-307`, `:1214-1267`, `:1253-1264`; `sql_minting.py:184`, `:287-289` | cited in code: invincible-agent ADR-0044 (not read) | `test_broker_mints_ticket_credentials.py`: `test_minting_failure_fails_closed` | `built` |
| Object-store credential scoped to the prefix; decision ignored | `main.py:226-296`, `:270` | cited in code: invincible-agent ADR-0044 (not read) | — | `built` |
| ClickHouse and Postgres minting: expiry, read-only, grants, row policies, row-security refusal | `sql_minting.py:112-116`, `:152-243`, `:246-363` | not stated | `test_sql_minting.py`: `test_ch_credential_expires`, `test_ch_is_readonly_and_that_is_a_SEPARATE_claim`, `test_ch_columns_narrow_when_topaz_narrows`, `test_pg_credential_expires_and_is_table_scoped`, `test_pg_revokes_ambient_grants_first`, `test_pg_REFUSES_when_rls_is_off_and_a_filter_was_required` | `built` |
| No minter: producer credential, counted | `main.py:75`, `:1219-1223` | not stated | — | `built` |
| Older path: fixed placeholder token for SQL; fixed one-hour object-store lifetime; 400 otherwise; the decision not read | `main.py:1238-1242`, `:1273-1360` | not stated | no test | `built` |
| Minting identity order; region fallback | `main.py:153-224`; `sql_minting.py:117-123`, `:152-200` | not stated | `test_broker_mints_ticket_credentials.py`: `test_sts_identity_prefers_the_purpose_named_pair`, `test_sts_identity_accepts_the_pods_existing_s3_vars`; `test_sql_minting.py`: `test_no_minting_identity_names_the_variables` | `built` |
| No account removal | `sql_minting.py:22-33` | not stated | — | `not built` |
| Ticket identity; no version reader | `ticket_identity.py:47-64`, `:110`, `:149-226` | dag-tools AGENTS.md §4o | `test_ticket_identity.py`: `test_no_as_of_reader_ships_by_default`, `test_an_absent_as_of_is_not_the_issue_time` | `built` |
| Four counts on `/health`; unadvertised datasets in a load warning only | `main.py:68-108`, `:368-370`, `:942-948`, `:1097-1142` | cited in code: invincible-agent ADR-0044 (not read) | — | `built` |
| Identifiers refused, not cleaned; random passwords and names | `sql_minting.py:53`, `:91-110` | not stated | — | `built` |
| Remedies | [the roadmap note](../ROADMAP.md), rows 2, 3, 5, 9 and 10 | none | — | `proposed` |
