# Keycloak (CO-KEYCLOAK)

Keycloak is the identity provider the mesh code expects. It meets this repo in two places. The [central gateway](central-gateway.md) verifies the tokens it issues. The [mesh data client](mesh-data-client.md) can obtain a service token from it by a client-credentials grant. The dag-tools chart does not deploy Keycloak.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **The chart deploys no identity provider.** No template in the dag-tools chart creates one. The chart's values document the gateway's verification settings in comments and set none of them. `built`
2. **The gateway names no issuer, audience or key of its own.** Accepted issuers, accepted audiences and the key source come only from the environment. An empty list refuses every token, and the refusal names the setting. A key set address is preferred over a static key. The accepted algorithms default to RS256. The reasons are in AGENTS.md §4o-1 and in [the gateway record](central-gateway.md). `built`
3. **Verification checks signature, audience and issuer, in that order, and requires three claims.** A token must carry an expiry, an issuer and an audience. A token signed correctly by an issuer the gateway does not accept is refused for that reason, not as an invalid token. `built`
4. **The key set is fetched with a three-second limit, cached, and a failed fetch is not remembered.** The key set client keeps the keys it has fetched and is rebuilt only when the address changes. If the fetch fails, that request gets a 503 and the next request tries again. `built`
5. **Under the default posture, verification does not decide the principal.** The posture defaults to `observe`, under which the gateway takes the principal from a header. The token is verified under `enforce`. [Roadmap row 1](../ROADMAP.md) (dag-tools #70) would make `enforce` the default. `built`
6. **The internal service doors use the same verification.** The broker's resolve route and the gateway's register route verify a service's token with the same code as the read path and then require a service claim. `built`
7. **The mesh data client fetches one service token and never refreshes it.** When no token is handed in and no development token is set, the client exchanges a client id and secret for a token at construction, with a ten-second limit. It keeps that token for its lifetime and does not fetch another when it expires. A development token from the environment wins over the service credentials. The token address has a code default naming an in-cluster service. `built`
8. **The read-only IO manager builds a new client, and so a new token, for every input it loads.** The IO manager's client id and secret are required. Each load constructs a client, which fetches a token. Writing through it always raises. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `GATEWAY_JWT_ISSUERS` (or the singular form) | none | accepted issuers; empty refuses everything |
| `GATEWAY_JWT_AUDIENCES` (or the singular form) | none | accepted audiences; empty refuses everything |
| `GATEWAY_JWKS_URL`, else `GATEWAY_JWT_PUBLIC_KEY`, else a legacy public-key variable | none | where the gateway gets its verification key |
| `GATEWAY_JWT_ALGORITHMS` | `RS256` | accepted signing algorithms |
| `GATEWAY_IDENTITY_POSTURE` | `observe` | whether the verified token decides the principal |
| `CORTEX_CLIENT_ID`, `CORTEX_CLIENT_SECRET` (or arguments) | none | the client's service credentials |
| `KEYCLOAK_TOKEN_URL` (or an argument) | an in-cluster token address | where the client exchanges them |
| `MESH_DEV_TOKEN` | none | a development token that wins over the service credentials |

## Gaps

- **The principal is not taken from the verified token by default** (finding 5). Remedy: [roadmap row 1](../ROADMAP.md) (dag-tools #70).
- **A long-lived client keeps an expired token** (finding 7). After the token expires every read through that client fails until it is rebuilt. Remedy: not on the roadmap.
- **A token request on every IO manager load** (finding 8). The identity provider receives one grant per input loaded. Remedy: not on the roadmap.

## Not established from code

- Which identity provider a deployment runs, and which issuers, audiences and key source it sets.
- How long a deployment's service tokens live, and so how soon a long-lived client stops working.
- Which claim a deployment's tokens carry for the entitlement key.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-GATEWAY | is called by | HTTP | none | the signing key set, fetched with a three-second limit and cached | `dag_tools/central_gateway/identity.py:296-325`, `:422-427`; no test | `built` |
| CO-CLIENT | is called by | HTTP | client id and secret exchanged by a client-credentials grant | a service token, fetched once at construction and never refreshed | `dag_tools/cortex_data/client.py:95-112`, `:146-159`; `test_cortex_client_documented_contract.py`: `test_dev_token_wins_over_m2m` | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | issues the tokens the gateway verifies and the client presents | — |
| How the mesh wires it | an external service; the gateway reads its key set and the client posts to its token address; not deployed by the chart | — |
| Sign-in | it is the sign-in: user tokens and client-credentials service tokens | [NATO R7](../alignment/nato-dcra.md) |
| Data path | none; it issues and publishes keys, and sees no data | — |
| Plane | identity: the authority behind each principal | [NATO R9](../alignment/nato-dcra.md) |
| Security role | authentication of people and services; verification is enforced only under `enforce` | [NATO R5](../alignment/nato-dcra.md), [NATO R7](../alignment/nato-dcra.md) |
| Services served | the identity every governed exchange is decided for | [DoD S7](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No identity provider in the chart; settings documented only | `helm/dag-tools/templates/`; `helm/dag-tools/values.yaml:186-216` | not stated | — | `built` |
| No compiled issuer, audience or key; empty refuses; key set preferred; RS256 | `dag_tools/central_gateway/identity.py:208-236`, `:257`, `:264-265`, `:417-421` | AGENTS.md §4o-1 | `test_gateway_accepted_issuers_and_audiences.py`: `test_no_issuer_and_no_audience_is_compiled_in`, `test_the_library_writes_down_no_issuer_and_no_audience_value`, `test_an_empty_list_refuses_everything`, `test_a_jwks_url_is_a_key_source_so_it_clears_that_blocker` | `built` |
| Order of checks; three required claims | `dag_tools/central_gateway/identity.py:399-445` | not stated | `test_gateway_accepted_issuers_and_audiences.py`: `test_an_unaccepted_issuer_is_refused_for_THAT_reason_not_as_an_invalid_token`, `test_a_token_naming_another_audience_is_refused` | `built` |
| Key set: three seconds, cached, failure not remembered | `dag_tools/central_gateway/identity.py:91`, `:296-325`, `:422-427` | not stated | no test named here | `built` |
| Posture default `observe` | `dag_tools/central_gateway/identity.py:186-198` | not stated | `test_gateway_verified_identity.py`: `test_posture_parse` | `built` |
| Internal doors share verification | `dag_tools/central_gateway/internal_door.py:101-131` | AGENTS.md §4 | — | `built` |
| One service token, no refresh; development token wins | `dag_tools/cortex_data/client.py:95-112`, `:146-159`, `:190` | not stated | `test_cortex_client_documented_contract.py`: `test_missing_auth_fails_at_construction_not_at_read`, `test_dev_token_wins_over_m2m`; no test of expiry | `built` |
| A client per IO manager load; writes raise | `dag_tools/io_managers/cortex_io_manager.py:111-117`, `:147-153`, `:155-175` | not stated | no test of the per-load grant | `built` |
