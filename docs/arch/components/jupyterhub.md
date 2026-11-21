# JupyterHub (CO-JUPYTERHUB)

JupyterHub serves notebooks to many users from one place. The mesh's documentation names it as a place the [mesh data client](mesh-data-client.md) is used, and the client is a library a notebook can import. Nothing at either pin deploys JupyterHub or configures a notebook's environment. This record says what a notebook would need to set, from the client's code, and what is not wired.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **Neither chart deploys JupyterHub.** The dag-tools chart does not mention it, and no invincible-agent chart template or default value does. `not built`
2. **The documentation names JupyterHub as a place the data client runs without arguments.** The client's guide says it needs no arguments when its environment is set, in the cluster and in JupyterHub. Nothing at either pin sets that environment for a notebook. `not built`
3. **What a notebook must set is fixed by the client's construction.** The gateway address is required, from an argument or `CORTEX_BROKER_URL`. A token is required too: a developer token from an argument or `MESH_DEV_TOKEN`, or else a client id and secret from `CORTEX_CLIENT_ID` and `CORTEX_CLIENT_SECRET`, which the client exchanges for a token at the identity provider's token address. With neither, construction fails, naming both (see [the data client record](mesh-data-client.md), finding 2). `built`
4. **A notebook can name its user through `CORTEX_USER_TOKEN`.** Outside a request, the client takes the user's token from that variable. With the variable unset, a notebook that names no subject reads as the service, because every constructed client already holds the service opt-in (see [the data client record](mesh-data-client.md), findings 3 and 4). The client does not refresh the user's token. `built`
5. **A notebook's read goes through the gateway like any other.** The client authorizes each read with the gateway and then reads the data itself (see [the data client record](mesh-data-client.md), findings 5 and 7). Nothing in that path knows the caller is a notebook. `built`
6. **How a user's token reaches a notebook is the subject of an invincible-agent plan.** The plan is cited by path only. Nothing at either pin delivers the token. `proposed`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `CORTEX_BROKER_URL` | none; required | the gateway address the client authorizes reads with |
| `MESH_DEV_TOKEN` | unset | a token the client sends as is |
| `CORTEX_CLIENT_ID`, `CORTEX_CLIENT_SECRET` | unset | service credentials the client exchanges for a token |
| `KEYCLOAK_TOKEN_URL` | an in-cluster default | where that exchange is made |
| `CORTEX_USER_TOKEN` | unset | the user a read is for, outside a request |

## Gaps

- **Nothing deploys JupyterHub or sets a notebook's environment** (findings 1 and 2). Remedy: not on the roadmap.
- **Nothing delivers a user's token to a notebook, and the client does not refresh one** (findings 4 and 6). Remedy: not on the roadmap.
- **A notebook that sets no user token reads with the service's entitlements** (finding 4). Remedy: not on the roadmap.

## Not established from code

- Whether a deployment runs JupyterHub, and how its users sign in.
- How a user's token would reach a notebook, and how long it would live.
- Whether a notebook in a deployment reads as the user or as a service.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-CLIENT | calls | in-process | the notebook's environment, which nothing at the pin sets | the notebook imports the library | `docs/cortex-data-client.md:25-27`; no test | `not built` |
| CO-GATEWAY | calls | HTTP | developer token, or a token from the client credentials exchange; a user token from the environment names the user | authorization of a read, answered with a ticket | `dag_tools/cortex_data/client.py:88-112`; `dag_tools/cortex_data/identity.py:34`, `:213-215`; no test here | `built` |
| CO-KEYCLOAK | calls | HTTP | client id and secret | a service token request | `dag_tools/cortex_data/client.py:88-112`; no test here | `built` |
| XE-TOKEN-DELIVERY | is called by | not established | not established | a user's token | invincible-agent `docs/plans/jupyter-user-token-data-access.md`; no test | `proposed` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | notebooks where people explore mesh data | — |
| How the mesh wires it | not wired; the data client is a library a notebook can import | — |
| Sign-in | whatever token the notebook's environment carries; nothing sets it | [NATO R7](../alignment/nato-dcra.md) |
| Data path | notebook to the gateway for a ticket, then to the store; the same as any client | — |
| Plane | `analytical-data`: exploration by people | [NATO L4](../alignment/nato-dcra.md) |
| Security role | none of its own; the gateway decides each read | — |
| Services served | none of its own; it consumes data exchange through the client | [DoD S7](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. Invincible-agent chart paths are under `helm/invincible-agent/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Not in either chart | `helm/dag-tools/` (no match for `jupyter`, any case); invincible-agent `templates/` and `values.yaml` (no match) | not stated | — | `not built` |
| Documentation names JupyterHub | `docs/cortex-data-client.md:25-27` | not stated | — | `not built` |
| Construction requirements | `dag_tools/cortex_data/client.py:88-112` | not stated | see [the data client record](mesh-data-client.md) | `built` |
| User token rung, no refresh | `dag_tools/cortex_data/identity.py:34`, `:213-215` (no refresh in that module) | not stated | see [the data client record](mesh-data-client.md) | `built` |
| Reads go through the gateway | see [the data client record](mesh-data-client.md), findings 5 and 7 | not stated | see that record | `built` |
| Token delivery plan | invincible-agent `docs/plans/jupyter-user-token-data-access.md` | not stated | — | `proposed` |
