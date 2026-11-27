# One file-backed asset to a published dataset, on a dag-tools older than 0.17

Status: describes the code at `4fbd272` (v0.16.1), the last release before
0.17.0. Where master (`2f8441b`) differs, the difference is named in its own
section below and nowhere else, so the rest of the page can be read against a
deployment that has not moved. Every bucket, prefix and namespace below is
invented.

For the engineer who runs a deployment pinned at 0.16.x or earlier: what a
producer actually does to make one file-backed asset readable by another
process, using only what that release ships — and the one thing on this path
that is broken on your version with a symptom naming the wrong component.

**Read the version section first if you are debugging rather than building.**
Below 0.17 the central gateway image cannot verify an RS256 token — the default
— and reports that as `token_invalid`, so a correct producer, a correct broker
and a correctly minted credential still end in a refusal that blames the
consumer's token. It is a deployment fault in a token fault's words, and it is
the most expensive thing on this page.

---

## The five steps

| # | What the producer does | The shipped file that does it |
|---|---|---|
| 1 | Write the bytes where another process can reach them | [`mesh_demo_assets.py`](../dag_tools/user_deployment/mesh_demo_assets.py) |
| 2 | Advertise the location from the IO manager | [`mesh_publishing.py`](../dag_tools/io_managers/mesh_publishing.py) |
| 3 | Let the broker register it | [`domain_broker/main.py`](../dag_tools/domain_broker/main.py) |
| 4 | Check what it actually advertised | [`diagnose_unadvertised_assets.py`](../scripts/diagnose_unadvertised_assets.py) |
| 5 | Read it back from somewhere else | [`docs/cortex-data-client.md`](cortex-data-client.md) |

Note what that column is not. Every step on the [warehouse-table
path](warehouse-table-to-published-dataset.md) is runnable from an `examples/`
directory with no warehouse and no credentials. This path has no such directory
at either pin: the storage, the broker and the gateway all have to be real
before anything here is observable. That is the first entry under [Where this
falls short](#where-this-falls-short).

### 1. Write the bytes somewhere another process can reach

A file-backed asset is published by *where its writer wrote*, so the first
decision is storage, not metadata.

The only runnable file-backed published dataset this release ships is
`dag_tools/user_deployment/mesh_demo_assets.py`, gated on
`DAG_TOOLS_DEMO_MODE`. `build_mesh_demo_io_manager()` reads bucket, prefix,
credentials and endpoint from the environment and returns a
`ConfigurableArrowIOManager` over an `S3FSConfig`. Two things in its docstring
are load-bearing and easy to skip:

- **the bucket must exist before the first materialization.** Nothing on this
  path creates it.
- **the endpoint and credentials must be the ones the broker pod carries**, or
  the ticket the broker advertises resolves for the producer and for nobody
  else — which presents as a consumer bug.

### 2. Advertise the location from the IO manager

The contract is duck-typed. The broker calls
`physical_coordinates(asset_key_path)` on whatever sits in
`Definitions(resources=...)` under the asset's `io_manager_key`, after a
`hasattr` check. Nothing requires a dag-tools class — which is also how a
**vendored copy** of one passes every health check and serves nothing: same
class name, no shared ancestry, none of the protocol. Check the *module* in a
stack trace, not the class name.

`MeshPublishable` supplies `physical_coordinates` from hooks. One is required:

```python
from dag_tools.io_managers import MeshPublishable

class MyIOManager(MeshPublishable, ConfigurableIOManagerFactory):
    def mesh_uri(self, asset_key_path):
        return f"{self.uri_base.rstrip('/')}/{'/'.join(asset_key_path)}/"

    def mesh_endpoint(self):
        return self.fs.common.end_point      # an FQDN — see below
```

`mesh_source_type`, `mesh_endpoint`, `mesh_region`, `mesh_scope` and
`mesh_extra` are optional. Three things that look fine and are not:

- **`mesh_uri` must be where the writer wrote.** Deriving it from the asset key
  is a guess the moment anything other than this IO manager did the write — a
  dlt pipeline, for instance.
- **a directory of part files needs the trailing slash.** Without it the read
  404s against real object storage and *works* on a local filesystem, so it
  passes in development and fails in the cluster.
- **the endpoint must resolve from where the consumer runs.**
  `<service>.<namespace>.svc.cluster.local:9000`, not a bare service name that
  resolves only inside the producer's own namespace. Violations are counted in
  `/health.adr0044.non_fqdn_hosts`.

`mesh_source_type` defaults to `s3_parquet`, and a value outside
`KNOWN_SOURCE_TYPES` makes `physical_coordinates` return `None` — refused at
the producer rather than discovered at read time by a consumer, who would
otherwise get a `ValueError` naming no producer at all.

**There is no hook for credentials, and that is structural.** The broker mints,
per request, scoped to the asset and expiring with the access window. Anything
a producer puts under `credentials` is stripped at load and counted in
`/health.adr0044.echoed_credentials_dropped`. Minting inside
`physical_coordinates()` would not be a smaller version of the same thing: it
is called once at load and cached in `LOCAL_ASSETS` for the process lifetime,
so what it returned would be one credential shared by every caller of every
asset until the pod restarted.

**Returning `None` is a first-class answer.** An asset on the pod's local disk,
or in a format no consumer can read, has no location another process can use.
An advertised-but-unreadable location is *worse* than an unadvertised asset,
because the gateway routes consumers to it with full confidence.

### 3. Let the broker register it

Four preconditions, in the broker's own order:

1. an IO manager resolves for the asset's `io_manager_key`;
2. it has `physical_coordinates`;
3. calling it does not raise;
4. it returns a ticket — not `None` — whose `source_type` maps to a physical
   platform.

Fail any one and the asset is withheld rather than registered with placeholder
coordinates, and the reason is named.

**A `dagster`-platform URN is not a misspelling of the `s3` one.** It means the
asset has no physical location. The name is the symptom; the missing protocol
is the cause.

### 4. Check what it actually advertised

Three places, and at this pin only the first and third are any use:

- **the broker pod's startup log.** `Advertising N of M asset(s). NOT
  advertised: {...}` at WARNING, with a count per reason, plus one line per
  withheld asset naming it.
- **`/health.adr0044`**, which at this pin carries
  `echoed_credentials_dropped`, `unprotected_source_types` and
  `non_fqdn_hosts` — and **no per-reason `unadvertised` count**, although
  `_unadvertisable_reason`'s own docstring calls its return values "stable keys
  for `/health.adr0044.unadvertised`". That key is not in the payload at this
  pin, and is not on master either: the reasons are logged, not served. Read
  the log.
- **`scripts/diagnose_unadvertised_assets.py`**, which walks the same
  preconditions per asset and names which one declined:

```bash
kubectl cp scripts/diagnose_unadvertised_assets.py <ns>/<broker-pod>:/tmp/d.py
kubectl exec -n <ns> <broker-pod> -- python /tmp/d.py
kubectl exec -n <ns> <broker-pod> -- python /tmp/d.py <asset-name-substring>
```

### 5. Read it back from somewhere else

```python
from dag_tools.cortex_data.client import CortexDataClient

lf = CortexDataClient().get_dataframe(
    "urn:li:dataset:(urn:li:dataPlatform:s3,example-lake/mesh_demo/my_asset,PROD)"
)
```

One constructor, one method, a Polars `LazyFrame` back. The read goes through
the gateway, which resolves a ticket from the broker and carries the minted
credential. `s3_parquet`, `s3_delta` and `s3_iceberg` are genuinely lazy;
`postgres` and `clickhouse` are a `.lazy()` wrapper around a table already
pulled whole. [`docs/cortex-data-client.md`](cortex-data-client.md) is the
consumer's half of this path and is not restated here.

---

## What 0.17 and 0.18 changed on this path

Measured per tag range, not read off the CHANGELOG's section headings — the
0.17.0 entry for this does not sit under a gateway heading.

### 0.17.0: the gateway image rejected every RS256 token

Its Dockerfile installed bare `pyjwt`, and PyJWT's signature algorithms live in
the optional `crypto` extra. Without it `import jwt` still succeeds, the
container still starts, it still passes its healthcheck, and only
`HS256/384/512` register. A gateway configured for RS256 — the default — comes
up clean and refuses every token as `token_invalid` whatever its signature,
while PyJWT's own explanation (`RS256 requires 'cryptography' to be installed`)
is swallowed by the verifier's broad `except`.

Two ways out that do not need a library upgrade:

- install `pyjwt[crypto]` into your gateway image. **Only the Dockerfile
  governs the image** — the gateway's build context never reads the project
  file, so adding the extra to a dependency list changes nothing about what
  runs.
- or set `GATEWAY_JWT_ALGORITHMS` to an HMAC algorithm the installed library
  can actually verify. A smaller change and a weaker posture.

0.17.0 itself does **not** change the refusal vocabulary, the refusal counts or
the HTTP statuses. It adds a startup check that signs and verifies a token with
each configured algorithm, sorts them into verified / unexercised / unsupported
/ failed, and makes `/ready` refuse — before Redis — when any is unsupported or
failed. Upgrading does not fix a token; it stops the deployment claiming to be
ready to verify one.

It also adds `scripts/check_gateway_signing.py`, which verifies a freshly
minted in-memory RS256 token on both paths the gateway can take (a key resolved
through `jwt.PyJWK`, and a static PEM — they fail differently). It needs no
network and writes no key material, so it can be copied into an older image and
run there.

### 0.18.0: a posture, a status and an announcement

- **An unminted ticket was served carrying the producer's own credential** — a
  write credential, unscoped to the asset, not expiring with the access window.
  0.18.0 adds `BROKER_UNMINTED_POSTURE` (`observe`, today's behaviour plus
  numbers, or `enforce`) at the one seam every resolve path returns through,
  before the ticket is stamped, and publishes `would_refuse` and `refused`
  separately under `/health.adr0044.unminted_credentials`. On your version
  nothing refuses this and nothing counts it.

  The bound on your exposure is real and worth knowing:
  `KNOWN_SOURCE_TYPES == set(_MINTERS)` at this pin, so every source type
  `MeshPublishable` will advertise has a minter, and a producer using the mixin
  cannot fall off the minted path. A **hand-rolled** `physical_coordinates`
  naming a backend with no minter can, and on your version it is served with
  the producer's credential in silence.
- **The gateway turned every non-200, non-404 broker response into a 502**,
  reaching a reader as `MeshUnavailable` — telling the reader to retry and the
  operator to look at broker health, both the wrong place. 0.18.0 forwards a
  403 as a refusal, classified `NotEntitled`.
- The internal door announces itself, and `docs/cortex-data-client.md` gains
  the matching section.

`dag_tools/io_managers/mesh_publishing.py` is otherwise **unchanged** between
this pin and master apart from one comment correction, so step 2 above is the
same code on every version in this range.

## Where this falls short

- **No shipped example runs this path.** The one runnable file-backed published
  dataset is demo assets in the user deployment behind `DAG_TOOLS_DEMO_MODE`,
  not an `examples/` directory you can walk with no cluster. Storage, broker
  and gateway all have to be real first — unlike every step of the
  [warehouse-table path](warehouse-table-to-published-dataset.md).
- **The reasons an asset was withheld are logged, not served** (step 4). An
  operator watching `/health` sees the advertised count fall and no reason for
  it.
- **Nothing on this path classifies the data.** The column labelling, the
  propagation and the catalog registration all sit on `warehouse_source`, which
  serves tables. A file-backed asset is advertised and read with no column
  labels and no catalog entry of its own, so there is nothing for a broker to
  decide a column-level question against.
- **A consumer cannot pin the read to a version.** The mechanism exists at this
  pin and the absence is counted
  (`/health.ticket_identity.as_of_absent_by_source_type`), but no reader ships,
  so `as_of` carries only what a producer advertised, labelled as such. Two
  measured readers (`s3_parquet`, `postgres`) are on master and unreleased as
  of `2f8441b`; Delta and Iceberg are deliberately still absent.
- **An unminted ticket is a live exposure here with no number against it**, as
  the version section says. If anything in your deployment implements
  `physical_coordinates` by hand, that is the thing to audit before anything
  else on this list.

## What to do first

1. **Run `scripts/check_gateway_signing.py` inside your gateway image**, before
   building anything. If it fails, no consumer read described above can succeed
   and the error you get will name the consumer's token.
2. **Grep your deployment for `physical_coordinates` defined by hand**, and for
   IO manager classes whose module is not `dag_tools`. Those are the two ways
   this path fails while every health check passes.
3. **Then one asset, end to end**: materialize it, read the broker's startup
   log for the `Advertising N of M` line, and run the diagnose script against
   that asset's name before trying a read.
4. Only then widen. A deployment that advertises 104 assets and can serve none
   of them is a real failure mode on this path, and it reports itself as a
   naming puzzle.
