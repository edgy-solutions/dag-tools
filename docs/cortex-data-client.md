# CortexDataClient — reading data from the mesh

`CortexDataClient` is the mesh's read path. You give it a DataHub URN; it
gives you a Polars `LazyFrame`. Where the bytes actually live — Parquet on
S3, Delta, Iceberg, PostgreSQL, ClickHouse — and which deployment owns them
is not your problem.

```python
import polars as pl
from dag_tools.cortex_data.client import CortexDataClient

client = CortexDataClient()
lf = client.get_dataframe(
    "urn:li:dataset:(urn:li:dataPlatform:s3,"
    "iagent-minio.publog-lake/publog/v_h2_fsg,PROD)"
)
df = lf.filter(pl.col("fsg") == "10").collect()
```

That is the whole API. One constructor, one method.

---

## Construction

The client is zero-config when the environment is set up, which is the
normal case inside the cluster and in JupyterHub.

| Variable | Required | Meaning |
|---|---|---|
| `CORTEX_BROKER_URL` | **yes** | Central Gateway base URL. No default — missing it raises `ValueError`. |
| `MESH_DEV_TOKEN` | one of these two | A pre-obtained JWT. Used as-is. |
| `CORTEX_CLIENT_ID` + `CORTEX_CLIENT_SECRET` | one of these two | M2M service-account credentials; the client fetches its own token via `client_credentials`. |
| `KEYCLOAK_TOKEN_URL` | no | Defaults to `http://keycloak/realms/master/protocol/openid-connect/token`. |

`MESH_DEV_TOKEN` wins if both are present. With neither, the constructor
raises rather than deferring the failure to read time.

Every parameter can also be passed explicitly, which is what the Dagster IO
manager does since resource config is not environment:

```python
client = CortexDataClient(
    broker_url="http://iagent-central-gateway:8000",
    client_id="...",
    client_secret="...",
)
```

### Who the read is authorized as

Identity resolves in a fixed order, and **the order is the point**:

```
caller=  ->  request context  ->  CORTEX_USER_TOKEN  ->  service (provisioned)
```

Inside a mesh request, failing to resolve **raises** rather than falling through.

```python
@app.execute()
def detect(data: In, caller: CallerIdentity) -> Out:
    client = CortexDataClient(caller=caller)   # explicit, always correct
    client = CortexDataClient()                # also correct: reads the request context
```

Both now read as the *user*. Previously the second line read as the **service** — every user of
the handler getting the service's entitlements, with rows returned and nothing erroring.

`caller=` takes any object exposing `authz_id`. The SDK's `CallerIdentity` satisfies it
structurally with no adapter; so can anything else, which is why `dag-tools` declares the shape
rather than importing the type — `iagent_mesh` is an **optional peer** (`current_caller` exported
as of 0.4.0), and naming its type in a public signature would make an optional dependency part of
this package's contract.

**Why the order matters.** Reversed — the environment above the caller — a variable set on a pod
would outrank the request's caller, and a config change nobody reviews as authorization becomes a
cross-tenant read. The caller outranking it is what keeps `CORTEX_USER_TOKEN` harmless on an
agent pod.

**Inside a request, there is no fall-through.** The SDK distinguishes *no request*
(`current_caller()` is `None`) from *a request whose caller did not resolve* (a caller with
`authz_id=None`). The second raises. Collapsing them is what would let an agent-pod read fall
back to a notebook-shaped environment lookup.

Without the SDK installed the request rung is skipped, and the client **says so** at
construction — a client outside the mesh should know it has no request context rather than
silently reading as something else.

### Failure modes are distinct

A composing verb one level up cannot report honestly over a client that collapses these, so it
doesn't:

| Exception | Means |
| --- | --- |
| `CallerUnresolved` | no identity to read as — a defect in the *call* |
| `NotEntitled` | the caller resolved and the gate refused (401/403) |
| `MeshUnavailable` | the read could not be attempted — **including a gateway 404** |

**A 404 is `MeshUnavailable`, not "absent".** Routes carry a TTL and are re-pushed on a
heartbeat, so it reports the owning deployment's *liveness*. Reporting "no such data" when the
truth is "the owner is down" is the dishonest report this taxonomy exists to prevent.

### Reading on behalf of a user

This is the part that is easy to get wrong, and it fails **closed** — you
get a blanket 403 rather than a warning.

When a service reads data *for an end user* (Engine DA answering a
question, a notebook proxy), the M2M token identifies the **service**, not
the person. Authorization has to be evaluated against the person. Two
headers carry that, and the client sets them from two constructor
arguments:

```python
client = CortexDataClient(
    client_id=..., client_secret=...,
    originator_sub="8f3e...",              # -> X-Originator-Sub
    originator_email="user@example.com",   # -> X-Originator-Email
)
```

`originator_email` is the one that matters for allow/deny. The gateway's
Topaz `can_read` check is **keyed on email** (the entitlement key is email
in sandbox, employee-id at work-deploy), and an M2M token carries no user
email. Omit it and the check has no subject to evaluate, so it denies —
for every user, uniformly.

`originator_sub` is checked against the explicit deny list and takes
precedence over the token's own `sub`, so a service account cannot be used
to read around a per-user denial.

Omit both when the service is genuinely reading as itself (a Dagster asset
materializing, a scheduled job). Then the M2M identity *is* the subject and
that is correct.

---

## What happens on `get_dataframe(urn)`

1. `POST {gateway}/api/v1/assets/{urn}/authorize` with the bearer token.
2. The gateway checks the deny list, then Topaz. **403** if not authorized.
3. It looks up `mesh_route:{urn}` in Redis to find the owning domain
   broker. **404** if no broker currently advertises that URN.
4. It proxies to that broker's `/api/v1/internal/resolve`, which returns a
   *routing ticket*: `source_type`, `physical_uri`, `credentials`, plus any
   `allowed_columns` / `row_filters` the gateway grafts on from Topaz.
5. The client dispatches on `source_type` and builds the LazyFrame.

The important consequence of step 3: **a URN is only readable while a
broker is advertising it.** `mesh_route:*` keys carry a TTL (5 minutes in
sandbox) and each broker re-pushes every `BROKER_REGISTER_INTERVAL_SEC`
seconds (120 by default), giving two to three attempts per TTL window. So
a 404 usually means the owning deployment is down, still starting, or has
missed enough pushes to fall out of the table — not that the asset does
not exist. It is a liveness signal, not a catalog lookup. If you need to
know whether an asset exists at all, ask DataHub, not the gateway.

Both HTTP calls use a 10-second timeout and there is no retry. Treat
`get_dataframe` as a call that can fail transiently.

---

## Source types

| `source_type` | Read via | Lazy? |
|---|---|---|
| `s3_parquet` | `pl.scan_parquet` | yes |
| `s3_delta` | `pl.scan_delta` | yes |
| `s3_iceberg` | `pyiceberg.catalog.load_catalog` → `pl.scan_iceberg(table)` | yes |
| `postgres` | `pl.read_database_uri` (ADBC/ConnectorX) | **no** |
| `clickhouse` | `clickhouse_connect` Arrow → `pl.from_arrow` | **no** |

Producers pick this by which IO manager they publish with:
`ConfigurableArrowIOManager` and `ConfigurableDuckDBIOManager` →
`s3_parquet`, `ConfigurableDeltaIOManager` → `s3_delta`,
`ConfigurableSQLIOManager` → `postgres` or `clickhouse` by dialect.

### Laziness is not uniform, and it matters

The return type is always a `LazyFrame`, but for the two database backends
that is a `.lazy()` wrapper around an **already-materialized** DataFrame.
The client issues `SELECT * FROM {schema}.{table}` and pulls the whole
table before you ever see it.

So this:

```python
lf = client.get_dataframe(postgres_urn)
df = lf.filter(pl.col("region") == "EMEA").head(10).collect()
```

reads the entire table over the wire and filters it in memory. The filter
is not pushed to the database. For object-store sources (`scan_parquet`,
`scan_delta`, `scan_iceberg`) predicate and projection pushdown work
normally and the same code is efficient.

If you need a real pushdown against Postgres or ClickHouse today, query it
directly rather than through the mesh read path.

### Extra dependencies

`pyiceberg` (for `s3_iceberg`) and `clickhouse-connect` (for `clickhouse`)
are imported lazily, inside their branches. A missing one surfaces as an
`ImportError` at read time, not at construction — so it can appear in
production for one asset while every other asset in the same process reads
fine.

`s3_iceberg` additionally requires the ticket to carry `catalog_uri` and
`table_identifier`; the client raises `ValueError` if either is absent,
because `pl.scan_iceberg(uri)` only understands Hadoop-style tables and
would silently fail on a real SQL/REST/Glue catalog.

---

## Row and column security

Topaz can return `allowed_columns` and `row_filters` alongside the
authorization decision. The client applies them:

```python
lf = lf.select(allowed_columns)
lf = lf.filter(pl.sql_expr(row_filters))
```

**These are applied client-side, in the returned LazyFrame** — for
`s3_parquet`, `s3_delta`, `s3_iceberg`, and `clickhouse`.

The one exception is `postgres`, which sets `apply_security = False` on the
grounds that PostgreSQL enforces RLS/CLS natively. Note that ClickHouse
does *not* get that exemption and is filtered client-side like the object
stores.

What this means in practice: the credentials in the ticket grant access to
the **whole** object or table. The narrowing happens in your process. That
is fine for the intended use — you asked for data and you got the subset
you are entitled to — but it is worth being clear-eyed that this is a
data-plane convention, not a storage-enforced boundary. Anyone holding a
ticket holds credentials broader than their entitlement.

---

## From Dagster

Do not construct the client in an asset body. Bind
`CortexPolarsIOManager` to the asset you want to read, and Dagster does it
for you:

```python
from dagster import AssetSpec, Definitions, asset
from dag_tools.io_managers import CortexPolarsIOManager

# A read handle for data another deployment owns.
other_domain_sales = AssetSpec(
    key="other_domain_sales",
    metadata={"dagster/io_manager_key": "cortex"},
)

@asset
def my_report(other_domain_sales):     # arrives as a Polars LazyFrame
    return other_domain_sales.filter(...).collect()

defs = Definitions(
    assets=[other_domain_sales, my_report],
    resources={"cortex": CortexPolarsIOManager(
        broker_url=..., client_id=..., client_secret=...,
    )},
)
```

Dagster loads an input using the IO manager of the asset that **produced**
it — which is why this manager binds to the upstream stub, not to the
asset doing the reading.

**`CortexPolarsIOManager` is read-only. `handle_output` raises.** It is
attached to assets another deployment may own, so letting it write would
make a consumer-side marker into a claim of ownership: the broker would
advertise a path this deployment never wrote and compete with the real
owner for the same routing key. To *publish*, use an IO manager that
implements `physical_coordinates` truthfully — `ConfigurableArrowIOManager`,
`ConfigurableSQLIOManager`, `ConfigurableDeltaIOManager`, or
`ConfigurableDuckDBIOManager`, all importable from
`dag_tools.io_managers`.

One caveat on the IO-manager path: it resolves the upstream URN from the
upstream's `datahub/urn` materialization metadata, and when that is absent
falls back to deriving `urn:li:dataset:(urn:li:dataPlatform:dagster,<key
with dots>,PROD)` from the asset key. That fallback names the *dagster*
platform, which will not match a URN the owner published on `s3` or
`postgres`, and the read 404s at the gateway. If you hit that, the fix is
on the producing side — the upstream should be emitting its physical URN.

---

## Failure modes

| Symptom | Cause |
|---|---|
| `ValueError: Must provide broker_url...` | `CORTEX_BROKER_URL` unset. |
| `ValueError: Must provide either jwt_token...` | Neither `MESH_DEV_TOKEN` nor both M2M vars set. |
| `403 Not authorized to access this asset` | Topaz denied. If it denies for *every* user, suspect a missing `originator_email` before suspecting the policy. |
| `403 user is on explicit deny list` | `effective_sub` matched `DENIED_USER_SUBS`. |
| `404 No active domain broker found` | No live route for that URN. Owning deployment down, still starting, or never advertised it. |
| `502 Bad gateway` | The gateway reached Redis but not the broker. |
| `ValueError: Unsupported source_type` | Broker returned a type this client version does not handle. |
| `ImportError` mid-read | Missing `pyiceberg` / `clickhouse-connect` for that specific source type. |
| S3 read 404s on a path that exists | A directory URI without its trailing slash. Producers must advertise `s3://bucket/prefix/`; `scan_parquet` HEADs a slash-less path and 404s. |

---

## See also

- [`dag_tools/cortex_data/client.py`](../dag_tools/cortex_data/client.py) — the implementation
- [`dag_tools/central_gateway/main.py`](../dag_tools/central_gateway/main.py) — authorization + routing
- [`dag_tools/domain_broker/main.py`](../dag_tools/domain_broker/main.py) — ticket minting
- README §9 — how the mesh fits together
