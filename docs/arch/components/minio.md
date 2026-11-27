# MinIO (CO-MINIO)

MinIO is the S3-compatible object store the mesh code is written to reach. Nothing in this repo is specific to it: every client speaks the S3 protocol and takes an endpoint address, so the same code reaches any compatible store. The dag-tools chart does not deploy an object store. This record lists the clients that reach one, how each is given its credential, and what a reader's ticket carries.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the dag-tools pin listed there (`503607d`). The record shape is provisional.

## Findings

1. **The chart deploys no object store.** No template in the dag-tools chart creates one, and no chart value names a bucket. A deployment supplies the store and tells each client where it is. `built`
2. **The Delta and Arrow IO managers, and the Arrow resource, take an explicit key pair and endpoint in config.** The two IO managers each define an identical common block; neither imports the other's, so they agree by duplication. In each the access key id, the secret and the endpoint are required. The region has a fixed default, and plain HTTP is off unless a deployment turns it on. Delta's renames are not marked safe, so a backend that cannot rename atomically is not treated as one that can. The Arrow resource declares the same required key pair and endpoint, a fixed region default and plain HTTP off; the [S3 sensor](s3-sensor.md)'s Arrow component constructs it with its required fields unset (that record, finding 10). `built`
3. **The PyArrow clients may create a bucket.** The backend of each IO manager that uses PyArrow's S3 filesystem sets bucket creation on by default, so a write to a bucket that does not exist creates it rather than failing. The Arrow resource's client sets it on in code with no setting to turn it off, and that client's only method reads. `built`
4. **A reader's ticket carries only the region and the endpoint, never the writer's key.** The coordinates an IO manager advertises for a ticket are the region and, when set, the endpoint address. The credential a reader receives is minted by the [domain broker](domain-broker.md) per request, read-only and scoped to the dataset's prefix (that record, findings 6 and 9). The reasons are in invincible-agent ADR-0044. `built`
5. **The demo IO manager reads its credential from the standard environment variables, and an unset one becomes an empty string.** It builds an Arrow IO manager from the platform's standard key, secret, endpoint and region variables, with a default bucket name and a default prefix. Plain HTTP is turned on when the endpoint address begins with `http://`. A missing key or secret is passed on as an empty string, so the failure shows at the first read or write, not at load. It is registered only in demo mode. `built`
6. **The S3 sensor and the document tables reach the store with the client library's own credential.** The [S3 sensor](s3-sensor.md) lists a prefix through Dagster's S3 resource, whose credential is whatever that resource resolves (that record, finding 6). The [document tables](document-tables.md) component passes no credential to the store and relies on the client's default chain (that record, findings 2 and 11). `built`
7. **A dlt filesystem destination with an AWS key pair is classed as MinIO.** When the destination is a filesystem and its config carries an AWS access key id, the factory labels its kind `minio` and builds dlt's AWS credentials from the environment. `built`
8. **The qualification registry uses the default credential chain and an optional endpoint.** The registry client builds a boto3 client from the default chain, with the bucket from `--registry` or `DAGTOOLS_REGISTRY` and the endpoint from `--endpoint-url` or `DAGTOOLS_S3_ENDPOINT`. Its immutable write checks for the key first and refuses to overwrite. `built`
9. **The DuckDB resource takes an optional key pair and splits an endpoint address into a host and a TLS flag.** A key id and secret are passed to DuckDB only when set. The region is set on every connection, from a field with a fixed region default. Given an endpoint, the resource derives the host and whether to use TLS from the address's scheme, and defaults to path-style addressing. With no endpoint it sets no endpoint and no TLS flag, but still applies an addressing style a deployment sets. A constructor reads the key pair, region and endpoint from the four standard AWS variables. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| IO manager `access_key_id`, `secret_access_key`, `end_point` | required | the writer's key pair and the store's address |
| IO manager `region` | a fixed region default | sent to the store and advertised on the ticket |
| IO manager `allow_http` | `false` | allows plain HTTP to the store |
| PyArrow backend `allow_bucket_creation` | `true` | creates a missing bucket on write; not a setting on the Arrow resource's client, which always sets it on |
| Delta `allow_unsafe_rename` | `false` | whether the backend is treated as able to rename atomically |
| demo bucket and prefix variables | a default bucket name and a default prefix | where the demo IO manager writes |
| demo key, secret and endpoint variables | empty | the demo IO manager's credential and address |
| `--registry` / `DAGTOOLS_REGISTRY` | unset | the qualification registry's bucket |
| `--endpoint-url` / `DAGTOOLS_S3_ENDPOINT` | unset, meaning the cloud default | the registry's store address |
| DuckDB `aws_access_key_id`, `aws_secret_access_key` | unset | DuckDB's key pair, passed only when set |
| DuckDB `aws_region` | a fixed region default | set on every connection |
| DuckDB `endpoint_url`, `url_style` | unset; path style when an endpoint is set | the store address DuckDB reads through |
| registry region; signature version | a fixed region default; pinned in code | the registry client's region and request signing |

## Gaps

- **Each client names its credential differently** (findings 2, 5, 6, 7, 8 and 9). The IO managers and the Arrow resource take it in config, DuckDB in config or from the standard variables, the demo and dlt read the standard variables, and the sensor, the document tables and the registry use the default chain. A deployment configures one store several ways. Remedy: not on the roadmap. Status: `built`.
- **Plain HTTP is a setting, and the demo infers it from the address** (findings 2 and 5). Remedy: not on the roadmap. Status: `built`.
- **A write, or a read through the Arrow resource, may create a bucket** (finding 3). A misspelt bucket name produces a new bucket rather than an error. Remedy: not on the roadmap. Status: `built`.
- **A demo credential left unset fails at the first read or write** (finding 5). Remedy: not on the roadmap. Status: `built`.
- **A narrowing reaches an object-store read only at the client** (finding 4). The minted credential is scoped to a prefix, not to columns or rows. Remedy: [roadmap row 2](../ROADMAP.md) (dag-tools #71). Status: `built`.

## Not established from code

- Which object store a deployment runs, and whether it is MinIO.
- Which credential names a deployment sets for each client, and whether they are the same key.
- Whether a deployment's store refuses plain HTTP.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| XE-IO-MANAGER | is called by | S3 API | access key pair and endpoint required in the IO manager's config | a Delta or Arrow IO manager's files, written and read | `dag_tools/io_managers/delta.py:102-114`, `:162`, `:180`; `test_image_s3fs_floor.py`: `test_the_installed_s3fs_accepts_an_endpoint_url` | `built` |
| XE-QUAL-TOOLING | is called by | S3 API | the default credential chain; optional endpoint | the registry client's immutable per-build records, with a pointer written last | `dag_tools/qual/registry/client.py:45-55`, `:62-75`; `test_registry_client.py`: `test_storage_put_immutable_refuses_overwrite` | `built` |
| XE-DUCKDB-RESOURCE | is called by | S3 API | not established | files read through an endpoint split into a host and a TLS flag | `dag_tools/resources/duckdb.py:90-98`; `test_duckdb_resource.py`: `test_split_endpoint` | `built` |
| XE-IO-MANAGER | is called by | S3 API | standard key and secret from the environment; an unset one becomes empty | demo files written and read, by an IO manager registered only in demo mode | `dag_tools/user_deployment/definitions.py:62-67`, `:233-256`; no test of an unset key | `built` |

CO-CLIENT, CO-S3-SENSOR, CO-DOCUMENT-TABLES and CO-DLT-PIPELINE also reach an S3-compatible store, through their own configuration. Their records draw that store by an `XE-` id (`XE-OBJECT-STORE`, or `XE-DLT-DESTINATION` for a pipeline's destination) rather than as this component, because which store a deployment runs is still open in this record. They are therefore not rows here.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | holds the files that IO managers write and readers read | — |
| How the mesh wires it | an external store each client reaches by endpoint address; not deployed by the chart | — |
| Sign-in | a key pair per client, from config, the environment or the default chain; readers get a minted, prefix-scoped key | [NATO R7](../alignment/nato-dcra.md) |
| Data path | producers write directly; readers read directly with the ticket's coordinates and a minted key | — |
| Plane | `infrastructure`: where data at rest lives. The shared lifecycle services keep their records in it too | [NATO L1](../alignment/nato-dcra.md) |
| Security role | holds credentials and enforces the minted key's prefix scope | [NATO R5](../alignment/nato-dcra.md) |
| Services served | the storage behind governed exchanges | [DoD S7](../alignment/dod-dmra.md) |

## Sources

All paths are at `503607dd321cd6627509bfc93955357b8566597f`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No object store in the chart | `helm/dag-tools/templates/` (no store template) | not stated | — | `built` |
| Explicit key pair and endpoint; HTTP off; renames not marked safe | `dag_tools/io_managers/delta.py:102-114`, `:162`, `:180`, `:357-359`; `dag_tools/io_managers/arrow.py:36-41` | not stated | `test_image_s3fs_floor.py`: `test_the_installed_s3fs_accepts_an_endpoint_url` | `built` |
| Bucket creation on | `dag_tools/io_managers/delta.py:141-143`; `dag_tools/io_managers/arrow.py:51-54`, `:135-141`; `dag_tools/resources/arrow.py:10`, `:29`, `:32` | not stated | no test | `built` |
| Arrow resource: same fields; constructed unset | `dag_tools/resources/arrow.py:64-81`; `dag_tools/components/s3_sensor/arrow_component.py:65` | not stated | no test | `built` |
| Ticket carries region and endpoint only | `dag_tools/io_managers/delta.py:229-246`, `:298-302`; `dag_tools/io_managers/arrow.py:370` | invincible-agent ADR-0044 | `test_mesh_publishable_mixin.py`: `test_one_hook_produces_a_complete_ticket`, `test_the_mixin_offers_no_way_to_supply_credentials`, `test_fqdn_endpoint_passes_the_broker_check` | `built` |
| Demo IO manager from the environment; demo-gated | the demo assets module in `dag_tools/user_deployment/`, lines 188 and 212-225; `dag_tools/user_deployment/definitions.py:62-67`, `:233-256` | not stated | no test of an unset key | `built` |
| Sensor and document tables use the client's credential | `dag_tools/resources/s3.py:11-30`; `dag_tools/documents/sources.py:595`, `:611-616` | not stated | `test_documents_sources.py`: `test_s3_endpoint_url_is_accepted` | `built` |
| dlt filesystem with a key pair is MinIO | `dag_tools/asset_wrappers/dlt_assets_factory.py:325-335` | not stated | no test | `built` |
| Registry: default chain, endpoint option, immutable write; region default and signature version | `dag_tools/qual/registry/client.py:45-55`, `:62-75`; `dag_tools/qual/cli.py:105-118` | AGENTS.md §4b | `test_registry_client.py`: `test_storage_put_immutable_refuses_overwrite`, `test_publish_build_is_immutable_for_same_sha` | `built` |
| DuckDB key pair, region, endpoint split, path style; from the environment | `dag_tools/resources/duckdb.py:46-60`, `:84-98`, `:238-254`, `:256-270` | not stated | `test_duckdb_resource.py`: `test_split_endpoint`, `test_url_style_defaults_to_path_when_endpoint_set`, `test_no_endpoint_leaves_aws_defaults_alone` | `built` |
