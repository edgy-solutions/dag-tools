# S3 sensor (CO-S3-SENSOR)

The S3 sensor package turns an object-store prefix into Dagster runs. It defines three components. `S3SensorComponent` polls a prefix and requests one run per new object. `S3ToFileComponent` builds an asset that records a file reference. `S3ToArrowComponent` is meant to build an asset that opens the file as a PyArrow dataset; as written it does not build (finding 10). The sensor is meant to feed one of the two asset components through a shared dynamic partition name.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **`S3SensorComponent` builds one sensor and one resource, and no assets or jobs.** The sensor is named from `name`, else from the prefix with slashes and hyphens turned into underscores plus a suffix, else a bare default. The resource key is the sensor name plus a suffix. The sensor points at a job by name (`target_job`); the job must be defined elsewhere. `built`
2. **The sensor starts stopped, and any value other than `RUNNING` also means stopped.** `default_status` defaults to `STOPPED`. The code compares the string to `RUNNING` and falls back to stopped for anything else, so a misspelling is not refused. `built`
3. **Each tick lists the prefix, keeps objects after a cursor, and requests one run per object that passes the filters.** The cursor is the `LastModified` text of the newest object seen. Objects are dropped if the key contains an excluded substring (default two entries), ends in a slash, contains `_SUCCESS` or `.metadata`, or fails the optional regex. An object with no `LastModified` is dropped only when it is the only object listed. When two or more objects are listed and any of them lacks a `LastModified`, the sort that orders them raises and the tick fails. If the cursor's timestamp is no longer in the listing, every object is returned again, and the run key, which joins the object's ETag and key, is what keeps a run from being requested twice. The partition key is the key with slashes replaced by double underscores. `built`
4. **The run config carries the object's URL to `target_op` and any `additional_target_ops`, and the sensor registers the partition keys on a dynamic partition by name.** The sensor does not define the partition; it adds keys to the one named `partition_name`. If every new object is filtered out, the cursor still advances and the tick is skipped. `built`
5. **Required config is the bucket, prefix, partition name, target job and target op; everything else has a default.** The bucket and prefix are required strings on the sensor. The two asset components each require one name field (`partition_name` for the arrow one; `name` and `partition_name` for the file one). `built`
6. **The object-store credential is whatever the underlying `S3Resource` resolves, and the sensor reads it through that resource.** The `s3_resource` dict is unpacked into Dagster's `S3Resource`; with none, its defaults apply, which leave credential lookup to boto. The sensor lists with the boto client from that resource. The component names no environment variable and holds no secret field. `built`
7. **The sensor connects directly to the object store. It does not use the central gateway, the domain broker or a ticket.** It calls `list_objects_v2` with the resource's client and presents no identity to the mesh. `built`
8. **`S3ToArrowComponent`'s asset would read the file through a separate credential path, a PyArrow filesystem built from explicit key fields; none of it is reached, because the component does not build (finding 10).** `ArrowResource` has four required fields (a base URI, an access key id, a secret access key and an endpoint) and two with defaults (a region and a plain-HTTP flag). The component's code binds `ArrowResource()` with no arguments, so the four required fields would be given no value. The asset would return a dataset object to its IO manager, with the format chosen from the file suffix and CSV (also `.gz`) using the delimiter. `not built`
9. **`S3ToFileComponent` builds an asset and a job, and the asset stores only a reference.** The asset has the file kind, takes `file_url` in op config, and returns a metadata dict (URL, partition, extra metadata, static `op_config`). The job is named from the asset name and selects that one asset. No object is read. `built`
10. **`S3ToArrowComponent` does not build.** Its `build_defs` declares the asset's op config with the delimiter's default given as pydantic's `Field` rather than Dagster's config field. Dagster refuses that when the asset decorator runs, inside `build_defs`, with a config definition error, so a code location that declares the component fails to load. This was run against the real class on Dagster 1.13.1, and the same refusal is reported on 1.10.19. The asset, job and resource the code would define (an asset named from the partition name plus a suffix, a job named from the partition name, an IO manager key defaulting to `io_manager`) are therefore never produced. No test and no example builds the component; it is named only in the package's exports. `not built`
11. **Few things are refused at load, and the component itself checks none of the names it is given.** Missing required fields fail the load through the model. A `target_job` that names no job in the code location also fails the load: the component does not check it, but Dagster refuses a sensor whose job is not found when the definitions are assembled. A bad `s3_resource` dict fails when the resource is built. A listing failure raises in the tick. Nothing confirms that `target_op` or `partition_name` exist; a wrong op name is found only when a run's config is checked. Neither the sensor nor either asset component publishes to the catalog. `built`
12. **Tests pin the resource and the sensor's naming and partition key, and nothing else in the package.** `test_s3_sensor.py` covers the filter methods, four naming cases and the partition key. No test covers `get_s3_keys`, the cursor, the file or arrow components, or the default status. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `S3SensorComponent.bucket` | required | the bucket to list |
| `S3SensorComponent.prefix` | required | the prefix to list; a trailing slash is added |
| `S3SensorComponent.partition_name` | required | the dynamic partition that receives the keys |
| `S3SensorComponent.target_job` | required | the job each run request targets, by name |
| `S3SensorComponent.target_op` | required | the op whose config receives `file_url` |
| `S3SensorComponent.name` | none | sensor name; else derived from the prefix |
| `S3SensorComponent.additional_target_ops` | empty list | other ops that receive the same `file_url` config |
| `S3SensorComponent.s3_resource` | none | arguments for the underlying `S3Resource`, such as an endpoint |
| `S3SensorComponent.s3_filter` | none | a regex matched from the start of the key |
| `S3SensorComponent.filter_patterns` | two substrings | substrings that exclude a key |
| `S3SensorComponent.default_status` | `STOPPED` | `RUNNING` starts the sensor; any other value stops it |
| `S3ToFileComponent.name` | required | asset name |
| `S3ToFileComponent.partition_name` | required | dynamic partition name |
| `S3ToFileComponent.op_config` | empty | static values merged into the asset's metadata |
| `S3ToArrowComponent.partition_name` | required | dynamic partition name |
| `S3ToArrowComponent.asset_name` | none | asset name; else derived from the partition name |
| `S3ToArrowComponent.io_manager_key` | `io_manager` | the IO manager that stores the dataset |
| `S3ToArrowComponent.delimiter` | `,` | CSV delimiter, passed as op config |

## Gaps

- **The cursor is a timestamp, and the listing is keyed by it** (finding 3). Objects sharing one `LastModified` text collapse to one entry, so all but one are not seen. No test covers `get_s3_keys`. Remedy: not on the roadmap.
- **`S3ToArrowComponent` does not build** (finding 10). A code location that declares it fails to load. No test builds it, which is how the defect stands at the pin. Remedy: not on the roadmap.
- **An object with no `LastModified` fails the tick when it is listed with others** (finding 3). A single such object is dropped silently instead. No test covers either case. Remedy: not on the roadmap.
- **A bad `default_status` is accepted as stopped** (finding 2). A deployment that meant `RUNNING` and misspelled it gets a stopped sensor with no message. No test covers it. Remedy: not on the roadmap.

## Not established from code

- How a deployment would supply the arrow resource's four required fields, once the component builds (finding 8).
- Which credential a deployment gives `S3Resource`, and whether it is read-only. The component sets no such limit.
- Whether a deployment wires `S3ToFileComponent` or `S3ToArrowComponent` to the sensor, and which job name it passes as `target_job`.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | turns new objects under a prefix into partitioned Dagster runs, and gives one working asset shape to receive them; the second does not build | — |
| How the mesh wires it | YAML components in a code location, joined by a shared dynamic partition name and a job name | — |
| Sign-in | to the object store, with the credential the S3 resource resolves, or explicit keys on the arrow resource; nothing is presented to the mesh | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct from the object store to the sensor and the asset; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | operational, where producers' files arrive | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none; no access decision is made here | — |
| Services served | moves arriving files into partitioned assets for a consumer | [DoD S7](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Component paths are under `dag_tools/components/s3_sensor/`; resources are under `dag_tools/resources/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| Sensor builds one sensor and one resource; naming; job by name | `sensor_component.py:67-80`, `:159-170` | not stated | `test_s3_sensor.py`: `test_default_sensor_name`, `test_custom_sensor_name`, `test_empty_prefix_default_name`, `test_custom_sensor_name_with_hyphens` | `built` |
| Default status | `sensor_component.py:62-65`, `:73` | not stated | no test | `built` |
| Tick: listing, cursor, filters, run key, partition key | `sensor_component.py:86-120`; `utils.py:6-52` (sorted and keyed by `LastModified` at `:35-39`, cursor not found at `:41-42`); `s3.py:54-73` | not stated | `test_s3_sensor.py`: `test_sensor_partition_key_is_full_path`, `test_apply_filter_directories`, `test_apply_filter_metadata`, `test_apply_filter_regex`, `test_apply_filter_no_regex`; no test of the cursor | `built` |
| Run config and partition registration | `sensor_component.py:35-45`, `:122-153` | not stated | no test of `additional_target_ops` | `built` |
| Required config | `sensor_component.py:28-33`; `file_component.py:26-28`; `arrow_component.py:23` | AGENTS.md §5 | no test | `built` |
| Credential from the S3 resource | `sensor_component.py:47-50`, `:157`; `s3.py:11-44` | not stated | `test_s3_sensor.py`: `test_get_client` | `built` |
| Direct connection, no gateway or ticket | `sensor_component.py:89-95`; `utils.py:26` | not stated | no test of absence | `built` |
| Arrow component credential path | `arrow_component.py:16`, `:64-66`; `arrow.py:10-81` | not stated | no test | `not built` |
| File component asset and job | `file_component.py:17-71` | not stated | no test | `built` |
| Arrow component does not build | `arrow_component.py:2`, `:28-67` (the decorator at `:37-43`, the config at `:41`); exported only at `dag_tools/__init__.py:16`, `:29` and `s3_sensor/__init__.py:5`, `:10` | not stated | run against the real class on Dagster 1.13.1: `DagsterInvalidConfigDefinitionError` from `build_defs`; no test | `not built` |
| Load and run refusals; no catalog publish | `sensor_component.py:23-65`, `:75-77`, `:157`; `utils.py:10-12` | not stated | no test | `built` |
| Test coverage | `test_s3_sensor.py` | not stated | file lists ten test functions, none on the file or arrow components | `built` |
