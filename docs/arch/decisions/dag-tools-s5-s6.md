# dag-tools decisions: §5 and §6

This record covers the decisions in `AGENTS.md` sections 5 and 6 at the component-records pin (`add5bc4`): configuration design and backwards compatibility. [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §5 Normalized configuration design

### §5.1 Dagster configuration comes first

- **Decision.** Every new component and resource takes its settings through Dagster's configuration framework:
  - run config classes;
  - configurable resources;
  - component attributes.
- **Why.** One configuration surface means a deployment configures every tool the same way.
- **Where else.** [rulings.md](../../rulings.md), 2026-11-04, holds the decision that everything built here is a component configured from YAML.
- **Status.** `built`. The component records list each component's attributes and which of them are required. Six component records cite this section, where the required settings are the configuration: the dlt pipeline, Grist ingest, the telemetry-to-API sync, both Restate dispatch components and the S3 sensor.

### §5.2 Tool configuration is wrapped, not passed through

- **Decision.** A component does not expose an underlying tool's raw configuration when that differs from Dagster practice. It defines its own schema and translates.
- **Why.** A pass-through ties the component's surface to another tool's vocabulary, which changes when that tool changes.
- **Status.** `built`, with two exceptions where a raw shape reaches the tool:
  - The S3 sensor takes a dictionary and unpacks it into Dagster's S3 resource ([S3 sensor record](../components/s3-sensor.md), finding 6).
  - The dbt project component keeps the Dagster dbt integration's own field for command-line arguments ([dbt project record](../components/dbt-project.md)).

### §5.3 The component translates its settings for the engine

- **Decision.** A component's `build_defs`, or a resource's set-up, maps the standard inputs into what the engine needs.
- **Status.** `built`. Examples:
  - the dlt pipeline component pops its schedule and table settings before it builds the dlt configuration ([dlt pipeline record](../components/dlt-pipeline.md));
  - the contract mapping component renders a plan into dbt models and tests ([contract mapping record](../components/contract-mapping.md)).

## §6 Backwards compatibility

### §6.1 No breaking change to a signature or an export without approval

- **Decision.** Function signatures and exported names do not change in a breaking way unless the repository owner approves.
- **Why.** Other repositories depend on the library.
- **Status.** `built` where a test pins it. Two tests pin public entry points:
  - the legacy dlt entry points keep their signatures;
  - the data client's frame read still returns only a frame.

  No general check covers every export.

### §6.2 A new parameter on a shared component is optional, with a default

- **Decision.** A feature added to an existing shared function or component arrives as an optional parameter with a sensible default.
- **Status.** `built` where the census records a case. For example, the contract mapping row route takes the load's timestamp as an optional keyword with a default.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §5.1 Dagster configuration first | component attributes; see the component records | AGENTS.md §5; rulings.md 2026-11-04 | see the component records | `built` |
| §5.2 S3 resource dictionary unpacked | `dag_tools/components/s3_sensor/sensor_component.py:47-50`, `:157` | AGENTS.md §5 | `test_s3_sensor.py`: `test_get_client` | `built` |
| §5.2 dbt command-line field kept | `dag_tools/components/dbt_project/component.py:21`, `:137` | AGENTS.md §5 | see the dbt project record | `built` |
| §5.3 translation in the component | see the dlt pipeline and contract mapping records | AGENTS.md §5 | see those records | `built` |
| §6.1 signatures pinned | `dag_tools_tests/test_dlt_source_kinds.py:220`; `dag_tools_tests/test_ticket_receipt.py:168` | AGENTS.md §6 | `test_the_legacy_entry_points_keep_their_signatures`, `test_get_dataframe_still_returns_only_a_frame` | `built` where a test pins it |
| §6.2 optional keyword with a default | `dag_tools/contract_mapping/emit.py:354-357` | AGENTS.md §6 | no test of the default | `built` where the census records a case |
