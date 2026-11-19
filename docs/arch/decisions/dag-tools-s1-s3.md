# dag-tools decisions: §1 to §3

This record covers the decisions in `AGENTS.md` sections 1 to 3 at the component-records pin (`add5bc4`): reusability, declarative components, and durable state with double writes. [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). Line numbers are at the pin. The record shape is provisional.

## §1 Reusability

### §1.1 Nothing project-specific is named in library code

- **Decision.** The library hardcodes no project's buckets, tables or other names. A deployment supplies them.
- **Why.** Other repositories consume the library. A name compiled into it becomes every consumer's answer.
- **Where else.** [rulings.md](../../rulings.md), 2026-11-04, widened 2026-11-05, holds the wider rule.
- **Status.** `built`, with exceptions. Some fields still default to a generic object or platform name:
  - a time column in the telemetry readiness spec;
  - a driver in the telemetry component;
  - a platform fallback in the dbt project component.

  None names a project. The component records list such defaults without quoting them.

### §1.2 Resources and IO managers take their configuration at run time

- **Decision.** Every resource, IO manager and sensor takes its configuration through Dagster's configurable classes, so a deployment can inject its own values.
- **Why.** Configuration fixed in code cannot be set by a downstream project.
- **Status.** `built`. Nine classes in eight files subclass Dagster's configurable resource or IO manager classes.

  One component does not supply what its resource needs. The S3 arrow component binds its resource with no arguments, although the resource has four required fields ([S3 sensor record](../components/s3-sensor.md), finding 8). That component does not build at all (finding 10).

  The SAP induction handler is a Restate handler, not a Dagster resource. It reads its settings from the environment.

## §2 Declarative components

### §2.1 New wrappers use Dagster's GA component API

- **Decision.** New integration wrappers are built on `dagster.components`, not on the older experimental package.
- **Why.** The experimental package is deprecated.
- **Status.** `built`. Components import from `dagster.components`. The deprecated package is named nowhere in the library or its dependencies.

### §2.2 A component inherits Component, Resolvable and Model

- **Decision.** A new component subclasses all three classes and implements `build_defs`.
- **Why.** That combination lets Dagster resolve the component's attributes from YAML and build its definitions.
- **Status.** `built`. Fifteen component classes use the pattern.

  The dbt project component instead subclasses the Dagster dbt integration's own component. One component that uses the pattern does not build ([S3 sensor record](../components/s3-sensor.md), finding 10).

### §2.3 Restate integrations are wrapped in declarative components

- **Decision.** Extractors that dispatch to Restate are wrapped in declarative components, not loose factory scripts.
- **Why.** The component is the configurable surface, so dispatch is set from YAML like everything else.
- **Status.** `built`. Two components do this: one dispatches dlt rows and one dispatches API calls. See the [Restate dlt sync record](../components/restate-dlt-sync.md) and the [Restate API sync record](../components/restate-api-sync.md).

### §2.4 Metadata and configuration live in YAML

- **Decision.** Pipeline metadata, hints and configuration go in YAML, not in Python factory arguments.
- **Where else.** [rulings.md](../../rulings.md), 2026-11-04, holds the decision that everything built here is a component configured from YAML.
- **Status.** `built`. Each component's attributes are its YAML surface. Each component record lists its settings.

### §2.5 An asset defined inside build_defs comes from a factory function

- **Decision.** An asset that closes over component values is made by a factory function, not defined inline with the values as parameters.
- **Why.** Dagster reads every parameter of an asset function as an asset input. A closure value written as a parameter would be read as an input.
- **Status.** `built`. Factory functions build the assets in contract mapping, document tables, read audit, warehouse source and both Restate dispatch components.

## §3 Durable state machines and double writes

### §3.1 A handler that changes a database and an external API uses an outbox

- **Decision.** Such a handler records each change locally and also pushes it to the remote API.
- **Why.** Neither system can be the only record of a transition the other depends on.
- **Status.** `built`. The SAP induction handler is the case. One inner function performs both writes for every status change.

### §3.2 Every state transition is written in both places

- **Decision.** Each transition is recorded in the local database and pushed to the callback API.
- **Status.** `built`. Every transition goes through the one inner function: pending, error, permanent failure and success. No test covers the handler's transitions.

### §3.3 The two writes are separate durable steps

- **Decision.** The notification and the update each run in their own durable step, so each completes independently even if the other system fails.
- **Status.** `built`, with one exception. The local update is bounded with the shared retry options. The callback push raises on any status that is not a success and carries no retry options. So the push is retried for as long as its retry policy allows. That retry policy is not established from code.

### §3.4 Database clients are created once, not in a loop

- **Decision.** A handler never builds a database client inside a loop. It uses a shared instance.
- **Why.** A client per iteration leaks connections.
- **Status.** `built`.
  - The SAP handler keeps one module-level database client per worker. Its OData client is built once per invocation.
  - The Oracle and SQL Server handlers open one connection per step and close it when the step ends. None of them connects inside a loop.

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Handler paths are under `dag_tools/restate_handlers/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| §1.1 no project names; generic defaults remain | `dag_tools/otel_api_sync/spec.py:224-226`; `dag_tools/components/otel_api_sync/component.py:99`; `dag_tools/components/dbt_project/component.py:129` | AGENTS.md §1; rulings.md 2026-11-04 | no test of absence | `built` |
| §1.2 configurable resources and IO managers | `dag_tools/io_managers/cortex_io_manager.py`, `s3.py`, `sql.py`; `dag_tools/resources/arrow.py`, `duckdb.py`, `grist.py`, `s3.py`, `warehouse.py` | AGENTS.md §1 | — | `built` |
| §1.2 arrow component binds no arguments | `dag_tools/components/s3_sensor/arrow_component.py:64-66`; `dag_tools/resources/arrow.py:10-81` | AGENTS.md §1 | no test | `not built` |
| §2.1 GA component API | `dag_tools/components/restate_api_sync/component.py:7-9`; no match for the deprecated package in `dag_tools/` or `pyproject.toml` | AGENTS.md §2 | — | `built` |
| §2.2 fifteen triple-inheritance components | `dag_tools/components/*/component.py` (for example `restate_api_sync/component.py:17`, `restate_dlt_sync/component.py:347`, `warehouse_source/component.py:466`); `s3_sensor/*_component.py` | AGENTS.md §2 | — | `built` |
| §2.2 dbt component subclasses the integration's own | `dag_tools/components/dbt_project/component.py:21` | AGENTS.md §2 | — | `built` |
| §2.3 Restate wrapped in components | `dag_tools/components/restate_api_sync/component.py:17`; `dag_tools/components/restate_dlt_sync/component.py:347` | AGENTS.md §2 | see the two component records | `built` |
| §2.4 YAML configuration | each component's attributes | AGENTS.md §2; rulings.md 2026-11-04 | see the component records | `built` |
| §2.5 factory functions | `dag_tools/components/contract_mapping/component.py:374`; `document_tables/component.py:372`; `read_audit/component.py:170`; `warehouse_source/component.py:330`; `restate_api_sync/component.py:84`; `restate_dlt_sync/component.py:543`, `:756`, `:782`, `:811` | AGENTS.md §2 | — | `built` |
| §3.1, §3.2 outbox; every transition through one function | `sap_induction/service.py:23`, `:58-105`; transitions at `:118`, `:137`, `:178`, `:180`, `:186` | AGENTS.md §3 | no test of the transitions | `built` |
| §3.3 two steps; push unbounded and raising | `sap_induction/service.py:79`, `:82`, `:101-104` | AGENTS.md §3 | `test_restate_db_retry.py`: `test_every_connecting_step_in_the_handlers_carries_a_bound` (the update step only) | `built` |
| §3.4 one database client per worker; per-step connections elsewhere | `sap_induction/service.py:13-20`, `:33`, `:39`; `oracle_ack.py:115`; `oracle_control.py:43`, `:332`, `:389`; `api_sync.py:173` | AGENTS.md §3 | no test | `built` |
