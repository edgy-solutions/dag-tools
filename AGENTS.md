# Guide for AI Agents Operating on `dag-tools`

Welcome, AI Agent! When interacting with this project, you must adhere strictly to this operational guide to maintain safety, reusability, and consistency.

## Pre-Flight Checklist
Before modifying *any* code or executing external network commands, you **MUST**:
1. Read `README.md` to understand the library's purpose and how to test changes locally.
2. Read `llms.txt` to align your semantic context (this is a shared utility library, not a pipeline execution repo).
3. Read `.cursorrules` to ensure your proposed edits comply with the strict tech stack (`uv`) and type-hinting standards.

## Operational Safety Boundaries

### 1. Reusability Focus
- **DO NOT** hardcode project-specific logic, bucket names, or table names into this repository. 
- All resources, IO Managers, and sensors must accept configuration at runtime via Dagster's `ConfigurableResource` or `ConfigurableIOManager` patterns so downstream projects can inject their own values.

### 2. Declarative Component First (Dagster 1.12 GA)
- When building new data integration wrappers (e.g., DLT, DBT, Airbyte), use the **Dagster 1.12 GA Component API** (`dagster.components`). The old experimental `dagster-components` library is deprecated.
- New components **MUST** use the `Component, Resolvable, Model` triple-inheritance pattern:
  ```python
  class MyComponent(Component, Resolvable, Model):
      my_field: str
      def build_defs(self, context: ComponentLoadContext) -> Definitions: ...
  ```
- When integrating with event-driven or stateful engines like **Restate**, avoid loose programmatic Python factory scripts. Wrap the existing extractors inside Declarative Components (like `restate_dlt_sync` and `restate_api_sync`).
- Pipeline metadata, hints, and configurations should be offloaded to YAML structure rather than hardcoded Python factory kwargs.
- When defining `@asset` functions inside `build_defs()` with closure variables, use a **factory function** pattern — Dagster 1.12 introspects all function parameters as asset inputs.

### 3. Durable State Machines & Double-Writes
- When building handlers that modify both a database and an external API (e.g., `SapInductionService`), follow the **Double-Write / Outbox Pattern**.
- Every state transition (e.g., SUCCESS -> ERROR) must be recorded in the local database AND pushed to any remote callback/webhook APIs.
- Use Restate's `ctx.run()` to wrap **both** the notification and the update in separate, durable steps to ensure they are eventually consistent, even if one system fails.
- **NEVER** instantiate database clients or heavy resources inside a handler loop; use a global singleton/cache pattern to prevent connection leaks.

### 4. Restate Worker Deployment
- **DO NOT** write per-project Restate worker entrypoints or Dockerfiles. New durable handlers are registered in `dag_tools.restate_handlers.serve.SERVICE_REGISTRY` and ship in the shared `restate-worker` image (`Dockerfile.restate-worker`).
- Deployments select handlers and wire Restate **purely through environment variables** (`RESTATE_SERVICES`, `RESTATE_ADMIN_URL`, `RESTATE_ADVERTISED_URI`); the worker self-registers on startup. When adding a handler module, add its key to `SERVICE_REGISTRY` so it becomes selectable.

### 4a. Shared Inventory Contract (`dag_tools.inventory`)
- The `AssetRecord` pydantic schema in `dag_tools/inventory/schema.py` is the **cross-process contract** between the runtime Domain Broker (long-lived pods, last quarter's release) and the CI Dagster qualification survey (every commit, today's release). They will read each other's records across versions.
- **Evolution rules**: additive-only. Never rename or remove fields. Only add new `Optional[...]` fields with defaults, and bump `SCHEMA_VERSION` by 1 in the same commit. Readers tolerate unknown fields via `extra="ignore"`. If you truly need a breaking change, it's a major library version and requires coordinated rollout — talk to the runtime mesh team first.
- **IO manager classification**: extend `dag_tools/inventory/classifier.py::FAMILY_REGISTRY` with the FQN of any new IO manager. MRO walking catches custom subclasses of registered ancestors. The substring fallback is a logged-at-WARNING last resort whose presence in the logs is a signal to add a registry entry.
- **Soft-failure discipline**: the extractor must never abort because one asset is malformed. Wrap every field access in per-field exception handling; record `None` and log WARNING. The whole point of the schema being mostly-optional is to make this safe.

### 5. Normalized Configuration Design
- **Dagster Configuration First**: All new components and resources MUST use the Dagster configuration framework (`Config`, `ConfigurableResource`, or `dagster.components` attributes).
- **Wrap & Standardize**: Do not expose raw, pass-through configurations for underlying tools (like `dlt` or `dbt`) directly if they deviate from Dagster's standard practices. Instead, define a Dagster-centric configuration schema that wraps and translates to the native tool's requirements.
- **Component Translation**: The component's `build_defs()` or the resource's initialization logic is responsible for mapping these standardized Dagster inputs into the specific format required by the underlying engine.

### 6. Backwards Compatibility
- Since other repositories (like `pub-tools`) depend on `dag-tools`, **DO NOT** make breaking changes to function signatures or export names without explicit approval from the user. 
- If adding a new feature to an existing shared component, make the new parameters optional with sensible defaults.

### 7. Maintaining the Documentation Trifecta
As an agent operating on this project, part of your job is self-maintenance of the AI guardrails.
If you add a new category of common tool (e.g., a new suite of dbt wrappers), you MUST concurrently update:
1.  `llms.txt` (if the domain/intent changes).
2.  `.cursorrules` (if the enforced stack rules change).
3.  `README.md` (to document the new tools for human developers).
