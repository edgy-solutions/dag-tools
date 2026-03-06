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

### 2. Backwards Compatibility
- Since other repositories (like `pub-tools`) depend on `dag-tools`, **DO NOT** make breaking changes to function signatures or export names without explicit approval from the user. 
- If adding a new feature to an existing shared component, make the new parameters optional with sensible defaults.

### 3. Maintaining the Documentation Trifecta
As an agent operating on this project, part of your job is self-maintenance of the AI guardrails.
If you add a new category of common tool (e.g., a new suite of dbt wrappers), you MUST concurrently update:
1.  `llms.txt` (if the domain/intent changes).
2.  `.cursorrules` (if the enforced stack rules change).
3.  `README.md` (to document the new tools for human developers).
