# IntelliOps V2

A **support agent for Databricks cost observability** at the job and cluster level. Backed by Unity Catalog system tables, pre-aggregated Delta features, and a LangGraph LLM agent with tool access.

Documentation lives in [`doc/`](./doc/):

- [`doc/README.md`](./doc/README.md) — how to run the framework on Databricks (prerequisites, setup, scheduled jobs, agent usage, dashboards, troubleshooting).
- [`doc/ARCHITECTURE.md`](./doc/ARCHITECTURE.md) — module responsibilities, data flow, design decisions, rules for future changes.
- [`doc/data_catalog.md`](./doc/data_catalog.md) — every table, view, and Vector Search index this framework creates, with columns and purpose.
- [`doc/data_reconciliation.md`](./doc/data_reconciliation.md) — reconciliation plan for `intelliops.feature_store` (checks + tolerances + how to respond to failures).
- [`doc/questions.md`](./doc/questions.md) — example questions to ask the agent, grouped by category.
- [`doc/plan.md`](./doc/plan.md) — original implementation plan.
