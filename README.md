# ⌬ OpenCode (PostgreSQL Edition)

> Custom fork of [OpenCode v2](https://github.com/anomalyco/opencode) providing native PostgreSQL database storage and horizontal clustering support for agentic inference.

<p align="center">
  <a href="https://opencode.ai">
    <picture>
      <source srcset="packages/console/app/src/asset/logo-ornate-dark.svg" media="(prefers-color-scheme: dark)">
      <source srcset="packages/console/app/src/asset/logo-ornate-light.svg" media="(prefers-color-scheme: light)">
      <img src="packages/console/app/src/asset/logo-ornate-light.svg" alt="OpenCode logo">
    </picture>
  </a>
</p>

---

## Modifications in this Fork

This fork adds first-class multi-database capabilities to the OpenCode v2 Effect-TS runtime:

1. **Native PostgreSQL Backend (`@effect/sql-pg` + Drizzle ORM)**:
   - Full ACID transaction isolation and rollback support in Effect fibers.
   - Schema auto-bootstrapping and migration idempotency for dedicated PostgreSQL databases.
   - Native `BIGINT` numeric normalization and JSON serialization.
   - PostgreSQL `json_extract` polyfill for cross-dialect compatibility with OpenCode history and compaction queries.

2. **Dual-Engine Auto-Detection**:
   - Defaults to standard SQLite when no database URL is set.
   - Automatically activates PostgreSQL when `OPENCODE_DATABASE_URL` (or `OPENCODE_DATABASE_DIALECT=postgres`) is provided.
   - Hashed background daemon state files (`service-pg-<hash>.json`) to prevent port and socket collisions between SQLite and PostgreSQL instances.

3. **Coexistence via `opencodepg` Binary**:
   - Distributed as `opencodepg` so it can be installed and executed alongside upstream `opencode` without naming conflicts.

4. **Roadmap / Upcoming Features**:
   - Dynamic per-session and per-request MCP server configurations and headers.
   - Stateless cluster load balancing with shared PostgreSQL state.
   - Collaborative multiplayer thread subscriptions.

---

## Quick Start

### 1. Run with PostgreSQL

```bash
# Headless prompt mode
OPENCODE_DATABASE_URL="postgres://user:password@localhost:5432/opencode" opencodepg run "What is 2+2?"

# Interactive TUI
OPENCODE_DATABASE_URL="postgres://user:password@localhost:5432/opencode" opencodepg

# Background cluster server
OPENCODE_DATABASE_URL="postgres://user:password@localhost:5432/opencode" opencodepg serve --port 4000
```

### 2. Run with SQLite (Default)

```bash
# Operates out of the box with standard SQLite storage
opencodepg
```

---

## Testing

Run the dual-engine parity matrix test suite verifying 100% equivalence between SQLite and PostgreSQL:

```bash
bun test test/database-parity.test.ts test/database-postgres.test.ts test/database-migration.test.ts test/database-drizzle.test.ts
```

---

## Acknowledgments & Attribution

This project is a fork built on top of [OpenCode](https://github.com/anomalyco/opencode) by Anomaly Innovations / SST. It is not officially built or affiliated with the upstream OpenCode team.
