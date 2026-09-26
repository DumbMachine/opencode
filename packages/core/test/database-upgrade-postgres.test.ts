import { expect, test } from "bun:test"
import { Effect } from "effect"
import { Database } from "@opencode/core/database/database"
import { DatabaseMigration } from "@opencode/core/database/migration"
import viewed from "../src/database/migration/20260819222447_session_viewed_state"
import binding from "../src/database/migration/20260823191254_nullable_workspace_binding"
import permission from "../src/database/migration/20260910120000_clear_v1_session_permission"
import active from "../src/database/migration/20260923013825_project_time_active"

// Use a dedicated test database: the Database layer initializes its public schema.
const url = process.env.OPENCODE_TEST_DATABASE_URL

test.skipIf(!url)("upgrades existing PostgreSQL rows and preserves workspace references", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db.transaction((tx) =>
        Effect.gen(function* () {
          // Transaction-scoped fixture leaves the public schema and its journal intact.
          const schema = `upgrade_${crypto.randomUUID().replaceAll("-", "")}`
          yield* tx.run(`CREATE SCHEMA "${schema}"`)
          yield* tx.run(`SET LOCAL search_path TO "${schema}"`)
          yield* tx.run(`CREATE TABLE project (id text PRIMARY KEY, time_updated bigint NOT NULL)`)
          yield* tx.run(`CREATE TABLE workspace (id text PRIMARY KEY, binding text NOT NULL)`)
          yield* tx.run(
            `CREATE TABLE session_v2 (id text PRIMARY KEY, workspace_id text REFERENCES workspace(id), permission text)`,
          )
          yield* tx.run(`CREATE TABLE migration (id text PRIMARY KEY, time_completed bigint NOT NULL)`)
          yield* tx.run(`INSERT INTO migration VALUES ('fixture_baseline', 0)`)
          yield* tx.run(`INSERT INTO project VALUES ('project', 1780000000000)`)
          yield* tx.run(`INSERT INTO workspace VALUES ('workspace', 'existing-binding')`)
          yield* tx.run(`INSERT INTO session_v2 VALUES ('session', 'workspace', 'legacy-permission')`)

          const migrations = [viewed, binding, permission, active]
          yield* DatabaseMigration.applyOnly(tx, migrations, "postgres")
          yield* DatabaseMigration.applyOnly(tx, migrations, "postgres")
          yield* tx.run(`UPDATE session_v2 SET time_idle = 1780000000001, time_viewed = 1780000000002`)
          yield* tx.run(`INSERT INTO workspace VALUES ('unbound', NULL)`)
          expect(yield* tx.get(`SELECT time_active FROM project`)).toEqual({ time_active: 1780000000000 })
          expect(yield* tx.get(`SELECT workspace_id, permission, time_idle, time_viewed FROM session_v2`)).toEqual({
            workspace_id: "workspace",
            permission: null,
            time_idle: 1780000000001,
            time_viewed: 1780000000002,
          })
          expect(yield* tx.get(`SELECT binding FROM workspace WHERE id = 'workspace'`)).toEqual({
            binding: "existing-binding",
          })
          expect((yield* tx.all(`SELECT id FROM migration`)).length).toBe(5)
          expect(
            (yield* tx.all(
              `SELECT * FROM information_schema.table_constraints WHERE table_schema = '${schema}' AND constraint_type = 'FOREIGN KEY'`,
            )).length,
          ).toBe(1)
          yield* tx.run(`DROP SCHEMA "${schema}" CASCADE`)
        }),
      )
    }).pipe(Effect.provide(Database.postgresDatabaseLayer(url!))),
  )
})
