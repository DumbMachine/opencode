import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

// Protected (context policy) sessions are retired. Drop them with their storage instead of migrating them.
const migration: DatabaseMigration.Migration = {
  id: "20261028123620_drop_protected_sessions",
  up(tx, dialect = "sqlite") {
    return Effect.gen(function* () {
      const column = dialect === "postgres" ? "DROP COLUMN IF EXISTS" : "DROP COLUMN"
      const protectedSessions = `SELECT "id" FROM "session_v2" WHERE "context_policy" IS NOT NULL`
      yield* tx.run(`DELETE FROM "event_sequence" WHERE "aggregate_id" IN (${protectedSessions});`)
      yield* tx.run(
        `DELETE FROM "session_lifecycle" WHERE "context_policy" IS NOT NULL OR "id" IN (${protectedSessions});`,
      )
      yield* tx.run(`DELETE FROM "session_v2" WHERE "context_policy" IS NOT NULL;`)
      yield* tx.run(`DROP TABLE IF EXISTS "session_terminal_control";`)
      yield* tx.run(`DROP TABLE IF EXISTS "session_terminal_invocation";`)
      yield* tx.run(`ALTER TABLE "session_lifecycle" ${column} "context_policy";`)
      yield* tx.run(`ALTER TABLE "session_message" ${column} "publication_provenance";`)
      yield* tx.run(`ALTER TABLE "session_v2" ${column} "context_policy";`)
    })
  },
}

export default migration
