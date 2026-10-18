import { Effect } from "effect"
import { sql } from "drizzle-orm"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261018133558_session_context_policy",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(
        sql`ALTER TABLE ${sql.identifier("session_lifecycle")} ADD COLUMN ${sql.identifier("context_policy")} text`,
      )
      yield* tx.run(
        sql`ALTER TABLE ${sql.identifier("session_v2")} ADD COLUMN ${sql.identifier("context_policy")} text`,
      )
    })
  },
}

export default migration
