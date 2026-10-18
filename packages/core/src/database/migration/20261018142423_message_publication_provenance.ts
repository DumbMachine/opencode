import { Effect } from "effect"
import { sql } from "drizzle-orm"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261018142423_message_publication_provenance",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(
        sql`ALTER TABLE ${sql.identifier("session_message")} ADD COLUMN ${sql.identifier("publication_provenance")} text`,
      )
    })
  },
}

export default migration
