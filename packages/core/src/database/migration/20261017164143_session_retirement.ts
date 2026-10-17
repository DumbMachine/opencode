import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261017164143_session_retirement",
  up(tx, dialect = "sqlite") {
    return Effect.gen(function* () {
      yield* tx.run(
        `CREATE TABLE "session_lifecycle" ("id" text PRIMARY KEY, "retired_at" ${dialect === "postgres" ? "bigint" : "integer"});`,
      )
      yield* tx.run(
        `INSERT INTO "session_lifecycle" ("id") SELECT "id" FROM "session_v2" WHERE true ON CONFLICT ("id") DO NOTHING;`,
      )
    })
  },
}

export default migration
