import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20260823191254_nullable_workspace_binding",
  foreignKeys: false,
  up(tx, dialect = "sqlite") {
    return Effect.gen(function* () {
      if (dialect === "postgres") {
        yield* tx.run(`ALTER TABLE "workspace" ALTER COLUMN "binding" DROP NOT NULL`)
        return
      }
      yield* tx.run(`
        CREATE TABLE \`__new_workspace\` (
          \`id\` text PRIMARY KEY,
          \`provider\` text NOT NULL,
          \`binding\` text,
          \`created_at\` integer NOT NULL,
          \`last_used_at\` integer NOT NULL
        );
      `)
      yield* tx.run(
        `INSERT INTO \`__new_workspace\`(\`id\`, \`provider\`, \`binding\`, \`created_at\`, \`last_used_at\`) SELECT \`id\`, \`provider\`, \`binding\`, \`created_at\`, \`last_used_at\` FROM \`workspace\`;`,
      )
      yield* tx.run(`DROP TABLE \`workspace\`;`)
      yield* tx.run(`ALTER TABLE \`__new_workspace\` RENAME TO \`workspace\`;`)
    })
  },
}

export default migration
