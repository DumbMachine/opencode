import { Effect } from "effect"
import { sql } from "drizzle-orm"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261019214250_terminal_invocation_intent",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(sql`CREATE TABLE session_terminal_invocation (
        session_id text NOT NULL REFERENCES session_v2(id) ON DELETE CASCADE,
        input_id text NOT NULL, invocation_id text NOT NULL, context_reference text NOT NULL,
        evidence text NOT NULL, created bigint NOT NULL,
        CONSTRAINT session_terminal_invocation_pk PRIMARY KEY (session_id, input_id, invocation_id)
      )`)
    })
  },
}
export default migration
