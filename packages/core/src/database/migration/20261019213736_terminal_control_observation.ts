import { Effect } from "effect"
import { sql } from "drizzle-orm"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261019213736_terminal_control_observation",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(sql`CREATE TABLE session_terminal_control (
        session_id text NOT NULL REFERENCES session_v2(id) ON DELETE CASCADE,
        input_id text NOT NULL, attempt text NOT NULL, message_id text NOT NULL,
        tool_call_id text NOT NULL, producer_epoch text NOT NULL, producer_key_id text NOT NULL,
        context_reference text NOT NULL, receipt_reference text NOT NULL, evidence text NOT NULL,
        unit_id text, unit_digest text, created bigint NOT NULL,
        CONSTRAINT session_terminal_control_pk PRIMARY KEY (session_id, input_id)
      )`)
    })
  },
}
export default migration
