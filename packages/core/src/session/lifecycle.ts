export * as SessionLifecycle from "./lifecycle.js"

import { eq, isNull, sql } from "drizzle-orm"
import { Effect } from "effect"
import type { Database } from "../database/database.js"
import { NotFoundError } from "./error.js"
import type { SessionSchema } from "./schema.js"
import { SessionLifecycleTable } from "./sql.js"

type DatabaseService = Database.Interface["db"]

// Call inside the projection/admission transaction. Both activation and
// retirement write the same retained row, serializing separate PG connections.
export const activate = Effect.fn("SessionLifecycle.activate")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
) {
  const row = yield* db
    .insert(SessionLifecycleTable)
    .values({ id: sessionID })
    .onConflictDoUpdate({
      target: SessionLifecycleTable.id,
      set: { id: sessionID },
      setWhere: isNull(SessionLifecycleTable.retired_at),
    })
    .returning({ id: SessionLifecycleTable.id })
    .get()
    .pipe(Effect.orDie)
  if (!row) return yield* new NotFoundError({ sessionID })
})

// Early rejection avoids preparing work for a retired identity; activate is
// still required at the final transactional materialization/admission boundary.
export const check = Effect.fn("SessionLifecycle.check")(function* (db: DatabaseService, sessionID: SessionSchema.ID) {
  const row = yield* db
    .select({ retiredAt: SessionLifecycleTable.retired_at })
    .from(SessionLifecycleTable)
    .where(eq(SessionLifecycleTable.id, sessionID))
    .get()
    .pipe(Effect.orDie)
  if (row?.retiredAt != null) return yield* new NotFoundError({ sessionID })
})

export const retire = Effect.fn("SessionLifecycle.retire")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
) {
  yield* db
    .insert(SessionLifecycleTable)
    .values({ id: sessionID, retired_at: Date.now() })
    .onConflictDoUpdate({
      target: SessionLifecycleTable.id,
      set: { retired_at: sql`coalesce(${SessionLifecycleTable.retired_at}, excluded.retired_at)` },
    })
    .run()
    .pipe(Effect.orDie)
})
