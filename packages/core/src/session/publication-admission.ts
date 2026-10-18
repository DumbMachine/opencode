export * as SessionPublicationAdmission from "./publication-admission.js"

import { Context, Effect, Option } from "effect"
import { eq } from "drizzle-orm"
import type { Event } from "@opencode/schema/event"
import type { Database } from "../database/database.js"
import { SessionLifecycleTable } from "./sql.js"
import type { SessionSchema } from "./schema.js"

// A capture-owned publisher supplies this local service only after verifying
// host authority for its exact immutable unit. No request metadata carries it.
export class Service extends Context.Service<
  Service,
  {
    readonly accepts: (event: Event.Payload) => boolean
  }
>()("@opencode/SessionPublicationAdmission") {}

// Called by the existing session projectors on their own transaction. It is
// intentionally scoped to opted-in original sessions; the general Bus is unchanged.
export const assert = Effect.fnUntraced(function* (
  db: Database.Interface["db"],
  sessionID: SessionSchema.ID,
  event: Event.Payload,
) {
  const retained = yield* db
    .select({ policy: SessionLifecycleTable.context_policy })
    .from(SessionLifecycleTable)
    .where(eq(SessionLifecycleTable.id, sessionID))
    .get()
    .pipe(Effect.orDie)
  if (!retained?.policy) return
  const admission = Option.getOrUndefined(yield* Effect.serviceOption(Service))
  if (!admission?.accepts(event))
    return yield* Effect.die(new Error("Execution publication authorization is unavailable"))
})
