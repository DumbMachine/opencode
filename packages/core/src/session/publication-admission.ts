export * as SessionPublicationAdmission from "./publication-admission.js"

import { Context, Effect, Option } from "effect"
import { eq } from "drizzle-orm"
import type { Event } from "@opencode/schema/event"
import { SessionEvent } from "@opencode/schema/session-event"
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

// Source-free control data can terminate a denied execution without publishing
// provider text, context, model output, metadata or an unbounded error message.
export const rejectedExecution = Object.freeze({
  type: "authorization",
  message: "Execution stopped because authorization is unavailable.",
})

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
  const data = event.data as Record<string, unknown>
  if (
    event.type === SessionEvent.Execution.Failed.type &&
    event.metadata === undefined &&
    Object.keys(data).length === 2 &&
    Object.hasOwn(data, "sessionID") &&
    Object.hasOwn(data, "error") &&
    data.sessionID === sessionID &&
    typeof data.sessionID === "string" &&
    typeof data.error === "object" &&
    data.error !== null &&
    Object.keys(data.error).length === 2 &&
    Object.hasOwn(data.error, "type") &&
    Object.hasOwn(data.error, "message") &&
    "type" in data.error &&
    data.error.type === rejectedExecution.type &&
    "message" in data.error &&
    data.error.message === rejectedExecution.message
  )
    return
  const admission = Option.getOrUndefined(yield* Effect.serviceOption(Service))
  if (!admission?.accepts(event))
    return yield* Effect.die(new Error("Execution publication authorization is unavailable"))
})
