export * as SessionPublicationAdmission from "./publication-admission.js"

import { Context, Effect, Option } from "effect"
import { eq } from "drizzle-orm"
import type { Event } from "@opencode/schema/event"
import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { SessionEvent } from "@opencode/schema/session-event"
import type { Database } from "../database/database.js"
import { PublicationAdmissionError } from "./error.js"
import { SessionLifecycleTable } from "./sql.js"
import type { SessionSchema } from "./schema.js"
import type { SessionError } from "@opencode/schema/session-error"

// A capture-owned publisher supplies this local service only after verifying
// host authority for its exact immutable unit. No request metadata carries it.
export class Service extends Context.Service<
  Service,
  {
    readonly accepts: (event: Event.Payload) => boolean
    readonly publicationFor?: (
      event: Event.Payload,
    ) => ExecutionDisclosure.PublicationProof | ExecutionDisclosure.ToolPublicationProof | undefined
    readonly publication?: ExecutionDisclosure.PublicationProof | ExecutionDisclosure.ToolPublicationProof
  }
>()("@opencode/SessionPublicationAdmission") {}

// Source-free control data can terminate a denied execution without publishing
// provider text, context, model output, metadata or an unbounded error message.
export const rejectedExecution = Object.freeze({
  type: "authorization",
  message: "Execution stopped because authorization is unavailable.",
})

export const rejectedProviderAuthentication = Object.freeze({
  type: "provider.auth",
  message: "Your model provider login was rejected. Reconnect it and try again.",
})

export const rejectedModel = Object.freeze({
  type: "provider.no-route",
  message: "The selected model is unavailable. Choose another model and try again.",
})

// Retain only source-free failure categories. Provider messages may contain
// credentials, request bodies, or content the reader is not allowed to see.
export function failure(error: SessionError.Error) {
  if (error.type === rejectedProviderAuthentication.type) return rejectedProviderAuthentication
  if (error.type === rejectedModel.type) return rejectedModel
  return rejectedExecution
}

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
  const error = data.error
  if (
    event.type === SessionEvent.Execution.Failed.type &&
    event.metadata === undefined &&
    Object.keys(data).length === 2 &&
    Object.hasOwn(data, "sessionID") &&
    Object.hasOwn(data, "error") &&
    data.sessionID === sessionID &&
    typeof data.sessionID === "string" &&
    typeof error === "object" &&
    error !== null &&
    Object.keys(error).length === 2 &&
    Object.hasOwn(error, "type") &&
    Object.hasOwn(error, "message") &&
    "type" in error &&
    "message" in error &&
    [rejectedExecution, rejectedProviderAuthentication, rejectedModel].some(
      (allowed) => error.type === allowed.type && error.message === allowed.message,
    )
  )
    return
  const admission = Option.getOrUndefined(yield* Effect.serviceOption(Service))
  if (!admission?.accepts(event))
    return yield* Effect.die(
      new PublicationAdmissionError({
        eventType: event.type,
        reason: admission ? "event-rejected" : "service-missing",
      }),
    )
})
