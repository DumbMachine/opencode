import { Schema } from "effect"
import type { Event } from "./event.js"
import { ascending } from "./identifier.js"
import { statics } from "./schema.js"

export const SessionMessageID = Schema.String.check(Schema.isStartsWith("msg_")).pipe(
  Schema.brand("Session.Message.ID"),
  statics((schema) => ({
    create: () => schema.make("msg_" + ascending()),
    fromEvent: (eventID: Event.ID) => schema.make(eventID.replace(/^evt_/, "msg_")),
  })),
)
export type SessionMessageID = typeof SessionMessageID.Type
