export * as SessionExecutionCapability from "./execution-capability.js"

import { Effect } from "effect"
import { Mcp } from "@opencode-ai/schema/mcp"
import { SessionMessage } from "./message.js"
import { SessionSchema } from "./schema.js"

export interface Grant {
  readonly inputID: SessionMessage.ID
  readonly mcp: Mcp.RequestServers
}

const grants = new Map<SessionSchema.ID, Grant>()

export const get = (sessionID: SessionSchema.ID) => Effect.sync(() => grants.get(sessionID))
export const set = (sessionID: SessionSchema.ID, grant: Grant | undefined) =>
  Effect.sync(() => {
    if (grant) grants.set(sessionID, grant)
    else grants.delete(sessionID)
  })
export const clear = (sessionID: SessionSchema.ID) => Effect.sync(() => void grants.delete(sessionID))
