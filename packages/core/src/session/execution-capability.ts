export * as SessionExecutionCapability from "./execution-capability.js"

import { Effect } from "effect"
import { isDeepStrictEqual } from "node:util"
import { Mcp } from "@opencode/schema/mcp"
import { SessionMessage } from "./message.js"
import { SessionSchema } from "./schema.js"

export interface Grant {
  readonly inputID: SessionMessage.ID
  readonly mcp: Mcp.RequestServers
  readonly revision?: number
}

// Credentials stay process-local and are bound to the admitted input that
// needs them. Admitting a queued prompt must not replace the active grant.
const grants = new Map<SessionSchema.ID, Map<SessionMessage.ID, Grant>>()
const selected = new Map<SessionSchema.ID, SessionMessage.ID>()

export const get = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID | undefined) =>
  Effect.sync(() => {
    if (inputID === undefined) {
      selected.delete(sessionID)
      return undefined
    }
    selected.set(sessionID, inputID)
    return grants.get(sessionID)?.get(inputID)
  })

// Input identity is process-local, like its installed credentials. During
// delivery it is unknown until the durable user message commits.
export const selectedInput = (sessionID: SessionSchema.ID) => selected.get(sessionID)

export const clearSelection = (sessionID: SessionSchema.ID) =>
  Effect.sync(() => {
    selected.delete(sessionID)
  })

export const select = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID) =>
  Effect.sync(() => {
    selected.set(sessionID, inputID)
  })

export const set = (sessionID: SessionSchema.ID, grant: Grant) =>
  Effect.sync(() => {
    let inputs = grants.get(sessionID)
    if (!inputs) grants.set(sessionID, (inputs = new Map()))
    const previous = inputs.get(grant.inputID)
    if (previous?.revision !== undefined) {
      if (grant.revision === undefined || grant.revision < previous.revision) return false
      if (grant.revision === previous.revision) return isDeepStrictEqual(grant.mcp, previous.mcp)
    }
    inputs.set(grant.inputID, grant)
    return true
  })

// Keep pending grants and, after interruption, the last input grant for explicit
// continuation. A settled execution never keeps a live control target.
// Credentials are never stored in the durable event log.
export const retain = (sessionID: SessionSchema.ID, pending: ReadonlySet<SessionMessage.ID>, keepSelected = false) =>
  Effect.sync(() => {
    const current = selected.get(sessionID)
    selected.delete(sessionID)
    const inputs = grants.get(sessionID)
    if (!inputs) return
    for (const id of inputs.keys()) if (!pending.has(id) && (!keepSelected || id !== current)) inputs.delete(id)
    if (inputs.size === 0) grants.delete(sessionID)
  })

export const drop = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID) =>
  Effect.sync(() => {
    const inputs = grants.get(sessionID)
    inputs?.delete(inputID)
    if (inputs?.size === 0) grants.delete(sessionID)
    if (selected.get(sessionID) === inputID) selected.delete(sessionID)
  })
