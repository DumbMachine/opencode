export * as SessionExecutionCapability from "./execution-capability.js"

import { Effect } from "effect"
import { Mcp } from "@opencode/schema/mcp"
import { SessionMessage } from "./message.js"
import { SessionSchema } from "./schema.js"

export interface Grant {
  readonly inputID: SessionMessage.ID
  readonly mcp: Mcp.RequestServers
}

// Credentials stay process-local and are bound to the admitted input that
// needs them. Admitting a queued prompt must not replace the active grant.
const grants = new Map<SessionSchema.ID, Map<SessionMessage.ID, Grant>>()
const selected = new Map<SessionSchema.ID, SessionMessage.ID>()

export const get = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID | undefined) =>
  Effect.sync(() => {
    if (inputID === undefined) return undefined
    selected.set(sessionID, inputID)
    return grants.get(sessionID)?.get(inputID)
  })

export const set = (sessionID: SessionSchema.ID, grant: Grant) =>
  Effect.sync(() => {
    let inputs = grants.get(sessionID)
    if (!inputs) grants.set(sessionID, (inputs = new Map()))
    inputs.set(grant.inputID, grant)
  })

// Keep pending inputs and the selected input for explicit continuation after
// an interrupt. Credentials are never stored in the durable event log.
export const retain = (sessionID: SessionSchema.ID, pending: ReadonlySet<SessionMessage.ID>, keepSelected = false) =>
  Effect.sync(() => {
    const inputs = grants.get(sessionID)
    if (!inputs) return
    for (const id of inputs.keys())
      if (!pending.has(id) && (!keepSelected || id !== selected.get(sessionID))) inputs.delete(id)
    if (!keepSelected) selected.delete(sessionID)
    if (inputs.size === 0) grants.delete(sessionID)
  })

export const drop = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID) =>
  Effect.sync(() => {
    const inputs = grants.get(sessionID)
    inputs?.delete(inputID)
    if (inputs?.size === 0) grants.delete(sessionID)
    if (selected.get(sessionID) === inputID) selected.delete(sessionID)
  })
