export * as SessionExecutionCapability from "./execution-capability.js"

import { Clock, Effect } from "effect"
import { Hash } from "@opencode/util/hash"
import { Mcp } from "@opencode/schema/mcp"
import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { SessionInputRequirements } from "./input-requirements.js"
import { SessionMessage } from "./message.js"
import { SessionSchema } from "./schema.js"

export interface Grant {
  readonly inputID: SessionMessage.ID
  readonly mcp?: Mcp.RequestServers
  readonly disclosure?: ExecutionDisclosure.Service
  readonly revision?: number
}

// Credentials stay process-local and are bound to the admitted input that
// needs them. Admitting a queued prompt must not replace the active grant.
const grants = new Map<SessionSchema.ID, Map<SessionMessage.ID, Grant>>()
const selected = new Map<SessionSchema.ID, SessionMessage.ID>()

type Renewal = {
  claim: string
  expiresAt: number
  minimumRevision?: number
  minimumFingerprint?: string
  committed?: string
  timer: ReturnType<typeof setTimeout>
}
const renewals = new Map<string, Renewal>()
const renewalKey = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID) => sessionID + "\0" + inputID
const claimLifetime = 120_000
const witnessLifetime = 30 * 60_000

function retainRenewal(key: string, input: Omit<Renewal, "timer">, lifetime: number) {
  const previous = renewals.get(key)
  if (previous) clearTimeout(previous.timer)
  const timer = setTimeout(() => {
    // Lease expiry must keep the pending input blocked and retain its revision
    // floor. Only a committed response witness may expire independently.
    if (renewals.get(key)?.timer === timer && input.committed !== undefined) renewals.delete(key)
  }, lifetime)
  if (typeof timer === "object" && "unref" in timer) timer.unref()
  const renewal = { ...input, timer }
  renewals.set(key, renewal)
  return renewal
}

function fingerprint(grant: Grant) {
  const canonical = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(canonical)
    if (value !== null && typeof value === "object")
      return Object.fromEntries(
        Object.entries(value)
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([key, item]) => [key, canonical(item)]),
      )
    return value
  }
  return Hash.sha256(JSON.stringify(canonical(grant)))
}

// Called only while the session inbox mutex protects the pending identity.
// Suspending readiness before ACK keeps prepared and fresh selectors parked.
export const beginRenewal = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID, claim: string) =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis
    const key = renewalKey(sessionID, inputID)
    const previous = renewals.get(key)
    if (previous?.claim === claim) {
      if (previous.expiresAt <= now) return undefined
      return { claim, expiresAt: previous.expiresAt }
    }
    if (previous && previous.expiresAt > now && previous.committed === undefined) return undefined
    const installed = grants.get(sessionID)?.get(inputID)
    const minimumRevision = installed?.revision ?? previous?.minimumRevision
    const minimumFingerprint = installed ? fingerprint(installed) : previous?.minimumFingerprint
    const renewal = retainRenewal(
      key,
      { claim, expiresAt: now + claimLifetime, minimumRevision, minimumFingerprint },
      claimLifetime,
    )
    const inputs = grants.get(sessionID)
    inputs?.delete(inputID)
    if (inputs?.size === 0) grants.delete(sessionID)
    if (selected.get(sessionID) === inputID) selected.delete(sessionID)
    return { claim, expiresAt: renewal.expiresAt }
  })

// A committed witness contains only a digest, never credentials. It allows a
// lost-response retry after native has already delivered the original input.
export const committedRenewal = (sessionID: SessionSchema.ID, grant: Grant, claim: string) =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis
    const renewal = renewals.get(renewalKey(sessionID, grant.inputID))
    return !!renewal && renewal.claim === claim && renewal.expiresAt > now && renewal.committed === fingerprint(grant)
  })

export const commitRenewal = (sessionID: SessionSchema.ID, grant: Grant, claim: string) =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis
    const key = renewalKey(sessionID, grant.inputID)
    const renewal = renewals.get(key)
    if (!renewal || renewal.claim !== claim || renewal.expiresAt <= now || renewal.committed !== undefined) return false
    if (
      renewal.minimumRevision !== undefined &&
      (grant.revision === undefined || grant.revision < renewal.minimumRevision)
    )
      return false
    if (
      grant.revision === renewal.minimumRevision &&
      renewal.minimumRevision !== undefined &&
      fingerprint(grant) !== renewal.minimumFingerprint
    )
      return false
    if (!install(sessionID, grant)) return false
    retainRenewal(key, { ...renewal, committed: fingerprint(grant), expiresAt: now + witnessLifetime }, witnessLifetime)
    return true
  })

// Called under the inbox mutex before waking an exact repaired input. An old
// claim cannot wake work after another installation, cancellation or restart.
export const resumableRenewal = (
  sessionID: SessionSchema.ID,
  inputID: SessionMessage.ID,
  claim: string,
  revision: number,
  required: readonly string[],
) =>
  Effect.gen(function* () {
    const grant = grants.get(sessionID)?.get(inputID)
    if (!grant || grant.revision !== revision || !(yield* committedRenewal(sessionID, grant, claim))) return false
    return yield* ready(sessionID, inputID, required)
  })

export const ready = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID, required: readonly string[] = []) =>
  Effect.sync(() => {
    const renewal = renewals.get(renewalKey(sessionID, inputID))
    const grant = grants.get(sessionID)?.get(inputID)
    return (
      SessionInputRequirements.supported(required) &&
      SessionInputRequirements.serviceSupported(grant?.disclosure) &&
      (!renewal || renewal.committed !== undefined) &&
      grant !== undefined &&
      (!required.includes("mcp") || grant.mcp !== undefined) &&
      (!required.includes("disclosure") || grant.disclosure !== undefined)
    )
  })

export const clear = (sessionID: SessionSchema.ID) =>
  Effect.sync(() => {
    grants.delete(sessionID)
    selected.delete(sessionID)
    for (const [key, renewal] of renewals) {
      if (!key.startsWith(sessionID + "\0")) continue
      clearTimeout(renewal.timer)
      renewals.delete(key)
    }
  })

export const get = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID | undefined) =>
  Effect.sync(() => {
    if (inputID === undefined) {
      selected.delete(sessionID)
      return undefined
    }
    selected.set(sessionID, inputID)
    const grant = grants.get(sessionID)?.get(inputID)
    return grant === undefined ? undefined : structuredClone(grant)
  })

// Input identity is process-local, like its installed credentials. During
// delivery it is unknown until the durable user message commits.
export const selectedInput = (sessionID: SessionSchema.ID) => selected.get(sessionID)

// A publication callback must retain this exact installation, not merely an
// equal input ID/revision reinstalled after cancellation or process recovery.
// This observer never changes the selected input or exposes credentials.
export const captureSelection = (
  sessionID: SessionSchema.ID,
  inputID: SessionMessage.ID,
  revision: number,
  service?: ExecutionDisclosure.Service,
) => {
  const installed = grants.get(sessionID)?.get(inputID)
  const matches =
    service === undefined ||
    (installed !== undefined && fingerprint({ ...installed, disclosure: service }) === fingerprint(installed))
  return () => {
    const renewal = renewals.get(renewalKey(sessionID, inputID))
    return (
      matches &&
      installed !== undefined &&
      installed.disclosure !== undefined &&
      installed.revision === revision &&
      grants.get(sessionID)?.get(inputID) === installed &&
      selected.get(sessionID) === inputID &&
      (!renewal || renewal.committed !== undefined)
    )
  }
}

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
    const renewal = renewals.get(renewalKey(sessionID, grant.inputID))
    if (renewal && renewal.committed === undefined) return false
    return install(sessionID, grant)
  })

function install(sessionID: SessionSchema.ID, grant: Grant) {
  if (!SessionInputRequirements.serviceSupported(grant.disclosure)) return false
  if (grant.disclosure !== undefined && grant.revision === undefined) return false
  let inputs = grants.get(sessionID)
  if (!inputs) grants.set(sessionID, (inputs = new Map()))
  const previous = inputs.get(grant.inputID)
  if (previous?.revision !== undefined) {
    if (grant.revision === undefined || grant.revision < previous.revision) return false
    if (grant.revision === previous.revision) return fingerprint(grant) === fingerprint(previous)
  }
  inputs.set(grant.inputID, structuredClone(grant))
  return true
}

// Keep pending grants and, after interruption, the last input grant for explicit
// continuation. A settled execution never keeps a live control target.
// Credentials are never stored in the durable event log.
export const retain = (sessionID: SessionSchema.ID, pending: ReadonlySet<SessionMessage.ID>, keepSelected = false) =>
  Effect.sync(() => {
    const current = selected.get(sessionID)
    selected.delete(sessionID)
    for (const [key, renewal] of renewals) {
      if (!key.startsWith(sessionID + "\0") || renewal.committed !== undefined) continue
      const inputID = key.slice(sessionID.length + 1)
      if ([...pending].some((id) => id === inputID) || (keepSelected && inputID === current)) continue
      clearTimeout(renewal.timer)
      renewals.delete(key)
    }
    const inputs = grants.get(sessionID)
    if (!inputs) return
    for (const id of inputs.keys()) if (!pending.has(id) && (!keepSelected || id !== current)) inputs.delete(id)
    if (inputs.size === 0) grants.delete(sessionID)
  })

export const drop = (sessionID: SessionSchema.ID, inputID: SessionMessage.ID) =>
  Effect.sync(() => {
    const key = renewalKey(sessionID, inputID)
    const renewal = renewals.get(key)
    if (renewal) clearTimeout(renewal.timer)
    renewals.delete(key)
    const inputs = grants.get(sessionID)
    inputs?.delete(inputID)
    if (inputs?.size === 0) grants.delete(sessionID)
    if (selected.get(sessionID) === inputID) selected.delete(sessionID)
  })
