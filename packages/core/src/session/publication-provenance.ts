export * as SessionPublicationProvenance from "./publication-provenance.js"

import { Effect, Option, Schema } from "effect"
import { and, eq } from "drizzle-orm"
import type { Event } from "@opencode/schema/event"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { optional } from "@opencode/schema/schema"
import { SessionMessage } from "@opencode/schema/session-message"
import type { SessionID } from "@opencode/schema/session-id"
import type { Database } from "../database/database.js"
import { SessionMessageTable } from "./sql.js"
import { SessionDisclosure } from "./disclosure.js"
import { SessionPublicationAdmission } from "./publication-admission.js"

// Derived native metadata, never a transcript field or an authorization grant.
// The host resolves each referenced immutable receipt to its source manifest.
export const Reference = ExecutionProducer.PublicationReference
export interface Reference extends Schema.Schema.Type<typeof Reference> {}
export const State = Schema.Struct({
  version: Schema.Literals([1, 2]),
  terminalControl: Schema.Struct({
    unitID: ExecutionDisclosure.PublicationProof.fields.unitID,
    unitDigest: ExecutionDisclosure.PublicationProof.fields.unitDigest,
  }).pipe(optional),
  complete: Schema.Boolean.pipe(optional),
  groups: Schema.Array(
    Schema.Struct({
      contextReference: Reference.fields.contextReference,
      manifestBinding: ExecutionDisclosure.PublicationProof.fields.manifestBinding.pipe(optional),
      complete: Schema.Boolean,
      reference: Reference,
    }),
  ).pipe(optional),
  revision: ExecutionDisclosure.PublicationProof.fields.unitDigest,
  references: Schema.Array(Reference),
}).check(Schema.makeFilter((state) => state.terminalControl !== undefined || state.references.length > 0))
export interface State extends Schema.Schema.Type<typeof State> {}

export const revision = (message: SessionMessage.Info) =>
  SessionDisclosure.digest(Schema.encodeSync(SessionMessage.Info)(message))

// Called only by the existing projector's actual message writes. The before
// row is read before mutation, under the same native event transaction.
export const next = Effect.fnUntraced(function* (
  db: Database.Interface["db"],
  sessionID: SessionID,
  event: Event.Payload,
  message: SessionMessage.Info,
) {
  if (message.type !== "assistant") return null
  const admission = Option.getOrUndefined(yield* Effect.serviceOption(SessionPublicationAdmission.Service))
  const proof = admission?.accepts(event) ? (admission.publicationFor?.(event) ?? admission.publication) : undefined
  const source =
    proof &&
    !(proof.version === 2 && proof.mode === "terminal-control") &&
    proof.kind !== "human-input" &&
    proof.sessionID === sessionID &&
    proof.unitID === event.id
      ? Schema.decodeUnknownSync(Reference)(proof)
      : undefined
  const before = yield* db
    .select()
    .from(SessionMessageTable)
    .where(and(eq(SessionMessageTable.id, message.id), eq(SessionMessageTable.session_id, sessionID)))
    .get()
    .pipe(Effect.orDie)
  const prior = before?.publication_provenance
  const terminal = proof?.version === 2 && proof.mode === "terminal-control"
  if (!source && !prior && !terminal) return null
  const old =
    before === undefined
      ? undefined
      : Schema.decodeUnknownSync(SessionMessage.Info)({ ...before.data, id: before.id, type: before.type })
  // Existing unknown or changed rows stay unknown. A later permitted write
  // cannot retroactively classify their previous content.
  if (!terminal && before !== undefined && (!prior || !old || prior.revision !== revision(old))) return null
  if (proof?.version === 2 && proof.mode === "terminal-control") {
    if (proof.kind !== "tool-control" || proof.complete) return null
    const retained =
      prior && old && prior.revision === revision(old)
        ? prior
        : { version: 2 as const, complete: false, groups: [], references: prior?.references ?? [] }
    return State.make({
      ...retained,
      revision: revision(message),
      terminalControl: { unitID: proof.unitID, unitDigest: proof.unitDigest },
    })
  }
  if (!source && old) {
    // Native terminal controls can clear retry bookkeeping. Preserve lineage
    // only when every other encoded field is exactly unchanged.
    const withoutRetry = (value: SessionMessage.Info) => ({
      ...Schema.encodeSync(SessionMessage.Info)(value),
      retry: undefined,
    })
    if (SessionDisclosure.digest(withoutRetry(old)) !== SessionDisclosure.digest(withoutRetry(message))) return null
  }
  if (proof?.version === 2 || prior?.version === 2) {
    const groups = [
      ...(prior?.groups ??
        (prior?.references ?? []).map((reference) => ({
          contextReference: reference.contextReference,
          manifestBinding: undefined,
          complete: true,
          reference,
        }))),
    ]
    if (source && proof) {
      const complete = proof.version === 1 || proof.complete
      if (
        !groups.some(
          (group) =>
            group.contextReference === source.contextReference &&
            group.manifestBinding === proof.manifestBinding &&
            group.complete === complete,
        )
      )
        groups.push({
          contextReference: source.contextReference,
          manifestBinding: proof.manifestBinding,
          complete,
          reference: source,
        })
    }
    if (groups.length === 0) return null
    return State.make({
      version: 2,
      revision: revision(message),
      complete: (prior?.complete ?? true) && groups.every((group) => group.complete),
      groups,
      references: groups.map((group) => group.reference),
    })
  }
  const references = [...(prior?.references ?? [])]
  if (source && !references.some((item) => item.contextReference === source.contextReference)) references.push(source)
  // Never truncate dependencies to make an otherwise unknown projection fit.
  if (references.length === 0 || references.length > 128) return null
  return State.make({ version: 1, revision: revision(message), references })
})
