export * as SessionPublicationWriter from "./publication-writer.js"

import { Cause, Clock, Effect, Option, Schema } from "effect"
import { eq } from "drizzle-orm"
import { Bus } from "../bus.js"
import { Database } from "../database/database.js"
import { Event } from "@opencode/schema/event"
import { AbsolutePath } from "@opencode/schema/schema"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionDisclosure } from "./disclosure.js"
import { SessionPublication } from "./publication.js"
import { SessionRunCoordinator } from "./run-coordinator.js"
import { SessionExecutionCapability } from "./execution-capability.js"
import { SessionLifecycle } from "./lifecycle.js"
import { SessionTable } from "./sql.js"

const definitions = new Map<string, Event.Definition>(
  [
    SessionEvent.Text.Started,
    SessionEvent.Text.Delta,
    SessionEvent.Text.Ended,
    SessionEvent.Reasoning.Started,
    SessionEvent.Reasoning.Delta,
    SessionEvent.Reasoning.Ended,
  ].map((definition) => [definition.type, definition]),
)
const unavailable = () => new Error("Execution publication authorization is unavailable")
class AdmissionDenied extends Error {}

export interface Unit {
  readonly id: Event.ID
  readonly digest: string
  /** Reusing this effect retains the exact unit, witness, sequence and envelope. */
  readonly publish: Effect.Effect<{ readonly inserted: boolean }>
}

// Consumes host authority, never attests a producer. The trusted caller must
// supply the actual generating context for the chosen event. This adapter is
// not installed for legacy/preflight-only inputs and does not establish coverage.
export const make = Effect.fnUntraced(function* (input: {
  readonly prepared: Effect.Success<ReturnType<typeof SessionDisclosure.prepare>>
  readonly revision: number
}) {
  const context = Schema.decodeUnknownSync(ExecutionDisclosure.ContextProof)(structuredClone(input.prepared.proof))
  const authorizer = input.prepared.publication
  const revision = input.revision
  const bus = yield* Bus.Service
  const database = yield* Database.Service
  const owner = Option.getOrUndefined(yield* Effect.serviceOption(SessionRunCoordinator.Ownership))
  if (
    !bus.persistent ||
    !owner ||
    !authorizer ||
    !context.publication ||
    context.kind !== "primary" ||
    context.publication.capabilityGeneration !== revision
  )
    return yield* Effect.die(unavailable())
  const descriptor = context.publication
  const installed = SessionExecutionCapability.captureSelection(context.sessionID, context.inputID, revision)
  const live = () => owner.live() && installed()
  if (!live()) return yield* Effect.die(unavailable())
  let nextSequence = 0

  const prepare = Effect.fnUntraced(
    function* <D extends Event.Definition>(
      definition: D,
      data: Event.Data<D>,
      options?: { readonly metadata?: Record<string, unknown> },
    ): Effect.fn.Return<Unit> {
      if (
        !live() ||
        definitions.get(definition.type) !== definition ||
        (definition.durable && definition.durable.aggregate !== "sessionID")
      )
        return yield* Effect.die(unavailable())
      // Own every alias before the first clock/database wait. The aggregate's
      // eventual database sequence is allocated only at the local commit.
      const encoded = JSON.parse(JSON.stringify(Schema.encodeUnknownSync(definition.data)(data))) as Record<
        string,
        unknown
      >
      if (encoded.sessionID !== context.sessionID) return yield* Effect.die(unavailable())
      const metadata =
        options?.metadata === undefined
          ? undefined
          : (JSON.parse(JSON.stringify(options.metadata)) as Record<string, unknown>)
      const owned = Schema.decodeUnknownSync(definition.data)(encoded)
      const sequence = nextSequence++
      if (!Number.isSafeInteger(sequence)) return yield* Effect.die(unavailable())
      const id = Event.ID.create(),
        witnessID = Event.ID.create()
      const created = yield* Clock.currentTimeMillis
      const session = yield* database.db
        .select({ directory: SessionTable.directory, workspaceID: SessionTable.workspace_id })
        .from(SessionTable)
        .where(eq(SessionTable.id, context.sessionID))
        .get()
        .pipe(Effect.orDie)
      if (!session) return yield* Effect.die(unavailable())
      yield* SessionLifecycle.check(database.db, context.sessionID).pipe(Effect.orDie)
      const location = {
        directory: AbsolutePath.make(session.directory),
        ...(session.workspaceID ? { workspaceID: session.workspaceID } : {}),
      }
      const batch = definition.durable
        ? { operation: "durable-event" as const, length: 2 as const }
        : { operation: "ephemeral-event" as const, length: 1 as const }
      const envelope = {
        id,
        created,
        type: definition.type,
        data: owned,
        location,
        ...(metadata === undefined ? {} : { metadata }),
      }
      const digest = SessionDisclosure.digest({
        version: 1,
        kind: "model-output",
        sessionID: context.sessionID,
        inputID: context.inputID,
        contextReference: context.contextReference,
        contextDigest: context.contextDigest,
        componentsDigest: context.componentsDigest,
        attempt: context.attempt,
        sequence,
        batch: { ...batch, witnessID, events: definition.durable ? [witnessID, id] : [witnessID] },
        eventVersion: definition.durable?.version ?? 1,
        event: { ...envelope, data: encoded },
      })
      let retained: SessionPublication.Permit | undefined
      const valid = (permit: SessionPublication.Permit) => live() && permit.valid()
      const check = (permit: SessionPublication.Permit) =>
        Effect.sync(() => {
          if (!valid(permit)) throw new AdmissionDenied()
        })
      const commit = (permit: SessionPublication.Permit) =>
        Effect.gen(function* () {
          const proof = permit.proof
          const witness: Event.Data<typeof SessionEvent.PublicationCommitted> = {
            sessionID: context.sessionID,
            inputID: context.inputID,
            attempt: context.attempt,
            contextReference: context.contextReference,
            contextDigest: context.contextDigest,
            componentsDigest: context.componentsDigest,
            capabilityID: proof.capabilityID,
            capabilityGeneration: proof.capabilityGeneration,
            unitID: id,
            unitDigest: digest,
            receiptReference: proof.receiptReference,
            authorizationVersion: proof.authorizationVersion,
            manifestBinding: proof.manifestBinding,
            audienceBinding: proof.audienceBinding,
            sequence,
            batch,
          }
          const firstCommit = () =>
            Effect.gen(function* () {
              yield* check(permit)
              // Writes/locks the retained row and rejects a PostgreSQL retirement.
              // This lock spans only the short native commit, never host I/O.
              yield* SessionLifecycle.activate(database.db, context.sessionID).pipe(Effect.orDie)
              const present = yield* database.db
                .select({ id: SessionTable.id })
                .from(SessionTable)
                .where(eq(SessionTable.id, context.sessionID))
                .get()
                .pipe(Effect.orDie)
              if (!present) return yield* Effect.die(new AdmissionDenied())
              yield* check(permit)
            })
          const witnessOptions = { id: witnessID, created, location, commit: firstCommit }
          const result = definition.durable
            ? yield* bus.publishAllOnce([
                [SessionEvent.PublicationCommitted, witness, witnessOptions],
                [
                  definition,
                  owned,
                  {
                    id,
                    created,
                    location,
                    ...(metadata === undefined ? {} : { metadata }),
                    commit: () => check(permit),
                  },
                ],
              ])
            : yield* bus.publishAllOnce([[SessionEvent.PublicationCommitted, witness, witnessOptions]])
          // The witness commits admission of this exact unit, not delivery.
          // Retirement ordered after that commit may let the admitted unit
          // finish; accepted local interruption still drops its raw notification.
          if (result.inserted && !definition.durable) {
            yield* bus.publishPreparedEphemeral<Event.EphemeralDefinition>(definition, envelope, () => valid(permit))
          }
          return { inserted: result.inserted }
        })
      const publish = Effect.gen(function* () {
        if (retained) {
          // A matching consumed witness exposes nothing and can acknowledge an
          // expired permit. A missing witness still runs every admission check.
          const existing = yield* commit(retained).pipe(
            Effect.catchCauseIf(
              (cause) => !Cause.hasInterrupts(cause) && Cause.squash(cause) instanceof AdmissionDenied,
              () => Effect.succeed(undefined),
            ),
          )
          if (existing) return existing
        }
        if (!live()) return yield* Effect.die(unavailable())
        const permit = yield* authorizer.authorize({ unitID: id, unitDigest: digest })
        const expected = {
          version: 1,
          purpose: "thread-publication",
          kind: "model-output",
          issuer: context.issuer,
          audience: descriptor.audience,
          organizationID: descriptor.organizationID,
          threadID: descriptor.threadID,
          sessionID: context.sessionID,
          inputID: context.inputID,
          attempt: context.attempt,
          contextReference: context.contextReference,
          contextDigest: context.contextDigest,
          componentsDigest: context.componentsDigest,
          capabilityID: descriptor.capabilityID,
          capabilityGeneration: revision,
          unitID: id,
          unitDigest: digest,
        }
        if (
          !valid(permit) ||
          Object.entries(expected).some(
            ([key, value]) => permit.proof[key as keyof ExecutionDisclosure.PublicationProof] !== value,
          )
        )
          return yield* Effect.die(unavailable())
        retained = permit
        return yield* commit(permit)
      }).pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterrupts(cause),
          () => Effect.die(unavailable()),
        ),
      )
      return Object.freeze({ id, digest, publish })
    },
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterrupts(cause),
      () => Effect.die(unavailable()),
    ),
  )
  return { prepare }
})
