export * as SessionPublicationWriter from "./publication-writer.js"

import { Cause, Clock, Effect, Option, Schema } from "effect"
import { and, eq, isNull } from "drizzle-orm"
import { Money } from "@opencode/schema/money"
import { Bus } from "../bus.js"
import { Database } from "../database/database.js"
import { Event } from "@opencode/schema/event"
import { AbsolutePath } from "@opencode/schema/schema"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { SessionDisclosure } from "./disclosure.js"
import { SessionPublication } from "./publication.js"
import { SessionRunCoordinator } from "./run-coordinator.js"
import { SessionExecutionCapability } from "./execution-capability.js"
import { SessionLifecycle } from "./lifecycle.js"
import {
  SessionInboxTable,
  SessionMessageTable,
  SessionTable,
  SessionTerminalControlTable,
  SessionTerminalInvocationTable,
} from "./sql.js"
import { SessionPublicationAdmission } from "./publication-admission.js"
import { NativeProducer } from "../execution-producer.js"
import { ToolEventPublication } from "../event/tool-publication.js"
import { SessionHistory } from "./history.js"
import { interruptedToolError } from "./tool-interruption.js"

const modelDefinitions = new Map<string, Event.Definition>(
  [
    SessionEvent.Step.Started,
    SessionEvent.Step.Streamed,
    SessionEvent.Step.Ended,
    SessionEvent.Step.Failed,
    SessionEvent.Text.Started,
    SessionEvent.Text.Delta,
    SessionEvent.Text.Ended,
    SessionEvent.Reasoning.Started,
    SessionEvent.Reasoning.Delta,
    SessionEvent.Reasoning.Ended,
  ].map((definition) => [definition.type, definition]),
)
const inputDefinitions = new Map<string, Event.Definition>(
  [SessionEvent.InboxEnqueued, SessionEvent.InboxDelivered].map((definition) => [definition.type, definition]),
)
const unavailable = () => new Error("Execution publication authorization is unavailable")
class AdmissionDenied extends Error {}

export interface Unit {
  readonly id: Event.ID
  readonly digest: string
  readonly created: number
  /** Reusing this effect retains the exact unit, witness, sequence and envelope. */
  readonly publish: Effect.Effect<{ readonly inserted: boolean }>
}

interface Captured {
  readonly sessionID: ExecutionDisclosure.ContextProof["sessionID"]
  readonly inputID: ExecutionDisclosure.ContextProof["inputID"]
  readonly revision: number
  readonly bus: Bus.Interface
  readonly database: Database.Interface
  readonly service: ExecutionDisclosure.Service
  readonly pending?: ReturnType<typeof NativeProducer.capturePendingTools>
  readonly live: () => boolean
}

type CaptureInput = Pick<Captured, "sessionID" | "inputID" | "revision"> & {
  readonly service: ExecutionDisclosure.Service
}

// Capture before hooks/preflight, including when an older host supplies no
// publication descriptor. A descriptor can never be silently ignored: it still
// requires the exact captured owner, installation and persistent local writer.
export const capturePreflight = Effect.fnUntraced(
  function* (input: CaptureInput) {
    const service = Schema.decodeUnknownSync(ExecutionDisclosure.Service)(structuredClone(input.service))
    const sessionID = input.sessionID,
      inputID = input.inputID,
      revision = input.revision
    const installed = SessionExecutionCapability.captureSelection(sessionID, inputID, revision, service)
    const bus = Option.getOrUndefined(yield* Effect.serviceOption(Bus.Service))
    const database = Option.getOrUndefined(yield* Effect.serviceOption(Database.Service))
    const owner = Option.getOrUndefined(yield* Effect.serviceOption(SessionRunCoordinator.Ownership))
    if (!owner || !Number.isSafeInteger(revision) || revision <= 0) return yield* Effect.die(unavailable())
    const live = () => owner.live() && installed()
    if (!live()) return yield* Effect.die(unavailable())
    const prepareRequest = Effect.fnUntraced(
      function* (request: ExecutionDisclosure.ContextRequest) {
        const pending = NativeProducer.capturePendingTools(request)
        const owned = Schema.decodeUnknownSync(ExecutionDisclosure.ContextRequest)(structuredClone(request))
        if (!live() || owned.sessionID !== sessionID || owned.inputID !== inputID || owned.kind !== "primary")
          return yield* Effect.die(unavailable())
        if (database && (yield* SessionHistory.terminalInput(database.db, sessionID, inputID)))
          return yield* Effect.die(unavailable())
        const prepared = yield* SessionDisclosure.prepareRequest(service, owned)
        if (!live()) return yield* Effect.die(unavailable())
        if (prepared.publication && (!bus?.persistent || !database)) return yield* Effect.die(unavailable())
        const writer =
          prepared.publication && bus && database
            ? yield* make({ prepared, state: { sessionID, inputID, revision, bus, database, live, service, pending } })
            : undefined
        return { proof: structuredClone(prepared.proof), signed: structuredClone(prepared.signed), writer }
      },
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterrupts(cause),
        () => Effect.die(unavailable()),
      ),
    )
    return Object.freeze({ prepareRequest, available: bus?.persistent === true && database !== undefined })
  },
  Effect.catchCauseIf(
    (cause) => !Cause.hasInterrupts(cause),
    () => Effect.die(unavailable()),
  ),
)

// Only the actual admission/promotion path consumes this capture. Input
// publication does not select an active input or borrow another turn's owner.
export const captureInput = Effect.fnUntraced(
  function* (input: CaptureInput) {
    const service = Schema.decodeUnknownSync(ExecutionDisclosure.Service)(structuredClone(input.service))
    const sessionID = input.sessionID,
      inputID = input.inputID,
      revision = input.revision
    const installed = SessionExecutionCapability.captureInstallation(sessionID, inputID, revision, service)
    const bus = yield* Bus.Service
    const database = yield* Database.Service
    const grant = yield* SessionExecutionCapability.getInstalled(sessionID, inputID)
    const session = yield* database.db
      .select({ policy: SessionTable.context_policy })
      .from(SessionTable)
      .where(eq(SessionTable.id, sessionID))
      .get()
      .pipe(Effect.orDie)
    if (
      !session?.policy ||
      (session.policy.version === 1 && grant?.mcp !== undefined && Object.keys(grant.mcp).length !== 0)
    )
      return yield* Effect.die(unavailable())
    const owner = Option.getOrUndefined(yield* Effect.serviceOption(SessionRunCoordinator.Ownership))
    const live = () => installed() && (owner === undefined || owner.live())
    if (
      service.publicationVersion !== session.policy.version ||
      !bus.persistent ||
      !live() ||
      !Number.isSafeInteger(revision) ||
      revision <= 0
    )
      return yield* Effect.die(unavailable())
    const prepareInput = Effect.fnUntraced(
      function* (payload: SessionInbox.UserPayload) {
        const owned = Schema.decodeUnknownSync(SessionInbox.UserPayload)(
          JSON.parse(JSON.stringify(Schema.encodeUnknownSync(SessionInbox.UserPayload)(payload))),
        )
        if (
          (service.compositionVersion !== 3 && service.compositionVersion !== 4 && owned.files?.length) ||
          owned.agents?.length ||
          owned.skills?.length
        )
          return yield* Effect.die(unavailable())
        const contextDigest = SessionDisclosure.digest({ kind: "native-human-input-v1", payload: owned })
        const request =
          service.compositionVersion === 2 || service.compositionVersion === 3 || service.compositionVersion === 4
            ? yield* NativeProducer.inputContext(
                sessionID,
                inputID,
                owned,
                service.compositionVersion === 3 || service.compositionVersion === 4,
                service.compositionVersion === 4,
              )
            : SessionDisclosure.makeRequest({
                sessionID,
                inputID,
                kind: "input",
                contextDigest,
                components: [{ kind: "message", id: inputID, revision: SessionDisclosure.digest(owned) }],
              })
        if (!request || request.contextDigest !== contextDigest) return yield* Effect.die(unavailable())
        if (!live()) return yield* Effect.die(unavailable())
        const prepared = yield* SessionDisclosure.prepareRequest(service, request)
        if (
          !live() ||
          !prepared.publication ||
          ((service.compositionVersion === 2 || service.compositionVersion === 3) && !prepared.proof.complete)
        )
          return yield* Effect.die(unavailable())
        return yield* make({ prepared, state: { sessionID, inputID, revision, bus, database, live, service } })
      },
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterrupts(cause),
        () => Effect.die(unavailable()),
      ),
    )
    return Object.freeze({ prepareInput })
  },
  Effect.catchCauseIf(
    (cause) => !Cause.hasInterrupts(cause),
    () => Effect.die(unavailable()),
  ),
)

// Strict writer capture preserves its original fail-before-I/O behavior.
export const capture = Effect.fnUntraced(function* (input: CaptureInput) {
  const sessionID = input.sessionID,
    inputID = input.inputID
  const captured = yield* capturePreflight(input)
  if (!captured.available) return yield* Effect.die(unavailable())
  const prepareRequest = Effect.fnUntraced(function* (request: ExecutionDisclosure.ContextRequest) {
    const prepared = yield* captured.prepareRequest(request)
    if (!prepared.writer) return yield* Effect.die(unavailable())
    return { ...prepared, writer: prepared.writer }
  })
  const prepareContext = (request: Pick<ExecutionDisclosure.ContextRequest, "contextDigest" | "components">) =>
    Effect.suspend(() =>
      prepareRequest(
        SessionDisclosure.makeRequest({
          ...request,
          sessionID,
          inputID,
          kind: "primary",
        }),
      ),
    )
  return Object.freeze({ prepareContext, prepareRequest })
})

// Consumes host authority, never attests a producer. The actual generating
// request must use this capture-owned preflight. Legacy inputs do not install it.
const make = Effect.fnUntraced(function* (input: {
  readonly prepared: Effect.Success<ReturnType<typeof SessionDisclosure.prepare>>
  readonly state: Captured
}) {
  const context = Schema.decodeUnknownSync(ExecutionDisclosure.ContextProof)(structuredClone(input.prepared.proof))
  const authorizer = input.prepared.publication
  const revision = input.state.revision
  const bus = input.state.bus
  const database = input.state.database
  const live = input.state.live
  if (
    !live() ||
    !authorizer ||
    !context.publication ||
    (context.kind !== "primary" && context.kind !== "input") ||
    context.sessionID !== input.state.sessionID ||
    context.inputID !== input.state.inputID ||
    context.publication.capabilityGeneration !== revision
  )
    return yield* Effect.die(unavailable())
  const descriptor = context.publication
  const kind = context.kind === "input" ? "human-input" : "model-output"
  const definitions = context.kind === "input" ? inputDefinitions : modelDefinitions
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
      if (context.kind === "input") {
        if (encoded.inboxID !== context.inputID) return yield* Effect.die(unavailable())
        if (definition.type === SessionEvent.InboxEnqueued.type) {
          const item = Schema.decodeUnknownSync(SessionInbox.Item)(encoded.item)
          if (
            item.type !== "user" ||
            SessionDisclosure.digest({ kind: "native-human-input-v1", payload: item.payload }) !== context.contextDigest
          )
            return yield* Effect.die(unavailable())
        }
      }
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
        version: descriptor.version,
        kind,
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
      const envelopeDigest = SessionDisclosure.digest({ ...envelope, data: encoded })
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
              if (context.kind === "input" && definition.type === SessionEvent.InboxDelivered.type) {
                const pending = yield* database.db
                  .select()
                  .from(SessionInboxTable)
                  .where(eq(SessionInboxTable.id, context.inputID))
                  .get()
                  .pipe(Effect.orDie)
                if (
                  !pending ||
                  pending.session_id !== context.sessionID ||
                  pending.type !== "user" ||
                  SessionDisclosure.digest({
                    kind: "native-human-input-v1",
                    payload: Schema.decodeUnknownSync(SessionInbox.UserPayload)(pending.payload),
                  }) !== context.contextDigest
                )
                  return yield* Effect.die(new AdmissionDenied())
              }
              yield* check(permit)
            })
          const witnessOptions = { id: witnessID, created, location, commit: firstCommit }
          const result = definition.durable
            ? yield* bus
                .publishAllOnce([
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
                .pipe(
                  Effect.provideService(SessionPublicationAdmission.Service, {
                    publication: structuredClone(proof),
                    accepts: (event) =>
                      valid(permit) &&
                      event.id === id &&
                      event.type === definition.type &&
                      SessionDisclosure.digest({
                        id: event.id,
                        created: event.created,
                        type: event.type,
                        data: Schema.encodeUnknownSync(definition.data)(event.data),
                        location: event.location,
                        ...(event.metadata === undefined ? {} : { metadata: event.metadata }),
                      }) === envelopeDigest,
                  }),
                )
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
          version: descriptor.version,
          purpose: "thread-publication",
          kind,
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
      return Object.freeze({ id, digest, created, publish })
    },
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterrupts(cause),
      () => Effect.die(unavailable()),
    ),
  )
  const control =
    descriptor.version === 2 && context.kind === "primary"
      ? NativeProducer.captureToolControl(input.state.service, input.prepared.signed)
      : undefined
  let terminalObserved = false
  const valid = (permit: SessionPublication.Permit) => live() && permit.valid()
  const ordinaryValid = (permit: SessionPublication.Permit) => !terminalObserved && valid(permit)
  const check = (permit: SessionPublication.Permit) =>
    Effect.sync(() => {
      if (!valid(permit)) throw new AdmissionDenied()
    })
  // The same retained lifecycle row serializes ordinary commits against
  // observation of a verified terminal outcome across native SQL connections.
  // This runs again inside the canonical Bus publication transaction.
  const ordinaryCheck = (permit: SessionPublication.Permit) =>
    Effect.gen(function* () {
      if (!ordinaryValid(permit)) return yield* Effect.die(unavailable())
      yield* SessionLifecycle.activate(database.db, context.sessionID).pipe(Effect.orDie)
      const stopped = yield* database.db
        .select({ inputID: SessionTerminalControlTable.input_id })
        .from(SessionTerminalControlTable)
        .where(
          and(
            eq(SessionTerminalControlTable.session_id, context.sessionID),
            eq(SessionTerminalControlTable.input_id, context.inputID),
          ),
        )
        .get()
        .pipe(Effect.orDie)
      if (stopped) return yield* Effect.die(unavailable())
      if (!ordinaryValid(permit)) return yield* Effect.die(unavailable())
    })
  const witness = Effect.fnUntraced(function* (
    unit: {
      id: Event.ID
      digest: string
      witnessID: Event.ID
      created: number
      sequence: number
      operation: "tool-event" | "tool-spill"
    },
    permit: SessionPublication.Permit,
  ) {
    yield* ordinaryCheck(permit)
    const proof = permit.proof
    const session = yield* database.db
      .select({ directory: SessionTable.directory, workspaceID: SessionTable.workspace_id })
      .from(SessionTable)
      .where(eq(SessionTable.id, context.sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!session) return yield* Effect.die(unavailable())
    return yield* ToolEventPublication.without(
      bus.publishAllOnce([
        [
          SessionEvent.PublicationCommitted,
          {
            sessionID: context.sessionID,
            inputID: context.inputID,
            attempt: context.attempt,
            contextReference: context.contextReference,
            contextDigest: context.contextDigest,
            componentsDigest: context.componentsDigest,
            capabilityID: proof.capabilityID,
            capabilityGeneration: proof.capabilityGeneration,
            unitID: unit.id,
            unitDigest: unit.digest,
            receiptReference: proof.receiptReference,
            authorizationVersion: proof.authorizationVersion,
            manifestBinding: proof.manifestBinding,
            audienceBinding: proof.audienceBinding,
            sequence: unit.sequence,
            batch: { operation: unit.operation, length: 1 },
          },
          {
            id: unit.witnessID,
            created: unit.created,
            location: {
              directory: AbsolutePath.make(session.directory),
              ...(session.workspaceID ? { workspaceID: session.workspaceID } : {}),
            },
            commit: () => ordinaryCheck(permit),
          },
        ],
      ]),
    )
  })
  const authorize = Effect.fnUntraced(function* (unit: {
    id: Event.ID
    digest: string
    witnessID: Event.ID
    created: number
    sequence: number
    operation: "tool-event" | "tool-spill"
    kind: "tool-output" | "tool-control" | "tool-spill"
    projection?: ExecutionDisclosure.ToolPublicationRequest["projection"]
  }) {
    if (!live() || !control || descriptor.version !== 2) return yield* Effect.die(unavailable())
    const permit = yield* ToolEventPublication.without(
      authorizer.authorize({
        unitID: unit.id,
        unitDigest: unit.digest,
        kind: unit.kind,
        ...(unit.projection === undefined ? {} : { projection: unit.projection }),
      }),
    )
    const expected = {
      version: 2,
      purpose: "thread-publication",
      kind: unit.kind,
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
      unitID: unit.id,
      unitDigest: unit.digest,
    }
    if (
      !valid(permit) ||
      (permit.proof.version === 2 && permit.proof.mode === "terminal-control") ||
      Object.entries(expected).some(([key, value]) => permit.proof[key as keyof typeof permit.proof] !== value)
    )
      return yield* Effect.die(unavailable())
    yield* witness(unit, permit)
    return permit
  })
  const authorizeEvent = Effect.fnUntraced(function* (
    definition: Event.Definition,
    event: Event.Payload,
    source?: object,
  ): Effect.fn.Return<ToolEventPublication.Admission & { readonly digest: string; readonly valid: () => boolean }> {
    if (!control || !live() || terminalObserved) return yield* Effect.die(unavailable())
    const encoded = Schema.encodeUnknownSync(definition.data)(event.data) as Record<string, unknown>
    if (
      (typeof encoded.sessionID === "string" && encoded.sessionID !== context.sessionID) ||
      (typeof encoded.form === "object" &&
        encoded.form !== null &&
        "sessionID" in encoded.form &&
        encoded.form.sessionID !== context.sessionID)
    )
      return yield* Effect.die(unavailable())
    const envelope = { ...event, data: encoded }
    const envelopeDigest = SessionDisclosure.digest(envelope)
    const sequence = nextSequence++,
      witnessID = Event.ID.create()
    if (!Number.isSafeInteger(sequence)) return yield* Effect.die(unavailable())
    const kind = definition === SessionEvent.Tool.Success ? ("tool-output" as const) : ("tool-control" as const)
    const digest = SessionDisclosure.digest({
      version: 2,
      kind,
      sessionID: context.sessionID,
      inputID: context.inputID,
      contextReference: context.contextReference,
      contextDigest: context.contextDigest,
      componentsDigest: context.componentsDigest,
      attempt: context.attempt,
      sequence,
      batch: { operation: "tool-event", length: 1, witnessID, events: [event.id] },
      eventVersion: definition.durable?.version ?? 1,
      event: envelope,
    })
    const projection =
      (source ? NativeProducer.projectToolUnit(digest, source) : undefined) ??
      control.project(definition, encoded, digest) ??
      (yield* NativeProducer.projectCurrentToolUnit(digest)) ??
      input.state.pending?.project(definition, encoded, digest) ??
      control.unknown(definition, encoded, digest)
    const permit = yield* authorize({
      id: event.id,
      digest,
      witnessID,
      created: event.created,
      sequence,
      operation: "tool-event",
      kind,
      projection,
    })
    const accepts = (actual: Event.Payload) =>
      ordinaryValid(permit) &&
      actual.id === event.id &&
      actual.type === definition.type &&
      SessionDisclosure.digest({
        id: actual.id,
        created: actual.created,
        type: actual.type,
        data: Schema.encodeUnknownSync(definition.data)(actual.data),
        location: actual.location,
        ...(actual.metadata === undefined ? {} : { metadata: actual.metadata }),
      }) === envelopeDigest
    return {
      digest,
      valid: () => ordinaryValid(permit),
      check: ordinaryCheck(permit),
      provide: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        Effect.gen(function* () {
          const previous = Option.getOrUndefined(yield* Effect.serviceOption(SessionPublicationAdmission.Service))
          return yield* effect.pipe(
            Effect.provideService(SessionPublicationAdmission.Service, {
              accepts: (actual) => accepts(actual) || previous?.accepts(actual) === true,
              publicationFor: (actual) =>
                accepts(actual) ? permit.proof : (previous?.publicationFor?.(actual) ?? previous?.publication),
            }),
          )
        }),
    }
  })
  const prepareTool = Effect.fnUntraced(function* <D extends Event.Definition>(
    definition: D,
    data: Event.Data<D>,
    source?: object,
  ): Effect.fn.Return<Unit> {
    // No caller aliases survive a clock or host wait.
    const owned = Schema.decodeUnknownSync(definition.data)(
      JSON.parse(JSON.stringify(Schema.encodeUnknownSync(definition.data)(data))),
    )
    const id = Event.ID.create(),
      created = yield* Clock.currentTimeMillis
    const session = yield* database.db
      .select({ directory: SessionTable.directory, workspaceID: SessionTable.workspace_id })
      .from(SessionTable)
      .where(eq(SessionTable.id, context.sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!session) return yield* Effect.die(unavailable())
    const event = {
      id,
      created,
      type: definition.type,
      data: owned,
      location: {
        directory: AbsolutePath.make(session.directory),
        ...(session.workspaceID ? { workspaceID: session.workspaceID } : {}),
      },
    }
    const admission = yield* ToolEventPublication.without(authorizeEvent(definition, event, source))
    const publish = admission.provide(
      ToolEventPublication.without(
        Effect.gen(function* () {
          yield* admission.check
          if (definition.durable)
            return yield* bus.publishAllOnce([
              [definition, owned, { id, created, location: event.location, commit: () => admission.check }],
            ])
          yield* bus.publishPreparedEphemeral(definition, event, admission.valid)
          return { inserted: true }
        }),
      ),
    )
    return Object.freeze({ id, digest: admission.digest, created, publish })
  })
  const invocationKey = (fact: NonNullable<ReturnType<typeof NativeProducer.invocationObservation>>) =>
    and(
      eq(SessionTerminalInvocationTable.session_id, fact.sessionID),
      eq(SessionTerminalInvocationTable.input_id, fact.inputID),
      eq(SessionTerminalInvocationTable.invocation_id, fact.invocationID),
    )
  const prepareInvocation = Effect.fnUntraced(function* (source: object) {
    const fact = NativeProducer.invocationObservation(source)
    if (
      !control ||
      !live() ||
      !fact ||
      fact.sessionID !== context.sessionID ||
      fact.inputID !== context.inputID ||
      fact.attempt !== context.attempt ||
      fact.contextReference !== context.contextReference ||
      Buffer.byteLength(fact.evidence) > 8192
    )
      return yield* Effect.die(unavailable())
    yield* database.db
      .transaction(() =>
        Effect.gen(function* () {
          yield* SessionLifecycle.activate(database.db, context.sessionID).pipe(Effect.orDie)
          yield* database.db
            .insert(SessionTerminalInvocationTable)
            .values({
              session_id: fact.sessionID,
              input_id: fact.inputID,
              invocation_id: fact.invocationID,
              context_reference: fact.contextReference,
              evidence: fact.evidence,
              created: Date.now(),
            })
            .onConflictDoNothing()
            .run()
            .pipe(Effect.orDie)
          const retained = yield* database.db
            .select()
            .from(SessionTerminalInvocationTable)
            .where(invocationKey(fact))
            .get()
            .pipe(Effect.orDie)
          if (retained?.evidence !== fact.evidence) return yield* Effect.die(unavailable())
        }),
      )
      .pipe(Effect.orDie)
  }, Effect.uninterruptible)
  const completeInvocation = Effect.fnUntraced(function* (source: object) {
    const fact = NativeProducer.invocationObservation(source)
    if (
      !control ||
      !fact ||
      fact.sessionID !== context.sessionID ||
      fact.inputID !== context.inputID ||
      fact.attempt !== context.attempt ||
      fact.contextReference !== context.contextReference
    )
      return yield* Effect.die(unavailable())
    // Only the actual SDK verifier calls this after its exact ordinary result.
    yield* database.db
      .delete(SessionTerminalInvocationTable)
      .where(and(invocationKey(fact), eq(SessionTerminalInvocationTable.evidence, fact.evidence)))
      .run()
      .pipe(Effect.orDie)
  }, Effect.uninterruptible)
  // Retain a deny-only execution barrier as soon as the signed result arrives,
  // before hooks unwind or pub2 starts. Cancellation cannot erase an uncertain
  // committed host mutation. This private row never authorizes publication.
  const observeTerminal = Effect.fnUntraced(function* (source: object) {
    const fact = NativeProducer.terminalObservation(source)
    if (
      !control ||
      !fact ||
      fact.sessionID !== context.sessionID ||
      fact.inputID !== context.inputID ||
      fact.attempt !== context.attempt ||
      fact.contextReference !== context.contextReference ||
      Buffer.byteLength(fact.evidence) > 32768
    )
      return yield* Effect.die(unavailable())
    terminalObserved = true
    const key = and(
      eq(SessionTerminalControlTable.session_id, context.sessionID),
      eq(SessionTerminalControlTable.input_id, context.inputID),
    )
    yield* database.db
      .transaction(() =>
        Effect.gen(function* () {
          yield* SessionLifecycle.activate(database.db, context.sessionID).pipe(Effect.orDie)
          const value = {
            session_id: fact.sessionID,
            input_id: fact.inputID,
            attempt: fact.attempt,
            message_id: fact.messageID,
            tool_call_id: fact.toolCallID,
            producer_epoch: fact.producerEpoch,
            producer_key_id: fact.producerKeyID,
            context_reference: fact.contextReference,
            receipt_reference: fact.receiptReference,
            evidence: fact.evidence,
          }
          yield* database.db
            .insert(SessionTerminalControlTable)
            .values({ ...value, created: Date.now() })
            .onConflictDoNothing()
            .run()
            .pipe(Effect.orDie)
          const retained = yield* database.db
            .select()
            .from(SessionTerminalControlTable)
            .where(key)
            .get()
            .pipe(Effect.orDie)
          if (
            !retained ||
            Object.entries(value).some(([name, value]) => retained[name as keyof typeof retained] !== value)
          )
            return yield* Effect.die(unavailable())
        }),
      )
      .pipe(Effect.orDie)
  }, Effect.uninterruptible)
  // This bounded UI control unit is the only publication path for a verified
  // terminal outcome. Its witness and every visible event share one commit.
  const prepareTerminal = Effect.fnUntraced(function* (terminal: {
    readonly id: string
    readonly assistantMessageID: ExecutionDisclosure.ContextProof["inputID"]
    readonly source: object
    readonly pending: readonly { readonly id: string; readonly executed: boolean }[]
  }): Effect.fn.Return<Unit> {
    if (
      !control ||
      !live() ||
      !NativeProducer.isTerminalToolResult(terminal.source) ||
      terminal.pending.length > 128 ||
      new Set(terminal.pending.map((item) => item.id)).size !== terminal.pending.length ||
      terminal.pending.some((item) => item.id === terminal.id)
    )
      return yield* Effect.die(unavailable())
    yield* observeTerminal(terminal.source)
    const row = yield* database.db
      .select()
      .from(SessionMessageTable)
      .where(
        and(
          eq(SessionMessageTable.session_id, context.sessionID),
          eq(SessionMessageTable.id, terminal.assistantMessageID),
        ),
      )
      .get()
      .pipe(Effect.orDie)
    if (!row) return yield* Effect.die(unavailable())
    const assistant = yield* SessionHistory.decodeMessageRow(row).pipe(Effect.orDie)
    if (assistant.type !== "assistant") return yield* Effect.die(unavailable())
    const actualTools = assistant.content.filter((item) => item.type === "tool")
    const completed = actualTools.find((item) => item.id === terminal.id)
    if (!completed || (completed.state.status !== "running" && completed.state.status !== "streaming"))
      return yield* Effect.die(unavailable())
    const pending = actualTools.filter(
      (item) => item.id !== terminal.id && (item.state.status === "running" || item.state.status === "streaming"),
    )
    if (pending.length > 128) return yield* Effect.die(unavailable())
    const data = [
      {
        definition: SessionEvent.Tool.Success,
        data: {
          sessionID: context.sessionID,
          assistantMessageID: terminal.assistantMessageID,
          id: terminal.id,
          content: [{ type: "text", text: "Operation completed." }],
          executed: false,
        },
      },
      ...pending.map((item) => ({
        definition: SessionEvent.Tool.Failed,
        data: {
          sessionID: context.sessionID,
          assistantMessageID: terminal.assistantMessageID,
          id: item.id,
          error: interruptedToolError,
          executed: item.executed === true,
        },
      })),
      {
        definition: SessionEvent.Step.Ended,
        data: {
          sessionID: context.sessionID,
          assistantMessageID: terminal.assistantMessageID,
          finish: "stop",
          cost: Money.USD.make(0),
          tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        },
      },
    ].map((item) => ({
      definition: item.definition,
      data: Schema.decodeUnknownSync(item.definition.data)(
        JSON.parse(JSON.stringify(Schema.encodeUnknownSync(item.definition.data)(item.data))),
      ),
      id: Event.ID.create(),
    }))
    const id = data[0].id,
      witnessID = Event.ID.create(),
      created = yield* Clock.currentTimeMillis,
      sequence = nextSequence++
    if (!Number.isSafeInteger(sequence)) return yield* Effect.die(unavailable())
    const session = yield* database.db
      .select({ directory: SessionTable.directory, workspaceID: SessionTable.workspace_id })
      .from(SessionTable)
      .where(eq(SessionTable.id, context.sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!session) return yield* Effect.die(unavailable())
    const location = {
      directory: AbsolutePath.make(session.directory),
      ...(session.workspaceID ? { workspaceID: session.workspaceID } : {}),
    }
    const events = data.map((item) => ({
      id: item.id,
      created,
      type: item.definition.type,
      data: Schema.encodeUnknownSync(item.definition.data)(item.data),
      location,
    }))
    const batch = { operation: "terminal-control" as const, length: events.length + 1 }
    const digest = SessionDisclosure.digest({
      version: 2,
      kind: "tool-control",
      sessionID: context.sessionID,
      inputID: context.inputID,
      contextReference: context.contextReference,
      contextDigest: context.contextDigest,
      componentsDigest: context.componentsDigest,
      attempt: context.attempt,
      sequence,
      batch: { ...batch, witnessID, events: [witnessID, ...events.map((event) => event.id)] },
      events,
    })
    const key = and(
      eq(SessionTerminalControlTable.session_id, context.sessionID),
      eq(SessionTerminalControlTable.input_id, context.inputID),
    )
    yield* database.db
      .update(SessionTerminalControlTable)
      .set({ unit_id: id, unit_digest: digest })
      .where(and(key, isNull(SessionTerminalControlTable.unit_id)))
      .run()
      .pipe(Effect.orDie)
    const retainedUnit = yield* database.db
      .select()
      .from(SessionTerminalControlTable)
      .where(key)
      .get()
      .pipe(Effect.orDie)
    if (retainedUnit?.unit_id !== id || retainedUnit.unit_digest !== digest) return yield* Effect.die(unavailable())
    const projection = NativeProducer.projectTerminalUnit(digest, terminal.source)
    if (!projection) return yield* Effect.die(unavailable())
    const identity = Schema.decodeUnknownSync(Schema.fromJsonString(ExecutionProducer.ToolResultProjection))(
      projection.payload,
    )
    if (
      identity.sessionID !== context.sessionID ||
      identity.inputID !== context.inputID ||
      identity.attempt !== context.attempt ||
      identity.messageID !== terminal.assistantMessageID ||
      identity.toolCallID !== terminal.id
    )
      return yield* Effect.die(unavailable())
    // Do not retain caller-owned arrays or result aliases across authorization.
    const envelopes = new Map(events.map((event) => [event.id, SessionDisclosure.digest(event)]))
    let retained: SessionPublication.Permit | undefined
    const publish = Effect.gen(function* () {
      const permit =
        retained ??
        (yield* ToolEventPublication.without(
          authorizer.authorize({
            unitID: id,
            unitDigest: digest,
            kind: "tool-control",
            projection,
          }),
        ))
      const expected = {
        version: 2,
        purpose: "thread-publication",
        kind: "tool-control",
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
        mode: "terminal-control",
        complete: false,
      }
      if (Object.entries(expected).some(([key, value]) => permit.proof[key as keyof typeof permit.proof] !== value))
        return yield* Effect.die(unavailable())
      retained = permit
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
      const admission = {
        publication: proof,
        accepts: (event: Event.Payload) =>
          valid(permit) &&
          envelopes.get(event.id) ===
            SessionDisclosure.digest({
              id: event.id,
              created: event.created,
              type: event.type,
              data: event.data,
              location: event.location,
              ...(event.metadata === undefined ? {} : { metadata: event.metadata }),
            }),
      }
      return yield* ToolEventPublication.without(
        bus.publishAllOnce([
          [
            SessionEvent.PublicationCommitted,
            witness,
            {
              id: witnessID,
              created,
              location,
              commit: () =>
                check(permit).pipe(
                  Effect.andThen(SessionLifecycle.activate(database.db, context.sessionID).pipe(Effect.orDie)),
                  Effect.andThen(check(permit)),
                ),
            },
          ],
          ...data.map(
            (item) =>
              [item.definition, item.data, { id: item.id, created, location, commit: () => check(permit) }] as const,
          ),
        ]),
      ).pipe(Effect.provideService(SessionPublicationAdmission.Service, admission))
    })
    return Object.freeze({ id, digest, created, publish })
  })
  const prepareSpill = Effect.fnUntraced(function* (
    spill: { readonly path: string; readonly content: string; readonly projection: object },
    source: object,
  ) {
    const owned = structuredClone(spill)
    const id = Event.ID.create(),
      witnessID = Event.ID.create(),
      created = yield* Clock.currentTimeMillis,
      sequence = nextSequence++
    if (!Number.isSafeInteger(sequence)) return yield* Effect.die(unavailable())
    const digest = SessionDisclosure.digest({
      version: 2,
      kind: "tool-spill",
      sessionID: context.sessionID,
      inputID: context.inputID,
      contextReference: context.contextReference,
      contextDigest: context.contextDigest,
      componentsDigest: context.componentsDigest,
      attempt: context.attempt,
      sequence,
      spill: owned,
    })
    const projection = NativeProducer.projectToolUnit(digest, source)
    const permit = yield* authorize({
      id,
      digest,
      witnessID,
      created,
      sequence,
      operation: "tool-spill",
      kind: "tool-spill",
      projection,
    })
    return { check: check(permit) }
  })
  const interruptions = new Map<string, Unit>()
  const settleInterrupted = Effect.fnUntraced(function* () {
    for (const target of input.state.pending?.targets ?? []) {
      const key = target.messageID + "\0" + target.toolCallID
      let unit = interruptions.get(key)
      if (!unit) {
        unit = yield* prepareTool(SessionEvent.Tool.Failed, {
          sessionID: context.sessionID,
          assistantMessageID: target.messageID,
          id: target.toolCallID,
          executed: target.executed,
          error: interruptedToolError,
        })
        interruptions.set(key, unit)
      }
      yield* unit.publish
    }
  })
  const assertExecution = Effect.gen(function* () {
    if (!live() || (yield* SessionHistory.terminalInput(database.db, context.sessionID, context.inputID)))
      return yield* Effect.die(unavailable())
  })
  return Object.freeze({
    prepare,
    ...(control
      ? {
          assertExecution,
          terminalSettlement: {
            deadline: context.expiresAt,
            pending: database.db
              .select({ id: SessionTerminalInvocationTable.invocation_id })
              .from(SessionTerminalInvocationTable)
              .where(
                and(
                  eq(SessionTerminalInvocationTable.session_id, context.sessionID),
                  eq(SessionTerminalInvocationTable.input_id, context.inputID),
                ),
              )
              .limit(1)
              .get()
              .pipe(
                Effect.orDie,
                Effect.map((row) => row !== undefined),
              ),
          },
          prepareTool,
          prepareTerminal,
          observeTerminal,
          prepareInvocation,
          completeInvocation,
          prepareSpill,
          settleInterrupted,
          authorizeEvent,
          toolScope: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
            ToolEventPublication.captured(effect, { authorize: authorizeEvent }),
        }
      : {}),
  })
})

export type Writer = Effect.Success<ReturnType<typeof make>>
