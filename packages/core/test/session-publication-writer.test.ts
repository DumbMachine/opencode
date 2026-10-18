import { describe, expect, test } from "bun:test"
import { Context, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect"
import { eq, sql } from "drizzle-orm"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { SessionPublicationWriter } from "@opencode/core/session/publication-writer"
import { SessionPublicationProvenance } from "@opencode/core/session/publication-provenance"
import { toLLMMessages } from "@opencode/core/session/runner/to-llm-message"
import { SessionExecutionCapability } from "@opencode/core/session/execution-capability"
import { SessionRunCoordinator } from "@opencode/core/session/run-coordinator"
import { SessionLifecycle } from "@opencode/core/session/lifecycle"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionStore } from "@opencode/core/session/store"
import { SessionInbox } from "@opencode/core/session/inbox"
import { SessionTable, SessionMessageTable, SessionLifecycleTable } from "@opencode/core/session/sql"
import { EventTable, EventSequenceTable } from "@opencode/core/event/sql"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { SessionEvent } from "@opencode/schema/session-event"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { Project } from "@opencode/schema/project"
import { AbsolutePath } from "@opencode/schema/schema"
import { ProjectTable } from "@opencode/core/project/sql"
import { testEffect } from "./lib/effect"
import { Global } from "@opencode/util/global"
import { tmpdir } from "./fixture/tmpdir"
import { publicationHost } from "./fixture/publication-host"

const graph = LayerNode.group([Database.node, Bus.node, SessionProjector.node, SessionStore.node, SessionInbox.node])
const it = testEffect(AppNodeBuilder.build(graph, [Bus.node.replace(Bus.configured({ persist: true }))]))
const withoutPersistence = testEffect(AppNodeBuilder.build(graph))
const fixture = Effect.fnUntraced(function* (options?: { readonly deferAssistant?: boolean }) {
  const database = yield* Database.Service
  const bus = yield* Bus.Service
  const sessionID = Session.ID.create(),
    inputID = SessionMessage.ID.create(),
    assistantMessageID = SessionMessage.ID.create()
  yield* database.db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
  yield* database.db
    .insert(SessionTable)
    .values({ id: sessionID, project_id: Project.ID.global, slug: sessionID, directory: "/project", version: "test" })
    .run()
    .pipe(Effect.orDie)
  if (!options?.deferAssistant) {
    yield* bus.publish(SessionEvent.Step.Started, {
      sessionID,
      assistantMessageID,
      agent: Agent.ID.make("build"),
      model: { id: Model.ID.make("fixture"), providerID: Provider.ID.make("fixture") },
      started: 1,
    })
    yield* bus.publish(SessionEvent.Text.Started, { sessionID, assistantMessageID, ordinal: 0 })
  }
  const state: {
    active: boolean
    calls: Array<{ unitID: string; unitDigest: string }>
    contexts: Array<ExecutionDisclosure.ContextRequest>
    ttl: number
    contextReference: string
    deadlines: Map<number, number>
    expired: Set<number>
    request?: () => Effect.Effect<void>
    preflight?: () => Effect.Effect<void>
    mutate?: (proof: ExecutionDisclosure.PublicationProof) => ExecutionDisclosure.PublicationProof
  } = {
    active: true,
    calls: [],
    contexts: [],
    ttl: 30_000,
    contextReference: "1".repeat(64),
    deadlines: new Map(),
    expired: new Set(),
  }
  const host = publicationHost({
    context: (proof) => ({ ...proof, contextReference: state.contextReference }),
    ttl: () => state.ttl,
    observe: async (unit, authorization) => {
      expect(authorization).toBe("Bearer private-original-token")
      state.calls.push({ unitID: unit.unitID, unitDigest: unit.unitDigest })
      await Effect.runPromise(state.request?.() ?? Effect.void)
    },
    observeContext: async (context, authorization) => {
      expect(authorization).toBe("Bearer private-original-token")
      state.contexts.push(context)
      await Effect.runPromise(state.preflight?.() ?? Effect.void)
    },
    mutate: (proof) => {
      const index = state.calls.length
      state.deadlines.set(index, proof.expiresAt)
      if (state.expired.has(index)) return { ...proof, expiresAt: Date.now() - 1 }
      return state.mutate?.(proof) ?? proof
    },
  })
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      host.server.stop(true)
    }),
  )
  const service = host.config
  const captureInput = { sessionID, inputID, revision: 7, service }
  const request = { contextDigest: "c".repeat(64), components: [] }
  yield* SessionExecutionCapability.set(sessionID, { inputID, disclosure: service, mcp: {}, revision: 7 })
  yield* SessionExecutionCapability.select(sessionID, inputID)
  yield* Effect.addFinalizer(() => SessionExecutionCapability.clear(sessionID))
  const capture = () =>
    SessionPublicationWriter.capture(captureInput).pipe(
      Effect.provideService(SessionRunCoordinator.Ownership, { live: () => state.active }),
      Effect.provideService(Bus.Service, bus),
      Effect.provideService(Database.Service, database),
    )
  const prepare = () =>
    Effect.gen(function* () {
      const selected = yield* capture()
      return yield* selected.prepareContext(request)
    })
  const make = () => prepare().pipe(Effect.map((context) => context.writer))
  const expire = (index: number) =>
    Effect.promise(() => Bun.sleep(Math.max(0, state.deadlines.get(index)! - Date.now() + 25)))
  const text = (value = "Original answer") => ({ sessionID, assistantMessageID, ordinal: 0, text: value })
  const delta = () => ({ sessionID, assistantMessageID, ordinal: 0, delta: "same" })
  const witnesses = () =>
    database.db
      .select()
      .from(EventTable)
      .where(eq(EventTable.aggregate_id, sessionID))
      .all()
      .pipe(
        Effect.orDie,
        Effect.map((rows) =>
          rows.filter((row) => row.type === Bus.versionedType(SessionEvent.PublicationCommitted.type, 1)),
        ),
      )
  const message = () =>
    database.db
      .select()
      .from(SessionMessageTable)
      .where(eq(SessionMessageTable.id, assistantMessageID))
      .get()
      .pipe(Effect.orDie)
  return {
    database,
    bus,
    sessionID,
    inputID,
    assistantMessageID,
    captureInput,
    request,
    capture,
    prepare,
    expire,
    service,
    state,
    make,
    text,
    delta,
    witnesses,
    message,
  }
})

function* verifyProjectionProvenance(f: Effect.Success<ReturnType<typeof fixture>>) {
  const writer = yield* f.make()
  const model = { id: Model.ID.make("fixture"), providerID: Provider.ID.make("fixture") }
  const started = yield* writer.prepare(SessionEvent.Step.Started, {
    sessionID: f.sessionID,
    assistantMessageID: f.assistantMessageID,
    agent: Agent.ID.make("build"),
    model,
    started: 1,
  })
  yield* started.publish
  const reasoning = yield* writer.prepare(SessionEvent.Reasoning.Started, {
    sessionID: f.sessionID,
    assistantMessageID: f.assistantMessageID,
    ordinal: 0,
    state: { signature: "opaque-reasoning-state" },
  })
  yield* reasoning.publish
  const first = yield* f.message()
  if (!first) throw new Error("Missing projected assistant")
  const decoded = Schema.decodeUnknownSync(SessionMessage.Info)({ ...first.data, id: first.id, type: first.type })
  expect(first.publication_provenance?.revision).toBe(SessionPublicationProvenance.revision(decoded))
  expect(first.publication_provenance?.references).toHaveLength(1)
  expect(first.publication_provenance?.references[0]?.unitID).toBe(started.id)
  expect(toLLMMessages([decoded], model, "fixture")[0]?.content).toMatchObject([
    { type: "reasoning", text: "", providerMetadata: { fixture: { signature: "opaque-reasoning-state" } } },
  ])
  // Retry status is native bookkeeping; terminal clearing must still update
  // the exact stored revision while retaining the same source dependencies.
  yield* f.bus.publish(SessionEvent.RetryScheduled, {
    sessionID: f.sessionID,
    assistantMessageID: f.assistantMessageID,
    attempt: 1,
    at: 2,
    error: { type: "authorization", message: "Synthetic retry" },
  })
  expect((yield* f.message())?.data).toHaveProperty("retry")
  yield* f.bus.publish(SessionEvent.Execution.Interrupted, { sessionID: f.sessionID, reason: "shutdown" })
  const cleared = yield* f.message()
  if (!cleared) throw new Error("Missing cleared assistant")
  expect(cleared.data).not.toHaveProperty("retry")
  expect(cleared.publication_provenance?.revision).toBe(
    SessionPublicationProvenance.revision(
      Schema.decodeUnknownSync(SessionMessage.Info)({ ...cleared.data, id: cleared.id, type: cleared.type }),
    ),
  )
  // A new step also mutates the previous assistant through the real updater.
  f.state.contextReference = "2".repeat(64)
  const next = yield* f.make()
  const assistantMessageID = SessionMessage.ID.create()
  const secondStart = yield* next.prepare(SessionEvent.Step.Started, {
    sessionID: f.sessionID,
    assistantMessageID,
    agent: Agent.ID.make("build"),
    model,
    started: 3,
  })
  yield* secondStart.publish
  const completed = yield* f.message()
  if (!completed) throw new Error("Missing completed previous assistant")
  expect(completed.publication_provenance?.revision).toBe(
    SessionPublicationProvenance.revision(
      Schema.decodeUnknownSync(SessionMessage.Info)({ ...completed.data, id: completed.id, type: completed.type }),
    ),
  )
  expect(completed.publication_provenance?.references.map((item) => item.contextReference)).toEqual([
    "1".repeat(64),
    "2".repeat(64),
  ])
  const textStart = yield* next.prepare(SessionEvent.Text.Started, {
    sessionID: f.sessionID,
    assistantMessageID,
    ordinal: 0,
  })
  yield* textStart.publish
  const read = () =>
    f.database.db
      .select()
      .from(SessionMessageTable)
      .where(eq(SessionMessageTable.id, assistantMessageID))
      .get()
      .pipe(Effect.orDie)
  const before = yield* read()
  let reject = true
  yield* f.bus.project(SessionEvent.Text.Ended, () =>
    Effect.sync(() => {
      if (reject) {
        reject = false
        throw new Error("Synthetic provenance rollback")
      }
    }),
  )
  const textEnd = yield* next.prepare(SessionEvent.Text.Ended, {
    sessionID: f.sessionID,
    assistantMessageID,
    ordinal: 0,
    text: "After rollback",
  })
  expect(Exit.isFailure(yield* textEnd.publish.pipe(Effect.exit))).toBe(true)
  expect(yield* read()).toEqual(before)
  expect((yield* textEnd.publish).inserted).toBe(true)
  const committed = yield* read()
  if (!committed) throw new Error("Missing committed assistant")
  expect(committed.publication_provenance?.revision).toBe(
    SessionPublicationProvenance.revision(
      Schema.decodeUnknownSync(SessionMessage.Info)({ ...committed.data, id: committed.id, type: committed.type }),
    ),
  )
  // A changed prior row cannot acquire a repaired source association from a
  // later legitimate event, even though that event has its own valid permit.
  yield* f.database.db
    .update(SessionMessageTable)
    .set({ data: { ...committed.data, metadata: { changed: true } } })
    .where(eq(SessionMessageTable.id, assistantMessageID))
    .run()
    .pipe(Effect.orDie)
  const afterDrift = yield* next.prepare(SessionEvent.Text.Ended, {
    sessionID: f.sessionID,
    assistantMessageID,
    ordinal: 0,
    text: "Later permitted bytes",
  })
  yield* afterDrift.publish
  expect((yield* read())?.publication_provenance).toBeNull()
}

describe("Exact native publication writer", () => {
  it.effect("associates actual projected revisions and preserves unknown drift through native writes", () =>
    Effect.gen(function* () {
      for (let index = 0; index < 2; index++) {
        const f = yield* fixture({ deferAssistant: true })
        yield* verifyProjectionProvenance(f)
      }
    }),
  )

  it.effect("commits one original durable result and acknowledges an expired consumed unit without new exposure", () =>
    Effect.gen(function* () {
      for (let index = 0; index < 2; index++) {
        const f = yield* fixture(),
          writer = yield* f.make()
        f.state.ttl = 2000
        let observed = 0
        const stop = yield* f.bus.listen((event) =>
          Effect.sync(() => {
            if (event.type === SessionEvent.Text.Ended.type) observed++
          }),
        )
        const unit = yield* writer.prepare(SessionEvent.Text.Ended, f.text())
        expect(f.state.calls).toHaveLength(0)
        expect(Object.isFrozen(unit)).toBe(true)
        expect((yield* unit.publish).inserted).toBe(true)
        expect((yield* f.message())?.data).toMatchObject({ content: [{ type: "text", text: "Original answer" }] })
        expect((yield* f.message())?.publication_provenance).toBeNull()
        yield* f.expire(1)
        f.state.active = false
        yield* SessionExecutionCapability.clear(f.sessionID)
        expect((yield* unit.publish).inserted).toBe(false)
        expect(f.state.calls).toHaveLength(1)
        expect(observed).toBe(1)
        expect(yield* f.witnesses()).toHaveLength(1)
        yield* stop
      }
    }),
  )

  it.effect("gives equal delta bytes distinct identities and never replays a consumed raw delta", () =>
    Effect.gen(function* () {
      const f = yield* fixture(),
        writer = yield* f.make()
      const deltas: string[] = []
      yield* f.bus.listen((event) =>
        Effect.sync(() => {
          if (event.type === SessionEvent.Text.Delta.type) deltas.push((event.data as { delta: string }).delta)
        }),
      )
      const first = yield* writer.prepare(SessionEvent.Text.Delta, f.delta()),
        second = yield* writer.prepare(SessionEvent.Text.Delta, f.delta())
      expect(first.id).not.toBe(second.id)
      expect(first.digest).not.toBe(second.digest)
      yield* first.publish
      yield* second.publish
      yield* first.publish
      expect(deltas).toEqual(["same", "same"])
      const witnesses = yield* f.witnesses()
      expect(witnesses.map((row) => row.data.sequence)).toEqual([0, 1])
      expect(
        witnesses.every(
          (row) => JSON.stringify(row.data.batch) === JSON.stringify({ operation: "ephemeral-event", length: 1 }),
        ),
      ).toBe(true)
      expect((yield* f.message())?.data).toMatchObject({ content: [{ type: "text", text: "" }] })
    }),
  )

  it.effect("holds detached content, metadata and routing across the host wait", () =>
    Effect.gen(function* () {
      const f = yield* fixture(),
        writer = yield* f.make()
      const arrived = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>()
      f.state.request = () => Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(release)))
      const data = f.text(),
        metadata = { nested: { value: "original" }, date: new Date("2026-10-18T00:00:00Z") }
      const unit = yield* writer.prepare(SessionEvent.Text.Ended, data, { metadata })
      const observed: Array<{ location: unknown; metadata: unknown; data: unknown }> = []
      yield* f.bus.listen((event) =>
        Effect.sync(() => {
          if (event.type === SessionEvent.Text.Ended.type)
            observed.push({ location: event.location, metadata: event.metadata, data: event.data })
        }),
      )
      const pending = yield* unit.publish.pipe(Effect.forkScoped)
      yield* Deferred.await(arrived)
      data.text = "Substituted"
      metadata.nested.value = "substituted"
      metadata.date.setUTCFullYear(2030)
      yield* f.database.db
        .update(SessionTable)
        .set({ directory: "/moved" })
        .where(eq(SessionTable.id, f.sessionID))
        .run()
        .pipe(Effect.orDie)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(pending)
      expect(observed).toHaveLength(1)
      expect(observed[0]).toMatchObject({
        location: { directory: "/project" },
        metadata: { nested: { value: "original" }, date: "2026-10-18T00:00:00.000Z" },
        data: { text: "Original answer" },
      })
    }),
  )

  it.effect("rejects late RPCs after cancellation, installation replacement, retirement or deletion", () =>
    Effect.gen(function* () {
      for (const change of ["cancel", "replace", "retire", "delete"] as const) {
        const f = yield* fixture(),
          writer = yield* f.make()
        const arrived = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>()
        f.state.request = () => Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(release)))
        const unit = yield* writer.prepare(SessionEvent.Text.Delta, f.delta())
        let observed = 0
        const stop = yield* f.bus.listen((event) =>
          Effect.sync(() => {
            if (event.type === SessionEvent.Text.Delta.type) observed++
          }),
        )
        const pending = yield* unit.publish.pipe(Effect.exit, Effect.forkScoped)
        yield* Deferred.await(arrived)
        if (change === "cancel") f.state.active = false
        if (change === "replace") {
          yield* SessionExecutionCapability.clear(f.sessionID)
          yield* SessionExecutionCapability.set(f.sessionID, {
            inputID: f.inputID,
            disclosure: f.service,
            mcp: {},
            revision: 7,
          })
          yield* SessionExecutionCapability.select(f.sessionID, f.inputID)
        }
        if (change === "retire") yield* SessionLifecycle.retire(f.database.db, f.sessionID)
        if (change === "delete")
          yield* f.database.db.delete(SessionTable).where(eq(SessionTable.id, f.sessionID)).run().pipe(Effect.orDie)
        yield* Deferred.succeed(release, undefined)
        expect(Exit.isFailure(yield* Fiber.join(pending))).toBe(true)
        expect(yield* f.witnesses()).toHaveLength(0)
        expect(observed).toBe(0)
        yield* stop
      }
    }),
  )

  it.effect("rolls back projected bytes when ownership changes after the first commit check", () =>
    Effect.gen(function* () {
      const f = yield* fixture(),
        writer = yield* f.make()
      yield* f.bus.project(SessionEvent.Text.Ended, () =>
        Effect.sync(() => {
          f.state.active = false
        }),
      )
      const unit = yield* writer.prepare(SessionEvent.Text.Ended, f.text())
      expect(Exit.isFailure(yield* unit.publish.pipe(Effect.exit))).toBe(true)
      expect(yield* f.witnesses()).toHaveLength(0)
      expect((yield* f.message())?.data).toMatchObject({ content: [{ type: "text", text: "" }] })
    }),
  )

  it.effect("refreshes an expired unconsumed permit after rollback without changing its unit", () =>
    Effect.gen(function* () {
      const f = yield* fixture(),
        writer = yield* f.make()
      f.state.ttl = 2000
      let reject = true
      yield* f.bus.project(SessionEvent.Text.Ended, () =>
        Effect.sync(() => {
          if (reject) {
            reject = false
            throw new Error("fixture rollback")
          }
        }),
      )
      const unit = yield* writer.prepare(SessionEvent.Text.Ended, f.text())
      expect(Exit.isFailure(yield* unit.publish.pipe(Effect.exit))).toBe(true)
      expect(yield* f.witnesses()).toHaveLength(0)
      expect((yield* f.message())?.data).toMatchObject({ content: [{ type: "text", text: "" }] })
      yield* f.expire(1)
      expect((yield* unit.publish).inserted).toBe(true)
      expect(f.state.calls).toHaveLength(2)
      expect(f.state.calls[0]).toEqual(f.state.calls[1])
      expect((yield* unit.publish).inserted).toBe(false)
      expect(f.state.calls).toHaveLength(2)
    }),
  )

  it.effect("retains a consumed witness when a raw delta is dropped and never replays it", () =>
    Effect.gen(function* () {
      const f = yield* fixture(),
        writer = yield* f.make()
      let raw = 0
      yield* f.bus.listen((event) =>
        Effect.sync(() => {
          if (event.type === SessionEvent.PublicationCommitted.type) f.state.active = false
          if (event.type === SessionEvent.Text.Delta.type) raw++
        }),
      )
      const unit = yield* writer.prepare(SessionEvent.Text.Delta, f.delta())
      expect(Exit.isFailure(yield* unit.publish.pipe(Effect.exit))).toBe(true)
      expect(yield* f.witnesses()).toHaveLength(1)
      expect(raw).toBe(0)
      expect((yield* unit.publish).inserted).toBe(false)
      expect(raw).toBe(0)
      expect(f.state.calls).toHaveLength(1)
    }),
  )

  it.effect("serializes concurrent sends of one unit and keeps private service material out of history", () =>
    Effect.gen(function* () {
      const f = yield* fixture(),
        writer = yield* f.make()
      const barrier = yield* Deferred.make<void>()
      f.state.request = () =>
        Effect.gen(function* () {
          if (f.state.calls.length === 2) yield* Deferred.succeed(barrier, undefined)
          yield* Deferred.await(barrier)
        })
      const unit = yield* writer.prepare(SessionEvent.Text.Ended, f.text())
      const results = yield* Effect.all([unit.publish, unit.publish], { concurrency: "unbounded" })
      expect(results.map((result) => result.inserted).sort()).toEqual([false, true])
      expect(f.state.calls[0]).toEqual(f.state.calls[1])
      const rows = yield* f.database.db
        .select()
        .from(EventTable)
        .where(eq(EventTable.aggregate_id, f.sessionID))
        .all()
        .pipe(Effect.orDie)
      const encoded = JSON.stringify(rows)
      expect(encoded).not.toContain(f.service.token)
      expect(encoded).not.toContain("signature")
      expect(encoded).not.toContain(f.service.endpoint)
      expect(yield* f.witnesses()).toHaveLength(1)
    }),
  )

  it.effect("supports reasoning on independent originals and rejects equal grant replacement after projection", () =>
    Effect.gen(function* () {
      for (let index = 0; index < 2; index++) {
        const f = yield* fixture(),
          writer = yield* f.make()
        const data = { sessionID: f.sessionID, assistantMessageID: f.assistantMessageID, ordinal: 1 }
        const start = yield* writer.prepare(SessionEvent.Reasoning.Started, data)
        yield* start.publish
        const delta = yield* writer.prepare(SessionEvent.Reasoning.Delta, { ...data, delta: "reason" })
        yield* delta.publish
        const end = yield* writer.prepare(SessionEvent.Reasoning.Ended, { ...data, text: "reason" })
        yield* end.publish
        expect((yield* f.message())?.data).toMatchObject({
          content: [
            { type: "text", text: "" },
            { type: "reasoning", text: "reason" },
          ],
        })
        yield* f.bus.project(SessionEvent.Text.Ended, () =>
          Effect.gen(function* () {
            yield* SessionExecutionCapability.clear(f.sessionID)
            yield* SessionExecutionCapability.set(f.sessionID, {
              inputID: f.inputID,
              disclosure: f.service,
              mcp: {},
              revision: 7,
            })
            yield* SessionExecutionCapability.select(f.sessionID, f.inputID)
          }),
        )
        const late = yield* writer.prepare(SessionEvent.Text.Ended, f.text("denied"))
        expect(Exit.isFailure(yield* late.publish.pipe(Effect.exit))).toBe(true)
        expect(yield* f.witnesses()).toHaveLength(3)
        expect((yield* f.message())?.data).toMatchObject({
          content: [
            { type: "text", text: "" },
            { type: "reasoning", text: "reason" },
          ],
        })
      }
    }),
  )

  it.effect("rejects substituted or expired fresh permits before output", () =>
    Effect.gen(function* () {
      for (const change of ["unit", "scope", "generation", "expiry"] as const) {
        const f = yield* fixture(),
          writer = yield* f.make()
        f.state.mutate = (proof) =>
          change === "unit"
            ? { ...proof, unitDigest: "9".repeat(64) }
            : change === "scope"
              ? { ...proof, threadID: "other-thread" }
              : change === "generation"
                ? { ...proof, capabilityGeneration: 8 }
                : proof
        if (change === "expiry")
          f.state.request = () =>
            Effect.sync(() => {
              f.state.expired.add(1)
            })
        const unit = yield* writer.prepare(SessionEvent.Text.Ended, f.text())
        expect(Exit.isFailure(yield* unit.publish.pipe(Effect.exit))).toBe(true)
        expect(yield* f.witnesses()).toHaveLength(0)
        expect((yield* f.message())?.data).toMatchObject({ content: [{ type: "text", text: "" }] })
      }
    }),
  )

  it.effect("fails missing ownership, wrong revision, foreign sessions and unsupported events without RPC", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      expect(Exit.isFailure(yield* SessionPublicationWriter.capture(f.captureInput).pipe(Effect.exit))).toBe(true)
      expect(
        Exit.isFailure(
          yield* SessionPublicationWriter.capture({ ...f.captureInput, revision: 8 }).pipe(
            Effect.provideService(SessionRunCoordinator.Ownership, { live: () => true }),
            Effect.exit,
          ),
        ),
      ).toBe(true)
      const writer = yield* f.make()
      expect(
        Exit.isFailure(
          yield* writer
            .prepare(SessionEvent.Text.Ended, { ...f.text(), sessionID: Session.ID.create() })
            .pipe(Effect.exit),
        ),
      ).toBe(true)
      expect(
        Exit.isFailure(
          yield* writer
            .prepare(SessionEvent.Moved, {
              sessionID: f.sessionID,
              projectID: Project.ID.global,
              location: { directory: AbsolutePath.make("/moved") },
            })
            .pipe(Effect.exit),
        ),
      ).toBe(true)
      const unsupported = Bus.ephemeral({ type: "test.unsupported", schema: SessionEvent.Text.Delta.data.fields })
      expect(Exit.isFailure(yield* writer.prepare(unsupported, f.delta()).pipe(Effect.exit))).toBe(true)
      expect(f.state.calls).toHaveLength(0)
      expect(yield* f.witnesses()).toHaveLength(0)
    }),
  )

  it.effect("rejects cancellation and equal installation replacement while preflight is waiting", () =>
    Effect.gen(function* () {
      for (const change of ["cancel", "replace"] as const) {
        const f = yield* fixture(),
          selected = yield* f.capture()
        const arrived = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>()
        f.state.preflight = () => Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(release)))
        const pending = yield* selected.prepareContext(f.request).pipe(Effect.exit, Effect.forkScoped)
        yield* Deferred.await(arrived)
        if (change === "cancel") f.state.active = false
        if (change === "replace") {
          yield* SessionExecutionCapability.clear(f.sessionID)
          yield* SessionExecutionCapability.set(f.sessionID, {
            inputID: f.inputID,
            disclosure: f.service,
            mcp: {},
            revision: 7,
          })
          yield* SessionExecutionCapability.select(f.sessionID, f.inputID)
        }
        yield* Deferred.succeed(release, undefined)
        expect(Exit.isFailure(yield* Fiber.join(pending))).toBe(true)
        expect(f.state.contexts).toHaveLength(1)
        expect(f.state.calls).toHaveLength(0)
        expect(yield* f.witnesses()).toHaveLength(0)
      }
    }),
  )

  it.effect("owns the service before preflight and isolates public proof aliases from publication", () =>
    Effect.gen(function* () {
      const f = yield* fixture(),
        other = yield* fixture(),
        selected = yield* f.capture()
      f.service.endpoint = other.service.endpoint
      f.service.token = "substituted-token"
      f.service.keys[0]!.publicKey = other.service.keys[0]!.publicKey
      const context = yield* selected.prepareContext(f.request)
      expect(f.state.contexts).toHaveLength(1)
      expect(other.state.contexts).toHaveLength(0)
      Object.assign(context.proof, {
        sessionID: other.sessionID,
        contextDigest: "9".repeat(64),
        attempt: "substituted",
      })
      Object.assign(context.proof.publication!, { threadID: "substituted-thread", endpoint: other.service.endpoint })
      Object.assign(context.signed, { payload: "{}", signature: "z".repeat(86) })
      const unit = yield* context.writer.prepare(SessionEvent.Text.Ended, f.text())
      expect((yield* unit.publish).inserted).toBe(true)
      expect(f.state.calls).toHaveLength(1)
      expect(other.state.calls).toHaveLength(0)
      expect((yield* f.witnesses())[0]?.data).toMatchObject({
        sessionID: f.sessionID,
        inputID: f.inputID,
        contextDigest: f.request.contextDigest,
      })
      expect("make" in SessionPublicationWriter).toBe(false)
    }),
  )

  it.effect("requires the exact installed service snapshot before any preflight RPC", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      for (const service of [
        { ...f.service, token: "other" },
        { ...f.service, endpoint: f.service.endpoint + "/other" },
        { ...f.service, issuer: "other" },
        { ...f.service, audience: "other" },
        { ...f.service, keys: [{ ...f.service.keys[0]!, publicKey: "z".repeat(43) }] },
        { ...f.service, publicationVersion: 1 as const },
      ]) {
        const result = yield* SessionPublicationWriter.capture({ ...f.captureInput, service }).pipe(
          Effect.provideService(SessionRunCoordinator.Ownership, { live: () => true }),
          Effect.exit,
        )
        expect(Exit.isFailure(result)).toBe(true)
      }
      expect(f.state.contexts).toHaveLength(0)
      expect(f.state.calls).toHaveLength(0)
    }),
  )

  it.effect("keeps a capture live across unrelated input and session installations", () =>
    Effect.gen(function* () {
      const f = yield* fixture(),
        selected = yield* f.capture(),
        other = yield* fixture()
      yield* SessionExecutionCapability.set(f.sessionID, {
        inputID: SessionMessage.ID.create(),
        mcp: {},
        disclosure: f.service,
        revision: 7,
      })
      yield* SessionExecutionCapability.set(other.sessionID, {
        inputID: other.inputID,
        mcp: {},
        disclosure: other.service,
        revision: 8,
      })
      const context = yield* selected.prepareContext(f.request)
      const unit = yield* context.writer.prepare(SessionEvent.Text.Ended, f.text())
      expect((yield* unit.publish).inserted).toBe(true)
      expect(f.state.contexts).toHaveLength(1)
      expect(f.state.calls).toHaveLength(1)
      expect(other.state.contexts).toHaveLength(0)
    }),
  )

  withoutPersistence.effect("rejects actual disabled retention before requesting publication", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      expect(Exit.isFailure(yield* f.make().pipe(Effect.exit))).toBe(true)
      expect(f.state.calls).toHaveLength(0)
    }),
  )
})

const postgres = process.env.OPENCODE_TEST_DATABASE_URL
test.skipIf(!postgres)(
  "orders publication and retirement across independent PostgreSQL pools",
  async () => {
    if (!new URL(postgres!).pathname.startsWith("/native_sharing_"))
      throw new Error("dedicated native_sharing_ database required")
    await using temporary = await tmpdir()
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const layer = () =>
            AppNodeBuilder.build(graph, [
              Database.node.replace(Database.postgresDatabaseLayer(postgres!)),
              Bus.node.replace(Bus.configured({ persist: true })),
            ])
          const first = yield* Layer.build(layer()),
            second = yield* Layer.build(layer())
          const other = Context.get(second, Database.Service)
          expect(yield* Context.get(first, Database.Service).db.get(sql`SELECT pg_backend_pid() AS pid`)).not.toEqual(
            yield* other.db.get(sql`SELECT pg_backend_pid() AS pid`),
          )
          for (const winner of ["retirement", "publication", "provenance"] as const) {
            const f = yield* fixture({ deferAssistant: winner === "provenance" }).pipe(Effect.provideContext(first))
            try {
              if (winner === "provenance") {
                yield* verifyProjectionProvenance(f)
                continue
              }
              const writer = yield* f.make(),
                unit = yield* writer.prepare(SessionEvent.Text.Ended, f.text())
              const arrived = yield* Deferred.make<void>(),
                release = yield* Deferred.make<void>()
              let observed = 0
              yield* f.bus.listen((event) =>
                Effect.sync(() => {
                  if (event.type === SessionEvent.Text.Ended.type) observed++
                }),
              )
              if (winner === "retirement") {
                const retired = yield* other.db
                  .transaction(() =>
                    Effect.gen(function* () {
                      yield* SessionLifecycle.retire(other.db, f.sessionID)
                      yield* Deferred.succeed(arrived, undefined)
                      yield* Deferred.await(release)
                    }),
                  )
                  .pipe(Effect.orDie, Effect.forkScoped)
                yield* Deferred.await(arrived)
                const entering = yield* Deferred.make<void>()
                yield* f.bus.project(SessionEvent.PublicationCommitted, () =>
                  Deferred.succeed(entering, undefined).pipe(Effect.asVoid),
                )
                const publishing = yield* unit.publish.pipe(Effect.exit, Effect.forkScoped)
                yield* Deferred.await(entering)
                yield* Deferred.succeed(release, undefined)
                yield* Fiber.join(retired)
                expect(Exit.isFailure(yield* Fiber.join(publishing))).toBe(true)
                expect(yield* f.witnesses()).toHaveLength(0)
                expect(observed).toBe(0)
                expect((yield* f.message())?.data).toMatchObject({ content: [{ type: "text", text: "" }] })
              }
              if (winner === "publication") {
                yield* f.bus.project(SessionEvent.Text.Ended, () =>
                  Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(release))),
                )
                const publishing = yield* unit.publish.pipe(Effect.forkScoped)
                yield* Deferred.await(arrived)
                const retiring = yield* Deferred.make<number>()
                const retired = yield* other.db
                  .transaction(() =>
                    Effect.gen(function* () {
                      const row = yield* other.db
                        .get<{ pid: number }>(sql`SELECT pg_backend_pid() AS pid`)
                        .pipe(Effect.orDie)
                      yield* Deferred.succeed(retiring, row!.pid)
                      yield* SessionLifecycle.retire(other.db, f.sessionID)
                    }),
                  )
                  .pipe(Effect.orDie, Effect.forkScoped)
                const pid = yield* Deferred.await(retiring)
                let blocked = false
                for (let attempt = 0; attempt < 100 && !blocked; attempt++) {
                  const row = yield* f.database.db
                    .get<{ blocked: boolean }>(sql`SELECT cardinality(pg_blocking_pids(${pid})) > 0 AS blocked`)
                    .pipe(Effect.orDie)
                  blocked = row!.blocked
                  if (!blocked) yield* Effect.sleep(10)
                }
                expect(blocked).toBe(true)
                yield* Deferred.succeed(release, undefined)
                expect((yield* Fiber.join(publishing)).inserted).toBe(true)
                yield* Fiber.join(retired)
                expect(yield* f.witnesses()).toHaveLength(1)
                expect(observed).toBe(1)
                expect((yield* f.message())?.data).toMatchObject({
                  content: [{ type: "text", text: "Original answer" }],
                })
                expect(
                  Exit.isFailure(yield* writer.prepare(SessionEvent.Text.Ended, f.text("late")).pipe(Effect.exit)),
                ).toBe(true)
                expect(f.state.calls).toHaveLength(1)
              }
            } finally {
              yield* f.database.db.delete(SessionTable).where(eq(SessionTable.id, f.sessionID)).run().pipe(Effect.orDie)
              yield* f.database.db
                .delete(EventSequenceTable)
                .where(eq(EventSequenceTable.aggregate_id, f.sessionID))
                .run()
                .pipe(Effect.orDie)
              yield* f.database.db
                .delete(SessionLifecycleTable)
                .where(eq(SessionLifecycleTable.id, f.sessionID))
                .run()
                .pipe(Effect.orDie)
            }
          }
        }),
      ).pipe(Effect.provideService(Global.Service, Global.make({ data: temporary.path }))),
    )
  },
  20000,
)
