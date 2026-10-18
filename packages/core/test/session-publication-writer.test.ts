import { describe, expect, test } from "bun:test"
import { Context, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { eq, sql } from "drizzle-orm"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { SessionPublicationWriter } from "@opencode/core/session/publication-writer"
import { SessionDisclosure } from "@opencode/core/session/disclosure"
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

const graph = LayerNode.group([Database.node, Bus.node, SessionProjector.node, SessionStore.node, SessionInbox.node])
const it = testEffect(AppNodeBuilder.build(graph, [Bus.node.replace(Bus.configured({ persist: true }))]))
const withoutPersistence = testEffect(AppNodeBuilder.build(graph))
const fixture = Effect.fnUntraced(function* () {
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
  yield* bus.publish(SessionEvent.Step.Started, {
    sessionID,
    assistantMessageID,
    agent: Agent.ID.make("build"),
    model: { id: Model.ID.make("fixture"), providerID: Provider.ID.make("fixture") },
    started: 1,
  })
  yield* bus.publish(SessionEvent.Text.Started, { sessionID, assistantMessageID, ordinal: 0 })
  const service: ExecutionDisclosure.Service = {
    version: 1,
    endpoint: "http://127.0.0.1:8123/preflight",
    token: "private-original",
    issuer: "fixture-host",
    audience: "fixture-context",
    keys: [{ id: "context", algorithm: "Ed25519", publicKey: "a".repeat(43) }],
  }
  const descriptor: ExecutionDisclosure.PublicationService = {
    version: 1,
    endpoint: "http://127.0.0.1:8123/publication",
    audience: "fixture-publication",
    keys: [{ id: "publication", algorithm: "Ed25519", publicKey: "b".repeat(43) }],
    organizationID: "org-original",
    threadID: "thread-original",
    capabilityID: "cap-original",
    capabilityGeneration: 7,
  }
  const context: ExecutionDisclosure.ContextProof = {
    version: 1,
    issuer: service.issuer,
    audience: service.audience,
    sessionID,
    inputID,
    kind: "primary",
    attempt: "original-attempt",
    contextReference: SessionDisclosure.digest([sessionID, inputID]),
    contextDigest: "c".repeat(64),
    componentsDigest: "d".repeat(64),
    publication: descriptor,
    mode: "actor-private-unknown",
    complete: false,
    issuedAt: Date.now(),
    expiresAt: Date.now() + 90_000,
  }
  const state: {
    active: boolean
    calls: Array<{ unitID: string; unitDigest: string }>
    expired: Set<number>
    request?: () => Effect.Effect<void>
    mutate?: (proof: ExecutionDisclosure.PublicationProof) => ExecutionDisclosure.PublicationProof
  } = { active: true, calls: [], expired: new Set() }
  const signed = { keyID: "fixture", payload: "{}", signature: "x".repeat(86) }
  const prepared = {
    proof: context,
    signed,
    publication: {
      authorize: (unit: { unitID: string; unitDigest: string }) =>
        Effect.gen(function* () {
          state.calls.push({ ...unit })
          const index = state.calls.length
          yield* state.request?.() ?? Effect.void
          const proof: ExecutionDisclosure.PublicationProof = {
            version: 1,
            purpose: "thread-publication",
            issuer: service.issuer,
            audience: descriptor.audience,
            organizationID: descriptor.organizationID,
            threadID: descriptor.threadID,
            sessionID,
            inputID,
            capabilityID: descriptor.capabilityID,
            capabilityGeneration: 7,
            contextReference: context.contextReference,
            contextDigest: context.contextDigest,
            componentsDigest: context.componentsDigest,
            attempt: context.attempt,
            kind: "model-output",
            ...unit,
            receiptReference: SessionDisclosure.digest(["receipt", unit.unitID]),
            authorizationVersion: "original-version",
            manifestBinding: "e".repeat(64),
            audienceBinding: "f".repeat(64),
            issuedAt: Date.now(),
            expiresAt: Date.now() + 30_000,
          }
          return { proof: state.mutate?.(proof) ?? proof, signed, valid: () => !state.expired.has(index) }
        }),
    },
  }
  yield* SessionExecutionCapability.set(sessionID, { inputID, disclosure: service, mcp: {}, revision: 7 })
  yield* SessionExecutionCapability.select(sessionID, inputID)
  yield* Effect.addFinalizer(() => SessionExecutionCapability.clear(sessionID))
  const make = () =>
    SessionPublicationWriter.make({ prepared, revision: 7 }).pipe(
      Effect.provideService(SessionRunCoordinator.Ownership, { live: () => state.active }),
      Effect.provideService(Bus.Service, bus),
      Effect.provideService(Database.Service, database),
    )
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
    prepared,
    service,
    state,
    make,
    text,
    delta,
    witnesses,
    message,
  }
})

describe("Exact native publication writer", () => {
  it.effect("commits one original durable result and acknowledges an expired consumed unit without new exposure", () =>
    Effect.gen(function* () {
      for (let index = 0; index < 2; index++) {
        const f = yield* fixture(),
          writer = yield* f.make()
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
        f.state.expired.add(1)
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
      f.state.expired.add(1)
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
      expect(
        Exit.isFailure(yield* SessionPublicationWriter.make({ prepared: f.prepared, revision: 7 }).pipe(Effect.exit)),
      ).toBe(true)
      expect(
        Exit.isFailure(
          yield* SessionPublicationWriter.make({ prepared: f.prepared, revision: 8 }).pipe(
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
          for (const winner of ["retirement", "publication"] as const) {
            const f = yield* fixture().pipe(Effect.provideContext(first))
            try {
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
