import { describe, expect } from "bun:test"
import { and, eq } from "drizzle-orm"
import { Cause, Context, DateTime, Deferred, Effect, Exit, Fiber, Layer, Scope } from "effect"
import { Agent } from "@opencode/schema/agent"
import { Event } from "@opencode/schema/event"
import { Model } from "@opencode/schema/model"
import { Money } from "@opencode/schema/money"
import { Project } from "@opencode/schema/project"
import { Provider } from "@opencode/schema/provider"
import { ID, Info, Output } from "@opencode/schema/shell"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { FSUtil } from "@opencode/util/fs-util"
import { Global } from "@opencode/util/global"
import { Bus } from "../src/bus.js"
import { Database } from "../src/database/database.js"
import { EventTable } from "../src/event/sql.js"
import { Image } from "../src/image.js"
import { Instance } from "../src/instance/service.js"
import { Location } from "../src/location.js"
import { Plugin } from "../src/plugin.js"
import { PluginHooks } from "../src/plugin/hooks.js"
import { ProjectTable } from "../src/project/sql.js"
import { AbsolutePath, RelativePath } from "../src/schema.js"
import { InboxConflictError, NotFoundError, PromptConflictError } from "../src/session/error.js"
import { SessionEvent } from "../src/session/event.js"
import { SessionExecution } from "../src/session/execution.js"
import { SessionExecutionCapability } from "../src/session/execution-capability.js"
import { SessionInbox } from "../src/session/inbox.js"
import { SessionMessage } from "../src/session/message.js"
import { SessionPrompt } from "../src/session/prompt.js"
import { SessionProjector } from "../src/session/projector.js"
import { SessionRevert } from "../src/session/revert.js"
import { SessionRunCoordinator } from "../src/session/run-coordinator.js"
import { SessionSchema } from "../src/session/schema.js"
import { Session } from "../src/session/session.js"
import { SessionTable } from "../src/session/sql.js"
import { SessionStore } from "../src/session/store.js"
import { Shell } from "../src/shell.js"
import { Skill } from "../src/skill.js"
import { Snapshot } from "../src/snapshot.js"
import { tempGlobalLayer } from "./fixture/global"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"

const it = testEffect(
  LayerNode.compile(
    LayerNode.group([
      Database.node,
      Bus.node,
      SessionProjector.node,
      SessionStore.node,
      SessionInbox.node,
      FSUtil.node,
    ]),
    {
      replacements: [Bus.node.replace(Bus.configured({ persist: true })), Global.node.replace(tempGlobalLayer)],
    },
  ),
)
const sessionID = SessionSchema.ID.make("ses_owned")
const otherID = SessionSchema.ID.make("ses_owned_other")
const source = Location.Ref.make({ directory: AbsolutePath.make("/project") })
const skillInfo = Skill.Info.make({
  id: Skill.ID.make("guide"),
  name: Skill.Name.make("Guide"),
  description: "Session guidance",
  path: AbsolutePath.make("/skills/guide/SKILL.md"),
  content: "  Raw guidance\n",
})

const setup = Effect.fnUntraced(function* (options?: {
  execution?: SessionExecution.Interface
  enqueued?: Effect.Effect<void>
  shell?: Layer.Layer<Shell.Service>
  skills?: (ref: Location.Ref) => Layer.Layer<Skill.Service>
  snapshot?: (ref: Location.Ref) => Layer.Layer<Snapshot.Service>
}) {
  yield* SessionExecutionCapability.clear(sessionID)
  const database = yield* Database.Service
  const bus = yield* Bus.Service
  const store = yield* SessionStore.Service
  yield* database.db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: source.directory, sandboxes: [] })
    .run()
    .pipe(Effect.orDie)
  yield* Effect.forEach([sessionID, otherID], (id) =>
    bus.publish(SessionEvent.Created, {
      sessionID: id,
      projectID: Project.ID.global,
      location: source,
      slug: "owned",
      title: "Owned session",
      version: "test",
    }),
  )
  const hooks = yield* PluginHooks.Service.pipe(Effect.provide(LayerNode.compile(PluginHooks.node)))
  const locations: Location.Ref[] = []
  const activationWaits: Location.Ref[] = []
  const resumes: SessionSchema.ID[] = []
  const wakes: Array<{ sessionID: SessionSchema.ID; pending: SessionMessage.ID[]; enqueued: number }> = []
  const execution = SessionExecution.Service.of({
    active: Effect.succeed(new Set<SessionSchema.ID>()),
    isActive: () => Effect.succeed(false),
    inspect: () => Effect.succeed({ active: false }),
    resume: (id) =>
      Effect.sync(() => {
        resumes.push(id)
      }),
    awaitIdle: () => Effect.void,
    interrupt: () => Effect.succeed(false),
    wake: (id) =>
      Effect.gen(function* () {
        const pending = yield* SessionInbox.list(database.db, id)
        const events = yield* database.db
          .select({ id: EventTable.id })
          .from(EventTable)
          .where(
            and(
              eq(EventTable.aggregate_id, id),
              eq(EventTable.type, Bus.versionedType(SessionEvent.InboxEnqueued.type, 1)),
            ),
          )
          .all()
          .pipe(Effect.orDie)
        wakes.push({ sessionID: id, pending: pending.map((item) => item.id), enqueued: events.length })
      }),
  })
  const services = Layer.mergeAll(
    Layer.succeed(PluginHooks.Service, hooks),
    Layer.mock(Image.Service, {}),
    options?.shell ?? Layer.mock(Shell.Service, {}),
  )
  const servicesFor = (ref: Location.Ref) => {
    locations.push(ref)
    return Layer.mergeAll(
      services,
      Layer.succeed(Location.Service, location(ref)),
      options?.skills?.(ref) ??
        Layer.mock(Skill.Service, {
          get: (id) => Effect.succeed(id === skillInfo.id ? skillInfo : undefined),
        }),
      options?.snapshot?.(ref) ?? Layer.mock(Snapshot.Service, {}),
      Layer.mock(Plugin.Service, {
        awaitActivation: Effect.sync(() => {
          activationWaits.push(ref)
        }),
      }),
    ).pipe(Layer.fresh)
  }
  const instances = Instance.Service.of({
    // This fixture supplies only the instance services exercised by Session.
    provide: (session) => Effect.provide(servicesFor(session.location) as Layer.Layer<Instance.Services>),
  })
  const admission = options?.enqueued
    ? yield* SessionInbox.make().pipe(
        Effect.provideService(
          Bus.Service,
          Bus.Service.of({
            ...bus,
            publish: (definition, data, input) =>
              bus
                .publish(definition, data, input)
                .pipe(
                  Effect.tap((event) =>
                    event.type === SessionEvent.InboxEnqueued.type ? options.enqueued! : Effect.void,
                  ),
                ),
          }),
        ),
      )
    : yield* SessionInbox.Service
  const sessions = yield* Session.make().pipe(
    Effect.satisfiesServicesType<
      | Bus.Service
      | Database.Service
      | FSUtil.Service
      | SessionStore.Service
      | Instance.Service
      | SessionExecution.Service
      | SessionInbox.Service
      | Scope.Scope
    >(),
    Effect.provideService(Instance.Service, instances),
    Effect.provideService(SessionInbox.Service, admission),
    Effect.provideService(SessionExecution.Service, options?.execution ?? execution),
  )
  return { sessions, instances, hooks, locations, activationWaits, resumes, wakes, db: database.db, bus, store }
})

describe("Session-owned handles", () => {
  it.live("commits the current staged revert once for concurrent prepared inputs", () =>
    Effect.gen(function* () {
      const fixture = yield* setup({
        snapshot: () => Layer.mock(Snapshot.Service, { capture: () => Effect.undefined }),
      })
      const handle = fixture.sessions.forSession(sessionID)
      const boundary = yield* handle.synthetic({ text: "Boundary", resume: false })
      yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "steer")
      yield* handle.revert.stage({ messageID: boundary.id, files: false })
      const ready = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let prepared = 0
      const hook = yield* fixture.hooks.register("session", "prompt", () =>
        Effect.gen(function* () {
          prepared++
          if (prepared === 2) yield* Deferred.succeed(ready, undefined)
          yield* Deferred.await(release)
        }),
      )
      const first = yield* handle
        .prompt({
          id: SessionMessage.ID.make("msg_revert_first"),
          text: "First",
          mcp: {},
          mcpRevision: 1,
          resume: false,
        })
        .pipe(Effect.exit, Effect.forkChild)
      const second = yield* handle
        .prompt({
          id: SessionMessage.ID.make("msg_revert_second"),
          text: "Second",
          mcp: {},
          mcpRevision: 1,
          resume: false,
        })
        .pipe(Effect.exit, Effect.forkChild)
      yield* Deferred.await(ready)
      yield* Deferred.succeed(release, undefined)
      expect(Exit.isSuccess(yield* Fiber.join(first))).toBe(true)
      expect(Exit.isSuccess(yield* Fiber.join(second))).toBe(true)
      expect((yield* handle.inbox()).map((item) => item.id).sort()).toEqual([
        SessionMessage.ID.make("msg_revert_first"),
        SessionMessage.ID.make("msg_revert_second"),
      ])
      expect((yield* handle.get()).revert).toBeUndefined()
      yield* hook.dispose
    }),
  )

  it.live("installs MCP credentials before a concurrent drain can promote the new input", () =>
    Effect.gen(function* () {
      const committed = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const fixture = yield* setup({
        enqueued: Deferred.succeed(committed, undefined).pipe(Effect.andThen(Deferred.await(release))),
      })
      const id = SessionMessage.ID.make("msg_atomic_capability_install")
      const mcp = {
        host: {
          type: "remote" as const,
          url: "https://fixture.invalid",
          headers: { authorization: "Bearer synthetic" },
        },
      }
      const admitting = yield* fixture.sessions
        .forSession(sessionID)
        .prompt({ id, text: "Original", mcp, mcpRevision: 1, resume: false })
        .pipe(Effect.forkChild)
      yield* Deferred.await(committed)
      const promoting = yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "input", () =>
        Effect.gen(function* () {
          expect((yield* SessionExecutionCapability.get(sessionID, id))?.mcp).toEqual(mcp)
          return true
        }),
      ).pipe(Effect.exit, Effect.forkChild)
      // The durable row is visible while the publisher is paused. An active
      // drain must wait for its credential installation, never consume it early.
      yield* Effect.sleep("100 millis")
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(admitting)
      expect(Exit.isSuccess(yield* Fiber.join(promoting))).toBe(true)
      expect((yield* fixture.sessions.forSession(sessionID).message(id))?.type).toBe("user")
      expect(fixture.wakes).toEqual([])
      yield* SessionExecutionCapability.drop(sessionID, id)
    }),
  )

  it.live("renews only existing MCP inputs without changing admission or waking execution", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const handle = fixture.sessions.forSession(sessionID)
      for (const suffix of ["one", "two"]) {
        const id = SessionMessage.ID.make(`msg_renew_${suffix}`)
        const original = yield* handle.prompt({
          id,
          text: "original request",
          metadata: { immutable: suffix },
          capabilities: ["mcp"],
          resume: false,
          delivery: "queue",
        })
        const mcp = {
          host: {
            type: "remote" as const,
            url: "https://host.example.test",
            headers: { authorization: "Bearer first" },
          },
        }
        const claim = `renew_${suffix}`
        yield* handle.beginInboxCapabilityRenewal(id, claim)
        yield* handle.renewInboxCapability(id, { mcp, revision: 2, claim })
        yield* handle.renewInboxCapability(id, {
          mcp: {
            host: { headers: { authorization: "Bearer first" }, url: "https://host.example.test", type: "remote" },
          },
          revision: 2,
          claim,
        })
        expect(yield* handle.inbox()).toContainEqual(original)
        expect((yield* SessionExecutionCapability.get(sessionID, id))?.mcp).toEqual(mcp)
        for (const input of [{ mcp, revision: 1 }, { mcp }, { mcp: {}, revision: 2 }]) {
          expect(yield* handle.renewInboxCapability(id, { ...input, claim }).pipe(Effect.flip)).toBeInstanceOf(
            InboxConflictError,
          )
        }
        expect((yield* SessionExecutionCapability.get(sessionID, id))?.mcp).toEqual(mcp)
        yield* handle.beginInboxCapabilityRenewal(id, claim + "_next")
        yield* handle.renewInboxCapability(id, { mcp: {}, revision: 3, claim: claim + "_next" })
        expect((yield* SessionExecutionCapability.get(sessionID, id))?.mcp).toEqual({})
        expect(
          yield* fixture.sessions.forSession(otherID).renewInboxCapability(id, { mcp, revision: 4 }).pipe(Effect.flip),
        ).toBeInstanceOf(InboxConflictError)
        yield* handle.cancelInbox(id)
        expect(yield* handle.renewInboxCapability(id, { mcp, revision: 4 }).pipe(Effect.flip)).toBeInstanceOf(
          InboxConflictError,
        )
        expect(yield* handle.inbox()).toEqual([])
        expect(yield* handle.message(id)).toBeUndefined()
        expect(yield* SessionExecutionCapability.get(sessionID, id)).toBeUndefined()
      }
      const plain = yield* handle.prompt({ text: "no capability marker", resume: false })
      expect(yield* handle.renewInboxCapability(plain.id, { mcp: {} }).pipe(Effect.flip)).toBeInstanceOf(
        InboxConflictError,
      )
      expect(yield* SessionExecutionCapability.get(sessionID, plain.id)).toBeUndefined()
      expect(
        yield* fixture.sessions
          .forSession(SessionSchema.ID.make("ses_missing_renewal"))
          .renewInboxCapability(plain.id, { mcp: {} })
          .pipe(Effect.flip),
      ).toBeInstanceOf(NotFoundError)
      expect(fixture.wakes).toEqual([])
      // Only the first admissions and cancellations were recorded; renewal
      // credentials and revisions must not appear in the durable event log.
      const events = yield* fixture.db
        .select({ type: EventTable.type })
        .from(EventTable)
        .where(eq(EventTable.aggregate_id, sessionID))
        .all()
        .pipe(Effect.orDie)
      expect(events).toHaveLength(6)
    }),
  )

  it.live("rechecks pending existence after waiting on the cancellation and delivery boundary", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const handle = fixture.sessions.forSession(sessionID)
      for (const terminal of ["cancelled", "delivered"] as const) {
        const id = SessionMessage.ID.make(`msg_renew_${terminal}`)
        yield* handle.prompt({ id, text: "original", capabilities: ["mcp"], resume: false })
        const acquired = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const holder = yield* SessionInbox.serialized(
          sessionID,
          Deferred.succeed(acquired, undefined).pipe(Effect.andThen(Deferred.await(release))),
        ).pipe(Effect.forkChild)
        yield* Deferred.await(acquired)
        const renewing = yield* handle
          .renewInboxCapability(id, { mcp: {}, revision: 1 })
          .pipe(Effect.exit, Effect.forkChild)
        // Replay can invalidate a pending row while a local operation waits.
        yield* fixture.bus.publish(
          terminal === "cancelled" ? SessionEvent.InboxCancelled : SessionEvent.InboxDelivered,
          { sessionID, inboxID: id },
        )
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(holder)
        expect(Exit.isFailure(yield* Fiber.join(renewing))).toBe(true)
        expect(yield* handle.inbox()).toEqual([])
        expect(yield* SessionExecutionCapability.get(sessionID, id)).toBeUndefined()
      }
      expect(fixture.wakes).toEqual([])
    }),
  )

  it.live("owns state changes and message reads without caller services or Location acquisition", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const handle = fixture.sessions.forSession(sessionID)
      const model = { id: Model.ID.make("test-model"), providerID: Provider.ID.make("test-provider") }
      const messageID = SessionMessage.ID.create()
      yield* fixture.bus.publish(SessionEvent.Step.Started, {
        sessionID,
        assistantMessageID: messageID,
        agent: Agent.ID.make("build"),
        model: { ...model, id: Model.ID.make("initial-model") },
        started: 0,
      })
      yield* fixture.bus.publish(SessionEvent.Step.Ended, {
        sessionID,
        assistantMessageID: messageID,
        finish: "stop",
        cost: Money.USD.zero,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      })
      yield* fixture.db
        .update(SessionTable)
        .set({ time_idle: 0 })
        .where(eq(SessionTable.id, sessionID))
        .run()
        .pipe(Effect.orDie)
      const { rename, switchAgent, switchModel, view, message } = handle

      yield* Effect.gen(function* () {
        yield* rename({ title: "Renamed" })
        yield* switchAgent({ agent: Agent.ID.make("review") })
        yield* switchModel({ model })
        yield* switchModel({ model })
        yield* view({ idle: 0 })
        yield* view({ idle: 0 })
        expect(yield* message(messageID)).toMatchObject({ type: "assistant", content: [] })
      }).pipe(Effect.satisfiesServicesType<never>(), Effect.setContext(Context.empty()))

      const session = yield* handle.get()
      expect(session).toMatchObject({ title: "Renamed", agent: "review", model })
      expect(session.time.viewed && DateTime.toEpochMillis(session.time.viewed)).toBe(0)
      expect(yield* fixture.sessions.forSession(otherID).message(messageID)).toBeUndefined()
      expect((yield* fixture.sessions.forSession(otherID).get()).title).toBe("Owned session")
      expect(fixture.locations).toEqual([])
      expect(fixture.wakes).toEqual([])
      const events = yield* fixture.db
        .select()
        .from(EventTable)
        .where(eq(EventTable.aggregate_id, sessionID))
        .all()
        .pipe(Effect.orDie)
      expect(events.filter((event) => event.type === Bus.versionedType(SessionEvent.Viewed.type, 1))).toHaveLength(1)
      expect(
        events.filter((event) => event.type === Bus.versionedType(SessionEvent.ModelSelected.type, 1)),
      ).toHaveLength(1)
    }),
  )

  it.live("acquires Location only for new prompt preparation and persists before waking", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const handle = fixture.sessions.forSession(sessionID)
      const { get, prompt } = handle
      expect(handle.id).toBe(sessionID)
      expect((yield* get().pipe(Effect.satisfiesServicesType<never>())).location).toEqual(source)
      const synthetic = yield* handle.synthetic({ text: "Background result", resume: false })
      expect(fixture.locations).toEqual([])
      expect(fixture.wakes).toEqual([])

      const calls: string[] = []
      yield* fixture.hooks.register("session", "prompt", (event) =>
        Effect.sync(() => {
          expect(fixture.activationWaits).toEqual([source])
          calls.push(event.prompt.text)
          event.prompt.text += " prepared"
        }),
      )
      const first = yield* prompt({
        id: SessionMessage.ID.make("msg_owned_prepared"),
        text: "Original",
        files: [{ uri: new URL("./session-owned.test.ts", import.meta.url).href }],
      })
      const retried = yield* fixture.sessions.forSession(sessionID).prompt({
        id: first.id,
        text: "Ignored retry",
        files: [{ uri: "file:///missing-owned-retry" }],
        delivery: "queue",
      })

      expect(retried).toEqual(first)
      expect(first.payload.text).toBe("Original prepared")
      expect(first.payload.files?.[0]?.mime).toBe("text/plain")
      expect(Buffer.from(first.payload.files?.[0]?.data ?? "", "base64").toString()).toBe(
        yield* Effect.promise(() => Bun.file(import.meta.path).text()),
      )
      expect(calls).toEqual(["Original"])
      expect(fixture.locations).toEqual([source])
      expect(fixture.activationWaits).toEqual([source])
      expect(fixture.wakes).toEqual([
        { sessionID, pending: [synthetic.id, first.id], enqueued: 2 },
        { sessionID, pending: [synthetic.id, first.id], enqueued: 2 },
      ])
      expect(yield* SessionInbox.find(fixture.db, first.id)).toEqual(first)
      expect(yield* fixture.store.context(sessionID)).toEqual([])
    }),
  )

  it.live("keeps the first admission across handles, including delivered retries and identity conflicts", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const first = fixture.sessions.forSession(sessionID)
      const second = fixture.sessions.forSession(sessionID)
      const other = fixture.sessions.forSession(otherID)
      const prompt = yield* first.prompt({ text: "Keep this", metadata: { source: "first" }, resume: false })
      const retry = { id: prompt.id, text: "Ignore this", metadata: { source: "retry" }, resume: false }
      expect(yield* second.prompt({ ...retry, delivery: "queue" })).toEqual(prompt)
      const conflict = yield* other.prompt(retry).pipe(Effect.flip)
      expect(conflict).toBeInstanceOf(PromptConflictError)
      expect(conflict).toMatchObject({ _tag: "Session.PromptConflictError", sessionID: otherID, messageID: prompt.id })
      expect(yield* second.synthetic(retry).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.SyntheticConflictError",
        sessionID,
        inputID: prompt.id,
      })
      const synthetic = yield* first.synthetic({ text: "Original completion", description: "Job", resume: false })
      expect(yield* second.synthetic({ ...retry, id: synthetic.id })).toEqual(synthetic)
      expect(yield* first.inbox()).toEqual([prompt, synthetic])

      yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "steer")
      // Delivered identity must be recoverable from the message, without retained enqueue history.
      yield* fixture.db
        .delete(EventTable)
        .where(
          and(
            eq(EventTable.aggregate_id, sessionID),
            eq(EventTable.type, Bus.versionedType(SessionEvent.InboxEnqueued.type, 1)),
          ),
        )
        .run()
        .pipe(Effect.orDie)
      expect((yield* second.prompt({ ...retry, files: [{ uri: "file:///missing-owned-retry" }] })).payload).toEqual(
        prompt.payload,
      )
      expect((yield* second.synthetic({ ...retry, id: synthetic.id })).payload).toEqual(synthetic.payload)
      expect(yield* other.synthetic({ ...retry, id: synthetic.id }).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.SyntheticConflictError",
        sessionID: otherID,
        inputID: synthetic.id,
      })
      expect(yield* second.prompt({ ...retry, id: synthetic.id }).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.PromptConflictError",
        sessionID,
        messageID: synthetic.id,
      })
      expect(yield* second.inbox()).toEqual([])
      expect(yield* fixture.store.context(sessionID)).toMatchObject([
        { id: prompt.id, text: "Keep this", metadata: { source: "first" } },
        { id: synthetic.id, text: "Original completion", description: "Job" },
      ])
      expect(fixture.locations).toEqual([source])
    }),
  )

  it.live("reads fresh placement through an existing handle after a projected move", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const handle = fixture.sessions.forSession(sessionID)
      yield* handle.prompt({ text: "Before move", resume: false })
      const get = handle.get()
      const prompt = handle.prompt({ text: "After move", resume: false })
      const destination = Location.Ref.make({ directory: AbsolutePath.make("/project/moved") })

      yield* fixture.bus.publish(SessionEvent.Moved, {
        sessionID,
        location: destination,
        projectID: Project.ID.global,
        subpath: RelativePath.make("moved"),
      })

      expect((yield* get).location).toEqual(destination)
      expect(fixture.locations).toEqual([source])
      yield* prompt
      expect(fixture.locations).toEqual([source, destination])
      expect(fixture.activationWaits).toEqual([source, destination])
      expect((yield* fixture.sessions.forSession(otherID).get()).location).toEqual(source)
    }),
  )

  it.live("activates skills through detached handles using fresh placement and ambient publication context", () =>
    Effect.gen(function* () {
      const fixture = yield* setup({
        skills: (ref) =>
          Layer.mock(Skill.Service, { get: () => Effect.succeed({ ...skillInfo, content: ref.directory }) }),
      })
      const handle = fixture.sessions.forSession(sessionID)
      const { skill } = handle
      const events: Event.Payload[] = []
      yield* fixture.bus.listen((event) =>
        Effect.sync(() => {
          events.push(event)
        }),
      )
      const initial = SessionMessage.ID.make("msg_owned_skill_initial")
      yield* skill({ messageID: initial, skill: skillInfo.id, resume: false }).pipe(
        Effect.satisfiesServicesType<never>(),
        Effect.setContext(Context.empty()),
      )
      const moved = SessionMessage.ID.make("msg_owned_skill_moved")
      const activation = skill({ messageID: moved, skill: skillInfo.id, resume: false })
      const destination = Location.Ref.make({ directory: AbsolutePath.make("/project/moved") })
      yield* fixture.bus.publish(SessionEvent.Moved, {
        sessionID,
        location: destination,
        projectID: Project.ID.global,
        subpath: RelativePath.make("moved"),
      })

      yield* activation.pipe(Effect.satisfiesServicesType<never>(), Effect.setContext(Context.empty()))
      yield* skill({ skill: skillInfo.id, resume: false }).pipe(
        Effect.provideService(Location.Service, location(source)),
      )

      expect(fixture.locations).toEqual([source, destination, destination])
      expect(yield* handle.message(initial)).toMatchObject({ type: "skill", text: source.directory })
      expect(yield* handle.message(moved)).toMatchObject({ type: "skill", text: destination.directory })
      expect(
        events.filter((event) => event.type === SessionEvent.Skill.Activated.type).map((event) => event.location),
      ).toEqual([undefined, undefined, source])
      expect(fixture.activationWaits).toEqual([source, destination, destination])
      expect(fixture.resumes).toEqual([])
      expect(fixture.wakes).toEqual([])
    }),
  )

  it.live("checks Session existence before skill lookup and leaves missing activations untouched", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const events: Event.Payload[] = []
      yield* fixture.bus.listen((event) =>
        Effect.sync(() => {
          events.push(event)
        }),
      )
      const missingID = SessionSchema.ID.make("ses_missing_skill")
      expect(
        yield* fixture.sessions.forSession(missingID).skill({ skill: skillInfo.id }).pipe(Effect.flip),
      ).toMatchObject({ _tag: "Session.NotFoundError", sessionID: missingID })
      expect(fixture.locations).toEqual([])
      const handle = fixture.sessions.forSession(sessionID)
      const before = yield* handle.get()
      const missing = Skill.ID.make("missing")

      expect(yield* handle.skill({ skill: missing }).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.SkillNotFoundError",
        skill: missing,
      })

      expect(fixture.locations).toEqual([source])
      expect(events).toEqual([])
      expect(yield* handle.get()).toEqual(before)
      expect(yield* handle.inbox()).toEqual([])
      expect(yield* fixture.store.context(sessionID)).toEqual([])
      expect(fixture.activationWaits).toEqual([source])
      expect(fixture.resumes).toEqual([])
      expect(fixture.wakes).toEqual([])
    }),
  )

  it.live("publishes skills before detached resumes and owns those resumes in the host scope", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const host = yield* Scope.fork(scope, "sequential")
      const calls: string[] = []
      const stopped: SessionSchema.ID[] = []
      const execution = yield* SessionExecution.Service.pipe(Effect.provide(SessionExecution.noopLayer))
      const fixture = yield* setup({
        execution: {
          ...execution,
          resume: (id) =>
            Effect.gen(function* () {
              calls.push(`resume:${id}`)
              yield* Effect.never
            }).pipe(
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  stopped.push(id)
                }),
              ),
            ),
          wake: () =>
            Effect.sync(() => {
              calls.push("wake")
            }),
        },
      }).pipe(Scope.provide(host))
      yield* fixture.bus.listen((event) =>
        Effect.sync(() => {
          if (event.type === SessionEvent.Skill.Activated.type) calls.push(`published:${event.id}`)
        }),
      )
      const { skill } = fixture.sessions.forSession(sessionID)

      yield* skill({ messageID: SessionMessage.ID.make("msg_skill_no_resume"), skill: skillInfo.id, resume: false })
      expect(calls).toEqual(["published:evt_skill_no_resume"])
      yield* Effect.forEach(
        [
          { messageID: SessionMessage.ID.make("msg_skill_default_resume"), skill: skillInfo.id },
          { messageID: SessionMessage.ID.make("msg_skill_explicit_resume"), skill: skillInfo.id, resume: true },
        ],
        (input) => skill(input).pipe(Effect.scoped, Effect.forkScoped, Effect.flatMap(Fiber.join)),
      )

      expect(calls).toEqual([
        "published:evt_skill_no_resume",
        "published:evt_skill_default_resume",
        `resume:${sessionID}`,
        "published:evt_skill_explicit_resume",
        `resume:${sessionID}`,
      ])
      expect(stopped).toEqual([])
      yield* Scope.close(host, Exit.void)
      expect(stopped).toEqual([sessionID, sessionID])
      expect(yield* fixture.sessions.forSession(sessionID).inbox()).toEqual([])
    }),
  )

  it.live("keeps prompt wakes independent of shell work across handles", () =>
    Effect.gen(function* () {
      const blocked = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const started = Info.make({
        id: ID.make("sh_owned"),
        command: "echo owned",
        cwd: source.directory,
        shell: "sh",
        file: "/project/shell.out",
        status: "running",
        metadata: { sessionID, background: true },
        time: { started: 0 },
      })
      const fixture = yield* setup({
        shell: Layer.mock(Shell.Service, {
          create: (input) =>
            Effect.sync(() => {
              expect(input).toEqual({
                command: started.command,
                cwd: source.directory,
                timeout: 0,
                metadata: { sessionID, background: true },
              })
              return started
            }),
          result: () =>
            Deferred.succeed(blocked, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as({
                info: Info.make({ ...started, status: "exited", exit: 0, time: { started: 0, completed: 1 } }),
                capture: { output: "owned", truncated: false },
              }),
            ),
          output: () => Effect.succeed(Output.make({ output: "owned", cursor: 5, size: 5, truncated: false })),
        }),
      })
      const shell = yield* fixture.sessions
        .forSession(sessionID)
        .shell({ id: SessionMessage.ID.make("msg_owned_shell"), command: started.command })
        .pipe(Effect.forkScoped)
      yield* Deferred.await(blocked)

      const admitted = yield* fixture.sessions.forSession(sessionID).prompt({ text: "Admit while the shell runs" })
      expect(yield* SessionInbox.find(fixture.db, admitted.id)).toEqual(admitted)
      expect(fixture.wakes).toEqual([{ sessionID, pending: [admitted.id], enqueued: 1 }])
      const other = yield* fixture.sessions.forSession(otherID).prompt({ text: "Independent Session" })
      expect(fixture.wakes).toEqual([
        { sessionID, pending: [admitted.id], enqueued: 1 },
        { sessionID: otherID, pending: [other.id], enqueued: 1 },
      ])

      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(shell)
      expect(fixture.wakes).toEqual([
        { sessionID, pending: [admitted.id], enqueued: 1 },
        { sessionID: otherID, pending: [other.id], enqueued: 1 },
      ])
      expect(yield* fixture.store.context(sessionID)).toMatchObject([
        { type: "shell", shellID: started.id, status: "exited", output: { output: "owned" } },
      ])
      expect(yield* fixture.sessions.forSession(sessionID).inbox()).toMatchObject([
        { id: admitted.id, type: "user" },
        { type: "synthetic", payload: { metadata: { source: "shell", shellID: started.id, state: "completed" } } },
      ])
    }),
  )

  it.live("mutates only this handle's pending inbox and preserves public conflict tags", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const handle = fixture.sessions.forSession(sessionID)
      const second = fixture.sessions.forSession(sessionID)
      const queued = yield* handle.synthetic({ text: "Queued", delivery: "queue", resume: false })
      const steer = yield* handle.prompt({ text: "Steer", resume: false })
      const compact = yield* handle.compact({ delivery: "queue" })

      yield* second.steerInbox(queued.id)
      yield* second.queueInbox(steer.id)
      expect(yield* handle.inbox()).toMatchObject([
        { id: queued.id, delivery: "steer" },
        { id: steer.id, delivery: "queue" },
        { id: compact.id, type: "compaction", delivery: "queue" },
      ])
      expect(fixture.wakes).toHaveLength(2)
      expect(yield* fixture.sessions.forSession(otherID).cancelInbox(queued.id).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.InboxConflictError",
        sessionID: otherID,
        inboxID: queued.id,
      })
      yield* second.cancelInbox(compact.id)
      const cancelled = yield* handle.cancelInbox(compact.id).pipe(Effect.flip)
      expect(cancelled).toBeInstanceOf(InboxConflictError)
      expect(cancelled).toMatchObject({ _tag: "Session.InboxConflictError", sessionID, inboxID: compact.id })
      expect(yield* handle.compact({ id: steer.id }).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.CompactionConflictError",
        sessionID,
        inputID: steer.id,
      })

      expect(yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "steer")).toBe(1)
      expect(yield* second.queueInbox(queued.id).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.InboxConflictError",
        sessionID,
        inboxID: queued.id,
      })
      expect(yield* handle.inbox()).toMatchObject([{ id: steer.id, delivery: "queue" }])
      yield* second.cancelInbox(steer.id)
      expect(yield* handle.inbox()).toEqual([])
      const missingID = SessionSchema.ID.make("ses_owned_missing")
      const missing = yield* fixture.sessions.forSession(missingID).inbox().pipe(Effect.flip)
      expect(missing).toBeInstanceOf(NotFoundError)
      expect(missing).toMatchObject({ _tag: "Session.NotFoundError", sessionID: missingID })
      expect(fixture.locations).toEqual([source])
    }),
  )

  it.live("joins same-ID resumes without transferring execution ownership to a cancelled caller", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const joining = yield* Deferred.make<void>()
      const drains: SessionSchema.ID[] = []
      const resumes: SessionSchema.ID[] = []
      const interrupts: Array<{ sessionID: SessionSchema.ID; options?: { readonly resume?: boolean } }> = []
      const coordinator = yield* SessionRunCoordinator.make<SessionSchema.ID, never>({
        drain: (id) =>
          Effect.sync(() => void drains.push(id)).pipe(
            Effect.andThen(Deferred.succeed(started, undefined)),
            Effect.andThen(Deferred.await(release)),
          ),
      })
      const fixture = yield* setup({
        execution: SessionExecution.Service.of({
          active: coordinator.active,
          isActive: coordinator.isActive,
          inspect: (sessionID) => coordinator.isActive(sessionID).pipe(Effect.map((active) => ({ active }))),
          resume: (id) =>
            Effect.gen(function* () {
              resumes.push(id)
              if (resumes.length === 2) yield* Deferred.succeed(joining, undefined)
              yield* coordinator.run(id)
            }),
          wake: coordinator.wake,
          awaitIdle: coordinator.awaitIdle,
          interrupt: (id, options) =>
            Effect.sync(() => void interrupts.push({ sessionID: id, options })).pipe(
              Effect.andThen(coordinator.interrupt(id)),
            ),
        }),
      })
      const first = yield* fixture.sessions.forSession(sessionID).resume().pipe(Effect.forkScoped)
      yield* Deferred.await(started)
      const second = yield* fixture.sessions.forSession(sessionID).resume().pipe(Effect.forkScoped)
      yield* Deferred.await(joining)
      yield* Fiber.interrupt(second)

      const cancelled = yield* Fiber.await(second)
      expect(Exit.isFailure(cancelled) && Cause.hasInterruptsOnly(cancelled.cause)).toBe(true)
      expect(yield* coordinator.active).toEqual(new Set([sessionID]))
      expect(drains).toEqual([sessionID])
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(first)
      yield* fixture.sessions.forSession(sessionID).wait()
      expect(drains).toEqual([sessionID])
      expect(yield* coordinator.active).toEqual(new Set())
      expect(yield* fixture.sessions.forSession(sessionID).interrupt({ resume: true })).toBe(false)
      expect(yield* fixture.sessions.forSession(sessionID).interrupt()).toBe(false)
      expect(interrupts).toEqual([
        { sessionID, options: { resume: true } },
        { sessionID, options: undefined },
      ])
      expect(fixture.locations).toEqual([])
    }),
  )

  it.live("keeps preparation interruptible without admitting input or committing a staged revert", () =>
    Effect.gen(function* () {
      const fixture = yield* setup({
        snapshot: () => Layer.mock(Snapshot.Service, { capture: () => Effect.undefined }),
      })
      const handle = fixture.sessions.forSession(sessionID)
      const boundary = yield* handle.synthetic({ text: "Revert boundary", resume: false })
      yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "steer")
      yield* handle.revert.stage({ messageID: boundary.id, files: false })
      const entered = yield* Deferred.make<void>()
      const hook = yield* fixture.hooks.register("session", "prompt", () =>
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      )

      const submission = yield* handle.prompt({ text: "Cancelled before admission" }).pipe(Effect.forkScoped)
      yield* Deferred.await(entered)
      yield* Fiber.interrupt(submission)

      const cancelled = yield* Fiber.await(submission)
      expect(Exit.isFailure(cancelled) && Cause.hasInterruptsOnly(cancelled.cause)).toBe(true)
      expect(yield* handle.inbox()).toEqual([])
      expect((yield* handle.get()).revert?.messageID).toBe(boundary.id)
      expect(yield* fixture.store.context(sessionID)).toMatchObject([{ id: boundary.id }])
      expect(fixture.wakes).toEqual([])
      yield* hook.dispose
      yield* handle.revert.clear()
      expect((yield* handle.get()).revert).toBeUndefined()
      expect(yield* fixture.store.context(sessionID)).toMatchObject([{ id: boundary.id }])
      yield* handle.revert.stage({ messageID: boundary.id, files: false })
      const acquisitions = fixture.locations.length
      yield* fixture.sessions.forSession(sessionID).revert.commit()
      expect((yield* handle.get()).revert).toBeUndefined()
      expect(yield* fixture.store.context(sessionID)).toEqual([])
      expect(fixture.locations).toHaveLength(acquisitions)
    }),
  )

  it.live("selects the destination's snapshot service after a move", () =>
    Effect.gen(function* () {
      const captures: Location.Ref[] = []
      const fixture = yield* setup({
        snapshot: (ref) =>
          Layer.mock(Snapshot.Service, {
            capture: () =>
              Effect.sync(() => {
                captures.push(ref)
                return undefined
              }),
          }),
      })
      const handle = fixture.sessions.forSession(sessionID)
      const boundary = yield* handle.synthetic({ text: "Revert boundary", resume: false })
      yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "steer")
      yield* handle.revert.stage({ messageID: boundary.id, files: false })
      const destination = Location.Ref.make({ directory: AbsolutePath.make("/project/moved") })
      yield* fixture.bus.publish(SessionEvent.Moved, {
        sessionID,
        location: destination,
        projectID: Project.ID.global,
        subpath: RelativePath.make("moved"),
      })

      yield* handle.revert.stage({ messageID: boundary.id, files: false })
      yield* handle.revert.clear()

      expect(captures).toEqual([source, destination])
      expect(fixture.locations).toEqual([source, destination, destination])
      expect(fixture.activationWaits).toEqual([])
      expect((yield* handle.get()).revert).toBeUndefined()
    }),
  )
})

describe("SessionPrompt preparation", () => {
  it.live("prepares repeatable input without admitting it", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const input = { text: "Original", files: [{ uri: new URL("./session-owned.test.ts", import.meta.url).href }] }
      const request = {
        session: yield* fixture.sessions.forSession(sessionID).get(),
        messageID: SessionMessage.ID.create(),
        input,
      }
      const items = yield* Effect.forEach([0, 1], () => SessionPrompt.prepare(request)).pipe(
        Effect.provideService(Instance.Service, fixture.instances),
        Effect.satisfiesServicesType<FSUtil.Service>(),
      )

      expect(items[0]).toEqual(items[1])
      expect(items[0]).toMatchObject({ type: "user", payload: { text: "Original" }, delivery: "steer" })
      expect(items[0]?.payload.files?.[0]?.mime).toBe("text/plain")
      expect(input.text).toBe("Original")
      expect(yield* fixture.sessions.forSession(sessionID).inbox()).toEqual([])
      expect(fixture.wakes).toEqual([])
    }),
  )
})

describe("SessionRevert operations", () => {
  it.live("captures snapshots when staging and restores them when clearing", () =>
    Effect.gen(function* () {
      const calls: string[] = []
      const fixture = yield* setup({
        snapshot: () =>
          Layer.mock(Snapshot.Service, {
            capture: () =>
              Effect.sync(() => {
                calls.push("capture")
                return Snapshot.ID.make("captured-tree")
              }),
            diff: () =>
              Effect.sync(() => {
                calls.push("diff")
                return []
              }),
            restore: () =>
              Effect.sync(() => {
                calls.push("restore")
              }),
          }),
      })
      const handle = fixture.sessions.forSession(sessionID)
      const boundary = yield* handle.synthetic({ text: "Revert boundary", resume: false })
      yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "steer")
      expect(calls).toEqual([])
      const session = yield* handle.get()
      yield* SessionRevert.stage({ session, messageID: boundary.id, files: false }).pipe(
        Effect.provideService(Instance.Service, fixture.instances),
      )
      expect(calls).toEqual(["capture", "capture", "diff"])

      const staged = yield* handle.get()
      expect(staged.revert?.snapshot).toBe(Snapshot.ID.make("captured-tree"))
      yield* SessionRevert.clear(staged).pipe(Effect.provideService(Instance.Service, fixture.instances))
      const cleared = yield* handle.get()
      expect(cleared.revert).toBeUndefined()
      yield* SessionRevert.clear(cleared).pipe(Effect.provideService(Instance.Service, fixture.instances))
      expect(calls).toEqual(["capture", "capture", "diff", "restore"])
    }),
  )
})

describe("SessionInbox command contracts", () => {
  it.live("captures the provided Inbox service when constructing Session", () =>
    Effect.gen(function* () {
      const admission = yield* SessionInbox.Service
      const cancelled: SessionMessage.ID[] = []
      const fixture = yield* setup().pipe(
        Effect.provideService(
          SessionInbox.Service,
          SessionInbox.Service.of({
            ...admission,
            cancel: (input) =>
              admission.cancel(input).pipe(Effect.tap(() => Effect.sync(() => cancelled.push(input.id)))),
          }),
        ),
      )
      const handle = fixture.sessions.forSession(sessionID)
      const pending = yield* handle.synthetic({ text: "Pending", resume: false })

      yield* handle.cancelInbox(pending.id).pipe(Effect.setContext(Context.empty()))

      expect(cancelled).toEqual([pending.id])
      expect(yield* handle.inbox()).toEqual([])
      expect(fixture.wakes).toEqual([])
    }),
  )

  it.live("captures the host dependencies for detached commands", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const { list, admit, reconcile, admitCompaction, cancel, steer, queue } = yield* SessionInbox.Service
      const other = yield* SessionInbox.make()
      expect(yield* SessionInbox.list(fixture.db, sessionID)).toEqual([])

      yield* Effect.gen(function* () {
        expect(yield* list(sessionID)).toEqual([])
        const user = yield* admit({
          id: SessionMessage.ID.create(),
          sessionID,
          item: { type: "user", payload: { text: "Captured services" }, delivery: "queue" },
        })
        expect(yield* reconcile({ id: user.id, sessionID, type: "user", delivery: "queue" })).toEqual(user)
        yield* steer({ id: user.id, sessionID })
        yield* queue({ id: user.id, sessionID })
        yield* cancel({ id: user.id, sessionID })
        const [compaction, duplicate] = yield* Effect.all(
          [
            admitCompaction({ id: SessionMessage.ID.create(), sessionID, delivery: "queue" }),
            other.admitCompaction({ id: SessionMessage.ID.create(), sessionID, delivery: "queue" }),
          ],
          { concurrency: "unbounded" },
        )
        expect(compaction).toEqual(duplicate)
        yield* cancel({ id: compaction.id, sessionID })
        expect(yield* list(sessionID)).toEqual([])
      }).pipe(Effect.satisfiesServicesType<never>(), Effect.setContext(Context.empty()))

      expect(yield* SessionInbox.list(fixture.db, sessionID)).toEqual([])
      expect(fixture.wakes).toEqual([])
    }),
  )

  it.live("returns checked user and synthetic admissions and typed pending or delivered conflicts", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const admission = yield* SessionInbox.Service
      const user = yield* admission
        .admit({
          id: SessionMessage.ID.create(),
          sessionID,
          item: { type: "user", payload: { text: "Keep user input" }, delivery: "steer" },
        })
        .pipe(
          Effect.satisfiesSuccessType<SessionInbox.User>(),
          Effect.satisfiesErrorType<SessionInbox.LifecycleConflict | NotFoundError>(),
        )
      const synthetic = yield* admission
        .admit({
          id: SessionMessage.ID.create(),
          sessionID,
          item: { type: "synthetic", payload: { text: "Keep synthetic input" }, delivery: "steer" },
        })
        .pipe(Effect.satisfiesSuccessType<SessionInbox.Synthetic>())

      yield* Effect.forEach([false, true], (delivered) =>
        Effect.gen(function* () {
          if (delivered) yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "steer")
          const reconciled = yield* admission
            .reconcile({
              id: user.id,
              sessionID,
              type: "user",
              delivery: "steer",
            })
            .pipe(Effect.satisfiesSuccessType<SessionInbox.User | undefined>())
          expect(reconciled).toMatchObject({
            id: user.id,
            sessionID,
            type: "user",
            payload: user.payload,
            delivery: "steer",
          })
          if (!delivered) expect(reconciled).toEqual(user)

          yield* Effect.forEach([user, synthetic], (original) =>
            Effect.gen(function* () {
              expect(
                yield* admission.admit({
                  id: original.id,
                  sessionID,
                  item: { type: original.type, payload: { text: "Ignored retry" }, delivery: "queue" },
                }),
              ).toMatchObject({ id: original.id, sessionID, type: original.type, payload: original.payload })
              yield* Effect.forEach(
                [
                  { sessionID: otherID, type: original.type },
                  { sessionID, type: original.type === "user" ? ("synthetic" as const) : ("user" as const) },
                ],
                (conflict) =>
                  Effect.gen(function* () {
                    expect(
                      yield* admission
                        .reconcile({
                          ...conflict,
                          id: original.id,
                          delivery: "steer",
                        })
                        .pipe(Effect.flip),
                    ).toBeInstanceOf(SessionInbox.LifecycleConflict)
                    expect(
                      yield* admission
                        .admit({
                          id: original.id,
                          sessionID: conflict.sessionID,
                          item: { type: conflict.type, payload: { text: "Conflicting input" }, delivery: "steer" },
                        })
                        .pipe(Effect.flip),
                    ).toMatchObject({ _tag: "SessionInbox.LifecycleConflict", id: original.id })
                  }),
              )
            }),
          )
        }),
      )
      expect(fixture.locations).toEqual([])
      expect(fixture.wakes).toEqual([])
    }),
  )

  it.live("checks the winner of concurrent admissions before returning it", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const admission = yield* SessionInbox.Service
      const other = yield* SessionInbox.make()
      const id = SessionMessage.ID.create()
      const requests = [
        { sessionID, item: { type: "user", payload: { text: "First" }, delivery: "steer" } },
        { sessionID, item: { type: "user", payload: { text: "Retry" }, delivery: "queue" } },
        { sessionID, item: { type: "synthetic", payload: { text: "Other type" }, delivery: "steer" } },
        { sessionID: otherID, item: { type: "user", payload: { text: "Other Session" }, delivery: "steer" } },
      ] satisfies Array<{ sessionID: SessionSchema.ID; item: SessionInbox.Item }>
      const results = yield* Effect.forEach(
        requests,
        (request, index) => (index % 2 === 0 ? admission : other).admit({ id, ...request }).pipe(Effect.exit),
        { concurrency: "unbounded" },
      )
      const stored = yield* SessionInbox.find(fixture.db, id)
      expect(stored).toBeDefined()
      expect(results.some(Exit.isSuccess)).toBe(true)
      results.forEach((result, index) => {
        if (Exit.isSuccess(result)) {
          expect(stored).toEqual(result.value)
          expect(result.value.sessionID).toBe(requests[index]?.sessionID)
          expect(result.value.type).toBe(requests[index]?.item.type)
          return
        }
        expect(Cause.hasDies(result.cause)).toBe(false)
        expect(Cause.hasFails(result.cause)).toBe(true)
      })
      expect(
        (yield* SessionInbox.list(fixture.db, sessionID)).length +
          (yield* SessionInbox.list(fixture.db, otherID)).length,
      ).toBe(1)
    }),
  )

  it.live("exposes failed pending transitions as typed conflicts and rolls back their events", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const admission = yield* SessionInbox.Service
      const pending = yield* admission.admit({
        id: SessionMessage.ID.create(),
        sessionID,
        item: { type: "user", payload: { text: "Pending" }, delivery: "queue" },
      })
      const input = { id: pending.id, sessionID }
      yield* Effect.forEach([admission.cancel, admission.steer, admission.queue], (mutation) =>
        Effect.gen(function* () {
          expect(yield* mutation({ ...input, sessionID: otherID }).pipe(Effect.flip)).toMatchObject({
            _tag: "SessionInbox.LifecycleConflict",
            id: pending.id,
          })
        }),
      )
      yield* admission.steer(input)
      expect(yield* admission.steer(input).pipe(Effect.flip)).toBeInstanceOf(SessionInbox.LifecycleConflict)
      yield* admission.queue(input)
      expect(yield* admission.queue(input).pipe(Effect.flip)).toBeInstanceOf(SessionInbox.LifecycleConflict)
      yield* admission.cancel(input)
      expect(yield* admission.cancel(input).pipe(Effect.flip)).toBeInstanceOf(SessionInbox.LifecycleConflict)
      expect(yield* SessionInbox.list(fixture.db, sessionID)).toEqual([])
      expect(
        (yield* fixture.db
          .select({ type: EventTable.type })
          .from(EventTable)
          .where(eq(EventTable.aggregate_id, sessionID))
          .orderBy(EventTable.seq)
          .all()
          .pipe(Effect.orDie))
          .filter((event) => event.type.startsWith("session.inbox."))
          .map((event) => event.type),
      ).toEqual([
        Bus.versionedType(SessionEvent.InboxEnqueued.type, 1),
        Bus.versionedType(SessionEvent.InboxDeliveryChanged.type, 1),
        Bus.versionedType(SessionEvent.InboxDeliveryChanged.type, 1),
        Bus.versionedType(SessionEvent.InboxCancelled.type, 1),
      ])
    }),
  )

  it.live("does not turn unrelated projector defects into conflicts", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const admission = yield* SessionInbox.Service
      const pending = yield* admission.admit({
        id: SessionMessage.ID.create(),
        sessionID,
        item: { type: "user", payload: { text: "Pending" }, delivery: "queue" },
      })
      const defect = new Error("Projector failed")
      yield* fixture.bus.project(SessionEvent.InboxEnqueued, () => Effect.die(defect))
      yield* fixture.bus.project(SessionEvent.InboxCancelled, () => Effect.die(defect))
      expect(
        yield* admission
          .admit({
            id: SessionMessage.ID.create(),
            sessionID,
            item: { type: "user", payload: { text: "Rolled back" }, delivery: "steer" },
          })
          .pipe(Effect.catchDefect(Effect.succeed)),
      ).toBe(defect)
      expect(yield* admission.cancel({ id: pending.id, sessionID }).pipe(Effect.catchDefect(Effect.succeed))).toBe(
        defect,
      )
      expect(yield* SessionInbox.list(fixture.db, sessionID)).toEqual([pending])
    }),
  )
  it.live(
    "claims park prepared pending inputs and committed retries never readmit or restore settled credentials",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup()
        const handle = fixture.sessions.forSession(sessionID)
        for (const suffix of ["first", "second"]) {
          const id = SessionMessage.ID.make("msg_claim_" + suffix)
          const mcp = {
            host: {
              type: "remote" as const,
              url: "https://host.example.test",
              headers: { authorization: "Bearer synthetic" },
            },
          }
          yield* handle.prompt({ id, text: "original", mcp, mcpRevision: 1, resume: false })
          expect(yield* SessionExecutionCapability.ready(sessionID, id)).toBe(true)
          const claim = "claim_" + suffix
          const receipt = yield* handle.beginInboxCapabilityRenewal(id, claim)
          expect(receipt.claim).toBe(claim)
          expect(yield* handle.beginInboxCapabilityRenewal(id, claim)).toEqual(receipt)
          expect(yield* handle.beginInboxCapabilityRenewal(id, claim + "_other").pipe(Effect.flip)).toBeInstanceOf(
            InboxConflictError,
          )
          expect(yield* SessionExecutionCapability.ready(sessionID, id)).toBe(false)
          expect(
            yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "input", () => Effect.succeed(true)),
          ).toBe("blocked")
          expect(yield* handle.message(id)).toBeUndefined()
          expect((yield* handle.inbox()).map((item) => item.id)).toEqual([id])
          expect(
            yield* handle.renewInboxCapability(id, { mcp, revision: 2, claim: "wrong_claim" }).pipe(Effect.flip),
          ).toBeInstanceOf(InboxConflictError)
          expect(yield* SessionExecutionCapability.set(sessionID, { inputID: id, mcp, revision: 2 })).toBe(false)
          yield* handle.renewInboxCapability(id, { mcp, revision: 2, claim })
          expect(yield* SessionExecutionCapability.ready(sessionID, id)).toBe(true)
          // A delayed duplicate begin must not suspend its committed grant again.
          yield* handle.beginInboxCapabilityRenewal(id, claim)
          expect(yield* SessionExecutionCapability.ready(sessionID, id)).toBe(true)
          expect(yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "input")).toBe(1)
          expect(yield* handle.beginInboxCapabilityRenewal(id, "delivered_claim").pipe(Effect.flip)).toBeInstanceOf(
            InboxConflictError,
          )
          yield* SessionExecutionCapability.retain(sessionID, new Set())
          yield* handle.renewInboxCapability(id, { mcp, revision: 2, claim })
          expect(yield* SessionExecutionCapability.ready(sessionID, id)).toBe(false)
          expect(yield* handle.inbox()).toEqual([])
          expect((yield* handle.message(id))?.id).toBe(id)
          expect(
            yield* handle.renewInboxCapability(id, { mcp: {}, revision: 2, claim }).pipe(Effect.flip),
          ).toBeInstanceOf(InboxConflictError)
        }
        expect(fixture.wakes).toEqual([])
      }),
  )

  it.live("delivery winning before a renewal claim preserves the installed credential", () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const handle = fixture.sessions.forSession(sessionID)
      const id = SessionMessage.ID.make("msg_claim_delivery_wins")
      const mcp = { host: { type: "remote" as const, url: "https://host.example.test" } }
      yield* handle.prompt({ id, text: "original", mcp, mcpRevision: 4, resume: false })
      expect(yield* SessionInbox.promote(fixture.db, fixture.bus, sessionID, "input")).toBe(1)
      expect(yield* handle.beginInboxCapabilityRenewal(id, "too_late").pipe(Effect.flip)).toBeInstanceOf(
        InboxConflictError,
      )
      expect((yield* SessionExecutionCapability.get(sessionID, id))?.revision).toBe(4)
    }),
  )
})
