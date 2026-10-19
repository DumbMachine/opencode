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
import { Event } from "@opencode/schema/event"
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
import { toolPublicationHost } from "./fixture/tool-publication-host"
import { NativeProducer } from "@opencode/core/execution-producer"
import { Tool } from "@opencode/core/tool"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Image } from "@opencode/core/image"
import { SessionPrompt } from "@opencode/core/session/prompt"
import { Instance } from "@opencode/core/instance/service"
import { Plugin } from "@opencode/core/plugin/service"
import { FSUtil } from "@opencode/util/fs-util"
import { ToolProvenance } from "@opencode/core/tool/provenance"
import { LLM, Media, Message } from "@opencode/ai"
import { withinInlineRequestLimit } from "@opencode/core/session/model-request"
import { OpenAIChat } from "@opencode/ai/protocols"
import { DateTime } from "effect"
import { SessionDisclosure } from "@opencode/core/session/disclosure"
import { SessionInbox as SessionInboxSchema } from "@opencode/schema/session-inbox"
import { interruptedToolError } from "@opencode/core/session/tool-interruption"

const graph = LayerNode.group([Database.node, Bus.node, SessionProjector.node, SessionStore.node, SessionInbox.node])
const it = testEffect(AppNodeBuilder.build(graph, [Bus.node.replace(Bus.configured({ persist: true }))]))
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
  if (false) {
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
    units: ExecutionDisclosure.ToolPublicationRequest[]
    contexts: Array<ExecutionDisclosure.ContextRequest>
    ttl: number
    contextReference: string
    deadlines: Map<number, number>
    expired: Set<number>
    request?: () => Effect.Effect<void>
    preflight?: () => Effect.Effect<void>
    mutate?: (proof: ExecutionDisclosure.ToolPublicationProof) => ExecutionDisclosure.ToolPublicationProof
  } = {
    active: true,
    calls: [],
    units: [],
    contexts: [],
    ttl: 30_000,
    contextReference: "1".repeat(64),
    deadlines: new Map(),
    expired: new Set(),
  }
  const host = toolPublicationHost({
    context: (proof) => ({ ...proof, contextReference: state.contextReference }),
    ttl: () => state.ttl,
    observe: async (unit, authorization) => {
      expect(authorization).toBe("Bearer private-original-token")
      state.calls.push({ unitID: unit.unitID, unitDigest: unit.unitDigest })
      state.units.push(structuredClone(unit))
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
  yield* database.db
    .update(SessionTable)
    .set({ context_policy: { version: 2 } })
    .where(eq(SessionTable.id, sessionID))
    .run()
    .pipe(Effect.orDie)
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
  const make = () =>
    prepare().pipe(
      Effect.flatMap((context) =>
        Effect.gen(function* () {
          const writer = context.writer
          yield* (yield* writer.prepare(SessionEvent.Step.Started, {
            sessionID,
            assistantMessageID,
            agent: Agent.ID.make("build"),
            model: { id: Model.ID.make("fixture"), providerID: Provider.ID.make("fixture") },
            started: 1,
          })).publish
          yield* (yield* writer.prepare(SessionEvent.Text.Started, { sessionID, assistantMessageID, ordinal: 0 }))
            .publish
          state.calls.length = 0
          return writer
        }),
      ),
    )
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

const startTool = Effect.fnUntraced(function* (
  f: Effect.Success<ReturnType<typeof fixture>>,
  writer: SessionPublicationWriter.Writer,
  id: string,
) {
  if (!writer.prepareTool) throw new Error("Missing tool writer")
  const identity = { sessionID: f.sessionID, assistantMessageID: f.assistantMessageID, id }
  yield* (yield* writer.prepareTool(SessionEvent.Tool.Input.Started, { ...identity, name: "lookup" })).publish
  yield* (yield* writer.prepareTool(SessionEvent.Tool.Input.Ended, { ...identity, text: "{}" })).publish
})

for (const operation of ["called", "success", "failed"] as const) {
  it.live(`admits a durable pub2 tool ${operation} using its exact authored envelope`, () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      const writer = yield* f.make()
      if (!writer.prepareTool) throw new Error("Missing tool writer")
      yield* startTool(f, writer, "call-first")
      const identity = { sessionID: f.sessionID, assistantMessageID: f.assistantMessageID, id: "call-first" }
      const called = yield* writer.prepareTool(SessionEvent.Tool.Called, {
        ...identity,
        input: { value: "request" },
        executed: false,
      })
      yield* called.publish
      if (operation === "success") {
        const unit = yield* writer.prepareTool(SessionEvent.Tool.Success, {
          ...identity,
          content: [{ type: "text", text: "tool value" }],
          executed: false,
        })
        yield* unit.publish
      }
      if (operation === "failed") {
        const unit = yield* writer.prepareTool(SessionEvent.Tool.Failed, {
          ...identity,
          error: { type: "tool.execution", message: "tool failure" },
          executed: false,
        })
        yield* unit.publish
      }
      expect((yield* f.witnesses()).length).toBe(operation === "called" ? 5 : 6)
      expect((yield* f.message())?.publication_provenance?.version).toBe(2)
    }),
  )
}

it.live("merges independently signed pub2 admissions in one atomic tool batch and rejects a changed member", () =>
  Effect.gen(function* () {
    const f = yield* fixture()
    const writer = yield* f.make()
    if (!writer.toolScope) throw new Error("Missing tool scope")
    yield* startTool(f, writer, "one")
    yield* startTool(f, writer, "two")
    f.state.calls.length = 0
    const item = (id: string) =>
      [
        SessionEvent.Tool.Called,
        {
          sessionID: f.sessionID,
          assistantMessageID: f.assistantMessageID,
          id,
          input: { id },
          executed: false,
        },
        { id: Event.ID.create(), created: Date.now(), location: { directory: AbsolutePath.make("/project") } },
      ] as const
    yield* writer.toolScope(f.bus.publishAllOnce([item("one"), item("two")]))
    expect((yield* f.witnesses()).length).toBe(8)
    expect(f.state.calls).toHaveLength(2)
    const before = yield* f.message()
    f.state.mutate = (proof) => ({ ...proof, unitDigest: "0".repeat(64) })
    expect(
      Exit.isFailure(yield* writer.toolScope(f.bus.publishAllOnce([item("three"), item("four")])).pipe(Effect.exit)),
    ).toBeTrue()
    expect((yield* f.message())?.data).toEqual(before?.data)
  }),
)

it.live("keeps long text streams and a tool reply in bounded private source groups", () =>
  Effect.gen(function* () {
    const f = yield* fixture()
    const writer = yield* f.make()
    for (let index = 0; index < 150; index++) {
      const unit = yield* writer.prepare(SessionEvent.Text.Delta, {
        sessionID: f.sessionID,
        assistantMessageID: f.assistantMessageID,
        ordinal: 0,
        delta: "x",
      })
      yield* unit.publish
    }
    if (!writer.prepareTool) throw new Error("Missing tool writer")
    yield* startTool(f, writer, "reply")
    yield* (yield* writer.prepareTool(SessionEvent.Tool.Called, {
      sessionID: f.sessionID,
      assistantMessageID: f.assistantMessageID,
      id: "reply",
      input: {},
      executed: false,
    })).publish
    yield* (yield* writer.prepareTool(SessionEvent.Tool.Success, {
      sessionID: f.sessionID,
      assistantMessageID: f.assistantMessageID,
      id: "reply",
      content: [{ type: "text", text: "reply" }],
      executed: false,
    })).publish
    const row = yield* f.message()
    expect(row?.publication_provenance?.version).toBe(2)
    expect(row?.publication_provenance?.references).toHaveLength(1)
    const message = Schema.decodeUnknownSync(SessionMessage.Info)({ ...row!.data, id: row!.id, type: row!.type })
    expect(row!.publication_provenance!.revision).toBe(SessionPublicationProvenance.revision(message))
  }),
)

const registry = testEffect(
  AppNodeBuilder.build(LayerNode.group([Tool.node, PluginHooks.node]), [
    Image.node.replace(Layer.mock(Image.Service, { normalize: () => Effect.die("unused image normalization") })),
  ]),
)
registry.effect("does not reuse same-name CodeMode catalogs across request actor definitions", () =>
  Effect.gen(function* () {
    const tools = yield* Tool.Service
    const make = (description: string, field: string): Tool.Info => ({
      name: "same",
      description,
      input: { type: "object", properties: { [field]: { type: "string" } } },
      execute: () => Effect.succeed({ content: "unused" }),
    })
    const first = yield* tools.snapshot([], { tools: [make("Actor one definition", "one")] })
    const second = yield* tools.snapshot([], { tools: [make("Actor two definition", "two")] })
    expect(first.codeModeCatalog).not.toEqual(second.codeModeCatalog)
    expect(JSON.stringify(first.codeModeCatalog)).toContain("Actor one definition")
    expect(JSON.stringify(second.codeModeCatalog)).toContain("Actor two definition")
    expect(JSON.stringify(first.codeModeCatalog)).not.toContain("Actor two definition")
  }),
)

it.live("captures every actual LLM position when one assistant row contains tool calls and replies", () =>
  Effect.gen(function* () {
    const f = yield* fixture()
    const writer = yield* f.make()
    if (!writer.prepareTool) throw new Error("Missing tool writer")
    yield* startTool(f, writer, "multi-position")
    const identity = { sessionID: f.sessionID, assistantMessageID: f.assistantMessageID, id: "multi-position" }
    yield* (yield* writer.prepareTool(SessionEvent.Tool.Called, { ...identity, input: { number: 2 }, executed: false }))
      .publish
    yield* (yield* writer.prepareTool(SessionEvent.Tool.Success, {
      ...identity,
      content: [{ type: "text", text: "4" }],
      executed: false,
    })).publish
    const row = (yield* f.message())!
    const assistant = Schema.decodeUnknownSync(SessionMessage.Info)({ ...row.data, id: row.id, type: row.type })
    const prepared = yield* NativeProducer.prepareInput({
      sessionID: f.sessionID,
      inputID: f.inputID,
      original: { text: "Calculate" },
      payload: SessionInboxSchema.UserPayload.make({ text: "Calculate" }),
      invoked: 0,
      inlineIdentity: true,
    })
    if (!prepared?.preparedInput) throw new Error("Missing prepared input")
    const user = SessionMessage.User.make({
      ...prepared,
      id: f.inputID,
      type: "user",
      time: { created: DateTime.makeUnsafe(0) },
    })
    const messages = [user, assistant]
    const ref = { id: Model.ID.make("fixture"), providerID: Provider.ID.make("fixture") }
    const tools = ToolProvenance.recordSnapshot(
      { definitions: [], execute: () => Effect.die("unused tool execution") },
      true,
      [],
    )
    const producer = NativeProducer.captureToolContext({
      sessionID: f.sessionID,
      inputID: f.inputID,
      model: ref,
      providerMetadataKey: "fixture",
      messages,
      provenance: [{ id: assistant.id, state: row.publication_provenance }],
      tools,
      system: [],
    })
    const rendered = toLLMMessages(messages, ref, "fixture", true, true)
    const request = LLM.request({
      model: OpenAIChat.route.model({ id: "fixture", provider: "fixture" }),
      messages: rendered,
      system: [],
      tools: [],
    })
    const observed = yield* producer.observe(request, 0)
    const composition = JSON.parse(observed.composition!.payload)
    expect(composition.complete).toBeTrue()
    expect(composition.messageCount).toBe(rendered.length)
    const components = observed.components.filter((component) => component.kind === "message")
    expect(components.map((component) => JSON.parse(component.proof!).index)).toEqual(rendered.map((_, index) => index))
    expect(components.filter((component) => component.id === assistant.id).length).toBeGreaterThan(1)
    const changed = yield* producer.observe(LLM.request({ ...request, messages: [...rendered].reverse() }), 0)
    expect(JSON.parse(changed.composition!.payload).complete).toBeFalse()
  }),
)

registry.effect("never classifies missing or fabricated request catalog evidence as complete", () =>
  Effect.gen(function* () {
    const tools = yield* Tool.Service
    const execution = {}
    const external = ToolProvenance.catalogDefinition(
      {
        name: "lookup",
        description: "Observed tool",
        input: { type: "object" },
        execute: () => Effect.succeed({ content: "unused" }),
      },
      { execution, server: "actor-server", tool: "lookup", revision: "1".repeat(64) },
    )
    const snapshot = yield* tools.snapshot([], { tools: [external] })
    const sessionID = Session.ID.make("ses_catalog_coverage"),
      inputID = SessionMessage.ID.make("msg_catalog_coverage")
    const model = OpenAIChat.route.model({ id: "fixture", provider: "fixture" })
    for (const catalog of [undefined, { observations: [], complete: true, sessionID, inputID }]) {
      const producer = NativeProducer.captureToolContext({
        sessionID,
        inputID,
        model: { id: Model.ID.make("fixture"), providerID: Provider.ID.make("fixture") },
        providerMetadataKey: "fixture",
        messages: [],
        provenance: [],
        tools: snapshot,
        system: [],
        catalog,
      })
      const observed = yield* producer.observe(
        LLM.request({
          model,
          messages: [Message.make({ role: "user", content: [{ type: "text", text: "Unknown history" }] })],
          system: [],
          tools: snapshot.definitions,
        }),
        0,
      )
      const definitions = observed.components.find((component) => component.kind === "tools")!
      expect(JSON.parse(JSON.parse(definitions.proof!).payload).complete).toBeFalse()
    }
  }),
)

it.live("rejects managed prompt hooks before invoking them and preserves ordinary prompt hooks", () =>
  Effect.gen(function* () {
    const f = yield* fixture()
    const store = yield* SessionStore.Service
    const session = (yield* store.get(f.sessionID))!
    yield* Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      let invoked = 0
      yield* hooks.register("session", "prompt", () =>
        Effect.sync(() => {
          invoked++
        }),
      )
      const request = { session, messageID: f.inputID, input: { text: "Exact prompt" } }
      expect(Exit.isFailure(yield* SessionPrompt.prepare(request).pipe(Effect.exit))).toBeTrue()
      expect(invoked).toBe(0)
      const ordinary = yield* SessionPrompt.prepare({ ...request, session: { ...session, contextPolicy: undefined } })
      expect(ordinary.payload.text).toBe("Exact prompt")
      expect(invoked).toBe(1)
    }).pipe(
      Effect.provide(AppNodeBuilder.build(LayerNode.group([PluginHooks.node, FSUtil.node]))),
      Effect.provide(Layer.mock(Plugin.Service, { awaitActivation: Effect.void })),
      Effect.provideService(Instance.Service, { provide: () => (effect) => effect as never }),
    )
  }),
)

it.live(
  "settles a retained pending tool only after fresh preflight and publishes fixed source-bound interruption bytes",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      const first = yield* f.make()
      yield* startTool(f, first, "pending")
      yield* (yield* first.prepareTool!(SessionEvent.Tool.Called, {
        sessionID: f.sessionID,
        assistantMessageID: f.assistantMessageID,
        id: "pending",
        input: {},
        executed: false,
      })).publish
      const row = (yield* f.message())!
      const assistant = Schema.decodeUnknownSync(SessionMessage.Info)({ ...row.data, id: row.id, type: row.type })
      const prepared = yield* NativeProducer.prepareInput({
        sessionID: f.sessionID,
        inputID: f.inputID,
        original: { text: "Continue" },
        payload: SessionInboxSchema.UserPayload.make({ text: "Continue" }),
        invoked: 0,
        inlineIdentity: true,
      })
      if (!prepared) throw new Error("Missing input")
      const user = SessionMessage.User.make({
        ...prepared,
        id: f.inputID,
        type: "user",
        time: { created: DateTime.makeUnsafe(0) },
      })
      const ref = { id: Model.ID.make("fixture"), providerID: Provider.ID.make("fixture") }
      let executions = 0
      const tools = ToolProvenance.recordSnapshot(
        {
          definitions: [],
          execute: () =>
            Effect.sync(() => {
              executions++
              throw new Error("must not execute during settlement")
            }),
        },
        true,
        [],
      )
      const producer = NativeProducer.captureToolContext({
        sessionID: f.sessionID,
        inputID: f.inputID,
        model: ref,
        providerMetadataKey: "fixture",
        messages: [user, assistant],
        provenance: [{ id: assistant.id, state: row.publication_provenance }],
        tools,
        system: [],
      })
      const rendered = toLLMMessages([user, assistant], ref, "fixture", true, true)
      expect(JSON.stringify(rendered)).toContain("inspect its outcome before retrying")
      expect(JSON.stringify(toLLMMessages([user, assistant], ref, "fixture", true))).not.toContain(
        "inspect its outcome before retrying",
      )
      const observed = yield* producer.observe(
        LLM.request({
          model: OpenAIChat.route.model({ id: "fixture", provider: "fixture" }),
          system: [],
          tools: [],
          messages: rendered,
        }),
        0,
      )
      expect(NativeProducer.capturePendingTools(structuredClone(observed))).toBeUndefined()
      const selected = yield* f.capture()
      const current = yield* selected.prepareRequest(observed)
      if (!current.writer.settleInterrupted) throw new Error("Missing settlement writer")
      yield* current.writer.settleInterrupted()
      const unit = f.state.units.at(-1)!
      expect(unit.kind).toBe("tool-control")
      expect(JSON.parse(unit.projection!.payload)).toMatchObject({
        messageID: assistant.id,
        toolCallID: "pending",
        complete: true,
        parentReceipts: [],
        projectionRevision: unit.unitDigest,
      })
      const settled = (yield* f.message())!
      const message = Schema.decodeUnknownSync(SessionMessage.Info)({
        ...settled.data,
        id: settled.id,
        type: settled.type,
      })
      if (message.type !== "assistant") throw new Error("Missing assistant")
      const tool = message.content.find((part) => part.type === "tool")
      expect(tool).toMatchObject({
        state: { status: "error", error: interruptedToolError },
      })
      const count = f.state.units.length
      yield* current.writer.settleInterrupted()
      expect(f.state.units).toHaveLength(count)
      expect(executions).toBe(0)
      expect(toLLMMessages([user, message], ref, "fixture", true, true)).toEqual(rendered)
    }),
)

test("bounds human and nested tool-result media together before physical request I/O", () => {
  const encoded = Buffer.alloc(21 * 1024 * 1024).toString("base64")
  const human = Message.make({
    role: "user",
    content: [
      { type: "media", media: Media.base64(encoded, "image/png") },
    ],
  })
  const tool = (uri: string) =>
    Message.make({
      role: "tool",
      content: [
        {
          type: "tool-result",
          id: "media",
          name: "lookup",
          result: { type: "content", value: [{ type: "file", mime: "image/png", uri }] },
        },
      ],
    })
  expect(withinInlineRequestLimit([human])).toBeTrue()
  expect(withinInlineRequestLimit([tool("data:image/png;base64,eA==")])).toBeTrue()
  expect(withinInlineRequestLimit([human, tool("data:image/png;base64," + encoded)])).toBeFalse()
  for (const uri of [
    "file:///private/data.png",
    "https://example.invalid/image.png",
    "data:image/png;base64,eA",
    "data:image/png;base64,eB==",
  ])
    expect(withinInlineRequestLimit([tool(uri)])).toBeFalse()
})
