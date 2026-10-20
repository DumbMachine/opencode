import { expect } from "bun:test"
import { Model } from "@opencode/core/model"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { ModelResolver } from "@opencode/core/model-resolver"
import { NativeProducer } from "@opencode/core/execution-producer"
import { SessionDisclosure } from "@opencode/core/session/disclosure"
import { toLLMMessages } from "@opencode/core/session/runner/to-llm-message"
import { ToolProvenance } from "@opencode/core/tool/provenance"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Database } from "@opencode/core/database/database"
import { Bus } from "@opencode/core/bus"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionStore } from "@opencode/core/session/store"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { PluginInternal } from "@opencode/core/plugin/internal"
import { Plugin } from "@opencode/core/plugin"
import { Location } from "@opencode/core/location"
import { tempLocationLayer } from "./fixture/location"
import { offlineModels } from "./fixture/models"
import { SessionModelRequest } from "@opencode/core/session/model-request"
import { SessionModelTransport } from "@opencode/core/session/model-transport"
import { SessionRunCoordinator } from "@opencode/core/session/run-coordinator"
import { SessionExecutionCapability } from "@opencode/core/session/execution-capability"
import { SessionTable } from "@opencode/core/session/sql"
import { ProjectTable } from "@opencode/core/project/sql"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { SessionMessage } from "@opencode/schema/session-message"
import { Session } from "@opencode/schema/session"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { Provider } from "@opencode/schema/provider"
import { Agent } from "@opencode/schema/agent"
import { Project } from "@opencode/schema/project"
import { Money } from "@opencode/schema/money"
import { AbsolutePath } from "@opencode/schema/schema"
import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { DateTime, Effect } from "effect"
import { testEffect } from "./lib/effect"
import { toolPublicationHost } from "./fixture/tool-publication-host"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      SessionProjector.node,
      SessionStore.node,
      PluginHooks.node,
      PluginInternal.requirements,
      Plugin.node,
    ]),
    [Bus.node.replace(Bus.configured({ persist: true })), offlineModels, Location.node.replace(tempLocationLayer)],
  ),
)

for (const scenario of ["compiled-route", "external-route", "other-provider", "changed-message"] as const) {
  it.live(`keeps provider routing provenance bounded: ${scenario}`, () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const hooks = yield* PluginHooks.Service
      const credential = Credential.OAuth.make({
        type: "oauth",
        methodID: Integration.MethodID.make("chatgpt-browser"),
        access: "synthetic-access",
        refresh: "synthetic-refresh",
        expires: Date.now() + 60_000,
        metadata: { accountID: "synthetic-account", server: "https://synthetic.example", orgID: "synthetic-org" },
      })
      const credentials = yield* Credential.Service
      yield* credentials.create({ integrationID: Integration.ID.make("openai"), value: credential })
      const registry = yield* Plugin.Service
      const internal = yield* PluginInternal.list()
      const openai = internal.pre.find((plugin) => plugin.id === "opencode.provider.openai")
      if (!openai) throw new Error("Missing native OpenAI plugin")
      yield* registry.activate([{ ...openai, revision: "controls-test" }])
      const sessionID = Session.ID.create()
      const inputID = SessionMessage.ID.create()
      const session = Session.Info.make({
        id: sessionID,
        projectID: Project.ID.global,
        contextPolicy: { version: 2 },
        cost: Money.USD.zero,
        tokens: {
          input: 0,
          output: 0,
          reasoning: 0,
          cache: { read: 0, write: 0 },
        },
        time: {
          created: DateTime.makeUnsafe(0),
          updated: DateTime.makeUnsafe(0),
        },
        location: { directory: AbsolutePath.make("/project") },
      })
      yield* db
        .insert(ProjectTable)
        .values({
          id: Project.ID.global,
          worktree: AbsolutePath.make("/project"),
          sandboxes: [],
        })
        .onConflictDoNothing()
        .run()
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "provider-provenance",
          directory: "/project",
          version: "test",
          context_policy: { version: 2 },
        })
        .run()
      let posted: ExecutionDisclosure.ContextRequest | undefined
      const host = toolPublicationHost({
        observeContext: (request) => {
          posted = request
        },
      })
      yield* Effect.addFinalizer(() => Effect.sync(() => host.server.stop(true)))
      const service = host.config
      yield* SessionExecutionCapability.set(sessionID, {
        inputID,
        disclosure: service,
        mcp: {},
        revision: 7,
      })
      yield* SessionExecutionCapability.select(sessionID, inputID)
      yield* Effect.addFinalizer(() => SessionExecutionCapability.clear(sessionID))
      const captured = yield* SessionExecutionCapability.getWithSelection(sessionID, inputID)
      const preparedInput = yield* NativeProducer.prepareInput({
        sessionID,
        inputID,
        original: { text: "Original input" },
        payload: SessionInbox.UserPayload.make({ text: "Original input" }),
        invoked: 0,
        inlineIdentity: true,
      })
      if (!preparedInput) throw new Error("Missing input provenance")
      const message = SessionMessage.User.make({
        ...preparedInput,
        id: inputID,
        type: "user",
        time: { created: DateTime.makeUnsafe(0) },
      })
      const catalog = {
        ...Model.Info.default(Provider.ID.openai, Model.ID.make("gpt-6.1-sol")),
        package: "@opencode/ai/providers/openai",
        settings: {},
        variants: [{ id: Model.VariantID.make("medium"), settings: { reasoningEffort: "medium" } }],
      }
      const selected = yield* ModelResolver.withVariant(catalog, Model.VariantID.make("medium"))
      const model = SessionRunnerModel.resolved(yield* ModelResolver.fromCatalogModel(selected, credential), {
        capabilities: { tools: true, input: ["text"], output: ["text"] },
        variant: Model.VariantID.make("medium"),
        cost: [],
        limit: { context: 100_000, output: 1000 },
      })
      const observed = yield* hooks.triggerObserved("session", "model.request", {
        sessionID,
        agent: Agent.ID.make("build"),
        model: model.ref,
        kind: "primary",
        baseURL: "https://api.openai.com/v1",
        headers: {},
      })
      expect(observed.invoked).toBe(1)
      expect(observed.untracked).toBe(0)
      if (scenario === "external-route")
        yield* hooks.register(
          "session",
          "model.request",
          (event) =>
            Effect.sync(() => {
              event.baseURL = "https://routing.example.test/v1"
            }),
          { providerID: Provider.ID.openai },
        )
      if (scenario === "other-provider")
        yield* hooks.register(
          "session",
          "model.request",
          (event) =>
            Effect.sync(() => {
              event.baseURL = "https://unrelated.example.test/v1"
            }),
          { providerID: Provider.ID.make("unrelated") },
        )
      const tools = ToolProvenance.recordSnapshot({ definitions: [], execute: () => Effect.die("unused") }, true, [])
      const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer))
      const messages = toLLMMessages([message], model.ref, "openai", true, true)
      const prepared = yield* requests.primary({
        session,
        inputID,
        agent: Agent.ID.make("build"),
        model,
        system: [],
        messages:
          scenario === "changed-message"
            ? [
                {
                  role: "user",
                  content: [{ type: "text", text: "Unobserved replacement" }],
                },
              ]
            : messages,
        disclosure: service,
        disclosureRevision: 7,
        disclosureCurrent: captured.current,
        disclosureMessages: [message],
        disclosureProvenance: [],
        tools,
      })
      if (!posted?.composition) throw new Error("Missing signed composition")
      expect(JSON.parse(posted.composition.payload).complete).toBe(
        scenario === "compiled-route" || scenario === "other-provider",
      )
      expect(posted.contextDigest).toBe(SessionDisclosure.digest(SessionDisclosure.logicalContext(prepared.request)))
      expect(prepared.request.providerOptions).toMatchObject({ store: false, reasoningEffort: "medium" })
    }).pipe(
      Effect.provideService(
        SessionModelTransport.Service,
        SessionModelTransport.Service.of({
          bind: () => ({ execute: () => Effect.die("unused") }),
          close: () => Effect.void,
          closeAll: Effect.void,
        }),
      ),
      Effect.provideService(SessionRunCoordinator.Ownership, {
        live: () => true,
      }),
    ),
  )
}
