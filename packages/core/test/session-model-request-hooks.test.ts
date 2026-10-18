import { describe, expect } from "bun:test"
import { OpenAIChat } from "@opencode/ai/protocols"
import { compileRequest } from "@opencode/ai/route/client"
import { Message, SystemPart, ToolDefinition, Media } from "@opencode/ai"
import { SessionDisclosure } from "@opencode/core/session/disclosure"
import { disclosureHost } from "./fixture/disclosure-host"
import { Agent } from "@opencode/schema/agent"
import { Money } from "@opencode/schema/money"
import { SessionMessage } from "@opencode/schema/session-message"
import { Session } from "@opencode/schema/session"
import type { SessionRequestKind } from "@opencode/plugin/effect/session"
import { Location } from "@opencode/core/location"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Project } from "@opencode/core/project"
import { AbsolutePath } from "@opencode/core/schema"
import { SessionModelRequest } from "@opencode/core/session/model-request"
import { SessionModelTransport } from "@opencode/core/session/model-transport"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { DateTime, Effect, Fiber, Stream } from "effect"
import { HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { testEffect } from "./lib/effect"
import { PluginTestLayer } from "./plugin/fixture"

const it = testEffect(PluginTestLayer)

const KINDS: ReadonlyArray<SessionRequestKind> = ["primary", "compaction", "title", "generate"]

const session = Session.Info.make({
  id: Session.ID.make("ses_hook_kind"),
  projectID: Project.ID.global,
  cost: Money.USD.zero,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: DateTime.makeUnsafe(0), updated: DateTime.makeUnsafe(0) },
  location: Location.Ref.make({ directory: AbsolutePath.make("/project") }),
})
const model = SessionRunnerModel.resolved(OpenAIChat.route.model({ id: "gpt-5.5", provider: "test" }), {
  capabilities: { tools: true, input: ["text"], output: ["text"] },
  cost: [],
  limit: { context: 200_000, output: 32_000 },
})
const transport = SessionModelTransport.Service.of({
  bind: () => ({ execute: () => Effect.die("unused WebSocket execution") }),
  close: () => Effect.void,
  closeAll: Effect.void,
})

describe("SessionModelRequest HTTP hooks", () => {
  it.effect("tags every Session request kind on http.request and http.response", () =>
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      const seen: Array<{ hook: string; kind: SessionRequestKind; agent: Agent.ID }> = []
      yield* hooks.register("session", "http.request", (event) =>
        Effect.sync(() => {
          seen.push({ hook: "request", kind: event.kind, agent: event.agent })
        }),
      )
      yield* hooks.register("session", "http.response", (event) =>
        Effect.sync(() => {
          seen.push({ hook: "response", kind: event.kind, agent: event.agent })
        }),
      )
      const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer))

      for (const kind of KINDS) {
        const prepared = yield* requests[kind]({
          session,
          agent: Agent.ID.make("build"),
          model,
          system: [],
          messages: [],
        })
        const http = prepared.options.http
        if (!http) throw new Error(`Expected HTTP middleware for ${kind}`)
        yield* http(HttpClientRequest.post("https://example.test/v1/chat/completions"), (request) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, new Response("{}", { status: 200 }))),
        )
      }

      expect(seen).toEqual(
        KINDS.flatMap((kind) => [
          { hook: "request", kind, agent: Agent.ID.make("build") },
          { hook: "response", kind, agent: Agent.ID.make("build") },
        ]),
      )
    }).pipe(Effect.provideService(SessionModelTransport.Service, transport)),
  )

  it.effect("offers the WebSocket executor alongside HTTP hooks and routes the WebSocket hooks", () =>
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      const seen: string[] = []
      yield* hooks.register("session", "http.request", () => Effect.sync(() => void seen.push("http.request")))
      yield* hooks.register("session", "experimental.ws.handshake", (event) =>
        Effect.sync(() => {
          seen.push(`handshake:${event.kind}:${event.url}`)
          event.headers.authorization = "Bearer minted"
          delete event.headers["api-key"]
        }),
      )
      yield* hooks.register("session", "experimental.ws.send", (event) =>
        Effect.sync(() => {
          seen.push(`send:${event.kind}:${event.frame}`)
          event.frame = `${event.frame}+plugin`
        }),
      )
      yield* hooks.register("session", "experimental.ws.receive", (event) =>
        Effect.sync(() => {
          seen.push(`receive:${event.kind}:${event.frame}`)
          event.frame = event.frame.toUpperCase()
        }),
      )
      const bound: Array<{ url: string; headers: Record<string, string> }> = []
      const frames: string[] = []
      const websocketTransport = SessionModelTransport.Service.of({
        bind: (_sessionID, interceptor) => ({
          execute: () =>
            Effect.gen(function* () {
              if (!interceptor?.handshake || !interceptor.send || !interceptor.receive)
                throw new Error("Expected a full WebSocket interceptor")
              bound.push(
                yield* interceptor.handshake({ url: "wss://example.test/v1/responses", headers: { "api-key": "k" } }),
              )
              frames.push(yield* interceptor.send("create"))
              frames.push(yield* interceptor.receive("created"))
              return { frames: Stream.empty, complete: Effect.void }
            }),
        }),
        close: () => Effect.void,
        closeAll: Effect.void,
      })
      const requests = yield* SessionModelRequest.Service.pipe(
        Effect.provide(SessionModelRequest.layer),
        Effect.provideService(SessionModelTransport.Service, websocketTransport),
      )
      const prepared = yield* requests.primary({
        session,
        agent: Agent.ID.make("build"),
        model: SessionRunnerModel.resolved(OpenAIChat.route.model({ id: "gpt-5.5", provider: "test" }), {
          capabilities: { tools: true, input: ["text"], output: ["text"] },
          cost: [],
          limit: { context: 200_000, output: 32_000 },
          transport: "websocket",
        }),
        system: [],
        messages: [],
        webSocket: "session",
      })

      expect(prepared.options.http).toBeDefined()
      expect(prepared.options.webSocket).toBeDefined()
      yield* prepared.options.webSocket!.execute({} as never)
      expect(bound).toEqual([{ url: "wss://example.test/v1/responses", headers: { authorization: "Bearer minted" } }])
      expect(frames).toEqual(["create+plugin", "CREATED"])
      expect(seen).toEqual([
        "handshake:primary:wss://example.test/v1/responses",
        "send:primary:create",
        "receive:primary:created",
      ])
    }),
  )
})

describe("SessionModelRequest input provenance", () => {
  it.effect("uses the prepared input instead of a later tool caller and leaves unattributed work unknown", () =>
    Effect.gen(function* () {
      const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer))
      const observed: Array<SessionMessage.ID | undefined> = []
      for (const inputID of [SessionMessage.ID.make("msg_original_requester"), undefined]) {
        const prepared = yield* requests.primary({
          session,
          inputID,
          agent: Agent.ID.make("build"),
          model,
          system: [],
          messages: [],
          tools: {
            definitions: [],
            execute: (input) =>
              Effect.sync(() => {
                observed.push(input.inputID)
                return { content: [] }
              }),
          },
        })
        yield* prepared.executeTool({
          sessionID: session.id,
          agent: Agent.ID.make("build"),
          messageID: SessionMessage.ID.make("msg_assistant"),
          inputID: SessionMessage.ID.make("msg_later_requester"),
          call: { type: "tool-call", id: "call-frozen", name: "echo", input: {} },
        })
      }
      expect(observed).toEqual([SessionMessage.ID.make("msg_original_requester"), undefined])
    }).pipe(Effect.provideService(SessionModelTransport.Service, transport)),
  )
})

describe("SessionModelRequest disclosure boundary", () => {
  it.effect("attests the post-hook, post-media snapshot and isolates aliases retained by hooks", () =>
    Effect.gen(function* () {
      const arrived = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      let observed = ""
      const host = disclosureHost({
        observe: async (input) => {
          observed = input.contextDigest
          arrived.resolve()
          await release.promise
        },
      })
      const schema = { type: "object", properties: { query: { type: "string", description: "original-schema" } } }
      const system = SystemPart.make("original-system")
      const text = { type: "text" as const, text: "original-user" }
      const hooks = yield* PluginHooks.Service
      yield* hooks.register("session", "context", (event) =>
        Effect.sync(() => {
          event.messages.push(Message.system("hook-added-context"))
        }),
      )
      const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer))
      const fiber = yield* Effect.forkChild(
        requests.primary({
          session,
          inputID: SessionMessage.ID.make("msg_prepared_original"),
          agent: Agent.ID.make("build"),
          model,
          disclosure: host.config,
          system: [system],
          messages: [Message.user([text, { type: "media", media: Media.base64("aGVsbG8=", "image/png") }])],
          tools: {
            definitions: [
              ToolDefinition.make({
                name: "lookup",
                description: "Lookup",
                inputSchema: schema,
                outputSchema: { type: "string" },
                native: { provider: { type: "native-search" } },
                metadata: { version: "one" },
              }),
            ],
            execute: () => Effect.die("unused"),
          },
        }),
      )
      yield* Effect.promise(() => arrived.promise)
      text.text = "changed-during-host-wait"
      schema.properties.query.description = "changed-schema-during-host-wait"
      release.resolve()
      const prepared = yield* Fiber.join(fiber)
      host.server.stop(true)
      expect(prepared.disclosure?.proof.contextDigest).toBe(observed)
      expect(SessionDisclosure.digest(SessionDisclosure.logicalContext(prepared.request))).toBe(observed)
      expect(JSON.stringify(prepared.request.messages)).toContain("hook-added-context")
      expect(JSON.stringify(prepared.request.messages)).toContain("original-user")
      expect(JSON.stringify(prepared.request.messages)).not.toContain("changed-during-host-wait")
      expect(JSON.stringify(prepared.request.messages)).toContain("this model does not support image input")
      expect(JSON.stringify(prepared.request.tools)).toContain("original-schema")
      expect(JSON.stringify(prepared.request.tools)).toContain("native-search")
      const sent = yield* compileRequest(prepared.request)
      expect(JSON.stringify(sent.body)).toContain("original-user")
      expect(JSON.stringify(sent.body)).not.toContain("changed-during-host-wait")
      expect(SessionDisclosure.digest(SessionDisclosure.logicalContext(prepared.request))).toBe(observed)
      expect(JSON.stringify(prepared.event)).not.toContain(host.config.token)
    }).pipe(Effect.provideService(SessionModelTransport.Service, transport)),
  )
  it.effect(
    "rejects required primary transport hooks and unsupported auxiliary requests before preparation returns",
    () =>
      Effect.gen(function* () {
        const host = disclosureHost()
        const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer))
        const input = {
          session,
          inputID: SessionMessage.ID.make("msg_blocked"),
          agent: Agent.ID.make("build"),
          model,
          disclosure: host.config,
          system: [],
          messages: [],
        }
        for (const kind of ["compaction", "title", "generate"] as const) {
          const operation: Effect.Effect<unknown> = requests[kind](input)
          const exit = yield* Effect.exit(operation)
          expect(exit._tag).toBe("Failure")
        }
        const hooks = yield* PluginHooks.Service
        yield* hooks.register("session", "http.request", () => Effect.void)
        expect((yield* Effect.exit(requests.primary(input)))._tag).toBe("Failure")
        host.server.stop(true)
      }).pipe(Effect.provideService(SessionModelTransport.Service, transport)),
  )
})
