export * as SessionModelRequest from "./model-request.js"

import {
  GenerationOptions,
  type GenerationOptionsFields,
  HttpOptions,
  LanguageModel,
  LLM,
  LLMRequest,
  type Media,
  Message,
  SystemPart,
} from "@opencode/ai"
import { LLMClient, type StreamOptions } from "@opencode/ai/route"
import { LayerNodePlatform } from "../effect/app-node-platform.js"
import type {
  SessionCompaction,
  SessionContext,
  SessionGenerate,
  SessionRequest,
  SessionRequestKind,
  SessionTitle,
} from "@opencode/plugin/effect/session"
import type { Agent } from "@opencode/schema/agent"
import type { Model } from "@opencode/schema/model"
import type { Content } from "@opencode/schema/tool"
import { Cause, Context, Effect, Layer, Option, Result, Stream } from "effect"
import { HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { App } from "../app.js"
import { Permission } from "../permission.js"
import { PluginHooks } from "../plugin/hooks.js"
import { QuestionTool } from "../tool/plugin/question.js"
import { Tool } from "../tool.js"
import { SessionModelTransport } from "./model-transport.js"
import { SessionProviderContext } from "./provider-context.js"
import { SessionRunnerModel } from "./runner/model.js"
import { SessionSchema } from "./schema.js"
import { SessionSystemPrompt } from "./system-prompt.js"
import { toLLMMessages } from "./runner/to-llm-message.js"
import type { SessionMessage } from "./message.js"
import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { SessionInputRequirements } from "./input-requirements.js"
import { SessionDisclosure } from "./disclosure.js"
import { SessionExecutionCapability } from "./execution-capability.js"
import { SessionRunCoordinator } from "./run-coordinator.js"
import { NativeProducer } from "../execution-producer.js"
import { SessionPublicationWriter } from "./publication-writer.js"
import { Bus } from "../bus.js"
import { Database } from "../database/database.js"

const IMAGE_BYTES_TRIGGER = 25 * 1024 * 1024 // 25 MiB
const IMAGE_BYTES_TARGET = 15 * 1024 * 1024 // 15 MiB
const IMAGE_REMOVED =
  "[This image was removed to reduce the request size and is no longer visible. Do not make claims about its contents from memory. If needed, retrieve it again with an available tool or ask the user to attach it again.]"
const GENERATION_KEYS = new Set(Object.keys(GenerationOptions.fields))

/** Tool errors, plus the user declining a permission or dismissing a question. */
export type ExecuteError = Tool.Error | Permission.DeclinedError | QuestionTool.CancelledError

export interface Prepared<Event = SessionRequest> {
  readonly disclosure?: Effect.Success<ReturnType<typeof SessionDisclosure.prepare>>
  readonly assertCurrent?: Effect.Effect<void>
  readonly publication?: SessionPublicationWriter.Writer
  readonly event: Event
  readonly request: LLMRequest
  readonly options: StreamOptions
  readonly retry: (event: PluginHooks.Domains["session"]["retry"]) => Effect.Effect<void>
  /** Runs a tool call against the tools this request advertised. */
  readonly executeTool: (
    input: Parameters<Tool.Snapshot["execute"]>[0],
  ) => Effect.Effect<Tool.NormalizedResult, ExecuteError>
}

export interface Input {
  readonly disclosure?: ExecutionDisclosure.Service
  readonly disclosureRevision?: number
  readonly disclosureCurrent?: () => boolean
  readonly disclosureComponents?: readonly ExecutionDisclosure.Component[]
  readonly disclosureMessages?: readonly SessionMessage.Info[]
  readonly session: SessionSchema.Info
  readonly agent: Agent.ID
  readonly inputID?: SessionMessage.ID
  readonly model: SessionRunnerModel.Resolved
  readonly tools?: Tool.Snapshot
  readonly system: Array<SystemPart>
  readonly messages: Array<Message>
  readonly toolChoice?: LLM.RequestInput["toolChoice"]
  /** Only the durable runner may use a stateful WebSocket. */
  readonly webSocket?: "session"
}

export const baseTranscript = (input: {
  readonly agent: Agent.Info
  readonly contextPolicy?: SessionSchema.Info["contextPolicy"]
  readonly model: SessionRunnerModel.Resolved
  readonly tools: Tool.Snapshot
  readonly initial: string
  readonly ephemeral?: string
  readonly instructions?: string
  readonly messages: ReadonlyArray<SessionMessage.Info>
}) => {
  const providerMetadataKey = input.model.model.route.providerMetadataKey ?? input.model.model.provider
  return {
    providerMetadataKey,
    system: (input.contextPolicy
      ? [input.instructions ?? ""]
      : [
          input.agent.system
            ? input.agent.system
            : SessionSystemPrompt.make(input.tools.definitions.map((tool) => tool.name)),
          input.initial,
          input.ephemeral ?? "",
          input.instructions ?? "",
        ]
    )
      .filter((part) => part.length > 0)
      .map(SystemPart.make),
    messages: toLLMMessages(input.messages, input.model.ref, providerMetadataKey),
  }
}

const mimeToModality = (mime: string) => {
  if (mime.startsWith("image/")) return "image"
  if (mime.startsWith("audio/")) return "audio"
  if (mime.startsWith("video/")) return "video"
  if (mime === "application/pdf") return "pdf"
}

const unsupportedMedia = (mime: string, name: string | undefined, capabilities: Model.Capabilities) => {
  const modality = mimeToModality(mime)
  if (!modality || capabilities.input.some((item) => item.startsWith(modality))) return
  return {
    type: "text" as const,
    text: `ERROR: Cannot read ${name ? `"${name}"` : modality} (this model does not support ${modality} input). Inform the user.`,
  }
}

export const unsupportedParts = (messages: LLMRequest["messages"], capabilities: Model.Capabilities) =>
  messages.map((message) =>
    Message.make({
      ...message,
      content: message.content.map((part) => {
        if (part.type === "media") {
          return unsupportedMedia(part.media.mediaType, part.filename, capabilities) ?? part
        }
        if (part.type !== "tool-result" || part.result.type !== "content") return part
        return {
          ...part,
          result: {
            ...part.result,
            value: part.result.value.map((item: Content) => {
              if (item.type !== "file") return item
              return unsupportedMedia(item.mime, item.name, capabilities) ?? item
            }),
          },
        }
      }),
    }),
  )

export const boundImages = (messages: LLMRequest["messages"]) => {
  const isImage = (mime: string) => mime.toLowerCase().startsWith("image/")
  // Remote and provider-referenced media carry no local payload and never count toward the inline budget.
  const size = (media: Media.Asset) => {
    if (media.source.type === "base64") return Buffer.byteLength(media.source.data)
    if (media.source.type === "bytes") return Math.ceil(media.source.data.byteLength / 3) * 4
    return 0
  }
  const imageBytes = messages.reduce(
    (total, message) =>
      total +
      message.content.reduce((sum, part) => {
        if (part.type === "media" && isImage(part.media.mediaType)) return sum + size(part.media)
        if (part.type !== "tool-result" || part.result.type !== "content") return sum
        return (
          sum +
          part.result.value.reduce(
            (bytes: number, item: Content) =>
              bytes + (item.type === "file" && isImage(item.mime) ? Buffer.byteLength(item.uri) : 0),
            0,
          )
        )
      }, 0),
    0,
  )
  if (imageBytes <= IMAGE_BYTES_TRIGGER) return messages

  let removed = 0
  return messages.map((message) =>
    Message.make({
      ...message,
      content: message.content.map((part) => {
        if (part.type === "media" && isImage(part.media.mediaType) && imageBytes - removed > IMAGE_BYTES_TARGET) {
          removed += size(part.media)
          return Message.text(IMAGE_REMOVED)
        }
        if (part.type !== "tool-result" || part.result.type !== "content") return part
        return {
          ...part,
          result: {
            ...part.result,
            value: part.result.value.map((item: Content) => {
              if (item.type !== "file" || !isImage(item.mime) || imageBytes - removed <= IMAGE_BYTES_TARGET) return item
              removed += Buffer.byteLength(item.uri)
              return { type: "text" as const, text: IMAGE_REMOVED }
            }),
          },
        }
      }),
    }),
  )
}

type Definitions = PluginHooks.Domains["session"]["context"]["tools"]

/** Builds the model request for each session flow. Each entry runs its own plugin hook. */
export interface Interface {
  readonly primary: (input: Input) => Effect.Effect<Prepared<SessionContext>>
  readonly compaction: (input: Input) => Effect.Effect<Prepared<SessionCompaction>>
  readonly generate: (input: Input) => Effect.Effect<Prepared<SessionGenerate>>
  readonly title: (input: Input) => Effect.Effect<Prepared<SessionTitle>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionModelRequest") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const hooks = yield* PluginHooks.Service
    const transport = yield* SessionModelTransport.Service
    const app = yield* App.Metadata
    const prepare = Effect.fn("SessionModelRequest.prepare")(function* <
      S extends SessionRequest & { tools?: Definitions },
    >(
      kind: SessionRequestKind,
      input: Input,
      shape: (
        draft: SessionRequest,
        tools: Definitions,
      ) => Effect.Effect<{ readonly event: S; readonly invoked: number }>,
    ) {
      const service = input.disclosure === undefined ? undefined : structuredClone(input.disclosure)
      if (input.session.contextPolicy && (kind !== "primary" || service?.publicationVersion !== 1))
        return yield* Effect.die(new Error("Execution publication authorization is unavailable"))
      if (!SessionInputRequirements.serviceSupported(service))
        return yield* Effect.die(new Error("Execution publication authorization is unavailable"))
      const inputID = input.inputID
      const session = input.session
      const sessionID = session.id
      const model = input.model
      // Own canonical history and its rendered positions before the first wait,
      // including waits for ownership, hooks and the host callback.
      const producer =
        service?.compositionVersion === 1 && kind === "primary" && inputID !== undefined && input.disclosureMessages
          ? NativeProducer.captureContext({
              sessionID,
              inputID,
              model: model.ref,
              providerMetadataKey: model.model.route.providerMetadataKey ?? model.model.provider,
              messages: input.disclosureMessages,
              components: input.disclosureComponents ?? [],
            })
          : undefined
      const revision = input.disclosureRevision
      const earlier = input.disclosureCurrent
      const owner =
        service === undefined
          ? undefined
          : Option.getOrUndefined(yield* Effect.serviceOption(SessionRunCoordinator.Ownership))
      if (
        service !== undefined &&
        (kind !== "primary" || inputID === undefined || revision === undefined || !earlier || !owner)
      )
        return yield* Effect.die(new Error("Execution context authorization is unavailable"))
      const installed =
        service === undefined
          ? undefined
          : SessionExecutionCapability.captureSelection(sessionID, inputID!, revision!, service)
      const current = Effect.sync(() => {
        if (service !== undefined && (!owner!.live() || !earlier!() || !installed!()))
          throw new Error("Execution context authorization is unavailable")
      })
      const publicationCapture =
        service !== undefined && (service.compositionVersion === 1 || service.publicationVersion === 1)
          ? yield* SessionPublicationWriter.capturePreflight({
              sessionID,
              inputID: inputID!,
              revision: revision!,
              service,
            })
          : undefined
      yield* current
      const scope = { sessionID, agent: input.agent, model: model.ref, kind }
      const tools = input.tools ?? {
        definitions: [],
        execute: () => new Tool.Error({ message: "Tools are not available for this request" }),
      }
      // Remember which tool each definition object came from. Hooks rename a tool by moving
      // its definition to a new key, so after the hook we find the tool by object identity.
      const given = new Map(
        tools.definitions.map((t) => [{ description: t.description, input: { ...t.inputSchema } }, t] as const),
      )
      const shapedObservation = yield* shape(
        { sessionID, model: model.ref, system: input.system, messages: input.messages, options: {} },
        Object.fromEntries(Array.from(given, ([d, t]) => [t.name, d])),
      )
      const shaped = shapedObservation.event
      // Match by identity first, then by key. Entries matching neither were invented by a
      // hook and are dropped. `t.name` stays the real name so execution can map renames back.
      const byName = new Map(tools.definitions.map((t) => [t.name, t]))
      const hooked = new Map(
        Object.entries(shaped.tools ?? {}).flatMap(([name, d]) => {
          const t = given.get(d) ?? byName.get(name)
          return t ? [[name, { ...t, description: d.description, inputSchema: d.input }] as const] : []
        }),
      )
      const entries = Object.entries(shaped.options)
      const generation = Object.fromEntries(entries.filter(([k]) => GENERATION_KEYS.has(k))) as GenerationOptionsFields
      const providerOptions = Object.fromEntries(entries.filter(([k]) => !GENERATION_KEYS.has(k)))
      const affinity = session.parentID ?? session.fork?.sessionID ?? session.id
      const base = LLM.request({
        model: model.model,
        http: {
          headers: {
            "x-session-affinity": session.id,
            "X-Session-Id": session.id,
            ...(session.parentID ? { "x-parent-session-id": session.parentID } : {}),
            "User-Agent": App.useragent(app),
            "x-opencode-project": session.projectID,
            "x-opencode-session": session.id,
            "x-opencode-client": app.name,
          },
        },
        // TODO: Persist cache lineage so nested forks reuse the root session's cache key.
        promptCacheKey: /^ses_[0-9a-f]{64}$/.test(affinity) ? affinity.slice(4) : affinity,
        system: shaped.system,
        messages: boundImages(unsupportedParts(shaped.messages, model.capabilities)),
        tools: Array.from(hooked, ([name, t]) => ({ ...t, name })),
        toolChoice: input.toolChoice,
        generation: Object.keys(generation).length === 0 ? undefined : generation,
        providerOptions: Object.keys(providerOptions).length === 0 ? undefined : providerOptions,
      })

      const baseURL = base.model.route.endpoint.baseURL
      const modelObservation = yield* hooks.triggerObserved("session", "model.request", {
        ...scope,
        baseURL: typeof baseURL === "string" ? baseURL : undefined,
        headers: { ...base.http?.headers },
      })
      const modelHook = modelObservation.event
      const route =
        modelHook.baseURL !== undefined && modelHook.baseURL !== baseURL
          ? base.model.route.with({ endpoint: { baseURL: modelHook.baseURL } })
          : base.model.route
      // Plugins may route/provider-authorize requests, but cannot lend another input's billing authority.
      const headers = Object.fromEntries(
        Object.entries(modelHook.headers).filter(
          ([key]) => key.toLowerCase() !== "x-opencode-session" && key.toLowerCase() !== "x-opencode-input",
        ),
      )
      headers["x-opencode-session"] = scope.sessionID
      if (inputID !== undefined) headers["x-opencode-input"] = inputID
      const routed = LLMRequest.update(base, {
        model: route === base.model.route ? base.model : LanguageModel.update(base.model, { route }),
        http: new HttpOptions({
          body: base.http?.body,
          headers,
          query: base.http?.query,
        }),
      })
      const client = Option.getOrUndefined(yield* Effect.serviceOption(LLMClient.Service))
      const bounded = client?.prepareRequest ? yield* client.prepareRequest(routed).pipe(Effect.orDie) : routed
      const request =
        service === undefined
          ? bounded
          : yield* Effect.try({
              try: () => SessionDisclosure.snapshot(bounded),
              catch: () => new Error("Execution context authorization is unavailable"),
            }).pipe(Effect.orDie)
      // History selects native windows against the catalog route before hooks run. A newly installed
      // routing hook must not send an existing opaque window to another deployment; `prepare` has no
      // error channel, so like hook failures this surfaces as a defect.
      const selected = SessionProviderContext.provenance(model)
      if (
        selected &&
        !SessionProviderContext.compatible(
          selected,
          SessionProviderContext.provenance({ model: request.model, ref: model.ref }),
        ) &&
        request.messages.some((message) => message.content.some((part) => part.type === "compaction"))
      )
        return yield* Effect.die(
          new Error("Provider context is incompatible with the route selected by model request hooks"),
        )

      const hasHttpHooks =
        (yield* hooks.has("session", "http.request", model.ref.providerID)) ||
        (yield* hooks.has("session", "http.response", model.ref.providerID))
      const http: StreamOptions["http"] = hasHttpHooks
        ? (req, handler) =>
            Effect.gen(function* () {
              const before = yield* hooks.trigger("session", "http.request", {
                ...scope,
                request: yield* HttpClientRequest.toWeb(req),
              })
              let sent = HttpClientRequest.fromWeb(before.request)
              if (before.request.body)
                sent = HttpClientRequest.bodyUint8Array(
                  sent,
                  new Uint8Array(yield* Effect.promise(() => before.request.clone().arrayBuffer())),
                  before.request.headers.get("content-type") ?? undefined,
                )
              const res = yield* handler(sent)
              const after = yield* hooks.trigger("session", "http.response", {
                ...scope,
                request: before.request,
                response: new Response(
                  [204, 205, 304].includes(res.status) ? null : yield* Stream.toReadableStreamEffect(res.stream),
                  { status: res.status, headers: res.headers },
                ),
              })
              return HttpClientResponse.fromWeb(sent, after.response)
            }).pipe(Effect.mapError((cause) => (cause instanceof Error ? cause : new Error(String(cause)))))
        : undefined
      // Untracked transport hooks can replace attested bytes after this seam.
      // Until they carry lineage, a required disclosure request cannot use them.
      const hasWebSocketHooks =
        service !== undefined &&
        ((yield* hooks.has("session", "experimental.ws.send", model.ref.providerID)) ||
          (yield* hooks.has("session", "experimental.ws.receive", model.ref.providerID)) ||
          (yield* hooks.has("session", "experimental.ws.handshake", model.ref.providerID)))
      if (service !== undefined && (hasHttpHooks || hasWebSocketHooks || inputID === undefined || kind !== "primary"))
        return yield* Effect.die(new Error("Execution context authorization is unavailable"))
      const disclosure =
        service === undefined
          ? undefined
          : yield* Effect.gen(function* () {
              const contextDigest = SessionDisclosure.digest(SessionDisclosure.logicalContext(request))
              yield* current
              const extra = [
                {
                  kind: "tools" as const,
                  id: "advertised-definitions",
                  revision: SessionDisclosure.digest(request.tools),
                },
                { kind: "prepared-context" as const, id: "logical-request", revision: contextDigest },
              ]
              const observed = producer
                ? yield* producer.observe(request, shapedObservation.invoked + modelObservation.invoked, extra)
                : undefined
              const exact =
                observed ??
                SessionDisclosure.makeRequest({
                  sessionID,
                  inputID: inputID!,
                  kind,
                  contextDigest,
                  components: [...(input.disclosureComponents ?? []), ...extra],
                })
              const prepared = publicationCapture
                ? { ...(yield* publicationCapture.prepareRequest(exact)), publication: undefined }
                : { ...(yield* SessionDisclosure.prepareRequest(service!, exact)), writer: undefined }
              yield* current
              return prepared
            })
      if (input.session.contextPolicy && (!disclosure?.proof.complete || !disclosure.writer))
        return yield* Effect.die(new Error("Execution publication authorization is unavailable"))
      const transportNames = [
        "http.request",
        "http.response",
        "experimental.ws.handshake",
        "experimental.ws.send",
        "experimental.ws.receive",
      ] as const
      const transportCurrent = Effect.gen(function* () {
        for (const name of transportNames)
          if (yield* hooks.has("session", name, model.ref.providerID))
            return yield* Effect.die(new Error("Execution context authorization is unavailable"))
      })
      const protectedTransport = <A>(name: (typeof transportNames)[number], value: A) =>
        hooks
          .has("session", name, model.ref.providerID)
          .pipe(
            Effect.flatMap((present) =>
              present ? Effect.die(new Error("Execution context authorization is unavailable")) : Effect.succeed(value),
            ),
          )
      // Capture primitive bindings independently of the returned proof object.
      const assertedDigest =
        disclosure === undefined ? undefined : SessionDisclosure.digest(SessionDisclosure.logicalContext(request))
      const assertRequest =
        assertedDigest === undefined
          ? undefined
          : (actual: LLMRequest) =>
              Effect.sync(() => {
                if (
                  actual.http?.body ||
                  actual.model.defaults?.http?.body ||
                  actual.model.route.defaults.http?.body ||
                  SessionDisclosure.digest(SessionDisclosure.logicalContext(actual)) !== assertedDigest
                )
                  throw new Error("Execution context authorization is unavailable")
              })
      const expiresAt = disclosure?.proof.expiresAt
      const assertCurrent =
        expiresAt === undefined
          ? undefined
          : current.pipe(
              Effect.andThen(transportCurrent),
              Effect.andThen(
                Effect.sync(() => {
                  if (Date.now() >= expiresAt) throw new Error("Execution context authorization is unavailable")
                }),
              ),
            )
      // HTTP hooks wrap every HTTP request, including the WebSocket fallback path. The route decides
      // which transport actually carries the request, so both hook families are always offered.
      const webSocket =
        input.webSocket === "session" && model.transport === "websocket"
          ? transport.bind(
              session.id,
              {
                handshake: (connect) =>
                  service !== undefined
                    ? protectedTransport("experimental.ws.handshake", connect)
                    : hooks
                        .trigger("session", "experimental.ws.handshake", {
                          ...scope,
                          url: connect.url,
                          headers: connect.headers,
                        })
                        .pipe(Effect.map((event) => ({ url: event.url, headers: event.headers }))),
                send: (frame) =>
                  service !== undefined
                    ? protectedTransport("experimental.ws.send", frame)
                    : hooks
                        .trigger("session", "experimental.ws.send", { ...scope, frame })
                        .pipe(Effect.map((event) => event.frame)),
                receive: (frame) =>
                  service !== undefined
                    ? protectedTransport("experimental.ws.receive", frame)
                    : hooks
                        .trigger("session", "experimental.ws.receive", { ...scope, frame })
                        .pipe(Effect.map((event) => event.frame)),
              },
              model.chunkTimeout,
            )
          : undefined

      return {
        event: shaped,
        disclosure,
        publication: disclosure?.writer,
        assertCurrent,
        request,
        options: {
          ...(assertCurrent ? { beforeDispatch: assertCurrent } : {}),
          ...(assertRequest ? { assertRequest } : {}),
          ...(http ? { http } : {}),
          ...(webSocket ? { webSocket } : {}),
        },
        retry: (event: Parameters<Prepared["retry"]>[0]) =>
          hooks.trigger("session", "retry", event).pipe(Effect.asVoid),
        // Permission.assert and the question tool throw declines as defects so tools cannot
        // catch them and turn a "no" into model-visible output. Recover them here as failures.
        executeTool: (call: Parameters<Prepared["executeTool"]>[0]) =>
          current.pipe(
            Effect.andThen(() => tools.execute({ ...call, inputID, definitions: hooked })),
            Effect.catchCauseFilter(
              (cause) => {
                const decline = cause.reasons.flatMap((r) =>
                  Cause.isDieReason(r) &&
                  (r.defect instanceof Permission.DeclinedError || r.defect instanceof QuestionTool.CancelledError)
                    ? [r.defect]
                    : [],
                )[0]
                return decline ? Result.succeed(decline) : Result.fail(cause)
              },
              (decline) => Effect.fail(decline),
            ),
          ),
      }
    })

    const agentHook =
      (name: "context" | "compaction" | "generate", agent: Agent.ID) => (draft: SessionRequest, tools: Definitions) =>
        hooks.triggerObserved("session", name, { ...draft, agent, tools })

    return Service.of({
      primary: (input) => prepare("primary", input, agentHook("context", input.agent)),
      compaction: (input) => prepare("compaction", input, agentHook("compaction", input.agent)),
      generate: (input) => prepare("generate", input, agentHook("generate", input.agent)),
      title: (input) => prepare("title", input, (draft) => hooks.triggerObserved("session", "title", draft)),
    })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [PluginHooks.node, SessionModelTransport.node, App.node, Bus.node, Database.node, LayerNodePlatform.llmClient],
})
