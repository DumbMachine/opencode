export * as McpTool from "./mcp.js"

import { ToolFailure } from "@opencode/ai"
import { McpEvent } from "@opencode/schema/mcp-event"
import { Context, Effect, Fiber, type JsonSchema, Layer, PubSub, Semaphore, Stream } from "effect"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { Bus } from "../bus.js"

import { Mcp } from "../mcp/index.js"
import { Permission } from "../permission.js"
import { Tool } from "../tool.js"

/**
 * Registry namespace and permission action names for MCP tools.
 */
export const namespace = (server: string) => server.replace(/[^a-zA-Z0-9_-]/g, "_")
export const name = (server: string, tool: string) => `${namespace(server)}_${tool.replace(/[^a-zA-Z0-9_-]/g, "_")}`

const APPROVAL_REQUEST_META = "access.dev/approval"
const APPROVAL_CONTINUATION_META = "access.dev/approval-continuation"

interface ApprovalRequest {
  readonly version: 1
  readonly runId: string
  readonly action: string
  readonly resources: ReadonlyArray<string>
  readonly metadata: Record<string, unknown>
}

function approvalRequest(meta: Readonly<Record<string, unknown>> | undefined): ApprovalRequest | undefined {
  const value = meta?.[APPROVAL_REQUEST_META]
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined
  const request = value as Record<string, unknown>
  if (
    request.version !== 1 ||
    typeof request.runId !== "string" ||
    request.runId.trim() === "" ||
    typeof request.action !== "string" ||
    request.action.trim() === "" ||
    !Array.isArray(request.resources) ||
    !request.resources.every((resource) => typeof resource === "string") ||
    !request.metadata ||
    typeof request.metadata !== "object" ||
    Array.isArray(request.metadata)
  )
    return undefined
  return request as unknown as ApprovalRequest
}

export interface Interface {
  /** Wait for the initial MCP tool registration to settle. */
  readonly flush: Effect.Effect<void>
  /** Builds request-local tools backed by the supplied MCP execution. */
  readonly tools: (mcp: Mcp.Execution) => Effect.Effect<ReadonlyArray<Tool.Info>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/McpTool") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const mcp = yield* Mcp.Service
    const tools = yield* Tool.Service
    const bus = yield* Bus.Service
    const permission = yield* Permission.Service
    const lock = Semaphore.makeUnsafe(1)
    let discovered: Mcp.Tool[] = []

    const requestTools: Interface["tools"] = Effect.fn("McpTool.tools")(function* (execution) {
      const found = yield* execution.tools()
      return found.map((tool): Tool.Info => {
        const schema = (tool.inputSchema ?? {}) as JsonSchema.JsonSchema
        return {
          name: tool.name,
          options: { namespace: namespace(tool.server), codemode: tool.codemode !== false, group: "mcp" },
          description: tool.description ?? "",
          input: {
            ...schema,
            type: "object",
            properties: schema.properties ?? {},
            additionalProperties: false,
          },
          output: (tool.outputSchema ?? {}) as JsonSchema.JsonSchema,
          execute: (input, context) =>
            Effect.gen(function* () {
              const args = (input ?? {}) as Record<string, unknown>
              if (!tool.approvalBridge)
                yield* permission.assert({
                  action: name(tool.server, tool.name),
                  resources: ["*"],
                  save: ["*"],
                  metadata: {},
                  sessionID: context.sessionID,
                  agent: context.agent,
                  source: { type: "tool", messageID: context.messageID, id: context.id },
                })
              const call = (meta?: Readonly<Record<string, unknown>>) =>
                execution
                  .callTool({ server: tool.server, name: tool.name, args, meta, sessionID: context.sessionID })
                  .pipe(
                    Effect.catchTags({
                      "MCP.NotFoundError": (error) =>
                        new ToolFailure({ message: `MCP server "${error.server}" is not available` }),
                      "MCP.ToolCallError": (error) => new ToolFailure({ message: error.message }),
                    }),
                  )
              let result = yield* call()
              const approval = tool.approvalBridge ? approvalRequest(result.meta) : undefined
              if (
                tool.approvalBridge &&
                ((result.meta && APPROVAL_REQUEST_META in result.meta && !approval) ||
                  (result.structured &&
                    typeof result.structured === "object" &&
                    "kind" in result.structured &&
                    (result.structured as { kind?: unknown }).kind === "approval_required" &&
                    !approval))
              )
                return yield* new ToolFailure({ message: "MCP approval response is missing a valid host continuation" })
              if (approval) {
                const permissionID = Permission.ID.create()
                yield* permission.assert({
                  id: permissionID,
                  action: approval.action,
                  resources: [...approval.resources],
                  save: [],
                  metadata: approval.metadata,
                  sessionID: context.sessionID,
                  agent: context.agent,
                  source: { type: "tool", messageID: context.messageID, id: context.id },
                })
                result = yield* call({
                  [APPROVAL_CONTINUATION_META]: {
                    version: 1,
                    runId: approval.runId,
                    permissionId: permissionID,
                    sessionId: context.sessionID,
                    messageId: context.messageID,
                    toolCallId: context.id,
                  },
                })
              }
              if (result.isError)
                return yield* new ToolFailure({
                  message:
                    result.content
                      .flatMap((part) => (part.type === "text" ? [part.text] : []))
                      .join("\n")
                      .trim() || "MCP tool returned an error",
                })
              const content = result.content.map((part) =>
                part.type === "text"
                  ? { type: "text" as const, text: part.text }
                  : { type: "file" as const, uri: `data:${part.mimeType};base64,${part.data}`, mime: part.mimeType },
              )
              const text = content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n")
              return {
                output: result.structured ?? (text === "" ? null : text),
                ...(content.length === 0 ? {} : { content }),
              }
            }).pipe(
              Effect.mapError((error) =>
                error instanceof ToolFailure
                  ? error
                  : new ToolFailure({ message: `Unable to execute ${name(tool.server, tool.name)}` }),
              ),
            ),
        }
      })
    })

    // Register once after initial discovery; only subsequent updates need a debounced reload.
    const initial = yield* lock
      .withPermit(
        Effect.gen(function* () {
          discovered = yield* mcp.tools()
          yield* tools.transform((editor) => {
            for (const tool of discovered) {
              editor.add({
                name: tool.name,
                options: { namespace: namespace(tool.server), codemode: tool.codemode !== false, group: "mcp" },
                description: tool.description ?? "",
                input: (tool.inputSchema ?? { type: "object", properties: {} }) as JsonSchema.JsonSchema,
                output: (tool.outputSchema ?? {}) as JsonSchema.JsonSchema,
                execute: (input, context) =>
                  Effect.gen(function* () {
                    yield* permission.assert({
                      action: name(tool.server, tool.name),
                      resources: ["*"],
                      save: ["*"],
                      metadata: {},
                      sessionID: context.sessionID,
                      agent: context.agent,
                      source: {
                        type: "tool",
                        messageID: context.messageID,
                        id: context.id,
                      },
                    })
                    const result = yield* mcp
                      .callTool({
                        server: tool.server,
                        name: tool.name,
                        args: (input ?? {}) as Record<string, unknown>,
                        sessionID: context.sessionID,
                      })
                      .pipe(
                        Effect.catchTags({
                          "MCP.NotFoundError": (error) =>
                            new ToolFailure({ message: `MCP server "${error.server}" is not available` }),
                          "MCP.ToolCallError": (error) => new ToolFailure({ message: error.message }),
                        }),
                      )
                    if (result.isError)
                      return yield* new ToolFailure({
                        message:
                          result.content
                            .flatMap((part) => (part.type === "text" ? [part.text] : []))
                            .join("\n")
                            .trim() || "MCP tool returned an error",
                      })
                    const content = result.content.map((part) =>
                      part.type === "text"
                        ? { type: "text" as const, text: part.text }
                        : {
                            type: "file" as const,
                            uri: `data:${part.mimeType};base64,${part.data}`,
                            mime: part.mimeType,
                          },
                    )
                    const text = content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n")
                    const output = () => {
                      if (result.structured !== undefined) return result.structured
                      if (text === "") return null
                      // Agents assume JSON returned as text is already an object, so parse it when the server declares no schema.
                      if (tool.outputSchema === undefined && (text.startsWith("{") || text.startsWith("["))) {
                        try {
                          return JSON.parse(text)
                        } catch {}
                      }
                      return text
                    }
                    return {
                      output: output(),
                      ...(content.length === 0 ? {} : { content }),
                    }
                  }).pipe(
                    Effect.mapError((error) =>
                      error instanceof ToolFailure
                        ? error
                        : new ToolFailure({ message: `Unable to execute ${name(tool.server, tool.name)}` }),
                    ),
                  ),
              })
            }
          })
        }),
      )
      .pipe(Effect.forkScoped)
    const reconcile = lock.withPermit(
      Effect.gen(function* () {
        discovered = yield* mcp.tools()
        yield* tools.reload()
      }),
    )

    // Servers announce tools in bursts and each read loads the whole catalog, so settle and refresh
    // once. The bus subscription stays eager; only the already-open sliding subscription is debounced.
    const changes = yield* PubSub.sliding<void>(1)
    yield* bus.subscribe(McpEvent.ToolsChanged).pipe(
      Stream.runForEach(() => PubSub.publish(changes, undefined)),
      Effect.forkScoped({ startImmediately: true }),
    )
    const updates = yield* PubSub.subscribe(changes)
    yield* Stream.fromSubscription(updates).pipe(
      Stream.debounce("100 millis"),
      Stream.runForEach(() => reconcile),
      Effect.forkScoped({ startImmediately: true }),
    )
    return Service.of({ flush: Effect.asVoid(Fiber.await(initial)), tools: requestTools })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Tool.node, Mcp.node, Bus.node, Permission.node],
})
