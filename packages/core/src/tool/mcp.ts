export * as McpTool from "./mcp.js"

import { ToolFailure } from "@opencode-ai/ai"
import { McpEvent } from "@opencode-ai/schema/mcp-event"
import { Context, Effect, Exit, Fiber, type JsonSchema, Layer, Scope, Semaphore, Stream } from "effect"
import { makeLocationNode } from "@opencode-ai/util/effect/app-node"
import { Bus } from "../bus.js"

import { MCP } from "../mcp/index.js"
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
  readonly tools: (mcp: MCP.Execution) => Effect.Effect<ReadonlyArray<Tool.Info>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/McpTool") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const mcp = yield* MCP.Service
    const tools = yield* Tool.Service
    const bus = yield* Bus.Service
    const permission = yield* Permission.Service
    const scope = yield* Scope.Scope
    const lock = Semaphore.makeUnsafe(1)
    let current: Scope.Closeable | undefined

    const toolInfos: Interface["tools"] = Effect.fn("McpTool.tools")(function* (execution) {
      const discovered = yield* execution.tools()
      return discovered.map((tool): Tool.Info => {
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
              let result = yield* execution.callTool({ server: tool.server, name: tool.name, args }).pipe(
                Effect.catchTags({
                  "MCP.NotFoundError": (error) =>
                    new ToolFailure({ message: `MCP server "${error.server}" is not available` }),
                  "MCP.ToolCallError": (error) => new ToolFailure({ message: error.message }),
                }),
              )
              const approval = tool.approvalBridge ? approvalRequest(result.meta) : undefined
              if (
                tool.approvalBridge &&
                ((result.meta && APPROVAL_REQUEST_META in result.meta && !approval) ||
                  (result.structured &&
                    typeof result.structured === "object" &&
                    "kind" in result.structured &&
                    result.structured.kind === "approval_required" &&
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
                result = yield* execution
                  .callTool({
                    server: tool.server,
                    name: tool.name,
                    args,
                    meta: {
                      [APPROVAL_CONTINUATION_META]: {
                        version: 1,
                        runId: approval.runId,
                        permissionId: permissionID,
                        sessionId: context.sessionID,
                        messageId: context.messageID,
                        toolCallId: context.id,
                      },
                    },
                  })
                  .pipe(
                    Effect.catchTags({
                      "MCP.NotFoundError": (error) =>
                        new ToolFailure({ message: `MCP server "${error.server}" is not available` }),
                      "MCP.ToolCallError": (error) => new ToolFailure({ message: error.message }),
                    }),
                  )
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

    // Register the current tool set under a fresh child scope, then close the previous one so the
    // registry never has a gap where MCP tools disappear mid-swap.
    const reconcile = lock.withPermit(
      Effect.gen(function* () {
        const discovered = yield* toolInfos(mcp)
        const next = yield* Scope.fork(scope)
        yield* tools
          .transform((draft) => {
            for (const tool of discovered) draft.add(tool)
          })
          .pipe(Scope.provide(next), Effect.orDie)
        if (current) yield* Scope.close(current, Exit.void)
        current = next
      }),
    )

    const initial = yield* reconcile.pipe(Effect.forkScoped)
    yield* bus.subscribe(McpEvent.ToolsChanged).pipe(
      Stream.runForEach(() => reconcile),
      Effect.forkScoped({ startImmediately: true }),
    )
    return Service.of({ flush: Effect.asVoid(Fiber.await(initial)), tools: toolInfos })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Tool.node, MCP.node, Bus.node, Permission.node],
})
