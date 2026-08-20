import { Tool } from "@opencode-ai/schema/tool"
import type { Agent } from "@opencode-ai/schema/agent"
import type { Session } from "@opencode-ai/schema/session"
import type { SessionMessage } from "@opencode-ai/schema/session-message"
import type { Hooks, Transform } from "./registration.js"

export interface ToolDraft {
  add<Input extends Tool.ValueSchema<any>, Output extends Tool.ValueSchema<any> | undefined>(
    tool: Tool.Info<Input, Output>,
  ): void
}

export interface ToolHooks {
  /** Declaratively rename execution-scoped tools before the request snapshot is built. */
  readonly resolve: {
    readonly sessionID: Session.ID
    readonly agent: Agent.ID
    readonly tools: ReadonlyArray<{
      readonly name: string
      readonly namespace?: string
      readonly effectiveName: string
      readonly group?: string
    }>
    renames: Array<{ readonly from: string; readonly to: string }>
  }
  readonly "execute.before": {
    readonly tool: string
    readonly sessionID: Session.ID
    readonly agent: Agent.ID
    readonly messageID: SessionMessage.ID
    readonly id: Tool.CallID
    input: unknown
  }
  readonly "execute.after": {
    readonly tool: string
    readonly sessionID: Session.ID
    readonly agent: Agent.ID
    readonly messageID: SessionMessage.ID
    readonly id: Tool.CallID
    readonly input: unknown
  } & (
    | {
        readonly status: "completed"
        result: Tool.Result
      }
    | {
        readonly status: "error"
        error: Tool.Error
      }
  )
}

// Only execute.before may fail: a Tool.Error rejects the call before the tool runs.
export interface ToolFailures extends Record<keyof ToolHooks, unknown> {
  readonly resolve: never
  readonly "execute.before": Tool.Error
  readonly "execute.after": never
}

export interface ToolDomain {
  readonly transform: Transform<ToolDraft>
  readonly hook: Hooks<ToolHooks, ToolFailures>
}
