import { describe, expect } from "bun:test"
import { Agent } from "@opencode-ai/core/agent"
import { Plugin } from "@opencode-ai/core/plugin"
import { PluginHooks } from "@opencode-ai/core/plugin/hooks"
import { PluginHost } from "@opencode-ai/core/plugin/host"
import { ToolRenamePlugin } from "@opencode-ai/core/plugin/tool-rename"
import { Session } from "@opencode-ai/core/session"
import { Effect } from "effect"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const it = testEffect(PluginTestLayer)

describe("ToolRenamePlugin", () => {
  it.effect("renames only execution tools present in the request", () =>
    Effect.gen(function* () {
      const plugins = yield* Plugin.Service
      const hooks = yield* PluginHooks.Service
      const host = yield* PluginHost.make(plugins)
      yield* ToolRenamePlugin.Plugin.effect({
        ...host,
        options: {
          renames: {
            access_files_read: "read",
            access_files_write: "write",
          },
        },
      })
      const event = {
        sessionID: Session.ID.make("ses_tool_rename"),
        agent: Agent.ID.make("build"),
        tools: [
          {
            name: "files_read",
            namespace: "access",
            effectiveName: "access_files_read",
            group: "mcp",
          },
        ],
        renames: [] as Array<{ from: string; to: string }>,
      }

      yield* hooks.trigger("tool", "resolve", event)

      expect(event.renames).toEqual([{ from: "access_files_read", to: "read" }])
    }),
  )
})
