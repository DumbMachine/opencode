export * as ToolRenamePlugin from "./tool-rename.js"

import type { Context as PluginContext } from "@opencode/plugin/effect/plugin"
import { Effect, Option, Schema } from "effect"

const Options = Schema.Struct({
  renames: Schema.Record(Schema.String, Schema.String),
})

export const Plugin = {
  id: "opencode.tool.rename",
  effect: Effect.fn("ToolRenamePlugin.Plugin")(function* (ctx: PluginContext) {
    const options = Schema.decodeUnknownOption(Options)(ctx.options)
    if (Option.isNone(options)) return
    const renames = Object.entries(options.value.renames)
    if (renames.length === 0) return

    yield* ctx.tool.hook("resolve", (event) =>
      Effect.sync(() => {
        const available = new Set(event.tools.map((tool) => tool.effectiveName))
        renames.filter(([from]) => available.has(from)).forEach(([from, to]) => event.renames.push({ from, to }))
      }),
    )
  }),
}
