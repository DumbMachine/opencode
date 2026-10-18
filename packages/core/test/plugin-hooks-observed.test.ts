import { expect } from "bun:test"
import { Effect, Scope } from "effect"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { SessionID } from "@opencode/schema/session-id"
import { SessionMessageID } from "@opencode/schema/session-message-id"
import { testEffect } from "./lib/effect"

const it = testEffect(LayerNode.compile(PluginHooks.node))
const event = () => ({
  sessionID: SessionID.make("ses_hooks"),
  messageID: SessionMessageID.make("msg_hooks"),
  prompt: { text: "Original" },
  delivery: "steer" as const,
})

it.effect("counts actual dispatch callbacks and keeps its registration snapshot", () =>
  Effect.gen(function* () {
    const hooks = yield* PluginHooks.Service
    const calls: string[] = []
    const scope = yield* Scope.Scope
    const first = yield* hooks.register("session", "prompt", () =>
      Effect.gen(function* () {
        calls.push("first")
        yield* second.dispose
        yield* hooks
          .register("session", "prompt", () =>
            Effect.sync(() => {
              calls.push("late")
            }),
          )
          .pipe(Effect.provideService(Scope.Scope, scope))
      }),
    )
    const second = yield* hooks.register("session", "prompt", (event) =>
      Effect.sync(() => {
        calls.push("second")
        event.prompt.text = "Changed"
      }),
    )
    const observed = yield* hooks.triggerObserved("session", "prompt", event())
    expect(observed.invoked).toBe(2)
    expect(observed.event.prompt.text).toBe("Changed")
    expect(calls).toEqual(["first", "second"])
    yield* first.dispose
    calls.length = 0
    const ordinary = yield* hooks.trigger("session", "prompt", event())
    expect(ordinary.prompt.text).toBe("Original")
    expect(calls).toEqual(["late"])
  }),
)

it.effect("does not count provider-filtered callbacks or disposed hooks", () =>
  Effect.gen(function* () {
    const hooks = yield* PluginHooks.Service
    const skipped = yield* hooks.register("session", "prompt", () => Effect.die("must not run"), {
      providerID: "other",
    })
    expect((yield* hooks.triggerObserved("session", "prompt", event())).invoked).toBe(0)
    yield* skipped.dispose
    const active = yield* hooks.register("session", "prompt", () => Effect.void)
    expect((yield* hooks.triggerObserved("session", "prompt", event())).invoked).toBe(1)
    yield* active.dispose
    expect((yield* hooks.triggerObserved("session", "prompt", event())).invoked).toBe(0)
  }),
)
