export * as ToolEventPublication from "./tool-publication.js"

import { Context, Effect, Option } from "effect"
import type { Event } from "@opencode/schema/event"

export interface Admission {
  readonly check: Effect.Effect<void>
  readonly provide: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}
export interface Capture {
  readonly authorize: (definition: Event.Definition, event: Event.Payload) => Effect.Effect<Admission>
}
class Service extends Context.Service<Service, Capture | undefined>()("@opencode/ToolEventPublication") {}

// The actual Bus consults this scope even when a native leaf retained its Bus
// during construction. Closing the scope also rejects detached child fibers.
export const captured = <A, E, R>(effect: Effect.Effect<A, E, R>, capture: Capture) =>
  Effect.suspend(() => {
    let active = true
    return effect.pipe(
      Effect.provideService(Service, {
        authorize: (definition, event) =>
          active
            ? capture.authorize(definition, event)
            : Effect.die(new Error("Execution publication authorization is unavailable")),
      }),
      Effect.onExit(() =>
        Effect.sync(() => {
          active = false
        }),
      ),
    )
  })
export const current = Effect.serviceOption(Service).pipe(Effect.map(Option.getOrUndefined))
// Authorization/witness I/O uses the same Bus without authorizing itself.
export const without = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provideService(Service, undefined))
