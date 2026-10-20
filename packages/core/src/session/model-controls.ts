export * as SessionModelControls from "./model-controls.js"

import { type LLMRequest } from "@opencode/ai"
import { OpenAIChat, OpenAIResponses } from "@opencode/ai/protocols"
import { Option, Schema } from "effect"

// This closed contract admits semantic switches, never authored text, metadata,
// arbitrary provider options or raw HTTP body overrides. Do not reuse the
// provider's lenient wire schemas: they accept unknown strings and drop keys.
const generation = Schema.Struct({
  maxTokens: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10_000_000 }))),
  temperature: Schema.optional(Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 2 }))),
  topP: Schema.optional(Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 }))),
  topK: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1_000_000 }))),
  frequencyPenalty: Schema.optional(Schema.Finite.check(Schema.isBetween({ minimum: -2, maximum: 2 }))),
  presencePenalty: Schema.optional(Schema.Finite.check(Schema.isBetween({ minimum: -2, maximum: 2 }))),
  seed: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 4_294_967_295 }))),
})
const options = Schema.Struct({
  store: Schema.optional(Schema.Boolean),
  reasoningEffort: Schema.optional(Schema.Literals(["none", "minimal", "low", "medium", "high", "xhigh", "max"])),
  reasoningSummary: Schema.optional(Schema.Literals(["auto", "concise", "detailed"])),
  textVerbosity: Schema.optional(Schema.Literals(["low", "medium", "high"])),
  serviceTier: Schema.optional(Schema.Literals(["auto", "default", "flex", "priority", "scale"])),
  include: Schema.optional(Schema.Array(Schema.Literal("reasoning.encrypted_content")).check(Schema.isMaxLength(1))),
})
const decodeGeneration = Schema.decodeUnknownOption(generation, { onExcessProperty: "error" })
const defined = (value: unknown) =>
  value !== null && typeof value === "object"
    ? Object.fromEntries(Object.entries(value).filter(([, field]) => field !== undefined))
    : value
const validGeneration = (value: unknown) => Option.isSome(decodeGeneration(defined(value)))
const decodeOptions = Schema.decodeUnknownOption(options, { onExcessProperty: "error" })
const validOptions = (value: unknown) => Option.isSome(decodeOptions(defined(value)))
// Only these compiled lowerers own the declaration. Provider/protocol names,
// user configuration and copied body objects cannot create a registration.
// route.with retains the original body identity while changing routing/defaults.
const protocols = new WeakMap<
  object,
  { readonly from: unknown; readonly schema: unknown; readonly controls: typeof validOptions }
>(
  [OpenAIChat.protocol.body, OpenAIResponses.protocol.body].map(
    (body) => [body, { from: body.from, schema: body.schema, controls: validOptions }] as const,
  ),
)
const empty = (value: object | undefined) => value === undefined || Object.keys(value).length === 0

export const known = (request: LLMRequest) => {
  if (request.metadata !== undefined) return false
  const body = request.model.route.body
  const declared = protocols.get(body)
  const controls = declared?.from === body.from && declared?.schema === body.schema ? declared.controls : undefined
  if (!controls) return false
  return [request.model.route.defaults, request.model.defaults, request].every(
    (source) =>
      source === undefined ||
      (empty(source.http?.body) &&
        (source.generation === undefined || validGeneration(source.generation)) &&
        (empty(source.providerOptions) || controls?.(source.providerOptions) === true)),
  )
}
