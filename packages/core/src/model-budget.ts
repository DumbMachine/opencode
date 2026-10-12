export * as ModelBudget from "./model-budget.js"

import {
  AIError,
  type ContentPart,
  GenerationOptions,
  HttpContext,
  LLMEvent,
  LLMRequest,
  LLMResponse,
  QuotaExceededError,
  UnsupportedOperationError,
  type ToolEntry,
} from "@opencode/ai"
import { LLMClient, type LLMClientShape } from "@opencode/ai/route"
import { Cause, Effect, Exit, Layer, Option, Schema, Schedule, Semaphore, Stream } from "effect"
import { KV } from "./kv.js"
import { Token } from "./util/token.js"

const failure = (message: string, http?: HttpContext) =>
  new AIError({ reason: new QuotaExceededError({ message, http }) })
const tokenCount = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)
const completion = Schema.Struct({
  operation: Schema.Literals(["complete", "cancel"]),
  session_id: Schema.String,
  attempt_id: Schema.String,
  provider_id: Schema.String,
  model_id: Schema.String,
  usage: Schema.Struct({
    input: tokenCount,
    output: tokenCount,
    reasoning: tokenCount,
    cache_read: tokenCount,
    cache_write: tokenCount,
  }),
})
const journalPrefix = "model-budget:callbacks/"
const sessionPrefix = (session: string) => `${journalPrefix}${encodeURIComponent(session)}/`
const journalKey = (record: typeof completion.Type) => `${sessionPrefix(record.session_id)}${record.attempt_id}`
const zeroUsage = { input: 0, output: 0, reasoning: 0, cache_read: 0, cache_write: 0 }

const hasOpaqueInput = (request: LLMRequest) =>
  request.messages.some((message) =>
    message.content.some(
      (part) =>
        part.type === "media" ||
        (part.type === "tool-result" &&
          part.result.type === "content" &&
          part.result.value.some((content) => content.type === "file")),
    ),
  )

// A byte bound avoids tokenizer/network work on the first-token path. For
// media that can expand at the provider, reserve the entire model input window.
export const inputBound = (request: LLMRequest) => {
  const text = JSON.stringify({ system: request.system, messages: request.messages, tools: request.tools })
  const limit = request.model.inputLimit
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1))
    throw new Error("Model has an invalid input limit for billing")
  if (hasOpaqueInput(request)) {
    if (limit === undefined) throw new Error("Model has no input limit for media billing")
    return limit
  }
  const bytes = new TextEncoder().encode(text).byteLength + 256 * (request.messages.length + request.tools.length + 1)
  // The provider cannot accept more than its model's input window. A local
  // byte upper bound must never reserve more tokens than that entire window.
  return limit === undefined ? bytes : Math.min(bytes, limit)
}

// Customer admission uses the same approximate text token accounting as
// compaction. UTF-8/non-ASCII text is not an exact tokenizer measurement; the
// separate byte/model bound remains authoritative for maximum exposure.
export const inputEstimate = (request: LLMRequest, bound = inputBound(request)) => {
  if (hasOpaqueInput(request)) return bound
  const estimateTool = (tool: ToolEntry): number =>
    Token.estimate(tool.name + (tool.description ?? "")) +
    (tool.type === "tool"
      ? Token.estimate(JSON.stringify(tool.inputSchema))
      : tool.tools.reduce((sum, entry) => sum + estimateTool(entry), 0))
  const estimatePart = (part: ContentPart): number => {
    if (part.type === "effort" || part.type === "media") return 0
    if (part.type === "compaction") return Token.estimate(part.text ?? "")
    if (part.type === "text" || part.type === "reasoning") return Token.estimate(part.text)
    if (part.type === "tool-call")
      return Token.estimate(part.id + part.name + (part.namespace ?? "") + (JSON.stringify(part.input) ?? ""))
    const overhead = Token.estimate(part.id + part.name + (part.namespace ?? ""))
    if (part.result.type === "content")
      return (
        overhead +
        part.result.value.reduce(
          (sum, content) => sum + (content.type === "text" ? Token.estimate(content.text) : 0),
          0,
        )
      )
    return (
      overhead +
      Token.estimate(
        typeof part.result.value === "string" ? part.result.value : (JSON.stringify(part.result.value) ?? ""),
      )
    )
  }
  return Math.min(
    bound,
    request.system.reduce((sum, part) => sum + Token.estimate(part.text), 0) +
      request.tools.reduce((sum, tool) => sum + estimateTool(tool), 0) +
      request.messages.reduce(
        (sum, message) =>
          sum + Token.estimate(message.role) + message.content.reduce((total, part) => total + estimatePart(part), 0),
        0,
      ),
  )
}

export const make = (client: LLMClientShape, endpoint: string, token: string, kv: KV.Interface, outputLimit = 8192) => {
  const replaying = Semaphore.makeUnsafe(1)
  let cursor: string | undefined
  const send = (path: string, body: unknown, tries = path === "/reserve" ? 1 : 3) =>
    Effect.tryPromise({
      try: async () => {
        // Admission is never retried: an accepted-but-lost response must not
        // authorize duplicate execution. Completion is idempotent and may retry.
        for (let attempt = 0; attempt < tries; attempt++) {
          try {
            const response = await fetch(endpoint + path, {
              method: "POST",
              headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
              body: JSON.stringify(body),
              signal: AbortSignal.timeout(5000),
            })
            if (!response.ok)
              throw failure(
                await response.text(),
                new HttpContext({ url: endpoint + path, status: response.status, headers: {} }),
              )
            return
          } catch (error) {
            if (attempt + 1 === tries) throw error
            await new Promise((resolve) => setTimeout(resolve, 50 * (attempt + 1)))
          }
        }
      },
      catch: (error) =>
        error instanceof AIError
          ? error
          : failure(error instanceof Error ? error.message : "Model budget is unavailable"),
    })
  const deliver = (record: typeof completion.Type, tries = 3) =>
    send(`/${record.operation}`, record, tries).pipe(Effect.tap(() => kv.remove(journalKey(record))))
  const decode = (entry: KV.Entry) =>
    Effect.gen(function* () {
      const value = yield* Schema.decodeUnknownEffect(completion)(entry.value).pipe(
        Effect.mapError(() => failure("Model budget callback journal is invalid")),
      )
      if (entry.key !== journalKey(value)) return yield* failure("Model budget callback journal identity mismatch")
      return value
    })
  const record = (value: typeof completion.Type) =>
    kv.set(journalKey(value), value).pipe(
      Effect.flatMap(() => deliver(value)),
      // Measured usage remains durable even when the bridge is unavailable.
      // Delivery failure must not consume the provider's native finish event.
      Effect.catch((error) =>
        Effect.logWarning("Model budget callback remains pending", {
          attemptID: value.attempt_id,
          error: error.message,
        }),
      ),
      Effect.uninterruptible,
    )
  const flush = (session?: string) =>
    Effect.gen(function* () {
      const page = yield* kv.scan({
        prefix: session === undefined ? journalPrefix : sessionPrefix(session),
        limit: 10,
      })
      for (const entry of page.entries) {
        yield* decode(entry).pipe(Effect.flatMap((value) => deliver(value)))
      }
      if (page.next !== undefined)
        return yield* failure("Model budget callbacks remain pending; retry before starting another call")
    }).pipe(
      Effect.timeout("20 seconds"),
      Effect.mapError((error) =>
        error instanceof AIError
          ? error
          : failure("Model budget callback replay timed out; retry before starting another call"),
      ),
    )
  const replay = replaying.withPermit(
    Effect.gen(function* () {
      const page = yield* kv.scan({ prefix: journalPrefix, after: cursor, limit: 10 })
      for (const entry of page.entries) {
        // A failed or malformed entry remains durable, but must not starve later
        // sessions. One try per row keeps a ten-row pass within 50 seconds.
        yield* decode(entry).pipe(
          Effect.flatMap((value) => deliver(value, 1)),
          Effect.catch((error) =>
            Effect.logWarning("Model budget replay remains pending", { key: entry.key, error: error.message }),
          ),
        )
      }
      cursor = page.next
    }),
  )
  const stream: LLMClientShape["stream"] = (request, options) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const session = request.http?.headers?.["x-opencode-session"]
        if (!session) return yield* failure("Model request has no billing session")
        if (request.tools.some((tool) => "native" in tool && tool.native !== undefined))
          return yield* failure("Provider-hosted tools require a separately priced budget")
        if (request.http?.body || request.model.defaults?.http?.body || request.model.route.defaults.http?.body)
          return yield* failure("Raw model body overrides are unavailable with enforced budgets")
        const acceptsOutputCap = request.model.compatibility?.supportsMaxOutputTokens !== false
        if (
          request.model.outputLimit !== undefined &&
          (!Number.isSafeInteger(request.model.outputLimit) || request.model.outputLimit < 1)
        )
          return yield* failure("Model has an invalid output limit for billing")
        const outputEstimate = Math.min(
          outputLimit,
          request.generation?.maxTokens ?? outputLimit,
          request.model.outputLimit ?? outputLimit,
        )
        const maxTokens = acceptsOutputCap ? outputEstimate : request.model.outputLimit
        if (maxTokens === undefined || !Number.isSafeInteger(maxTokens) || maxTokens < 1)
          return yield* failure("Model has no enforceable output limit for billing")
        if (!Number.isSafeInteger(outputEstimate) || outputEstimate < 1)
          return yield* failure("Model has an invalid output estimate for billing")
        const input = yield* Effect.try({
          try: () => inputBound(request),
          catch: () => failure("Model input cannot be bounded"),
        })
        // A session must settle its exact earlier callbacks before incurring
        // more exposure. Other sessions retain independent admission authority.
        yield* flush(session)
        const attempt = {
          session_id: session,
          attempt_id: crypto.randomUUID(),
          provider_id: request.model.provider,
          model_id: request.model.id,
        }
        const state = yield* Effect.acquireRelease(
          Effect.sync(() => ({ dispatched: false, admitted: false, uncertain: false })),
          (state) =>
            !state.dispatched && (state.admitted || state.uncertain)
              ? record({ ...attempt, operation: "cancel", usage: zeroUsage })
              : Effect.void,
        )
        yield* send("/reserve", {
          ...attempt,
          input_token_limit: input,
          output_token_limit: maxTokens,
          input_token_estimate: inputEstimate(request, input),
          output_token_estimate: outputEstimate,
        }).pipe(
          // Cancellation must not lose an accepted admission between its HTTP
          // response and the no-dispatch finalizer's state transition.
          Effect.tap(() =>
            Effect.sync(() => {
              state.admitted = true
            }),
          ),
          Effect.uninterruptible,
          Effect.onExit((exit) =>
            Effect.sync(() => {
              if (Exit.isSuccess(exit)) return
              const error = Option.getOrUndefined(Exit.findErrorOption(exit))
              const status = error?.reason.http?.status
              state.uncertain = status === undefined || status < 400 || status >= 500
            }),
          ),
        )
        // An endpoint that rejects the cap remains bounded by its model window.
        // Its smaller admission estimate is never sent as a provider hard cap.
        const bounded = acceptsOutputCap
          ? LLMRequest.update(request, { generation: GenerationOptions.make({ ...request.generation, maxTokens }) })
          : request
        let completed = false
        let observed = false
        const complete = (usage: (typeof completion.Type)["usage"]) =>
          record({ ...attempt, operation: "complete", usage }).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                completed = true
              }),
            ),
          )
        return Stream.suspend(() => {
          // From this point a provider outcome may be unknown. Never issue a
          // no-charge cancellation merely because this stream is interrupted.
          state.dispatched = true
          return client.stream(bounded, options)
        }).pipe(
          Stream.tap((event) => {
            observed = true
            if (completed || !LLMEvent.is.stepFinish(event) || !event.usage) return Effect.void
            const usage = event.usage
            if (!Number.isSafeInteger(usage.inputTokens) || !Number.isSafeInteger(usage.outputTokens))
              return Effect.fail(failure("Provider usage is incomplete; budget remains reserved"))
            // No usage / disconnected streams retain their durable reservation.
            // A later verified completion can retry this idempotent callback.
            return Schema.decodeUnknownEffect(completion.fields.usage)({
              input: usage.nonCachedInputTokens ?? usage.inputTokens ?? 0,
              output: usage.visibleOutputTokens,
              reasoning: usage.reasoningTokens ?? 0,
              cache_read: usage.cacheReadInputTokens ?? 0,
              cache_write: usage.cacheWriteInputTokens ?? 0,
            }).pipe(
              Effect.mapError(() => failure("Provider usage is incomplete; budget remains reserved")),
              Effect.flatMap(complete),
            )
          }),
          Stream.catch((error) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const reason = error.reason
                const status = reason.http?.status
                // A typed HTTP refusal before any provider output proves this
                // attempt did not execute. Interruptions and ambiguous transport
                // outcomes keep their hold until verified usage is available.
                const rejected =
                  ["InvalidRequest", "Authentication", "RateLimit", "QuotaExceeded"].includes(reason._tag) &&
                  status !== undefined &&
                  status >= 400 &&
                  status < 500
                if (!completed && !observed && rejected) yield* complete(zeroUsage)
                return Stream.fail(error)
              }),
            ),
          ),
        )
      }),
    )
  const generate: LLMClientShape["generate"] = (request, options) =>
    stream(request, options).pipe(
      Stream.runFold(LLMResponse.empty, LLMResponse.reduce),
      Effect.flatMap((state) => {
        const result = LLMResponse.complete(state)
        return result ? Effect.succeed(result) : Effect.fail(failure("Model response was incomplete"))
      }),
    )
  const compact: LLMClientShape["compact"] = (request) =>
    Effect.fail(
      new AIError({
        reason: new UnsupportedOperationError({
          operation: "compact",
          provider: request.model.provider,
          route: request.model.route.id,
          message: "Native compaction is unavailable while model budgets are enforced",
        }),
      }),
    )
  return {
    stream,
    generate,
    compact,
    flush,
    replay,
    maintain: replay.pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterrupts(cause),
        (cause) => Effect.logWarning("Model budget replay failed; callbacks remain durable", { cause }),
      ),
      Effect.repeat(Schedule.spaced("30 seconds")),
    ),
  }
}

export const layer = Layer.effect(
  LLMClient.Service,
  Effect.gen(function* () {
    const client = yield* LLMClient.Service
    const endpoint = process.env.OPENCODE_MODEL_BUDGET_ENDPOINT
    if (!endpoint) return client
    const token = process.env.OPENCODE_MODEL_BUDGET_TOKEN
    if (!token) return yield* Effect.die(new Error("Model budget token is required"))
    const kv = yield* KV.Service
    const budgeted = make(client, endpoint, token, kv)
    // Restart replay uses this runtime's current bridge credentials. No secret
    // or endpoint is retained in the callback journal.
    yield* budgeted.maintain.pipe(Effect.forkScoped)
    return budgeted
  }),
).pipe(Layer.provide(LLMClient.layer))
