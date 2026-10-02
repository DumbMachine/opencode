export * as ModelBudget from "./model-budget.js"

import { AIError, GenerationOptions, LLMEvent, LLMRequest, LLMResponse, QuotaExceededError } from "@opencode/ai"
import { LLMClient, type LLMClientShape } from "@opencode/ai/route"
import { Effect, Layer, Stream } from "effect"

const failure = (message: string) => new AIError({ reason: new QuotaExceededError({ message }) })

// A byte bound avoids tokenizer/network work on the first-token path. For
// media that can expand at the provider, reserve the entire model input window.
export const inputBound = (request: LLMRequest) => {
  const text = JSON.stringify({ system: request.system, messages: request.messages, tools: request.tools })
  const opaque = request.messages.some((message) =>
    message.content.some(
      (part) =>
        part.type === "media" ||
        (part.type === "tool-result" &&
          part.result.type === "content" &&
          part.result.value.some((content) => content.type === "file")),
    ),
  )
  const limit = request.model.inputLimit
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1))
    throw new Error("Model has an invalid input limit for billing")
  if (opaque) {
    if (limit === undefined) throw new Error("Model has no input limit for media billing")
    return limit
  }
  const bytes = new TextEncoder().encode(text).byteLength + 256 * (request.messages.length + request.tools.length + 1)
  // The provider cannot accept more than its model's input window. A local
  // byte upper bound must never reserve more tokens than that entire window.
  return limit === undefined ? bytes : Math.min(bytes, limit)
}

export const make = (client: LLMClientShape, endpoint: string, token: string, outputLimit = 8192): LLMClientShape => {
  const send = (path: string, body: unknown) =>
    Effect.tryPromise({
      try: async () => {
        // Admission is never retried: an accepted-but-lost response must not
        // authorize duplicate execution. Completion is idempotent and may retry.
        const tries = path === "/complete" ? 3 : 1
        for (let attempt = 0; attempt < tries; attempt++) {
          try {
            const response = await fetch(endpoint + path, {
              method: "POST",
              headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
              body: JSON.stringify(body),
              signal: AbortSignal.timeout(5000),
            })
            if (!response.ok) throw new Error(await response.text())
            return
          } catch (error) {
            if (attempt + 1 === tries) throw error
            await new Promise((resolve) => setTimeout(resolve, 50 * (attempt + 1)))
          }
        }
      },
      catch: (error) => failure(error instanceof Error ? error.message : "Model budget is unavailable"),
    })
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
        const maxTokens = acceptsOutputCap
          ? Math.min(outputLimit, request.generation?.maxTokens ?? outputLimit)
          : request.model.outputLimit
        if (maxTokens === undefined || !Number.isSafeInteger(maxTokens) || maxTokens < 1)
          return yield* failure("Model has no enforceable output limit for billing")
        const input = yield* Effect.try({
          try: () => inputBound(request),
          catch: () => failure("Model input cannot be bounded"),
        })
        const attempt = {
          session_id: session,
          attempt_id: crypto.randomUUID(),
          provider_id: request.model.provider,
          model_id: request.model.id,
        }
        yield* send("/reserve", { ...attempt, input_token_limit: input, output_token_limit: maxTokens })
        // A model whose endpoint rejects the cap is bounded by its published
        // maximum output window. Reserve that full window before the call.
        const bounded = acceptsOutputCap
          ? LLMRequest.update(request, { generation: GenerationOptions.make({ ...request.generation, maxTokens }) })
          : request
        let completed = false
        let observed = false
        const complete = (usage: {
          input: number
          output: number
          reasoning: number
          cache_read: number
          cache_write: number
        }) =>
          send("/complete", { ...attempt, usage }).pipe(
            Effect.uninterruptible,
            Effect.tap(() =>
              Effect.sync(() => {
                completed = true
              }),
            ),
          )
        return client.stream(bounded, options).pipe(
          Stream.tap((event) => {
            observed = true
            if (completed || !LLMEvent.is.stepFinish(event) || !event.usage) return Effect.void
            const usage = event.usage
            if (!Number.isSafeInteger(usage.inputTokens) || !Number.isSafeInteger(usage.outputTokens))
              return Effect.fail(failure("Provider usage is incomplete; budget remains reserved"))
            // No usage / disconnected streams retain their durable reservation.
            // A later verified completion can retry this idempotent callback.
            return complete({
              input: usage.nonCachedInputTokens ?? usage.inputTokens ?? 0,
              output: usage.visibleOutputTokens,
              reasoning: usage.reasoningTokens ?? 0,
              cache_read: usage.cacheReadInputTokens ?? 0,
              cache_write: usage.cacheWriteInputTokens ?? 0,
            })
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
                if (!completed && !observed && rejected)
                  yield* complete({ input: 0, output: 0, reasoning: 0, cache_read: 0, cache_write: 0 })
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
  return { stream, generate, compact: client.compact }
}

export const layer = Layer.effect(
  LLMClient.Service,
  Effect.gen(function* () {
    const client = yield* LLMClient.Service
    const endpoint = process.env.OPENCODE_MODEL_BUDGET_ENDPOINT
    if (!endpoint) return client
    const token = process.env.OPENCODE_MODEL_BUDGET_TOKEN
    if (!token) return yield* Effect.die(new Error("Model budget token is required"))
    return make(client, endpoint, token)
  }),
).pipe(Layer.provide(LLMClient.layer))
