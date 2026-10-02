import { expect, test } from "bun:test"
import { LLM, Message } from "@opencode/ai"
import { Media } from "@opencode/ai/media"
import { OpenAIChat, OpenAIResponses } from "@opencode/ai/protocols"
import { Auth, LLMClient, RequestExecutor } from "@opencode/ai/route"
import { Effect, Fiber, Layer, Stream } from "effect"
import { FetchHttpClient } from "effect/unstable/http"
import { ModelBudget } from "../src/model-budget.js"

test("admission precedes provider execution and sends an enforced output limit", async () => {
  const events: string[] = []
  let reject = false
  let completionFailures = 0
  let omitUsage = false
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      const body = (await req.json()) as Record<string, unknown>
      events.push(path)
      if (path === "/reserve") {
        expect(body.output_token_limit).toBe(128)
        return new Response("{}", { status: reject ? 402 : 200 })
      }
      if (path === "/complete") {
        if (completionFailures-- > 0) return new Response("temporary outage", { status: 503 })
        expect(body.usage).toEqual({ input: 10, output: 3, reasoning: 0, cache_read: 0, cache_write: 0 })
        return Response.json({ ok: true })
      }
      expect(body.max_completion_tokens).toBe(128)
      if (omitUsage) return new Response("data: [DONE]\n\n", { headers: { "content-type": "text/event-stream" } })
      return new Response(
        'data: {"id":"c1","object":"chat.completion.chunk","model":"test","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}\n\ndata: {"id":"c1","object":"chat.completion.chunk","model":"test","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13}}\n\ndata: [DONE]\n\n',
        { headers: { "content-type": "text/event-stream" } },
      )
    },
  })
  const layer = LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer))))
  const run = Effect.gen(function* () {
    const base = yield* LLMClient.Service
    const client = ModelBudget.make(base, server.url.origin, "test", 128)
    const request = LLM.request({
      model: OpenAIChat.route
        .with({ endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("test") })
        .model({ id: "test" }),
      http: { headers: { "x-opencode-session": "s1" } },
      prompt: "hello",
      generation: { maxTokens: 5000 },
    })
    return yield* client.stream(request).pipe(Stream.runCollect)
  }).pipe(Effect.provide(layer))
  try {
    await Effect.runPromise(run)
    expect(events).toEqual(["/reserve", "/v1/chat/completions", "/complete"])
    events.length = 0
    completionFailures = 1
    await Effect.runPromise(run)
    expect(events).toEqual(["/reserve", "/v1/chat/completions", "/complete", "/complete"])
    events.length = 0
    omitUsage = true
    await Effect.runPromise(run).catch(() => undefined)
    expect(events).toEqual(["/reserve", "/v1/chat/completions"])
    events.length = 0
    reject = true
    await expect(Effect.runPromise(run)).rejects.toThrow()
    expect(events).toEqual(["/reserve"])
  } finally {
    server.stop(true)
  }
})

test("reserves the full model window when an endpoint rejects output caps", async () => {
  const events: string[] = []
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      const body = (await req.json()) as Record<string, unknown>
      events.push(path)
      if (path === "/reserve") {
        expect(body.output_token_limit).toBe(512)
        return Response.json({ ok: true })
      }
      if (path === "/complete") {
        expect(body.usage).toEqual({ input: 0, output: 0, reasoning: 0, cache_read: 0, cache_write: 0 })
        return Response.json({ ok: true })
      }
      expect(path).toBe("/v1/responses")
      expect(body.max_output_tokens).toBeUndefined()
      return Response.json({ error: { message: "stop after inspecting request" } }, { status: 400 })
    },
  })
  const layer = LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer))))
  const run = Effect.gen(function* () {
    const base = yield* LLMClient.Service
    const client = ModelBudget.make(base, server.url.origin, "test", 128)
    const model = OpenAIResponses.route
      .with({ endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("test") })
      .model({ id: "test", compatibility: { supportsMaxOutputTokens: false }, outputLimit: 512 })
    return yield* client
      .stream(
        LLM.request({
          model,
          http: { headers: { "x-opencode-session": "s1" } },
          prompt: "hello",
          generation: { maxTokens: 32 },
        }),
      )
      .pipe(Stream.runCollect)
  }).pipe(Effect.provide(layer))
  try {
    await Effect.runPromise(run).catch(() => undefined)
    expect(events).toEqual(["/reserve", "/v1/responses", "/complete"])
    const withoutLimit = Effect.gen(function* () {
      const base = yield* LLMClient.Service
      const client = ModelBudget.make(base, server.url.origin, "test", 128)
      const model = OpenAIResponses.route
        .with({ endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("test") })
        .model({ id: "test", compatibility: { supportsMaxOutputTokens: false } })
      return yield* client
        .stream(LLM.request({ model, http: { headers: { "x-opencode-session": "s1" } }, prompt: "hello" }))
        .pipe(Stream.runCollect)
    }).pipe(Effect.provide(layer))
    await expect(Effect.runPromise(withoutLimit)).rejects.toThrow("Model has no enforceable output limit")
    expect(events).toEqual(["/reserve", "/v1/responses", "/complete"])
  } finally {
    server.stop(true)
  }
})

test("input bounds use each model window and media requires a declared limit", () => {
  const route = OpenAIChat.route.with({ auth: Auth.bearer("test") })
  for (const inputLimit of [256, 512]) {
    const model = route.model({ id: `window-${inputLimit}`, inputLimit })
    const long = LLM.request({ model, prompt: "history".repeat(10000) })
    expect(ModelBudget.inputBound(long)).toBe(inputLimit)
    const media = LLM.request({ model, messages: [Message.user(Message.media(Media.base64("aGVsbG8=", "image/png")))] })
    expect(ModelBudget.inputBound(media)).toBe(inputLimit)
  }
  const small = LLM.request({ model: route.model({ id: "small", inputLimit: 10000 }), prompt: "hello" })
  expect(ModelBudget.inputBound(small)).toBeLessThan(10000)
  const unknown = LLM.request({
    model: route.model({ id: "unknown" }),
    messages: [Message.user(Message.media(Media.base64("aGVsbG8=", "image/png")))],
  })
  expect(() => ModelBudget.inputBound(unknown)).toThrow("Model has no input limit for media billing")
  const invalid = LLM.request({ model: route.model({ id: "invalid", inputLimit: 0 }), prompt: "hello" })
  expect(() => ModelBudget.inputBound(invalid)).toThrow("Model has an invalid input limit for billing")
})

test("ambiguous provider failure retains the hold", async () => {
  const events: string[] = []
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      events.push(path)
      if (path === "/reserve") return Response.json({ ok: true })
      return Response.json({ error: { message: "provider unavailable" } }, { status: 500 })
    },
  })
  const layer = LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer))))
  const run = Effect.gen(function* () {
    const base = yield* LLMClient.Service
    const client = ModelBudget.make(base, server.url.origin, "test", 128)
    const model = OpenAIChat.route
      .with({ endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("test") })
      .model({ id: "test", inputLimit: 1024 })
    return yield* client
      .stream(LLM.request({ model, http: { headers: { "x-opencode-session": "s2" } }, prompt: "hello" }))
      .pipe(Stream.runCollect)
  }).pipe(Effect.provide(layer))
  try {
    await expect(Effect.runPromise(run)).rejects.toThrow()
    expect(events).toEqual(["/reserve", "/v1/chat/completions"])
  } finally {
    server.stop(true)
  }
})

test("interrupt waits for confirmed usage to finish its completion callback", async () => {
  const entered = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  let completed = false
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      if (path === "/reserve") return Response.json({ ok: true })
      if (path === "/complete") {
        entered.resolve()
        await release.promise
        completed = true
        return Response.json({ ok: true })
      }
      return new Response(
        'data: {"id":"c1","object":"chat.completion.chunk","model":"test","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}\n\ndata: {"id":"c1","object":"chat.completion.chunk","model":"test","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13}}\n\ndata: [DONE]\n\n',
        { headers: { "content-type": "text/event-stream" } },
      )
    },
  })
  const layer = LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer))))
  const run = Effect.gen(function* () {
    const base = yield* LLMClient.Service
    const client = ModelBudget.make(base, server.url.origin, "test", 128)
    const model = OpenAIChat.route
      .with({ endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("test") })
      .model({ id: "test", inputLimit: 1024 })
    return yield* client
      .stream(LLM.request({ model, http: { headers: { "x-opencode-session": "s3" } }, prompt: "hello" }))
      .pipe(Stream.runCollect)
  }).pipe(Effect.provide(layer))
  const fiber = Effect.runFork(run)
  try {
    await entered.promise
    let stopped = false
    const stopping = Effect.runPromise(Fiber.interrupt(fiber)).then(() => {
      stopped = true
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(stopped).toBe(false)
    release.resolve()
    await stopping
    expect(completed).toBe(true)
  } finally {
    release.resolve()
    await Effect.runPromise(Fiber.interrupt(fiber))
    server.stop(true)
  }
})
