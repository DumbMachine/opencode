import { expect, test } from "bun:test"
import { AIError, LLM, Message, TransportError } from "@opencode/ai"
import { SessionModelTransport } from "@opencode/core/session/model-transport"
import { Session } from "@opencode/schema/session"
import { Media } from "@opencode/ai/media"
import { OpenAIChat, OpenAIResponses } from "@opencode/ai/protocols"
import { Auth, LLMClient, RequestExecutor } from "@opencode/ai/route"
import { Effect, Fiber, Layer, Stream } from "effect"
import { FetchHttpClient } from "effect/unstable/http"
import { ModelBudget } from "../src/model-budget.js"
import { KV } from "../src/kv.js"
import { LayerNode } from "@opencode/util/effect/layer-node"

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
      if (path === "/reserve" || path === "/complete" || path === "/cancel")
        expect(body.input_id).toBe("msg_budget_input")
      if (path === "/reserve") {
        expect(body.output_token_limit).toBe(128)
        expect(body.output_token_estimate).toBe(128)
        expect(body.input_token_estimate).toBeGreaterThanOrEqual(0)
        expect(body.input_token_estimate).toBeLessThan(Number(body.input_token_limit))
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
  const layer = Layer.merge(
    LayerNode.compile(KV.node),
    LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
  )
  const run = Effect.gen(function* () {
    const base = yield* LLMClient.Service
    const journal = yield* KV.Service
    const client = ModelBudget.make(base, server.url.origin, "test", journal, 128)
    const request = LLM.request({
      model: OpenAIChat.route
        .with({ endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("test") })
        .model({ id: "test" }),
      http: { headers: { "x-opencode-session": "s1", "x-opencode-input": "msg_budget_input" } },
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

test("pairs an output estimate with the full window when an endpoint rejects caps", async () => {
  const events: string[] = []
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      const body = (await req.json()) as Record<string, unknown>
      events.push(path)
      if (path === "/reserve") {
        expect(body.output_token_limit).toBe(512)
        expect(body.output_token_estimate).toBe(32)
        expect(Number.isInteger(body.input_token_estimate)).toBe(true)
        expect(body.input_token_estimate).toBeLessThan(Number(body.input_token_limit))
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
  const layer = Layer.merge(
    LayerNode.compile(KV.node),
    LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
  )
  const run = Effect.gen(function* () {
    const base = yield* LLMClient.Service
    const journal = yield* KV.Service
    const client = ModelBudget.make(base, server.url.origin, "test", journal, 128)
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
      const journal = yield* KV.Service
      const client = ModelBudget.make(base, server.url.origin, "test", journal, 128)
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
    expect(ModelBudget.inputEstimate(long)).toBe(inputLimit)
    const media = LLM.request({ model, messages: [Message.user(Message.media(Media.base64("aGVsbG8=", "image/png")))] })
    expect(ModelBudget.inputBound(media)).toBe(inputLimit)
    expect(ModelBudget.inputEstimate(media)).toBe(inputLimit)
  }
  const small = LLM.request({ model: route.model({ id: "small", inputLimit: 10000 }), prompt: "hello" })
  expect(ModelBudget.inputBound(small)).toBeLessThan(10000)
  const unknown = LLM.request({
    model: route.model({ id: "unknown" }),
    messages: [Message.user(Message.media(Media.base64("aGVsbG8=", "image/png")))],
  })
  expect(() => ModelBudget.inputBound(unknown)).toThrow("Model has no input limit for media billing")
  expect(() => ModelBudget.inputEstimate(unknown)).toThrow("Model has no input limit for media billing")
  const invalid = LLM.request({ model: route.model({ id: "invalid", inputLimit: 0 }), prompt: "hello" })
  expect(() => ModelBudget.inputBound(invalid)).toThrow("Model has an invalid input limit for billing")
})

test("text estimates count provider content without treating local metadata as tokens", () => {
  const model = OpenAIChat.route.with({ auth: Auth.bearer("test") }).model({ id: "estimate", inputLimit: 100000 })
  const tool = { name: "lookup", description: "Look up a saved record", inputSchema: { type: "object" } }
  const request = LLM.request({
    model,
    system: "Use the connected workspace tools.",
    messages: [Message.user("Summarize the saved record. ".repeat(10))],
    tools: [{ type: "namespace", name: "workspace", tools: [tool] }],
  })
  const estimate = ModelBudget.inputEstimate(request)
  expect(Number.isInteger(estimate)).toBe(true)
  expect(estimate).toBeGreaterThan(0)
  expect(estimate).toBeLessThan(ModelBudget.inputBound(request))
  const local = "local accounting metadata ".repeat(10000)
  const annotated = LLM.request({
    ...request,
    metadata: { local },
    system: [{ type: "text", text: request.system[0].text, metadata: { local } }],
    messages: [
      Message.user({
        type: "text",
        text: request.messages[0].content[0].type === "text" ? request.messages[0].content[0].text : "",
        metadata: { local },
      }),
    ],
    tools: [{ type: "namespace", name: "workspace", tools: [{ ...tool, metadata: { local } }] }],
  })
  expect(ModelBudget.inputEstimate(annotated)).toBe(estimate)
  const toolResult = LLM.request({
    ...request,
    messages: [
      ...request.messages,
      Message.assistant([{ type: "tool-call", id: "call-1", name: "lookup", input: { id: "1" } }]),
      Message.tool({ id: "call-1", name: "lookup", result: { saved: "record content ".repeat(10) } }),
    ],
  })
  expect(ModelBudget.inputEstimate(toolResult)).toBeGreaterThan(estimate)
  const fileResult = LLM.request({
    model,
    messages: [
      Message.tool({
        id: "call-file",
        name: "lookup",
        resultType: "content",
        result: [
          { type: "file", mime: "application/pdf", name: "fixture.pdf", uri: "data:application/pdf;base64,aGVsbG8=" },
        ],
      }),
    ],
  })
  expect(ModelBudget.inputEstimate(fileResult)).toBe(model.inputLimit!)
})

test("UTF-8 input estimates remain distinct from byte bounds and clamp to each window", () => {
  const route = OpenAIChat.route.with({ auth: Auth.bearer("test") })
  for (const prompt of ["ASCII context ".repeat(100), "भारत में ऐप 🌏 ".repeat(100)]) {
    const request = LLM.request({ model: route.model({ id: "utf8", inputLimit: 100000 }), prompt })
    const estimate = ModelBudget.inputEstimate(request)
    const bound = ModelBudget.inputBound(request)
    expect(Number.isInteger(estimate)).toBe(true)
    expect(estimate).toBeGreaterThan(0)
    expect(estimate).toBeLessThan(bound)
    expect(bound).toBeGreaterThanOrEqual(new TextEncoder().encode(prompt).byteLength)
    const small = LLM.request({ model: route.model({ id: "small", inputLimit: 32 }), prompt })
    expect(ModelBudget.inputEstimate(small)).toBe(32)
    expect(ModelBudget.inputBound(small)).toBe(32)
  }
})

test("the default output estimate preserves API wire caps and uncapped model maxima", async () => {
  const admissions: Array<Record<string, number>> = []
  const bodies: Array<Record<string, unknown>> = []
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      const body = (await req.json()) as Record<string, number>
      if (path === "/reserve") {
        admissions.push(body)
        return Response.json({ ok: true })
      }
      if (path === "/complete") return Response.json({ ok: true })
      bodies.push(body)
      return Response.json({ error: { message: "fixture refuses before execution" } }, { status: 400 })
    },
  })
  const layer = Layer.merge(
    LayerNode.compile(KV.node),
    LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
  )
  const run = (supportsMaxOutputTokens: boolean) =>
    Effect.gen(function* () {
      const base = yield* LLMClient.Service
      const journal = yield* KV.Service
      const client = ModelBudget.make(base, server.url.origin, "test", journal)
      return yield* client
        .stream(
          LLM.request({
            model: OpenAIResponses.route
              .with({ endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("test") })
              .model({ id: "test", compatibility: { supportsMaxOutputTokens }, outputLimit: 128000 }),
            http: { headers: { "x-opencode-session": "output-default" } },
            prompt: "hello",
          }),
        )
        .pipe(Stream.runCollect)
    }).pipe(Effect.provide(layer))
  try {
    for (const accepts of [true, false]) await Effect.runPromise(run(accepts)).catch(() => undefined)
    expect(admissions.map((body) => body.output_token_estimate)).toEqual([8192, 8192])
    expect(admissions.map((body) => body.output_token_limit)).toEqual([8192, 128000])
    expect(bodies.map((body) => body.max_output_tokens)).toEqual([8192, undefined])
  } finally {
    server.stop(true)
  }
})

test("capable endpoints reserve and send no more than the declared output window", async () => {
  const limits: number[] = []
  const estimates: number[] = []
  const sent: number[] = []
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      const body = (await req.json()) as Record<string, number>
      if (path === "/reserve") {
        limits.push(body.output_token_limit)
        estimates.push(body.output_token_estimate)
        return Response.json({ ok: true })
      }
      if (path === "/complete") return Response.json({ ok: true })
      sent.push(body.max_completion_tokens)
      return Response.json({ error: { message: "fixture rejects without execution" } }, { status: 400 })
    },
  })
  const layer = Layer.merge(
    LayerNode.compile(KV.node),
    LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
  )
  const run = (outputLimit: number) =>
    Effect.gen(function* () {
      const base = yield* LLMClient.Service
      const journal = yield* KV.Service
      const client = ModelBudget.make(base, server.url.origin, "test", journal, 128)
      const model = OpenAIChat.route
        .with({ endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("test") })
        .model({ id: "test", outputLimit })
      return yield* client
        .stream(
          LLM.request({
            model,
            http: { headers: { "x-opencode-session": "s-output" } },
            prompt: "hello",
            generation: { maxTokens: 5000 },
          }),
        )
        .pipe(Stream.runCollect)
    }).pipe(Effect.provide(layer))
  try {
    for (const window of [64, 256]) await Effect.runPromise(run(window)).catch(() => undefined)
    expect(limits).toEqual([64, 128])
    expect(estimates).toEqual([64, 128])
    expect(sent).toEqual([64, 128])
    for (const invalid of [0, -1, 10000.5, Number.NaN])
      await expect(Effect.runPromise(run(invalid))).rejects.toThrow("invalid output limit")
    expect(limits).toEqual([64, 128])
  } finally {
    server.stop(true)
  }
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
  const layer = Layer.merge(
    LayerNode.compile(KV.node),
    LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
  )
  const run = Effect.gen(function* () {
    const base = yield* LLMClient.Service
    const journal = yield* KV.Service
    const client = ModelBudget.make(base, server.url.origin, "test", journal, 128)
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
  const layer = Layer.merge(
    LayerNode.compile(KV.node),
    LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
  )
  const run = Effect.gen(function* () {
    const base = yield* LLMClient.Service
    const journal = yield* KV.Service
    const client = ModelBudget.make(base, server.url.origin, "test", journal, 128)
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

test("budgeted native compaction fails explicitly before HTTP or journaling for multiple routes", async () => {
  const events: string[] = []
  const server = Bun.serve({
    port: 0,
    fetch(req) {
      events.push(new URL(req.url).pathname)
      return new Response("unexpected request", { status: 500 })
    },
  })
  const layer = Layer.merge(
    LayerNode.compile(KV.node),
    LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
  )
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const base = yield* LLMClient.Service
        const journal = yield* KV.Service
        const client = ModelBudget.make(base, server.url.origin, "fixture", journal)
        for (const id of ["compact-first", "compact-second"]) {
          const model = OpenAIResponses.route
            .with({ id, endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("fixture") })
            .model({ id: "test" })
          const input = LLM.request({ model, http: { headers: { "x-opencode-session": id } }, prompt: "fixture" })
          if (!LLMClient.canCompact(input)) throw new Error("fixture requires an endpoint compaction route")
          yield* client.compact(input).pipe(
            Effect.flip,
            Effect.tap((error) =>
              Effect.sync(() => {
                expect(error.reason._tag).toBe("UnsupportedOperation")
                expect(error.message).toContain("Native compaction is unavailable")
              }),
            ),
          )
          if (!LLMClient.canCompact(input, { mechanism: "trigger" }))
            throw new Error("fixture requires a trigger compaction route")
          yield* client.compact(input, { mechanism: "trigger" }).pipe(
            Effect.flip,
            Effect.tap((error) => Effect.sync(() => expect(error.reason._tag).toBe("UnsupportedOperation"))),
          )
        }
        expect((yield* journal.scan({ prefix: "model-budget:callbacks/" })).entries).toEqual([])
        expect(events).toEqual([])
      }).pipe(Effect.provide(layer)),
    )
  } finally {
    server.stop(true)
  }
})

for (const boundary of ["reserve", "transport-preparation"] as const) {
  test(`cancels a reservation when physical admission ends during ${boundary}`, async () => {
    const events: string[] = []
    let live = true
    const server = Bun.serve({
      port: 0,
      async fetch(req) {
        const path = new URL(req.url).pathname
        await req.json()
        events.push(path)
        if (path === "/reserve" && boundary === "reserve") live = false
        return Response.json({ ok: true })
      },
    })
    const layer = Layer.merge(
      LayerNode.compile(KV.node),
      LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
    )
    try {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const base = yield* LLMClient.Service
          const client = ModelBudget.make(base, server.url.origin, "test", yield* KV.Service, 128)
          const request = LLM.request({
            model: OpenAIChat.route
              .with({
                endpoint: { baseURL: server.url.origin + "/v1/" },
                auth: Auth.custom((input) =>
                  Effect.sync(() => {
                    if (boundary === "transport-preparation") live = false
                    return input.headers
                  }),
                ),
              })
              .model({ id: "test" }),
            http: { headers: { "x-opencode-session": `physical-${boundary}`, "x-opencode-input": "msg_original" } },
            prompt: "Original input",
          })
          return yield* client
            .stream(request, {
              beforeDispatch: Effect.sync(() => {
                if (!live) throw new Error("Admission ended")
              }),
            })
            .pipe(Stream.runDrain, Effect.exit)
        }).pipe(Effect.provide(layer)),
      )
      expect(result._tag).toBe("Failure")
      expect(events).toEqual(["/reserve", "/cancel"])
    } finally {
      server.stop(true)
    }
  })
}

for (const interruptedCleanup of [false, true]) {
  test(`cancels after proven unsent WebSocket with ${interruptedCleanup ? "interrupted cleanup" : "denied HTTP fallback"}`, async () => {
    const events: string[] = []
    const closing = Promise.withResolvers<void>()
    const releaseClose = Promise.withResolvers<void>()
    let live = true,
      attempted = 0
    const server = Bun.serve({
      port: 0,
      async fetch(req) {
        events.push(new URL(req.url).pathname)
        await req.json()
        return Response.json({ ok: true })
      },
    })
    const layer = Layer.mergeAll(
      LayerNode.compile(KV.node),
      LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
      SessionModelTransport.makeLayer({
        open: () =>
          Effect.succeed({
            sendText: () =>
              Effect.sync(() => {
                live = false
                attempted++
              }).pipe(
                Effect.andThen(
                  Effect.fail(
                    new AIError({
                      reason: new TransportError({
                        message: "Socket closed before send",
                        transport: "websocket",
                        operation: "write",
                        phase: "send",
                        delivery: "not-sent",
                      }),
                    }),
                  ),
                ),
              ),
            messages: Stream.empty,
            close: interruptedCleanup
              ? Effect.promise(() => {
                  closing.resolve()
                  return releaseClose.promise
                })
              : Effect.void,
          }),
      }),
    )
    try {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const base = yield* LLMClient.Service
          const transport = yield* SessionModelTransport.Service
          const client = ModelBudget.make(base, server.url.origin, "test", yield* KV.Service, 128)
          const request = LLM.request({
            model: OpenAIResponses.route
              .with({ endpoint: { baseURL: server.url.origin + "/v1/" }, auth: Auth.bearer("test") })
              .model({ id: "test" }),
            http: { headers: { "x-opencode-session": "ses_unsent_fallback", "x-opencode-input": "msg_original" } },
            prompt: "Original input",
          })
          const operation = client
            .stream(request, {
              webSocket: transport.bind(Session.ID.make("ses_unsent_fallback")),
              beforeDispatch: Effect.sync(() => {
                if (!live) throw new Error("Admission ended")
              }),
            })
            .pipe(Stream.runDrain)
          if (!interruptedCleanup) return yield* Effect.exit(operation)
          const pending = yield* Effect.forkChild(operation)
          yield* Effect.promise(() => closing.promise)
          const interrupted = yield* Fiber.interrupt(pending).pipe(Effect.forkChild)
          // Let interruption reach the blocked close before releasing cleanup.
          yield* Effect.yieldNow
          releaseClose.resolve()
          yield* Fiber.join(interrupted)
          return yield* Fiber.await(pending)
        }).pipe(Effect.provide(layer), Effect.scoped),
      )
      expect(result._tag).toBe("Failure")
      expect(attempted).toBe(1)
      expect(events).toEqual(["/reserve", "/cancel"])
    } finally {
      releaseClose.resolve()
      server.stop(true)
    }
  })
}
