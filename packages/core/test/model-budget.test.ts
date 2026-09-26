import { expect, test } from "bun:test"
import { LLM } from "@opencode/ai"
import { OpenAIChat } from "@opencode/ai/protocols"
import { Auth, LLMClient, RequestExecutor } from "@opencode/ai/route"
import { Effect, Layer, Stream } from "effect"
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
        if (completionFailures-- > 0) return new Response("temporary outage", {status: 503})
        expect(body.usage).toEqual({ input: 10, output: 3, reasoning: 0, cache_read: 0, cache_write: 0 })
        return Response.json({ ok: true })
      }
      expect(body.max_tokens).toBe(128)
      if (omitUsage) return new Response('data: [DONE]\n\n', {headers: {"content-type": "text/event-stream"}})
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
