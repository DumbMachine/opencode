import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { LLM, LLMEvent } from "@opencode/ai"
import { OpenAIChat } from "@opencode/ai/protocols"
import { Auth, LLMClient, RequestExecutor } from "@opencode/ai/route"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Effect, Fiber, Layer, Logger, Stream } from "effect"
import { TestClock } from "effect/testing"
import { FetchHttpClient } from "effect/unstable/http"
import { Database } from "../src/database/database.js"
import { KV } from "../src/kv.js"
import { ModelBudget } from "../src/model-budget.js"

const prefix = "model-budget:callbacks/"
const zero = { input: 0, output: 0, reasoning: 0, cache_read: 0, cache_write: 0 }
const layer = (path = ":memory:") =>
  Layer.merge(
    LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer)))),
    LayerNode.compile(KV.node, { replacements: [Database.node.replace(Database.sqliteDatabaseLayer(path))] }),
  )
const request = (origin: string, session: string) =>
  LLM.request({
    model: OpenAIChat.route
      .with({ endpoint: { baseURL: origin + "/v1/" }, auth: Auth.bearer("fixture") })
      .model({ id: "test" }),
    http: { headers: { "x-opencode-session": session } },
    prompt: "fixture",
    generation: { maxTokens: 128 },
  })
const providerReply = () =>
  new Response(
    'data: {"id":"c1","object":"chat.completion.chunk","model":"test","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}\n\ndata: {"id":"c1","object":"chat.completion.chunk","model":"test","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13}}\n\ndata: [DONE]\n\n',
    { headers: { "content-type": "text/event-stream" } },
  )

test("failed delivery preserves native finish and exact callbacks across a real KV restart", async () => {
  const directory = await mkdtemp(join(tmpdir(), "opencode-budget-journal-"))
  const database = join(directory, "journal.sqlite")
  const events: string[] = []
  const attempts: string[] = []
  let kv: KV.Interface
  const old = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      const body = (await req.json()) as Record<string, KV.Value>
      events.push(path)
      if (path === "/reserve") {
        attempts.push(body.attempt_id as string)
        return Response.json({ ok: true })
      }
      if (path === "/complete") {
        const pending = await Effect.runPromise(kv.scan({ prefix }))
        expect(pending.entries).toHaveLength(1)
        expect(pending.entries[0].value).toEqual(body)
        return new Response("fixture callback outage", { status: 503 })
      }
      return providerReply()
    },
  })
  let pending: KV.Entry[] = []
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const base = yield* LLMClient.Service
        kv = yield* KV.Service
        const client = ModelBudget.make(base, old.url.origin, "old-private-fixture-token", kv, 128)
        const result = yield* client.stream(request(old.url.origin, "session-a")).pipe(Stream.runCollect)
        const finished = result.filter(LLMEvent.is.stepFinish)
        expect(finished).toHaveLength(1)
        expect(finished[0].usage?.inputTokens).toBe(10)
        expect(finished[0].usage?.visibleOutputTokens).toBe(3)
        pending = [...(yield* kv.scan({ prefix })).entries]
        yield* client.stream(request(old.url.origin, "session-a")).pipe(
          Stream.runCollect,
          Effect.flip,
          Effect.tap((error) => Effect.sync(() => expect(error.message).toContain("fixture callback outage"))),
        )
        expect(attempts).toHaveLength(1)
        expect(events.filter((event) => event === "/v1/chat/completions")).toHaveLength(1)
      }).pipe(Effect.provide(layer(database))),
    )
    expect(pending).toHaveLength(1)
    expect(pending[0].value).toEqual({
      operation: "complete",
      session_id: "session-a",
      attempt_id: attempts[0],
      provider_id: "openai",
      model_id: "test",
      usage: { ...zero, input: 10, output: 3 },
    })
    expect(JSON.stringify(pending)).not.toContain("private-fixture-token")
    expect(JSON.stringify(pending)).not.toContain(old.url.origin)
    old.stop(true)
    const replay = Bun.serve({
      port: 0,
      async fetch(req) {
        expect(new URL(req.url).pathname).toBe("/complete")
        expect(req.headers.get("authorization")).toBe("Bearer new-private-fixture-token")
        expect(await req.json()).toEqual(pending[0].value)
        return Response.json({ ok: true })
      },
    })
    try {
      await Effect.runPromise(
        Effect.gen(function* () {
          const base = yield* LLMClient.Service
          const journal = yield* KV.Service
          const client = ModelBudget.make(base, replay.url.origin, "new-private-fixture-token", journal)
          expect((yield* journal.scan({ prefix })).entries).toEqual(pending)
          yield* client.flush()
          expect((yield* journal.scan({ prefix })).entries).toEqual([])
        }).pipe(Effect.provide(layer(database))),
      )
    } finally {
      replay.stop(true)
    }
  } finally {
    old.stop(true)
    await rm(directory, { recursive: true, force: true })
  }
})

test("lost admission response cancels only the exact attempt without invoking a provider", async () => {
  const events: string[] = []
  let accepted: string | undefined
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      const body = (await req.json()) as Record<string, KV.Value>
      events.push(path)
      if (path === "/reserve") {
        accepted = body.attempt_id as string
        await Bun.sleep(5100)
        return Response.json({ ok: true })
      }
      expect(path).toBe("/cancel")
      expect(body).toEqual({
        operation: "cancel",
        session_id: "lost-admission",
        attempt_id: accepted!,
        provider_id: "openai",
        model_id: "test",
        usage: zero,
      })
      return Response.json({ ok: true })
    },
  })
  try {
    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const base = yield* LLMClient.Service
          const journal = yield* KV.Service
          const client = ModelBudget.make(base, server.url.origin, "fixture", journal, 128)
          return yield* client.stream(request(server.url.origin, "lost-admission")).pipe(Stream.runCollect)
        }).pipe(Effect.provide(layer())),
      ),
    ).rejects.toThrow()
    expect(events).toEqual(["/reserve", "/cancel"])
  } finally {
    server.stop(true)
  }
}, 10_000)

test("interrupt between admission and invocation journals no-dispatch cancellation", async () => {
  const entered = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  const events: string[] = []
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      events.push(path)
      if (path === "/reserve") {
        entered.resolve()
        await release.promise
        return Response.json({ ok: true })
      }
      expect(path).toBe("/cancel")
      expect(((await req.json()) as Record<string, KV.Value>).usage).toEqual(zero)
      return Response.json({ ok: true })
    },
  })
  const fiber = Effect.runFork(
    Effect.gen(function* () {
      const base = yield* LLMClient.Service
      const journal = yield* KV.Service
      const client = ModelBudget.make(base, server.url.origin, "fixture", journal, 128)
      return yield* client.stream(request(server.url.origin, "interrupted-admission")).pipe(Stream.runCollect)
    }).pipe(Effect.provide(layer())),
  )
  try {
    await entered.promise
    const stopped = Effect.runPromise(Fiber.interrupt(fiber))
    await Bun.sleep(0)
    release.resolve()
    await stopped
    expect(events).toEqual(["/reserve", "/cancel"])
  } finally {
    release.resolve()
    await Effect.runPromise(Fiber.interrupt(fiber))
    server.stop(true)
  }
})

test("an interrupted invoked provider retains unknown exposure without a zero callback", async () => {
  const invoked = Promise.withResolvers<void>()
  const events: string[] = []
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname
      events.push(path)
      if (path === "/reserve") return Response.json({ ok: true })
      invoked.resolve()
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(
              new TextEncoder().encode(
                'data: {"id":"c1","object":"chat.completion.chunk","model":"test","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}\n\n',
              ),
            )
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      )
    },
  })
  const fiber = Effect.runFork(
    Effect.gen(function* () {
      const base = yield* LLMClient.Service
      const journal = yield* KV.Service
      const client = ModelBudget.make(base, server.url.origin, "fixture", journal, 128)
      return yield* client.stream(request(server.url.origin, "unknown-provider")).pipe(Stream.runCollect)
    }).pipe(Effect.provide(layer())),
  )
  try {
    await invoked.promise
    await Effect.runPromise(Fiber.interrupt(fiber))
    expect(events).toEqual(["/reserve", "/v1/chat/completions"])
  } finally {
    await Effect.runPromise(Fiber.interrupt(fiber))
    server.stop(true)
  }
})

test("replay validates exact journal identity and is bounded before further admission", async () => {
  const events: string[] = []
  const server = Bun.serve({
    port: 0,
    fetch(req) {
      events.push(new URL(req.url).pathname)
      return Response.json({ ok: true })
    },
  })
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const base = yield* LLMClient.Service
        const journal = yield* KV.Service
        const client = ModelBudget.make(base, server.url.origin, "fixture", journal)
        const identity = {
          operation: "complete",
          session_id: "bounded",
          attempt_id: "mismatch",
          provider_id: "openai",
          model_id: "test",
          usage: zero,
        } as const
        yield* journal.set(`${prefix}other-session/unsupported`, {
          ...identity,
          session_id: "other-session",
          operation: "unsupported",
        })
        yield* client.flush("bounded")
        expect(events).toEqual([])
        yield* client.flush().pipe(
          Effect.flip,
          Effect.tap((error) => Effect.sync(() => expect(error.message).toContain("journal is invalid"))),
        )
        expect((yield* journal.scan({ prefix })).entries).toHaveLength(1)
        yield* journal.remove(`${prefix}other-session/unsupported`)
        yield* journal.set(`${prefix}bounded/wrong-key`, identity)
        yield* client.flush("bounded").pipe(
          Effect.flip,
          Effect.tap((error) => Effect.sync(() => expect(error.message).toContain("identity mismatch"))),
        )
        expect(events).toEqual([])
        yield* journal.remove(`${prefix}bounded/wrong-key`)
        for (let index = 0; index < 11; index++) {
          const attempt = `attempt-${index.toString().padStart(2, "0")}`
          yield* journal.set(`${prefix}bounded/${attempt}`, { ...identity, attempt_id: attempt })
        }
        yield* client.stream(request(server.url.origin, "bounded")).pipe(
          Stream.runCollect,
          Effect.flip,
          Effect.tap((error) => Effect.sync(() => expect(error.message).toContain("callbacks remain pending"))),
        )
        expect(events).toEqual(Array(10).fill("/complete"))
        expect((yield* journal.scan({ prefix })).entries).toHaveLength(1)
        yield* client.flush("bounded")
        expect((yield* journal.scan({ prefix })).entries).toEqual([])
        expect(events).toEqual(Array(11).fill("/complete"))
      }).pipe(Effect.provide(layer())),
    )
  } finally {
    server.stop(true)
  }
})

test("background replay skips failed entries and advances beyond the first ten records", async () => {
  const attempts: string[] = []
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = (await req.json()) as { session_id: string; attempt_id: string }
      attempts.push(body.attempt_id as string)
      return new Response("fixture unavailable", { status: body.session_id === "a-blocked" ? 503 : 200 })
    },
  })
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const base = yield* LLMClient.Service
        const journal = yield* KV.Service
        const client = ModelBudget.make(base, server.url.origin, "fixture", journal)
        for (let index = 0; index < 10; index++) {
          const id = `attempt-${index}`
          yield* journal.set(`${prefix}a-blocked/${id}`, {
            operation: index === 0 ? "unsupported" : "complete",
            session_id: "a-blocked",
            attempt_id: id,
            provider_id: "openai",
            model_id: "test",
            usage: zero,
          })
        }
        yield* journal.set(`${prefix}b-ready/ready`, {
          operation: "complete",
          session_id: "b-ready",
          attempt_id: "ready",
          provider_id: "openai",
          model_id: "test",
          usage: zero,
        })
        yield* client.replay
        expect(attempts).toHaveLength(9)
        expect((yield* journal.scan({ prefix })).entries).toHaveLength(11)
        yield* client.replay
        expect(attempts).toHaveLength(10)
        expect(attempts.at(-1)).toBe("ready")
        expect((yield* journal.scan({ prefix })).entries).toHaveLength(10)
        yield* client.replay
        expect(attempts).toHaveLength(19)
      }).pipe(Effect.provide(layer())),
    )
  } finally {
    server.stop(true)
  }
})

test("idle callback maintenance retries after an outage without another provider call", async () => {
  const first = Promise.withResolvers<void>()
  const acked = Promise.withResolvers<void>()
  let online = false
  let callbacks = 0
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      expect(new URL(req.url).pathname).toBe("/complete")
      callbacks++
      if (!online) {
        return new Response("fixture outage", { status: 503 })
      }
      acked.resolve()
      return Response.json({ ok: true })
    },
  })
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const base = yield* LLMClient.Service
        const journal = yield* KV.Service
        const client = ModelBudget.make(base, server.url.origin, "fixture", journal)
        yield* journal.set(`${prefix}idle/finished`, {
          operation: "complete",
          session_id: "idle",
          attempt_id: "finished",
          provider_id: "openai",
          model_id: "test",
          usage: { ...zero, input: 10, output: 3 },
        })
        const worker = yield* client.maintain.pipe(
          Effect.provideService(
            Logger.CurrentLoggers,
            new Set([
              Logger.make((entry) => {
                if (String(entry.message).includes("Model budget replay remains pending")) first.resolve()
              }),
            ]),
          ),
          Effect.forkScoped,
        )
        yield* Effect.promise(() => first.promise)
        yield* Effect.yieldNow
        online = true
        yield* TestClock.adjust("30 seconds")
        yield* Effect.promise(() => acked.promise)
        yield* Effect.promise(() => Bun.sleep(10))
        expect((yield* journal.scan({ prefix })).entries).toEqual([])
        expect(callbacks).toBe(2)
        yield* Fiber.interrupt(worker)
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(layer(), TestClock.layer()))),
    )
  } finally {
    server.stop(true)
  }
})

test.each(["scan", "remove"] as const)(
  "maintenance survives a transient KV %s defect and retains exact callbacks",
  async (operation) => {
    const failed = Promise.withResolvers<void>()
    const acked = Promise.withResolvers<void>()
    let available = false
    let callbacks = 0
    const server = Bun.serve({
      port: 0,
      async fetch(req) {
        expect(new URL(req.url).pathname).toBe("/complete")
        expect(((await req.json()) as Record<string, KV.Value>).attempt_id).toBe("storage-retry")
        callbacks++
        if (available) acked.resolve()
        return Response.json({ ok: true })
      },
    })
    try {
      await Effect.runPromise(
        Effect.gen(function* () {
          const base = yield* LLMClient.Service
          const journal = yield* KV.Service
          // Production KV operations defect on database errors via Effect.orDie.
          // Inject that boundary failure while retaining the real disposable store.
          const store: KV.Interface = {
            ...journal,
            scan: (options) =>
              !available && operation === "scan"
                ? Effect.die(new Error("fixture storage unavailable"))
                : journal.scan(options),
            remove: (key) =>
              !available && operation === "remove"
                ? Effect.die(new Error("fixture storage unavailable"))
                : journal.remove(key),
          }
          const client = ModelBudget.make(base, server.url.origin, "fixture", store)
          yield* journal.set(`${prefix}storage/storage-retry`, {
            operation: "complete",
            session_id: "storage",
            attempt_id: "storage-retry",
            provider_id: "openai",
            model_id: "test",
            usage: { ...zero, input: 10, output: 3 },
          })
          const worker = yield* client.maintain.pipe(
            Effect.provideService(
              Logger.CurrentLoggers,
              new Set([
                Logger.make((entry) => {
                  if (String(entry.message).includes("Model budget replay failed")) failed.resolve()
                }),
              ]),
            ),
            Effect.forkScoped,
          )
          yield* Effect.promise(() => failed.promise)
          yield* Effect.yieldNow
          expect((yield* journal.scan({ prefix })).entries).toHaveLength(1)
          expect(callbacks).toBe(operation === "scan" ? 0 : 1)
          available = true
          yield* TestClock.adjust("30 seconds")
          yield* Effect.promise(() => acked.promise)
          yield* Effect.promise(() => Bun.sleep(10))
          expect((yield* journal.scan({ prefix })).entries).toEqual([])
          expect(callbacks).toBe(operation === "scan" ? 1 : 2)
          yield* Fiber.interrupt(worker)
        }).pipe(Effect.scoped, Effect.provide(Layer.merge(layer(), TestClock.layer()))),
      )
    } finally {
      server.stop(true)
    }
  },
)
