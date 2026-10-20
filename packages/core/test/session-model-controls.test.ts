import { expect, test } from "bun:test"
import { LLM, LLMClient, LLMRequest, LanguageModel } from "@opencode/ai"
import { RequestExecutor } from "@opencode/ai/route"
import { configure } from "@opencode/ai/providers/openai"
import { OpenAIResponses } from "@opencode/ai/protocols"
import { SessionModelControls } from "@opencode/core/session/model-controls"
import { Effect, Layer, Schema } from "effect"

const prepare = (request: LLMRequest) =>
  Effect.gen(function* () {
    const client = yield* LLMClient.Service
    return yield* client.prepareRequest!(request)
  }).pipe(Effect.provide(LLMClient.layer), Effect.provide(Layer.mock(RequestExecutor.Service)({})), Effect.runPromise)
const factory = configure({ apiKey: "synthetic-key" })
const base = LLM.request({ model: factory.model("gpt-6.1-sol"), prompt: "Original input" })

test("recognizes actual Chat/Responses defaults and finite controls after client preparation", async () => {
  for (const model of [factory.chat("gpt-6.1-sol"), factory.model("gpt-6.1-sol"), factory.model("gpt-5.5")]) {
    const prepared = await prepare(LLM.request({ model, prompt: "Original input" }))
    expect(prepared.providerOptions?.store).toBe(false)
    expect(SessionModelControls.known(prepared)).toBe(true)
  }
  const route = base.model.route.with({
    endpoint: { baseURL: "https://routing.example.test/v1" },
    providerOptions: { reasoningEffort: "medium", reasoningSummary: "auto", textVerbosity: "low" },
    generation: { temperature: 0.4, maxTokens: 1000 },
  })
  const prepared = await prepare(
    LLM.request({ model: LanguageModel.update(base.model, { route }), prompt: "Original input" }),
  )
  expect(SessionModelControls.known(prepared)).toBe(true)
})

test("rejects authored data and unsupported controls at every merged defaults layer", async () => {
  const variants = [
    { providerOptions: { instructions: "Private data" } },
    { providerOptions: { prompt: "Private data" } },
    { providerOptions: { rawprompt: "Private data" } },
    { providerOptions: { metadata: { private: "Private data" } } },
    { providerOptions: { unknown: true } },
    { providerOptions: { reasoningEffort: "Private data" } },
    { providerOptions: { include: ["Private data"] } },
    { providerOptions: { include: ["reasoning.encrypted_content", "reasoning.encrypted_content"] } },
    { providerOptions: { store: "Private data" } },
    { generation: { stop: ["Private data"] } },
    { generation: { temperature: 20 } },
    { http: { body: { instructions: "Private data" } } },
    { http: { body: { prompt: "Private data" } } },
    { http: { body: { metadata: { private: "Private data" } } } },
  ]
  for (const patch of variants) {
    const requests = [
      LLMRequest.update(base, patch),
      LLMRequest.update(base, { model: LanguageModel.update(base.model, { defaults: patch }) }),
      LLMRequest.update(base, { model: LanguageModel.update(base.model, { route: base.model.route.with(patch) }) }),
    ]
    for (const request of requests) expect(SessionModelControls.known(await prepare(request))).toBe(false)
  }
  expect(
    SessionModelControls.known(await prepare(LLMRequest.update(base, { metadata: { private: "Private data" } }))),
  ).toBe(false)
})

test("requires the immutable compiled body declaration rather than its protocol name", async () => {
  const prepared = await prepare(base)
  const body = prepared.model.route.body
  for (const replacement of [
    { ...body },
    { ...body, from: () => Effect.succeed({ instructions: "Private data" }) },
    { ...body, schema: Schema.Unknown },
  ]) {
    const request = LLMRequest.update(prepared, {
      model: LanguageModel.update(prepared.model, { route: { ...prepared.model.route, body: replacement } }),
    })
    expect(SessionModelControls.known(request)).toBe(false)
    expect(SessionModelControls.known(LLMRequest.update(request, { providerOptions: undefined }))).toBe(false)
  }
  const original = { from: OpenAIResponses.protocol.body.from, schema: OpenAIResponses.protocol.body.schema }
  try {
    Object.assign(OpenAIResponses.protocol.body, { from: () => Effect.succeed({ instructions: "Private data" }) })
    expect(SessionModelControls.known(prepared)).toBe(false)
    Object.assign(OpenAIResponses.protocol.body, { from: original.from, schema: Schema.Unknown })
    expect(SessionModelControls.known(prepared)).toBe(false)
  } finally {
    Object.assign(OpenAIResponses.protocol.body, original)
  }
  expect(SessionModelControls.known(prepared)).toBe(true)
})
