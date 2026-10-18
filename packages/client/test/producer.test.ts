import { expect, test } from "bun:test"
import { Effect } from "effect"
import { HttpClient, HttpClientResponse } from "effect/unstable/http"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { OpenCode as PromiseOpenCode } from "../src/promise/index"
import { OpenCode as EffectOpenCode, ExecutionProducer as ClientProducer } from "../src/effect/index"

const input = { version: 1 as const, challenge: "c".repeat(43), audience: "host" }
const output = {
  key: { id: "key", algorithm: "Ed25519" as const, publicKey: "p".repeat(43) },
  signed: { keyID: "key", payload: "{}", signature: "s".repeat(86) },
}

test("Promise producer enrollment uses the canonical authenticated control path", async () => {
  const requests: Request[] = []
  const client = PromiseOpenCode.make({
    baseUrl: "http://localhost:3000",
    fetch: (async (url, init) => {
      requests.push(new Request(url, init))
      return Response.json(output)
    }) as typeof fetch,
  })
  expect(await client.server.enrollProducer(input)).toEqual(output)
  expect(requests[0].url).toBe("http://localhost:3000/api/producer/enrollment")
  expect(requests[0].method).toBe("POST")
  expect(await requests[0].json()).toEqual(input)
})

test("Effect producer enrollment retains the canonical browser-safe contract", async () => {
  expect(ClientProducer.Enrollment).toBe(ExecutionProducer.Enrollment)
  expect(ClientProducer.SignedFact).toBe(ExecutionProducer.SignedFact)
  const requests: string[] = []
  const httpClient = HttpClient.make((request) => {
    requests.push(request.method + " " + request.url)
    return Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(output)))
  })
  const result = await Effect.gen(function* () {
    const client = yield* EffectOpenCode.make({ baseUrl: "http://localhost:3000" })
    return yield* client.server.enrollProducer(input)
  }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.runPromise)
  expect(result).toEqual(output)
  expect(requests).toEqual(["POST http://localhost:3000/api/producer/enrollment"])
})
