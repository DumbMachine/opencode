import { expect } from "bun:test"
import { createPublicKey, verify } from "node:crypto"
import { Effect, Schema } from "effect"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const options = {
  app: { version: "test" },
  database: { path: ":memory:" },
  fs: { filewatcher: false },
  models: { fetch: false },
}
const challenge = { version: 1, challenge: "c".repeat(43), audience: "host-fixture" }
const request = (payload: unknown = challenge, password?: string) =>
  new Request("http://opencode.local/api/producer/enrollment", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(password === undefined ? {} : { authorization: `Basic ${btoa(`opencode:${password}`)}` }),
    },
    body: JSON.stringify(payload),
  })

it.live("producer enrollment requires configured and authenticated native server access", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make({ ...options, password: "fixture-native-password" })
    for (const password of [undefined, "wrong-password"]) {
      const response = yield* Effect.promise(() => handler(request(challenge, password)))
      expect(response.status).toBe(401)
    }
    const response = yield* Effect.promise(() => handler(request(challenge, "fixture-native-password")))
    expect(response.status).toBe(200)
    const body = yield* Effect.promise(() => response.json())
    const result = Schema.decodeUnknownSync(ExecutionProducer.Enrollment)(body)
    const proof = Schema.decodeUnknownSync(ExecutionProducer.EnrollmentProof)(JSON.parse(result.signed.payload))
    expect(proof.challenge).toBe(challenge.challenge)
    expect(proof.audience).toBe(challenge.audience)
    expect(proof.key).toEqual(result.key)
    expect(result.signed.keyID).toBe(result.key.id)
    expect(
      verify(
        null,
        Buffer.from(ExecutionProducer.SignatureDomain + result.signed.payload),
        createPublicKey({
          key: Buffer.concat([
            Buffer.from("302a300506032b6570032100", "hex"),
            Buffer.from(result.key.publicKey, "base64url"),
          ]),
          format: "der",
          type: "spki",
        }),
        Buffer.from(result.signed.signature, "base64url"),
      ),
    ).toBeTrue()
    const malformed = yield* Effect.promise(() =>
      handler(request({ ...challenge, challenge: "short" }, "fixture-native-password")),
    )
    expect(malformed.status).toBe(400)
    const info = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/info", {
          headers: { authorization: `Basic ${btoa("opencode:fixture-native-password")}` },
        }),
      ),
    )
    expect(info.status).toBe(200)
    expect(info.headers.get("x-opencode-context-publication")).toBeNull()
  }).pipe(Effect.scoped),
)

it.live("an unauthenticated embedded native server cannot offer enrollment", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make(options)
    const info = yield* Effect.promise(() => handler(new Request("http://opencode.local/api/info")))
    expect(info.status).toBe(200)
    const response = yield* Effect.promise(() => handler(request()))
    expect(response.status).toBe(401)
  }).pipe(Effect.scoped),
)
