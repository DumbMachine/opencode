import { expect } from "bun:test"
import { createPublicKey, verify } from "node:crypto"
import { Effect, Schema } from "effect"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { Session } from "@opencode/schema/session"
import { it } from "../../core/test/lib/effect"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
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
    expect(info.headers.get("x-opencode-context-policy")).toBe("1")
    expect(info.headers.get("x-opencode-context-composition")).toBe("2")
    expect(info.headers.get("x-opencode-context-inline-composition")).toBe("1")
    expect(yield* Effect.promise(() => info.json())).toMatchObject({ contextInlineCompositionVersion: 1 })
    const contracts = [
      "human-plaintext-identity-v1",
      "model-plaintext-projection-v1",
      "model-request-only-plaintext-v1",
      "human-inline-identity-v1",
      "model-request-only-inline-v1",
    ]
    const inline = yield* Effect.promise(() => handler(request({ ...challenge, contracts }, "fixture-native-password")))
    expect(inline.status).toBe(200)
    const enrolledInline = Schema.decodeUnknownSync(ExecutionProducer.Enrollment)(
      yield* Effect.promise(() => inline.json()),
    )
    expect(JSON.parse(enrolledInline.signed.payload).contracts).toEqual(contracts)
    expect(info.headers.get("x-opencode-context-publication")).toBe("1")
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

const SessionResponse = Schema.Struct({ data: Schema.toEncoded(Session.Info) })
const sessionRequest = (path: string, payload?: unknown) =>
  new Request(`http://opencode.local/api/session${path}`, {
    method: payload === undefined ? "GET" : "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Basic ${btoa("opencode:fixture-native-password")}`,
    },
    body: payload === undefined ? undefined : JSON.stringify(payload),
  })

it.live("persistent native HTTP sessions acknowledge protected birth and retain their reserved identities", () =>
  Effect.gen(function* () {
    const tmp = yield* tmpdirScoped()
    const handler = yield* ServerFetch.make({
      ...options,
      password: "fixture-native-password",
      events: { persist: true },
    })
    for (const id of ["ses_protected_original_one", "ses_protected_original_two"]) {
      const payload = { id, location: { directory: tmp.path }, contextPolicy: { version: 1 } }
      const response = yield* Effect.promise(() => handler(sessionRequest("", payload)))
      expect(response.status).toBe(200)
      const created = Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => response.json()))
      expect(created.data.id).toBe(id)
      expect(created.data.contextPolicy).toEqual({ version: 1 })
      expect(created.data.parentID).toBeUndefined()
      expect(created.data.location.directory).toBe(tmp.path)

      for (const contextPolicy of [{ version: 1 }, undefined]) {
        const repeated = yield* Effect.promise(() => handler(sessionRequest("", { ...payload, contextPolicy })))
        expect(repeated.status).toBe(200)
        expect(Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => repeated.json()))).toEqual(created)
      }
      const read = yield* Effect.promise(() => handler(sessionRequest(`/${id}`)))
      expect(read.status).toBe(200)
      expect(Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => read.json()))).toEqual(created)
    }
  }).pipe(Effect.scoped),
)

it.live("native HTTP creation cannot upgrade a legacy session to protected context", () =>
  Effect.gen(function* () {
    const tmp = yield* tmpdirScoped()
    const handler = yield* ServerFetch.make({
      ...options,
      password: "fixture-native-password",
      events: { persist: true },
    })
    const payload = { id: "ses_legacy_birth", location: { directory: tmp.path } }
    const response = yield* Effect.promise(() => handler(sessionRequest("", payload)))
    expect(response.status).toBe(200)
    const created = Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => response.json()))
    expect(created.data.id).toBe(payload.id)
    expect(created.data.contextPolicy).toBeUndefined()

    const upgrade = yield* Effect.promise(() =>
      handler(sessionRequest("", { ...payload, contextPolicy: { version: 1 } })),
    )
    expect(upgrade.status).toBe(500)
    const read = yield* Effect.promise(() => handler(sessionRequest(`/${payload.id}`)))
    expect(read.status).toBe(200)
    expect(Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => read.json()))).toEqual(created)
  }).pipe(Effect.scoped),
)

it.live("native HTTP protected birth requires event persistence and leaves no session on rejection", () =>
  Effect.gen(function* () {
    const tmp = yield* tmpdirScoped()
    const handler = yield* ServerFetch.make({
      ...options,
      password: "fixture-native-password",
      events: { persist: false },
    })
    const payload = {
      id: "ses_protected_without_persistence",
      location: { directory: tmp.path },
      contextPolicy: { version: 1 },
    }
    const response = yield* Effect.promise(() => handler(sessionRequest("", payload)))
    expect(response.status).toBe(500)
    const read = yield* Effect.promise(() => handler(sessionRequest(`/${payload.id}`)))
    expect(read.status).toBe(404)
    expect(yield* Effect.promise(() => read.json())).toMatchObject({
      _tag: "SessionNotFoundError",
      sessionID: payload.id,
    })
  }).pipe(Effect.scoped),
)
