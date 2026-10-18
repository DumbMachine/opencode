import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { SessionDisclosure } from "@opencode/core/session/disclosure"
import { disclosureHost } from "./fixture/disclosure-host"

const request = () => ({
  sessionID: Session.ID.make("ses_disclosure_original"),
  inputID: SessionMessage.ID.make("msg_disclosure_original"),
  kind: "primary" as const,
  contextDigest: "a".repeat(64),
  components: [{ kind: "message" as const, id: "msg_original", revision: "b".repeat(64) }],
})

describe("Session disclosure preflight", () => {
  test("verifies the exact original context and preserves actor-private unknown", async () => {
    for (const unknown of [false, true]) {
      const host = disclosureHost({
        unknown,
        observe: (input, authorization) => {
          expect(authorization).toBe("Bearer fixture-private-token")
          expect(input).toMatchObject(request())
          expect(input.componentsDigest).toBe(SessionDisclosure.digest(input.components))
        },
      })
      try {
        const result = await Effect.runPromise(SessionDisclosure.prepare(host.config, request()))
        expect(result.proof.complete).toBe(!unknown)
        expect(result.proof.mode).toBe(unknown ? "actor-private-unknown" : "complete")
        expect(JSON.stringify(result)).not.toContain(host.config.token)
      } finally {
        host.server.stop(true)
      }
    }
  })

  const mutations: Array<[string, (proof: ExecutionDisclosure.ContextProof) => ExecutionDisclosure.ContextProof]> = [
    ["issuer", (p) => ({ ...p, issuer: "other" })],
    ["audience", (p) => ({ ...p, audience: "other" })],
    ["session", (p) => ({ ...p, sessionID: Session.ID.make("ses_other") })],
    ["input", (p) => ({ ...p, inputID: SessionMessage.ID.make("msg_other") })],
    ["kind", (p) => ({ ...p, kind: "title" })],
    ["attempt", (p) => ({ ...p, attempt: "other-attempt" })],
    ["context", (p) => ({ ...p, contextDigest: "c".repeat(64) })],
    ["components", (p) => ({ ...p, componentsDigest: "d".repeat(64) })],
    ["unknown upgrade", (p) => ({ ...p, complete: true, mode: "actor-private-unknown" })],
    ["expired", (p) => ({ ...p, expiresAt: Date.now() - 1 })],
    ["future", (p) => ({ ...p, issuedAt: Date.now() + 60_000, expiresAt: Date.now() + 90_000 })],
    ["overlong", (p) => ({ ...p, expiresAt: p.issuedAt + 121_000 })],
  ]
  for (const [name, mutate] of mutations)
    test(`rejects a valid signature with wrong ${name}`, async () => {
      const host = disclosureHost({ mutate })
      try {
        await expect(Effect.runPromise(SessionDisclosure.prepare(host.config, request()))).rejects.toThrow(
          "Execution context authorization is unavailable",
        )
      } finally {
        host.server.stop(true)
      }
    })

  test("rejects tampering, unknown keys, oversized and provider error responses without exposing bodies", async () => {
    for (const respond of [
      (p: ExecutionDisclosure.SignedProof) =>
        Response.json({ ...p, payload: p.payload.replace("opaque-original-context", "substituted-context") }),
      (p: ExecutionDisclosure.SignedProof) => Response.json({ ...p, keyID: "other-key" }),
      () => new Response("x".repeat(65537)),
      () => new Response("private fixture-private-token body", { status: 403 }),
    ]) {
      const host = disclosureHost({ respond })
      try {
        const result = await Effect.runPromiseExit(SessionDisclosure.prepare(host.config, request()))
        expect(result._tag).toBe("Failure")
        expect(JSON.stringify(result)).not.toContain("fixture-private-token")
      } finally {
        host.server.stop(true)
      }
    }
  })

  test("does not forward credentials through a redirect", async () => {
    let reached = false
    const destination = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch() {
        reached = true
        return new Response("private")
      },
    })
    const host = disclosureHost({ respond: () => Response.redirect(destination.url.href) })
    try {
      await expect(Effect.runPromise(SessionDisclosure.prepare(host.config, request()))).rejects.toThrow(
        "Execution context authorization is unavailable",
      )
      expect(reached).toBe(false)
    } finally {
      host.server.stop(true)
      destination.stop(true)
    }
  })

  test("captures configuration and component references before waiting for the host", async () => {
    const arrived = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const host = disclosureHost({
      observe: async () => {
        arrived.resolve()
        await release.promise
      },
    })
    const input = request()
    try {
      const pending = Effect.runPromise(SessionDisclosure.prepare(host.config, input))
      await arrived.promise
      host.config.token = "replacement-token"
      host.config.keys[0]!.publicKey = "z".repeat(43)
      input.components[0]!.revision = "d".repeat(64)
      release.resolve()
      const result = await pending
      expect(result.proof.componentsDigest).toBe(SessionDisclosure.digest(request().components))
    } finally {
      release.resolve()
      host.server.stop(true)
    }
  })
})

test("does not treat a context proof without a publication descriptor as publication authority", async () => {
  let calls = 0
  const host = disclosureHost({
    unknown: true,
    observe: () => {
      calls++
    },
  })
  try {
    const prepared = await Effect.runPromise(
      SessionDisclosure.prepare({ ...host.config, publicationVersion: 1 }, request()),
    )
    expect(prepared.publication).toBeUndefined()
    expect(prepared.proof.complete).toBe(false)
    expect(calls).toBe(1)
  } finally {
    host.server.stop(true)
  }
})

test("posts the exact owned attempt after optional producer signing and rejects digest substitution", async () => {
  const arrived = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  let contacted = 0
  const host = disclosureHost({
    unknown: true,
    observe: async () => {
      contacted++
      arrived.resolve()
      await release.promise
    },
  })
  try {
    const original = request()
    const owned = SessionDisclosure.makeRequest(original)
    const attempt = owned.attempt
    const pending = Effect.runPromise(SessionDisclosure.prepareRequest(host.config, owned))
    await arrived.promise
    original.components[0]!.revision = "c".repeat(64)
    release.resolve()
    const result = await pending
    expect(result.proof.attempt).toBe(attempt)
    expect(result.proof.componentsDigest).toBe(owned.componentsDigest)
    expect(result.proof.complete).toBeFalse()
    await expect(
      Effect.runPromise(SessionDisclosure.prepareRequest(host.config, { ...owned, componentsDigest: "f".repeat(64) })),
    ).rejects.toThrow("Execution context authorization is unavailable")
    expect(contacted).toBe(1)
  } finally {
    release.resolve()
    host.server.stop(true)
  }
})
