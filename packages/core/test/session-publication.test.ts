import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { SessionDisclosure } from "@opencode/core/session/disclosure"
import { publicationHost } from "./fixture/publication-host"

const input = {
  sessionID: Session.ID.make("ses_publication_original"),
  inputID: SessionMessage.ID.make("msg_publication_original"),
  kind: "primary" as const,
  contextDigest: "a".repeat(64),
  components: [{ kind: "message" as const, id: "msg_original", revision: "b".repeat(64) }],
}
const unit = { unitID: "evt_original", unitDigest: "c".repeat(64) }
const prepare = (fixture: ReturnType<typeof publicationHost>) =>
  Effect.runPromise(SessionDisclosure.prepare(fixture.config, input))
const authorize = async (fixture: ReturnType<typeof publicationHost>) =>
  (await prepare(fixture)).publication!.authorize(unit)

describe("Exact output publication authorization", () => {
  test("retains original context, destination, input generation and token across two service instances", async () => {
    for (let n = 0; n < 2; n++) {
      const fixture = publicationHost({
        observe: (request, authorization) => {
          expect(authorization).toBe("Bearer private-original-token")
          expect(request).toMatchObject({
            ...unit,
            sessionID: input.sessionID,
            inputID: input.inputID,
            kind: "model-output",
          })
          expect(Object.keys(request)).not.toContain("destination")
        },
      })
      try {
        const prepared = await prepare(fixture)
        fixture.config.token = "replacement-token"
        Object.assign(prepared.proof.publication!, { threadID: "substituted-thread" })
        const permit = await Effect.runPromise(prepared.publication!.authorize(unit))
        expect(permit.proof.threadID).toBe("thread-original")
        expect(permit.proof.capabilityGeneration).toBe(7)
        expect(permit.valid()).toBe(true)
        expect(JSON.stringify({ prepared, permit })).not.toContain("private-original-token")
        expect(Object.isFrozen(permit.proof)).toBe(true)
      } finally {
        fixture.server.stop(true)
      }
    }
  })
  test("waits for a committed host proof beyond five seconds and still validates it", async () => {
    const fixture = publicationHost({ observe: async () => { await new Promise((resolve) => setTimeout(resolve, 5_200)) } })
    try {
      const prepared = await prepare(fixture)
      const permit = await Effect.runPromise(prepared.publication!.authorize(unit))
      expect(permit.valid()).toBe(true)
      expect(permit.proof.threadID).toBe("thread-original")
    } finally { fixture.server.stop(true) }
  }, 15_000)
  const mutations: Array<[string, (p: ExecutionDisclosure.PublicationProof) => ExecutionDisclosure.PublicationProof]> =
    [
      ["purpose", (p) => ({ ...p, purpose: "other" as typeof p.purpose })],
      ["issuer", (p) => ({ ...p, issuer: "other" })],
      ["audience", (p) => ({ ...p, audience: "other" })],
      ["organization", (p) => ({ ...p, organizationID: "other" })],
      ["thread", (p) => ({ ...p, threadID: "other" })],
      ["session", (p) => ({ ...p, sessionID: Session.ID.make("ses_other") })],
      ["input", (p) => ({ ...p, inputID: SessionMessage.ID.make("msg_other") })],
      ["capability", (p) => ({ ...p, capabilityID: "other" })],
      ["generation", (p) => ({ ...p, capabilityGeneration: 8 })],
      ["context reference", (p) => ({ ...p, contextReference: "2".repeat(64) })],
      ["context digest", (p) => ({ ...p, contextDigest: "2".repeat(64) })],
      ["components digest", (p) => ({ ...p, componentsDigest: "2".repeat(64) })],
      ["attempt", (p) => ({ ...p, attempt: "other" })],
      ["kind", (p) => ({ ...p, kind: "tool-output" as typeof p.kind })],
      ["unit ID", (p) => ({ ...p, unitID: "other" })],
      ["unit digest", (p) => ({ ...p, unitDigest: "2".repeat(64) })],
      ["expiry", (p) => ({ ...p, expiresAt: Date.now() - 1 })],
      ["future", (p) => ({ ...p, issuedAt: Date.now() + 60_000, expiresAt: Date.now() + 70_000 })],
      ["overlong", (p) => ({ ...p, expiresAt: p.issuedAt + 30_001 })],
    ]
  for (const [name, mutate] of mutations)
    test(`refuses signed substitution of ${name}`, async () => {
      const fixture = publicationHost({ mutate })
      try {
        await expect(Effect.runPromise(await authorize(fixture))).rejects.toThrow(
          "Execution publication authorization is unavailable",
        )
      } finally {
        fixture.server.stop(true)
      }
    })
  test("an older context without the signed descriptor supplies no publication authority", async () => {
    const fixture = publicationHost({ context: (proof) => ({ ...proof, publication: undefined }) })
    try {
      expect((await prepare(fixture)).publication).toBeUndefined()
    } finally {
      fixture.server.stop(true)
    }
  })
  test("rejects a signed foreign endpoint before sending the original credential", async () => {
    let reached = false
    const foreign = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch() {
        reached = true
        return new Response()
      },
    })
    const fixture = publicationHost({
      context: (proof) => ({ ...proof, publication: { ...proof.publication!, endpoint: foreign.url.href } }),
    })
    try {
      await expect(prepare(fixture)).rejects.toThrow("Execution context authorization is unavailable")
      expect(reached).toBe(false)
    } finally {
      fixture.server.stop(true)
      foreign.stop(true)
    }
  })
  test("captures exact unit identity before waiting; extra fields cannot replace original context", async () => {
    const arrived = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>()
    const fixture = publicationHost({
      observe: async (request) => {
        expect(request.contextDigest).toBe(input.contextDigest)
        arrived.resolve()
        await release.promise
      },
    })
    try {
      const prepared = await prepare(fixture)
      const candidate = { ...unit, contextDigest: "9".repeat(64) }
      const pending = Effect.runPromise(prepared.publication!.authorize(candidate))
      await arrived.promise
      candidate.unitDigest = "8".repeat(64)
      release.resolve()
      expect((await pending).proof.unitDigest).toBe(unit.unitDigest)
    } finally {
      release.resolve()
      fixture.server.stop(true)
    }
  })
  test("uses fresh publication authority after context expiry, then rejects its own expired permit", async () => {
    const fixture = publicationHost({
      context: (p) => ({ ...p, expiresAt: Date.now() + 80 }),
      mutate: (p) => ({ ...p, issuedAt: Date.now(), expiresAt: Date.now() + 80 }),
    })
    try {
      const prepared = await prepare(fixture)
      await Bun.sleep(100)
      const permit = await Effect.runPromise(prepared.publication!.authorize(unit))
      expect(permit.valid()).toBe(true)
      await Bun.sleep(100)
      expect(permit.valid()).toBe(false)
    } finally {
      fixture.server.stop(true)
    }
  })
  test("rejects wrong-purpose signatures, tampering, redirects, unknown keys, large and failed responses without leaking bodies", async () => {
    let reached = false
    const foreign = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch() {
        reached = true
        return new Response()
      },
    })
    for (const options of [
      { wrongKey: true },
      { respond: (p: ExecutionDisclosure.SignedProof) => Response.json({ ...p, keyID: "unknown" }) },
      {
        respond: (p: ExecutionDisclosure.SignedProof) =>
          Response.json({ ...p, payload: p.payload.replace("thread-original", "thread-tampered") }),
      },
      { respond: () => Response.redirect(foreign.url.href) },
      { respond: () => new Response("x".repeat(65537)) },
      { respond: () => new Response("private-original-token response", { status: 503 }) },
    ]) {
      const fixture = publicationHost(options)
      try {
        const result = await Effect.runPromiseExit(await authorize(fixture))
        expect(result._tag).toBe("Failure")
        expect(JSON.stringify(result)).not.toContain("private-original-token")
      } finally {
        fixture.server.stop(true)
      }
    }
    expect(reached).toBe(false)
    foreign.stop(true)
  })
})
