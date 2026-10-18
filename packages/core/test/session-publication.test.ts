import { describe, expect, test } from "bun:test"
import { generateKeyPairSync, sign } from "node:crypto"
import { Effect } from "effect"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { SessionDisclosure } from "@opencode/core/session/disclosure"

const input = {
  sessionID: Session.ID.make("ses_publication_original"),
  inputID: SessionMessage.ID.make("msg_publication_original"),
  kind: "primary" as const,
  contextDigest: "a".repeat(64),
  components: [{ kind: "message" as const, id: "msg_original", revision: "b".repeat(64) }],
}
const unit = { unitID: "evt_original", unitDigest: "c".repeat(64) }
const host = (
  options: {
    mutate?: (proof: ExecutionDisclosure.PublicationProof) => ExecutionDisclosure.PublicationProof
    context?: (proof: ExecutionDisclosure.ContextProof) => ExecutionDisclosure.ContextProof
    observe?: (request: ExecutionDisclosure.PublicationRequest, authorization: string | null) => Promise<void> | void
    respond?: (proof: ExecutionDisclosure.SignedProof) => Response
    wrongKey?: boolean
  } = {},
) => {
  const contextKeys = generateKeyPairSync("ed25519")
  const publicationKeys = generateKeyPairSync("ed25519")
  const key = (id: string, pair: typeof contextKeys) => ({
    id,
    algorithm: "Ed25519" as const,
    publicKey: pair.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("base64url"),
  })
  const signature = (proof: unknown, id: string, pair: typeof contextKeys) => {
    const payload = JSON.stringify(proof)
    return { keyID: id, payload, signature: sign(null, Buffer.from(payload), pair.privateKey).toString("base64url") }
  }
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const now = Date.now()
      if (new URL(request.url).pathname === "/publication") {
        const received = (await request.json()) as ExecutionDisclosure.PublicationRequest
        await options.observe?.(received, request.headers.get("authorization"))
        const proof: ExecutionDisclosure.PublicationProof = {
          ...received,
          purpose: "thread-publication",
          issuer: "fixture-host",
          audience: "fixture-publication",
          organizationID: "org-original",
          threadID: "thread-original",
          capabilityID: "cap-original",
          capabilityGeneration: 7,
          receiptReference: "d".repeat(64),
          authorizationVersion: "original-authority",
          manifestBinding: "e".repeat(64),
          audienceBinding: "f".repeat(64),
          issuedAt: now,
          expiresAt: now + 30_000,
        }
        const signed = signature(
          options.mutate?.(proof) ?? proof,
          "publication",
          options.wrongKey ? contextKeys : publicationKeys,
        )
        return options.respond?.(signed) ?? Response.json(signed)
      }
      const received = (await request.json()) as ExecutionDisclosure.ContextRequest
      const proof: ExecutionDisclosure.ContextProof = {
        ...received,
        contextReference: "1".repeat(64),
        issuer: "fixture-host",
        audience: "fixture-context",
        mode: "actor-private-unknown",
        complete: false,
        issuedAt: now,
        expiresAt: now + 90_000,
        publication: {
          version: 1,
          endpoint: new URL("/publication", server.url).href,
          audience: "fixture-publication",
          keys: [key("publication", publicationKeys)],
          organizationID: "org-original",
          threadID: "thread-original",
          capabilityID: "cap-original",
          capabilityGeneration: 7,
        },
      }
      return Response.json(signature(options.context?.(proof) ?? proof, "context", contextKeys))
    },
  })
  return {
    server,
    config: {
      version: 1 as const,
      endpoint: new URL("/preflight", server.url).href,
      token: "private-original-token",
      issuer: "fixture-host",
      audience: "fixture-context",
      keys: [key("context", contextKeys)],
    },
  }
}
const prepare = (fixture: ReturnType<typeof host>) =>
  Effect.runPromise(SessionDisclosure.prepare(fixture.config, input))
const authorize = async (fixture: ReturnType<typeof host>) => (await prepare(fixture)).publication!.authorize(unit)

describe("Exact output publication authorization", () => {
  test("retains original context, destination, input generation and token across two service instances", async () => {
    for (let n = 0; n < 2; n++) {
      const fixture = host({
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
      const fixture = host({ mutate })
      try {
        await expect(Effect.runPromise(await authorize(fixture))).rejects.toThrow(
          "Execution publication authorization is unavailable",
        )
      } finally {
        fixture.server.stop(true)
      }
    })
  test("an older context without the signed descriptor supplies no publication authority", async () => {
    const fixture = host({ context: (proof) => ({ ...proof, publication: undefined }) })
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
    const fixture = host({
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
    const fixture = host({
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
    const fixture = host({
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
      const fixture = host(options)
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
