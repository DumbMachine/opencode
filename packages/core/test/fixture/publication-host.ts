import { generateKeyPairSync, sign } from "node:crypto"
import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"

export const publicationHost = (
  options: {
    mutate?: (proof: ExecutionDisclosure.PublicationProof) => ExecutionDisclosure.PublicationProof
    context?: (proof: ExecutionDisclosure.ContextProof) => ExecutionDisclosure.ContextProof
    observe?: (request: ExecutionDisclosure.PublicationRequest, authorization: string | null) => Promise<void> | void
    respond?: (proof: ExecutionDisclosure.SignedProof) => Response
    wrongKey?: boolean
    ttl?: () => number
    observeContext?: (request: ExecutionDisclosure.ContextRequest, authorization: string | null) => Promise<void> | void
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
      if (new URL(request.url).pathname === "/publication") {
        const received = (await request.json()) as ExecutionDisclosure.PublicationRequest
        await options.observe?.(received, request.headers.get("authorization"))
        const now = Date.now()
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
          expiresAt: now + (options.ttl?.() ?? 30_000),
        }
        const signed = signature(
          options.mutate?.(proof) ?? proof,
          "publication",
          options.wrongKey ? contextKeys : publicationKeys,
        )
        return options.respond?.(signed) ?? Response.json(signed)
      }
      const received = (await request.json()) as ExecutionDisclosure.ContextRequest
      await options.observeContext?.(received, request.headers.get("authorization"))
      const now = Date.now()
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
