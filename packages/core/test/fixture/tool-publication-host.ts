import { generateKeyPairSync, sign } from "node:crypto"
import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"

export const toolPublicationHost = (
  options: {
    mutate?: (proof: ExecutionDisclosure.ToolPublicationProof) => ExecutionDisclosure.ToolPublicationProof
    context?: (proof: ExecutionDisclosure.ContextProof) => ExecutionDisclosure.ContextProof
    observe?: (
      request: ExecutionDisclosure.ToolPublicationRequest,
      authorization: string | null,
    ) => Promise<void> | void
    respond?: (proof: ExecutionDisclosure.SignedProof) => Response
    wrongKey?: boolean
    ttl?: () => number
    observeContext?: (request: ExecutionDisclosure.ContextRequest, authorization: string | null) => Promise<void> | void
  } = {},
) => {
  const contextKeys = generateKeyPairSync("ed25519")
  const publicationKeys = generateKeyPairSync("ed25519")
  const toolsKeys = generateKeyPairSync("ed25519")
  const catalogKeys = generateKeyPairSync("ed25519")
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
        const received = (await request.json()) as ExecutionDisclosure.ToolPublicationRequest
        await options.observe?.(received, request.headers.get("authorization"))
        const now = Date.now()
        const { projection: _projection, ...identity } = received
        const proof: ExecutionDisclosure.ToolPublicationProof = {
          ...identity,
          mode: "complete",
          complete: true,
          purpose: "thread-publication",
          issuer: "fixture-host",
          audience: "fixture-publication",
          organizationID: "org-original",
          threadID: "thread-original",
          capabilityID: "cap-original",
          capabilityGeneration: 7,
          receiptReference: received.unitDigest,
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
      const { components: _components, composition: _composition, ...contextIdentity } = received
      const proof: ExecutionDisclosure.ContextProof = {
        ...contextIdentity,
        contextReference: "1".repeat(64),
        issuer: "fixture-host",
        audience: "fixture-context",
        mode: "complete",
        complete: true,
        issuedAt: now,
        expiresAt: now + 90_000,
        tools: { version: 1, audience: "fixture-tools", keys: [key("tools", toolsKeys)] },
        publication: {
          version: 2,
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
      publicationVersion: 2 as const,
      compositionVersion: 4 as const,
      catalog: { version: 1 as const, audience: "fixture-catalog", keys: [key("catalog", catalogKeys)] },
      endpoint: new URL("/preflight", server.url).href,
      token: "private-original-token",
      issuer: "fixture-host",
      audience: "fixture-context",
      keys: [key("context", contextKeys)],
    },
  }
}
