import { generateKeyPairSync, sign } from "node:crypto"
import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"

export const disclosureHost = (
  options: {
    mutate?: (proof: ExecutionDisclosure.ContextProof) => ExecutionDisclosure.ContextProof
    respond?: (signed: ExecutionDisclosure.SignedProof) => Response
    observe?: (input: ExecutionDisclosure.ContextRequest, authorization: string | null) => Promise<void> | void
    unknown?: boolean
  } = {},
) => {
  const pair = generateKeyPairSync("ed25519")
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const input = (await req.json()) as ExecutionDisclosure.ContextRequest
      await options.observe?.(input, req.headers.get("authorization"))
      const now = Date.now()
      const original: ExecutionDisclosure.ContextProof = {
        version: 1,
        issuer: "fixture-host",
        audience: "fixture-native",
        sessionID: input.sessionID,
        inputID: input.inputID,
        kind: input.kind,
        attempt: input.attempt,
        contextDigest: input.contextDigest,
        componentsDigest: input.componentsDigest,
        contextReference: "opaque-original-context",
        mode: options.unknown ? "actor-private-unknown" : "complete",
        complete: !options.unknown,
        issuedAt: now,
        expiresAt: now + 30_000,
      }
      const payload = JSON.stringify(options.mutate?.(original) ?? original)
      const signed = {
        keyID: "fixture-key",
        payload,
        signature: sign(null, Buffer.from(payload), pair.privateKey).toString("base64url"),
      }
      return options.respond?.(signed) ?? Response.json(signed)
    },
  })
  return {
    server,
    config: {
      version: 1 as const,
      endpoint: server.url.href,
      token: "fixture-private-token",
      issuer: "fixture-host",
      audience: "fixture-native",
      keys: [
        {
          id: "fixture-key",
          algorithm: "Ed25519" as const,
          publicKey: pair.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("base64url"),
        },
      ],
    },
  }
}
