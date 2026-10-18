export * as SessionPublication from "./publication.js"

import { createPublicKey, verify } from "node:crypto"
import { Effect, Schema } from "effect"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"

export interface Permit {
  readonly proof: ExecutionDisclosure.PublicationProof
  readonly signed: ExecutionDisclosure.SignedProof
  /** Rechecked at local commit; a successful RPC alone never admits output. */
  readonly valid: () => boolean
}

export interface Authorizer {
  readonly authorize: (unit: { readonly unitID: string; readonly unitDigest: string }) => Effect.Effect<Permit>
}

// Only SessionDisclosure.prepare calls this, after verifying ContextProof.
// The returned closures retain the original token without serializing it.
export const make = (
  installed: ExecutionDisclosure.Service,
  context: ExecutionDisclosure.ContextProof,
): Authorizer | undefined => {
  if (context.publication === undefined) return undefined
  const service = Schema.decodeUnknownSync(ExecutionDisclosure.Service)(structuredClone(installed))
  const captured = Schema.decodeUnknownSync(ExecutionDisclosure.ContextProof)(structuredClone(context))
  const descriptor = captured.publication!
  const endpoint = new URL(descriptor.endpoint)
  if (
    endpoint.origin !== new URL(service.endpoint).origin ||
    endpoint.username ||
    endpoint.password ||
    endpoint.hash ||
    endpoint.search ||
    (endpoint.protocol !== "https:" &&
      (endpoint.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname))) ||
    descriptor.audience === service.audience ||
    new Set(descriptor.keys.map((key) => key.id)).size !== descriptor.keys.length ||
    descriptor.keys.some((key) => service.keys.some((preflight) => preflight.publicKey === key.publicKey))
  )
    throw new Error("Execution publication authorization is unavailable")

  return {
    authorize: (unit) =>
      Effect.tryPromise({
        try: async (signal) => {
          const request = Schema.decodeUnknownSync(ExecutionDisclosure.PublicationRequest)({
            version: 1,
            sessionID: captured.sessionID,
            inputID: captured.inputID,
            contextReference: captured.contextReference,
            contextDigest: captured.contextDigest,
            componentsDigest: captured.componentsDigest,
            attempt: captured.attempt,
            kind: "model-output",
            unitID: unit.unitID,
            unitDigest: unit.unitDigest,
          })
          const response = await fetch(endpoint, {
            method: "POST",
            redirect: "error",
            credentials: "omit",
            cache: "no-store",
            headers: { authorization: "Bearer " + service.token, "content-type": "application/json" },
            body: JSON.stringify(request),
            signal: AbortSignal.any([signal, AbortSignal.timeout(5000)]),
          })
          if (!response.ok || !response.body) {
            await response.body?.cancel()
            throw new Error()
          }
          const reader = response.body.getReader()
          const chunks: Uint8Array[] = []
          let size = 0
          try {
            while (true) {
              const chunk = await reader.read()
              if (chunk.done) break
              size += chunk.value.byteLength
              if (size > 64 * 1024) throw new Error()
              chunks.push(chunk.value)
            }
          } finally {
            await reader.cancel()
          }
          const signed = Schema.decodeUnknownSync(Schema.fromJsonString(ExecutionDisclosure.SignedProof))(
            Buffer.concat(chunks).toString("utf8"),
          )
          const key = descriptor.keys.find((key) => key.id === signed.keyID)
          if (!key) throw new Error()
          const publicKey = createPublicKey({
            key: Buffer.concat([
              Buffer.from("302a300506032b6570032100", "hex"),
              Buffer.from(key.publicKey, "base64url"),
            ]),
            format: "der",
            type: "spki",
          })
          if (!verify(null, Buffer.from(signed.payload, "utf8"), publicKey, Buffer.from(signed.signature, "base64url")))
            throw new Error()
          const proof = Schema.decodeUnknownSync(Schema.fromJsonString(ExecutionDisclosure.PublicationProof))(
            signed.payload,
          )
          if (
            Object.entries(request).some(([key, value]) => proof[key as keyof typeof request] !== value) ||
            proof.issuer !== service.issuer ||
            proof.audience !== descriptor.audience ||
            proof.organizationID !== descriptor.organizationID ||
            proof.threadID !== descriptor.threadID ||
            proof.capabilityID !== descriptor.capabilityID ||
            proof.capabilityGeneration !== descriptor.capabilityGeneration
          )
            throw new Error()
          // Keep the decision independent of mutable caller-visible proof objects.
          const issuedAt = proof.issuedAt
          const expiresAt = proof.expiresAt
          const valid = () => {
            const now = Date.now()
            return (
              expiresAt > now &&
              issuedAt <= now + 30_000 &&
              expiresAt > issuedAt &&
              expiresAt - issuedAt <= 30_000 &&
              now - issuedAt <= 60_000
            )
          }
          if (!valid()) throw new Error()
          return { proof: Object.freeze(proof), signed: Object.freeze(signed), valid }
        },
        catch: () => new Error("Execution publication authorization is unavailable"),
      }).pipe(Effect.orDie),
  }
}
