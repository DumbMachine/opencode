export * as SessionDisclosure from "./disclosure.js"

import { createPublicKey, randomUUID, verify } from "node:crypto"
import { Effect, Schema } from "effect"
import { LLMRequest, Media } from "@opencode/ai"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { Hash } from "@opencode/util/hash"
import { SessionInputRequirements } from "./input-requirements.js"
import { SessionPublication } from "./publication.js"

// Canonical ordering is for content identity, never a provenance assertion.
export const digest = (value: unknown): string => {
  const canonical = (item: unknown): unknown => {
    if (item instanceof Uint8Array) return { bytes: Buffer.from(item).toString("base64") }
    if (Array.isArray(item)) return item.map(canonical)
    if (item !== null && typeof item === "object")
      return Object.fromEntries(
        Object.entries(item)
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([key, value]) => [key, canonical(value)]),
      )
    return item
  }
  return Hash.sha256(JSON.stringify(canonical(value)))
}

type ContextInput = Omit<ExecutionDisclosure.ContextRequest, "version" | "attempt" | "componentsDigest" | "composition">

// Fix one owned attempt before any producer signs its identity. Composition
// remains outside the components digest, so neither signature hashes itself.
export const makeRequest = (request: ContextInput): ExecutionDisclosure.ContextRequest => {
  try {
    const owned = structuredClone(request)
    return Schema.decodeUnknownSync(ExecutionDisclosure.ContextRequest)({
      ...owned,
      version: 1,
      attempt: randomUUID(),
      componentsDigest: digest(owned.components),
    })
  } catch {
    throw new Error("Execution context authorization is unavailable")
  }
}

// Convenience for unobserved/legacy context. Each execution keeps the original
// behavior of generating its own attempt rather than reusing an Effect value.
export const prepare = (service: ExecutionDisclosure.Service, request: ContextInput) =>
  Effect.suspend(() => prepareRequest(service, makeRequest(request)))

// Called after shaping and optional producer observation. Post this exact
// owned attempt; do not generate another attempt after its signature.
export const prepareRequest = (service: ExecutionDisclosure.Service, request: ExecutionDisclosure.ContextRequest) =>
  Effect.tryPromise({
    try: async (signal) => {
      const config = Schema.decodeUnknownSync(ExecutionDisclosure.Service)(structuredClone(service))
      if (!SessionInputRequirements.serviceSupported(config)) throw new Error()
      const endpoint = new URL(config.endpoint)
      if (
        endpoint.username ||
        endpoint.password ||
        endpoint.hash ||
        endpoint.search ||
        (endpoint.protocol !== "https:" &&
          (endpoint.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname)))
      )
        throw new Error()
      if (new Set(config.keys.map((key) => key.id)).size !== config.keys.length) throw new Error()
      const input = Schema.decodeUnknownSync(ExecutionDisclosure.ContextRequest)(structuredClone(request))
      if (input.componentsDigest !== digest(input.components)) throw new Error()
      if (
        config.compositionVersion !== 4 &&
        input.components.some((component) => (component.proof?.length ?? 0) > 4096)
      )
        throw new Error()
      const body = JSON.stringify(input)
      if (Buffer.byteLength(body) > 512 * 1024) throw new Error()
      const response = await fetch(endpoint, {
        method: "POST",
        redirect: "error",
        credentials: "omit",
        cache: "no-store",
        headers: { authorization: "Bearer " + config.token, "content-type": "application/json" },
        body,
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
      const key = config.keys.find((key) => key.id === signed.keyID)
      if (!key) throw new Error()
      const publicKey = createPublicKey({
        key: Buffer.concat([Buffer.from("302a300506032b6570032100", "hex"), Buffer.from(key.publicKey, "base64url")]),
        format: "der",
        type: "spki",
      })
      if (!verify(null, Buffer.from(signed.payload, "utf8"), publicKey, Buffer.from(signed.signature, "base64url")))
        throw new Error()
      const proof = Schema.decodeUnknownSync(Schema.fromJsonString(ExecutionDisclosure.ContextProof))(signed.payload)
      const now = Date.now()
      if (
        proof.issuer !== config.issuer ||
        proof.audience !== config.audience ||
        proof.sessionID !== input.sessionID ||
        proof.inputID !== input.inputID ||
        proof.kind !== input.kind ||
        proof.attempt !== input.attempt ||
        proof.contextDigest !== input.contextDigest ||
        proof.componentsDigest !== input.componentsDigest ||
        proof.complete !== (proof.mode === "complete") ||
        proof.expiresAt <= now ||
        proof.issuedAt > now + 30_000 ||
        proof.expiresAt <= proof.issuedAt ||
        proof.expiresAt - proof.issuedAt > 120_000 ||
        now - proof.issuedAt > 120_000
      )
        throw new Error()
      return { proof, signed, publication: SessionPublication.make(config, proof) }
    },
    catch: () => new Error("Execution context authorization is unavailable"),
  }).pipe(Effect.orDie)

// Hooks may retain aliases to their drafts. Own the complete logical content
// before callback I/O, retaining schema-class prototypes and media behavior.
// Remote/provider-only media cannot prove exact content at this boundary yet.
export const snapshot = (request: LLMRequest) => {
  const own = (value: unknown): unknown => {
    if (value instanceof Media.Asset) {
      if (value.source.type !== "bytes" && value.source.type !== "base64")
        throw new Error("Execution context authorization is unavailable")
      return Object.freeze(
        Media.from(structuredClone(value.source), {
          info: structuredClone(value.info),
          providerMetadata: structuredClone(value.providerMetadata),
        }),
      )
    }
    if (value instanceof Uint8Array) return value.slice()
    if (Array.isArray(value)) return Object.freeze(value.map(own))
    if (typeof value === "function" || typeof value === "symbol")
      throw new Error("Execution context authorization is unavailable")
    if (value === null || typeof value !== "object") return value
    const result = Object.create(Object.getPrototypeOf(value))
    for (const [key, item] of Object.entries(value))
      Object.defineProperty(result, key, { value: own(item), enumerable: true })
    return Object.freeze(result)
  }
  const logical = logicalContext(request)
  // The recursive copy preserves every logical field's type and prototype.
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
  return LLMRequest.update(request, own(logical) as typeof logical)
}

export const logicalContext = (request: LLMRequest) => ({
  system: request.system,
  messages: request.messages,
  tools: request.tools,
  toolChoice: request.toolChoice,
  generation: request.generation,
  providerOptions: request.providerOptions,
  metadata: request.metadata,
})
