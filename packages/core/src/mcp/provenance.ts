export * as McpProvenance from "./provenance.js"

import { createPublicKey, verify } from "node:crypto"
import { Schema } from "effect"
import { parseTree, type Node, type ParseError } from "jsonc-parser"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import type { ExecutionProducer } from "@opencode/schema/execution-producer"
import { Hash } from "@opencode/util/hash"

export const RequestKey = "access.execution.context"
export const ResultKey = "access.execution.result"
export const CatalogKey = "access.execution.catalog"
export const MaximumSnapshot = 4 * 1024 * 1024
const unavailable = () => new Error("MCP provenance authorization is unavailable")
const unicode = (value: string) => Buffer.from(value, "utf8").toString("utf8") === value
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)

// Compare decimal values to the receiving number's shortest JSON spelling.
// Parsing both copies with JSON.parse alone would hide rounded signed tokens.
const decimal = (token: string) => {
  const parts = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(token)
  if (!parts) throw unavailable()
  let digits = (parts[2] + (parts[3] ?? "")).replace(/^0+/, "")
  let exponent = BigInt(parts[4] ?? "0") - BigInt((parts[3] ?? "").length)
  if (digits === "") return parts[1] + "0"
  const zeros = /0+$/.exec(digits)?.[0].length ?? 0
  if (zeros) {
    digits = digits.slice(0, -zeros)
    exponent += BigInt(zeros)
  }
  return parts[1] + digits + "e" + exponent
}

// JSONC's syntax tree retains original numeric spelling and duplicate keys.
// Comments, trailing commas, invalid Unicode and excess root values fail closed.
export const parse = (raw: string): unknown => {
  if (!unicode(raw) || Buffer.byteLength(raw) > MaximumSnapshot) throw unavailable()
  const errors: ParseError[] = []
  const root = parseTree(raw, errors, { disallowComments: true, allowTrailingComma: false })
  if (!root || errors.length) throw unavailable()
  const read = (node: Node, depth: number): unknown => {
    if (depth > 128) throw unavailable()
    if (node.type === "number") {
      const token = raw.slice(node.offset, node.offset + node.length)
      const value: unknown = node.value
      if (
        token.length > 128 ||
        typeof value !== "number" ||
        !Number.isFinite(value) ||
        (Number.isInteger(value) && !Number.isSafeInteger(value)) ||
        decimal(token) !== decimal(JSON.stringify(value))
      )
        throw unavailable()
      return value
    }
    if (node.type === "string") {
      const value: unknown = node.value
      if (typeof value !== "string" || !unicode(value)) throw unavailable()
      return value
    }
    if (node.type === "array") return (node.children ?? []).map((child) => read(child, depth + 1))
    if (node.type === "object") {
      const keys = new Set<string>()
      return Object.fromEntries(
        (node.children ?? []).map((child) => {
          const [name, value] = child.children ?? []
          const key = name && read(name, depth + 1)
          if (typeof key !== "string" || !value || keys.has(key)) throw unavailable()
          keys.add(key)
          return [key, read(value, depth + 1)]
        }),
      )
    }
    if (node.type === "null" || node.type === "boolean") return node.value
    throw unavailable()
  }
  return read(root, 0)
}

// Own the complete SDK value without executing getters/toJSON or losing keys,
// undefined values, holes, nonfinite numbers or special object prototypes.
export const own = (value: unknown, depth = 0): unknown => {
  if (depth > 128) throw unavailable()
  if (value === null || typeof value === "boolean") return value
  if (typeof value === "string") {
    if (!unicode(value)) throw unavailable()
    return value
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value) || Object.is(value, -0)) throw unavailable()
    return value
  }
  if (typeof value !== "object") throw unavailable()
  const prototype: unknown = Object.getPrototypeOf(value)
  if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null) throw unavailable()
  const descriptors = Object.getOwnPropertyDescriptors(value)
  if (Object.getOwnPropertySymbols(value).length !== 0) throw unavailable()
  const entries = Object.entries(descriptors).filter(([key]) => !Array.isArray(value) || key !== "length")
  if (entries.some(([, descriptor]) => !descriptor.enumerable || !Object.hasOwn(descriptor, "value")))
    throw unavailable()
  if (Array.isArray(value)) {
    if (entries.length !== value.length || entries.some(([key], index) => key !== String(index))) throw unavailable()
    return Object.freeze(entries.map(([, descriptor]) => own(descriptor.value, depth + 1)))
  }
  if (entries.some(([key]) => !unicode(key))) throw unavailable()
  return Object.freeze(Object.fromEntries(entries.map(([key, descriptor]) => [key, own(descriptor.value, depth + 1)])))
}

export const equal = (left: unknown, right: unknown): boolean => {
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object")
    return Object.is(left, right)
  if (Array.isArray(left) || Array.isArray(right))
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => equal(value, right[index]))
    )
  if (!record(left) || !record(right)) return false
  const a = Object.entries(left)
  const b = Object.entries(right)
  return a.length === b.length && a.every(([key, value]) => Object.hasOwn(right, key) && equal(value, right[key]))
}

const decode = <S extends Schema.Top & { readonly DecodingServices: never }>(schema: S, input: unknown) =>
  Schema.decodeUnknownSync(schema, { onExcessProperty: "error" })(input)

const signedProof = (input: unknown, keys: readonly ExecutionDisclosure.VerificationKey[]) => {
  const signed = decode(ExecutionDisclosure.SignedProof, own(input))
  const key = keys.find((key) => key.id === signed.keyID)
  if (!key) throw unavailable()
  const publicKey = createPublicKey({
    key: Buffer.concat([Buffer.from("302a300506032b6570032100", "hex"), Buffer.from(key.publicKey, "base64url")]),
    format: "der",
    type: "spki",
  })
  if (!verify(null, Buffer.from(signed.payload, "utf8"), publicKey, Buffer.from(signed.signature, "base64url")))
    throw unavailable()
  return Object.freeze(signed)
}

export interface Capture {
  readonly issuer: string
  readonly context: ExecutionDisclosure.ContextProof
  readonly signed: ExecutionDisclosure.SignedProof
}

// This is a second cryptographic check at capture construction, rather than
// trusting caller-owned decoded fields or unsigned arbitrary metadata.
export const capture = (installed: ExecutionDisclosure.Service, raw: ExecutionDisclosure.SignedProof): Capture => {
  const service = decode(ExecutionDisclosure.Service, own(installed))
  const signed = signedProof(raw, service.keys)
  const context = decode(ExecutionDisclosure.ContextProof, parse(signed.payload))
  const tools = context.tools
  const publication = context.publication
  const now = Date.now()
  if (
    !tools ||
    !publication ||
    context.issuer !== service.issuer ||
    context.audience !== service.audience ||
    context.kind !== "primary" ||
    context.complete !== (context.mode === "complete") ||
    !/^[a-f0-9]{64}$/.test(context.contextReference) ||
    !Number.isSafeInteger(context.issuedAt) ||
    !Number.isSafeInteger(context.expiresAt) ||
    context.issuedAt > now + 30_000 ||
    context.expiresAt <= now ||
    context.expiresAt <= context.issuedAt ||
    context.expiresAt - context.issuedAt > 120_000 ||
    now - context.issuedAt > 120_000 ||
    tools.audience === service.audience ||
    tools.audience === publication.audience ||
    new Set(tools.keys.map((key) => key.id)).size !== tools.keys.length ||
    tools.keys.some((key) => [...service.keys, ...publication.keys].some((other) => key.publicKey === other.publicKey))
  )
    throw unavailable()
  // Runtime-owned deep immutable JSON copies survive caller mutation.
  return Object.freeze({
    issuer: service.issuer,
    context: own(context) as ExecutionDisclosure.ContextProof,
    signed: Object.freeze(signed),
  })
}

export interface Observation {
  readonly invocation: ExecutionProducer.SignedFact
  readonly proof: ExecutionDisclosure.ToolResultProof
  readonly signed: ExecutionDisclosure.SignedProof
  readonly snapshot: string
}

export const result = <A>(capture: Capture, invocation: ExecutionProducer.McpInvocation, actual: A) => {
  const received = own(actual)
  if (!record(received) || !record(received._meta) || !Object.hasOwn(received._meta, ResultKey)) throw unavailable()
  const carrier = decode(ExecutionDisclosure.ToolResultCarrier, received._meta[ResultKey])
  const bytes = Buffer.from(carrier.snapshot, "base64")
  if (bytes.length > MaximumSnapshot || bytes.toString("base64") !== carrier.snapshot) throw unavailable()
  const snapshot = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes)
  const signed = signedProof(carrier.proof, capture.context.tools!.keys)
  const proof = decode(ExecutionDisclosure.ToolResultProof, parse(signed.payload))
  const publication = capture.context.publication!
  const now = Date.now()
  if (
    proof.issuer !== capture.issuer ||
    proof.audience !== capture.context.tools!.audience ||
    proof.organizationID !== publication.organizationID ||
    proof.threadID !== publication.threadID ||
    proof.capabilityID !== publication.capabilityID ||
    proof.capabilityGeneration !== publication.capabilityGeneration ||
    proof.responseRevision !== Hash.sha256(bytes) ||
    proof.complete !== (proof.mode === "complete") ||
    !Number.isSafeInteger(proof.issuedAt) ||
    !Number.isSafeInteger(proof.expiresAt) ||
    proof.issuedAt > now + 30_000 ||
    proof.expiresAt <= now ||
    proof.expiresAt <= proof.issuedAt ||
    proof.expiresAt - proof.issuedAt > 30_000 ||
    now - proof.issuedAt > 30_000 ||
    (
      [
        "sessionID",
        "inputID",
        "attempt",
        "invocationID",
        "messageID",
        "toolCallID",
        "server",
        "tool",
        "argumentsRevision",
        "contextReference",
        "contextProofRevision",
      ] as const
    ).some((key) => proof[key] !== invocation[key])
  )
    throw unavailable()
  const expected = parse(snapshot)
  if (!record(expected)) throw unavailable()
  if (expected._meta !== undefined && (!record(expected._meta) || Object.hasOwn(expected._meta, ResultKey)))
    throw unavailable()
  const meta = Object.fromEntries(Object.entries(received._meta).filter(([key]) => key !== ResultKey))
  const cleaned = Object.fromEntries(Object.entries(received).filter(([key]) => key !== "_meta"))
  if (Object.keys(meta).length || Object.hasOwn(expected, "_meta")) cleaned._meta = meta
  if (!equal(expected, cleaned)) throw unavailable()
  // Full strict tree equality preserves the SDK result type at this boundary.
  return { result: own(cleaned) as A, proof: own(proof) as ExecutionDisclosure.ToolResultProof, signed, snapshot }
}

export interface CatalogObservation {
  readonly server: string
  readonly proof: ExecutionDisclosure.CatalogProof
  readonly signed: ExecutionDisclosure.SignedProof
  readonly snapshot: string
}

// Raw discovery pages are checked before SDK aggregation/filtering. Namespace
// comes from installed native configuration, never a catalog-controlled field.
export const catalog = <A>(
  input: {
    readonly service: ExecutionDisclosure.Service
    readonly sessionID: ExecutionDisclosure.ContextProof["sessionID"]
    readonly inputID: ExecutionDisclosure.ContextProof["inputID"]
    readonly revision: number
    readonly server: string
    readonly kind: "initialize" | "tools/list"
    readonly cursor: string
  },
  actual: A,
) => {
  const received = own(actual)
  if (!record(received) || !record(received._meta) || !Object.hasOwn(received._meta, CatalogKey)) throw unavailable()
  const carrier = decode(ExecutionDisclosure.ToolResultCarrier, received._meta[CatalogKey])
  const bytes = Buffer.from(carrier.snapshot, "base64")
  if (bytes.length > MaximumSnapshot || bytes.toString("base64") !== carrier.snapshot) throw unavailable()
  const snapshot = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes)
  const descriptor = input.service.catalog
  if (
    !descriptor ||
    descriptor.audience === input.service.audience ||
    new Set(descriptor.keys.map((key) => key.id)).size !== descriptor.keys.length ||
    descriptor.keys.some((key) => input.service.keys.some((other) => key.publicKey === other.publicKey))
  )
    throw unavailable()
  const signed = signedProof(carrier.proof, descriptor.keys)
  const proof = decode(ExecutionDisclosure.CatalogProof, parse(signed.payload))
  const now = Date.now()
  if (
    proof.issuer !== input.service.issuer ||
    proof.audience !== descriptor.audience ||
    proof.sessionID !== input.sessionID ||
    proof.inputID !== input.inputID ||
    proof.capabilityGeneration !== input.revision ||
    proof.kind !== input.kind ||
    proof.cursor !== input.cursor ||
    proof.responseRevision !== Hash.sha256(bytes) ||
    proof.complete !== (proof.mode === "complete") ||
    !Number.isSafeInteger(proof.issuedAt) ||
    !Number.isSafeInteger(proof.expiresAt) ||
    proof.issuedAt > now + 30_000 ||
    proof.expiresAt <= now ||
    proof.expiresAt <= proof.issuedAt ||
    proof.expiresAt - proof.issuedAt > 30_000 ||
    now - proof.issuedAt > 30_000
  )
    throw unavailable()
  const expected = parse(snapshot)
  if (
    !record(expected) ||
    (expected._meta !== undefined && (!record(expected._meta) || Object.hasOwn(expected._meta, CatalogKey)))
  )
    throw unavailable()
  const meta = Object.fromEntries(Object.entries(received._meta).filter(([key]) => key !== CatalogKey))
  const cleaned = Object.fromEntries(Object.entries(received).filter(([key]) => key !== "_meta"))
  if (Object.keys(meta).length || Object.hasOwn(expected, "_meta")) cleaned._meta = meta
  if (!equal(expected, cleaned)) throw unavailable()
  return {
    result: own(cleaned) as A,
    observation: Object.freeze({
      server: input.server,
      proof: own(proof) as ExecutionDisclosure.CatalogProof,
      signed,
      snapshot,
    }),
  }
}
