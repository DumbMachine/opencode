export * as ExecutionDisclosure from "./execution-disclosure.js"

import { Schema } from "effect"
import { optional } from "./schema.js"
import { Session } from "./session.js"
import { SessionMessage } from "./session-message.js"

const bounded = (maximum: number) => Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(maximum))
const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))

export interface VerificationKey extends Schema.Schema.Type<typeof VerificationKey> {}
export const VerificationKey = Schema.Struct({
  id: bounded(128),
  algorithm: Schema.Literal("Ed25519"),
  publicKey: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{43}$/)),
}).annotate({ identifier: "ExecutionDisclosure.VerificationKey" })

// Host-selected and process-local. Never include this in an admitted payload,
// history, event, tool definition, plugin event or public response.
export interface Service extends Schema.Schema.Type<typeof Service> {}
export const Service = Schema.Struct({
  version: Schema.Literal(1),
  endpoint: bounded(2048),
  token: bounded(16384),
  issuer: bounded(512),
  audience: bounded(512),
  keys: Schema.Array(VerificationKey).check(Schema.isMinLength(1), Schema.isMaxLength(8)),
}).annotate({ identifier: "ExecutionDisclosure.Service" })

// References identify selected content; a hash alone supplies no authority or
// source-free claim. The host resolves their original trusted provenance.
export interface Component extends Schema.Schema.Type<typeof Component> {}
export const Component = Schema.Struct({
  kind: Schema.Literals([
    "message",
    "instructions",
    "input-instructions",
    "agent",
    "tools",
    "ephemeral",
    "prepared-context",
  ]),
  id: bounded(256),
  revision: Digest,
  proof: bounded(4096).pipe(optional),
}).annotate({ identifier: "ExecutionDisclosure.Component" })

export interface ContextRequest extends Schema.Schema.Type<typeof ContextRequest> {}
export const ContextRequest = Schema.Struct({
  version: Schema.Literal(1),
  sessionID: Session.ID,
  inputID: SessionMessage.ID,
  kind: Schema.Literals(["primary", "compaction", "title", "generate"]),
  attempt: bounded(128),
  contextDigest: Digest,
  componentsDigest: Digest,
  components: Schema.Array(Component).check(Schema.isMaxLength(2048)),
}).annotate({ identifier: "ExecutionDisclosure.ContextRequest" })

export interface ContextProof extends Schema.Schema.Type<typeof ContextProof> {}
export const ContextProof = Schema.Struct({
  version: Schema.Literal(1),
  issuer: bounded(512),
  audience: bounded(512),
  sessionID: Session.ID,
  inputID: SessionMessage.ID,
  kind: Schema.Literals(["primary", "compaction", "title", "generate"]),
  attempt: bounded(128),
  contextDigest: Digest,
  componentsDigest: Digest,
  contextReference: bounded(256),
  mode: Schema.Literals(["complete", "actor-private-unknown"]),
  complete: Schema.Boolean,
  issuedAt: Schema.Int,
  expiresAt: Schema.Int,
}).annotate({ identifier: "ExecutionDisclosure.ContextProof" })

// The signature covers the exact UTF-8 payload, not a re-encoding of JSON.
export interface SignedProof extends Schema.Schema.Type<typeof SignedProof> {}
export const SignedProof = Schema.Struct({
  keyID: bounded(128),
  payload: bounded(16384),
  signature: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{86}$/)),
}).annotate({ identifier: "ExecutionDisclosure.SignedProof" })
