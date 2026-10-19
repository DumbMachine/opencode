export * as ExecutionDisclosure from "./execution-disclosure.js"

import { Schema } from "effect"
import { ExecutionProducer } from "./execution-producer.js"
import { VerificationKey } from "./execution-verification-key.js"
export { VerificationKey } from "./execution-verification-key.js"
import { optional } from "./schema.js"
import { SessionID } from "./session-id.js"
import { SessionMessageID } from "./session-message-id.js"

const bounded = (maximum: number) => Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(maximum))
const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))

// Host-selected and process-local. Never include this in an admitted payload,
// history, event, tool definition, plugin event or public response.
export interface Service extends Schema.Schema.Type<typeof Service> {}
export const Service = Schema.Struct({
  version: Schema.Literal(1),
  endpoint: bounded(2048),
  token: bounded(16384),
  // Recognized requirement; admission/execution support is negotiated separately.
  publicationVersion: Schema.Literals([1, 2]).pipe(optional),
  // The installed host callback explicitly accepts partial producer evidence.
  // Absence preserves the original request shape for older installations.
  compositionVersion: Schema.Literals([1, 2, 3, 4]).pipe(optional),
  issuer: bounded(512),
  audience: bounded(512),
  keys: Schema.Array(VerificationKey).check(Schema.isMinLength(1), Schema.isMaxLength(8)),
  catalog: Schema.Struct({
    version: Schema.Literal(1),
    audience: bounded(512),
    keys: Schema.Array(VerificationKey).check(Schema.isMinLength(1), Schema.isMaxLength(8)),
  }).pipe(optional),
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
  proof: bounded(16384).pipe(optional),
}).annotate({ identifier: "ExecutionDisclosure.Component" })

export interface ContextRequest extends Schema.Schema.Type<typeof ContextRequest> {}
export const ContextRequest = Schema.Struct({
  version: Schema.Literal(1),
  sessionID: SessionID,
  inputID: SessionMessageID,
  kind: Schema.Literals(["primary", "input", "compaction", "title", "generate"]),
  attempt: bounded(128),
  contextDigest: Digest,
  componentsDigest: Digest,
  components: Schema.Array(Component).check(Schema.isMaxLength(2048)),
  // Kept outside the component digest to avoid a circular signature binding.
  composition: ExecutionProducer.SignedFact.pipe(optional),
}).annotate({ identifier: "ExecutionDisclosure.ContextRequest" })

// Signed inside ContextProof; never trusted from an unsigned response field.
export interface PublicationService extends Schema.Schema.Type<typeof PublicationService> {}
export const PublicationService = Schema.Struct({
  version: Schema.Literals([1, 2]),
  endpoint: bounded(2048),
  audience: bounded(512),
  keys: Schema.Array(VerificationKey).check(Schema.isMinLength(1), Schema.isMaxLength(8)),
  organizationID: bounded(32),
  threadID: bounded(64),
  capabilityID: bounded(32),
  capabilityGeneration: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
}).annotate({ identifier: "ExecutionDisclosure.PublicationService" })

// Separate purpose-derived result keys, pinned by the signed model context.
export interface ToolService extends Schema.Schema.Type<typeof ToolService> {}
export const ToolService = Schema.Struct({
  version: Schema.Literal(1),
  audience: bounded(512),
  keys: Schema.Array(VerificationKey).check(Schema.isMinLength(1), Schema.isMaxLength(8)),
}).annotate({ identifier: "ExecutionDisclosure.ToolService" })

export interface ContextProof extends Schema.Schema.Type<typeof ContextProof> {}
export const ContextProof = Schema.Struct({
  version: Schema.Literal(1),
  issuer: bounded(512),
  audience: bounded(512),
  sessionID: SessionID,
  inputID: SessionMessageID,
  kind: Schema.Literals(["primary", "input", "compaction", "title", "generate"]),
  attempt: bounded(128),
  contextDigest: Digest,
  componentsDigest: Digest,
  contextReference: bounded(256),
  publication: PublicationService.pipe(optional),
  tools: ToolService.pipe(optional),
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

// A fresh permit binds one immutable output unit, not a general stream.
export interface PublicationRequest extends Schema.Schema.Type<typeof PublicationRequest> {}
export const PublicationRequest = Schema.Struct({
  version: Schema.Literal(1),
  sessionID: SessionID,
  inputID: SessionMessageID,
  contextReference: Digest,
  contextDigest: Digest,
  componentsDigest: Digest,
  attempt: bounded(128),
  kind: Schema.Literals(["model-output", "human-input"]),
  unitID: bounded(128),
  unitDigest: Digest,
}).annotate({ identifier: "ExecutionDisclosure.PublicationRequest" })

export interface PublicationProof extends Schema.Schema.Type<typeof PublicationProof> {}
export const PublicationProof = Schema.Struct({
  ...PublicationRequest.fields,
  purpose: Schema.Literal("thread-publication"),
  issuer: bounded(512),
  audience: bounded(512),
  organizationID: bounded(32),
  threadID: bounded(64),
  capabilityID: bounded(32),
  capabilityGeneration: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  receiptReference: Digest,
  authorizationVersion: bounded(256),
  manifestBinding: Digest,
  audienceBinding: Digest,
  issuedAt: Schema.Int,
  expiresAt: Schema.Int,
}).annotate({ identifier: "ExecutionDisclosure.PublicationProof" })

// Exact host observation before any native MCP conversion or normalization.
export interface ToolResultProof extends Schema.Schema.Type<typeof ToolResultProof> {}
export const ToolResultProof = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-mcp-result"),
  issuer: bounded(512),
  audience: bounded(512),
  organizationID: bounded(32),
  threadID: bounded(64),
  sessionID: SessionID,
  inputID: SessionMessageID,
  capabilityID: bounded(32),
  capabilityGeneration: PublicationService.fields.capabilityGeneration,
  contextReference: Digest,
  contextProofRevision: Digest,
  attempt: bounded(128),
  invocationID: bounded(128),
  messageID: SessionMessageID,
  toolCallID: bounded(256),
  server: bounded(256),
  tool: bounded(256),
  argumentsRevision: Digest,
  responseRevision: Digest,
  receiptReference: Digest,
  authorizationVersion: bounded(256),
  manifestBinding: Digest,
  audienceBinding: Digest,
  mode: Schema.Literals(["complete", "actor-private-unknown", "terminal-control"]),
  complete: Schema.Boolean,
  issuedAt: Schema.Int,
  expiresAt: Schema.Int,
}).annotate({ identifier: "ExecutionDisclosure.ToolResultProof" })

export interface ToolResultCarrier extends Schema.Schema.Type<typeof ToolResultCarrier> {}
export const ToolResultCarrier = Schema.Struct({
  version: Schema.Literal(1),
  // The receiving boundary enforces canonical base64 and the decoded 4 MiB limit.
  snapshot: bounded(4 * Math.ceil((4 * 1024 * 1024) / 3)),
  proof: SignedProof,
}).annotate({ identifier: "ExecutionDisclosure.ToolResultCarrier" })

// Discovery is needed before the first model context. Its keys are installed
// with the immutable input capability rather than obtained from ContextProof.
export interface CatalogProof extends Schema.Schema.Type<typeof CatalogProof> {}
export const CatalogProof = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-mcp-catalog"),
  issuer: bounded(512),
  audience: bounded(512),
  organizationID: bounded(32),
  threadID: bounded(64),
  sessionID: SessionID,
  inputID: SessionMessageID,
  capabilityID: bounded(32),
  capabilityGeneration: PublicationService.fields.capabilityGeneration,
  kind: Schema.Literals(["initialize", "tools/list"]),
  cursor: Schema.String.check(Schema.isMaxLength(2048)),
  responseRevision: Digest,
  receiptReference: Digest,
  authorizationVersion: bounded(256),
  manifestBinding: Digest,
  audienceBinding: Digest,
  mode: Schema.Literals(["complete", "actor-private-unknown"]),
  complete: Schema.Boolean,
  issuedAt: Schema.Int,
  expiresAt: Schema.Int,
}).annotate({ identifier: "ExecutionDisclosure.CatalogProof" })

export interface ToolPublicationRequest extends Schema.Schema.Type<typeof ToolPublicationRequest> {}
export const ToolPublicationRequest = Schema.Struct({
  ...PublicationRequest.fields,
  version: Schema.Literal(2),
  kind: Schema.Literals(["model-output", "human-input", "tool-output", "tool-control", "tool-spill"]),
  projection: ExecutionProducer.SignedToolFact.pipe(optional),
}).annotate({ identifier: "ExecutionDisclosure.ToolPublicationRequest" })

export interface ToolPublicationProof extends Schema.Schema.Type<typeof ToolPublicationProof> {}
export const ToolPublicationProof = Schema.Struct({
  ...PublicationProof.fields,
  version: Schema.Literal(2),
  kind: ToolPublicationRequest.fields.kind,
  mode: Schema.Literals(["complete", "actor-private-unknown", "terminal-control"]),
  complete: Schema.Boolean,
}).annotate({ identifier: "ExecutionDisclosure.ToolPublicationProof" })
