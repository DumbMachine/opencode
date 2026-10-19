export * as ExecutionProducer from "./execution-producer.js"

import { Schema } from "effect"
import { VerificationKey } from "./execution-verification-key.js"
import { SessionID } from "./session-id.js"
import { SessionMessageID } from "./session-message-id.js"
import { optional } from "./schema.js"

const bounded = (maximum: number) => Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(maximum))
const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))
export const SignatureDomain = "opencode.execution-producer.v1\0"
export const Contract = Schema.Literals([
  "human-plaintext-identity-v1",
  "model-plaintext-projection-v1",
  "model-request-only-plaintext-v1",
  "human-inline-identity-v1",
  "model-request-only-inline-v1",
  "mcp-invocation-v1",
  "mcp-result-projection-v1",
  "tool-definitions-v1",
  "model-tool-context-v1",
])

export interface SignedFact extends Schema.Schema.Type<typeof SignedFact> {}
export const SignedFact = Schema.Struct({
  keyID: bounded(128),
  payload: bounded(4096),
  signature: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{86}$/)),
}).annotate({ identifier: "ExecutionProducer.SignedFact" })

export interface EnrollmentRequest extends Schema.Schema.Type<typeof EnrollmentRequest> {}
export const EnrollmentRequest = Schema.Struct({
  version: Schema.Literal(1),
  challenge: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{43}$/)),
  audience: bounded(512),
  contracts: Schema.Array(Contract).check(Schema.isMinLength(1), Schema.isMaxLength(9)).pipe(optional),
}).annotate({ identifier: "ExecutionProducer.EnrollmentRequest" })

export interface EnrollmentProof extends Schema.Schema.Type<typeof EnrollmentProof> {}
export const EnrollmentProof = Schema.Struct({
  version: EnrollmentRequest.fields.version,
  challenge: EnrollmentRequest.fields.challenge,
  audience: EnrollmentRequest.fields.audience,
  purpose: Schema.Literal("native-producer-enrollment"),
  producerEpoch: bounded(128),
  bootNonce: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{43}$/)).pipe(optional),
  key: VerificationKey,
  contracts: Schema.Array(Contract).check(Schema.isMinLength(1), Schema.isMaxLength(9)),
  issuedAt: Schema.Int,
}).annotate({ identifier: "ExecutionProducer.EnrollmentProof" })

export interface Enrollment extends Schema.Schema.Type<typeof Enrollment> {}
export const Enrollment = Schema.Struct({
  key: VerificationKey,
  signed: SignedFact,
}).annotate({ identifier: "ExecutionProducer.Enrollment" })

// Preparation is an observation, not an independent admission receipt or a
// completeness assertion. Only the winning native payload retains this fact.
export interface PreparedInput extends Schema.Schema.Type<typeof PreparedInput> {}
export const PreparedInput = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-input-preparation"),
  producerEpoch: bounded(128),
  contract: Schema.Literal("human-plaintext-identity-v1"),
  sessionID: SessionID,
  inputID: SessionMessageID,
  parentTextRevision: Digest,
  preparedTextRevision: Digest,
  preparedPayloadRevision: Digest,
}).annotate({ identifier: "ExecutionProducer.PreparedInput" })

// Separate from plaintext identity: this commitment includes exact text and
// ordered inline descriptors, byte digests and lengths, including empty text.
export interface PreparedInlineInput extends Schema.Schema.Type<typeof PreparedInlineInput> {}
export const PreparedInlineInput = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-input-preparation"),
  producerEpoch: bounded(128),
  contract: Schema.Literal("human-inline-identity-v1"),
  sessionID: SessionID,
  inputID: SessionMessageID,
  parentContentRevision: Digest,
  preparedContentRevision: Digest,
  preparedPayloadRevision: Digest,
}).annotate({ identifier: "ExecutionProducer.PreparedInlineInput" })

// A projection is a bounded observation of one consumed human message, not a
// completeness claim about the rest of the request or its physical transport.
export interface HumanProjection extends Schema.Schema.Type<typeof HumanProjection> {}
export const HumanProjection = Schema.Struct({
  version: Schema.Literal(1),
  contract: Schema.Literal("session-user-plaintext-v1"),
  // Text identity does not establish provenance for metadata or other message
  // fields, even when their exact projection is included in the digest.
  complete: Schema.Literal(false),
  preparedInput: SignedFact,
  index: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  projectionRevision: Digest,
}).annotate({ identifier: "ExecutionProducer.HumanProjection" })

export interface ModelComposition extends Schema.Schema.Type<typeof ModelComposition> {}
export const ModelComposition = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-model-composition"),
  producerEpoch: bounded(128),
  contract: Schema.Literal("model-plaintext-projection-v1"),
  sessionID: SessionID,
  inputID: SessionMessageID,
  kind: Schema.Literal("primary"),
  attempt: bounded(128),
  contextDigest: Digest,
  componentsDigest: Digest,
  complete: Schema.Literal(false),
  issuedAt: Schema.Int,
}).annotate({ identifier: "ExecutionProducer.ModelComposition" })

// Measured dependencies of a closed request. The host still resolves their
// source manifests and current disclosure eligibility; these hashes grant none.
export const PublicationReference = Schema.Struct({
  contextReference: Digest,
  inputID: SessionMessageID,
  receiptReference: Digest,
  unitID: bounded(128),
  unitDigest: Digest,
}).annotate({ identifier: "ExecutionProducer.PublicationReference" })
export interface PublicationReference extends Schema.Schema.Type<typeof PublicationReference> {}

const ProjectionPosition = {
  version: Schema.Literal(1),
  index: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  projectionRevision: Digest,
}
export const RequestOnlyUserProjection = Schema.Struct({
  ...ProjectionPosition,
  contract: Schema.Literal("session-user-context-v1"),
  preparedInput: SignedFact,
  metadataRevision: Digest,
  instructionsRevision: Digest,
  controlRevision: Digest,
}).annotate({ identifier: "ExecutionProducer.RequestOnlyUserProjection" })
export interface RequestOnlyUserProjection extends Schema.Schema.Type<typeof RequestOnlyUserProjection> {}

export const RequestOnlyAssistantProjection = Schema.Struct({
  ...ProjectionPosition,
  contract: Schema.Literal("session-assistant-publication-v1"),
  messageRevision: Digest,
  references: Schema.Array(PublicationReference).check(Schema.isMinLength(1), Schema.isMaxLength(128)),
}).annotate({ identifier: "ExecutionProducer.RequestOnlyAssistantProjection" })
export interface RequestOnlyAssistantProjection extends Schema.Schema.Type<typeof RequestOnlyAssistantProjection> {}

export const RequestOnlyComposition = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-model-composition"),
  producerEpoch: bounded(128),
  contract: Schema.Literal("model-request-only-plaintext-v1"),
  sessionID: SessionID,
  inputID: SessionMessageID,
  kind: Schema.Literals(["primary", "input"]),
  attempt: bounded(128),
  contextDigest: Digest,
  componentsDigest: Digest,
  complete: Schema.Literal(true),
  messageCount: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(2048)),
  instructionInputID: SessionMessageID,
  issuedAt: Schema.Int,
}).annotate({ identifier: "ExecutionProducer.RequestOnlyComposition" })
export interface RequestOnlyComposition extends Schema.Schema.Type<typeof RequestOnlyComposition> {}

// Enrolled independently: the plaintext contract never widens implicitly.
export const InlineRequestOnlyComposition = Schema.Struct({
  ...RequestOnlyComposition.fields,
  contract: Schema.Literal("model-request-only-inline-v1"),
}).annotate({ identifier: "ExecutionProducer.InlineRequestOnlyComposition" })
export interface InlineRequestOnlyComposition extends Schema.Schema.Type<typeof InlineRequestOnlyComposition> {}

// Known separately from the contracts returned by production enrollment.
export interface McpInvocation extends Schema.Schema.Type<typeof McpInvocation> {}
export const McpInvocation = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-mcp-invocation"),
  contract: Schema.Literal("mcp-invocation-v1"),
  producerEpoch: bounded(128),
  sessionID: SessionID,
  inputID: SessionMessageID,
  attempt: bounded(128),
  invocationID: bounded(128),
  messageID: SessionMessageID,
  toolCallID: bounded(256),
  server: bounded(256),
  tool: bounded(256),
  argumentsRevision: Digest,
  contextReference: Digest,
  contextProofRevision: Digest,
  issuedAt: Schema.Int,
}).annotate({ identifier: "ExecutionProducer.McpInvocation" })

// Larger evidence is separately negotiated; legacy SignedFact remains 4096.
export interface SignedToolFact extends Schema.Schema.Type<typeof SignedToolFact> {}
export const SignedToolFact = Schema.Struct({
  keyID: SignedFact.fields.keyID,
  payload: bounded(16384),
  signature: SignedFact.fields.signature,
}).annotate({ identifier: "ExecutionProducer.SignedToolFact" })

export interface ToolResultProjection extends Schema.Schema.Type<typeof ToolResultProjection> {}
export const ToolResultProjection = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-tool-result-projection"),
  contract: Schema.Literal("mcp-result-projection-v1"),
  producerEpoch: bounded(128),
  sessionID: SessionID,
  inputID: SessionMessageID,
  attempt: bounded(128),
  messageID: SessionMessageID,
  toolCallID: bounded(256),
  projectionRevision: Digest,
  parentReceipts: Schema.Array(Digest).check(Schema.isMaxLength(128)),
  complete: Schema.Boolean,
  issuedAt: Schema.Int,
}).annotate({ identifier: "ExecutionProducer.ToolResultProjection" })

export interface ToolDefinitions extends Schema.Schema.Type<typeof ToolDefinitions> {}
export const ToolDefinitions = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-tool-definitions"),
  contract: Schema.Literal("tool-definitions-v1"),
  producerEpoch: bounded(128),
  sessionID: SessionID,
  inputID: SessionMessageID,
  attempt: bounded(128),
  kind: Schema.Literals(["tools", "instructions"]),
  index: HumanProjection.fields.index,
  projectionRevision: Digest,
  parentReceipts: Schema.Array(Digest).check(Schema.isMaxLength(128)),
  complete: Schema.Boolean,
  issuedAt: Schema.Int,
}).annotate({ identifier: "ExecutionProducer.ToolDefinitions" })

export interface ToolComposition extends Schema.Schema.Type<typeof ToolComposition> {}
export const ToolComposition = Schema.Struct({
  ...RequestOnlyComposition.fields,
  contract: Schema.Literal("model-tool-context-v1"),
  toolCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(2048)),
  complete: Schema.Boolean,
}).annotate({ identifier: "ExecutionProducer.ToolComposition" })

export interface ToolAssistantProjection extends Schema.Schema.Type<typeof ToolAssistantProjection> {}
export const ToolAssistantProjection = Schema.Struct({
  ...RequestOnlyAssistantProjection.fields,
  contract: Schema.Literal("session-assistant-tool-context-v1"),
}).annotate({ identifier: "ExecutionProducer.ToolAssistantProjection" })
