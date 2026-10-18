export * as ExecutionProducer from "./execution-producer.js"

import { Schema } from "effect"
import { VerificationKey } from "./execution-verification-key.js"
import { SessionID } from "./session-id.js"
import { SessionMessageID } from "./session-message-id.js"
import { optional } from "./schema.js"

const bounded = (maximum: number) => Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(maximum))
const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))
export const SignatureDomain = "opencode.execution-producer.v1\0"
export const Contract = Schema.Literals(["human-plaintext-identity-v1", "model-plaintext-projection-v1"])

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
  contracts: Schema.Array(Contract).check(Schema.isMinLength(1), Schema.isMaxLength(2)).pipe(optional),
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
  contracts: Schema.Array(Contract).check(Schema.isMinLength(1), Schema.isMaxLength(2)),
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
