export * as ExecutionProducer from "./execution-producer.js"

import { Schema } from "effect"
import { ExecutionDisclosure } from "./execution-disclosure.js"
import { SessionID } from "./session-id.js"
import { SessionMessageID } from "./session-message-id.js"
import { optional } from "./schema.js"

const bounded = (maximum: number) => Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(maximum))
const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))
export const SignatureDomain = "opencode.execution-producer.v1\0"
export const Contract = Schema.Literal("human-plaintext-identity-v1")

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
}).annotate({ identifier: "ExecutionProducer.EnrollmentRequest" })

export interface EnrollmentProof extends Schema.Schema.Type<typeof EnrollmentProof> {}
export const EnrollmentProof = Schema.Struct({
  ...EnrollmentRequest.fields,
  purpose: Schema.Literal("native-producer-enrollment"),
  producerEpoch: bounded(128),
  bootNonce: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{43}$/)).pipe(optional),
  key: ExecutionDisclosure.VerificationKey,
  contracts: Schema.Array(Contract).check(Schema.isMinLength(1), Schema.isMaxLength(1)),
  issuedAt: Schema.Int,
}).annotate({ identifier: "ExecutionProducer.EnrollmentProof" })

export interface Enrollment extends Schema.Schema.Type<typeof Enrollment> {}
export const Enrollment = Schema.Struct({
  key: ExecutionDisclosure.VerificationKey,
  signed: SignedFact,
}).annotate({ identifier: "ExecutionProducer.Enrollment" })

// Preparation is an observation, not an independent admission receipt or a
// completeness assertion. Only the winning native payload retains this fact.
export interface PreparedInput extends Schema.Schema.Type<typeof PreparedInput> {}
export const PreparedInput = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("native-input-preparation"),
  producerEpoch: bounded(128),
  contract: Contract,
  sessionID: SessionID,
  inputID: SessionMessageID,
  parentTextRevision: Digest,
  preparedTextRevision: Digest,
  preparedPayloadRevision: Digest,
}).annotate({ identifier: "ExecutionProducer.PreparedInput" })
