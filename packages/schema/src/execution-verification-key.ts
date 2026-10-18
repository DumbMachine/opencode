import { Schema } from "effect"

// Shared leaf prevents the producer/disclosure schemas from importing each
// other. Public namespaces re-export this exact value and stable identifier.
export interface VerificationKey extends Schema.Schema.Type<typeof VerificationKey> {}
export const VerificationKey = Schema.Struct({
  id: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
  algorithm: Schema.Literal("Ed25519"),
  publicKey: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{43}$/)),
}).annotate({ identifier: "ExecutionDisclosure.VerificationKey" })
