export * as NativeProducer from "./execution-producer.js"

import { generateKeyPairSync, randomUUID, sign } from "node:crypto"
import { Cause, Effect, Schema } from "effect"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { SessionID } from "@opencode/schema/session-id"
import { SessionMessageID } from "@opencode/schema/session-message-id"
import { Hash } from "@opencode/util/hash"
import { SessionDisclosure } from "./session/disclosure.js"

// Lazy process-local key material. No key is written or exposed to input,
// plugin, tool or enrollment configuration. These narrow operations cannot sign
// caller-selected payloads or purposes.
const candidate = process.env.OPENCODE_PRODUCER_BOOT_NONCE
const bootNonce = candidate !== undefined && /^[A-Za-z0-9_-]{43}$/.test(candidate) ? candidate : undefined
const create = () => {
  const pair = generateKeyPairSync("ed25519")
  const publicKey = pair.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("base64url")
  const key = Object.freeze({ id: Hash.sha256(publicKey), algorithm: "Ed25519" as const, publicKey })
  const epoch = randomUUID()
  const signed = (payload: string): ExecutionProducer.SignedFact =>
    Object.freeze({
      keyID: key.id,
      payload,
      signature: sign(null, Buffer.from(ExecutionProducer.SignatureDomain + payload, "utf8"), pair.privateKey).toString(
        "base64url",
      ),
    })
  return {
    enroll: (input: ExecutionProducer.EnrollmentRequest): ExecutionProducer.Enrollment => ({
      key: structuredClone(key),
      signed: signed(
        JSON.stringify(
          ExecutionProducer.EnrollmentProof.make({
            ...input,
            purpose: "native-producer-enrollment",
            producerEpoch: epoch,
            bootNonce,
            key,
            contracts: ["human-plaintext-identity-v1"],
            issuedAt: Date.now(),
          }),
        ),
      ),
    }),
    prepared: (input: Omit<ExecutionProducer.PreparedInput, "version" | "purpose" | "producerEpoch" | "contract">) =>
      signed(
        JSON.stringify(
          ExecutionProducer.PreparedInput.make({
            ...input,
            version: 1,
            purpose: "native-input-preparation",
            producerEpoch: epoch,
            contract: "human-plaintext-identity-v1",
          }),
        ),
      ),
  }
}
let signer: ReturnType<typeof create> | undefined
const processSigner = () => (signer ??= create())

export const enroll = (input: ExecutionProducer.EnrollmentRequest) =>
  Effect.try({
    try: () =>
      processSigner().enroll(Schema.decodeUnknownSync(ExecutionProducer.EnrollmentRequest)(structuredClone(input))),
    catch: () => new Error("Native producer enrollment is unavailable"),
  }).pipe(Effect.orDie)

// This hash matches the host's exact plaintext wire projection. Invalid Unicode
// cannot acquire a cross-language identity through UTF-8 replacement.
const textRevision = (text: string) =>
  text !== "" && Buffer.from(text, "utf8").toString("utf8") === text
    ? Hash.sha256("native-human-text-v1\0" + text)
    : undefined

export const prepareInput = (input: {
  readonly sessionID: typeof SessionID.Type
  readonly inputID: typeof SessionMessageID.Type
  readonly original: {
    readonly text: string
    readonly files?: readonly unknown[]
    readonly agents?: readonly unknown[]
    readonly skills?: readonly unknown[]
  }
  readonly invoked: number
  readonly payload: SessionInbox.UserPayload
}) =>
  Effect.sync(() => {
    if (
      input.invoked !== 0 ||
      input.original.files?.length ||
      input.original.agents?.length ||
      input.original.skills?.length ||
      input.payload.files?.length ||
      input.payload.agents?.length ||
      input.payload.skills?.length ||
      input.original.text !== input.payload.text
    )
      return undefined
    const parentTextRevision = textRevision(input.original.text)
    if (parentTextRevision === undefined) return undefined
    // Own the exact schema/JSON representation before signing. The fact itself
    // is absent from this projection, avoiding a self-referential payload hash.
    const content = { ...input.payload, preparedInput: undefined }
    const payload = Schema.decodeUnknownSync(SessionInbox.UserPayload)(
      JSON.parse(JSON.stringify(Schema.encodeUnknownSync(SessionInbox.UserPayload)(content))),
    )
    return {
      ...payload,
      preparedInput: processSigner().prepared({
        sessionID: input.sessionID,
        inputID: input.inputID,
        parentTextRevision,
        preparedTextRevision: parentTextRevision,
        preparedPayloadRevision: SessionDisclosure.digest(payload),
      }),
    } satisfies SessionInbox.UserPayload
  }).pipe(
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterrupts(cause),
      () => Effect.die(new Error("Native input preparation provenance is unavailable")),
    ),
  )
