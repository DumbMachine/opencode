export * as NativeProducer from "./execution-producer.js"

import { generateKeyPairSync, randomUUID, sign } from "node:crypto"
import { Cause, Effect, Schema } from "effect"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { SessionID } from "@opencode/schema/session-id"
import { SessionMessageID } from "@opencode/schema/session-message-id"
import { SessionMessage } from "@opencode/schema/session-message"
import type { Model } from "@opencode/schema/model"
import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import type { LLMRequest } from "@opencode/ai"
import { toLLMMessages } from "./session/runner/to-llm-message.js"
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
            contracts: [
              "human-plaintext-identity-v1",
              ...(input.contracts?.includes("model-plaintext-projection-v1")
                ? ["model-plaintext-projection-v1" as const]
                : []),
            ],
            issuedAt: Date.now(),
          }),
        ),
      ),
    }),
    composed: (input: ExecutionDisclosure.ContextRequest) =>
      signed(
        JSON.stringify(
          ExecutionProducer.ModelComposition.make({
            version: 1,
            purpose: "native-model-composition",
            producerEpoch: epoch,
            contract: "model-plaintext-projection-v1",
            sessionID: input.sessionID,
            inputID: input.inputID,
            kind: "primary",
            attempt: input.attempt,
            contextDigest: input.contextDigest,
            componentsDigest: input.componentsDigest,
            complete: false,
            issuedAt: Date.now(),
          }),
        ),
      ),
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

// Capture canonical winning history before preparation yields. This observes
// supported projections only; the host must independently verify every nested
// preparation signature and original admission before accepting a leaf.
export const captureContext = (input: {
  readonly sessionID: typeof SessionID.Type
  readonly inputID: typeof SessionMessageID.Type
  readonly model: Model.Ref
  readonly providerMetadataKey: string
  readonly messages: readonly SessionMessage.Info[]
  readonly components: readonly ExecutionDisclosure.Component[]
}) => {
  const sessionID = input.sessionID,
    inputID = input.inputID
  const components = structuredClone(input.components)
  const candidates: Array<{
    readonly id: typeof SessionMessageID.Type
    readonly revision: string
    readonly index: number
    readonly projectionRevision: string
    readonly preparedInput: ExecutionProducer.SignedFact
  }> = []
  try {
    let index = 0
    for (const original of input.messages) {
      const revision = Hash.sha256(JSON.stringify(original))
      const message = Schema.decodeUnknownSync(SessionMessage.Info)(
        JSON.parse(JSON.stringify(Schema.encodeUnknownSync(SessionMessage.Info)(original))),
      )
      const projected = toLLMMessages([message], input.model, input.providerMetadataKey)
      const position = index
      index += projected.length
      if (
        message.type !== "user" ||
        !message.preparedInput ||
        projected.length !== 1 ||
        message.files?.length ||
        message.agents?.length ||
        message.skills?.length ||
        Hash.sha256(JSON.stringify(message)) !== revision
      )
        continue
      const fact = Schema.decodeUnknownSync(ExecutionProducer.PreparedInput)(JSON.parse(message.preparedInput.payload))
      const { preparedInput: _, ...payload } = Schema.decodeUnknownSync(SessionInbox.UserPayload)(message)
      const text = textRevision(message.text)
      if (
        fact.sessionID !== sessionID ||
        fact.inputID !== message.id ||
        text === undefined ||
        fact.parentTextRevision !== text ||
        fact.preparedTextRevision !== text ||
        fact.preparedPayloadRevision !== SessionDisclosure.digest(payload)
      )
        continue
      candidates.push({
        id: message.id,
        revision,
        index: position,
        projectionRevision: SessionDisclosure.digest(projected[0]),
        preparedInput: structuredClone(message.preparedInput),
      })
    }
  } catch {
    // Unrecognized history cannot acquire a partial composition observation.
    return undefined
  }
  return {
    observe: (request: LLMRequest, invoked: number, extra: readonly ExecutionDisclosure.Component[]) =>
      Effect.sync(() => {
        if (invoked !== 0 || Object.keys(request.metadata ?? {}).length !== 0) return undefined
        // Caller-supplied evidence is never endorsed by this signer. Only the
        // exact observations measured below may enter the signed digest.
        const selected = structuredClone([...components, ...extra]).map(({ proof: _, ...component }) => component)
        const proofs = new Map<ExecutionDisclosure.Component, string>()
        for (const candidate of candidates) {
          const matches = selected.filter((component) => component.kind === "message" && component.id === candidate.id)
          if (
            matches.length !== 1 ||
            matches[0].revision !== candidate.revision ||
            request.messages.filter((message) => message.id === candidate.id).length !== 1 ||
            !request.messages[candidate.index] ||
            SessionDisclosure.digest(request.messages[candidate.index]) !== candidate.projectionRevision
          )
            continue
          proofs.set(
            matches[0],
            JSON.stringify(
              ExecutionProducer.HumanProjection.make({
                version: 1,
                contract: "session-user-plaintext-v1",
                complete: false,
                preparedInput: candidate.preparedInput,
                index: candidate.index,
                projectionRevision: candidate.projectionRevision,
              }),
            ),
          )
        }
        const owned = SessionDisclosure.makeRequest({
          sessionID,
          inputID,
          kind: "primary",
          contextDigest: SessionDisclosure.digest(SessionDisclosure.logicalContext(request)),
          components: selected.map((component) => {
            const proof = proofs.get(component)
            return proof === undefined ? component : { ...component, proof }
          }),
        })
        const observed = {
          ...owned,
          composition: processSigner().composed(owned),
        } satisfies ExecutionDisclosure.ContextRequest
        // Optional evidence must not turn a supported unknown-private request
        // into an oversized callback. The caller can use the unobserved path.
        return Buffer.byteLength(JSON.stringify(observed)) > 512 * 1024 ? undefined : observed
      }).pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterrupts(cause),
          () => Effect.die(new Error("Native context provenance is unavailable")),
        ),
      ),
  }
}
