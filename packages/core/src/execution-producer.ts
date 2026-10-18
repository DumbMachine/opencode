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
import { type LLMRequest, SystemPart } from "@opencode/ai"
import { SessionPublicationProvenance } from "./session/publication-provenance.js"
import { toLLMMessages } from "./session/runner/to-llm-message.js"
import { Hash } from "@opencode/util/hash"
import { SessionDisclosure } from "./session/disclosure.js"
import { SessionInlineIdentity } from "./session/inline-identity.js"

// Only the nonsecret durable execution selection and required contract markers.
// Instructions and metadata have independent source bindings.
const inputControl = (payload: SessionInbox.UserPayload) => ({
  execution: payload.execution ? { model: payload.execution.model, agent: payload.execution.agent } : null,
  capabilities: payload.capabilities ?? [],
})

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
              ...(input.contracts?.includes("model-plaintext-projection-v1") &&
              input.contracts.includes("model-request-only-plaintext-v1")
                ? ["model-request-only-plaintext-v1" as const]
                : []),
              ...(input.contracts?.includes("model-plaintext-projection-v1") &&
              input.contracts.includes("model-request-only-plaintext-v1") &&
              input.contracts.includes("human-inline-identity-v1")
                ? ["human-inline-identity-v1" as const]
                : []),
              ...(input.contracts?.includes("model-plaintext-projection-v1") &&
              input.contracts.includes("model-request-only-plaintext-v1") &&
              input.contracts.includes("human-inline-identity-v1") &&
              input.contracts.includes("model-request-only-inline-v1")
                ? ["model-request-only-inline-v1" as const]
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
    requestOnly: (input: ExecutionDisclosure.ContextRequest, messageCount: number, inlineIdentity = false) =>
      signed(
        JSON.stringify({
          version: 1,
          purpose: "native-model-composition",
          producerEpoch: epoch,
          contract: inlineIdentity ? "model-request-only-inline-v1" : "model-request-only-plaintext-v1",
          sessionID: input.sessionID,
          inputID: input.inputID,
          kind: input.kind === "input" ? "input" : "primary",
          attempt: input.attempt,
          contextDigest: input.contextDigest,
          componentsDigest: input.componentsDigest,
          complete: true,
          messageCount,
          instructionInputID: input.inputID,
          issuedAt: Date.now(),
        } satisfies ExecutionProducer.InlineRequestOnlyComposition | ExecutionProducer.RequestOnlyComposition),
      ),
    preparedInline: (
      input: Omit<ExecutionProducer.PreparedInlineInput, "version" | "purpose" | "producerEpoch" | "contract">,
    ) =>
      signed(
        JSON.stringify(
          ExecutionProducer.PreparedInlineInput.make({
            ...input,
            version: 1,
            purpose: "native-input-preparation",
            producerEpoch: epoch,
            contract: "human-inline-identity-v1",
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
  readonly inlineIdentity?: boolean
  readonly payload: SessionInbox.UserPayload
}) =>
  Effect.sync(() => {
    if (
      input.invoked !== 0 ||
      (!input.inlineIdentity && input.original.files?.length) ||
      input.original.agents?.length ||
      input.original.skills?.length ||
      (!input.inlineIdentity && input.payload.files?.length) ||
      input.payload.agents?.length ||
      input.payload.skills?.length ||
      input.original.text !== input.payload.text
    )
      return undefined
    const parentRevision = input.inlineIdentity
      ? SessionInlineIdentity.originalRevision(input.original)
      : textRevision(input.original.text)
    const preparedRevision = input.inlineIdentity
      ? SessionInlineIdentity.revision(input.payload)
      : textRevision(input.payload.text)
    if (parentRevision === undefined || parentRevision !== preparedRevision) return undefined
    // Own the exact schema/JSON representation before signing. The fact itself
    // is absent from this projection, avoiding a self-referential payload hash.
    const content = { ...input.payload, preparedInput: undefined }
    const payload = Schema.decodeUnknownSync(SessionInbox.UserPayload)(
      JSON.parse(JSON.stringify(Schema.encodeUnknownSync(SessionInbox.UserPayload)(content))),
    )
    return {
      ...payload,
      preparedInput: input.inlineIdentity
        ? processSigner().preparedInline({
            sessionID: input.sessionID,
            inputID: input.inputID,
            parentContentRevision: parentRevision,
            preparedContentRevision: parentRevision,
            preparedPayloadRevision: SessionDisclosure.digest(payload),
          })
        : processSigner().prepared({
            sessionID: input.sessionID,
            inputID: input.inputID,
            parentTextRevision: parentRevision,
            preparedTextRevision: parentRevision,
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
  readonly requestOnly?: {
    readonly inlineIdentity?: boolean
    readonly provenance: readonly {
      readonly id: typeof SessionMessageID.Type
      readonly state: SessionPublicationProvenance.State | null
    }[]
  }
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
    readonly metadataRevision: string
    readonly instructionsRevision: string
    readonly controlRevision: string
    readonly instructions: string
  }> = []
  const assistants: Array<{
    readonly id: typeof SessionMessageID.Type
    readonly revision: string
    readonly index: number
    readonly projectionRevision: string
    readonly provenance: SessionPublicationProvenance.State
  }> = []
  const requestOnly = input.requestOnly === undefined ? undefined : structuredClone(input.requestOnly)
  try {
    let index = 0
    for (const original of input.messages) {
      const revision = Hash.sha256(JSON.stringify(original))
      const message = Schema.decodeUnknownSync(SessionMessage.Info)(
        JSON.parse(JSON.stringify(Schema.encodeUnknownSync(SessionMessage.Info)(original))),
      )
      const projected = toLLMMessages([message], input.model, input.providerMetadataKey, requestOnly?.inlineIdentity)
      const position = index
      index += projected.length
      if (
        requestOnly &&
        message.type === "assistant" &&
        projected.length === 1 &&
        Hash.sha256(JSON.stringify(message)) === revision
      ) {
        const association = requestOnly.provenance.find((entry) => entry.id === message.id)?.state
        if (association) {
          const provenance = Schema.decodeUnknownSync(SessionPublicationProvenance.State)(association)
          if (provenance.revision === SessionPublicationProvenance.revision(message))
            assistants.push({
              id: message.id,
              revision,
              index: position,
              projectionRevision: SessionDisclosure.digest(projected[0]),
              provenance,
            })
        }
        continue
      }
      if (
        message.type !== "user" ||
        !message.preparedInput ||
        projected.length !== 1 ||
        (!requestOnly?.inlineIdentity && message.files?.length) ||
        message.agents?.length ||
        message.skills?.length ||
        Hash.sha256(JSON.stringify(message)) !== revision
      )
        continue
      const { preparedInput: _, ...payload } = Schema.decodeUnknownSync(SessionInbox.UserPayload)(message)
      if (!preparedIdentity(sessionID, message.id, message.preparedInput, payload, requestOnly?.inlineIdentity))
        continue
      const instructions = message.execution?.instructions ?? ""
      if (Buffer.from(instructions, "utf8").toString("utf8") !== instructions) continue
      candidates.push({
        metadataRevision: SessionDisclosure.digest(message.metadata ?? {}),
        instructionsRevision: Hash.sha256("native-input-instructions-v1\0" + instructions),
        controlRevision: SessionDisclosure.digest(inputControl(payload)),
        instructions,
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
        if (requestOnly) {
          const current = candidates.find((candidate) => candidate.id === inputID)
          if (
            !current ||
            request.messages.length === 0 ||
            request.messages.length > 2048 ||
            request.tools.length !== 0 ||
            request.toolChoice !== undefined ||
            Object.keys(request.providerOptions ?? {}).length !== 0 ||
            Object.values(request.generation ?? {}).some(
              (value) =>
                value !== undefined &&
                typeof value !== "boolean" &&
                (typeof value !== "number" || !Number.isFinite(value)),
            ) ||
            SessionDisclosure.digest(request.system) !==
              SessionDisclosure.digest(current.instructions === "" ? [] : [SystemPart.make(current.instructions)])
          )
            return undefined
          const components: ExecutionDisclosure.Component[] = []
          const seen = new Set<string>()
          for (const [index, message] of request.messages.entries()) {
            if (message.id === undefined || seen.has(message.id)) return undefined
            seen.add(message.id)
            const human = candidates.find((candidate) => candidate.id === message.id && candidate.index === index)
            const assistant = assistants.find((candidate) => candidate.id === message.id && candidate.index === index)
            const measured = human ?? assistant
            if (!measured || SessionDisclosure.digest(message) !== measured.projectionRevision) return undefined
            const proof = human
              ? ExecutionProducer.RequestOnlyUserProjection.make({
                  version: 1,
                  contract: "session-user-context-v1",
                  preparedInput: human.preparedInput,
                  index,
                  projectionRevision: human.projectionRevision,
                  metadataRevision: human.metadataRevision,
                  instructionsRevision: human.instructionsRevision,
                  controlRevision: human.controlRevision,
                })
              : ExecutionProducer.RequestOnlyAssistantProjection.make({
                  version: 1,
                  contract: "session-assistant-publication-v1",
                  index,
                  projectionRevision: assistant!.projectionRevision,
                  messageRevision: assistant!.provenance.revision,
                  references: assistant!.provenance.references,
                })
            const encoded = JSON.stringify(proof)
            if (encoded.length > 4096) return undefined
            components.push({ kind: "message", id: measured.id, revision: measured.revision, proof: encoded })
          }
          const owned = SessionDisclosure.makeRequest({
            sessionID,
            inputID,
            kind: "primary",
            contextDigest: SessionDisclosure.digest(SessionDisclosure.logicalContext(request)),
            components,
          })
          const observed = {
            ...owned,
            composition: processSigner().requestOnly(owned, request.messages.length, requestOnly.inlineIdentity),
          }
          return Buffer.byteLength(JSON.stringify(observed)) > 512 * 1024 ? undefined : observed
        }
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

// The real inbox publisher supplies the owned winning payload, then checks
// that the exact same payload is what its permitted native event consumes.
export const inputContext = (
  sessionID: typeof SessionID.Type,
  inputID: typeof SessionMessageID.Type,
  payload: SessionInbox.UserPayload,
  inlineIdentity = false,
) =>
  Effect.sync(() => {
    const owned = Schema.decodeUnknownSync(SessionInbox.UserPayload)(
      JSON.parse(JSON.stringify(Schema.encodeSync(SessionInbox.UserPayload)(payload))),
    )
    if (
      !owned.preparedInput ||
      (!inlineIdentity && owned.files?.length) ||
      owned.agents?.length ||
      owned.skills?.length
    )
      return undefined
    const { preparedInput: _, ...content } = owned
    const instructions = owned.execution?.instructions ?? ""
    if (
      !preparedIdentity(sessionID, inputID, owned.preparedInput, content, inlineIdentity) ||
      Buffer.from(instructions, "utf8").toString("utf8") !== instructions
    )
      return undefined
    const revision = SessionDisclosure.digest(owned)
    const projection = ExecutionProducer.RequestOnlyUserProjection.make({
      version: 1,
      contract: "session-user-context-v1",
      preparedInput: owned.preparedInput,
      index: 0,
      projectionRevision: revision,
      metadataRevision: SessionDisclosure.digest(owned.metadata ?? {}),
      instructionsRevision: Hash.sha256("native-input-instructions-v1\0" + instructions),
      controlRevision: SessionDisclosure.digest(inputControl(owned)),
    })
    const proof = JSON.stringify(projection)
    if (proof.length > 4096) return undefined
    const request = SessionDisclosure.makeRequest({
      sessionID,
      inputID,
      kind: "input",
      contextDigest: SessionDisclosure.digest({ kind: "native-human-input-v1", payload: owned }),
      components: [{ kind: "message", id: inputID, revision, proof }],
    })
    return { ...request, composition: processSigner().requestOnly(request, 1, inlineIdentity) }
  }).pipe(
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterrupts(cause),
      () => Effect.die(new Error("Native input context provenance is unavailable")),
    ),
  )

function preparedIdentity(
  sessionID: typeof SessionID.Type,
  inputID: typeof SessionMessageID.Type,
  signed: ExecutionProducer.SignedFact,
  payload: SessionInbox.UserPayload,
  inlineIdentity = false,
) {
  const decoded = JSON.parse(signed.payload)
  if (inlineIdentity && decoded.contract === "human-inline-identity-v1") {
    const fact = Schema.decodeUnknownSync(ExecutionProducer.PreparedInlineInput)(decoded)
    const content = SessionInlineIdentity.revision(payload)
    return (
      content !== undefined &&
      fact.sessionID === sessionID &&
      fact.inputID === inputID &&
      fact.parentContentRevision === content &&
      fact.preparedContentRevision === content &&
      fact.preparedPayloadRevision === SessionDisclosure.digest(payload)
    )
  }
  const fact = Schema.decodeUnknownSync(ExecutionProducer.PreparedInput)(decoded)
  const text = textRevision(payload.text)
  return (
    text !== undefined &&
    fact.sessionID === sessionID &&
    fact.inputID === inputID &&
    fact.parentTextRevision === text &&
    fact.preparedTextRevision === text &&
    fact.preparedPayloadRevision === SessionDisclosure.digest(payload)
  )
}
