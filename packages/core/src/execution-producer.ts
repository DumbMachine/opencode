export * as NativeProducer from "./execution-producer.js"

import type { Transport } from "@modelcontextprotocol/client"
import { generateKeyPairSync, randomUUID, sign } from "node:crypto"
import { Cause, Context, Effect, Option, Schema } from "effect"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { SessionID } from "@opencode/schema/session-id"
import { SessionMessageID } from "@opencode/schema/session-message-id"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionMessage } from "@opencode/schema/session-message"
import type { Tool } from "./tool.js"
import { ToolProvenance } from "./tool/provenance.js"
import type { Model } from "@opencode/schema/model"
import { CodeModeInstructions } from "./codemode/instructions.js"
import { CodeModeCatalog } from "./codemode/catalog.js"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { type LLMRequest, SystemPart, ToolFailure } from "@opencode/ai"
import { SessionPublicationProvenance } from "./session/publication-provenance.js"
import { toLLMMessages } from "./session/runner/to-llm-message.js"
import { Hash } from "@opencode/util/hash"
import { SessionDisclosure } from "./session/disclosure.js"
import { SessionInlineIdentity } from "./session/inline-identity.js"
import { McpProvenance } from "./mcp/provenance.js"
import { interruptedToolError } from "./session/tool-interruption.js"

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
              ...(input.contracts?.includes("human-plaintext-identity-v1") &&
              input.contracts?.includes("model-plaintext-projection-v1") &&
              input.contracts?.includes("model-request-only-plaintext-v1") &&
              input.contracts?.includes("human-inline-identity-v1") &&
              input.contracts?.includes("model-request-only-inline-v1") &&
              input.contracts?.includes("mcp-invocation-v1")
                ? ["mcp-invocation-v1" as const]
                : []),
              ...(input.contracts?.includes("human-plaintext-identity-v1") &&
              input.contracts?.includes("model-plaintext-projection-v1") &&
              input.contracts?.includes("model-request-only-plaintext-v1") &&
              input.contracts?.includes("human-inline-identity-v1") &&
              input.contracts?.includes("model-request-only-inline-v1") &&
              input.contracts?.includes("mcp-invocation-v1") &&
              input.contracts?.includes("mcp-result-projection-v1")
                ? ["mcp-result-projection-v1" as const]
                : []),
              ...(input.contracts?.includes("human-plaintext-identity-v1") &&
              input.contracts?.includes("model-plaintext-projection-v1") &&
              input.contracts?.includes("model-request-only-plaintext-v1") &&
              input.contracts?.includes("human-inline-identity-v1") &&
              input.contracts?.includes("model-request-only-inline-v1") &&
              input.contracts?.includes("mcp-invocation-v1") &&
              input.contracts?.includes("mcp-result-projection-v1") &&
              input.contracts?.includes("tool-definitions-v1")
                ? ["tool-definitions-v1" as const]
                : []),
              ...(input.contracts?.includes("human-plaintext-identity-v1") &&
              input.contracts?.includes("model-plaintext-projection-v1") &&
              input.contracts?.includes("model-request-only-plaintext-v1") &&
              input.contracts?.includes("human-inline-identity-v1") &&
              input.contracts?.includes("model-request-only-inline-v1") &&
              input.contracts?.includes("mcp-invocation-v1") &&
              input.contracts?.includes("mcp-result-projection-v1") &&
              input.contracts?.includes("tool-definitions-v1") &&
              input.contracts?.includes("model-tool-context-v1")
                ? ["model-tool-context-v1" as const]
                : []),
            ],
            issuedAt: Date.now(),
          }),
        ),
      ),
    }),
    mcpInvocation: (
      input: Omit<ExecutionProducer.McpInvocation, "version" | "purpose" | "producerEpoch" | "contract">,
    ) => {
      const invocation = Schema.decodeUnknownSync(ExecutionProducer.McpInvocation)({
        ...input,
        version: 1,
        purpose: "native-mcp-invocation",
        contract: "mcp-invocation-v1",
        producerEpoch: epoch,
      })
      return { invocation, signed: signed(JSON.stringify(invocation)) }
    },
    toolProjection: (
      input: Omit<ExecutionProducer.ToolResultProjection, "version" | "purpose" | "producerEpoch" | "contract">,
    ) =>
      signed(
        JSON.stringify(
          ExecutionProducer.ToolResultProjection.make({
            ...input,
            version: 1,
            purpose: "native-tool-result-projection",
            producerEpoch: epoch,
            contract: "mcp-result-projection-v1",
          }),
        ),
      ),
    toolDefinitions: (
      input: Omit<ExecutionProducer.ToolDefinitions, "version" | "purpose" | "producerEpoch" | "contract">,
    ) =>
      signed(
        JSON.stringify(
          ExecutionProducer.ToolDefinitions.make({
            ...input,
            version: 1,
            purpose: "native-tool-definitions",
            producerEpoch: epoch,
            contract: "tool-definitions-v1",
          }),
        ),
      ),
    toolComposition: (
      input: ExecutionDisclosure.ContextRequest,
      messageCount: number,
      toolCount: number,
      complete: boolean,
    ) =>
      signed(
        JSON.stringify(
          ExecutionProducer.ToolComposition.make({
            version: 1,
            purpose: "native-model-composition",
            producerEpoch: epoch,
            contract: "model-tool-context-v1",
            sessionID: input.sessionID,
            inputID: input.inputID,
            kind: input.kind === "input" ? "input" : "primary",
            attempt: input.attempt,
            contextDigest: input.contextDigest,
            componentsDigest: input.componentsDigest,
            messageCount,
            toolCount,
            instructionInputID: input.inputID,
            complete,
            issuedAt: Date.now(),
          }),
        ),
      ),
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
          if (provenance.version === 1 && provenance.revision === SessionPublicationProvenance.revision(message))
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
  toolContext = false,
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
    return {
      ...request,
      composition: toolContext
        ? processSigner().toolComposition(request, 1, 0, true)
        : processSigner().requestOnly(request, 1, inlineIdentity),
    }
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

// This service never leaves the capture closure. Tool arguments, hooks and MCP
// metadata cannot install it or obtain a general-purpose signing handle.
class ToolCapture extends Context.Service<
  ToolCapture,
  {
    readonly capture: McpProvenance.Capture
    readonly messageID: typeof SessionMessageID.Type
    readonly toolCallID: string
    readonly valid: () => boolean
    readonly reserve: () => void
    readonly observe: (observation: McpProvenance.Observation) => void
    readonly unknown: () => void
    readonly project: (unitDigest: string) => ExecutionProducer.SignedToolFact
  }
>()("@opencode/NativeProducer/ToolCapture") {}

// Dormant until a separately negotiated tool-capable model attempt supplies a
// verified host proof. Existing policy/composition selection remains unchanged.
export const captureTools = (input: {
  readonly service: ExecutionDisclosure.Service
  readonly signed: ExecutionDisclosure.SignedProof
  readonly current: () => boolean
}) =>
  Effect.sync(() => {
    const capture = McpProvenance.capture(input.service, input.signed)
    const current = input.current
    return {
      run: <A, E, R>(
        identity: {
          readonly messageID: typeof SessionMessageID.Type
          readonly toolCallID: string
        },
        effect: Effect.Effect<A, E, R>,
      ) =>
        Effect.suspend(() => {
          let active = true
          let calls = 0
          let bytes = 0
          let complete = true
          const observations: McpProvenance.Observation[] = []
          const valid = () => active && current()
          return effect.pipe(
            Effect.provideService(ToolCapture, {
              capture,
              ...identity,
              valid,
              reserve: () => {
                if (!valid() || ++calls > 128) throw new Error("MCP provenance authorization is unavailable")
              },
              unknown: () => {
                complete = false
              },
              project: (unitDigest) =>
                processSigner().toolProjection({
                  sessionID: capture.context.sessionID,
                  inputID: capture.context.inputID,
                  attempt: capture.context.attempt,
                  messageID: identity.messageID,
                  toolCallID: identity.toolCallID,
                  projectionRevision: unitDigest,
                  parentReceipts: Array.from(new Set(observations.map((item) => item.proof.receiptReference))),
                  complete,
                  issuedAt: Date.now(),
                }),
              observe: (observation) => {
                complete &&= observation.proof.complete
                bytes += Buffer.byteLength(observation.snapshot)
                if (!valid() || bytes > 32 * 1024 * 1024) throw new Error("MCP provenance authorization is unavailable")
                observations.push(Object.freeze(observation))
              },
            }),
            Effect.map((value) => {
              const evidence = Object.freeze({
                capture,
                ...identity,
                complete,
                observations: Object.freeze([...observations]),
              })
              if (value !== null && typeof value === "object")
                toolResults.set(value, { evidence, revision: toolRevision(value) })
              return { value, observations: evidence.observations, complete }
            }),
            Effect.tapError((error) =>
              Effect.sync(() => {
                if (error !== null && typeof error === "object")
                  toolResults.set(error, {
                    evidence: Object.freeze({
                      capture,
                      ...identity,
                      complete,
                      observations: Object.freeze([...observations]),
                    }),
                    revision: toolRevision(error),
                  })
              }),
            ),
            Effect.onExit(() =>
              Effect.sync(() => {
                active = false
              }),
            ),
          )
        }),
    }
  })

// Called only around the actual SDK dispatch, after mutable tool hooks and
// validation. Own and measure exactly the arguments subsequently serialized.
export const dispatchMcp = <A, E, R>(
  input: {
    readonly server: string
    readonly name: string
    readonly args?: Record<string, unknown>
    readonly meta?: Readonly<Record<string, unknown>>
    readonly sessionID?: string
  },
  transport: (request: {
    readonly name: string
    readonly args: Record<string, unknown>
    readonly meta: Readonly<Record<string, unknown>>
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const scope = Option.getOrUndefined(yield* Effect.serviceOption(ToolCapture))
    if (!scope) return yield* transport({ name: input.name, args: input.args ?? {}, meta: input.meta ?? {} })
    scope.reserve()
    if (
      input.sessionID !== scope.capture.context.sessionID ||
      Object.hasOwn(input.meta ?? {}, McpProvenance.RequestKey)
    )
      return yield* Effect.die(new Error("MCP provenance authorization is unavailable"))
    const args = McpProvenance.own(input.args ?? {}) as Record<string, unknown>
    const serialized = JSON.stringify(args)
    if (Buffer.byteLength(serialized) > McpProvenance.MaximumSnapshot)
      return yield* Effect.die(new Error("MCP provenance authorization is unavailable"))
    const { invocation, signed } = processSigner().mcpInvocation({
      sessionID: scope.capture.context.sessionID,
      inputID: scope.capture.context.inputID,
      attempt: scope.capture.context.attempt,
      invocationID: randomUUID(),
      messageID: scope.messageID,
      toolCallID: scope.toolCallID,
      server: input.server,
      tool: input.name,
      argumentsRevision: Hash.sha256(serialized),
      contextReference: scope.capture.context.contextReference,
      contextProofRevision: Hash.sha256(scope.capture.signed.payload),
      issuedAt: Date.now(),
    })
    const meta = McpProvenance.own({
      ...(input.meta ?? {}),
      [McpProvenance.RequestKey]: { version: 1, context: scope.capture.signed, invocation: signed },
    }) as Readonly<Record<string, unknown>>
    if (!scope.valid()) return yield* Effect.die(new Error("MCP provenance authorization is unavailable"))
    const raw = yield* transport({ name: input.name, args, meta }).pipe(
      Effect.tapError(() => Effect.sync(() => scope.unknown())),
    )
    if (
      scope.capture.context.mode === "actor-private-unknown" &&
      raw !== null &&
      typeof raw === "object" &&
      (!("_meta" in raw) ||
        raw._meta === undefined ||
        (typeof raw._meta === "object" && raw._meta !== null && !Object.hasOwn(raw._meta, McpProvenance.ResultKey)))
    ) {
      scope.unknown()
      return raw
    }
    const verified = McpProvenance.result(scope.capture, invocation, raw)
    scope.observe({ invocation: signed, proof: verified.proof, signed: verified.signed, snapshot: verified.snapshot })
    return verified.result
  })

const catalogExecutions = new WeakMap<object, CatalogEvidence>()
const catalogEvidence = new WeakSet<object>()
const catalogInstructionProjections = new WeakMap<object, Set<string>>()
export const recordCatalogInstructions = (execution: object, text: string) => {
  const evidence = catalogExecutions.get(execution)
  if (evidence) {
    const projections = catalogInstructionProjections.get(evidence) ?? new Set<string>()
    projections.add(SessionDisclosure.digest(SystemPart.make(text)))
    catalogInstructionProjections.set(evidence, projections)
  }
  return text
}
const catalogCovers = (snapshot: object, sessionID: string, inputID: string, evidence?: CatalogEvidence) => {
  const sources = ToolProvenance.sources(snapshot)
  if (!sources) return false
  if (sources.length === 0) return true
  if (
    !evidence ||
    !catalogEvidence.has(evidence) ||
    !evidence.complete ||
    evidence.sessionID !== sessionID ||
    evidence.inputID !== inputID
  )
    return false
  return sources.every(
    (source) =>
      catalogExecutions.get(source.execution) === evidence &&
      evidence.observations.some((observation) => {
        if (observation.server !== source.server || observation.proof.kind !== "tools/list") return false
        const raw = McpProvenance.parse(observation.snapshot)
        return (
          typeof raw === "object" &&
          raw !== null &&
          "tools" in raw &&
          Array.isArray(raw.tools) &&
          raw.tools.some(
            (tool: unknown) =>
              typeof tool === "object" &&
              tool !== null &&
              "name" in tool &&
              tool.name === source.tool &&
              SessionDisclosure.digest(tool) === source.revision,
          )
        )
      }),
  )
}
export interface CatalogEvidence {
  readonly observations: readonly McpProvenance.CatalogObservation[]
  readonly complete: boolean
  readonly sessionID: typeof SessionID.Type
  readonly inputID: typeof SessionMessageID.Type
}

class CatalogCapture extends Context.Service<
  CatalogCapture,
  {
    readonly service: ExecutionDisclosure.Service
    readonly sessionID: typeof SessionID.Type
    readonly inputID: typeof SessionMessageID.Type
    readonly revision: number
    readonly current: () => boolean
    readonly observe: (value: McpProvenance.CatalogObservation | undefined) => void
  }
>()("@opencode/NativeProducer/CatalogCapture") {}

export const captureCatalog = (input: {
  readonly service: ExecutionDisclosure.Service
  readonly sessionID: typeof SessionID.Type
  readonly inputID: typeof SessionMessageID.Type
  readonly revision: number
  readonly current: () => boolean
}) => {
  const service = Schema.decodeUnknownSync(ExecutionDisclosure.Service)(structuredClone(input.service))
  const sessionID = input.sessionID,
    inputID = input.inputID,
    revision = input.revision,
    current = input.current
  return {
    run: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      Effect.suspend(() => {
        let complete = true
        let active = true
        let bytes = 0
        const observations: McpProvenance.CatalogObservation[] = []
        return effect.pipe(
          Effect.provideService(CatalogCapture, {
            service,
            sessionID,
            inputID,
            revision,
            current: () => active && current(),
            observe: (value) => {
              if (!active || !current()) throw new Error("MCP provenance authorization is unavailable")
              if (!value) {
                complete = false
                return
              }
              bytes += Buffer.byteLength(value.snapshot)
              if (observations.length >= 128 || bytes > 32 * 1024 * 1024)
                throw new Error("MCP catalog provenance exceeds the bounded capture limit")
              complete &&= value.proof.complete
              observations.push(value)
            },
          }),
          Effect.map((value) => {
            const evidence = Object.freeze({
              complete,
              observations: Object.freeze([...observations]),
              sessionID,
              inputID,
            })
            catalogEvidence.add(evidence)
            if (value !== null && typeof value === "object") catalogExecutions.set(value, evidence)
            return { value, evidence }
          }),
          Effect.onExit(() =>
            Effect.sync(() => {
              active = false
            }),
          ),
        )
      }),
  }
}

// Observe the actual transport objects before SDK pagination, filtering and
// initialize-field conversion. The Proxy preserves SDK transport identity and
// binds underlying methods to their original receiver/private fields.
export const catalogTransport = (server: string, transport: Transport) =>
  Effect.gen(function* () {
    const scope = Option.getOrUndefined(yield* Effect.serviceOption(CatalogCapture))
    if (!scope) return transport
    const pending = new Map<string | number, { kind: "initialize" | "tools/list"; cursor: string }>()
    let receiver: Transport["onmessage"]
    const send: Transport["send"] = (message, options) => {
      if (
        "id" in message &&
        "method" in message &&
        (message.method === "initialize" || message.method === "server/discover" || message.method === "tools/list")
      ) {
        if (!scope.current()) return Promise.reject(new Error("MCP provenance authorization is unavailable"))
        if (pending.size >= 128)
          return Promise.reject(new Error("MCP catalog provenance exceeds the bounded capture limit"))
        const cursor = message.params?.cursor
        if (cursor !== undefined && typeof cursor !== "string")
          return Promise.reject(new Error("Invalid MCP catalog cursor"))
        pending.set(message.id, {
          kind: message.method === "tools/list" ? "tools/list" : "initialize",
          cursor: cursor ?? "",
        })
      }
      return transport.send(message, options)
    }
    transport.onmessage = (message, extra) => {
      const id = "id" in message ? message.id : undefined
      const operation = id === undefined ? undefined : pending.get(id)
      if (!operation || !("result" in message)) {
        receiver?.(message, extra)
        return
      }
      pending.delete(message.id)
      try {
        if (!scope.current()) throw new Error("MCP provenance authorization is unavailable")
        const result = message.result
        if (result._meta === undefined || !Object.hasOwn(result._meta, McpProvenance.CatalogKey)) {
          scope.observe(undefined)
          receiver?.(message, extra)
          return
        }
        const verified = McpProvenance.catalog({ ...scope, server, ...operation }, result)
        scope.observe(verified.observation)
        receiver?.({ ...message, result: verified.result }, extra)
      } catch {
        receiver?.(
          {
            jsonrpc: "2.0",
            id: message.id,
            error: { code: -32603, message: "MCP provenance authorization is unavailable" },
          },
          extra,
        )
      }
    }
    return new Proxy(transport, {
      get(target, property) {
        if (property === "onmessage") return receiver
        if (property === "send") return send
        // oxlint-disable-next-line no-restricted-globals -- genuine transparent transport reflection
        const value: unknown = Reflect.get(target, property, target)
        return typeof value === "function" ? value.bind(target) : value
      },
      set(target, property, value) {
        if (property === "onmessage") {
          receiver = value
          return true
        }
        // oxlint-disable-next-line no-restricted-globals -- genuine transparent transport reflection
        return Reflect.set(target, property, value, target)
      },
    })
  })

interface ToolEvidence {
  readonly capture: McpProvenance.Capture
  readonly messageID: typeof SessionMessageID.Type
  readonly toolCallID: string
  readonly complete: boolean
  readonly observations: readonly McpProvenance.Observation[]
}
const toolResults = new WeakMap<object, { readonly evidence: ToolEvidence; readonly revision: string }>()
const toolRevision = (value: object) =>
  SessionDisclosure.digest(value instanceof Error ? { ...value, name: value.name, message: value.message } : value)
// Preparation carries denial-only policy before a host model context exists.
// It cannot sign facts, supply actor credentials, or permit a tool operation.
class ManagedPreparation extends Context.Service<ManagedPreparation, true>()(
  "@opencode/NativeProducer/ManagedPreparation",
) {}
export const managedPreparation = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provideService(ManagedPreparation, true))
const bindingRequired = Effect.gen(function* () {
  return (
    Option.isSome(yield* Effect.serviceOption(ToolCapture)) ||
    Option.isSome(yield* Effect.serviceOption(ManagedPreparation))
  )
})
export const requireToolBinding = bindingRequired.pipe(
  Effect.flatMap((required) =>
    required
      ? Effect.fail(
          new ToolFailure({
            message: "Access needed: this tool has no execution binding for the current message actor",
          }),
        )
      : Effect.void,
  ),
)
export const assertAmbientMcpAccess = bindingRequired.pipe(
  Effect.flatMap((required) =>
    required
      ? Effect.die(new Error("Access needed: this MCP operation has no binding for the current message actor"))
      : Effect.void,
  ),
)
export const unknownToolSource = requireToolBinding

// Only the actual truncation producer propagates a measured parent's private
// evidence. Caller metadata and output paths never supply this association.
export const inheritToolResult = <A extends object>(before: object, after: A) => {
  const source = toolResults.get(before)
  if (source && source.revision === toolRevision(before))
    toolResults.set(after, { evidence: source.evidence, revision: toolRevision(after) })
  return after
}

export const projectToolUnit = (unitDigest: string, result: object): ExecutionProducer.SignedToolFact | undefined => {
  const source = toolResults.get(result)
  if (!source) return undefined
  const evidence = source.evidence
  return processSigner().toolProjection({
    sessionID: evidence.capture.context.sessionID,
    inputID: evidence.capture.context.inputID,
    attempt: evidence.capture.context.attempt,
    messageID: evidence.messageID,
    toolCallID: evidence.toolCallID,
    projectionRevision: unitDigest,
    parentReceipts: Array.from(new Set(evidence.observations.map((item) => item.proof.receiptReference))),
    complete: evidence.complete && source.revision === toolRevision(result),
    issuedAt: Date.now(),
  })
}

interface PendingTool {
  readonly messageID: typeof SessionMessageID.Type
  readonly toolCallID: string
  readonly executed: boolean
}
const pendingContexts = new WeakMap<object, { readonly revision: string; readonly targets: readonly PendingTool[] }>()
// This handle exists only for an actual observed request, before writer capture
// clones it. Caller-created requests or changed aliases have no source handle.
export const capturePendingTools = (request: ExecutionDisclosure.ContextRequest) => {
  const observed = pendingContexts.get(request)
  if (!observed || observed.revision !== SessionDisclosure.digest(request)) return undefined
  const sessionID = request.sessionID,
    inputID = request.inputID,
    attempt = request.attempt
  return Object.freeze({
    targets: observed.targets,
    project: (definition: object, data: Record<string, unknown>, unitDigest: string) => {
      if (definition !== SessionEvent.Tool.Failed) return undefined
      const target = observed.targets.find(
        (target) => data.assistantMessageID === target.messageID && data.id === target.toolCallID,
      )
      if (
        !target ||
        SessionDisclosure.digest(data) !==
          SessionDisclosure.digest({
            sessionID,
            assistantMessageID: target.messageID,
            id: target.toolCallID,
            executed: target.executed,
            error: interruptedToolError,
          })
      )
        return undefined
      return processSigner().toolProjection({
        sessionID,
        inputID,
        attempt,
        messageID: target.messageID,
        toolCallID: target.toolCallID,
        projectionRevision: unitDigest,
        parentReceipts: [],
        complete: true,
        issuedAt: Date.now(),
      })
    },
  })
}

// Capture every renderer position, including multiple assistant/tool messages
// produced by one native row. Positions, rather than IDs, identify projections.
export const captureToolContext = (input: {
  readonly sessionID: typeof SessionID.Type
  readonly inputID: typeof SessionMessageID.Type
  readonly model: Model.Ref
  readonly providerMetadataKey: string
  readonly messages: readonly SessionMessage.Info[]
  readonly provenance: readonly {
    readonly id: typeof SessionMessageID.Type
    readonly state: SessionPublicationProvenance.State | null
  }[]
  readonly catalog?: CatalogEvidence
  readonly tools: Tool.Snapshot
  readonly system: readonly SystemPart[]
}) => {
  const sessionID = input.sessionID,
    inputID = input.inputID
  const pending: PendingTool[] = []
  const positions: Array<{
    readonly id: string
    readonly revision: string
    readonly projected: string
    readonly proof?: object
    readonly complete: boolean
  }> = []
  const catalog = input.catalog
  const parentReceipts = Array.from(new Set(catalog?.observations.map((item) => item.proof.receiptReference) ?? []))
  const definitions = SessionDisclosure.digest(input.tools.definitions)
  const knownDefinitions =
    ToolProvenance.snapshotComplete(input.tools) && catalogCovers(input.tools, sessionID, inputID, catalog)
  const compiledInstructions = input.tools.codeModeCatalog
    ? SessionDisclosure.digest(
        SystemPart.make(CodeModeInstructions.render(CodeModeCatalog.summarize(input.tools.codeModeCatalog))),
      )
    : undefined
  const catalogInstructions =
    catalog &&
    catalogEvidence.has(catalog) &&
    catalog.complete &&
    catalog.sessionID === sessionID &&
    catalog.inputID === inputID
      ? catalogInstructionProjections.get(catalog)
      : undefined
  const current = input.messages.find((message) => message.id === inputID)
  const instructions = current?.type === "user" ? (current.execution?.instructions ?? "") : ""
  for (const original of input.messages) {
    const message = Schema.decodeUnknownSync(SessionMessage.Info)(
      JSON.parse(JSON.stringify(Schema.encodeSync(SessionMessage.Info)(original))),
    )
    const revision = Hash.sha256(JSON.stringify(message))
    if (message.type === "assistant")
      for (const tool of message.content) {
        if (tool.type === "tool" && (tool.state.status === "streaming" || tool.state.status === "running"))
          pending.push(Object.freeze({ messageID: message.id, toolCallID: tool.id, executed: tool.executed === true }))
      }
    const projected = toLLMMessages([message], input.model, input.providerMetadataKey, true, true)
    for (const part of projected) {
      const index = positions.length
      const projectionRevision = SessionDisclosure.digest(part)
      let proof: object | undefined
      let complete = false
      if (message.type === "user" && message.preparedInput && projected.length === 1) {
        const { preparedInput: _, ...payload } = Schema.decodeUnknownSync(SessionInbox.UserPayload)(message)
        if (preparedIdentity(sessionID, message.id, message.preparedInput, payload, true)) {
          proof = ExecutionProducer.RequestOnlyUserProjection.make({
            version: 1,
            contract: "session-user-context-v1",
            preparedInput: message.preparedInput,
            index,
            projectionRevision,
            metadataRevision: SessionDisclosure.digest(message.metadata ?? {}),
            instructionsRevision: Hash.sha256(
              "native-input-instructions-v1\0" + (message.execution?.instructions ?? ""),
            ),
            controlRevision: SessionDisclosure.digest(inputControl(payload)),
          })
          complete = true
        }
      }
      if (message.type === "assistant") {
        const state = input.provenance.find((entry) => entry.id === message.id)?.state
        if (
          state &&
          state.references.length <= 128 &&
          state.revision === SessionPublicationProvenance.revision(message)
        ) {
          proof = ExecutionProducer.ToolAssistantProjection.make({
            version: 1,
            contract: "session-assistant-tool-context-v1",
            index,
            projectionRevision,
            messageRevision: state.revision,
            references: state.references,
          })
          complete = state.version === 1 || state.complete === true
        }
      }
      positions.push({ id: message.id, revision, projected: projectionRevision, proof, complete })
    }
  }
  return {
    observe: (request: LLMRequest, invoked: number, _extra?: readonly ExecutionDisclosure.Component[]) =>
      Effect.sync(() => {
        const components: ExecutionDisclosure.Component[] = []
        let complete = invoked === 0 && request.metadata === undefined && request.providerOptions === undefined
        for (const [index, part] of request.messages.entries()) {
          const candidate = positions[index]
          const measured = SessionDisclosure.digest(part)
          const proof = candidate?.projected === measured ? candidate.proof : undefined
          const encoded = proof === undefined ? undefined : JSON.stringify(proof)
          const bounded = encoded !== undefined && Buffer.byteLength(encoded) <= 16384 ? encoded : undefined
          complete &&= candidate?.complete === true && bounded !== undefined
          components.push({
            kind: "message",
            id: candidate?.id ?? `position:${index}`,
            revision: candidate?.revision ?? measured,
            ...(bounded === undefined ? {} : { proof: bounded }),
          })
        }
        complete &&=
          request.messages.length === positions.length && request.messages.length > 0 && request.messages.length <= 2048
        const prepared = SessionDisclosure.makeRequest({
          sessionID,
          inputID,
          kind: "primary",
          contextDigest: SessionDisclosure.digest(SessionDisclosure.logicalContext(request)),
          components: [],
        })
        const observe = (kind: "tools" | "instructions", index: number, projectionRevision: string, known: boolean) => {
          const fact = processSigner().toolDefinitions({
            sessionID,
            inputID,
            attempt: prepared.attempt,
            kind,
            index,
            projectionRevision,
            parentReceipts,
            complete: known,
            issuedAt: Date.now(),
          })
          const proof = JSON.stringify(fact)
          if (Buffer.byteLength(proof) > 16384)
            throw new Error("Tool definition evidence exceeds the bounded context limit")
          complete &&= known
          components.push({
            kind: kind === "tools" ? "tools" : "instructions",
            id: kind === "tools" ? "advertised-definitions" : `system:${index}`,
            revision: projectionRevision,
            proof,
          })
        }
        observe(
          "tools",
          0,
          SessionDisclosure.digest(request.tools),
          knownDefinitions && definitions === SessionDisclosure.digest(request.tools),
        )
        for (const [index, part] of request.system.entries()) {
          const measured = SessionDisclosure.digest(part)
          const root = instructions !== "" && measured === SessionDisclosure.digest(SystemPart.make(instructions))
          observe(
            "instructions",
            index,
            measured,
            root ||
              (knownDefinitions && (compiledInstructions === measured || catalogInstructions?.has(measured) === true)),
          )
        }
        const owned = { ...prepared, components, componentsDigest: SessionDisclosure.digest(components) }
        const observed = {
          ...owned,
          composition: processSigner().toolComposition(owned, request.messages.length, request.tools.length, complete),
        }
        if (Buffer.byteLength(JSON.stringify(observed)) > 512 * 1024)
          throw new Error("Tool context exceeds the bounded context limit")
        pendingContexts.set(observed, {
          revision: SessionDisclosure.digest(observed),
          targets: Object.freeze([...pending]),
        })
        return observed
      }),
  }
}

export const projectCurrentToolUnit = (unitDigest: string) =>
  Effect.gen(function* () {
    const scope = Option.getOrUndefined(yield* Effect.serviceOption(ToolCapture))
    if (!scope || !scope.valid()) return undefined
    return scope.project(unitDigest)
  })

// This producer recognizes only canonical model-generated input/call events.
// Provider progress/errors and arbitrary event metadata cannot select this path.
export const captureToolControl = (service: ExecutionDisclosure.Service, signed: ExecutionDisclosure.SignedProof) => {
  const capture = McpProvenance.capture(service, signed)
  const allowed = new Set([
    SessionEvent.Tool.Input.Started,
    SessionEvent.Tool.Input.Delta,
    SessionEvent.Tool.Input.Ended,
    SessionEvent.Tool.Called,
  ])
  return {
    unknown: (_definition: object, data: Record<string, unknown>, unitDigest: string) => {
      if (
        data.sessionID !== capture.context.sessionID ||
        typeof data.assistantMessageID !== "string" ||
        typeof data.id !== "string"
      )
        return undefined
      return processSigner().toolProjection({
        sessionID: capture.context.sessionID,
        inputID: capture.context.inputID,
        attempt: capture.context.attempt,
        messageID: SessionMessageID.make(data.assistantMessageID),
        toolCallID: data.id,
        projectionRevision: unitDigest,
        parentReceipts: [],
        complete: false,
        issuedAt: Date.now(),
      })
    },
    project: (definition: object, data: Record<string, unknown>, unitDigest: string) => {
      if (
        !allowed.has(definition as typeof SessionEvent.Tool.Called) ||
        data.sessionID !== capture.context.sessionID ||
        typeof data.assistantMessageID !== "string" ||
        typeof data.id !== "string"
      )
        return undefined
      return processSigner().toolProjection({
        sessionID: capture.context.sessionID,
        inputID: capture.context.inputID,
        attempt: capture.context.attempt,
        messageID: SessionMessageID.make(data.assistantMessageID),
        toolCallID: data.id,
        projectionRevision: unitDigest,
        parentReceipts: [],
        complete: true,
        issuedAt: Date.now(),
      })
    },
  }
}
