import { expect } from "bun:test"
import { createPublicKey, verify } from "node:crypto"
import { DateTime, Effect, Schema } from "effect"
import { LLM, Message, SystemPart } from "@opencode/ai"
import { OpenAIChat } from "@opencode/ai/protocols"
import { SessionMessage } from "@opencode/schema/session-message"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { toLLMMessages } from "@opencode/core/session/runner/to-llm-message"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { SessionID } from "@opencode/schema/session-id"
import { SessionMessageID } from "@opencode/schema/session-message-id"
import { NativeProducer } from "@opencode/core/execution-producer"
import { SessionDisclosure } from "@opencode/core/session/disclosure"
import { Hash } from "@opencode/util/hash"
import { it } from "./lib/effect"

const enrollment = () => NativeProducer.enroll({ version: 1, challenge: "c".repeat(43), audience: "fixture-host" })
const valid = (signed: ExecutionProducer.SignedFact, key: ExecutionProducer.Enrollment["key"]) =>
  verify(
    null,
    Buffer.from(ExecutionProducer.SignatureDomain + signed.payload),
    createPublicKey({
      key: Buffer.concat([Buffer.from("302a300506032b6570032100", "hex"), Buffer.from(key.publicKey, "base64url")]),
      format: "der",
      type: "spki",
    }),
    Buffer.from(signed.signature, "base64url"),
  )
const prepare = (text: string, session = "ses_first", input = "msg_first") => ({
  sessionID: SessionID.make(session),
  inputID: SessionMessageID.make(input),
  original: { text },
  invoked: 0,
  payload: SessionInbox.UserPayload.make({ text }),
})

it.effect("signs bounded challenges with one process identity and no publication claim", () =>
  Effect.gen(function* () {
    const first = yield* enrollment()
    const second = yield* NativeProducer.enroll({ version: 1, challenge: "d".repeat(43), audience: "other-host" })
    const proof = Schema.decodeUnknownSync(ExecutionProducer.EnrollmentProof)(JSON.parse(first.signed.payload))
    const next = Schema.decodeUnknownSync(ExecutionProducer.EnrollmentProof)(JSON.parse(second.signed.payload))
    expect(valid(first.signed, first.key)).toBeTrue()
    expect(first.signed.keyID).toBe(first.key.id)
    expect(first.key.id).toBe(Hash.sha256(first.key.publicKey))
    expect(second.key).toEqual(first.key)
    expect(next.producerEpoch).toBe(proof.producerEpoch)
    expect(next.challenge).toBe("d".repeat(43))
    expect(next.audience).toBe("other-host")
    expect(proof.contracts).toEqual(["human-plaintext-identity-v1"])
    const negotiated = yield* NativeProducer.enroll({
      version: 1,
      challenge: "e".repeat(43),
      audience: "new-host",
      contracts: ["human-plaintext-identity-v1", "model-plaintext-projection-v1"],
    })
    expect(
      Schema.decodeUnknownSync(ExecutionProducer.EnrollmentProof)(JSON.parse(negotiated.signed.payload)).contracts,
    ).toEqual(["human-plaintext-identity-v1", "model-plaintext-projection-v1"])
    expect(negotiated.key).toEqual(first.key)
    expect(JSON.stringify(first)).not.toContain("privateKey")
    expect(valid({ ...first.signed, payload: second.signed.payload }, first.key)).toBeFalse()
    expect(valid({ ...first.signed, payload: "publication\0" + first.signed.payload }, first.key)).toBeFalse()
    expect((yield* Effect.exit(NativeProducer.enroll({ version: 1, challenge: "short", audience: "host" })))._tag).toBe(
      "Failure",
    )
  }),
)

it.effect("binds exact Unicode text, original IDs and an owned normalized payload for multiple inputs", () =>
  Effect.gen(function* () {
    const { key } = yield* enrollment()
    for (const [session, input, text] of [
      ["ses_first", "msg_first", "First λ\n"],
      ["ses_second", "msg_second", "Second 🦋"],
    ]) {
      const metadata = { nested: { value: "before" } }
      const capabilities = ["disclosure"]
      const candidate = { ...prepare(text, session, input), payload: { text, metadata, capabilities } }
      const output = yield* NativeProducer.prepareInput(candidate)
      if (!output?.preparedInput) throw new Error("Expected preparation observation")
      const fact = Schema.decodeUnknownSync(ExecutionProducer.PreparedInput)(JSON.parse(output.preparedInput.payload))
      expect(valid(output.preparedInput, key)).toBeTrue()
      expect(fact.sessionID).toBe(SessionID.make(session))
      expect(fact.inputID).toBe(SessionMessageID.make(input))
      expect(fact.parentTextRevision).toBe(Hash.sha256("native-human-text-v1\0" + text))
      expect(fact.preparedTextRevision).toBe(fact.parentTextRevision)
      expect(fact.preparedPayloadRevision).toBe(SessionDisclosure.digest({ ...output, preparedInput: undefined }))
      metadata.nested.value = "after"
      capabilities.push("publication-v1")
      expect(output.metadata).toEqual({ nested: { value: "before" } })
      expect(output.capabilities).toEqual(["disclosure"])
      expect(fact.preparedPayloadRevision).toBe(SessionDisclosure.digest({ ...output, preparedInput: undefined }))
    }
  }),
)

it.effect("leaves nonidentity preparation and unsupported dependencies unclassified", () =>
  Effect.gen(function* () {
    const base = prepare("Original")
    const variants = [
      { ...base, invoked: 1 },
      { ...base, original: { text: "Different" } },
      { ...base, original: { text: "" }, payload: { text: "" } },
      { ...base, original: { text: "\ud800" }, payload: { text: "\ud800" } },
      { ...base, original: { text: "Original", files: [{}] } },
      { ...base, original: { text: "Original", agents: [{}] } },
      { ...base, original: { text: "Original", skills: [{}] } },
      { ...base, payload: { text: "Original", agents: [{ name: "helper" }] } },
    ]
    for (const input of variants) expect(yield* NativeProducer.prepareInput(input)).toBeUndefined()
    expect(
      yield* NativeProducer.prepareInput({
        ...base,
        original: { text: "Original", files: [], agents: [], skills: [] },
      }),
    ).toBeDefined()
  }),
)

it.live("pins the launch nonce before mutable runtime environment and rotates identity on a new process", () =>
  Effect.gen(function* () {
    const run = () =>
      Effect.promise(async () => {
        const code = `import { NativeProducer } from "./src/execution-producer.ts";
        import { Effect } from "effect";
        process.env.OPENCODE_PRODUCER_BOOT_NONCE = "z".repeat(43);
        const value = await Effect.runPromise(NativeProducer.enroll({version:1, challenge:"c".repeat(43), audience:"fixture-host"}));
        process.stdout.write(JSON.stringify(value));`
        const child = Bun.spawn([process.execPath, "-e", code], {
          cwd: import.meta.dir + "/..",
          env: { PATH: process.env.PATH ?? "", OPENCODE_PRODUCER_BOOT_NONCE: "b".repeat(43) },
          stdout: "pipe",
          stderr: "pipe",
        })
        const [stdout, stderr, exit] = await Promise.all([
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
          child.exited,
        ])
        if (exit !== 0) throw new Error(`Producer fixture failed: ${stderr}`)
        return Schema.decodeUnknownSync(ExecutionProducer.Enrollment)(JSON.parse(stdout))
      })
    const first = yield* run()
    const second = yield* run()
    const proof = Schema.decodeUnknownSync(ExecutionProducer.EnrollmentProof)(JSON.parse(first.signed.payload))
    const next = Schema.decodeUnknownSync(ExecutionProducer.EnrollmentProof)(JSON.parse(second.signed.payload))
    expect(valid(first.signed, first.key)).toBeTrue()
    expect(proof.bootNonce).toBe("b".repeat(43))
    expect(next.bootNonce).toBe(proof.bootNonce)
    expect(next.producerEpoch).not.toBe(proof.producerEpoch)
    expect(second.key).not.toEqual(first.key)
  }),
)

it.effect("observes exact owned human projections and binds every logical field to one attempt", () =>
  Effect.gen(function* () {
    const { key } = yield* enrollment()
    const scope = {
      ...prepare("Original human"),
      payload: SessionInbox.UserPayload.make({ text: "Original human", metadata: { source: "unclassified" } }),
    }
    const payload = yield* NativeProducer.prepareInput(scope)
    if (!payload?.preparedInput) throw new Error("Expected preparation")
    const message = Schema.decodeUnknownSync(SessionMessage.User)(
      Schema.encodeSync(SessionMessage.User)(
        SessionMessage.User.make({
          id: scope.inputID,
          type: "user",
          time: { created: DateTime.makeUnsafe(0) },
          ...payload,
        }),
      ),
    )
    const model = OpenAIChat.route.model({ id: "fixture-model", provider: "fixture-provider" })
    const ref = Model.Ref.make({
      id: Model.ID.make("fixture-model"),
      providerID: Provider.ID.make("fixture-provider"),
      variant: Model.VariantID.make("default"),
    })
    const projected = toLLMMessages([message], ref, "fixture-provider")
    const components = [{ kind: "message" as const, id: message.id, revision: Hash.sha256(JSON.stringify(message)) }]
    const captured = NativeProducer.captureContext({
      sessionID: scope.sessionID,
      inputID: scope.inputID,
      model: ref,
      providerMetadataKey: "fixture-provider",
      messages: [message],
      components,
    })
    if (!captured) throw new Error("Expected owned capture")
    const request = SessionDisclosure.snapshot(
      LLM.request({ model, system: [], messages: projected, generation: { maxTokens: 100 } }),
    )
    components[0].revision = "f".repeat(64)
    const observed = yield* captured.observe(request, 0, [])
    if (!observed?.composition || !observed.components[0]?.proof) throw new Error("Expected partial observation")
    const composition = Schema.decodeUnknownSync(ExecutionProducer.ModelComposition)(
      JSON.parse(observed.composition.payload),
    )
    const projection = Schema.decodeUnknownSync(ExecutionProducer.HumanProjection)(
      JSON.parse(observed.components[0].proof),
    )
    expect(valid(observed.composition, key)).toBeTrue()
    expect(composition.complete).toBeFalse()
    expect(composition.attempt).toBe(observed.attempt)
    expect(composition.contextDigest).toBe(SessionDisclosure.digest(SessionDisclosure.logicalContext(request)))
    expect(composition.componentsDigest).toBe(SessionDisclosure.digest(observed.components))
    expect(projection.complete).toBeFalse()
    expect(projection.index).toBe(0)
    expect(projection.preparedInput).toEqual(payload.preparedInput)
    expect(projection.projectionRevision).toBe(SessionDisclosure.digest(request.messages[0]))
    expect(observed.components[0].revision).not.toBe(components[0].revision)
    expect(yield* captured.observe(request, 1, [])).toBeUndefined()
    const duplicated = SessionDisclosure.snapshot(
      LLM.request({ model, system: [], messages: [...projected, ...projected] }),
    )
    const duplicate = yield* captured.observe(duplicated, 0, [])
    expect(duplicate?.components[0].proof).toBeUndefined()
    const changed = SessionDisclosure.snapshot(
      LLM.request({
        model,
        system: [],
        messages: [Message.make({ id: message.id, role: "user", content: "Changed" })],
      }),
    )
    expect((yield* captured.observe(changed, 0, []))?.components[0].proof).toBeUndefined()
    const reordered = SessionDisclosure.snapshot(
      LLM.request({ model, system: [], messages: [Message.assistant("Earlier"), ...projected] }),
    )
    expect((yield* captured.observe(reordered, 0, []))?.components[0].proof).toBeUndefined()
    const transplanted = NativeProducer.captureContext({
      sessionID: scope.sessionID,
      inputID: scope.inputID,
      model: ref,
      providerMetadataKey: "fixture-provider",
      messages: [message],
      components: [
        { kind: "message", id: message.id, revision: "a".repeat(64), proof: observed.components[0].proof },
        { kind: "message", id: "msg_unmatched", revision: "b".repeat(64), proof: observed.components[0].proof },
      ],
    })
    if (!transplanted) throw new Error("Expected capture")
    const stripped = yield* transplanted.observe(changed, 0, [
      { kind: "prepared-context", id: "unmatched", revision: "c".repeat(64), proof: observed.components[0].proof },
    ])
    expect(stripped?.components).toHaveLength(3)
    expect(stripped?.components.every((component) => component.proof === undefined)).toBeTrue()
    expect(stripped?.componentsDigest).toBe(SessionDisclosure.digest(stripped?.components))
  }),
)

it.effect("measures closed request-only dependencies without endorsing unaccounted values", () =>
  Effect.gen(function* () {
    const enrolled = yield* NativeProducer.enroll({
      version: 1,
      challenge: "d".repeat(43),
      audience: "closed-host",
      contracts: ["human-plaintext-identity-v1", "model-plaintext-projection-v1", "model-request-only-plaintext-v1"],
    })
    expect(
      Schema.decodeUnknownSync(ExecutionProducer.EnrollmentProof)(JSON.parse(enrolled.signed.payload)).contracts,
    ).toEqual(["human-plaintext-identity-v1", "model-plaintext-projection-v1", "model-request-only-plaintext-v1"])
    const scope = prepare("Original full human")
    const payload = yield* NativeProducer.prepareInput({
      ...scope,
      payload: {
        text: scope.original.text,
        metadata: { source: "bound-host-metadata" },
      },
    })
    if (!payload) throw new Error("Missing original preparation")
    const message = SessionMessage.User.make({
      ...payload,
      id: scope.inputID,
      type: "user",
      time: { created: DateTime.makeUnsafe(0) },
    })
    const model = OpenAIChat.route.model({ id: "fixture-model", provider: "fixture-provider" })
    const ref = Model.Ref.make({ id: Model.ID.make("fixture-model"), providerID: Provider.ID.make("fixture-provider") })
    const request = LLM.request({ model, system: [], messages: toLLMMessages([message], ref, "fixture-provider") })
    const captured = NativeProducer.captureContext({
      sessionID: scope.sessionID,
      inputID: scope.inputID,
      model: ref,
      providerMetadataKey: "fixture-provider",
      messages: [message],
      components: [],
      requestOnly: { provenance: [] },
    })
    if (!captured) throw new Error("Missing closed capture")
    const observed = yield* captured.observe(request, 0, [])
    if (!observed?.composition) throw new Error("Missing closed observation")
    const composed = Schema.decodeUnknownSync(ExecutionProducer.RequestOnlyComposition)(
      JSON.parse(observed.composition.payload),
    )
    expect(valid(observed.composition, enrolled.key)).toBe(true)
    expect(composed.complete).toBe(true)
    expect(composed.messageCount).toBe(1)
    const projected = Schema.decodeUnknownSync(ExecutionProducer.RequestOnlyUserProjection)(
      JSON.parse(observed.components[0]!.proof!),
    )
    expect(projected.metadataRevision).toBe(SessionDisclosure.digest(payload.metadata))
    expect(projected.instructionsRevision).toBe(Hash.sha256("native-input-instructions-v1\0"))
    expect(projected.controlRevision).toBe(SessionDisclosure.digest({ execution: null, capabilities: [] }))
    const admitted = yield* NativeProducer.inputContext(scope.sessionID, scope.inputID, payload)
    expect(admitted?.kind).toBe("input")
    if (!admitted?.composition) throw new Error("Missing measured input")
    expect(valid(admitted.composition, enrolled.key)).toBe(true)
    for (const unaccounted of [
      LLM.request({ model, system: [SystemPart.make("Unaccounted system")], messages: request.messages }),
      LLM.request({ model, system: [], messages: request.messages, metadata: { hidden: "Private metadata" } }),
      LLM.request({
        model,
        system: [],
        messages: request.messages,
        providerOptions: { arbitrary: { input: "Private input" } },
      }),
      LLM.request({ model, system: [], messages: request.messages, toolChoice: "none" }),
      LLM.request({ model, system: [], messages: [...request.messages, Message.user("Unknown history")] }),
    ])
      expect(yield* captured.observe(unaccounted, 0, [])).toBeUndefined()
    expect(yield* captured.observe(request, 1, [])).toBeUndefined()
    expect(
      yield* NativeProducer.inputContext(scope.sessionID, scope.inputID, { ...payload, text: "Changed input" }),
    ).toBeUndefined()
  }),
)
