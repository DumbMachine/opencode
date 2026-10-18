import { expect } from "bun:test"
import { createPublicKey, verify } from "node:crypto"
import { Effect, Schema } from "effect"
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
