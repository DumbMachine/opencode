import { expect } from "bun:test"
import { DateTime, Effect, Schema } from "effect"
import { LLM, Media, Message } from "@opencode/ai"
import { OpenAIChat } from "@opencode/ai/protocols"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { SessionMessage } from "@opencode/schema/session-message"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { SessionID } from "@opencode/schema/session-id"
import { SessionMessageID } from "@opencode/schema/session-message-id"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { NativeProducer } from "@opencode/core/execution-producer"
import { SessionInlineIdentity } from "@opencode/core/session/inline-identity"
import { SessionDisclosure } from "@opencode/core/session/disclosure"
import { toLLMMessages } from "@opencode/core/session/runner/to-llm-message"
import { Hash } from "@opencode/util/hash"
import { it } from "./lib/effect"

const png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
const files = [
  {
    uri: "data:text/plain;base64," + Buffer.from("Inline λ\n").toString("base64"),
    name: "source.txt",
    description: "description <>&\u2028\u2029",
  },
  ...Array.from({ length: 2 }, () => ({
    uri: "data:image/png;base64," + png,
    name: "repeated.png",
    mention: { start: 0, end: 9, text: "[Image 1]" },
  })),
]
const preparedFiles = () =>
  files.map((file) => {
    const value = SessionInlineIdentity.materialize(file)
    if (!value) throw new Error("Missing inline fixture")
    return value
  })
const scope = (session: string, input: string, text = "") => ({
  sessionID: SessionID.make(session),
  inputID: SessionMessageID.make(input),
  original: { text, files },
  invoked: 0,
  inlineIdentity: true,
  payload: SessionInbox.UserPayload.make({ text, files: preparedFiles(), metadata: { original: input } }),
})
const ref = Model.Ref.make({ id: Model.ID.make("fixture-model"), providerID: Provider.ID.make("fixture-provider") })
const model = OpenAIChat.route.model({ id: "fixture-model", provider: "fixture-provider" })
const user = (input: Pick<ReturnType<typeof scope>, "inputID">, payload: SessionInbox.UserPayload) =>
  SessionMessage.User.make({
    ...payload,
    id: input.inputID,
    type: "user",
    time: { created: DateTime.makeUnsafe(0) },
  })

it.effect("bounds the whole projected request by raw media bytes while preserving duplicate occurrences", () =>
  Effect.sync(() => {
    const media = Media.base64(Buffer.alloc(11 * 1024 * 1024).toString("base64"), "image/png")
    const message = Message.make({ role: "user", content: [{ type: "media", media }] })
    expect(SessionInlineIdentity.withinRequestLimit(Array(3).fill(message))).toBe(true)
    expect(SessionInlineIdentity.withinRequestLimit(Array(4).fill(message))).toBe(false)
    expect(
      SessionInlineIdentity.withinRequestLimit([
        Message.make({
          role: "user",
          content: [{ type: "media", media: Media.url("https://example.invalid/image.png") }],
        }),
      ]),
    ).toBe(false)
  }),
)

it.effect("matches the shared Go Unicode and escaping commitment vector", () =>
  Effect.sync(() => {
    expect(
      SessionInlineIdentity.originalRevision({
        text: "Human λ<>&\u2028\u2029",
        files: [
          {
            uri: "data:text/plain;base64,SGVsbG8gzrsK",
            name: "source.txt",
            description: "<>&\u2028\u2029",
            mention: { start: 0, end: 4, text: "file" },
          },
        ],
      }),
    ).toBe("703c589b72a1111be96657378a595481ea19596ff18652eee9c565d2228641ab")
  }),
)

it.effect("negotiates inline contracts separately without widening plaintext enrollment", () =>
  Effect.gen(function* () {
    const human = yield* NativeProducer.enroll({
      version: 1,
      challenge: "i".repeat(43),
      audience: "inline-host",
      contracts: [
        "human-plaintext-identity-v1",
        "model-plaintext-projection-v1",
        "model-request-only-plaintext-v1",
        "human-inline-identity-v1",
      ],
    })
    expect(JSON.parse(human.signed.payload).contracts).toEqual([
      "human-plaintext-identity-v1",
      "model-plaintext-projection-v1",
      "model-request-only-plaintext-v1",
      "human-inline-identity-v1",
    ])
    const notHuman = yield* NativeProducer.enroll({
      version: 1,
      challenge: "j".repeat(43),
      audience: "inline-host",
      contracts: ["model-request-only-inline-v1"],
    })
    expect(JSON.parse(notHuman.signed.payload).contracts).toEqual(["human-plaintext-identity-v1"])
    const both = yield* NativeProducer.enroll({
      version: 1,
      challenge: "k".repeat(43),
      audience: "inline-host",
      contracts: [
        "human-plaintext-identity-v1",
        "model-plaintext-projection-v1",
        "model-request-only-plaintext-v1",
        "human-inline-identity-v1",
        "model-request-only-inline-v1",
      ],
    })
    expect(JSON.parse(both.signed.payload).contracts).toEqual([
      "human-plaintext-identity-v1",
      "model-plaintext-projection-v1",
      "model-request-only-plaintext-v1",
      "human-inline-identity-v1",
      "model-request-only-inline-v1",
    ])
  }),
)

it.effect(
  "commits exact attachment-only content, ordered repeated bytes and every descriptor for multiple originals",
  () =>
    Effect.gen(function* () {
      for (const [session, input] of [
        ["ses_inline_one", "msg_inline_one"],
        ["ses_inline_two", "msg_inline_two"],
      ]) {
        const candidate = scope(session!, input!)
        const payload = yield* NativeProducer.prepareInput(candidate)
        if (!payload?.preparedInput) throw new Error("Missing inline preparation")
        const fact = Schema.decodeUnknownSync(ExecutionProducer.PreparedInlineInput)(
          JSON.parse(payload.preparedInput.payload),
        )
        expect(fact.parentContentRevision).toBe(SessionInlineIdentity.originalRevision(candidate.original)!)
        expect(fact.preparedContentRevision).toBe(SessionInlineIdentity.revision(payload)!)
        expect(fact.preparedPayloadRevision).toBe(SessionDisclosure.digest({ ...payload, preparedInput: undefined }))
        expect(payload.files).toEqual(candidate.payload.files)
        expect(payload.files).toHaveLength(3)
        const tuple = [
          "native-human-inline-content-v1",
          "",
          preparedFiles().map((file) => [
            file.mime,
            file.name ?? null,
            file.description ?? null,
            file.mention === undefined ? null : [file.mention.start, file.mention.end, file.mention.text],
            Hash.sha256(Buffer.from(file.data, "base64")),
            Buffer.from(file.data, "base64").byteLength,
          ]),
        ]
        expect(fact.parentContentRevision).toBe(
          Hash.sha256(
            JSON.stringify(tuple)
              .replace(/\u2028/g, "\\u2028")
              .replace(/\u2029/g, "\\u2029"),
          ),
        )
        const admitted = yield* NativeProducer.inputContext(candidate.sessionID, candidate.inputID, payload, true)
        expect(admitted?.kind).toBe("input")
        expect(JSON.parse(admitted!.composition!.payload).contract).toBe("model-request-only-inline-v1")
        for (const changed of [
          { ...payload, text: "changed" },
          { ...payload, files: [...payload.files!].reverse() },
          {
            ...payload,
            files: payload.files!.map((file, i) =>
              i === 0 ? { ...file, data: "Y2hhbmdlZA==" as typeof file.data } : file,
            ),
          },
          { ...payload, files: payload.files!.map((file, i) => (i === 0 ? { ...file, name: "renamed.txt" } : file)) },
          {
            ...payload,
            files: payload.files!.map((file, i) => (i === 0 ? { ...file, description: "changed" } : file)),
          },
          {
            ...payload,
            files: payload.files!.map((file, i) =>
              i === 1 ? { ...file, mention: { start: 1, end: 10, text: "[Image 1]" } } : file,
            ),
          },
        ])
          expect(
            yield* NativeProducer.inputContext(candidate.sessionID, candidate.inputID, changed, true),
          ).toBeUndefined()
        expect(yield* NativeProducer.prepareInput({ ...candidate, invoked: 1 })).toBeUndefined()
        expect(yield* NativeProducer.prepareInput({ ...candidate, inlineIdentity: false })).toBeUndefined()
      }
    }),
)

it.effect(
  "observes the actual complete multifile renderer and rejects mutations, reorder and fact transplantation",
  () =>
    Effect.gen(function* () {
      const one = scope("ses_inline_composition", "msg_inline_first", "First")
      const two = scope("ses_inline_composition", "msg_inline_second")
      const first = yield* NativeProducer.prepareInput(one)
      const second = yield* NativeProducer.prepareInput(two)
      if (!first || !second) throw new Error("Missing inline preparation")
      const messages = [user(one, first), user(two, second)]
      const rendered = toLLMMessages(messages, ref, "fixture-provider", true)
      expect(rendered[0]!.content.filter((part) => part.type === "media")).toHaveLength(2)
      const captured = NativeProducer.captureContext({
        sessionID: one.sessionID,
        inputID: two.inputID,
        model: ref,
        providerMetadataKey: "fixture-provider",
        messages,
        components: [],
        requestOnly: { inlineIdentity: true, provenance: [] },
      })
      if (!captured) throw new Error("Missing inline capture")
      const request = SessionDisclosure.snapshot(LLM.request({ model, system: [], messages: rendered }))
      const observed = yield* captured.observe(request, 0, [])
      if (!observed?.composition) throw new Error("Missing complete inline composition")
      const composed = Schema.decodeUnknownSync(ExecutionProducer.InlineRequestOnlyComposition)(
        JSON.parse(observed.composition.payload),
      )
      expect(composed.complete).toBe(true)
      expect(composed.messageCount).toBe(2)
      expect(composed.contextDigest).toBe(SessionDisclosure.digest(SessionDisclosure.logicalContext(request)))
      expect(observed.components.map((part) => JSON.parse(part.proof!).index)).toEqual([0, 1])
      expect(yield* captured.observe(request, 1, [])).toBeUndefined()
      for (const messages of [
        [...request.messages].reverse(),
        [request.messages[0]!, request.messages[0]!],
        [
          Message.make({ ...request.messages[0]!, content: request.messages[0]!.content.slice(0, -1) }),
          request.messages[1]!,
        ],
        [Message.make({ ...request.messages[0]!, metadata: { changed: true } }), request.messages[1]!],
      ])
        expect(yield* captured.observe(LLM.request({ model, system: [], messages }), 0, [])).toBeUndefined()
      const transplanted = NativeProducer.captureContext({
        sessionID: one.sessionID,
        inputID: two.inputID,
        model: ref,
        providerMetadataKey: "fixture-provider",
        messages: [messages[0]!, { ...messages[1]!, preparedInput: first.preparedInput }],
        components: [],
        requestOnly: { inlineIdentity: true, provenance: [] },
      })
      expect(transplanted && (yield* transplanted.observe(request, 0, []))).toBeUndefined()
    }),
)

it.effect(
  "fails unsupported sources, encodings, media, Unicode, ranges and resource kinds without materialization I/O",
  () =>
    Effect.gen(function* () {
      const base = scope("ses_inline_denied", "msg_inline_denied")
      for (const file of [
        { uri: "file:///private/tmp/not-readable.txt" },
        { uri: "https://example.invalid/source.txt" },
        { uri: "resource://one" },
        { uri: "data:text/plain,Hello" },
        { uri: "data:text/plain;base64,YQ" },
        { uri: "data:application/pdf;base64,JVBERi0=" },
        { uri: "data:image/heic;base64,YQ==" },
        { uri: "data:image/png;base64,YQ==" },
        { uri: "data:text/plain;base64,/w==" },
        { uri: files[0]!.uri, name: "\ud800" },
        { uri: files[0]!.uri, description: "\ud800" },
        { uri: files[0]!.uri, mention: { start: -1, end: 1, text: "x" } },
        { uri: files[0]!.uri, mention: { start: 0.5, end: 1, text: "x" } },
        { uri: files[0]!.uri, mention: { start: 2, end: 1, text: "x" } },
        { uri: "file:///private/tmp/not-readable.txt?start=1&end=3" },
      ])
        expect(yield* NativeProducer.prepareInput({ ...base, original: { text: "", files: [file] } })).toBeUndefined()
      expect(SessionInlineIdentity.originalRevision({ text: "", files: Array(17).fill(files[0]) })).toBeUndefined()
      expect(SessionInlineIdentity.originalRevision({ text: "\ud800", files })).toBeUndefined()
      expect(SessionInlineIdentity.originalRevision({ text: "", files: [] })).toBeUndefined()
      expect(SessionInlineIdentity.originalRevision({ text: "", files, agents: [{ name: "helper" }] })).toBeUndefined()
      expect(SessionInlineIdentity.originalRevision({ text: "", files, skills: [{ id: "skill" }] })).toBeUndefined()
      const large = { uri: "data:text/plain;base64," + Buffer.alloc(11 * 1024 * 1024, 97).toString("base64") }
      expect(SessionInlineIdentity.originalRevision({ text: "", files: [large, large] })).toBeUndefined()
      for (const mime of ["image/png", "image/jpeg", "image/gif", "image/webp"]) {
        const bytes =
          mime === "image/png"
            ? Buffer.from(png, "base64")
            : mime === "image/jpeg"
              ? Buffer.from([255, 216, 255, 217])
              : mime === "image/gif"
                ? Buffer.from("GIF89a")
                : Buffer.from("RIFF0000WEBP")
        const original = { text: "", files: [{ uri: `data:${mime};base64,${bytes.toString("base64")}` }] }
        expect(SessionInlineIdentity.originalRevision(original)).toBeDefined()
      }
    }),
)

it.effect("reuses prior plaintext human facts inside the separately enrolled inline request lane", () =>
  Effect.gen(function* () {
    const first = {
      sessionID: SessionID.make("ses_inline_mixed"),
      inputID: SessionMessageID.make("msg_plain_prior"),
      original: { text: "Prior plaintext" },
      invoked: 0,
      payload: SessionInbox.UserPayload.make({ text: "Prior plaintext" }),
    }
    const previous = yield* NativeProducer.prepareInput(first)
    const current = scope("ses_inline_mixed", "msg_inline_current")
    const prepared = yield* NativeProducer.prepareInput(current)
    if (!previous || !prepared) throw new Error("Missing mixed preparation")
    const messages = [
      SessionMessage.User.make({
        ...previous,
        id: first.inputID,
        type: "user",
        time: { created: DateTime.makeUnsafe(0) },
      }),
      user(current, prepared),
    ]
    const captured = NativeProducer.captureContext({
      sessionID: current.sessionID,
      inputID: current.inputID,
      model: ref,
      providerMetadataKey: "fixture-provider",
      messages,
      components: [],
      requestOnly: { inlineIdentity: true, provenance: [] },
    })
    const observed =
      captured &&
      (yield* captured.observe(
        LLM.request({ model, system: [], messages: toLLMMessages(messages, ref, "fixture-provider", true) }),
        0,
        [],
      ))
    expect(
      observed?.components.map((part) => JSON.parse(JSON.parse(part.proof!).preparedInput.payload).contract),
    ).toEqual(["human-plaintext-identity-v1", "human-inline-identity-v1"])
  }),
)

it.effect("retains UTF8 BOM bytes in the actual text attachment renderer", () =>
  Effect.gen(function* () {
    const file = {
      uri: "data:text/plain;base64," + Buffer.from("\ufeffλ\n", "utf8").toString("base64"),
      name: "bom.txt",
    }
    const prepared = SessionInlineIdentity.materialize(file)
    if (!prepared) throw new Error("Missing BOM preparation")
    const candidate = {
      ...scope("ses_inline_bom", "msg_inline_bom"),
      original: { text: "", files: [file] },
      payload: SessionInbox.UserPayload.make({ text: "", files: [prepared] }),
    }
    const payload = yield* NativeProducer.prepareInput(candidate)
    if (!payload) throw new Error("Missing BOM identity")
    expect(payload.files![0]!.data).toBe(Buffer.from("\ufeffλ\n", "utf8").toString("base64"))
    const rendered = toLLMMessages([user(candidate, payload)], ref, "fixture-provider", true)
    expect(
      rendered[0]!.content
        .filter((part) => part.type === "text")
        .map((part) => part.text)
        .join(""),
    ).toContain("\ufeffλ\n")
  }),
)
