export * as SessionInlineIdentity from "./inline-identity.js"

import type { Message } from "@opencode/ai"
import { Option, Schema } from "effect"
import { Base64, FileAttachment, Prompt } from "@opencode/schema/prompt"
import { PromptInput } from "@opencode/schema/prompt-input"
import { Hash } from "@opencode/util/hash"
import { Mime } from "../mime.js"

export const MaximumFiles = 16
export const MaximumBytes = 20 * 1024 * 1024
export const MaximumRequestBytes = 40 * 1024 * 1024

// Count every projected media occurrence, including intentional duplicates.
export const withinRequestLimit = (messages: readonly Message[]) =>
  messages.reduce(
    (total, message) =>
      total +
      message.content.reduce((bytes, part) => {
        if (part.type !== "media") return bytes
        const source = part.media.source
        return (
          bytes +
          (source.type === "base64"
            ? Buffer.byteLength(source.data, "base64")
            : source.type === "bytes"
              ? source.data.byteLength
              : Infinity)
        )
      }, 0),
    0,
  ) <= MaximumRequestBytes
const mimes = new Set(["text/plain", "image/png", "image/jpeg", "image/gif", "image/webp"])
const unicode = (text: string) => Buffer.from(text, "utf8").toString("utf8") === text
const decodeFile = Schema.decodeUnknownOption(PromptInput.FileAttachment)
const decodeBase64 = Schema.decodeUnknownOption(Base64)

// Canonical data URLs are the only source in this identity contract. This
// validator never opens a path, fetches a URI, or normalizes image bytes.
export const materialize = (input: PromptInput.FileAttachment): FileAttachment | undefined => {
  if (input.uri.length > 32 + 4 * Math.ceil(MaximumBytes / 3)) return undefined
  const match = /^data:(text\/plain|image\/(?:png|jpeg|gif|webp));base64,/.exec(input.uri)
  if (!match) return undefined
  const data = Option.getOrUndefined(decodeBase64(input.uri.slice(match[0].length)))
  if (data === undefined || data.length > 4 * Math.ceil(MaximumBytes / 3)) return undefined
  const file = FileAttachment.create({
    data,
    mime: match[1]!,
    source: { type: "inline" },
    name: input.name,
    description: input.description,
    mention: input.mention,
  })
  return descriptor(file) === undefined ? undefined : file
}

// The tuple and explicit nulls keep the cross-language commitment independent
// of object key order. Go's canonical encoder escapes U+2028 and U+2029.
export const revision = (input: Pick<Prompt, "text" | "files" | "agents" | "skills">) => {
  if (
    !unicode(input.text) ||
    input.agents?.length ||
    input.skills?.length ||
    (input.files?.length ?? 0) > MaximumFiles ||
    (input.text === "" && !input.files?.length)
  )
    return undefined
  const files = (input.files ?? []).map(descriptor)
  if (files.some((file) => file === undefined)) return undefined
  const measured = files.filter((file) => file !== undefined)
  if (measured.reduce((bytes, file) => bytes + file[5], 0) > MaximumBytes) return undefined
  return Hash.sha256(
    JSON.stringify(["native-human-inline-content-v1", input.text, measured])
      .replace(/\u2028/g, "\\u2028")
      .replace(/\u2029/g, "\\u2029"),
  )
}

export const originalRevision = (input: {
  readonly text: string
  readonly files?: readonly unknown[]
  readonly agents?: readonly unknown[]
  readonly skills?: readonly unknown[]
}) => {
  if (input.agents?.length || input.skills?.length || (input.files?.length ?? 0) > MaximumFiles) return undefined
  const files = (input.files ?? []).map((value) => {
    const decoded = Option.getOrUndefined(decodeFile(value))
    return decoded === undefined ? undefined : materialize(decoded)
  })
  if (files.some((file) => file === undefined)) return undefined
  return revision({ text: input.text, files: files.filter((file) => file !== undefined) })
}

function descriptor(file: FileAttachment) {
  if (
    file.source.type !== "inline" ||
    !mimes.has(file.mime) ||
    file.data.length > 4 * Math.ceil(MaximumBytes / 3) ||
    (file.name !== undefined && !unicode(file.name)) ||
    (file.description !== undefined && !unicode(file.description)) ||
    (file.mention !== undefined &&
      (!unicode(file.mention.text) ||
        !Number.isSafeInteger(file.mention.start) ||
        !Number.isSafeInteger(file.mention.end) ||
        file.mention.start < 0 ||
        file.mention.end < file.mention.start))
  )
    return undefined
  const bytes = Buffer.from(file.data, "base64")
  if (bytes.toString("base64") !== file.data || bytes.byteLength > MaximumBytes) return undefined
  if (file.mime === "text/plain") {
    if (Buffer.from(bytes.toString("utf8"), "utf8").equals(bytes) === false) return undefined
  } else if (Mime.detect(bytes) !== file.mime) return undefined
  return [
    file.mime,
    file.name ?? null,
    file.description ?? null,
    file.mention === undefined ? null : [file.mention.start, file.mention.end, file.mention.text],
    Hash.sha256(bytes),
    bytes.byteLength,
  ] as const
}
