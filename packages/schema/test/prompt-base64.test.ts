import { expect, test } from "bun:test"
import { Schema } from "effect"
import { Base64 } from "../src/prompt.js"

const valid = Schema.is(Base64)

test("attachment base64 validates the existing padded language", () => {
  for (const input of ["", "YQ==", "YWI=", "YWJj", "YWJjZA==", "////", "+/+/", "Yb=="]) expect(valid(input)).toBe(true)
  for (const input of [
    "YQ",
    "YQ=",
    "Y===",
    "=",
    "YW Jj",
    "YWJj\n",
    "YWJj-___",
    "YW=J",
    "abcde===",
    "abc\n",
    "ab=\r",
    "abc\u2028",
    "abc\u2029",
  ])
    expect(valid(input)).toBe(false)
})

test("valid multi-megabyte attachments do not hit regexp repetition limits", () => {
  const data = Buffer.alloc(20 * 1024 * 1024, 97).toString("base64")
  expect(valid(data)).toBe(true)
  expect(Base64.make(data)).toBe(data)
})
