export * as Token from "./token.js"

const CHARS_PER_TOKEN = 4

export const estimateLength = (length: number) => Math.max(0, Math.round(length / CHARS_PER_TOKEN))

export const estimate = (input: string) => estimateLength(input.length)
