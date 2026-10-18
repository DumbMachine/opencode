export * as SessionInputRequirements from "./input-requirements.js"

import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import type { Mcp } from "@opencode/schema/mcp"

// Recognizing a schema is not support for its execution contract. Publication
// remains unavailable until every required producer and publisher is connected.
// Unknown durable markers must park work, never silently become ready.
export const supported = (required: readonly string[] = []) =>
  new Set(required).size === required.length && required.every((value) => value === "mcp" || value === "disclosure")

export const auxiliaryBlocked = (required: readonly string[] = []) =>
  !supported(required) || required.includes("disclosure")

export const serviceSupported = (service: ExecutionDisclosure.Service | undefined) =>
  service?.publicationVersion === undefined

export const matches = (
  required: readonly string[],
  input: { readonly mcp?: Mcp.RequestServers; readonly disclosure?: ExecutionDisclosure.Service },
) =>
  supported(required) &&
  serviceSupported(input.disclosure) &&
  (input.mcp !== undefined) === required.includes("mcp") &&
  (input.disclosure !== undefined) === required.includes("disclosure")
