export * as SessionInputRequirements from "./input-requirements.js"

import type { Mcp } from "@opencode/schema/mcp"

// Markers describe required execution contracts. The only contract is "mcp":
// the input needs its own request-scoped MCP servers. Unknown or malformed
// durable requirements must park work.
export const supported = (required: readonly string[] = []) =>
  new Set(required).size === required.length && required.every((value) => value === "mcp")

export const auxiliaryBlocked = (required: readonly string[] = []) => !supported(required)

export const matches = (required: readonly string[], input: { readonly mcp?: Mcp.RequestServers }) =>
  supported(required) && (input.mcp !== undefined) === required.includes("mcp")
