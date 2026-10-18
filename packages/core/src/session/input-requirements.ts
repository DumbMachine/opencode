export * as SessionInputRequirements from "./input-requirements.js"

import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import type { Mcp } from "@opencode/schema/mcp"

// Markers describe required execution contracts, not completeness of context.
// The original Session policy and actual publishers enforce publication-v1;
// unknown or malformed durable requirements must park work.
export const supported = (required: readonly string[] = []) =>
  new Set(required).size === required.length &&
  required.every((value) => value === "mcp" || value === "disclosure" || value === "publication-v1") &&
  (!required.includes("publication-v1") || required.includes("disclosure"))

export const auxiliaryBlocked = (required: readonly string[] = []) =>
  !supported(required) || required.includes("disclosure")

export const serviceSupported = (service: ExecutionDisclosure.Service | undefined) =>
  service?.publicationVersion === undefined || service.publicationVersion === 1

export const matches = (
  required: readonly string[],
  input: { readonly mcp?: Mcp.RequestServers; readonly disclosure?: ExecutionDisclosure.Service },
) =>
  supported(required) &&
  serviceSupported(input.disclosure) &&
  (input.disclosure?.publicationVersion === 1) === required.includes("publication-v1") &&
  (input.mcp !== undefined) === required.includes("mcp") &&
  (input.disclosure !== undefined) === required.includes("disclosure")
